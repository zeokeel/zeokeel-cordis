//! Regression tests for the 0.4.2 core fixes (FIX-1 .. FIX-10, FIX-14).
//!
//! Every test encodes behavior that was **broken** on the 0.4.1 baseline:
//! refresh latch after a cleanup panic (FIX-1/FIX-2), contained unload on the
//! retire path (FIX-2), `dispose_requested` orphans on update/restart early
//! returns (FIX-3), stale activation effect tagging (FIX-5), missing
//! `Active -> Unloading` status events (FIX-6), partial `Registry::replace`
//! rollback (FIX-7), `Fiber::replace_plugin` root residue (FIX-8),
//! `dispose_all` dropping a panic payload (FIX-9) and `update` committing the
//! raw config before the `internal/update` verdict (FIX-14).
//!
//! FIX-4 (sweep panic containment) and FIX-10 (`Drop for FiberData` owner
//! cleanup) need internal state that is not constructible through the public
//! API; their regression tests live in `src/fiber.rs` next to the code.
//!
//! Tests that coordinate threads run their body under a watchdog so a kernel
//! deadlock fails the test instead of hanging the whole harness.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use zeokeel_cordis::{
    block_on, cleanup, ConfigNext, ConfigSchema, Context, CordisError, Fiber, FiberState,
    JsonConfigSchema, Plugin, PluginKey, Result, ServiceKey, UpdateNext,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Run `body` on its own thread and fail the test (instead of hanging the
/// harness) if it does not finish in time.
fn watchdog<F, T>(label: &str, timeout: Duration, body: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = mpsc::channel::<std::thread::Result<T>>();
    let label = label.to_string();
    let handle = std::thread::spawn(move || {
        let outcome = catch_unwind(AssertUnwindSafe(body));
        let _ = tx.send(outcome);
    });
    match rx.recv_timeout(timeout) {
        Ok(Ok(value)) => {
            let _ = handle.join();
            value
        }
        Ok(Err(payload)) => {
            let _ = handle.join();
            std::panic::resume_unwind(payload)
        }
        Err(_) => panic!("WATCHDOG: `{label}` hung for {timeout:?} (kernel hang/deadlock)"),
    }
}

/// A reusable open-once latch (`Send + Sync`, unlike `mpsc::Sender`).
#[derive(Default)]
struct Latch {
    open: Mutex<bool>,
    signal: Condvar,
}

impl Latch {
    fn is_open(&self) -> bool {
        *self.open.lock().expect("latch poisoned")
    }

    fn wait(&self) {
        let mut open = self.open.lock().expect("latch poisoned");
        while !*open {
            open = self.signal.wait(open).expect("latch poisoned");
        }
    }

    fn open(&self) {
        *self.open.lock().expect("latch poisoned") = true;
        self.signal.notify_all();
    }
}

fn labels(fiber: &Fiber) -> Vec<String> {
    fiber
        .get_effects()
        .iter()
        .map(|effect| effect.label.clone())
        .collect()
}

// ---------------------------------------------------------------------------
// Contract: the two new error variants are part of the public API
// ---------------------------------------------------------------------------

#[test]
fn fix00_contract_error_variants_are_displayed() {
    let mismatch = CordisError::ServiceTypeMismatch("fix00.text".to_string());
    assert!(
        mismatch.to_string().contains("fix00.text"),
        "ServiceTypeMismatch must name the service: {mismatch}"
    );
    let rollback = CordisError::Rollback("original error: boom; rollback error: bang".to_string());
    let message = rollback.to_string();
    assert!(
        message.contains("boom") && message.contains("bang"),
        "Rollback must keep both errors: {message}"
    );
}

// ---------------------------------------------------------------------------
// FIX-1 / FIX-2: a cleanup panic must not latch dependency refresh forever
// ---------------------------------------------------------------------------

static FIX01: ServiceKey<u32> = ServiceKey::new("fix01.svc");

#[test]
fn fix01_refresh_recovers_after_a_contained_cleanup_panic() {
    struct Consumer {
        applies: Arc<AtomicUsize>,
    }

    impl Plugin for Consumer {
        fn name(&self) -> &'static str {
            "fix01-consumer"
        }

        fn inject(&self) -> &'static [&'static str] {
            &["fix01.svc"]
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            self.applies.fetch_add(1, Ordering::SeqCst);
            // Panics when this consumer is retired (gate closed) or disposed.
            ctx.effect(|| vec![cleanup(|| panic!("fix01 consumer cleanup boom"))])?;
            Ok(())
        }
    }

    let root = Context::root();
    let available = Arc::new(AtomicBool::new(true));
    let check = available.clone();
    let _service = root
        .provide_checked(FIX01, Arc::new(1u32), move || check.load(Ordering::SeqCst))
        .expect("provide gated service");
    let applies = Arc::new(AtomicUsize::new(0));
    let consumer = root
        .plugin(
            Arc::new(Consumer {
                applies: applies.clone(),
            }),
            Value::Null,
        )
        .expect("consumer");
    assert_eq!(consumer.state(), FiberState::Active);
    assert_eq!(applies.load(Ordering::SeqCst), 1);

    // Closing the gate retires the consumer; its cleanup panics. Before the
    // fix the panic unwound out of `refresh_all` and left the re-entrancy latch
    // set forever (all later refreshes became no-ops, silently).
    available.store(false, Ordering::SeqCst);
    let _ = catch_unwind(AssertUnwindSafe(|| root.refresh()));
    assert_eq!(
        consumer.state(),
        FiberState::Pending,
        "the dependent must be retired even when its cleanup panics"
    );

    // Reopening the gate must re-activate it: the refresh latch has to be
    // released again (FIX-1) and the retire path must not abort the provider
    // unload (FIX-2).
    available.store(true, Ordering::SeqCst);
    root.refresh();
    assert_eq!(
        consumer.state(),
        FiberState::Active,
        "FIX-1: dependency refresh must recover after a cleanup panic"
    );
    assert_eq!(applies.load(Ordering::SeqCst), 2);
}

// ---------------------------------------------------------------------------
// FIX-2: dependent cleanup panic must not stick the provider in `Unloading`
// ---------------------------------------------------------------------------

static FIX02: ServiceKey<u32> = ServiceKey::new("fix02.svc");

#[test]
fn fix02_provider_dispose_contains_a_panicking_dependent_cleanup() {
    struct Provider;

    impl Plugin for Provider {
        fn name(&self) -> &'static str {
            "fix02-provider"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            ctx.provide(FIX02, Arc::new(1u32))?;
            Ok(())
        }
    }

    struct Dependent {
        applies: Arc<AtomicUsize>,
    }

    impl Plugin for Dependent {
        fn name(&self) -> &'static str {
            "fix02-dependent"
        }

        fn inject(&self) -> &'static [&'static str] {
            &["fix02.svc"]
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            self.applies.fetch_add(1, Ordering::SeqCst);
            ctx.effect(|| vec![cleanup(|| panic!("fix02 dependent cleanup boom"))])?;
            Ok(())
        }
    }

    let root = Context::root();
    let provider = root
        .plugin(Arc::new(Provider), Value::Null)
        .expect("provider");
    let applies = Arc::new(AtomicUsize::new(0));
    let dependent = root
        .plugin(
            Arc::new(Dependent {
                applies: applies.clone(),
            }),
            Value::Null,
        )
        .expect("dependent");
    assert_eq!(dependent.state(), FiberState::Active);

    // The provider unload retires the dependent first (provider-withdrawal
    // order); the dependent's cleanup panics. Before the fix that panic escaped
    // `retire_unready` and left the provider stuck in `Unloading` forever.
    let outcome = catch_unwind(AssertUnwindSafe(|| provider.dispose()));
    assert!(
        matches!(outcome, Ok(Ok(()))),
        "FIX-2: the dependent's cleanup panic must be contained by the provider unload"
    );
    assert_eq!(provider.state(), FiberState::Disposed);
    assert_eq!(dependent.state(), FiberState::Pending);
    assert!(
        root.registry().is_idle(),
        "the registry must be idle after a contained cleanup panic"
    );

    // The kernel stays usable: a fresh provider re-activates the dependent.
    let _provider = root
        .plugin(Arc::new(Provider), Value::Null)
        .expect("provider again");
    assert_eq!(dependent.state(), FiberState::Active);
    assert_eq!(applies.load(Ordering::SeqCst), 2);
}

// ---------------------------------------------------------------------------
// FIX-3: `update` early errors must still finalize a requested dispose
// ---------------------------------------------------------------------------

#[test]
fn fix03_update_early_error_finalizes_a_requested_dispose() {
    watchdog("fix03_update", Duration::from_secs(30), || {
        struct SchemaChild;

        impl Plugin for SchemaChild {
            fn name(&self) -> &'static str {
                "fix03-child"
            }

            fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>> {
                Some(Arc::new(JsonConfigSchema::new().required("n")))
            }

            fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
                Ok(())
            }
        }

        struct Parent {
            child: Arc<Mutex<Option<Fiber>>>,
        }

        impl Plugin for Parent {
            fn name(&self) -> &'static str {
                "fix03-parent"
            }

            fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
                let child = ctx.plugin(Arc::new(SchemaChild), json!({ "n": 1 }))?;
                *self.child.lock().unwrap() = Some(child);
                Ok(())
            }
        }

        let root = Context::root();
        let child_slot = Arc::new(Mutex::new(None));
        let parent = root
            .plugin(
                Arc::new(Parent {
                    child: child_slot.clone(),
                }),
                Value::Null,
            )
            .expect("parent");
        let child = child_slot
            .lock()
            .unwrap()
            .clone()
            .expect("parent must have loaded the child");
        assert_eq!(child.state(), FiberState::Active);
        let child_id = child.id();

        // Latch the child's `internal/config` waterfall: the child update owns
        // the child's lifecycle while the parent disposes (and flags the child).
        let entered = Arc::new(Latch::default());
        let release = Arc::new(Latch::default());
        let entered_hook = entered.clone();
        let release_hook = release.clone();
        let _hook = root
            .on_internal_config(Arc::new(
                move |fiber: &Fiber, config: Value, next: &ConfigNext<'_>| {
                    if fiber.id() == child_id {
                        entered_hook.open();
                        release_hook.wait();
                    }
                    next.next(config)
                },
            ))
            .expect("config hook");

        let updater = {
            let child = child.clone();
            std::thread::spawn(move || child.update(json!({})))
        };
        entered.wait();
        // The parent sweep cannot dispose the busy child: it flags it instead.
        parent.dispose().expect("parent dispose");
        assert_eq!(parent.state(), FiberState::Disposed);
        release.open();

        let update_result = updater.join().expect("updater thread must not panic");
        assert!(
            matches!(update_result, Err(CordisError::Validation(_))),
            "the invalid config must be rejected: {update_result:?}"
        );
        assert_eq!(
            child.state(),
            FiberState::Disposed,
            "FIX-3: a flagged child must be finalized when `update` returns early"
        );
        assert_eq!(root.registry().fiber_count(), 0);
        assert!(root.registry().is_idle());
    });
}

// ---------------------------------------------------------------------------
// FIX-3: a panicking `restart` must still finalize a requested dispose
// ---------------------------------------------------------------------------

#[test]
fn fix03_restart_panic_finalizes_a_requested_dispose() {
    watchdog("fix03_restart", Duration::from_secs(30), || {
        struct PanickingCleanup {
            entered: Arc<Latch>,
            release: Arc<Latch>,
        }

        impl Plugin for PanickingCleanup {
            fn name(&self) -> &'static str {
                "fix03b-child"
            }

            fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
                let entered = self.entered.clone();
                let release = self.release.clone();
                ctx.effect(move || {
                    vec![cleanup(move || {
                        entered.open();
                        release.wait();
                        panic!("fix03b cleanup boom");
                    })]
                })?;
                Ok(())
            }
        }

        struct Parent {
            child: Arc<Mutex<Option<Fiber>>>,
            entered: Arc<Latch>,
            release: Arc<Latch>,
        }

        impl Plugin for Parent {
            fn name(&self) -> &'static str {
                "fix03b-parent"
            }

            fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
                let child = ctx.plugin(
                    Arc::new(PanickingCleanup {
                        entered: self.entered.clone(),
                        release: self.release.clone(),
                    }),
                    Value::Null,
                )?;
                *self.child.lock().unwrap() = Some(child);
                Ok(())
            }
        }

        let root = Context::root();
        let entered = Arc::new(Latch::default());
        let release = Arc::new(Latch::default());
        let child_slot = Arc::new(Mutex::new(None));
        let parent = root
            .plugin(
                Arc::new(Parent {
                    child: child_slot.clone(),
                    entered: entered.clone(),
                    release: release.clone(),
                }),
                Value::Null,
            )
            .expect("parent");
        let child = child_slot.lock().unwrap().clone().expect("child handle");
        assert_eq!(child.state(), FiberState::Active);

        // The child restart drains its effects; the cleanup blocks, then panics.
        let restarter = {
            let child = child.clone();
            std::thread::spawn(move || child.restart())
        };
        entered.wait();
        // The parent dispose sees a busy child and flags it for disposal.
        parent.dispose().expect("parent dispose");
        assert_eq!(parent.state(), FiberState::Disposed);
        release.open();

        // The restart re-raises the cleanup panic on its own thread; the
        // FinalizeGuard must still dispose the flagged child while unwinding.
        let restart_result = restarter.join();
        assert!(
            restart_result.is_err(),
            "a cleanup panic must stay visible to the restart caller"
        );
        assert_eq!(
            child.state(),
            FiberState::Disposed,
            "FIX-3: a panicking `restart` must not leak a flagged child"
        );
        assert!(root.registry().is_idle());
    });
}

// ---------------------------------------------------------------------------
// FIX-5: a stale activation's effects are tagged with their own epoch
// ---------------------------------------------------------------------------

static FIX05: ServiceKey<u32> = ServiceKey::new("fix05.svc");

#[test]
fn fix05_stale_activation_effects_are_discarded() {
    watchdog("fix05", Duration::from_secs(30), || {
        struct SlowApply {
            applies: Arc<AtomicUsize>,
            entered: Arc<Latch>,
            release: Arc<Latch>,
        }

        impl Plugin for SlowApply {
            fn name(&self) -> &'static str {
                "fix05-slow"
            }

            fn inject(&self) -> &'static [&'static str] {
                &["fix05.svc"]
            }

            fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
                let call = self.applies.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    // First attempt: block, then register its effect even though
                    // a newer activation has superseded it in the meantime.
                    self.entered.open();
                    self.release.wait();
                    ctx.effect_labeled("stale-tail", || vec![cleanup(|| {})])?;
                } else {
                    ctx.effect_labeled("new", || vec![cleanup(|| {})])?;
                }
                Ok(())
            }
        }

        let root = Context::root();
        let applies = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Latch::default());
        let release = Arc::new(Latch::default());
        let fiber = root
            .plugin(
                Arc::new(SlowApply {
                    applies: applies.clone(),
                    entered: entered.clone(),
                    release: release.clone(),
                }),
                Value::Null,
            )
            .expect("slow plugin stays Pending until its dependency exists");
        assert_eq!(fiber.state(), FiberState::Pending);

        // Providing the dependency activates the fiber on this thread: its
        // first `apply` blocks inside the latch.
        let provider_root = root.clone();
        let activator = std::thread::spawn(move || {
            provider_root
                .provide(FIX05, Arc::new(1u32))
                .expect("provide")
        });
        entered.wait();

        // A concurrent restart supersedes the in-flight activation with a new
        // epoch; its own `apply` completes and registers the `new` effect.
        fiber.restart().expect("restart during activation");
        assert_eq!(fiber.state(), FiberState::Active);
        assert_eq!(applies.load(Ordering::SeqCst), 2);

        // Let the stale attempt register its tail effect and finish.
        release.open();
        let _service = activator.join().expect("activator thread");

        let effect_labels = labels(&fiber);
        assert_eq!(
            effect_labels,
            vec!["new".to_string()],
            "FIX-5: an effect registered by a superseded activation must be discarded"
        );
    });
}

// ---------------------------------------------------------------------------
// FIX-6: `restart` announces the `Active -> Unloading` transition
// ---------------------------------------------------------------------------

#[test]
fn fix06_restart_announces_the_unloading_transition() {
    struct Noop;

    impl Plugin for Noop {
        fn name(&self) -> &'static str {
            "fix06-plugin"
        }

        fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
            Ok(())
        }
    }

    let root = Context::root();
    let trace: Arc<Mutex<Vec<(String, FiberState, FiberState)>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = trace.clone();
    let _hook = root
        .on_internal_status(Arc::new(move |fiber: &Fiber, old: FiberState| {
            if let Ok(mut trace) = sink.lock() {
                trace.push((fiber.name(), old, fiber.state()));
            }
        }))
        .expect("status hook");

    let fiber = root
        .plugin(Arc::new(Noop), Value::Null)
        .expect("noop plugin");
    trace.lock().unwrap().clear();
    fiber.restart().expect("restart");

    let transitions = trace.lock().unwrap().clone();
    assert!(
        transitions.iter().any(|(name, old, next)| {
            name == "fix06-plugin" && *old == FiberState::Active && *next == FiberState::Unloading
        }),
        "FIX-6: restart must announce Active -> Unloading, got {transitions:?}"
    );
    assert!(
        transitions.iter().any(|(name, old, next)| {
            name == "fix06-plugin" && *old == FiberState::Unloading && *next == FiberState::Pending
        }),
        "FIX-6: the Unloading -> Pending transition must now be sourced, got {transitions:?}"
    );
}

// ---------------------------------------------------------------------------
// FIX-7: a rejected `Registry::replace` reloads already-reloaded fibers back
// ---------------------------------------------------------------------------

#[test]
fn fix07_failed_replace_rolls_the_reloaded_fiber_back() {
    watchdog("fix07", Duration::from_secs(30), || {
        struct OldImpl {
            applies: Arc<AtomicUsize>,
        }

        impl Plugin for OldImpl {
            fn name(&self) -> &'static str {
                "fix07-old"
            }

            fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
                self.applies.fetch_add(1, Ordering::SeqCst);
                ctx.effect_labeled("old-effect", || vec![cleanup(|| {})])?;
                Ok(())
            }
        }

        struct NewImpl {
            applies: Arc<AtomicUsize>,
        }

        impl Plugin for NewImpl {
            fn name(&self) -> &'static str {
                "fix07-new"
            }

            fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
                self.applies.fetch_add(1, Ordering::SeqCst);
                ctx.effect_labeled("new-effect", || vec![cleanup(|| {})])?;
                Ok(())
            }
        }

        let root = Context::root();
        let old_applies = Arc::new(AtomicUsize::new(0));
        let new_applies = Arc::new(AtomicUsize::new(0));
        let old: Arc<dyn Plugin> = Arc::new(OldImpl {
            applies: old_applies.clone(),
        });
        let fiber1 = root.plugin(old.clone(), Value::Null).expect("fiber1");
        let fiber2 = root.plugin(old, Value::Null).expect("fiber2");
        assert_eq!(old_applies.load(Ordering::SeqCst), 2);
        let fiber2_id = fiber2.id();

        // Occupy fiber2's lifecycle slot so the replace loop rejects on it
        // *after* fiber1 was already reloaded with the new implementation.
        let entered = Arc::new(Latch::default());
        let release = Arc::new(Latch::default());
        let entered_hook = entered.clone();
        let release_hook = release.clone();
        let _hook = root
            .on_internal_config(Arc::new(
                move |fiber: &Fiber, config: Value, next: &ConfigNext<'_>| {
                    if fiber.id() == fiber2_id {
                        entered_hook.open();
                        release_hook.wait();
                    }
                    next.next(config)
                },
            ))
            .expect("config hook");
        let updater = {
            let fiber2 = fiber2.clone();
            std::thread::spawn(move || fiber2.update(json!({ "x": 1 })))
        };
        entered.wait();

        let replaced = root.registry().replace(
            PluginKey::Named("fix07-old"),
            Arc::new(NewImpl {
                applies: new_applies.clone(),
            }),
        );
        assert!(
            matches!(replaced, Err(CordisError::LifecycleBusy)),
            "the busy fiber must reject the replace"
        );
        assert_eq!(
            new_applies.load(Ordering::SeqCst),
            1,
            "fiber1 must have run the new implementation before the rejection"
        );
        assert_eq!(
            fiber1.name(),
            "fix07-old",
            "the plugin binding must be rolled back"
        );
        let effect_labels = labels(&fiber1);
        assert!(
            effect_labels.iter().any(|label| label == "old-effect"),
            "FIX-7: the rollback must re-run the old implementation: {effect_labels:?}"
        );
        assert!(
            !effect_labels.iter().any(|label| label == "new-effect"),
            "FIX-7: no effect of the new implementation may survive the rollback: {effect_labels:?}"
        );
        assert_eq!(
            old_applies.load(Ordering::SeqCst),
            3,
            "the rollback must reload fiber1 with the old implementation"
        );
        assert!(root.registry().has(PluginKey::Named("fix07-old")));
        assert!(!root.registry().has(PluginKey::Named("fix07-new")));

        release.open();
        let _ = updater.join().expect("updater thread");
    });
}

// ---------------------------------------------------------------------------
// FIX-8: `replace_plugin` is a lifecycle operation and the root fiber leaves
// no temporary runtime behind on failure
// ---------------------------------------------------------------------------

#[test]
fn fix08_failed_root_replace_leaves_the_registry_unchanged() {
    watchdog("fix08_root", Duration::from_secs(30), || {
        struct RootReplacement;

        impl Plugin for RootReplacement {
            fn name(&self) -> &'static str {
                "fix08-root-new"
            }

            fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
                Ok(())
            }
        }

        let root = Context::root();
        let entered = Arc::new(Latch::default());
        let release = Arc::new(Latch::default());
        let entered_hook = entered.clone();
        let release_hook = release.clone();
        let _hook = root
            .on_internal_config(Arc::new(
                move |fiber: &Fiber, config: Value, next: &ConfigNext<'_>| {
                    if fiber.id() == 0 {
                        entered_hook.open();
                        release_hook.wait();
                    }
                    next.next(config)
                },
            ))
            .expect("config hook");
        let updater = {
            let root = root.clone();
            std::thread::spawn(move || root.fiber().update(json!({ "x": 1 })))
        };
        entered.wait();

        let before_keys = root.registry().keys();
        let before_size = root.registry().size();
        let replaced = root.fiber().replace_plugin(Arc::new(RootReplacement));
        assert!(
            matches!(replaced, Err(CordisError::LifecycleBusy)),
            "FIX-8: a busy root fiber must report LifecycleBusy: {replaced:?}"
        );
        assert_eq!(
            root.registry().size(),
            before_size,
            "a failed root replace must not register a temporary runtime"
        );
        let mut after_keys = root.registry().keys();
        after_keys.sort_by_key(|key| format!("{key:?}"));
        let mut sorted_before = before_keys;
        sorted_before.sort_by_key(|key| format!("{key:?}"));
        assert_eq!(after_keys, sorted_before, "registry keys must be unchanged");
        assert!(!root.registry().has(PluginKey::Named("fix08-root-new")));

        release.open();
        let _ = updater.join().expect("updater thread");
    });
}

#[test]
fn fix08_replace_plugin_reports_busy_while_another_operation_owns_the_fiber() {
    watchdog("fix08_busy", Duration::from_secs(30), || {
        struct Noop;

        impl Plugin for Noop {
            fn name(&self) -> &'static str {
                "fix08-plugin"
            }

            fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
                Ok(())
            }
        }

        let root = Context::root();
        let fiber = root
            .plugin(Arc::new(Noop), Value::Null)
            .expect("noop plugin");
        let fiber_id = fiber.id();
        let entered = Arc::new(Latch::default());
        let release = Arc::new(Latch::default());
        let entered_hook = entered.clone();
        let release_hook = release.clone();
        let _hook = root
            .on_internal_config(Arc::new(
                move |candidate: &Fiber, config: Value, next: &ConfigNext<'_>| {
                    if candidate.id() == fiber_id {
                        entered_hook.open();
                        release_hook.wait();
                    }
                    next.next(config)
                },
            ))
            .expect("config hook");
        let updater = {
            let fiber = fiber.clone();
            std::thread::spawn(move || fiber.update(json!({ "x": 1 })))
        };
        entered.wait();

        // Before FIX-8 the plugin was swapped and re-bound before `restart`
        // noticed the busy lifecycle slot; now the whole operation is guarded.
        let replaced = fiber.replace_plugin(Arc::new(Noop));
        assert!(
            matches!(replaced, Err(CordisError::LifecycleBusy)),
            "FIX-8: replace_plugin must report LifecycleBusy: {replaced:?}"
        );
        assert_eq!(fiber.name(), "fix08-plugin");

        release.open();
        let _ = updater.join().expect("updater thread");
    });
}

// ---------------------------------------------------------------------------
// FIX-9: `dispose_all` surfaces a panic even when another fiber errored
// ---------------------------------------------------------------------------

#[test]
fn fix09_dispose_all_reports_the_panic_when_a_fiber_errors() {
    watchdog("fix09", Duration::from_secs(30), || {
        struct BlockingCleanup {
            entered: Arc<Latch>,
            release: Arc<Latch>,
        }

        impl Plugin for BlockingCleanup {
            fn name(&self) -> &'static str {
                "fix09-busy"
            }

            fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
                let entered = self.entered.clone();
                let release = self.release.clone();
                ctx.effect(move || {
                    vec![cleanup(move || {
                        entered.open();
                        release.wait();
                    })]
                })?;
                Ok(())
            }
        }

        struct PanickingCleanup;

        impl Plugin for PanickingCleanup {
            fn name(&self) -> &'static str {
                "fix09-panic"
            }

            fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
                ctx.effect(|| vec![cleanup(|| panic!("fix09 cleanup boom"))])?;
                Ok(())
            }
        }

        let root = Context::root();
        let entered = Arc::new(Latch::default());
        let release = Arc::new(Latch::default());
        let busy = root
            .plugin(
                Arc::new(BlockingCleanup {
                    entered: entered.clone(),
                    release: release.clone(),
                }),
                Value::Null,
            )
            .expect("blocking fiber");
        let _panicking = root
            .plugin(Arc::new(PanickingCleanup), Value::Null)
            .expect("panicking fiber");

        let disposer = {
            let busy = busy.clone();
            std::thread::spawn(move || busy.dispose())
        };
        entered.wait();

        // One fiber is busy (Err) and another panics: the panic payload wins
        // (the error is only logged) so the caller can observe the panic.
        let outcome = catch_unwind(AssertUnwindSafe(|| root.dispose_all()));
        assert!(
            outcome.is_err(),
            "FIX-9: a panic payload must not be dropped in favour of a LifecycleBusy error"
        );

        release.open();
        let _ = disposer.join().expect("disposer thread");
    });
}

// ---------------------------------------------------------------------------
// FIX-14: raw config commit follows the `internal/update` verdict
// ---------------------------------------------------------------------------

struct CfgPlugin {
    applies: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Value>>>,
}

impl Plugin for CfgPlugin {
    fn name(&self) -> &'static str {
        "fix14-plugin"
    }

    fn apply(&self, _ctx: Context, config: Value) -> Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        self.seen.lock().unwrap().push(config);
        Ok(())
    }
}

#[test]
fn fix14_vetoed_update_keeps_the_raw_config() {
    let root = Context::root();
    let applies = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(
            Arc::new(CfgPlugin {
                applies: applies.clone(),
                seen: seen.clone(),
            }),
            json!({ "v": 1 }),
        )
        .expect("plugin");
    assert_eq!(applies.load(Ordering::SeqCst), 1);

    let _veto = root
        .on_internal_update(Arc::new(
            |_fiber: &Fiber, _config: Value, _no_save: bool, _next: &UpdateNext<'_>| {
                // Never calls `next`: vetoes the update and the restart.
            },
        ))
        .expect("update hook");

    fiber
        .update(json!({ "v": 2 }))
        .expect("vetoed update is Ok");
    assert_eq!(
        fiber.raw_config(),
        json!({ "v": 1 }),
        "FIX-14: a vetoed update must not leak its config into raw_config"
    );
    assert_eq!(applies.load(Ordering::SeqCst), 1, "a veto must not restart");
}

/// TS parity (FIX-14): `update(config, noSave)` writes `_config`
/// unconditionally; `noSave` is only a hint for persistence hooks. The kernel
/// therefore keeps the new raw config and restarts, even when a hook forwards
/// the `no_save` hint.
#[test]
fn fix14_no_save_hint_still_stores_the_raw_config() {
    let root = Context::root();
    let applies = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(
            Arc::new(CfgPlugin {
                applies: applies.clone(),
                seen: seen.clone(),
            }),
            json!({ "v": 1 }),
        )
        .expect("plugin");
    let _hook = root
        .on_internal_update(Arc::new(
            |_fiber: &Fiber, config: Value, _no_save: bool, next: &UpdateNext<'_>| {
                next.next(config, true);
            },
        ))
        .expect("update hook");

    fiber
        .update(json!({ "v": 2 }))
        .expect("update with a no_save hint");
    assert_eq!(
        fiber.raw_config(),
        json!({ "v": 2 }),
        "FIX-14: no_save is a persistence hint, not a kernel-level skip \
         (Cordis assigns `_config` unconditionally)"
    );
    assert_eq!(
        applies.load(Ordering::SeqCst),
        2,
        "the hinted update still restarts the fiber"
    );
    assert_eq!(
        *seen.lock().unwrap(),
        vec![json!({ "v": 1 }), json!({ "v": 2 })],
        "the restart re-resolves the newly committed raw config"
    );
}

#[test]
fn fix14_replacement_config_becomes_the_new_raw_config() {
    let root = Context::root();
    let applies = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(
            Arc::new(CfgPlugin {
                applies: applies.clone(),
                seen: seen.clone(),
            }),
            json!({ "v": 1 }),
        )
        .expect("plugin");
    let _hook = root
        .on_internal_update(Arc::new(
            |_fiber: &Fiber, _config: Value, _no_save: bool, next: &UpdateNext<'_>| {
                next.next(json!({ "v": 3 }), false);
            },
        ))
        .expect("update hook");

    fiber.update(json!({ "v": 2 })).expect("replacing update");
    assert_eq!(
        fiber.raw_config(),
        json!({ "v": 3 }),
        "FIX-14: a validated replacement becomes the new raw config"
    );
    assert_eq!(
        applies.load(Ordering::SeqCst),
        2,
        "the replacement update restarts the fiber"
    );
    assert_eq!(
        *seen.lock().unwrap(),
        vec![json!({ "v": 1 }), json!({ "v": 3 })],
        "the restart re-resolves the replaced raw config"
    );
}

// ---------------------------------------------------------------------------
// D-1: concurrent rebind / unregister must not leak runtimes
// ---------------------------------------------------------------------------

struct D1Plugin {
    label: &'static str,
}

impl Plugin for D1Plugin {
    fn name(&self) -> &'static str {
        self.label
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.effect_labeled("d1-effect", || vec![cleanup(|| {})])?;
        Ok(())
    }
}

/// D-1: `Fiber::replace_plugin` / `Registry::replace` / `dispose` racing each
/// other must not strand a `Disposed` fiber handle inside a runtime. On 0.4.1
/// the per-key detach can miss the runtime the fiber was moved into, so the
/// registry keeps a non-empty entry after `dispose_all` (and `is_idle` still
/// reports true).
#[test]
fn d1_concurrent_rebind_and_unregister_do_not_leak_runtimes() {
    watchdog("d1", Duration::from_secs(180), || {
        for variant in 0..4u8 {
            for iteration in 0..120usize {
                let root = Context::root();
                let plugin: Arc<dyn Plugin> = Arc::new(D1Plugin { label: "d1-race" });
                let fiber0 = root.plugin(plugin.clone(), Value::Null).expect("fiber0");
                let fiber1 = root.plugin(plugin, Value::Null).expect("fiber1");
                let mut handles = Vec::new();
                match variant {
                    // Fiber::replace_plugin ∥ Registry::replace on the runtime.
                    0 => {
                        let f0 = fiber0.clone();
                        handles.push(std::thread::spawn(move || {
                            let _ = catch_unwind(AssertUnwindSafe(|| {
                                f0.replace_plugin(Arc::new(D1Plugin { label: "d1-r1" }))
                            }));
                        }));
                        let root2 = root.clone();
                        handles.push(std::thread::spawn(move || {
                            let _ = catch_unwind(AssertUnwindSafe(|| {
                                root2.registry().replace(
                                    PluginKey::Named("d1-race"),
                                    Arc::new(D1Plugin { label: "d1-r2" }),
                                )
                            }));
                        }));
                    }
                    // Two concurrent Fiber::replace_plugin calls.
                    1 => {
                        for (index, fiber) in
                            [fiber0.clone(), fiber1.clone()].into_iter().enumerate()
                        {
                            let label: &'static str = if index == 0 { "d1-r1" } else { "d1-r2" };
                            handles.push(std::thread::spawn(move || {
                                let _ = catch_unwind(AssertUnwindSafe(|| {
                                    fiber.replace_plugin(Arc::new(D1Plugin { label }))
                                }));
                            }));
                        }
                    }
                    // Fiber::replace_plugin ∥ sibling dispose.
                    2 => {
                        let f0 = fiber0.clone();
                        handles.push(std::thread::spawn(move || {
                            let _ = catch_unwind(AssertUnwindSafe(|| {
                                f0.replace_plugin(Arc::new(D1Plugin { label: "d1-r1" }))
                            }));
                        }));
                        let f1 = fiber1.clone();
                        handles.push(std::thread::spawn(move || {
                            let _ = catch_unwind(AssertUnwindSafe(|| f1.dispose()));
                        }));
                    }
                    // Registry::replace ∥ sibling dispose.
                    _ => {
                        let root2 = root.clone();
                        handles.push(std::thread::spawn(move || {
                            let _ = catch_unwind(AssertUnwindSafe(|| {
                                root2.registry().replace(
                                    PluginKey::Named("d1-race"),
                                    Arc::new(D1Plugin { label: "d1-r2" }),
                                )
                            }));
                        }));
                        let f1 = fiber1.clone();
                        handles.push(std::thread::spawn(move || {
                            let _ = catch_unwind(AssertUnwindSafe(|| f1.dispose()));
                        }));
                    }
                }
                for handle in handles {
                    assert!(handle.join().is_ok(), "worker thread panicked");
                }
                let _ = catch_unwind(AssertUnwindSafe(|| {
                    let _ = root.dispose_all();
                }));
                let keys = root.registry().keys();
                assert!(
                    keys.is_empty(),
                    "D-1: leaked runtimes (variant={variant} iteration={iteration}): keys={keys:?}"
                );
                assert_eq!(
                    root.registry().fiber_count(),
                    0,
                    "D-1: live fibers left (variant={variant} iteration={iteration})"
                );
                assert!(
                    root.registry().is_idle(),
                    "D-1: registry not idle (variant={variant} iteration={iteration})"
                );
                block_on(root.when_idle());
                assert!(
                    root.registry().keys().is_empty(),
                    "D-1: runtime appeared after quiescence (variant={variant} iteration={iteration})"
                );
            }
        }
    });
}

// ---------------------------------------------------------------------------
// D-2: a replaced implementation's cleanup panic must not escape `replace`
// ---------------------------------------------------------------------------

struct D2Plugin {
    label: &'static str,
    label_effect: &'static str,
    cleanup_panics: bool,
    applies: Arc<AtomicUsize>,
}

impl Plugin for D2Plugin {
    fn name(&self) -> &'static str {
        self.label
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        let panics_on_cleanup = self.cleanup_panics;
        ctx.effect_labeled(self.label_effect, move || {
            vec![cleanup(move || {
                if panics_on_cleanup {
                    panic!("d2 cleanup boom");
                }
            })]
        })?;
        Ok(())
    }
}

/// D-2: the reload drains the old implementation's cleanups. When one of them
/// panics, `Registry::replace` must return an error and roll back instead of
/// unwinding out of its `Result` API with a half-migrated registry.
#[test]
fn d2_replace_contains_reload_cleanup_panic_and_rolls_back() {
    let root = Context::root();
    let old_applies = Arc::new(AtomicUsize::new(0));
    let new_applies = Arc::new(AtomicUsize::new(0));
    let first = root
        .plugin(
            Arc::new(D2Plugin {
                label: "d2-a",
                label_effect: "old-effect",
                cleanup_panics: false,
                applies: old_applies.clone(),
            }),
            Value::Null,
        )
        .expect("first fiber");
    let second = root
        .plugin(
            Arc::new(D2Plugin {
                label: "d2-a",
                label_effect: "old-effect-panic",
                cleanup_panics: true,
                applies: old_applies.clone(),
            }),
            Value::Null,
        )
        .expect("second fiber");
    assert_eq!(first.state(), FiberState::Active);
    assert_eq!(second.state(), FiberState::Active);

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        root.registry().replace(
            PluginKey::Named("d2-a"),
            Arc::new(D2Plugin {
                label: "d2-b",
                label_effect: "new-effect",
                cleanup_panics: false,
                applies: new_applies.clone(),
            }),
        )
    }));
    let result = match outcome {
        Ok(result) => result,
        Err(_) => panic!("D-2: the cleanup panic escaped Registry::replace"),
    };
    assert!(
        matches!(
            result,
            Err(CordisError::PluginFailed(_)) | Err(CordisError::Rollback(_))
        ),
        "D-2: replace must report the contained panic as an error"
    );
    assert_eq!(
        new_applies.load(Ordering::SeqCst),
        1,
        "the first fiber must have run the new implementation before the failure"
    );
    // The binding is rolled back: old runtime key, no new key, and both fibers
    // are reloaded with the old implementation (including the fiber whose
    // reload panicked, which the old code left deactivated).
    assert!(root.registry().has(PluginKey::Named("d2-a")));
    assert!(!root.registry().has(PluginKey::Named("d2-b")));
    assert_eq!(first.name(), "d2-a");
    assert_eq!(second.name(), "d2-a");
    assert_eq!(
        first.state(),
        FiberState::Active,
        "D-2: the already-reloaded fiber must be reloaded with the old implementation"
    );
    assert_eq!(
        second.state(),
        FiberState::Active,
        "D-2: the fiber whose reload panicked must be reloaded with the old implementation"
    );
    let first_labels = labels(&first);
    assert!(
        first_labels.iter().any(|label| label == "old-effect"),
        "D-2: old implementation is not live again: {first_labels:?}"
    );
    assert!(
        !first_labels.iter().any(|label| label == "new-effect"),
        "D-2: an effect of the new implementation survived the rollback: {first_labels:?}"
    );
    // Shutdown still drains the registry completely (the panicking cleanup on
    // dispose is contained by `dispose_all`).
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let _ = root.dispose_all();
    }));
    assert!(
        root.registry().keys().is_empty(),
        "D-2: runtimes leaked after dispose_all"
    );
}

// ---------------------------------------------------------------------------
// C: `update_with_no_save` exposes the Cordis `noSave` hint
// ---------------------------------------------------------------------------

#[test]
fn c_update_with_no_save_forwards_the_hint_and_still_commits() {
    let root = Context::root();
    let applies = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(
            Arc::new(CfgPlugin {
                applies: applies.clone(),
                seen: seen.clone(),
            }),
            json!({ "v": 1 }),
        )
        .expect("plugin");

    let hint = Arc::new(Mutex::new(None::<bool>));
    let hint_slot = hint.clone();
    let _hook = root
        .on_internal_update(Arc::new(
            move |_fiber: &Fiber, config: Value, no_save: bool, next: &UpdateNext<'_>| {
                *hint_slot.lock().unwrap() = Some(no_save);
                next.next(config, no_save);
            },
        ))
        .expect("update hook");

    fiber
        .update_with_no_save(json!({ "v": 2 }), true)
        .expect("update_with_no_save");
    assert_eq!(
        *hint.lock().unwrap(),
        Some(true),
        "C: no_save must reach the internal/update hooks"
    );
    assert_eq!(
        fiber.raw_config(),
        json!({ "v": 2 }),
        "C: no_save is a hint, the kernel still commits the new raw config (TS parity)"
    );
    assert_eq!(applies.load(Ordering::SeqCst), 2);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![json!({ "v": 1 }), json!({ "v": 2 })]
    );

    // The default `update` passes `false` for the hint.
    *hint.lock().unwrap() = None;
    fiber.update(json!({ "v": 3 })).expect("plain update");
    assert_eq!(
        *hint.lock().unwrap(),
        Some(false),
        "update() defaults to no_save=false"
    );
    assert_eq!(fiber.raw_config(), json!({ "v": 3 }));
}

#[test]
fn c_update_async_with_no_save_forwards_the_hint() {
    let root = Context::root();
    let applies = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(
            Arc::new(CfgPlugin {
                applies: applies.clone(),
                seen: seen.clone(),
            }),
            json!({ "v": 1 }),
        )
        .expect("plugin");
    let hint = Arc::new(Mutex::new(None::<bool>));
    let hint_slot = hint.clone();
    let _hook = root
        .on_internal_update(Arc::new(
            move |_fiber: &Fiber, config: Value, no_save: bool, next: &UpdateNext<'_>| {
                *hint_slot.lock().unwrap() = Some(no_save);
                next.next(config, no_save);
            },
        ))
        .expect("update hook");
    block_on(fiber.update_async_with_no_save(json!({ "v": 2 }), true))
        .expect("update_async_with_no_save");
    assert_eq!(*hint.lock().unwrap(), Some(true));
    assert_eq!(fiber.raw_config(), json!({ "v": 2 }));
    assert_eq!(applies.load(Ordering::SeqCst), 2);
}

// ---------------------------------------------------------------------------
// s1: a stale async activation must not contaminate the winning activation
// ---------------------------------------------------------------------------

fn wait_until(label: &str, timeout: Duration, condition: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + timeout;
    while !condition() {
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for {label}");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

struct S1Plugin {
    calls: Arc<AtomicUsize>,
    first_latch: Arc<Latch>,
    second_latch: Arc<Latch>,
    first_started: Arc<AtomicBool>,
    second_started: Arc<AtomicBool>,
    first_finished: Arc<AtomicBool>,
}

impl zeokeel_cordis::AsyncPlugin for S1Plugin {
    fn name(&self) -> &'static str {
        "s1-async"
    }

    fn apply_async<'a>(
        &'a self,
        ctx: Context,
        _config: Value,
    ) -> zeokeel_cordis::BoxFuture<'a, Result<()>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let first_latch = self.first_latch.clone();
        let second_latch = self.second_latch.clone();
        let first_started = self.first_started.clone();
        let second_started = self.second_started.clone();
        let first_finished = self.first_finished.clone();
        zeokeel_cordis::boxed(async move {
            if call == 0 {
                // Attempt 1: park, then register its effect and finish late.
                first_started.store(true, Ordering::SeqCst);
                first_latch.wait();
                ctx.effect_labeled("s1-old", || vec![cleanup(|| {})])?;
                first_finished.store(true, Ordering::SeqCst);
            } else {
                // Attempt 2 (the winner): register an early effect, park, then
                // register a late effect.
                ctx.effect_labeled("s1-new-early", || vec![cleanup(|| {})])?;
                second_started.store(true, Ordering::SeqCst);
                second_latch.wait();
                ctx.effect_labeled("s1-new-late", || vec![cleanup(|| {})])?;
            }
            Ok(())
        })
    }
}

/// s1 (FIX-3/FIX-5 interaction): attempt 1 wakes up while attempt 2 is still
/// `Loading`. Its stale-return path discards only its own effects; the
/// `AsyncActivationGuard` used to mistake the *new* attempt for a cancelled one
/// and drain its effects, drop its listeners, set `error` and force `Failed`.
/// A stray error also made `await_settled` fail after a successful activation.
#[test]
fn s1_stale_async_activation_does_not_contaminate_the_winner() {
    watchdog("s1", Duration::from_secs(30), || {
        let root = Context::root();
        let plugin = Arc::new(S1Plugin {
            calls: Arc::new(AtomicUsize::new(0)),
            first_latch: Arc::new(Latch::default()),
            second_latch: Arc::new(Latch::default()),
            first_started: Arc::new(AtomicBool::new(false)),
            second_started: Arc::new(AtomicBool::new(false)),
            first_finished: Arc::new(AtomicBool::new(false)),
        });

        // Attempt 1: `plugin_async` blocks inside `apply_async` on its thread.
        let loader = {
            let plugin = plugin.clone();
            let root = root.clone();
            std::thread::spawn(move || block_on(root.plugin_async(plugin, Value::Null)))
        };
        wait_until("attempt 1 started", Duration::from_secs(5), || {
            plugin.first_started.load(Ordering::SeqCst)
        });
        let fiber = root
            .registry()
            .get(PluginKey::Named("s1-async"))
            .and_then(|runtime| runtime.fibers().into_iter().next())
            .expect("fiber registered");
        assert_eq!(fiber.state(), FiberState::Loading);

        // Attempt 2 wins the epoch race and parks inside `apply_async`.
        let restarter = {
            let fiber = fiber.clone();
            std::thread::spawn(move || block_on(fiber.restart_async()))
        };
        wait_until("attempt 2 started", Duration::from_secs(5), || {
            plugin.second_started.load(Ordering::SeqCst)
        });
        assert_eq!(fiber.state(), FiberState::Loading);

        // Let attempt 1 finish first: it is stale and must only drop its own
        // effect while the winner stays Loading with its early effect intact.
        plugin.first_latch.open();
        wait_until("attempt 1 finished", Duration::from_secs(5), || {
            plugin.first_finished.load(Ordering::SeqCst)
        });
        loader
            .join()
            .expect("loader thread")
            .expect("attempt 1 plugin_async must not fail");
        assert_eq!(
            fiber.state(),
            FiberState::Loading,
            "s1: the stale attempt must not fail the winning activation"
        );
        assert_eq!(
            fiber.error(),
            None,
            "s1: the stale attempt must not write an activation-cancelled error"
        );
        let during_winner = labels(&fiber);
        assert!(
            during_winner.iter().any(|label| label == "s1-new-early"),
            "s1: the winner's early effect was destroyed by the stale rollback: {during_winner:?}"
        );
        assert!(
            !during_winner.iter().any(|label| label == "s1-old"),
            "s1: the stale attempt's own effect must be discarded: {during_winner:?}"
        );

        // Let the winner finish: it must settle Active with no stray error.
        plugin.second_latch.open();
        restarter
            .join()
            .expect("restarter thread")
            .expect("attempt 2 restart_async must succeed");
        assert_eq!(fiber.state(), FiberState::Active);
        assert_eq!(fiber.error(), None, "s1: error must stay clear");
        let final_labels = labels(&fiber);
        assert!(
            final_labels.iter().any(|label| label == "s1-new-early")
                && final_labels.iter().any(|label| label == "s1-new-late"),
            "s1: the winning attempt's effects must survive: {final_labels:?}"
        );
        block_on(fiber.await_settled()).expect("s1: await_settled must succeed");
        drop(root);
    });
}

// ---------------------------------------------------------------------------
// A-2 / A-9: finishing a cancelled async unload
// ---------------------------------------------------------------------------

/// A waker-based async latch: parking on it returns `Pending` (a sync Condvar
/// wait inside an async cleanup would block the polling thread instead).
#[derive(Default)]
struct AsyncLatch {
    state: Mutex<(bool, Option<std::task::Waker>)>,
}

impl AsyncLatch {
    fn open(&self) {
        let mut state = self.state.lock().expect("async latch poisoned");
        state.0 = true;
        if let Some(waker) = state.1.take() {
            waker.wake();
        }
    }

    async fn wait(&self) {
        std::future::poll_fn(|cx| {
            let mut state = self.state.lock().expect("async latch poisoned");
            if state.0 {
                std::task::Poll::Ready(())
            } else {
                state.1 = Some(cx.waker().clone());
                std::task::Poll::Pending
            }
        })
        .await
    }
}

static A2_SVC: ServiceKey<u32> = ServiceKey::new("a2.svc");

struct A2Provider {
    available: Arc<AtomicBool>,
}

impl Plugin for A2Provider {
    fn name(&self) -> &'static str {
        "a2-provider"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let available = self.available.clone();
        ctx.provide_checked(A2_SVC, Arc::new(1u32), move || {
            available.load(Ordering::SeqCst)
        })?;
        Ok(())
    }
}

struct A2Consumer {
    latch: Arc<AsyncLatch>,
}

impl Plugin for A2Consumer {
    fn name(&self) -> &'static str {
        "a2-consumer"
    }

    fn inject(&self) -> &'static [&'static str] {
        &["a2.svc"]
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let latch = self.latch.clone();
        ctx.effect_async_labeled("a2-effect", move || {
            vec![zeokeel_cordis::cleanup_async(move || {
                let latch = latch.clone();
                async move {
                    latch.wait().await;
                }
            })]
        })?;
        Ok(())
    }
}

/// A-2 / A-9: cancelling `refresh_async` mid-teardown used to leave the
/// dependent stuck in `Unloading` (`is_idle` false, `when_idle` blocked, retries
/// rejected) or, with a state-only restore, in a fake `Active` with its services
/// already withdrawn. A cancelled unload now finishes the teardown
/// synchronously: the dependent lands on the target state (`Pending`).
#[test]
fn a2_cancelled_refresh_async_finishes_the_teardown() {
    watchdog("a2", Duration::from_secs(30), || {
        let root = Context::root();
        let available = Arc::new(AtomicBool::new(true));
        let cleanup_latch = Arc::new(AsyncLatch::default());
        let _provider = root
            .plugin(
                Arc::new(A2Provider {
                    available: available.clone(),
                }),
                Value::Null,
            )
            .expect("provider");
        let consumer = root
            .plugin(
                Arc::new(A2Consumer {
                    latch: cleanup_latch.clone(),
                }),
                Value::Null,
            )
            .expect("consumer");
        assert_eq!(consumer.state(), FiberState::Active);

        // Make the dependency unavailable, then poll a refresh until it parks on
        // the consumer's async cleanup and cancel it.
        available.store(false, Ordering::SeqCst);
        let ctx = root.clone();
        let mut refresh = Box::pin(async move { ctx.refresh_async().await });
        let waker = std::task::Waker::noop();
        let mut task_cx = std::task::Context::from_waker(waker);
        assert!(
            matches!(
                std::future::Future::poll(refresh.as_mut(), &mut task_cx),
                std::task::Poll::Pending
            ),
            "the refresh must park on the consumer's async cleanup"
        );
        assert_eq!(
            consumer.state(),
            FiberState::Unloading,
            "the consumer is mid-teardown while the refresh is parked"
        );
        drop(refresh); // cancel the refresh future

        assert_eq!(
            consumer.state(),
            FiberState::Pending,
            "A-9: a cancelled unload must land on the target state (not Unloading/Active)"
        );
        assert!(
            root.registry().is_idle(),
            "A-2: the registry must not stay busy after the cancellation"
        );
        block_on(root.when_idle());

        // Retry is a no-op that converges (the dependency stays unavailable).
        cleanup_latch.open();
        block_on(root.clone().refresh_async());
        assert_eq!(consumer.state(), FiberState::Pending);
        let _ = root.dispose_all();
        assert!(
            root.registry().keys().is_empty(),
            "no runtime may survive shutdown"
        );
    });
}

struct A9Plugin {
    sync_ran: Arc<AtomicUsize>,
    older_async_ran: Arc<AtomicUsize>,
    newest_async_latch: Arc<AsyncLatch>,
}

impl Plugin for A9Plugin {
    fn name(&self) -> &'static str {
        "a9-plugin"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        // Registration order: older async effect, newer async effect, sync
        // effect. The drain runs reverse (sync first), so the newest async
        // cleanup is the one in flight when the dispose_async future is
        // cancelled; the older async cleanup is still unstarted and must be
        // deferred to the next dispose (A-9).
        let older = self.older_async_ran.clone();
        ctx.effect_async_labeled("a9-older-async", move || {
            vec![zeokeel_cordis::cleanup_async(move || {
                let older = older.clone();
                async move {
                    older.fetch_add(1, Ordering::SeqCst);
                }
            })]
        })?;
        let latch = self.newest_async_latch.clone();
        ctx.effect_async_labeled("a9-newest-async", move || {
            vec![zeokeel_cordis::cleanup_async(move || {
                let latch = latch.clone();
                async move {
                    latch.wait().await;
                }
            })]
        })?;
        let sync_ran = self.sync_ran.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                sync_ran.fetch_add(1, Ordering::SeqCst);
            })]
        })?;
        Ok(())
    }
}

/// A-9: a cancelled `dispose_async` completes the teardown instead of leaving a
/// fake `Active`/`Unloading` fiber: the fiber is `Disposed`, sync cleanups ran
/// inline, and async cleanups that had not started are deferred to a retry
/// `dispose_async` (which the already-Disposed fiber now drains).
#[test]
fn a9_cancelled_dispose_async_completes_the_teardown() {
    watchdog("a9", Duration::from_secs(30), || {
        let root = Context::root();
        let sync_ran = Arc::new(AtomicUsize::new(0));
        let older_async_ran = Arc::new(AtomicUsize::new(0));
        let latch = Arc::new(AsyncLatch::default());
        let fiber = root
            .plugin(
                Arc::new(A9Plugin {
                    sync_ran: sync_ran.clone(),
                    older_async_ran: older_async_ran.clone(),
                    newest_async_latch: latch.clone(),
                }),
                Value::Null,
            )
            .expect("fiber");

        let mut dispose = Box::pin(fiber.dispose_async());
        let waker = std::task::Waker::noop();
        let mut task_cx = std::task::Context::from_waker(waker);
        assert!(
            matches!(
                std::future::Future::poll(dispose.as_mut(), &mut task_cx),
                std::task::Poll::Pending
            ),
            "the dispose must park on the newest async cleanup"
        );
        drop(dispose); // cancel mid-teardown

        assert_eq!(
            fiber.state(),
            FiberState::Disposed,
            "A-9: a cancelled dispose_async must still reach Disposed"
        );
        assert_eq!(
            sync_ran.load(Ordering::SeqCst),
            1,
            "A-9: remaining sync cleanups must run inline"
        );
        assert_eq!(
            older_async_ran.load(Ordering::SeqCst),
            0,
            "the unstarted async cleanup is deferred, not dropped-or-run"
        );
        assert!(root.registry().is_idle());

        // A retry dispose drains the deferred async cleanups.
        latch.open();
        block_on(fiber.dispose_async()).expect("retry dispose drains deferred cleanups");
        assert_eq!(
            older_async_ran.load(Ordering::SeqCst),
            1,
            "A-9: the deferred async cleanup must run on the retry"
        );
        assert!(root.registry().keys().is_empty());
    });
}

// ---------------------------------------------------------------------------
// A-8: overlapping replace rollbacks must not leave an empty runtime behind
// ---------------------------------------------------------------------------

struct A8Plugin {
    name: &'static str,
}

impl Plugin for A8Plugin {
    fn name(&self) -> &'static str {
        self.name
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.effect_labeled("a8-effect", || vec![cleanup(|| {})])?;
        Ok(())
    }
}

/// A-8: two overlapping `Registry::replace` calls that both roll back used to
/// leave an *empty* runtime entry (the shared-target rollback branch had no
/// empty prune). `dispose_all` only walks fibers, so such an entry survived
/// shutdown forever and `keys()/size()` stayed non-empty.
#[test]
fn a8_overlapping_replace_rollbacks_leave_no_empty_runtime() {
    watchdog("a8", Duration::from_secs(60), || {
        let root = Context::root();
        let a1 = root
            .plugin(Arc::new(A8Plugin { name: "a8-a" }), Value::Null)
            .expect("a1");
        let a2 = root
            .plugin(Arc::new(A8Plugin { name: "a8-a" }), Value::Null)
            .expect("a2");
        let c1 = root
            .plugin(Arc::new(A8Plugin { name: "a8-c" }), Value::Null)
            .expect("c1");
        let c2 = root
            .plugin(Arc::new(A8Plugin { name: "a8-c" }), Value::Null)
            .expect("c2");
        assert_eq!(root.registry().size(), 2, "two runtimes, four fibers");

        // One-shot gates per fiber: the first `internal/config` call parks.
        let gate = |id: u64| (id, Arc::new(Latch::default()), Arc::new(Latch::default()));
        let gates = Arc::new(vec![
            gate(a1.id()),
            gate(a2.id()),
            gate(c1.id()),
            gate(c2.id()),
        ]);
        let hook_gates = gates.clone();
        let _hook = root
            .on_internal_config(Arc::new(
                move |fiber: &Fiber, config: Value, next: &ConfigNext<'_>| {
                    for (id, entered, release) in hook_gates.iter() {
                        if fiber.id() == *id {
                            if !entered.is_open() {
                                entered.open();
                                release.wait();
                            }
                            break;
                        }
                    }
                    next.next(config)
                },
            ))
            .expect("config hook");
        let gate_of = |id: u64| {
            gates
                .iter()
                .find(|(candidate, _, _)| *candidate == id)
                .map(|(_, entered, release)| (entered.clone(), release.clone()))
                .expect("gate")
        };
        let (a2_entered, a2_release) = gate_of(a2.id());
        let (c2_entered, c2_release) = gate_of(c2.id());
        let (a1_entered, a1_release) = gate_of(a1.id());
        let (c1_entered, c1_release) = gate_of(c1.id());

        // Park a2 and c2 inside `update` so they hold their lifecycle slots.
        let park_a2 = {
            let a2 = a2.clone();
            std::thread::spawn(move || a2.update(json!({ "park": "a2" })))
        };
        let park_c2 = {
            let c2 = c2.clone();
            std::thread::spawn(move || c2.update(json!({ "park": "c2" })))
        };
        wait_until("a2 parked", Duration::from_secs(5), || a2_entered.is_open());
        wait_until("c2 parked", Duration::from_secs(5), || c2_entered.is_open());

        // T1: replace(a -> b); its first reload parks in a1's config gate.
        let registry = root.registry();
        let t1 = {
            let registry = registry.clone();
            std::thread::spawn(move || {
                registry.replace(
                    PluginKey::Named("a8-a"),
                    Arc::new(A8Plugin { name: "a8-b" }),
                )
            })
        };
        wait_until("T1 reload parked", Duration::from_secs(5), || {
            a1_entered.is_open()
        });
        assert!(registry.has(PluginKey::Named("a8-b")));

        // T2: replace(c -> b) merges into the existing b runtime
        // (target_created == false) and parks in c1's gate.
        let t2 = {
            let registry = registry.clone();
            std::thread::spawn(move || {
                registry.replace(
                    PluginKey::Named("a8-c"),
                    Arc::new(A8Plugin { name: "a8-b" }),
                )
            })
        };
        wait_until("T2 reload parked", Duration::from_secs(5), || {
            c1_entered.is_open()
        });

        // T1 rolls back first (a2 is busy), then T2 rolls back (c2 is busy).
        a1_release.open();
        assert!(
            t1.join().expect("t1 thread").is_err(),
            "T1 must fail on the busy a2"
        );
        c1_release.open();
        assert!(
            t2.join().expect("t2 thread").is_err(),
            "T2 must fail on the busy c2"
        );

        a2_release.open();
        c2_release.open();
        park_a2.join().expect("park a2").expect("a2 update");
        park_c2.join().expect("park c2").expect("c2 update");

        for fiber in [&a1, &a2, &c1, &c2] {
            let _ = catch_unwind(AssertUnwindSafe(|| {
                let _ = fiber.dispose();
            }));
        }
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _ = root.dispose_all();
        }));
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _ = root.dispose_all();
        }));
        let keys = root.registry().keys();
        assert!(
            keys.is_empty(),
            "A-8: empty runtime residue after dispose_all: keys={keys:?} size={}",
            root.registry().size()
        );
        assert_eq!(root.registry().fiber_count(), 0);
        assert!(root.registry().is_idle());
    });
}

// ---------------------------------------------------------------------------
// A-10: a cancelled async activation reclaims its children and listeners
// ---------------------------------------------------------------------------

struct A10Child {
    cleanups: Arc<AtomicUsize>,
}

impl Plugin for A10Child {
    fn name(&self) -> &'static str {
        "a10-child"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let cleanups = self.cleanups.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                cleanups.fetch_add(1, Ordering::SeqCst);
            })]
        })?;
        Ok(())
    }
}

struct A10Parent {
    latch: Arc<AsyncLatch>,
    started: Arc<AtomicBool>,
    cleanups: Arc<AtomicUsize>,
}

impl zeokeel_cordis::AsyncPlugin for A10Parent {
    fn name(&self) -> &'static str {
        "a10-parent"
    }

    fn apply_async<'a>(
        &'a self,
        ctx: Context,
        _config: Value,
    ) -> zeokeel_cordis::BoxFuture<'a, Result<()>> {
        let latch = self.latch.clone();
        let started = self.started.clone();
        let cleanups = self.cleanups.clone();
        zeokeel_cordis::boxed(async move {
            let _child = ctx.plugin(Arc::new(A10Child { cleanups }), Value::Null)?;
            ctx.on_async("a10-event", move |_args| async {
                zeokeel_cordis::EventResult::Continue
            })?;
            started.store(true, Ordering::SeqCst);
            latch.wait().await;
            Ok(())
        })
    }
}

/// A-10: cancelling an async activation must reclaim what the attempt created.
/// The epoch-precise rollback defers async effects, so without the explicit
/// owner/child cleanup the child would stay Active under a Failed parent and be
/// invisible to `is_idle`.
#[test]
fn a10_cancelled_async_activation_reclaims_its_children_and_listeners() {
    watchdog("a10", Duration::from_secs(30), || {
        let root = Context::root();
        let latch = Arc::new(AsyncLatch::default());
        let started = Arc::new(AtomicBool::new(false));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let plugin = Arc::new(A10Parent {
            latch: latch.clone(),
            started: started.clone(),
            cleanups: cleanups.clone(),
        });
        let mut creation = Box::pin(root.plugin_async(plugin, Value::Null));
        let waker = std::task::Waker::noop();
        let mut task_cx = std::task::Context::from_waker(waker);
        assert!(
            matches!(
                std::future::Future::poll(creation.as_mut(), &mut task_cx),
                std::task::Poll::Pending
            ),
            "the activation parks inside apply_async"
        );
        assert!(started.load(Ordering::SeqCst));
        drop(creation); // cancel the activation

        let fibers = root.registry().fibers();
        let parent = fibers
            .iter()
            .find(|fiber| fiber.name() == "a10-parent")
            .expect("parent fiber");
        assert_eq!(parent.state(), FiberState::Failed);
        let children: Vec<(String, FiberState)> = fibers
            .iter()
            .filter(|fiber| fiber.name() != "a10-parent")
            .map(|fiber| (fiber.name(), fiber.state()))
            .collect();
        assert!(
            children
                .iter()
                .all(|(_, state)| *state == FiberState::Disposed),
            "A-10: cancelled activation left children {children:?}"
        );
        assert_eq!(
            cleanups.load(Ordering::SeqCst),
            1,
            "A-10: the cancelled attempt's child must be disposed exactly once"
        );
        assert_eq!(root.registry().fiber_count(), 1, "only the Failed parent");
        assert!(root.registry().is_idle());
        let _ = root.dispose_all();
        assert!(root.registry().keys().is_empty());
    });
}

// ---------------------------------------------------------------------------
// B-3: a panicking `Plugin::inject()` must not poison the fiber lock
// ---------------------------------------------------------------------------

struct B3Base;

impl Plugin for B3Base {
    fn name(&self) -> &'static str {
        "b3-base"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.effect_labeled("b3-effect", || vec![cleanup(|| {})])?;
        Ok(())
    }
}

struct B3PanickyInject;

impl Plugin for B3PanickyInject {
    fn name(&self) -> &'static str {
        "b3-panicky-inject"
    }

    fn inject(&self) -> &'static [&'static str] {
        panic!("B-3 inject boom");
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        Ok(())
    }
}

/// B-3: `Plugin::inject()` is embedder code. Evaluating it while holding the
/// FiberData lock poisoned the fiber on panic (`state()` reported Disposed,
/// effects unreachable, provided services never withdrawn). It is now computed
/// before the lock, so the panic still propagates but the fiber stays healthy.
#[test]
fn b3_panicking_inject_does_not_poison_the_fiber_lock() {
    let root = Context::root();
    let fiber = root
        .plugin(Arc::new(B3Base), Value::Null)
        .expect("base plugin");
    assert_eq!(fiber.state(), FiberState::Active);

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        fiber.replace_plugin(Arc::new(B3PanickyInject))
    }));
    assert!(
        outcome.is_err(),
        "the embedder panic itself still propagates"
    );
    assert_eq!(
        fiber.state(),
        FiberState::Active,
        "B-3: the fiber lock must not be poisoned by a panicking inject()"
    );
    assert!(
        !fiber.get_effects().is_empty(),
        "B-3: the fiber's effects must stay reachable"
    );
    assert!(fiber.error().is_none());

    fiber.dispose().expect("dispose after the panic");
    assert_eq!(fiber.state(), FiberState::Disposed);
    assert!(root.registry().keys().is_empty());
}
