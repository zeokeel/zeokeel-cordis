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
fn b3_panicking_inject_is_contained_and_does_not_poison_the_fiber_lock() {
    let root = Context::root();
    let fiber = root
        .plugin(Arc::new(B3Base), Value::Null)
        .expect("base plugin");
    assert_eq!(fiber.state(), FiberState::Active);

    // B-3 / C2: the panicking `inject()` is computed before the lock and is
    // contained: the replacement activates with no dependency declarations.
    fiber
        .replace_plugin(Arc::new(B3PanickyInject))
        .expect("C2: a panicking inject() must be contained by the replace");
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(fiber.name(), "b3-panicky-inject");
    assert!(fiber.error().is_none());
    // Lock-health probe: the effect tree stays reachable (the replacement plugin
    // registers no effects, so an empty list is correct).
    assert_eq!(fiber.get_effects().len(), 0);

    fiber.dispose().expect("dispose after the contained panic");
    assert_eq!(fiber.state(), FiberState::Disposed);
    assert!(root.registry().keys().is_empty());
}

// ---------------------------------------------------------------------------
// C4: `when_idle()` inside a registration frame must not wait for itself
// ---------------------------------------------------------------------------

struct C4Noop;

impl Plugin for C4Noop {
    fn name(&self) -> &'static str {
        "c4-noop"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        Ok(())
    }
}

/// C4: a host that calls `when_idle()` from inside its own `internal/plugin`
/// create hook used to deadlock (>3s) because the registration window it is
/// standing in keeps the registry busy. `when_idle` now excludes *this thread's
/// own* registrations; `is_idle()` stays strict and external waiters still wait.
#[test]
fn c4_when_idle_inside_registration_frame_does_not_wait_for_itself() {
    watchdog("c4", Duration::from_secs(30), || {
        let root = Context::root();
        let registry = root.registry();
        let hook_entered = Arc::new(Latch::default());
        let hook_release = Arc::new(Latch::default());
        let elapsed: Arc<Mutex<Option<std::time::Duration>>> = Arc::new(Mutex::new(None));
        let strict_idle = Arc::new(AtomicBool::new(true));
        let _hook = {
            let registry = registry.clone();
            let entered = hook_entered.clone();
            let release = hook_release.clone();
            let elapsed = elapsed.clone();
            let strict_idle = strict_idle.clone();
            root.on_internal_plugin(Arc::new(move |_fiber: &Fiber| {
                // C4: the registration window is *not* idle for a strict check…
                strict_idle.store(registry.is_idle(), Ordering::SeqCst);
                // …but waiting from inside it must not deadlock.
                let started = std::time::Instant::now();
                block_on(async {
                    let registry = registry.clone();
                    registry.when_idle().await;
                });
                *elapsed.lock().unwrap() = Some(started.elapsed());
                entered.open();
                release.wait();
            }))
            .expect("plugin hook")
        };

        let loader = {
            let root = root.clone();
            std::thread::spawn(move || root.plugin(Arc::new(C4Noop), Value::Null))
        };
        hook_entered.wait();
        // An *external* waiter (outside the registration window) must still
        // wait: FIX-11b guarantee is unchanged.
        let ctx = root.clone();
        let mut external = Box::pin(async move { ctx.when_idle().await });
        let waker = std::task::Waker::noop();
        let mut task_cx = std::task::Context::from_waker(waker);
        assert!(
            matches!(
                std::future::Future::poll(external.as_mut(), &mut task_cx),
                std::task::Poll::Pending
            ),
            "FIX-11b: an external when_idle must still wait for the registration"
        );
        drop(external);
        hook_release.open();
        loader.join().expect("loader thread").expect("plugin");
        let elapsed = elapsed.lock().unwrap().expect("hook ran");
        assert!(
            elapsed < Duration::from_secs(1),
            "C4: when_idle inside the create hook must return immediately, took {elapsed:?}"
        );
        assert!(
            !strict_idle.load(Ordering::SeqCst),
            "C4: is_idle() must stay strict (false) during the registration window"
        );
        block_on(root.when_idle());
    });
}

// ---------------------------------------------------------------------------
// C5: `AccessorWriteRejected` is a distinct public error
// ---------------------------------------------------------------------------

#[test]
fn c5_accessor_write_rejected_is_distinct_from_service_write_forbidden() {
    let rejected = CordisError::AccessorWriteRejected("c5.accessor".to_string());
    let message = rejected.to_string();
    assert!(message.contains("c5.accessor"), "{message}");
    assert!(
        !message.contains("another fiber"),
        "the accessor-rejection message must not read like ServiceWriteForbidden: {message}"
    );
    assert_ne!(
        rejected.to_string(),
        CordisError::ServiceWriteForbidden("c5.accessor".to_string()).to_string()
    );
}

// ---------------------------------------------------------------------------
// C1: iterative (non-recursive) unload cascade
// ---------------------------------------------------------------------------

/// One link of a deep dependency chain: injects `dep`, provides `svc`, records
/// its cleanup label in `trace` and provides a synthetic service value.
struct ChainLink {
    svc: &'static str,
    dep: Option<&'static str>,
    label: &'static str,
    trace: Arc<Mutex<Vec<&'static str>>>,
}

impl Plugin for ChainLink {
    fn name(&self) -> &'static str {
        self.label
    }

    fn inject(&self) -> &'static [&'static str] {
        static EMPTY: &[&str] = &[];
        match self.dep {
            Some(dep) => {
                // `inject` must return a `'static` slice; the chain test leaks
                // exactly one one-element slice per link.
                Box::leak(vec![dep].into_boxed_slice())
            }
            None => EMPTY,
        }
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let svc = ServiceKey::new(self.svc);
        ctx.provide_checked(svc, Arc::new(1u32), || true)?;
        let trace = self.trace.clone();
        let label = self.label;
        ctx.effect(move || {
            vec![cleanup(move || {
                trace.lock().unwrap().push(label);
            })]
        })?;
        Ok(())
    }
}

/// Build a chain `P0 provides s0 ← D1 provides s1 ← … ← Dn provides sn` and run
/// `scope` with the fibers in order `[P0, D1, …, Dn]`.
fn with_chain<F>(n: usize, trace: Arc<Mutex<Vec<&'static str>>>, scope: F)
where
    F: FnOnce(&Context, Vec<Fiber>),
{
    let root = Context::root();
    let mut fibers = Vec::with_capacity(n + 1);
    let mut previous: Option<&'static str> = None;
    for index in 0..=n {
        let svc: &'static str = Box::leak(format!("c1-chain.svc.{index}").into_boxed_str());
        let label: &'static str = Box::leak(format!("L{index}").into_boxed_str());
        let plugin = Arc::new(ChainLink {
            svc,
            dep: previous,
            label,
            trace: trace.clone(),
        });
        let fiber = root.plugin(plugin, Value::Null).expect("chain link");
        assert_eq!(
            fiber.state(),
            FiberState::Active,
            "chain link {index} must activate"
        );
        fibers.push(fiber);
        previous = Some(svc);
    }
    scope(&root, fibers);
}

/// C1: a 1500-deep dependency chain must dispose **iteratively** (the old
/// recursive retire path overflowed the stack around a few hundred links on a
/// 2 MiB worker thread and aborted the process). Dependents are drained before
/// the provider, deepest first.
#[test]
fn c1_deep_chain_dispose_is_iterative_and_ordered() {
    watchdog("c1_deep", Duration::from_secs(180), || {
        const N: usize = 1500;
        let trace: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
        with_chain(N, trace.clone(), |root, fibers| {
            let base = fibers[0].clone();
            let started = std::time::Instant::now();
            base.dispose().expect("dispose the chain base");
            let elapsed = started.elapsed();
            let recorded = trace.lock().unwrap().clone();
            let mut expected: Vec<&'static str> = (1..=N)
                .rev()
                .map(|i| Box::leak(format!("L{i}").into_boxed_str()) as &'static str)
                .collect();
            expected.push("L0");
            assert_eq!(
                recorded, expected,
                "C1: dependents must drain before the provider, deepest first"
            );
            assert_eq!(base.state(), FiberState::Disposed);
            for (index, fiber) in fibers.iter().enumerate().skip(1) {
                assert_eq!(
                    fiber.state(),
                    FiberState::Pending,
                    "dependent {index} must end Pending after the cascade"
                );
            }
            eprintln!("c1_deep N={N} dispose elapsed={elapsed:?}");
            let _ = root.dispose_all();
            assert_eq!(root.registry().size(), 0);
            assert_eq!(root.registry().fiber_count(), 0);
            assert!(root.registry().is_idle());
        });
    });
}

/// C1: a panicking dependent cleanup is contained (FIX-2): the provider still
/// reaches `Disposed`, the dependent reaches its target state, and the panic is
/// not re-raised by the provider's `dispose`.
#[test]
fn c1_dependent_cleanup_panic_is_contained() {
    struct Base;

    impl Plugin for Base {
        fn name(&self) -> &'static str {
            "c1-panic-base"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            ctx.provide_checked(ServiceKey::new("c1-panic.svc"), Arc::new(1u32), || true)?;
            Ok(())
        }
    }

    struct Dependent;

    impl Plugin for Dependent {
        fn name(&self) -> &'static str {
            "c1-panic-dependent"
        }

        fn inject(&self) -> &'static [&'static str] {
            &["c1-panic.svc"]
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            ctx.effect(|| vec![cleanup(|| panic!("c1 dependent cleanup boom"))])?;
            Ok(())
        }
    }

    let root = Context::root();
    let base = root.plugin(Arc::new(Base), Value::Null).expect("base");
    let dependent = root
        .plugin(Arc::new(Dependent), Value::Null)
        .expect("dependent");
    assert_eq!(dependent.state(), FiberState::Active);
    base.dispose()
        .expect("FIX-2: the dependent's cleanup panic must be contained");
    assert_eq!(base.state(), FiberState::Disposed);
    assert_eq!(dependent.state(), FiberState::Pending);
    assert!(root.registry().is_idle());
    let _ = root.dispose_all();
    assert!(root.registry().keys().is_empty());
}

/// C1 + A-9: cancelling a deep-cascade `dispose_async` (the base's async cleanup
/// parks) must still finish the base teardown; the dependents were already
/// drained by the cascade, and a retry drains the deferred cleanups.
#[test]
fn c1_cancelled_deep_cascade_still_completes() {
    struct AsyncBase {
        svc: &'static str,
        latch: Arc<AsyncLatch>,
    }

    impl Plugin for AsyncBase {
        fn name(&self) -> &'static str {
            "c1-async-base"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            ctx.provide_checked(ServiceKey::new(self.svc), Arc::new(1u32), || true)?;
            let latch = self.latch.clone();
            ctx.effect_async_labeled("c1-async-cleanup", move || {
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

    struct Follower {
        svc: &'static str,
        deps: &'static [&'static str],
    }

    impl Plugin for Follower {
        fn name(&self) -> &'static str {
            "c1-async-follower"
        }

        fn inject(&self) -> &'static [&'static str] {
            self.deps
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            ctx.provide_checked(ServiceKey::new(self.svc), Arc::new(1u32), || true)?;
            Ok(())
        }
    }

    let root = Context::root();
    let latch = Arc::new(AsyncLatch::default());
    let base = root
        .plugin(
            Arc::new(AsyncBase {
                svc: "c1-async.svc.0",
                latch: latch.clone(),
            }),
            Value::Null,
        )
        .expect("async base");
    let follower = root
        .plugin(
            Arc::new(Follower {
                svc: "c1-async.svc.1",
                deps: Box::leak(vec!["c1-async.svc.0"].into_boxed_slice()),
            }),
            Value::Null,
        )
        .expect("follower");
    assert_eq!(follower.state(), FiberState::Active);

    let mut dispose = Box::pin(base.dispose_async());
    let waker = std::task::Waker::noop();
    let mut task_cx = std::task::Context::from_waker(waker);
    assert!(
        matches!(
            std::future::Future::poll(dispose.as_mut(), &mut task_cx),
            std::task::Poll::Pending
        ),
        "the cascade parks on the base's async cleanup"
    );
    // The cascade began and drained the dependent before the base parked.
    assert_eq!(follower.state(), FiberState::Pending);
    assert_eq!(base.state(), FiberState::Unloading);
    drop(dispose); // cancel
    assert_eq!(
        base.state(),
        FiberState::Disposed,
        "A-9: a cancelled cascade still lands the base on Disposed"
    );
    assert!(root.registry().is_idle());
    // Retry drains the deferred async cleanup.
    latch.open();
    block_on(base.dispose_async()).expect("retry dispose");
    let _ = root.dispose_all();
    assert!(root.registry().keys().is_empty());
}

// ---------------------------------------------------------------------------
// R3-A4: dynamic child loading must not grow the parent's bookkeeping
// ---------------------------------------------------------------------------

struct R3a4Child {
    svc: &'static str,
}

impl Plugin for R3a4Child {
    fn name(&self) -> &'static str {
        "r3a4-child"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.effect_labeled("r3a4-child-effect", || vec![cleanup(|| {})])?;
        let _ = self.svc;
        Ok(())
    }
}

fn r3a4_churn(root: &Context, iterations: usize) -> usize {
    let mut max_effects = 0usize;
    for _ in 0..iterations {
        let child = root
            .plugin(Arc::new(R3a4Child { svc: "r3a4.svc" }), Value::Null)
            .expect("dynamic child");
        assert_eq!(child.state(), FiberState::Active);
        child.dispose().expect("child dispose");
        max_effects = max_effects.max(root.fiber().get_effects().len());
    }
    max_effects
}

/// R3-A4: a long-lived root repeatedly loading and disposing plugins used to
/// accumulate one ownership effect (+ one `children` entry) per load forever
/// (60k churn: 60001 reported effects, +54MB RSS). After the fix the parent's
/// effect list stays O(1) (only live children).
#[test]
fn r3a4_dynamic_plugin_churn_keeps_parent_bookkeeping_bounded() {
    watchdog("r3a4-churn", Duration::from_secs(180), || {
        let root = Context::root();
        let baseline = root.fiber().get_effects().len();
        let max_effects = r3a4_churn(&root, 1_000);
        assert_eq!(
            max_effects, baseline,
            "R3-A4: parent effect list must not grow with disposed children (max={max_effects}, baseline={baseline})"
        );
        assert_eq!(
            root.registry().fiber_count(),
            0,
            "all dynamically loaded fibers are disposed"
        );
        assert_eq!(root.fiber().state(), FiberState::Active);
        assert!(root.registry().is_idle());
        let _ = root.dispose_all();
        assert!(root.registry().keys().is_empty());
    });
}

/// R3-A4 (long run, 10k loads): the bookkeeping must stay bounded and the
/// process must not grow linearly with churn. RSS is sampled around the churn
/// loop; a generous bound keeps the check robust under a parallel test runner
/// while still catching the old unbounded growth (933B/load ≈ 9.3MB per 10k;
/// 10x that bound still fails the pre-fix build by a wide margin at 60k+).
#[test]
fn r3a4_long_churn_keeps_memory_and_effects_bounded() {
    watchdog("r3a4-long", Duration::from_secs(300), || {
        fn rss_kb() -> u64 {
            let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
            statm
                .split_whitespace()
                .nth(1)
                .and_then(|pages| pages.parse::<u64>().ok())
                .map(|pages| pages * 4)
                .unwrap_or(0)
        }
        let root = Context::root();
        let _warmup = r3a4_churn(&root, 1_000);
        let rss_before = rss_kb();
        let max_effects = r3a4_churn(&root, 10_000);
        let rss_after = rss_kb();
        assert!(
            max_effects <= 2,
            "R3-A4: 10k churn grew the parent effect list to {max_effects}"
        );
        let growth = rss_after.saturating_sub(rss_before);
        eprintln!(
            "R3-A4 evidence: 10k dynamic loads -> parent effects max={max_effects}, RSS growth={growth}KB (pre-fix audit: 60001 effects, +54.7MB at 60k)"
        );
        assert!(
            growth < 64 * 1024,
            "R3-A4: 10k dynamic loads grew RSS by {growth}KB (unbounded bookkeeping?)"
        );
        let _ = root.dispose_all();
        assert!(root.registry().keys().is_empty());
    });
}

/// R3-A4: parent-first disposal is still handled by the ownership effect, and a
/// later child dispose (already disposed) is an idempotent no-op — the detach
/// path must be best-effort while the parent is Unloading/Disposed.
#[test]
fn r3a4_parent_dispose_then_child_dispose_is_idempotent() {
    struct Parent {
        child: Arc<Mutex<Option<Fiber>>>,
    }

    impl Plugin for Parent {
        fn name(&self) -> &'static str {
            "r3a4-parent"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            let child = ctx.plugin(
                Arc::new(R3a4Child {
                    svc: "r3a4.parent-svc",
                }),
                Value::Null,
            )?;
            *self.child.lock().unwrap() = Some(child);
            Ok(())
        }
    }

    let root = Context::root();
    let root_baseline = root.fiber().get_effects().len();
    let slot = Arc::new(Mutex::new(None));
    let parent = root
        .plugin(
            Arc::new(Parent {
                child: slot.clone(),
            }),
            Value::Null,
        )
        .expect("parent");
    let child = slot.lock().unwrap().clone().expect("child");
    assert_eq!(child.state(), FiberState::Active);

    parent.dispose().expect("parent dispose");
    assert_eq!(parent.state(), FiberState::Disposed);
    assert_eq!(child.state(), FiberState::Disposed);
    // The child was disposed through the parent's ownership cascade; repeating
    // it (and detaching again) must be a silent no-op.
    child.dispose().expect("idempotent child dispose");
    assert_eq!(child.state(), FiberState::Disposed);
    assert_eq!(
        root.fiber().get_effects().len(),
        root_baseline,
        "R3-A4: no ownership entry may survive on the root"
    );
    let _ = root.dispose_all();
    assert!(root.registry().keys().is_empty());
}

/// R3-A4: a parent sweep racing the child's own dispose must stay panic-free and
/// converge (both fibers disposed, registry empty, parent bookkeeping clean).
#[test]
fn r3a4_parent_sweep_races_child_self_dispose() {
    watchdog("r3a4-race", Duration::from_secs(60), || {
        for _ in 0..40 {
            let root = Context::root();
            let child = root
                .plugin(
                    Arc::new(R3a4Child {
                        svc: "r3a4.race-svc",
                    }),
                    Value::Null,
                )
                .expect("child");
            let parent = root.fiber();
            let child_for_thread = child.clone();
            let disposer = std::thread::spawn(move || {
                let _ = catch_unwind(AssertUnwindSafe(|| child_for_thread.dispose()));
            });
            let _ = catch_unwind(AssertUnwindSafe(|| parent.dispose()));
            disposer.join().expect("child disposer thread");
            assert_eq!(parent.state(), FiberState::Disposed);
            assert_eq!(child.state(), FiberState::Disposed);
            assert!(
                parent.get_effects().is_empty(),
                "R3-A4: parent bookkeeping leaked under the dispose race"
            );
            let _ = root.dispose_all();
            assert!(root.registry().keys().is_empty());
        }
    });
}

/// R3-A4: a cancelled `dispose_async` of the child (A-9 completes the teardown)
/// must still detach the child from the parent.
#[test]
fn r3a4_cancelled_child_dispose_still_detaches() {
    struct Parent {
        child: Arc<Mutex<Option<Fiber>>>,
    }

    impl Plugin for Parent {
        fn name(&self) -> &'static str {
            "r3a4-cancel-parent"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            let latch = Arc::new(AsyncLatch::default());
            // Keep the latch alive inside the child cleanup.
            let child = ctx.plugin(
                Arc::new(R3a4Child {
                    svc: "r3a4.cancel-svc",
                }),
                Value::Null,
            )?;
            let _ = latch;
            *self.child.lock().unwrap() = Some(child);
            Ok(())
        }
    }

    let root = Context::root();
    let slot = Arc::new(Mutex::new(None));
    let parent = root
        .plugin(
            Arc::new(Parent {
                child: slot.clone(),
            }),
            Value::Null,
        )
        .expect("parent");
    let child = slot.lock().unwrap().clone().expect("child");
    // Cancel a dispose_async before it runs: it is just never polled, which is
    // the "future dropped" case; the retry must dispose and detach.
    let mut dispose = Box::pin(child.dispose_async());
    let waker = std::task::Waker::noop();
    let mut task_cx = std::task::Context::from_waker(waker);
    let _ = std::future::Future::poll(dispose.as_mut(), &mut task_cx);
    drop(dispose);
    assert_eq!(child.state(), FiberState::Disposed);
    assert!(
        parent.get_effects().is_empty(),
        "R3-A4: cancelled child dispose must still detach the ownership entry from the parent"
    );
    let _ = root.dispose_all();
    assert!(root.registry().keys().is_empty());
}

// ---------------------------------------------------------------------------
// R1: deep plugin nesting must not overflow a 2 MiB worker-thread stack
// ---------------------------------------------------------------------------

/// R1: activating a deep `ctx.plugin` nesting chain used to overflow a 2 MiB
/// thread stack (the default for `std::thread`) and abort the process — a plain
/// `catch_unwind` cannot save the host. The activation path is now small enough
/// (and the unload cascade of the resulting ownership tree is iterative) that
/// the documented `MAX_PLUGIN_DEPTH` guard is reached safely on a 2 MiB stack.
#[test]
fn r1_deep_plugin_chain_activates_on_a_2mib_thread() {
    struct Deep {
        depth: usize,
        activated: Arc<AtomicUsize>,
    }

    impl Plugin for Deep {
        fn name(&self) -> &'static str {
            "r1-deep"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            // Count how deep the activation actually went.
            self.activated.fetch_add(1, Ordering::SeqCst);
            if self.depth > 0 {
                let _ = ctx.plugin(
                    Arc::new(Deep {
                        depth: self.depth - 1,
                        activated: self.activated.clone(),
                    }),
                    Value::Null,
                );
            }
            Ok(())
        }
    }

    let activated = Arc::new(AtomicUsize::new(0));
    let activated_in_thread = activated.clone();
    let handle = std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(move || {
            let root = Context::root();
            let fiber = root
                .plugin(
                    Arc::new(Deep {
                        depth: 400,
                        activated: activated_in_thread,
                    }),
                    Value::Null,
                )
                .expect("activation on a 2 MiB stack must not abort");
            assert_eq!(fiber.state(), FiberState::Active);
            // The ownership tree built by the chain is unloaded iteratively: a
            // deep parent->child hierarchy must not recurse either.
            let _ = root.dispose_all();
            assert!(root.registry().keys().is_empty());
            assert!(root.registry().is_idle());
        })
        .expect("spawn 2 MiB thread");
    handle
        .join()
        .expect("R1: deep plugin nesting overflowed the 2 MiB thread stack");
    assert!(
        activated.load(Ordering::SeqCst) >= 100,
        "the chain did not actually nest deeply (activated={})",
        activated.load(Ordering::SeqCst)
    );
}

// ---------------------------------------------------------------------------
// C3: disposed nested effects must not leave ghost nodes in the effect tree
// ---------------------------------------------------------------------------

struct C3NestedPlugin {
    child: Arc<Mutex<Option<zeokeel_cordis::Disposer>>>,
}

impl Plugin for C3NestedPlugin {
    fn name(&self) -> &'static str {
        "c3-nested"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let child_slot = self.child.clone();
        let nested_ctx = ctx.clone();
        ctx.effect_labeled("c3-parent", move || {
            let child = nested_ctx
                .effect_labeled("c3-child", || vec![cleanup(|| {})])
                .expect("nested effect");
            *child_slot.lock().unwrap() = Some(child);
            vec![cleanup(|| {})]
        })?;
        Ok(())
    }
}

/// C3: a user-disposed nested effect stayed in its parent's `EffectMeta.children`
/// forever (the parent node tree is not rewritten by the disposer). The reported
/// tree must match the live registrations.
#[test]
fn c3_disposed_nested_effect_is_not_reported_in_the_tree() {
    let root = Context::root();
    let slot = Arc::new(Mutex::new(None));
    let fiber = root
        .plugin(
            Arc::new(C3NestedPlugin {
                child: slot.clone(),
            }),
            Value::Null,
        )
        .expect("nested plugin");
    let tree = fiber.get_effects();
    let parent = tree
        .iter()
        .find(|meta| meta.label == "c3-parent")
        .expect("parent effect reported");
    assert_eq!(
        parent
            .children
            .iter()
            .map(|child| child.label.as_str())
            .collect::<Vec<_>>(),
        vec!["c3-child"],
        "the live nested effect must be reported"
    );

    let child = slot
        .lock()
        .unwrap()
        .take()
        .expect("nested disposer captured");
    child.dispose();

    let tree = fiber.get_effects();
    let parent = tree
        .iter()
        .find(|meta| meta.label == "c3-parent")
        .expect("parent effect still live");
    assert!(
        parent.children.is_empty(),
        "C3: disposed nested effect still reported as a ghost: {:?}",
        parent.children
    );
    let _ = root.dispose_all();
    assert!(root.registry().keys().is_empty());
}

// ---------------------------------------------------------------------------
// C2: a panicking `Plugin::key()` is contained and degrades to a placeholder
// ---------------------------------------------------------------------------

struct C2PanickyKey;

impl Plugin for C2PanickyKey {
    fn name(&self) -> &'static str {
        "c2-panicky-key"
    }

    fn key(&self) -> PluginKey {
        panic!("C2 key boom");
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        Ok(())
    }
}

/// C2: `Plugin::key()` is embedder code called by `ctx.plugin`; a panic must be
/// contained and degrade to `PluginKey::Named("<panicked plugin>")` instead of
/// unwinding out of the registration API.
#[test]
fn c2_panicking_key_degrades_to_a_placeholder_runtime() {
    let root = Context::root();
    let fiber = root
        .plugin(Arc::new(C2PanickyKey), Value::Null)
        .expect("C2: a panicking key() must not abort ctx.plugin");
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(fiber.name(), "c2-panicky-key");
    assert!(
        root.registry().has(PluginKey::Named("<panicked plugin>")),
        "C2: a panicking key() registers under the documented placeholder runtime"
    );
    fiber.dispose().expect("dispose");
    assert!(root.registry().keys().is_empty());
}

/// C1: audit order trace — a three-level chain `B (provider) ← D1 ← D2`
/// disposes its dependents **before** the provider, deepest first:
/// `["D2", "D1", "B"]`.
#[test]
fn c1_three_level_cascade_order_is_dependents_first() {
    let trace: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
    let root = Context::root();
    let mut previous: Option<&'static str> = None;
    let mut fibers = Vec::new();
    for (index, label) in ["B", "D1", "D2"].into_iter().enumerate() {
        let svc: &'static str = Box::leak(format!("c1-order.svc.{index}").into_boxed_str());
        let plugin = Arc::new(ChainLink {
            svc,
            dep: previous,
            label,
            trace: trace.clone(),
        });
        let fiber = root.plugin(plugin, Value::Null).expect("order link");
        assert_eq!(fiber.state(), FiberState::Active);
        fibers.push(fiber);
        previous = Some(svc);
    }
    fibers[0].dispose().expect("dispose provider");
    assert_eq!(
        trace.lock().unwrap().as_slice(),
        ["D2", "D1", "B"],
        "C1: dependents must unload before the provider, deepest first"
    );
    assert_eq!(fibers[1].state(), FiberState::Pending);
    assert_eq!(fibers[2].state(), FiberState::Pending);
    let _ = root.dispose_all();
    assert!(root.registry().keys().is_empty());
}

// ---------------------------------------------------------------------------
// F1 (round 4): a cancelled async cascade parks fibers as `Unloading`; a later
// root operation / when_idle must recover them (never a permanent Unloading)
// ---------------------------------------------------------------------------

/// One link of an F1 chain: async cleanup that records the run and waits on a
/// gate, so a cancelled cascade can be parked mid-cleanup deterministically.
struct F1Link {
    svc: &'static str,
    dep: Option<&'static str>,
    label: &'static str,
    gate: Arc<AsyncLatch>,
    runs: Arc<Mutex<Vec<&'static str>>>,
}

impl Plugin for F1Link {
    fn name(&self) -> &'static str {
        self.label
    }

    fn inject(&self) -> &'static [&'static str] {
        static EMPTY: &[&str] = &[];
        match self.dep {
            Some(dep) => Box::leak(vec![dep].into_boxed_slice()),
            None => EMPTY,
        }
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.provide_checked(ServiceKey::new(self.svc), Arc::new(1u32), || true)?;
        let gate = self.gate.clone();
        let runs = self.runs.clone();
        let label = self.label;
        ctx.effect_async_labeled("f1-cleanup", move || {
            vec![zeokeel_cordis::cleanup_async(move || {
                let gate = gate.clone();
                let runs = runs.clone();
                async move {
                    runs.lock().unwrap().push(label);
                    gate.wait().await;
                }
            })]
        })?;
        Ok(())
    }
}

fn f1_chain_on(
    root: &Context,
    n: usize,
    gate: Arc<AsyncLatch>,
    runs: Arc<Mutex<Vec<&'static str>>>,
) -> Vec<Fiber> {
    let mut fibers = Vec::with_capacity(n + 1);
    let mut previous: Option<&'static str> = None;
    for index in 0..=n {
        let svc: &'static str = Box::leak(format!("f1-chain.svc.{index}").into_boxed_str());
        let label: &'static str = Box::leak(format!("F{index}").into_boxed_str());
        let plugin = Arc::new(F1Link {
            svc,
            dep: previous,
            label,
            gate: gate.clone(),
            runs: runs.clone(),
        });
        let fiber = root.plugin(plugin, Value::Null).expect("f1 link");
        assert_eq!(fiber.state(), FiberState::Active, "F1 link {index}");
        fibers.push(fiber);
        previous = Some(svc);
    }
    fibers
}

/// Build an F1 chain `P0 ← D1 ← … ← Dn` (all `Active`) on a fresh root context.
fn f1_chain(
    n: usize,
    gate: Arc<AsyncLatch>,
    runs: Arc<Mutex<Vec<&'static str>>>,
) -> (Context, Vec<Fiber>) {
    let root = Context::root();
    let fibers = f1_chain_on(&root, n, gate, runs);
    (root, fibers)
}

fn f1_park_mid_cascade(provider: &Fiber) {
    let mut dispose = Box::pin(provider.dispose_async());
    let waker = std::task::Waker::noop();
    let mut task_cx = std::task::Context::from_waker(waker);
    assert!(
        matches!(
            std::future::Future::poll(dispose.as_mut(), &mut task_cx),
            std::task::Poll::Pending
        ),
        "the cascade must park on the deepest dependent's async cleanup"
    );
    drop(dispose); // cancel mid-cascade
}

/// F1: retrying the root `dispose_async` recovers every parked dependent
/// (`Pending`, never a permanent `Unloading`) and every cleanup runs exactly
/// once (the root's own deferred cleanups finish here too).
#[test]
fn f1_retried_root_dispose_async_recovers_parked_cascade() {
    watchdog("f1-retry", Duration::from_secs(60), || {
        let gate = Arc::new(AsyncLatch::default());
        let runs = Arc::new(Mutex::new(Vec::new()));
        let (root, fibers) = f1_chain(8, gate.clone(), runs.clone());
        let provider = fibers[0].clone();
        f1_park_mid_cascade(&provider);

        assert_eq!(
            provider.state(),
            FiberState::Disposed,
            "root A-9 completion"
        );
        assert!(
            fibers[1..]
                .iter()
                .any(|fiber| fiber.state() == FiberState::Unloading),
            "F1 setup: a dependent must be parked as Unloading"
        );

        gate.open();
        block_on(provider.dispose_async()).expect("retried root dispose");
        assert_eq!(provider.state(), FiberState::Disposed);
        for (index, fiber) in fibers.iter().enumerate().skip(1) {
            assert_eq!(
                fiber.state(),
                FiberState::Pending,
                "parked dependent {index} must be recovered to its cascade target"
            );
        }
        // Recovered dependents end `Pending` (their cascade target) and stay
        // registered for a later provider re-activation.
        assert_eq!(root.registry().fiber_count(), fibers.len() - 1);
        let mut recorded = runs.lock().unwrap().clone();
        recorded.sort_unstable();
        let mut expected: Vec<&'static str> = (0..=8)
            .map(|i| Box::leak(format!("F{i}").into_boxed_str()) as &'static str)
            .collect();
        expected.sort_unstable();
        assert_eq!(
            recorded, expected,
            "F1: every cleanup must run exactly once across cancel + retry"
        );
        let _ = root.dispose_all();
        assert!(root.registry().keys().is_empty());
    });
}

/// F1: `dispose_all` is also a recovery point (it must not observe a permanent
/// `Unloading` left behind by a cancelled cascade).
#[test]
fn f1_dispose_all_recovers_parked_cascade() {
    watchdog("f1-dispose-all", Duration::from_secs(60), || {
        let gate = Arc::new(AsyncLatch::default());
        let runs = Arc::new(Mutex::new(Vec::new()));
        let (root, fibers) = f1_chain(6, gate.clone(), runs.clone());
        f1_park_mid_cascade(&fibers[0]);
        assert!(fibers[1..]
            .iter()
            .any(|fiber| fiber.state() == FiberState::Unloading));

        gate.open();
        root.dispose_all()
            .expect("dispose_all after a cancelled cascade");
        for fiber in &fibers {
            assert_eq!(fiber.state(), FiberState::Disposed);
        }
        assert!(root.registry().keys().is_empty());
        let recorded = runs.lock().unwrap().len();
        assert!(
            recorded >= fibers.len() - 1,
            "F1: dispose_all recovery must finish the deferred cleanups (ran={recorded})"
        );
    });
}

/// F1: `refresh_async` is a recovery point too: parked unloads are finished
/// before the dependency re-evaluation, and nothing stays `Unloading`.
#[test]
fn f1_refresh_async_recovers_parked_cascade() {
    watchdog("f1-refresh", Duration::from_secs(60), || {
        let gate = Arc::new(AsyncLatch::default());
        let runs = Arc::new(Mutex::new(Vec::new()));
        let (root, fibers) = f1_chain(6, gate.clone(), runs.clone());
        f1_park_mid_cascade(&fibers[0]);
        assert!(fibers[1..]
            .iter()
            .any(|fiber| fiber.state() == FiberState::Unloading));

        gate.open();
        block_on(root.refresh_async());
        assert!(
            fibers
                .iter()
                .all(|fiber| fiber.state() != FiberState::Unloading),
            "F1: refresh_async must recover parked Unloading fibers"
        );
        assert_eq!(fibers[0].state(), FiberState::Disposed);
        assert_eq!(root.registry().fiber_count(), fibers.len() - 1);
        assert!(root.registry().is_idle());
        let _ = root.dispose_all();
        assert!(root.registry().keys().is_empty());
    });
}

// ---------------------------------------------------------------------------
// F4/F5 (round 5): recovery latch RAII + recovery-aware when_idle loop
// ---------------------------------------------------------------------------

fn poll_once_pin<F: std::future::Future>(
    future: std::pin::Pin<&mut F>,
) -> std::task::Poll<F::Output> {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    std::future::Future::poll(future, &mut cx)
}

/// F4: if the recovery pass is cancelled while awaiting a parked fiber's gated
/// cleanup, the single-recoverer latch must be released (RAII), so the next
/// `when_idle` recovers the parked fibers instead of hanging forever.
#[test]
fn f4_cancelled_recovery_releases_the_latch_for_the_next_when_idle() {
    watchdog("f4-latch", Duration::from_secs(60), || {
        let gate = Arc::new(AsyncLatch::default());
        let runs = Arc::new(Mutex::new(Vec::new()));
        let (root, fibers) = f1_chain(4, gate.clone(), runs.clone());
        f1_park_mid_cascade(&fibers[0]);
        assert!(fibers[1..]
            .iter()
            .any(|fiber| fiber.state() == FiberState::Unloading));

        // First `when_idle`: recovery starts, unloads the first parked fiber and
        // parks on its gated cleanup. Drop the future mid-recovery.
        let mut first = Box::pin(root.when_idle());
        assert!(
            poll_once_pin(first.as_mut()).is_pending(),
            "F4 setup: the first when_idle must start recovery and wait on the gate"
        );
        drop(first);

        gate.open();
        // Old (manual begin/end) implementation left the latch set and this
        // second call skipped recovery forever -> the watchdog would fire.
        let (done_tx, done_rx) = mpsc::channel();
        let idle_root = root.clone();
        std::thread::spawn(move || {
            block_on(idle_root.when_idle());
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(Duration::from_secs(20))
            .expect("F4: when_idle must recover after a cancelled recovery pass");
        assert!(
            fibers
                .iter()
                .all(|fiber| fiber.state() != FiberState::Unloading),
            "F4: no fiber may stay parked in Unloading after the recovery retry"
        );
        assert_eq!(
            runs.lock().unwrap().len(),
            4,
            "F4: each restored cleanup must run exactly once"
        );
        let _ = root.dispose_all();
        assert!(root.registry().keys().is_empty());
    });
}

struct F5Blocker {
    started: Mutex<Option<mpsc::Sender<()>>>,
    gate: Arc<AsyncLatch>,
    runs: Arc<AtomicUsize>,
}

impl Plugin for F5Blocker {
    fn name(&self) -> &'static str {
        "f5-blocker"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let started = self.started.lock().unwrap().take();
        let gate = self.gate.clone();
        let runs = self.runs.clone();
        ctx.effect_async_labeled("f5-blocker-cleanup", move || {
            vec![zeokeel_cordis::cleanup_async(move || {
                let started = started;
                let gate = gate.clone();
                let runs = runs.clone();
                async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    if let Some(started) = started {
                        let _ = started.send(());
                    }
                    gate.wait().await;
                }
            })]
        })?;
        Ok(())
    }
}

/// F5: `when_idle` must keep recovering while it waits, not only at entry.
///
/// Scenario: another unload body is in flight when `when_idle` enters (recovery
/// is skipped), and *then* a cancelled cascade parks new frames. The waiter must
/// be woken by `park_unload`/unload-completion, recover both parked sets once
/// the in-flight unload ends, and return without hanging.
#[test]
fn f5_when_idle_recovers_parked_frames_that_appear_while_waiting() {
    watchdog("f5-wait", Duration::from_secs(60), || {
        let root = Context::root();

        // A separate unload body that stays in flight across the whole scenario.
        let blocker_gate = Arc::new(AsyncLatch::default());
        let blocker_runs = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let blocker = root
            .plugin(
                Arc::new(F5Blocker {
                    started: Mutex::new(Some(started_tx)),
                    gate: blocker_gate.clone(),
                    runs: blocker_runs.clone(),
                }),
                Value::Null,
            )
            .expect("blocker");
        let blocker_for_thread = blocker.clone();
        let in_flight = std::thread::spawn(move || {
            let mut dispose = Box::pin(blocker_for_thread.dispose_async());
            assert!(
                poll_once_pin(dispose.as_mut()).is_pending(),
                "F5 setup: the blocker unload stays in flight"
            );
            let _ = release_rx.recv();
            drop(dispose); // cancel: blocker frame becomes parked
        });
        started_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the blocker cleanup started");

        // A dependency chain is cancelled while the blocker unload is in flight
        // (same registry as the blocker: one root context).
        let gate = Arc::new(AsyncLatch::default());
        let runs = Arc::new(Mutex::new(Vec::new()));
        let fibers = f1_chain_on(&root, 3, gate.clone(), runs.clone());
        f1_park_mid_cascade(&fibers[0]);

        // Wait for idle: at entry an unload body is active, so the up-front
        // recovery pass is skipped; the parked frames appear/are noticed later.
        let (done_tx, done_rx) = mpsc::channel();
        let idle_root = root.clone();
        std::thread::spawn(move || {
            block_on(idle_root.when_idle());
            let _ = done_tx.send(());
        });
        std::thread::sleep(Duration::from_millis(100));

        // Let both the chain cleanups and the blocker cleanup finish, then end
        // the in-flight unload (which parks the blocker frame and wakes waiters).
        gate.open();
        blocker_gate.open();
        release_tx.send(()).expect("release blocker thread");
        in_flight.join().expect("blocker thread");
        done_rx
            .recv_timeout(Duration::from_secs(20))
            .expect("F5: when_idle must recover parked frames that appear while waiting");

        for (index, fiber) in fibers.iter().enumerate() {
            assert_ne!(
                fiber.state(),
                FiberState::Unloading,
                "F5: chain fiber {index} must not stay Unloading"
            );
        }
        assert_ne!(
            blocker.state(),
            FiberState::Unloading,
            "F5: the cancelled blocker must be recovered too"
        );
        let mut recorded = runs.lock().unwrap().clone();
        recorded.sort_unstable();
        assert_eq!(
            recorded,
            vec!["F1", "F2", "F3"],
            "F5: every restored dependent cleanup must run exactly once"
        );
        // The provider's own deferred cleanup is A-9 retry work: an explicit
        // retry drains it exactly once (no double run of the dependents).
        block_on(fibers[0].dispose_async()).expect("retried provider dispose");
        let mut recorded = runs.lock().unwrap().clone();
        recorded.sort_unstable();
        assert_eq!(
            recorded,
            vec!["F0", "F1", "F2", "F3"],
            "F5: retained provider cleanup must run exactly once on retry"
        );
        assert_eq!(
            blocker_runs.load(Ordering::SeqCst),
            1,
            "F5: the blocker cleanup ran once (in flight before cancellation)"
        );
        let _ = root.dispose_all();
        assert!(root.registry().keys().is_empty());
    });
}

/// F5: 20 consecutive cancel -> when_idle rounds must never hang and must leave
/// no `Unloading` fiber behind (discriminator for the shared recovery latch).
#[test]
fn f5_repeated_cancel_then_when_idle_does_not_hang() {
    watchdog("f5-loop", Duration::from_secs(120), || {
        for round in 0..20 {
            let gate = Arc::new(AsyncLatch::default());
            let runs = Arc::new(Mutex::new(Vec::new()));
            let (root, fibers) = f1_chain(3, gate.clone(), runs.clone());
            f1_park_mid_cascade(&fibers[0]);
            gate.open();
            let (done_tx, done_rx) = mpsc::channel();
            let idle_root = root.clone();
            std::thread::spawn(move || {
                block_on(idle_root.when_idle());
                let _ = done_tx.send(());
            });
            done_rx
                .recv_timeout(Duration::from_secs(20))
                .unwrap_or_else(|_| panic!("F5: round {round} when_idle hung"));
            // Dependents are recovered with their cleanups (P0's own deferred
            // cleanup is A-9 retry work, drained below exactly once).
            assert_eq!(
                runs.lock().unwrap().len(),
                3,
                "F5 round {round}: dependent cleanups must run exactly once"
            );
            for fiber in &fibers {
                assert_ne!(fiber.state(), FiberState::Unloading);
            }
            block_on(fibers[0].dispose_async()).expect("retried provider dispose");
            assert_eq!(
                runs.lock().unwrap().len(),
                4,
                "F5 round {round}: provider cleanup runs exactly once on retry"
            );
            let _ = root.dispose_all();
            assert!(root.registry().keys().is_empty());
        }
    });
}

/// F4 (E scenario): cancelling the recovery pass started from `refresh_async`
/// must not lose the took-out batch — a later `when_idle` must still recover
/// every parked fiber (no permanently `Unloading` entry).
#[test]
fn f4_cancelled_refresh_recovery_is_retried_by_when_idle() {
    watchdog("f4-refresh-cancel", Duration::from_secs(60), || {
        let gate = Arc::new(AsyncLatch::default());
        let runs = Arc::new(Mutex::new(Vec::new()));
        let (root, fibers) = f1_chain(3, gate.clone(), runs.clone());
        f1_park_mid_cascade(&fibers[0]);
        assert!(fibers[1..]
            .iter()
            .any(|fiber| fiber.state() == FiberState::Unloading));

        // First poll of refresh_async: refresh_all_async starts the recovery and
        // parks on the first parked fiber's gated cleanup. Cancel it there.
        let mut refresh = Box::pin(root.refresh_async());
        assert!(
            poll_once_pin(refresh.as_mut()).is_pending(),
            "F4 setup: refresh_async recovery must park on the gate"
        );
        drop(refresh);

        gate.open();
        let (done_tx, done_rx) = mpsc::channel();
        let idle_root = root.clone();
        std::thread::spawn(move || {
            block_on(idle_root.when_idle());
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(Duration::from_secs(20))
            .expect("F4: when_idle must recover frames lost by a cancelled recovery");
        for (index, fiber) in fibers.iter().enumerate() {
            assert_ne!(
                fiber.state(),
                FiberState::Unloading,
                "F4: fiber {index} was lost by the cancelled recovery"
            );
        }
        assert_eq!(
            runs.lock().unwrap().len(),
            3,
            "F4: recovered dependents' cleanups run exactly once"
        );
        let _ = root.dispose_all();
        assert!(root.registry().keys().is_empty());
    });
}

// ---------------------------------------------------------------------------
// F5 (round 5, A finding): the RAW public `Registry::when_idle` must also be
// recovery-aware — a parked cancelled cascade must not hang it.
// ---------------------------------------------------------------------------

/// A raw `registry.when_idle()` waiter (not `Context::when_idle`) must recover
/// parked frames: enter while an unload body is in flight, cancel a cascade to
/// park frames, then let everything finish — the raw waiter returns without a
/// permanent `Unloading`. Also checks the immediate-return fast path.
#[test]
fn f5_raw_registry_when_idle_recovers_parked_cascade() {
    watchdog("f5-raw", Duration::from_secs(60), || {
        let root = Context::root();
        let registry = root.registry();
        // Public API invariant: the waiter stays `Send` (hosts poll it on a
        // worker thread; the recovery-aware state machine must not break that).
        fn assert_send<T: Send>(value: &T) -> bool {
            let _ = value;
            true
        }
        {
            let waiter = registry.when_idle();
            assert!(assert_send(&waiter));
        }
        // Fast path: a genuinely idle registry resolves immediately (repeatedly).
        for _ in 0..3 {
            block_on(registry.when_idle());
        }

        // An unrelated unload body stays in flight while the raw waiter enters.
        let blocker_gate = Arc::new(AsyncLatch::default());
        let blocker_runs = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let blocker = root
            .plugin(
                Arc::new(F5Blocker {
                    started: Mutex::new(Some(started_tx)),
                    gate: blocker_gate.clone(),
                    runs: blocker_runs.clone(),
                }),
                Value::Null,
            )
            .expect("blocker");
        let blocker_for_thread = blocker.clone();
        let in_flight = std::thread::spawn(move || {
            let mut dispose = Box::pin(blocker_for_thread.dispose_async());
            assert!(poll_once_pin(dispose.as_mut()).is_pending());
            let _ = release_rx.recv();
            drop(dispose);
        });
        started_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("blocker cleanup started");

        let gate = Arc::new(AsyncLatch::default());
        let runs = Arc::new(Mutex::new(Vec::new()));
        let fibers = f1_chain_on(&root, 3, gate.clone(), runs.clone());
        f1_park_mid_cascade(&fibers[0]);

        let (done_tx, done_rx) = mpsc::channel();
        let raw_registry = registry.clone();
        std::thread::spawn(move || {
            block_on(raw_registry.when_idle());
            let _ = done_tx.send(());
        });
        std::thread::sleep(Duration::from_millis(100));

        gate.open();
        blocker_gate.open();
        release_tx.send(()).expect("release blocker");
        in_flight.join().expect("blocker thread");
        done_rx
            .recv_timeout(Duration::from_secs(20))
            .expect("F5: raw Registry::when_idle must recover parked frames");

        for (index, fiber) in fibers.iter().enumerate() {
            assert_ne!(
                fiber.state(),
                FiberState::Unloading,
                "F5 raw: chain fiber {index} must not stay Unloading"
            );
        }
        assert_ne!(blocker.state(), FiberState::Unloading);
        assert_eq!(
            runs.lock().unwrap().len(),
            3,
            "F5 raw: restored dependent cleanups run exactly once"
        );
        let _ = root.dispose_all();
        assert!(root.registry().keys().is_empty());
    });
}

/// F5 (A probe `whenidle-preexisting-parked`): the waiter enters while a slow
/// unload is in flight and **no** parked frame exists yet; the cancellation that
/// parks frames happens *during* the wait. The waiter must be woken by the
/// parking/unload-completion, recover the new parked set and return — for both
/// `Context::when_idle` and the raw `Registry::when_idle`.
#[test]
fn f5_when_idle_sees_parked_frames_parked_after_entry() {
    watchdog("f5-preexisting", Duration::from_secs(120), || {
        fn scenario(raw_registry_api: bool) {
            let root = Context::root();
            let registry = root.registry();

            // 1) A slow unload is in flight (active_unloads > 0), no parked yet.
            let blocker_gate = Arc::new(AsyncLatch::default());
            let blocker_runs = Arc::new(AtomicUsize::new(0));
            let (started_tx, started_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let blocker = root
                .plugin(
                    Arc::new(F5Blocker {
                        started: Mutex::new(Some(started_tx)),
                        gate: blocker_gate.clone(),
                        runs: blocker_runs.clone(),
                    }),
                    Value::Null,
                )
                .expect("blocker");
            let blocker_for_thread = blocker.clone();
            let in_flight = std::thread::spawn(move || {
                let mut dispose = Box::pin(blocker_for_thread.dispose_async());
                assert!(poll_once_pin(dispose.as_mut()).is_pending());
                let _ = release_rx.recv();
                drop(dispose);
            });
            started_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("blocker cleanup started");

            // 2) The waiter enters now: no parked exists, recovery is skipped
            //    and the loop waits behind the in-flight unload.
            let (done_tx, done_rx) = mpsc::channel();
            let wait_root = root.clone();
            let wait_registry = registry.clone();
            std::thread::spawn(move || {
                if raw_registry_api {
                    block_on(wait_registry.when_idle());
                } else {
                    block_on(wait_root.when_idle());
                }
                let _ = done_tx.send(());
            });
            std::thread::sleep(Duration::from_millis(100));
            assert!(
                done_rx.try_recv().is_err(),
                "F5 preexisting: when_idle must wait behind the in-flight unload"
            );

            // 3) A cancellation parks a new frame while the waiter sleeps.
            let gate = Arc::new(AsyncLatch::default());
            let runs = Arc::new(Mutex::new(Vec::new()));
            let fibers = f1_chain_on(&root, 3, gate.clone(), runs.clone());
            f1_park_mid_cascade(&fibers[0]);
            assert!(fibers[1..]
                .iter()
                .any(|fiber| fiber.state() == FiberState::Unloading));

            // 4) Let everything finish: the unload ends, its frame parks, waiters
            //    are notified, and the parked set becomes recoverable.
            gate.open();
            blocker_gate.open();
            release_tx.send(()).expect("release blocker");
            in_flight.join().expect("blocker thread");
            done_rx
                .recv_timeout(Duration::from_secs(20))
                .expect("F5 preexisting: when_idle missed the parked wake");
            for (index, fiber) in fibers.iter().enumerate() {
                assert_ne!(
                    fiber.state(),
                    FiberState::Unloading,
                    "F5 preexisting: fiber {index} stuck in Unloading"
                );
            }
            assert_ne!(blocker.state(), FiberState::Unloading);
            assert_eq!(
                runs.lock().unwrap().len(),
                3,
                "F5 preexisting: restored dependent cleanups run exactly once"
            );
            assert_eq!(
                blocker_runs.load(Ordering::SeqCst),
                1,
                "F5 preexisting: blocker cleanup ran exactly once"
            );
            assert!(registry.is_idle(), "F5 preexisting: registry must be idle");
            let _ = root.dispose_all();
            assert!(root.registry().keys().is_empty());
        }
        scenario(false);
        scenario(true);
    });
}

// ---------------------------------------------------------------------------
// S7 (0.4.4 P1): a cancelled recovery that finishes the cascade *root* frame on
// its target state must still drain the restored deferred cleanups — they may
// not be silently postponed until an explicit dispose.
// ---------------------------------------------------------------------------

/// E's S7 scenario: 3 dependents with gated async cleanups; cancel the raw
/// `Registry::when_idle` recovery mid-pass (it parks on the first restored
/// cleanup), then a fresh `when_idle` must run every restored cleanup exactly
/// once before returning.
#[test]
fn s7_cancelled_recovery_drains_root_deferred_cleanup() {
    watchdog("s7", Duration::from_secs(60), || {
        let gate = Arc::new(AsyncLatch::default());
        let runs = Arc::new(Mutex::new(Vec::new()));
        let (root, fibers) = f1_chain(3, gate.clone(), runs.clone());
        f1_park_mid_cascade(&fibers[0]);
        assert!(fibers[1..]
            .iter()
            .any(|fiber| fiber.state() == FiberState::Unloading));

        // Start the raw registry recovery and cancel it while it awaits the
        // first restored cleanup. (This is the S7 path: the fiber it was
        // finishing lands on `Pending` with restored cleanups in `deferred_async`.)
        let registry = root.registry();
        {
            let mut waiter = Box::pin(registry.when_idle());
            assert!(
                poll_once_pin(waiter.as_mut()).is_pending(),
                "S7 setup: the recovery must park on a restored cleanup"
            );
            drop(waiter);
        }

        gate.open();
        let (done_tx, done_rx) = mpsc::channel();
        let idle_root = root.clone();
        std::thread::spawn(move || {
            block_on(idle_root.when_idle());
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(Duration::from_secs(20))
            .expect("S7: when_idle must return after finishing the restored cleanups");

        let mut recorded = runs.lock().unwrap().clone();
        recorded.sort_unstable();
        assert_eq!(
            recorded,
            vec!["F1", "F2", "F3"],
            "S7: every restored dependent cleanup must run exactly once (no duplicate, no silent postponement)"
        );
        for (index, fiber) in fibers.iter().enumerate() {
            assert_ne!(
                fiber.state(),
                FiberState::Unloading,
                "S7: fiber {index} stuck in Unloading"
            );
        }
        assert!(
            root.registry().is_idle(),
            "S7: the registry must be quiescent once when_idle returns"
        );
        // The provider's own deferred cleanups remain explicit-retry work (A-9).
        block_on(fibers[0].dispose_async()).expect("retried provider dispose");
        let mut recorded = runs.lock().unwrap().clone();
        recorded.sort_unstable();
        assert_eq!(recorded, vec!["F0", "F1", "F2", "F3"]);
        let _ = root.dispose_all();
        assert!(root.registry().keys().is_empty());
    });
}

/// XT-01 regression: dropping the last `Fiber` handle after a cancelled
/// `dispose_async` must not block on (or deadlock behind) reactor-backed
/// deferred cleanups. Async cleanups are skipped unless the embedder installed
/// a `BlockingExecutor`; sync cleanups still run.
#[test]
fn xt01_dropping_a_fiber_with_deferred_reactor_cleanups_does_not_hang() {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context as TaskContext, Poll};
    use std::time::Duration;

    struct TwoAsync {
        completed: Arc<AtomicUsize>,
    }
    impl zeokeel_cordis::AsyncPlugin for TwoAsync {
        fn name(&self) -> &'static str {
            "xt01-two-async"
        }
        fn apply_async<'a>(
            &'a self,
            ctx: zeokeel_cordis::Context,
            _config: serde_json::Value,
        ) -> zeokeel_cordis::BoxFuture<'a, zeokeel_cordis::Result<()>> {
            let completed = self.completed.clone();
            Box::pin(async move {
                let first = completed.clone();
                ctx.effect_async(move || {
                    vec![zeokeel_cordis::cleanup_async(move || {
                        let first = first.clone();
                        async move {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            first.fetch_add(1, Ordering::SeqCst);
                        }
                    })]
                })?;
                ctx.effect_async(move || {
                    vec![zeokeel_cordis::cleanup_async(move || async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    })]
                })?;
                Ok(())
            })
        }
    }

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        let waker = futures_util::task::noop_waker();
        let mut cx = TaskContext::from_waker(&waker);
        future.poll(&mut cx)
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let root = zeokeel_cordis::Context::root();
        let completed = Arc::new(AtomicUsize::new(0));
        let fiber = root
            .plugin_async(
                Arc::new(TwoAsync {
                    completed: completed.clone(),
                }),
                serde_json::Value::Null,
            )
            .await
            .unwrap();
        let mut disposal = Box::pin(fiber.dispose_async());
        assert!(poll_once(disposal.as_mut()).is_pending());
        drop(disposal); // cancel while the first cleanup is in flight
                         // The last handle is dropped here; without the fix this blocks forever
                         // on the deferred reactor cleanup.
        drop(fiber);
        assert_eq!(
            completed.load(Ordering::SeqCst),
            0,
            "no BlockingExecutor is installed, so async cleanups are skipped (documented XT-01 trade-off)"
        );
        root.dispose_all().unwrap();
    });
}
