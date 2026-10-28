//! Red-team acceptance tests for the v0.2.0 defect register.
//!
//! Every test encodes the **desired** behavior, so on the frozen v0.2.0
//! baseline the failing ones are the acceptance criteria for the fix branch.
//! Tests are independent (fresh `Context::root()` each) and deterministic;
//! anything that can block runs under a watchdog thread so `cargo test` can
//! never hang.
//!
//! Register map:
//!  1. `rt01`, `rt02`   zombie-Active: dispose/cancel during activation
//!  2. `rt03`, `rt04`   `ctx.effect` owner window / unowned registrations
//!  3. `rt05`           dispose panic must still finalize `Disposed`
//!  4. `rt06`           panicking waterfall must not fail open
//!  5. `rt07`           `deferred_async` must not leak on dispose/drop
//!  6. `rt08`           cross-lane rollback runs in reverse registration order
//!  7. `rt09`, `rt10`   weak teardown: dropped provider ctx; accessor leak
//!  8. `rt11`           `get_any` / `set_any` honor accessors
//!  9. `rt12`, `rt13`   lifecycle races must fail safely, never leave a zombie
//! 10. `rt14`..`rt20`   panic containment at every embedder boundary
//! 11. `rt21`           `Fiber::name()` must not call user code under the lock
//! 12. `rt22`           public enums must be `#[non_exhaustive]`

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use zeokeel_cordis::{
    async_waterfall_terminal, block_on, cleanup, cleanup_async, waterfall_terminal, AsyncPlugin,
    BoxFuture, ConfigSchema, Context, CordisError, EventResult, Fiber, FiberState, LogExporter,
    LogRecord, Plugin, ServiceKey, ServiceValue,
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

fn labels(fiber: &Fiber) -> Vec<String> {
    fiber
        .get_effects()
        .iter()
        .map(|effect| effect.label.clone())
        .collect()
}

/// The most recently created non-disposed plugin fiber of `root`.
fn wait_fiber(root: &Context, timeout: Duration) -> Fiber {
    let start = Instant::now();
    loop {
        let candidates: Vec<Fiber> = root
            .registry()
            .fibers()
            .into_iter()
            .filter(|fiber| fiber.id() > 0 && fiber.state() != FiberState::Disposed)
            .collect();
        if let Some(fiber) = candidates.into_iter().max_by_key(|fiber| fiber.id()) {
            return fiber;
        }
        if start.elapsed() > timeout {
            panic!("no plugin fiber appeared within {timeout:?}");
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

static RT_USABLE: ServiceKey<u8> = ServiceKey::new("rt.kernel.usable");

struct NoopPlugin;

impl Plugin for NoopPlugin {
    fn name(&self) -> &'static str {
        "rt-noop"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        Ok(())
    }
}

/// The kernel is still usable: plugin loading and a fresh service registration
/// both work and the service resolves.
fn assert_kernel_usable(root: &Context) {
    assert!(
        root.plugin(Arc::new(NoopPlugin), Value::Null).is_ok(),
        "kernel unusable: plugin loading failed after the contained panic"
    );
    assert!(
        root.provide(RT_USABLE, Arc::new(1u8)).is_ok(),
        "kernel unusable: service registration failed after the contained panic"
    );
    assert_eq!(root.get(RT_USABLE).map(|value| *value), Some(1));
}

// ---------------------------------------------------------------------------
// 1. Zombie-Active: dispose during activation
// ---------------------------------------------------------------------------

struct SlowSync {
    started: Mutex<Option<mpsc::Sender<()>>>,
    release: Mutex<Option<mpsc::Receiver<()>>>,
    late_seen: AtomicBool,
    late_rejected: AtomicBool,
}

impl Plugin for SlowSync {
    fn name(&self) -> &'static str {
        "rt01-slow-sync"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        if let Some(tx) = self.started.lock().unwrap().take() {
            let _ = tx.send(());
        }
        if let Some(rx) = self.release.lock().unwrap().take() {
            let _ = rx.recv_timeout(Duration::from_secs(10));
        }
        match ctx.effect_labeled("late", Vec::new) {
            Ok(_) => self.late_seen.store(true, Ordering::SeqCst),
            Err(_) => self.late_rejected.store(true, Ordering::SeqCst),
        }
        Ok(())
    }
}

/// Register item 1 (sync): `dispose()` from another thread while `apply` runs
/// must win; the fiber must never end `Active` again, and the registration the
/// plugin attempts after the dispose must be rejected (not silently attached).
#[test]
fn rt01_sync_dispose_during_apply_must_not_end_active() {
    watchdog(
        "rt01_sync_dispose_during_apply_must_not_end_active",
        Duration::from_secs(20),
        || {
            let root = Context::root();
            let (started_tx, started_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let plugin = Arc::new(SlowSync {
                started: Mutex::new(Some(started_tx)),
                release: Mutex::new(Some(release_rx)),
                late_seen: AtomicBool::new(false),
                late_rejected: AtomicBool::new(false),
            });
            let root_for_activation = root.clone();
            let plugin_for_activation = plugin.clone();
            let activation = std::thread::spawn(move || {
                root_for_activation.plugin(plugin_for_activation, Value::Null)
            });

            started_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("plugin apply never started");
            let fiber = wait_fiber(&root, Duration::from_secs(10));
            assert_eq!(
                fiber.state(),
                FiberState::Loading,
                "test setup: activation should be Loading while apply sleeps"
            );

            let dispose = catch_unwind(AssertUnwindSafe(|| fiber.dispose()));
            assert!(
                dispose.is_ok(),
                "dispose() while apply sleeps must not panic"
            );
            release_tx.send(()).expect("release apply");
            let created = activation.join().expect("activation thread must not panic");
            assert!(created.is_ok(), "ctx.plugin must not panic");

            let state = fiber.state();
            assert!(
                matches!(state, FiberState::Disposed | FiberState::Failed),
                "ZOMBIE-ACTIVE: dispose during apply ended {state:?}, expected Disposed/Failed"
            );
            assert!(
                fiber.get_effects().is_empty(),
                "effects survived a dispose that happened during apply: {:?}",
                labels(&fiber)
            );
            assert!(
                !plugin.late_seen.load(Ordering::SeqCst),
                "post-dispose effect registration succeeded (silently attached)"
            );
            assert!(
                plugin.late_rejected.load(Ordering::SeqCst),
                "post-dispose effect registration was not rejected with InactiveEffect"
            );
            assert_eq!(
                root.registry().fiber_count(),
                0,
                "disposed fiber is still counted as a live registry fiber"
            );
            assert_eq!(
                root.registry().size(),
                0,
                "disposed fiber left a runtime entry in the registry"
            );
        },
    );
}

struct SlowAsync {
    started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    early_cleanup_ran: Arc<AtomicBool>,
}

impl AsyncPlugin for SlowAsync {
    fn name(&self) -> &'static str {
        "rt02-slow-async"
    }

    fn apply_async<'a>(
        &'a self,
        ctx: Context,
        _config: Value,
    ) -> BoxFuture<'a, zeokeel_cordis::Result<()>> {
        Box::pin(async move {
            let early_cleanup = self.early_cleanup_ran.clone();
            ctx.effect_labeled("early", move || {
                vec![cleanup(move || {
                    early_cleanup.store(true, Ordering::SeqCst);
                })]
            })?;
            let started = self.started.lock().unwrap().take();
            if let Some(tx) = started {
                let _ = tx.send(());
            }
            let gate = self.gate.lock().unwrap().take();
            if let Some(rx) = gate {
                let _ = rx.await;
            }
            Ok(())
        })
    }
}

/// Register item 1 (async): cancelling `plugin_async` by dropping its future
/// mid-await must roll back the effects registered so far and leave the fiber
/// `Pending`/`Failed`, never `Loading`.
#[test]
fn rt02_cancelled_plugin_async_future_rolls_back() {
    watchdog(
        "rt02_cancelled_plugin_async_future_rolls_back",
        Duration::from_secs(30),
        || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("tokio runtime");
            runtime.block_on(async {
                let root = Context::root();
                let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
                let early_cleanup_ran = Arc::new(AtomicBool::new(false));
                let plugin = Arc::new(SlowAsync {
                    started: Mutex::new(Some(started_tx)),
                    gate: Mutex::new(Some(gate_rx)),
                    early_cleanup_ran: early_cleanup_ran.clone(),
                });
                let root_for_activation = root.clone();
                let task = tokio::spawn(async move {
                    root_for_activation
                        .plugin_async(plugin, Value::Null)
                        .await
                        .map(|_| ())
                });

                tokio::time::timeout(Duration::from_secs(10), started_rx)
                    .await
                    .expect("activation must start")
                    .expect("started signal");
                let fiber = wait_fiber(&root, Duration::from_secs(10));
                assert_eq!(
                    fiber.state(),
                    FiberState::Loading,
                    "test setup: async activation should be Loading while awaiting"
                );

                task.abort();
                let joined = task.await;
                assert!(
                    joined.as_ref().err().map(|error| error.is_cancelled()) == Some(true),
                    "the activation task should have been cancelled"
                );
                // Give a synchronous drop-guard rollback a chance to run.
                tokio::time::sleep(Duration::from_millis(50)).await;

                let state = fiber.state();
                assert!(
                    matches!(state, FiberState::Pending | FiberState::Failed),
                    "CANCELLED-ACTIVATION: dropping plugin_async mid-await left {state:?} (must be Pending/Failed, never Loading)"
                );
                assert!(
                    fiber.get_effects().is_empty(),
                    "CANCELLED-ACTIVATION: effects registered before cancellation were not rolled back: {:?}",
                    labels(&fiber)
                );
                assert!(
                    early_cleanup_ran.load(Ordering::SeqCst),
                    "CANCELLED-ACTIVATION: rollback did not run the early effect's cleanup"
                );

                let _ = fiber.dispose_async().await;
                drop(gate_tx);
            });
        },
    );
}

// ---------------------------------------------------------------------------
// 2. `ctx.effect` ownership window
// ---------------------------------------------------------------------------

/// Register item 2a: an effect body that panics must not leave a stale
/// `effect_stack`; later registrations stay visible top-level effects.
#[test]
fn rt03_effect_body_panic_keeps_effect_tree_consistent() {
    let root = Context::root();
    let escaped = catch_unwind(AssertUnwindSafe(|| {
        let _ = root.effect_labeled("panicky", || panic!("effect body boom"));
    }))
    .is_err();
    assert!(
        escaped,
        "test setup: a panicking effect body is expected to unwind out of ctx.effect"
    );

    let later = root.effect_labeled("later", Vec::new);
    assert!(
        later.is_ok(),
        "effect registration after a caught body panic failed: {:?}",
        later
            .as_ref()
            .map(|_| ())
            .map_err(|error| error.to_string())
    );
    let observed = labels(&root.fiber());
    // The root fiber always owns the built-in logger service effect
    // ("ctx.provide"); the only plugin-registered top-level effect after the
    // body panic must be `later` (on v0.2 it is hidden under the stale node).
    assert!(
        observed.iter().any(|label| label == "later"),
        "OWNERSHIP-WINDOW: stale effect_stack hid the later registration (visible effects {observed:?})"
    );
    assert!(
        !observed.iter().any(|label| label == "panicky"),
        "a panicked effect body must not appear as a live effect (visible effects {observed:?})"
    );
}

static RT04_KEY: ServiceKey<u32> = ServiceKey::new("rt04.unowned.value");

/// Register item 2b: an effect body that disposes its own fiber must not leave
/// nested listener/service registrations behind as unowned state.
#[test]
fn rt04_nested_registrations_after_self_dispose_are_not_left_unowned() {
    let root = Context::root();
    let events = root.events();
    let listener_rejected = Arc::new(AtomicBool::new(false));
    let provide_rejected = Arc::new(AtomicBool::new(false));
    let listener_flag = listener_rejected.clone();
    let provide_flag = provide_rejected.clone();

    let outer = catch_unwind(AssertUnwindSafe(|| {
        let ctx = root.clone();
        root.effect(move || {
            let _ = ctx.fiber().dispose();
            listener_flag.store(
                ctx.on("rt04.evt", |_args| EventResult::Continue).is_err(),
                Ordering::SeqCst,
            );
            provide_flag.store(
                ctx.provide(RT04_KEY, Arc::new(1u32)).is_err(),
                Ordering::SeqCst,
            );
            Vec::new()
        })
    }));

    assert!(
        matches!(&outer, Ok(Err(_))),
        "registering an effect on a fiber disposed from inside its own body must fail"
    );
    assert!(
        listener_rejected.load(Ordering::SeqCst),
        "nested listener registration reported success after self-dispose"
    );
    assert!(
        provide_rejected.load(Ordering::SeqCst),
        "nested service registration reported success after self-dispose"
    );
    assert_eq!(
        events.listener_count(),
        0,
        "UNOWNED: listener registered after self-dispose leaked into the event bus"
    );
    assert!(
        root.get(RT04_KEY).is_none(),
        "UNOWNED: service registered after self-dispose leaked into the service store"
    );
}

// ---------------------------------------------------------------------------
// 3. Dispose panic finalization
// ---------------------------------------------------------------------------

static RT05_KEY: ServiceKey<u32> = ServiceKey::new("rt05.service");

struct PanicCleanupPlugin;

impl Plugin for PanicCleanupPlugin {
    fn name(&self) -> &'static str {
        "rt05-panic-cleanup"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        ctx.effect(|| vec![cleanup(|| panic!("cleanup boom"))])?;
        ctx.on("rt05.evt", |_args| EventResult::Continue)?;
        ctx.provide(RT05_KEY, Arc::new(1u32))?;
        Ok(())
    }
}

/// Register item 3: a panicking cleanup must not freeze the fiber in
/// `Unloading` with a live registry entry; finalization must still run.
#[test]
fn rt05_dispose_panic_still_finalizes_disposed() {
    let root = Context::root();
    let fiber = root
        .plugin(Arc::new(PanicCleanupPlugin), Value::Null)
        .expect("plugin activation");
    assert_eq!(fiber.state(), FiberState::Active);

    let _ = catch_unwind(AssertUnwindSafe(|| fiber.dispose()));

    assert_eq!(
        fiber.state(),
        FiberState::Disposed,
        "DISPOSE-PANIC: panicking cleanup left the fiber {:?}",
        fiber.state()
    );
    assert_eq!(
        root.registry().size(),
        0,
        "DISPOSE-PANIC: registry runtime entry leaked after a panicking dispose"
    );
    assert_eq!(
        root.registry().fiber_count(),
        0,
        "DISPOSE-PANIC: fiber is still live in the registry"
    );
    assert_eq!(
        root.events().listener_count(),
        0,
        "DISPOSE-PANIC: event listener leaked after a panicking dispose"
    );
    assert!(
        root.get(RT05_KEY).is_none(),
        "DISPOSE-PANIC: service leaked after a panicking dispose"
    );
    let second = catch_unwind(AssertUnwindSafe(|| fiber.dispose()));
    assert!(
        second.is_ok(),
        "second dispose after a panicking one must be a safe no-op"
    );
}

// ---------------------------------------------------------------------------
// 4. Fail-open waterfall
// ---------------------------------------------------------------------------

/// Register item 4: a panicking waterfall middleware must abort the chain; the
/// terminal (the guarded operation) must not run.
#[test]
fn rt06_panicking_waterfall_must_not_allow_the_guarded_operation() {
    let root = Context::root();
    let terminal_ran = Arc::new(AtomicBool::new(false));
    root.on_waterfall("rt06.pipeline", |_args, _next| {
        panic!("policy middleware boom")
    })
    .expect("register waterfall middleware");
    let terminal_flag = terminal_ran.clone();
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let _ = root.waterfall(
            "rt06.pipeline",
            &[],
            Some(waterfall_terminal(move |_args| {
                terminal_flag.store(true, Ordering::SeqCst);
                json!("GUARDED-OP")
            })),
        );
    }));
    assert!(
        !terminal_ran.load(Ordering::SeqCst),
        "FAIL-OPEN: a panicking middleware still ran the terminal (guarded operation was allowed)"
    );
    assert_kernel_usable(&root);
}

// ---------------------------------------------------------------------------
// 5. `deferred_async` leak
// ---------------------------------------------------------------------------

struct FailAfterAsyncEffect {
    cleanup_ran: Arc<AtomicBool>,
}

impl Plugin for FailAfterAsyncEffect {
    fn name(&self) -> &'static str {
        "rt07-deferred-async"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        let cleanup_flag = self.cleanup_ran.clone();
        ctx.effect_async(move || {
            vec![cleanup_async(move || {
                let cleanup_flag = cleanup_flag.clone();
                async move {
                    cleanup_flag.store(true, Ordering::SeqCst);
                }
            })]
        })?;
        Err(CordisError::InvalidPlugin("fail after async effect".into()))
    }
}

/// Register item 5: async cleanups deferred by a failed sync activation must
/// run when the `Failed` fiber is disposed, and must not leak silently when the
/// host drops the whole graph without an explicit dispose.
#[test]
fn rt07_failed_activation_async_cleanup_is_not_leaked() {
    // v0.3: a failed sync activation drains async cleanups during rollback
    // (reverse order across lanes, via the blocking executor), so nothing is
    // left behind even if the Failed fiber is never disposed.
    // (a) explicit dispose path
    let root = Context::root();
    let cleanup_ran = Arc::new(AtomicBool::new(false));
    let fiber = root
        .plugin(
            Arc::new(FailAfterAsyncEffect {
                cleanup_ran: cleanup_ran.clone(),
            }),
            Value::Null,
        )
        .expect("plugin creation");
    assert_eq!(fiber.state(), FiberState::Failed);
    assert!(
        cleanup_ran.load(Ordering::SeqCst),
        "the async cleanup must run during activation rollback"
    );
    fiber.dispose().expect("dispose on a Failed fiber");
    assert_eq!(fiber.state(), FiberState::Disposed);
    assert_eq!(root.registry().size(), 0);

    // (b) host shutdown path: the caller dropped its fiber handle, then
    // recovers the Failed fiber from the registry and disposes it. The deferred
    // async cleanup must still run (a Failed fiber is not terminal while the
    // registry owns it; dropping the last handle without disposal is the
    // documented "undisposed fibers keep their graph alive" limitation).
    let root = Context::root();
    let cleanup_ran = Arc::new(AtomicBool::new(false));
    let fiber = root
        .plugin(
            Arc::new(FailAfterAsyncEffect {
                cleanup_ran: cleanup_ran.clone(),
            }),
            Value::Null,
        )
        .expect("plugin creation");
    assert_eq!(fiber.state(), FiberState::Failed);
    let fiber_id = fiber.id();
    drop(fiber);
    let recovered = root
        .registry()
        .fibers()
        .into_iter()
        .find(|candidate| candidate.id() == fiber_id)
        .expect("Failed fiber is still registry-owned");
    recovered
        .dispose()
        .expect("dispose of a registry-owned Failed fiber");
    assert!(
        cleanup_ran.load(Ordering::SeqCst),
        "DEFERRED-LEAK: disposing a registry-owned Failed fiber did not run the deferred async cleanup"
    );
    assert_eq!(root.registry().size(), 0);
}

// ---------------------------------------------------------------------------
// 6. Cross-lane rollback order (CORE-007)
// ---------------------------------------------------------------------------

struct MixedLaneRollback {
    log: Arc<Mutex<Vec<String>>>,
}

impl Plugin for MixedLaneRollback {
    fn name(&self) -> &'static str {
        "rt08-mixed-rollback"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        let sync_log = self.log.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                sync_log.lock().unwrap().push("sync".to_string());
            })]
        })?;
        let async_log = self.log.clone();
        ctx.effect_async(move || {
            vec![cleanup_async(move || {
                let async_log = async_log.clone();
                async move {
                    async_log.lock().unwrap().push("async".to_string());
                }
            })]
        })?;
        Err(CordisError::InvalidPlugin("rollback across lanes".into()))
    }
}

/// Register item 6 (CORE-007): rollback of a failed activation must run sync
/// and async cleanups in **reverse registration order**, not lane-by-lane.
#[test]
fn rt08_cross_lane_rollback_runs_in_reverse_registration_order() {
    let root = Context::root();
    let log = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(
            Arc::new(MixedLaneRollback { log: log.clone() }),
            Value::Null,
        )
        .expect("plugin creation");
    assert_eq!(fiber.state(), FiberState::Failed);
    fiber.dispose().expect("dispose after failed activation");
    assert_eq!(
        *log.lock().unwrap(),
        vec!["async".to_string(), "sync".to_string()],
        "CORE-007: sync+async cleanups must run in reverse registration order (sync was registered first)"
    );
    assert_eq!(fiber.state(), FiberState::Disposed);
}

// ---------------------------------------------------------------------------
// 7. Weak teardown correctness
// ---------------------------------------------------------------------------

static RT09_KEY: ServiceKey<u32> = ServiceKey::new("rt09.value");

/// Register item 7a: the provide cleanup must remove the service entry even
/// when the providing `Context` was dropped first (no stale `DuplicateService`).
#[test]
fn rt09_provide_cleanup_survives_provider_context_drop() {
    let root = Context::root();
    let disposer = {
        let child = root.extend();
        let disposer = child
            .provide(RT09_KEY, Arc::new(1u32))
            .expect("provide from child context");
        drop(child);
        disposer
    };
    assert_eq!(root.get(RT09_KEY).map(|value| *value), Some(1));
    disposer.dispose();
    let again = root.provide(RT09_KEY, Arc::new(2u32));
    assert!(
        again.is_ok(),
        "STALE-SERVICE: re-provide after the provider context was dropped returned {:?}",
        again.err()
    );
    assert_eq!(root.get(RT09_KEY).map(|value| *value), Some(2));
}

struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

static RT10_SERVICE: ServiceKey<DropProbe> = ServiceKey::new("rt10.service");
static RT10_CONTROL: ServiceKey<DropProbe> = ServiceKey::new("rt10.control");

/// Register item 7b: the accessor disposer must not keep the Context graph
/// alive. A `Drop` probe stored in the service store tells us whether dropping
/// the root actually released it.
#[test]
fn rt10_accessor_disposer_does_not_pin_the_context_graph() {
    // Control: without an accessor, dropping the root releases the graph.
    let control_drops = Arc::new(AtomicUsize::new(0));
    {
        let root = Context::root();
        root.provide(RT10_CONTROL, Arc::new(DropProbe(control_drops.clone())))
            .expect("provide control probe");
    }
    assert_eq!(
        control_drops.load(Ordering::SeqCst),
        1,
        "control failed: dropping the root did not release the context graph"
    );

    // Probe: the registered accessor disposer captures the context strongly.
    let drops = Arc::new(AtomicUsize::new(0));
    {
        let root = Context::root();
        let _accessor_disposer = root
            .accessor("rt10.computed", |_ctx| -> ServiceValue { Arc::new(0u32) })
            .expect("register accessor");
        root.provide(RT10_SERVICE, Arc::new(DropProbe(drops.clone())))
            .expect("provide probe");
    }
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "LEAK: the accessor disposer kept the Context graph alive after the root was dropped (DropProbe not released)"
    );
}

// ---------------------------------------------------------------------------
// 8. `get_any` / `set_any` and accessors
// ---------------------------------------------------------------------------

trait RtGreeter: Send + Sync + 'static {
    fn greet(&self) -> String;
}

struct Greeter(&'static str);

impl RtGreeter for Greeter {
    fn greet(&self) -> String {
        self.0.to_string()
    }
}

static RT11_GREETER: ServiceKey<dyn RtGreeter> = ServiceKey::new("rt11.greeter");
static RT11_SETTABLE: ServiceKey<u32> = ServiceKey::new("rt11.settable");

/// Register item 8: `get_any` / `set_any` must honor accessors exactly like
/// `get` / `set` do.
#[test]
fn rt11_get_any_set_any_honor_accessors() {
    let root = Context::root();
    root.provide_any(RT11_GREETER, Arc::new(Greeter("hello")))
        .expect("provide trait-object service");
    root.accessor("rt11.greeter.alias", |ctx| {
        ctx.get_named("rt11.greeter")
            .expect("the underlying service is registered")
    })
    .expect("register alias accessor");

    let aliased = root.get_any(ServiceKey::<dyn RtGreeter>::new("rt11.greeter.alias"));
    assert!(
        aliased.is_some(),
        "get_any bypassed the accessor registered for `rt11.greeter.alias`"
    );
    assert_eq!(aliased.unwrap().greet(), "hello");

    let setter_called = Arc::new(AtomicBool::new(false));
    let setter_flag = setter_called.clone();
    root.accessor_with_setter(
        "rt11.settable",
        |_ctx| -> ServiceValue { Arc::new(0u32) },
        move |_ctx, _value| {
            setter_flag.store(true, Ordering::SeqCst);
            true
        },
    )
    .expect("register settable accessor");
    let written = root.set_any(RT11_SETTABLE, Arc::new(3u32));
    assert!(
        written.is_ok(),
        "set_any bypassed the accessor setter: {:?}",
        written.err()
    );
    assert!(
        setter_called.load(Ordering::SeqCst),
        "set_any did not call the accessor setter"
    );
}

// ---------------------------------------------------------------------------
// 9. Concurrent lifecycle
// ---------------------------------------------------------------------------

/// Register item 9a: a serialized `restart` after `dispose` must fail safely
/// (Err) instead of resurrecting the fiber into a Pending/Active zombie.
#[test]
fn rt12_restart_after_dispose_fails_safely() {
    let root = Context::root();
    let fiber = root
        .plugin(Arc::new(NoopPlugin), Value::Null)
        .expect("plugin activation");
    assert_eq!(fiber.state(), FiberState::Active);
    fiber.dispose().expect("dispose");
    assert_eq!(fiber.state(), FiberState::Disposed);

    let restart = catch_unwind(AssertUnwindSafe(|| fiber.restart()));
    let state = fiber.state();
    assert_eq!(
        state,
        FiberState::Disposed,
        "RESURRECTION: restart on a disposed fiber left state {state:?}"
    );
    let restart_failed = match restart {
        Ok(result) => result.is_err(),
        Err(_) => true,
    };
    assert!(
        restart_failed,
        "restart on a disposed fiber must return Err(CordisError::Disposed)"
    );
    assert!(
        fiber.update(Value::Null).is_err(),
        "update on a disposed fiber must return an error"
    );
}

#[derive(Default)]
struct RestartBomb {
    calls: AtomicUsize,
    started: Mutex<Option<mpsc::Sender<()>>>,
    release: Mutex<Option<mpsc::Receiver<()>>>,
}

impl Plugin for RestartBomb {
    fn name(&self) -> &'static str {
        "rt13-restart-bomb"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            ctx.on("rt13.evt", |_args| EventResult::Continue)?;
            return Ok(());
        }
        if let Some(tx) = self.started.lock().unwrap().take() {
            let _ = tx.send(());
        }
        if let Some(rx) = self.release.lock().unwrap().take() {
            let _ = rx.recv_timeout(Duration::from_secs(10));
        }
        Ok(())
    }
}

/// Register item 9b: dispose racing a restart's in-flight `apply` must win and
/// must not leave the fiber `Active`/`Loading` with effects or listeners of the
/// lost race.
#[test]
fn rt13_dispose_during_restart_apply_cannot_leave_a_zombie() {
    watchdog(
        "rt13_dispose_during_restart_apply_cannot_leave_a_zombie",
        Duration::from_secs(20),
        || {
            let root = Context::root();
            let (started_tx, started_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let plugin = Arc::new(RestartBomb {
                started: Mutex::new(Some(started_tx)),
                release: Mutex::new(Some(release_rx)),
                ..RestartBomb::default()
            });
            let fiber = root
                .plugin(plugin, Value::Null)
                .expect("initial activation");
            assert_eq!(fiber.state(), FiberState::Active);

            let restart_fiber = fiber.clone();
            let restart_thread = std::thread::spawn(move || {
                catch_unwind(AssertUnwindSafe(|| restart_fiber.restart()))
            });
            started_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("restart activation never started");
            // Concurrent lifecycle operations are exclusive: a dispose issued
            // while a restart owns the fiber must fail safely (no panic, no
            // corrupted state), not race the restart.
            let dispose = catch_unwind(AssertUnwindSafe(|| fiber.dispose()));
            assert!(dispose.is_ok(), "dispose during restart must not panic");
            let dispose_result = dispose.expect("no panic");
            assert!(
                matches!(dispose_result, Err(zeokeel_cordis::CordisError::LifecycleBusy)),
                "dispose during restart must be rejected with LifecycleBusy, got {dispose_result:?}"
            );
            assert_eq!(fiber.state(), FiberState::Loading);
            release_tx.send(()).expect("release restart apply");
            let restart = restart_thread
                .join()
                .expect("restart thread must not panic");
            assert!(restart.is_ok(), "restart must not panic");

            let state = fiber.state();
            assert_eq!(
                state,
                FiberState::Active,
                "the rejected dispose must leave the restart in charge: {state:?}"
            );
            assert!(
                fiber.get_effects().is_empty(),
                "the second activation registers no effects; nothing stale may survive: {:?}",
                labels(&fiber)
            );
            assert_eq!(
                root.events().listener_count(),
                0,
                "listener from the first activation must be gone"
            );
            assert_eq!(
                root.registry().size(),
                1,
                "the restarted fiber must stay registered"
            );
        },
    );
}

// ---------------------------------------------------------------------------
// 10. Panic containment completeness
// ---------------------------------------------------------------------------

/// Register item 10a: a panicking `internal/status` hook must be contained and
/// the remaining hooks must still run.
#[test]
fn rt14_internal_status_hook_panic_is_contained() {
    let root = Context::root();
    let later_ran = Arc::new(AtomicBool::new(false));
    root.on_internal_status(Arc::new(|_fiber: &Fiber, _old: FiberState| {
        panic!("status hook boom")
    }))
    .expect("register panicking hook");
    let later_flag = later_ran.clone();
    root.on_internal_status(Arc::new(move |_fiber: &Fiber, _old: FiberState| {
        later_flag.store(true, Ordering::SeqCst);
    }))
    .expect("register later hook");

    let created = catch_unwind(AssertUnwindSafe(|| {
        root.plugin(Arc::new(NoopPlugin), Value::Null)
    }));
    assert!(
        created.is_ok(),
        "CONTAINMENT: a panicking internal/status hook escaped the kernel API"
    );
    assert!(
        created.unwrap().is_ok(),
        "ctx.plugin must still succeed when an internal/status hook panics"
    );
    assert!(
        later_ran.load(Ordering::SeqCst),
        "remaining internal/status hooks must still run"
    );
    assert_kernel_usable(&root);
}

struct BoomExporter;

impl LogExporter for BoomExporter {
    fn export(&self, _record: &LogRecord) {
        panic!("exporter boom")
    }
}

struct CountingExporter(Arc<AtomicUsize>);

impl LogExporter for CountingExporter {
    fn export(&self, _record: &LogRecord) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Register item 10b: a panicking log exporter must not escape the logger API
/// and must not stop the remaining exporters.
#[test]
fn rt15_logger_exporter_panic_is_contained() {
    let root = Context::root();
    let hits = Arc::new(AtomicUsize::new(0));
    root.log_exporter(Arc::new(BoomExporter))
        .expect("register boom exporter");
    root.log_exporter(Arc::new(CountingExporter(hits.clone())))
        .expect("register counting exporter");

    let logged = catch_unwind(AssertUnwindSafe(|| {
        root.logger().info("contained", Value::Null)
    }));
    assert!(
        logged.is_ok(),
        "CONTAINMENT: a panicking log exporter escaped the kernel API"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "other exporters must still receive the record"
    );
    assert_kernel_usable(&root);
}

/// Register item 10c: a panicking reflect accessor must not escape service
/// lookup.
#[test]
fn rt16_accessor_panic_is_contained() {
    let root = Context::root();
    root.accessor("rt16.computed", |_ctx| -> ServiceValue {
        panic!("accessor boom")
    })
    .expect("register panicking accessor");

    let named = catch_unwind(AssertUnwindSafe(|| root.get_named("rt16.computed")));
    assert!(
        named.is_ok(),
        "CONTAINMENT: a panicking accessor escaped get_named"
    );
    assert!(root.get(ServiceKey::<u32>::new("rt16.computed")).is_none());
    assert_kernel_usable(&root);
}

struct BoomSchema;

impl ConfigSchema for BoomSchema {
    fn validate(&self, _value: Value) -> std::result::Result<Value, Vec<String>> {
        panic!("schema validate boom")
    }

    fn merge(&self, base: Value, _overrides: &[Value]) -> Value {
        base
    }
}

struct SchemaPlugin;

impl Plugin for SchemaPlugin {
    fn name(&self) -> &'static str {
        "rt17-schema"
    }

    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>> {
        Some(Arc::new(BoomSchema))
    }

    fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        Ok(())
    }
}

/// Register item 10d: a panicking `ConfigSchema::validate` must not escape
/// `ctx.plugin` and must leave a definite (non-Loading) fiber state.
#[test]
fn rt17_config_schema_validate_panic_is_contained() {
    let root = Context::root();
    let created = catch_unwind(AssertUnwindSafe(|| {
        root.plugin(Arc::new(SchemaPlugin), Value::Null)
    }));
    assert!(
        created.is_ok(),
        "CONTAINMENT: a panicking ConfigSchema::validate escaped ctx.plugin"
    );
    let fiber = created
        .unwrap()
        .expect("plugin creation must succeed even when its schema panics");
    assert_ne!(
        fiber.state(),
        FiberState::Loading,
        "a panicking schema must not leave the fiber Loading"
    );
    assert_kernel_usable(&root);
}

/// Register item 10e: a panicking async listener must not escape
/// `parallel_async`.
#[test]
fn rt18_parallel_async_listener_panic_is_contained() {
    let root = Context::root();
    let hits = Arc::new(AtomicUsize::new(0));
    root.on_async("rt18.par", |_args| async { panic!("async listener boom") })
        .expect("register panicking async listener");
    let hits_flag = hits.clone();
    root.on_async("rt18.par", move |_args| {
        let hits_flag = hits_flag.clone();
        async move {
            hits_flag.fetch_add(1, Ordering::SeqCst);
            EventResult::Continue
        }
    })
    .expect("register healthy async listener");

    let dispatched = catch_unwind(AssertUnwindSafe(|| {
        block_on(root.parallel_async("rt18.par", &[]))
    }));
    assert!(
        dispatched.is_ok(),
        "CONTAINMENT: a panicking async listener escaped parallel_async"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "other async listeners must still run"
    );
    assert_kernel_usable(&root);
}

/// Register item 10f: a panicking async waterfall middleware must not escape
/// the dispatch and must stop the chain (no terminal = fail closed).
#[test]
fn rt19_async_waterfall_panic_is_contained_and_stops_the_chain() {
    let root = Context::root();
    let terminal_ran = Arc::new(AtomicBool::new(false));
    root.on_waterfall_async("rt19.pipeline", |_args, _next| {
        zeokeel_cordis::boxed(async { panic!("async middleware boom") })
    })
    .expect("register panicking async middleware");
    let terminal_flag = terminal_ran.clone();
    let terminal = async_waterfall_terminal(move |_args| {
        terminal_flag.store(true, Ordering::SeqCst);
        std::future::ready(json!("GUARDED-OP"))
    });

    let dispatched = catch_unwind(AssertUnwindSafe(|| {
        block_on(root.waterfall_async("rt19.pipeline", &[], Some(terminal)))
    }));
    assert!(
        dispatched.is_ok(),
        "CONTAINMENT: a panicking async waterfall middleware escaped the dispatch"
    );
    assert!(
        !terminal_ran.load(Ordering::SeqCst),
        "FAIL-OPEN: a panicking async middleware still ran the terminal (guarded operation allowed)"
    );
    assert_kernel_usable(&root);
}

// (Removed: the process-level BlockingExecutor panic probe was a development
// artifact; in-process panic containment is covered by the blocking-executor
// tests in tests/async_conformance.rs.)

// ---------------------------------------------------------------------------
// 11. `Fiber::name()` must not call user code under the lock
// ---------------------------------------------------------------------------

struct NameBomb {
    armed: Arc<AtomicBool>,
}

impl Plugin for NameBomb {
    fn name(&self) -> &'static str {
        if self.armed.load(Ordering::SeqCst) {
            panic!("name boom")
        } else {
            "rt21-name-bomb"
        }
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        ctx.on("rt21.evt", |_args| EventResult::Continue)?;
        Ok(())
    }
}

/// Register item 11: `Fiber::name()` must call `Plugin::name()` outside the
/// `FiberData` lock, so a panicking name cannot poison the fiber or make its
/// state lie.
#[test]
fn rt21_fiber_name_does_not_call_user_code_under_the_lock() {
    let root = Context::root();
    let armed = Arc::new(AtomicBool::new(false));
    let fiber = root
        .plugin(
            Arc::new(NameBomb {
                armed: armed.clone(),
            }),
            Value::Null,
        )
        .expect("plugin activation");
    assert_eq!(fiber.state(), FiberState::Active);

    armed.store(true, Ordering::SeqCst);
    let name = catch_unwind(AssertUnwindSafe(|| fiber.name()));
    assert!(
        name.is_err(),
        "test setup: the probe plugin name should panic while armed"
    );

    assert_eq!(
        fiber.state(),
        FiberState::Active,
        "NAME-PANIC: fiber state lied after plugin.name() panicked (expected Active, got {:?}: the lock was poisoned)",
        fiber.state()
    );
    let disposed = catch_unwind(AssertUnwindSafe(|| fiber.dispose()));
    assert!(disposed.is_ok(), "dispose after a name() panic escaped");
    assert!(
        disposed.unwrap().is_ok(),
        "dispose after a name() panic failed (poisoned fiber lock)"
    );
    assert_eq!(fiber.state(), FiberState::Disposed);
    assert_eq!(
        root.events().listener_count(),
        0,
        "fiber teardown did not run after a name() panic"
    );
}

// ---------------------------------------------------------------------------
// 12. Enum evolution (`#[non_exhaustive]`)
// ---------------------------------------------------------------------------

/// Register item 12: the public error/state/result enums must be
/// `#[non_exhaustive]` so downstream crates cannot exhaustively match them and
/// new variants stay source-compatible. A downstream compile-fail test needs a
/// separate crate; this static check encodes the same acceptance criterion.
#[test]
fn rt22_public_enums_are_non_exhaustive() {
    fn assert_non_exhaustive(source: &str, file: &str, marker: &str) {
        let index = source
            .find(marker)
            .unwrap_or_else(|| panic!("`{marker}` not found in {file}"));
        let start = index.saturating_sub(400);
        let attributes = &source[start..index];
        assert!(
            attributes.contains("#[non_exhaustive]"),
            "ENUM-EVOLUTION: `{marker}` in {file} is not #[non_exhaustive]"
        );
    }

    assert_non_exhaustive(
        include_str!("../src/error.rs"),
        "src/error.rs",
        "pub enum CordisError",
    );
    assert_non_exhaustive(
        include_str!("../src/fiber.rs"),
        "src/fiber.rs",
        "pub enum FiberState",
    );
    assert_non_exhaustive(
        include_str!("../src/events.rs"),
        "src/events.rs",
        "pub enum EventResult",
    );
}
