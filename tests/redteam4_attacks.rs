//! Round-3 adversarial attacks on the v0.3 R4/R5 semantics:
//! per-fiber lifecycle exclusivity (`LifecycleBusy`), slot-release on panic/Err,
//! reentrant lifecycle paths, activation-vs-lifecycle fences, and cancel-safe
//! async disposal (CORE-045/CORE-046).
//!
//! Every test asserts the documented/briefed semantics and runs under a
//! watchdog thread so a kernel hang fails the test instead of blocking
//! `cargo test`.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use zeokeel_cordis::{
    block_on, cleanup, cleanup_async, AsyncDisposer, AsyncPlugin, BoxFuture, Context, CordisError,
    Fiber, FiberState, Plugin, ServiceKey,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Run `body` on a watchdog thread: a hang fails the test instead of blocking
/// the harness.
fn watchdog<T, F>(label: &str, timeout: Duration, body: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = mpsc::channel::<std::thread::Result<T>>();
    let label = label.to_string();
    let handle = std::thread::spawn(move || {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));
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

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let waker = std::task::Waker::noop();
    let mut cx = TaskContext::from_waker(waker);
    future.poll(&mut cx)
}

fn current_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current_thread runtime")
}

fn multi_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("multi_thread runtime")
}

fn fiber_from_registry(root: &Context) -> Fiber {
    let start = Instant::now();
    loop {
        if let Some(fiber) = root
            .registry()
            .fibers()
            .into_iter()
            .find(|fiber| fiber.id() > 0 && fiber.state() != FiberState::Disposed)
        {
            return fiber;
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("plugin fiber never appeared");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn is_busy(result: &Result<(), CordisError>) -> bool {
    matches!(result, Err(CordisError::LifecycleBusy))
}

/// A plugin whose cleanup signals `started` and blocks on a channel.
struct Gated {
    started: Arc<AtomicUsize>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
}

impl Plugin for Gated {
    fn name(&self) -> &'static str {
        "r4-gated"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        let started = self.started.clone();
        let release = self.release.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                started.fetch_add(1, Ordering::SeqCst);
                let rx = release.lock().unwrap();
                let _ = rx.recv_timeout(Duration::from_secs(5));
            })]
        })?;
        Ok(())
    }
}

fn gated_fiber(root: &Context) -> (Fiber, Arc<AtomicUsize>, mpsc::Sender<()>) {
    let (tx, rx) = mpsc::channel();
    let started = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(
            Arc::new(Gated {
                started: started.clone(),
                release: Arc::new(Mutex::new(rx)),
            }),
            Value::Null,
        )
        .expect("gated plugin");
    (fiber, started, tx)
}

// ---------------------------------------------------------------------------
// A. Lifecycle exclusivity (R4)
// ---------------------------------------------------------------------------

/// One thread owns the lifecycle slot; concurrent dispose/restart/update/
/// dispose_async from other threads all return `LifecycleBusy`, the owner
/// finishes, and a later dispose succeeds.
#[test]
fn r4_01_concurrent_lifecycle_calls_are_exclusive() {
    watchdog("r4_01", Duration::from_secs(20), || {
        let root = Context::root();
        let (fiber, started, release) = gated_fiber(&root);
        let owner_fiber = fiber.clone();
        let owner = std::thread::spawn(move || owner_fiber.dispose());
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(is_busy(&fiber.dispose()), "concurrent dispose must be Busy");
        assert!(is_busy(&fiber.restart()), "concurrent restart must be Busy");
        assert!(
            is_busy(&fiber.update(json!({}))),
            "concurrent update must be Busy"
        );
        let async_fiber = fiber.clone();
        let async_result = std::thread::spawn(move || block_on(async_fiber.dispose_async()))
            .join()
            .expect("async caller thread");
        assert!(
            is_busy(&async_result),
            "concurrent dispose_async must be Busy"
        );
        release.send(()).expect("release owner");
        owner.join().expect("owner thread").expect("owner dispose");
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert_eq!(root.registry().size(), 0);
        fiber
            .dispose()
            .expect("idempotent dispose after completion");
    });
}

struct Simple {
    applies: Arc<AtomicUsize>,
}

impl Plugin for Simple {
    fn name(&self) -> &'static str {
        "r4-simple"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// N threads hammering dispose/restart/update/dispose_async: no panic, every
/// result is Ok/LifecycleBusy/Disposed, and the final state is consistent.
#[test]
fn r4_02_lifecycle_storm_is_safe_and_settles() {
    watchdog("r4_02", Duration::from_secs(30), || {
        let root = Context::root();
        let applies = Arc::new(AtomicUsize::new(0));
        let fiber = root
            .plugin(
                Arc::new(Simple {
                    applies: applies.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let outcomes = Arc::new(Mutex::new(Vec::<(usize, String)>::new()));
        let mut handles = Vec::new();
        for index in 0..8usize {
            let fiber = fiber.clone();
            let barrier = barrier.clone();
            let outcomes = outcomes.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let result = match index % 4 {
                    0 => fiber.dispose(),
                    1 => fiber.restart(),
                    2 => fiber.update(json!({"index": index})),
                    _ => block_on(fiber.dispose_async()),
                };
                outcomes.lock().unwrap().push((
                    index % 4,
                    match &result {
                        Ok(()) => "ok".to_string(),
                        Err(error) => error.to_string(),
                    },
                ));
            }));
        }
        for handle in handles {
            handle.join().expect("storm thread must not panic");
        }
        let outcomes = outcomes.lock().unwrap().clone();
        for (kind, text) in &outcomes {
            assert!(
                text == "ok"
                    || text.contains("already in progress")
                    || text.contains("fiber already disposed")
                    // `Fiber::assert_active` maps "disposed" to InactiveEffect for
                    // update(); pre-existing v0.2 API wording, not an R4 race.
                    || text.contains("cannot create effect on inactive context"),
                "unexpected lifecycle outcome {kind}: {text}"
            );
        }
        let mut final_result = fiber.dispose();
        let start = Instant::now();
        while matches!(final_result, Err(CordisError::LifecycleBusy))
            && start.elapsed() < Duration::from_secs(5)
        {
            std::thread::sleep(Duration::from_millis(1));
            final_result = fiber.dispose();
        }
        assert!(final_result.is_ok(), "final dispose: {final_result:?}");
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert!(
            fiber.get_effects().is_empty(),
            "effects leaked through the lifecycle storm: {:?}",
            fiber.get_effects()
        );
        assert_eq!(root.registry().size(), 0);
        assert_eq!(root.registry().fiber_count(), 0);
    });
}

/// A rejected (Busy) call does not leak the slot: the owner finishes and a
/// retry from another thread succeeds.
#[test]
fn r4_03_busy_rejection_does_not_leak_the_slot() {
    watchdog("r4_03", Duration::from_secs(20), || {
        let root = Context::root();
        let (fiber, started, release) = gated_fiber(&root);
        let owner_fiber = fiber.clone();
        let owner = std::thread::spawn(move || owner_fiber.dispose());
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(is_busy(&fiber.dispose()));
        assert!(is_busy(&fiber.restart()));
        release.send(()).expect("release");
        owner.join().expect("owner").expect("owner dispose");
        let retry_fiber = fiber.clone();
        let retry = std::thread::spawn(move || retry_fiber.dispose())
            .join()
            .expect("retry thread");
        assert!(retry.is_ok(), "retry dispose: {retry:?}");
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert_eq!(root.registry().size(), 0);
    });
}

/// A panic from a cleanup escapes `dispose` (documented), but the slot must be
/// released: another thread can finalize the fiber afterwards.
#[test]
fn r4_04_panic_in_cleanup_releases_the_lifecycle_slot() {
    watchdog("r4_04", Duration::from_secs(20), || {
        struct PanicCleanup;
        impl Plugin for PanicCleanup {
            fn name(&self) -> &'static str {
                "r4-panic-cleanup"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                ctx.effect(|| vec![cleanup(|| panic!("cleanup boom"))])?;
                Ok(())
            }
        }
        let root = Context::root();
        let fiber = root.plugin(Arc::new(PanicCleanup), Value::Null).unwrap();
        let first = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fiber.dispose()));
        assert!(first.is_err(), "the cleanup panic should be observable");
        let retry_fiber = fiber.clone();
        let retry = std::thread::spawn(move || retry_fiber.dispose())
            .join()
            .expect("retry thread");
        assert!(
            retry.is_ok(),
            "LEAKED SLOT: retry dispose after a cleanup panic returned {retry:?}"
        );
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert_eq!(root.registry().size(), 0);
    });
}

/// A panicking apply during `restart` stays contained; the slot is released and
/// another thread can dispose the Failed fiber.
#[test]
fn r4_05_panic_in_apply_during_restart_releases_the_slot() {
    watchdog("r4_05", Duration::from_secs(20), || {
        struct PanicSecond {
            applies: Arc<AtomicUsize>,
        }
        impl Plugin for PanicSecond {
            fn name(&self) -> &'static str {
                "r4-panic-apply"
            }
            fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                let calls = self.applies.fetch_add(1, Ordering::SeqCst);
                if calls >= 1 {
                    panic!("apply boom");
                }
                Ok(())
            }
        }
        let root = Context::root();
        let applies = Arc::new(AtomicUsize::new(0));
        let fiber = root
            .plugin(
                Arc::new(PanicSecond {
                    applies: applies.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let restart = fiber.restart();
        assert!(restart.is_ok(), "restart must contain the apply panic");
        assert_eq!(applies.load(Ordering::SeqCst), 2);
        assert_eq!(fiber.state(), FiberState::Failed);
        let retry_fiber = fiber.clone();
        let retry = std::thread::spawn(move || retry_fiber.dispose())
            .join()
            .expect("retry thread");
        assert!(retry.is_ok(), "retry dispose: {retry:?}");
        assert_eq!(fiber.state(), FiberState::Disposed);
    });
}

/// An error return from a lifecycle API (`restart` on an async plugin) releases
/// the slot; another thread can dispose the fiber.
#[test]
fn r4_06_error_return_releases_the_slot() {
    watchdog("r4_06", Duration::from_secs(20), || {
        struct AsyncUnit;
        impl AsyncPlugin for AsyncUnit {
            fn name(&self) -> &'static str {
                "r4-async-unit"
            }
            fn apply_async<'a>(
                &'a self,
                _ctx: Context,
                _config: Value,
            ) -> BoxFuture<'a, zeokeel_cordis::Result<()>> {
                Box::pin(async { Ok(()) })
            }
        }
        let runtime = current_runtime();
        runtime.block_on(async {
            let root = Context::root();
            let fiber = root
                .plugin_async(Arc::new(AsyncUnit), Value::Null)
                .await
                .unwrap();
            let restart = fiber.restart();
            assert!(restart.is_err(), "sync restart of an async plugin must Err");
            let retry_fiber = fiber.clone();
            let retry = std::thread::spawn(move || retry_fiber.dispose())
                .join()
                .expect("retry thread");
            assert!(retry.is_ok(), "retry dispose after Err: {retry:?}");
            assert_eq!(fiber.state(), FiberState::Disposed);
        });
    });
}

/// `update` nests `restart`; the lifecycle depth must unwind after 100 nested
/// update/restart cycles so a concurrent dispose from another thread works.
#[test]
fn r4_07_nested_update_restart_releases_depth() {
    watchdog("r4_07", Duration::from_secs(30), || {
        let root = Context::root();
        let applies = Arc::new(AtomicUsize::new(0));
        let fiber = root
            .plugin(
                Arc::new(Simple {
                    applies: applies.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        for index in 0..100u32 {
            fiber
                .update(json!({"index": index}))
                .unwrap_or_else(|error| panic!("update {index} failed: {error:?}"));
        }
        let retry_fiber = fiber.clone();
        let retry = std::thread::spawn(move || retry_fiber.dispose())
            .join()
            .expect("other thread");
        assert!(
            retry.is_ok(),
            "LEAKED DEPTH: dispose from another thread after nested updates returned {retry:?}"
        );
        assert_eq!(fiber.state(), FiberState::Disposed);
    });
}

/// A cleanup may dispose its own fiber reentrantly: no deadlock, the remaining
/// cleanups still run once, and the fiber ends `Disposed`.
#[test]
fn r4_08_reentrant_dispose_in_cleanup_is_safe() {
    watchdog("r4_08", Duration::from_secs(20), || {
        type Trace = Arc<Mutex<Vec<&'static str>>>;
        struct Two {
            trace: Trace,
            results: Arc<Mutex<Vec<String>>>,
        }
        impl Plugin for Two {
            fn name(&self) -> &'static str {
                "r4-reentrant-dispose"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                let trace = self.trace.clone();
                ctx.effect(move || vec![cleanup(move || trace.lock().unwrap().push("old"))])?;
                let trace = self.trace.clone();
                let results = self.results.clone();
                let fiber = ctx.fiber();
                ctx.effect(move || {
                    vec![cleanup(move || {
                        trace.lock().unwrap().push("new");
                        results
                            .lock()
                            .unwrap()
                            .push(format!("{:?}", fiber.dispose().map_err(|e| e.to_string())));
                    })]
                })?;
                Ok(())
            }
        }
        let root = Context::root();
        let trace: Trace = Arc::new(Mutex::new(Vec::new()));
        let results = Arc::new(Mutex::new(Vec::new()));
        let fiber = root
            .plugin(
                Arc::new(Two {
                    trace: trace.clone(),
                    results: results.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        fiber.dispose().expect("outer dispose");
        assert_eq!(*trace.lock().unwrap(), vec!["new", "old"]);
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert!(fiber.get_effects().is_empty());
        assert_eq!(root.registry().size(), 0);
    });
}

/// A reentrant `restart` from inside a cleanup must not leak live effects on
/// the disposed fiber.
#[test]
fn r4_09_reentrant_restart_in_cleanup_does_not_leak_effects() {
    watchdog("r4_09", Duration::from_secs(20), || {
        struct ReentrantRestart {
            applies: Arc<AtomicUsize>,
            cleanups: Arc<AtomicUsize>,
            results: Arc<Mutex<Vec<String>>>,
        }
        impl Plugin for ReentrantRestart {
            fn name(&self) -> &'static str {
                "r4-reentrant-restart"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                self.applies.fetch_add(1, Ordering::SeqCst);
                let fiber = ctx.fiber();
                let cleanups = self.cleanups.clone();
                let results = self.results.clone();
                ctx.effect(move || {
                    vec![cleanup(move || {
                        cleanups.fetch_add(1, Ordering::SeqCst);
                        results
                            .lock()
                            .unwrap()
                            .push(format!("{:?}", fiber.restart().map_err(|e| e.to_string())));
                    })]
                })?;
                Ok(())
            }
        }
        let root = Context::root();
        let applies = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let results = Arc::new(Mutex::new(Vec::new()));
        let fiber = root
            .plugin(
                Arc::new(ReentrantRestart {
                    applies: applies.clone(),
                    cleanups: cleanups.clone(),
                    results: results.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        fiber.dispose().expect("outer dispose");
        assert_eq!(fiber.state(), FiberState::Disposed);
        fiber
            .dispose()
            .expect("second dispose must still be a safe no-op");
        let live: Vec<String> = fiber
            .get_effects()
            .iter()
            .map(|effect| effect.label.clone())
            .collect();
        assert!(
            live.is_empty(),
            "REENTRANT-RESTART LEAK: effects still owned by a Disposed fiber: {live:?} \
             (cleanups ran {}, applies {})",
            cleanups.load(Ordering::SeqCst),
            applies.load(Ordering::SeqCst)
        );
        assert_eq!(
            cleanups.load(Ordering::SeqCst),
            applies.load(Ordering::SeqCst),
            "every activation must be cleaned up exactly once"
        );
    });
}

/// A reentrant `update` from inside a cleanup must not leak live effects on the
/// disposed fiber.
#[test]
fn r4_10_reentrant_update_in_cleanup_does_not_leak_effects() {
    watchdog("r4_10", Duration::from_secs(20), || {
        struct ReentrantUpdate {
            applies: Arc<AtomicUsize>,
            cleanups: Arc<AtomicUsize>,
            results: Arc<Mutex<Vec<String>>>,
        }
        impl Plugin for ReentrantUpdate {
            fn name(&self) -> &'static str {
                "r4-reentrant-update"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                self.applies.fetch_add(1, Ordering::SeqCst);
                let fiber = ctx.fiber();
                let cleanups = self.cleanups.clone();
                let results = self.results.clone();
                ctx.effect(move || {
                    vec![cleanup(move || {
                        cleanups.fetch_add(1, Ordering::SeqCst);
                        results.lock().unwrap().push(format!(
                            "{:?}",
                            fiber.update(json!({})).map_err(|e| e.to_string())
                        ));
                    })]
                })?;
                Ok(())
            }
        }
        let root = Context::root();
        let applies = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let results = Arc::new(Mutex::new(Vec::new()));
        let fiber = root
            .plugin(
                Arc::new(ReentrantUpdate {
                    applies: applies.clone(),
                    cleanups: cleanups.clone(),
                    results: results.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        fiber.dispose().expect("outer dispose");
        assert_eq!(fiber.state(), FiberState::Disposed);
        fiber.dispose().expect("second dispose");
        let live: Vec<String> = fiber
            .get_effects()
            .iter()
            .map(|effect| effect.label.clone())
            .collect();
        assert!(
            live.is_empty(),
            "REENTRANT-UPDATE LEAK: effects still owned by a Disposed fiber: {live:?} \
             (cleanups ran {}, applies {})",
            cleanups.load(Ordering::SeqCst),
            applies.load(Ordering::SeqCst)
        );
        assert_eq!(
            cleanups.load(Ordering::SeqCst),
            applies.load(Ordering::SeqCst)
        );
    });
}

/// Disposing fiber A from fiber B's cleanup is allowed (different lifecycles).
#[test]
fn r4_11_cross_fiber_dispose_from_cleanup() {
    watchdog("r4_11", Duration::from_secs(20), || {
        struct Target {
            cleanups: Arc<AtomicUsize>,
        }
        impl Plugin for Target {
            fn name(&self) -> &'static str {
                "r4-target"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                let cleanups = self.cleanups.clone();
                ctx.effect(move || {
                    vec![cleanup(move || {
                        cleanups.fetch_add(1, Ordering::SeqCst);
                    })]
                })?;
                Ok(())
            }
        }
        struct DisposerPlugin;
        impl Plugin for DisposerPlugin {
            fn name(&self) -> &'static str {
                "r4-disposer"
            }
            fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                Ok(())
            }
        }
        let root = Context::root();
        let target_cleanups = Arc::new(AtomicUsize::new(0));
        let target = root
            .plugin(
                Arc::new(Target {
                    cleanups: target_cleanups.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let disposer = root.plugin(Arc::new(DisposerPlugin), Value::Null).unwrap();
        let target_for_cleanup = target.clone();
        let results = Arc::new(Mutex::new(None));
        let results_for_cleanup = results.clone();
        disposer
            .context()
            .unwrap()
            .effect(move || {
                vec![cleanup(move || {
                    *results_for_cleanup.lock().unwrap() =
                        Some(target_for_cleanup.dispose().map_err(|e| e.to_string()));
                })]
            })
            .unwrap();
        disposer.dispose().expect("dispose the disposing fiber");
        assert!(results.lock().unwrap().is_some(), "cross-fiber cleanup ran");
        assert_eq!(target.state(), FiberState::Disposed);
        assert_eq!(target_cleanups.load(Ordering::SeqCst), 1);
    });
}

/// Disposing fiber A from A's own async cleanup (reentrant, same thread) is
/// allowed and does not deadlock.
#[test]
fn r4_12_reentrant_async_dispose_in_async_cleanup() {
    watchdog("r4_12", Duration::from_secs(30), || {
        type Cell = Arc<Mutex<Option<Fiber>>>;
        struct ReentrantAsync {
            results: Arc<Mutex<Vec<String>>>,
            cell: Cell,
        }
        impl AsyncPlugin for ReentrantAsync {
            fn name(&self) -> &'static str {
                "r4-reentrant-async"
            }
            fn apply_async<'a>(
                &'a self,
                ctx: Context,
                _config: Value,
            ) -> BoxFuture<'a, zeokeel_cordis::Result<()>> {
                let cell = self.cell.clone();
                let results = self.results.clone();
                Box::pin(async move {
                    ctx.effect_async(move || {
                        vec![cleanup_async(move || {
                            let cell = cell.clone();
                            let results = results.clone();
                            async move {
                                let fiber = cell.lock().unwrap().clone().expect("fiber cell");
                                let result = fiber.dispose_async().await;
                                results.lock().unwrap().push(format!(
                                    "reentrant={:?}",
                                    result.map_err(|e| e.to_string())
                                ));
                            }
                        })]
                    })?;
                    Ok(())
                })
            }
        }
        let runtime = multi_runtime();
        runtime.block_on(async {
            let root = Context::root();
            let results = Arc::new(Mutex::new(Vec::new()));
            let cell: Cell = Arc::new(Mutex::new(None));
            let fiber = root
                .plugin_async(
                    Arc::new(ReentrantAsync {
                        results: results.clone(),
                        cell: cell.clone(),
                    }),
                    Value::Null,
                )
                .await
                .unwrap();
            *cell.lock().unwrap() = Some(fiber.clone());
            let dispose = fiber.dispose_async().await;
            assert!(dispose.is_ok(), "outer dispose_async: {dispose:?}");
            assert_eq!(fiber.state(), FiberState::Disposed);
            assert!(fiber.get_effects().is_empty());
            assert_eq!(results.lock().unwrap().len(), 1);
            assert_eq!(root.registry().size(), 0);
        });
    });
}

/// `dispose` during a slow `apply` keeps the activation fence (it must run, not
/// return `LifecycleBusy`), rolls the activation back, and finalizes.
#[test]
fn r4_13_dispose_during_slow_apply_is_fenced_not_busy() {
    watchdog("r4_13", Duration::from_secs(20), || {
        struct Slow {
            started: Arc<AtomicUsize>,
            release: Arc<Mutex<mpsc::Receiver<()>>>,
        }
        impl Plugin for Slow {
            fn name(&self) -> &'static str {
                "r4-slow-dispose"
            }
            fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                self.started.fetch_add(1, Ordering::SeqCst);
                let rx = self.release.lock().unwrap();
                let _ = rx.recv_timeout(Duration::from_secs(5));
                Ok(())
            }
        }
        let root = Context::root();
        let started = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();
        let plugin = Arc::new(Slow {
            started: started.clone(),
            release: Arc::new(Mutex::new(rx)),
        });
        let root_for_activation = root.clone();
        let activation =
            std::thread::spawn(move || root_for_activation.plugin(plugin, Value::Null));
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let fiber = fiber_from_registry(&root);
        let dispose = fiber.dispose();
        assert!(
            !is_busy(&dispose),
            "activation does not own the lifecycle slot: dispose must not be Busy"
        );
        assert!(dispose.is_ok(), "fenced dispose: {dispose:?}");
        tx.send(()).expect("release apply");
        activation
            .join()
            .expect("activation thread")
            .expect("plugin creation");
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert!(fiber.get_effects().is_empty());
        assert_eq!(root.registry().size(), 0);
    });
}

/// `restart` during a slow `apply`: the restart wins, both activations run, and
/// every registered effect is cleaned up exactly once by the final dispose.
#[test]
fn r4_14_restart_during_slow_apply_stays_consistent() {
    watchdog("r4_14", Duration::from_secs(20), || {
        struct Slow {
            applies: Arc<AtomicUsize>,
            cleanups: Arc<AtomicUsize>,
            started: Arc<AtomicUsize>,
            release: Arc<Mutex<mpsc::Receiver<()>>>,
        }
        impl Plugin for Slow {
            fn name(&self) -> &'static str {
                "r4-slow-restart"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                let call = self.applies.fetch_add(1, Ordering::SeqCst);
                let cleanups = self.cleanups.clone();
                ctx.effect(move || {
                    vec![cleanup(move || {
                        cleanups.fetch_add(1, Ordering::SeqCst);
                    })]
                })?;
                if call == 0 {
                    self.started.fetch_add(1, Ordering::SeqCst);
                    let rx = self.release.lock().unwrap();
                    let _ = rx.recv_timeout(Duration::from_secs(5));
                }
                Ok(())
            }
        }
        let root = Context::root();
        let applies = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();
        let plugin = Arc::new(Slow {
            applies: applies.clone(),
            cleanups: cleanups.clone(),
            started: started.clone(),
            release: Arc::new(Mutex::new(rx)),
        });
        let root_for_activation = root.clone();
        let activation =
            std::thread::spawn(move || root_for_activation.plugin(plugin, Value::Null));
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let fiber = fiber_from_registry(&root);
        let restart = fiber.restart();
        assert!(restart.is_ok(), "restart during slow apply: {restart:?}");
        tx.send(()).expect("release apply");
        activation
            .join()
            .expect("activation thread")
            .expect("plugin creation");
        assert_eq!(fiber.state(), FiberState::Active);
        fiber.dispose().expect("dispose");
        assert_eq!(
            cleanups.load(Ordering::SeqCst),
            applies.load(Ordering::SeqCst),
            "each apply registered one effect; all must be cleaned exactly once"
        );
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert_eq!(root.registry().size(), 0);
    });
}

static R4_SERVICE: ServiceKey<u32> = ServiceKey::new("r4.svc");

/// `restart` during a slow `apply`: when the stale activation later fails, it
/// must not roll back / fail the new activation that already won.
#[test]
fn r4_15_stale_activation_failure_does_not_clobber_the_restart() {
    watchdog("r4_15", Duration::from_secs(20), || {
        struct Slow {
            applies: Arc<AtomicUsize>,
            started: Arc<AtomicUsize>,
            release: Arc<Mutex<mpsc::Receiver<()>>>,
        }
        impl Plugin for Slow {
            fn name(&self) -> &'static str {
                "r4-stale-failure"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                let call = self.applies.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    self.started.fetch_add(1, Ordering::SeqCst);
                    let rx = self.release.lock().unwrap();
                    let _ = rx.recv_timeout(Duration::from_secs(5));
                    // Stale activation: the new activation already provided it.
                    ctx.provide(R4_SERVICE, Arc::new(1u32))?;
                } else {
                    ctx.provide(R4_SERVICE, Arc::new(2u32))?;
                }
                ctx.effect(|| vec![cleanup(|| {})])?;
                Ok(())
            }
        }
        let root = Context::root();
        let applies = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();
        let plugin = Arc::new(Slow {
            applies: applies.clone(),
            started: started.clone(),
            release: Arc::new(Mutex::new(rx)),
        });
        let root_for_activation = root.clone();
        let activation =
            std::thread::spawn(move || root_for_activation.plugin(plugin, Value::Null));
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let fiber = fiber_from_registry(&root);
        let restart = fiber.restart();
        assert!(restart.is_ok(), "restart: {restart:?}");
        tx.send(()).expect("release stale apply");
        activation
            .join()
            .expect("activation thread")
            .expect("plugin creation");
        let state = fiber.state();
        assert_eq!(
            state,
            FiberState::Active,
            "STALE-CLOBBER: the failed stale activation left the fiber {state:?} \
             (service={:?}, error={:?})",
            root.get(R4_SERVICE).map(|value| *value),
            fiber.error()
        );
        assert_eq!(
            root.get(R4_SERVICE).map(|value| *value),
            Some(2),
            "the winning restart's service must stay registered"
        );
        assert!(
            fiber.error().is_none(),
            "no error expected: {:?}",
            fiber.error()
        );
        // Winning activation owns two effects: `ctx.provide` + its own body
        // effect. The stale attempt contributed none.
        assert_eq!(fiber.get_effects().len(), 2);
        fiber.dispose().expect("dispose");
        assert_eq!(fiber.state(), FiberState::Disposed);
    });
}

/// Cancelling a `dispose_async` that owns the lifecycle slot releases it: a
/// concurrent dispose returns Busy, and after the cancellation a retry works.
#[test]
fn r4_16_cancelled_dispose_async_releases_the_slot() {
    watchdog("r4_16", Duration::from_secs(20), || {
        struct Pending;
        impl Plugin for Pending {
            fn name(&self) -> &'static str {
                "r4-pending"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                ctx.effect_async(|| {
                    vec![cleanup_async(|| async {
                        std::future::pending::<()>().await;
                    })]
                })?;
                Ok(())
            }
        }
        let root = Context::root();
        let fiber = root.plugin(Arc::new(Pending), Value::Null).unwrap();
        let mut future = Box::pin(fiber.dispose_async());
        assert!(poll_once(future.as_mut()).is_pending());
        let busy_fiber = fiber.clone();
        let busy = std::thread::spawn(move || busy_fiber.dispose())
            .join()
            .expect("busy thread");
        assert!(
            is_busy(&busy),
            "dispose while dispose_async owns the slot must be Busy, got {busy:?}"
        );
        drop(future);
        let retry_fiber = fiber.clone();
        let retry = std::thread::spawn(move || retry_fiber.dispose())
            .join()
            .expect("retry thread");
        assert!(
            retry.is_ok(),
            "LEAKED SLOT: retry after cancelling dispose_async returned {retry:?}"
        );
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert_eq!(root.registry().size(), 0);
    });
}

/// Cancelling before the first poll leaves the fiber untouched; cancelling a
/// disposal of an already disposed fiber is a no-op.
#[test]
fn r4_17_cancel_before_poll_and_after_disposed() {
    watchdog("r4_17", Duration::from_secs(20), || {
        struct Noop;
        impl Plugin for Noop {
            fn name(&self) -> &'static str {
                "r4-noop"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                ctx.effect(|| vec![cleanup(|| {})])?;
                Ok(())
            }
        }
        let root = Context::root();
        let fiber = root.plugin(Arc::new(Noop), Value::Null).unwrap();
        let future = Box::pin(fiber.dispose_async());
        drop(future);
        assert_eq!(fiber.state(), FiberState::Active);
        fiber.dispose().expect("dispose");
        assert_eq!(fiber.state(), FiberState::Disposed);
        let mut after = Box::pin(fiber.dispose_async());
        assert!(matches!(poll_once(after.as_mut()), Poll::Ready(Ok(()))));
        drop(after);
    });
}

// ---------------------------------------------------------------------------
// B. Cancel-safe disposal (R5)
// ---------------------------------------------------------------------------

/// Cancel at the second await point (after one effect completed): the cleanups
/// that had not started still run exactly once in reverse order on retry.
#[test]
fn r5_01_cancel_at_second_await_preserves_order() {
    watchdog("r5_01", Duration::from_secs(20), || {
        type Trace = Arc<Mutex<Vec<&'static str>>>;
        struct Mixed {
            trace: Trace,
            gate: Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
        }
        impl Plugin for Mixed {
            fn name(&self) -> &'static str {
                "r5-mixed"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                let trace = self.trace.clone();
                ctx.effect(move || vec![cleanup(move || trace.lock().unwrap().push("s1"))])?;
                let trace = self.trace.clone();
                let gate = self.gate.lock().unwrap().take().unwrap();
                ctx.effect_async(move || {
                    vec![cleanup_async(move || {
                        let trace = trace.clone();
                        async move {
                            trace.lock().unwrap().push("a2-start");
                            gate.await.ok();
                            trace.lock().unwrap().push("a2-end");
                        }
                    })]
                })?;
                let trace = self.trace.clone();
                ctx.effect(move || vec![cleanup(move || trace.lock().unwrap().push("s3"))])?;
                Ok(())
            }
        }
        let root = Context::root();
        let trace: Trace = Arc::new(Mutex::new(Vec::new()));
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let fiber = root
            .plugin(
                Arc::new(Mixed {
                    trace: trace.clone(),
                    gate: Arc::new(Mutex::new(Some(gate_rx))),
                }),
                Value::Null,
            )
            .unwrap();
        let mut future = Box::pin(fiber.dispose_async());
        assert!(poll_once(future.as_mut()).is_pending());
        drop(future);
        // A-9: the cancelled unload completes the teardown: the not-yet-started
        // sync cleanup `s1` runs inline (reverse order preserved) and the fiber
        // is Disposed; the in-flight async `a2` tail is the documented loss.
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert_eq!(*trace.lock().unwrap(), vec!["s3", "a2-start", "s1"]);
        let _ = gate_tx.send(());
        let retry = block_on(fiber.dispose_async());
        assert!(retry.is_ok(), "retry: {retry:?}");
        assert_eq!(*trace.lock().unwrap(), vec!["s3", "a2-start", "s1"]);
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert_eq!(root.registry().size(), 0);
    });
}

/// Repeatedly cancelling the same AsyncDisposer: each attempt loses at most the
/// single in-flight cleanup, no cleanup starts twice, and the handle ends
/// disposed.
#[test]
fn r5_02_repeated_direct_disposer_cancels_are_safe() {
    watchdog("r5_02", Duration::from_secs(20), || {
        let starts = Arc::new(AtomicUsize::new(0));
        let mut gates = Vec::new();
        let mut cleanups = Vec::new();
        for _ in 0..3 {
            let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
            gates.push(gate_tx);
            let starts = starts.clone();
            cleanups.push(cleanup_async(move || async move {
                starts.fetch_add(1, Ordering::SeqCst);
                let _ = gate_rx.await;
            }));
        }
        let disposer = AsyncDisposer::new(cleanups);
        for attempt in 0..3 {
            let mut future = Box::pin(disposer.dispose());
            assert!(
                poll_once(future.as_mut()).is_pending(),
                "attempt {attempt} should be pending"
            );
            drop(future);
        }
        assert_eq!(
            starts.load(Ordering::SeqCst),
            3,
            "each cancelled attempt starts exactly its one in-flight cleanup (no duplicates)"
        );
        let mut final_future = Box::pin(disposer.dispose());
        assert!(
            matches!(poll_once(final_future.as_mut()), Poll::Ready(())),
            "the disposer must be settled after the cancellations (only in-flight cleanups were lost)"
        );
        assert_eq!(starts.load(Ordering::SeqCst), 3);
        assert!(disposer.is_disposed());
        drop(gates);
    });
}

/// A separate future joining on the same thread is **not** a self-join (R2-1 /
/// drain-frame): it must wait for the in-flight teardown. Cancelling the owner
/// then releases the joiner, and a retry runs the restored cleanups once.
///
/// The real self-join case (a cleanup that `await`s its own disposer) is covered
/// by `regression_semantics::fix15_self_join_from_cleanup_via_async_dispose_returns`.
#[test]
fn r5_03_cancel_wakes_joiner_and_retry_runs_remaining() {
    watchdog("r5_03", Duration::from_secs(20), || {
        let trace = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let trace_for_fast = trace.clone();
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let disposer = AsyncDisposer::new(vec![
            cleanup_async(move || {
                let trace = trace_for_fast.clone();
                async move {
                    trace.lock().unwrap().push("c1");
                }
            }),
            cleanup_async(move || async move {
                let _ = gate_rx.await;
            }),
        ]);
        let mut first = Box::pin(disposer.dispose());
        assert!(poll_once(first.as_mut()).is_pending());
        // A separate joiner future on the same thread is not a self-join: it
        // must wait instead of short-circuiting.
        let joiner_disposer = disposer.clone();
        let mut joiner = Box::pin(joiner_disposer.dispose());
        assert!(
            poll_once(joiner.as_mut()).is_pending(),
            "a separate future joining on the same thread is not a self-join: it must wait"
        );
        drop(joiner);
        // Cancelling the owner releases any joiner and leaves the unstarted
        // cleanups restored; a retry runs them exactly once.
        drop(first);
        let _ = gate_tx.send(());
        block_on(disposer.dispose());
        assert_eq!(*trace.lock().unwrap(), vec!["c1"]);
        assert!(disposer.is_disposed());
    });
}

/// A `dispose_async` future dropped before its first poll leaves the fiber
/// untouched.
#[test]
fn r5_04_cancel_before_first_poll_keeps_fiber_state() {
    watchdog("r5_04", Duration::from_secs(20), || {
        struct Noop;
        impl Plugin for Noop {
            fn name(&self) -> &'static str {
                "r5-noop"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                ctx.effect(|| vec![cleanup(|| {})])?;
                Ok(())
            }
        }
        let root = Context::root();
        let fiber = root.plugin(Arc::new(Noop), Value::Null).unwrap();
        let future = Box::pin(fiber.dispose_async());
        drop(future);
        assert_eq!(fiber.state(), FiberState::Active);
        fiber.dispose().expect("dispose");
        assert_eq!(fiber.state(), FiberState::Disposed);
    });
}

/// Cancelling during the deferred drain: the remaining deferred cleanups run
/// exactly once and the retry finalizes the fiber.
#[test]
fn r5_05_cancel_during_deferred_drain_runs_remaining() {
    watchdog("r5_05", Duration::from_secs(20), || {
        struct AsyncCancel {
            remaining_runs: Arc<AtomicUsize>,
        }
        impl AsyncPlugin for AsyncCancel {
            fn name(&self) -> &'static str {
                "r5-deferred"
            }
            fn apply_async<'a>(
                &'a self,
                ctx: Context,
                _config: Value,
            ) -> BoxFuture<'a, zeokeel_cordis::Result<()>> {
                let remaining = self.remaining_runs.clone();
                Box::pin(async move {
                    ctx.effect_async(move || {
                        vec![cleanup_async(move || {
                            let remaining = remaining.clone();
                            async move {
                                remaining.fetch_add(1, Ordering::SeqCst);
                            }
                        })]
                    })?;
                    ctx.effect_async(|| {
                        vec![cleanup_async(|| async {
                            std::future::pending::<()>().await;
                        })]
                    })?;
                    std::future::pending::<()>().await;
                    Ok(())
                })
            }
        }
        let runtime = current_runtime();
        runtime.block_on(async {
            let root = Context::root();
            let remaining_runs = Arc::new(AtomicUsize::new(0));
            let plugin = Arc::new(AsyncCancel {
                remaining_runs: remaining_runs.clone(),
            });
            let mut creation = Box::pin(root.plugin_async(plugin, Value::Null));
            assert!(poll_once(creation.as_mut()).is_pending());
            drop(creation); // async-activation cancel guard defers the cleanups
            let fiber = fiber_from_registry(&root);
            assert_eq!(fiber.state(), FiberState::Failed);
            assert!(
                fiber.get_effects().is_empty(),
                "activation rollback restores async cleanups as deferred"
            );

            let mut future = Box::pin(fiber.dispose_async());
            assert!(poll_once(future.as_mut()).is_pending());
            drop(future);
            // A-9: a cancelled `dispose_async` lands the fiber on `Disposed`
            // directly; the still-unstarted deferred cleanup waits for the retry.
            assert_eq!(fiber.state(), FiberState::Disposed);
            assert_eq!(
                remaining_runs.load(Ordering::SeqCst),
                0,
                "deferred cleanups still wait for the retry dispose"
            );
            let retry = fiber.dispose_async().await;
            assert!(retry.is_ok(), "retry: {retry:?}");
            assert_eq!(fiber.state(), FiberState::Disposed);
            assert_eq!(
                remaining_runs.load(Ordering::SeqCst),
                1,
                "no duplicate cleanup on retry"
            );
            assert_eq!(root.registry().size(), 0);
        });
    });
}

/// A fibre with an async effect having two cleanups: cancelling while the newer
/// cleanup is in flight restores the older cleanup; if that restored cleanup
/// needs the reactor, the current-thread runtime must not hang (the cleanup has
/// to be restored for the retry, not run via the blocking Drop path).
#[test]
fn r5_06_fiber_cancel_with_reactor_remaining_must_not_hang() {
    watchdog("r5_06", Duration::from_secs(8), || {
        struct Two {
            completed: Arc<AtomicUsize>,
        }
        impl Plugin for Two {
            fn name(&self) -> &'static str {
                "r5-fiber-reactor"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                let completed = self.completed.clone();
                ctx.effect_async(move || {
                    vec![
                        cleanup_async(move || {
                            let completed = completed.clone();
                            async move {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                completed.fetch_add(1, Ordering::SeqCst);
                            }
                        }),
                        cleanup_async(|| async {
                            std::future::pending::<()>().await;
                        }),
                    ]
                })?;
                Ok(())
            }
        }
        let runtime = current_runtime();
        runtime.block_on(async {
            let root = Context::root();
            let completed = Arc::new(AtomicUsize::new(0));
            let fiber = root
                .plugin(
                    Arc::new(Two {
                        completed: completed.clone(),
                    }),
                    Value::Null,
                )
                .unwrap();
            let mut future = Box::pin(fiber.dispose_async());
            assert!(poll_once(future.as_mut()).is_pending());
            drop(future); // must not block: unstarted cleanup has to be restored
            let retry = fiber.dispose_async().await;
            assert!(retry.is_ok(), "retry: {retry:?}");
            assert_eq!(fiber.state(), FiberState::Disposed);
            assert_eq!(
                completed.load(Ordering::SeqCst),
                1,
                "the restored reactor cleanup must run exactly once on retry"
            );
        });
    });
}

/// Cancelling during the deferred drain with a reactor-backed deferred cleanup:
/// the deferred drain must restore it instead of dropping it through the
/// blocking `Drop` safety net.
#[test]
fn r5_07_deferred_cancel_with_reactor_remaining_must_not_hang() {
    watchdog("r5_07", Duration::from_secs(8), || {
        struct AsyncCancel {
            completed: Arc<AtomicUsize>,
        }
        impl AsyncPlugin for AsyncCancel {
            fn name(&self) -> &'static str {
                "r5-deferred-reactor"
            }
            fn apply_async<'a>(
                &'a self,
                ctx: Context,
                _config: Value,
            ) -> BoxFuture<'a, zeokeel_cordis::Result<()>> {
                let completed = self.completed.clone();
                Box::pin(async move {
                    ctx.effect_async(move || {
                        vec![cleanup_async(move || {
                            let completed = completed.clone();
                            async move {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                completed.fetch_add(1, Ordering::SeqCst);
                            }
                        })]
                    })?;
                    ctx.effect_async(|| {
                        vec![cleanup_async(|| async {
                            std::future::pending::<()>().await;
                        })]
                    })?;
                    std::future::pending::<()>().await;
                    Ok(())
                })
            }
        }
        let runtime = current_runtime();
        runtime.block_on(async {
            let root = Context::root();
            let completed = Arc::new(AtomicUsize::new(0));
            let plugin = Arc::new(AsyncCancel {
                completed: completed.clone(),
            });
            let mut creation = Box::pin(root.plugin_async(plugin, Value::Null));
            assert!(poll_once(creation.as_mut()).is_pending());
            drop(creation);
            let fiber = fiber_from_registry(&root);
            let mut future = Box::pin(fiber.dispose_async());
            assert!(poll_once(future.as_mut()).is_pending());
            drop(future);
            let retry = fiber.dispose_async().await;
            assert!(retry.is_ok(), "retry: {retry:?}");
            assert_eq!(fiber.state(), FiberState::Disposed);
            assert_eq!(
                completed.load(Ordering::SeqCst),
                1,
                "the restored deferred reactor cleanup must run exactly once"
            );
        });
    });
}

/// A disposal future cancelled on a non-runtime thread must not lose more than
/// the in-flight cleanup: the not-yet-started reactor cleanup has to be
/// restored for the retry (the current Drop path silently loses it).
#[test]
fn r5_08_cancel_outside_runtime_restores_remaining_cleanup() {
    watchdog("r5_08", Duration::from_secs(20), || {
        struct Two {
            completed: Arc<AtomicUsize>,
            gate: Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
        }
        impl Plugin for Two {
            fn name(&self) -> &'static str {
                "r5-outside-runtime"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                let completed = self.completed.clone();
                let gate = self.gate.lock().unwrap().take().unwrap();
                ctx.effect_async(move || {
                    vec![
                        cleanup_async(move || {
                            let completed = completed.clone();
                            async move {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                completed.fetch_add(1, Ordering::SeqCst);
                            }
                        }),
                        cleanup_async(move || async move {
                            let _ = gate.await;
                        }),
                    ]
                })?;
                Ok(())
            }
        }
        let runtime = current_runtime();
        runtime.block_on(async {
            let root = Context::root();
            let completed = Arc::new(AtomicUsize::new(0));
            let (_gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
            let fiber = root
                .plugin(
                    Arc::new(Two {
                        completed: completed.clone(),
                        gate: Arc::new(Mutex::new(Some(gate_rx))),
                    }),
                    Value::Null,
                )
                .unwrap();
            let mut future = Box::pin(fiber.dispose_async());
            assert!(poll_once(future.as_mut()).is_pending());
            // Drop on a plain thread: the restored reactor cleanup cannot make
            // progress there, so it must be kept for the retry.
            std::thread::scope(|scope| {
                scope.spawn(move || drop(future));
            });
            // A-9: the cancelled unload finishes the teardown synchronously, so
            // the fiber is already Disposed; the not-yet-started reactor cleanup
            // waits for the retry (it cannot progress on the plain thread).
            assert_eq!(fiber.state(), FiberState::Disposed);
            let retry = fiber.dispose_async().await;
            assert!(retry.is_ok(), "retry: {retry:?}");
            assert_eq!(fiber.state(), FiberState::Disposed);
            assert_eq!(
                completed.load(Ordering::SeqCst),
                1,
                "not-yet-started cleanup lost on cancellation outside the runtime \
                 (exactly-once violated)"
            );
        });
    });
}

/// Cancelling while the async effect cleanup would later panic: the in-flight
/// cleanup is lost (documented), the retry finalizes Disposed, no panic escapes.
#[test]
fn r5_09_cancel_while_cleanup_would_panic() {
    watchdog("r5_09", Duration::from_secs(20), || {
        struct Boom;
        impl Plugin for Boom {
            fn name(&self) -> &'static str {
                "r5-boom"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                ctx.effect_async(|| {
                    vec![cleanup_async(|| async {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        panic!("cleanup boom");
                    })]
                })?;
                Ok(())
            }
        }
        let runtime = multi_runtime();
        runtime.block_on(async {
            let root = Context::root();
            let fiber = root.plugin(Arc::new(Boom), Value::Null).unwrap();
            let mut future = Box::pin(fiber.dispose_async());
            assert!(poll_once(future.as_mut()).is_pending());
            drop(future);
            let retry = block_on(fiber.dispose_async());
            assert!(
                retry.is_ok(),
                "retry after cancelling a panicking cleanup: {retry:?}"
            );
            assert_eq!(fiber.state(), FiberState::Disposed);
            assert_eq!(root.registry().size(), 0);
        });
    });
}

/// Documented loss bound: cancelling loses at most the single in-flight cleanup
/// future; every other cleanup starts and completes exactly once.
#[test]
fn r5_10_cancel_loses_at_most_the_in_flight_cleanup() {
    watchdog("r5_10", Duration::from_secs(20), || {
        struct Two {
            completed: Arc<AtomicUsize>,
            started: Arc<AtomicUsize>,
        }
        impl Plugin for Two {
            fn name(&self) -> &'static str {
                "r5-loss-bound"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                let completed = self.completed.clone();
                let started = self.started.clone();
                ctx.effect_async(move || {
                    vec![
                        cleanup_async(move || {
                            let completed = completed.clone();
                            async move {
                                completed.fetch_add(1, Ordering::SeqCst);
                            }
                        }),
                        cleanup_async(move || {
                            let started = started.clone();
                            async move {
                                started.fetch_add(1, Ordering::SeqCst);
                                std::future::pending::<()>().await;
                            }
                        }),
                    ]
                })?;
                Ok(())
            }
        }
        let runtime = multi_runtime();
        runtime.block_on(async {
            let root = Context::root();
            let completed = Arc::new(AtomicUsize::new(0));
            let started = Arc::new(AtomicUsize::new(0));
            let fiber = root
                .plugin(
                    Arc::new(Two {
                        completed: completed.clone(),
                        started: started.clone(),
                    }),
                    Value::Null,
                )
                .unwrap();
            let mut future = Box::pin(fiber.dispose_async());
            assert!(poll_once(future.as_mut()).is_pending());
            drop(future);
            let retry = block_on(fiber.dispose_async());
            assert!(retry.is_ok(), "retry: {retry:?}");
            assert_eq!(fiber.state(), FiberState::Disposed);
            assert_eq!(
                started.load(Ordering::SeqCst),
                1,
                "the in-flight cleanup must start exactly once"
            );
            assert_eq!(
                completed.load(Ordering::SeqCst),
                1,
                "every not-in-flight cleanup must complete exactly once (no duplicate, no extra loss)"
            );
        });
    });
}

/// Interaction: cancel a `dispose_async` (slot held), a concurrent `dispose`
/// reports Busy, and the retry after cancellation finalizes the fiber.
#[test]
fn r5_11_cancel_then_busy_then_retry() {
    watchdog("r5_11", Duration::from_secs(20), || {
        struct Pending;
        impl Plugin for Pending {
            fn name(&self) -> &'static str {
                "r5-pending"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                ctx.effect_async(|| {
                    vec![cleanup_async(|| async {
                        std::future::pending::<()>().await;
                    })]
                })?;
                Ok(())
            }
        }
        let runtime = multi_runtime();
        runtime.block_on(async {
            let root = Context::root();
            let fiber = root.plugin(Arc::new(Pending), Value::Null).unwrap();
            let mut future = Box::pin(fiber.dispose_async());
            assert!(poll_once(future.as_mut()).is_pending());
            let busy_fiber = fiber.clone();
            let busy = std::thread::spawn(move || busy_fiber.dispose())
                .join()
                .expect("busy thread");
            assert!(is_busy(&busy), "expected Busy, got {busy:?}");
            drop(future);
            let retry_fiber = fiber.clone();
            let retry = std::thread::spawn(move || retry_fiber.dispose())
                .join()
                .expect("retry thread");
            assert!(retry.is_ok(), "retry after cancel: {retry:?}");
            assert_eq!(fiber.state(), FiberState::Disposed);
            assert_eq!(root.registry().size(), 0);
        });
    });
}
