//! Lifecycle serialization (R4) and cancel-safe disposal (R5).

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

use serde_json::Value;
use zeokeel_cordis::{
    block_on, cleanup, cleanup_async, AsyncCleanup, AsyncDisposer, Cleanup, Context, CordisError,
    FiberState, Plugin,
};

type Trace = Arc<Mutex<Vec<&'static str>>>;

fn trace() -> Trace {
    Arc::new(Mutex::new(Vec::new()))
}

fn snapshot(trace: &Trace) -> Vec<&'static str> {
    trace.lock().unwrap().clone()
}

/// Run `body` on a watchdog thread so a deadlock fails the test instead of
/// hanging the harness.
fn with_watchdog<T, F>(label: &'static str, body: F) -> T
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let _ = tx.send(body());
    });
    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(value) => {
            let _ = handle.join();
            value
        }
        Err(_) => panic!("WATCHDOG: {label} hung (deadlock)"),
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::noop();
    let mut cx = TaskContext::from_waker(waker);
    future.poll(&mut cx)
}

// ---------------------------------------------------------------------------
// R4: concurrent lifecycle operations are exclusive
// ---------------------------------------------------------------------------

struct BlockingCleanup {
    started: Arc<AtomicUsize>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
}

impl Plugin for BlockingCleanup {
    fn name(&self) -> &'static str {
        "blocking-cleanup"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        let started = self.started.clone();
        let release = self.release.clone();
        let fiber = ctx.fiber();
        ctx.effect(move || {
            vec![cleanup(move || {
                started.fetch_add(1, Ordering::SeqCst);
                // Reentrant dispose from inside cleanup must be allowed.
                let _ = fiber.dispose();
                let rx = release.lock().unwrap();
                let _ = rx.recv_timeout(Duration::from_secs(2));
            })]
        })?;
        Ok(())
    }
}

fn blocking_plugin() -> (Arc<BlockingCleanup>, mpsc::Sender<()>) {
    let (release_tx, release_rx) = mpsc::channel();
    let started = Arc::new(AtomicUsize::new(0));
    (
        Arc::new(BlockingCleanup {
            started,
            release: Arc::new(Mutex::new(release_rx)),
        }),
        release_tx,
    )
}

#[test]
fn concurrent_dispose_returns_lifecycle_busy() {
    let root = Context::root();
    let (plugin, release) = blocking_plugin();
    let started = plugin.started.clone();
    let fiber = root.plugin(plugin, Value::Null).expect("plugin");

    let disposer = fiber.clone();
    let owner = std::thread::spawn(move || disposer.dispose());

    // Wait until the owning thread is inside the cleanup.
    while started.load(Ordering::SeqCst) == 0 {
        std::thread::sleep(Duration::from_millis(2));
    }
    // A concurrent dispose from another thread must fail safely.
    match fiber.dispose() {
        Err(CordisError::LifecycleBusy) => {}
        other => panic!("expected LifecycleBusy, got {other:?}"),
    }
    match fiber.restart() {
        Err(CordisError::LifecycleBusy) => {}
        other => panic!("expected LifecycleBusy from restart, got {other:?}"),
    }

    release.send(()).expect("release");
    owner.join().expect("owner thread").expect("dispose");
    assert_eq!(fiber.state(), FiberState::Disposed);
    // Idempotent dispose after completion is still Ok.
    fiber.dispose().expect("idempotent dispose");
}

#[test]
fn reentrant_lifecycle_calls_are_allowed() {
    let root = Context::root();
    let (plugin, release) = blocking_plugin();
    let fiber = root.plugin(plugin, Value::Null).expect("plugin");
    // The cleanup disposes its own fiber (reentrant, same thread).
    let dispose_fiber = fiber.clone();
    let result = with_watchdog("reentrant dispose", move || dispose_fiber.dispose());
    release.send(()).ok();
    result.expect("reentrant dispose must not deadlock");
    assert_eq!(fiber.state(), FiberState::Disposed);
}

// ---------------------------------------------------------------------------
// R5: cancelling a disposal restores unstarted cleanups
// ---------------------------------------------------------------------------

#[test]
fn cancelling_dispose_async_restores_unstarted_effects() {
    let root = Context::root();
    let trace = trace();
    let first = trace.clone();
    let second = trace.clone();
    let fiber = root
        .plugin(
            Arc::new(EffectsPlugin {
                sync: vec![Box::new(move || {
                    let first = first.clone();
                    vec![cleanup(move || first.lock().unwrap().push("first"))]
                })],
                asynchronous: vec![Box::new(move || {
                    let second = second.clone();
                    vec![cleanup_async(move || {
                        let second = second.clone();
                        async move {
                            // Cancelled while awaiting this cleanup.
                            let () = std::future::pending().await;
                            second.lock().unwrap().push("second");
                        }
                    })]
                })],
            }),
            Value::Null,
        )
        .expect("plugin");

    // Poll once: reverse order starts with the async effect (which hangs).
    let mut future = Box::pin(fiber.dispose_async());
    assert!(poll_once(future.as_mut()).is_pending());
    drop(future);
    // A-9: a cancelled `dispose_async` completes the teardown synchronously
    // instead of leaving the fiber `Unloading`: the not-yet-started sync cleanup
    // runs inline and the fiber reaches `Disposed`; the cleanup future that was
    // already in flight cannot be recovered (documented bound).
    assert_eq!(fiber.state(), FiberState::Disposed);
    assert_eq!(snapshot(&trace), vec!["first"]);

    // A retried dispose is idempotent and drains any deferred cleanup.
    block_on(fiber.dispose_async()).expect("retry dispose");
    assert_eq!(fiber.state(), FiberState::Disposed);
    assert_eq!(snapshot(&trace), vec!["first"]);
}

#[test]
fn cancelling_async_disposer_restores_unstarted_cleanups() {
    let trace = trace();
    let first = trace.clone();
    let second = trace.clone();
    let disposer = AsyncDisposer::new(vec![
        cleanup_async(move || {
            let first = first.clone();
            async move {
                first.lock().unwrap().push("first");
            }
        }),
        cleanup_async(move || {
            let second = second.clone();
            async move {
                let () = std::future::pending().await;
                second.lock().unwrap().push("second");
            }
        }),
    ]);

    let mut future = Box::pin(disposer.dispose());
    assert!(poll_once(future.as_mut()).is_pending());
    drop(future);

    // The unstarted cleanup is restored and runs on the next attempt.
    block_on(disposer.dispose());
    assert_eq!(snapshot(&trace), vec!["first"]);
}

struct EffectsPlugin {
    sync: Vec<Box<dyn Fn() -> Vec<Cleanup> + Send + Sync>>,
    asynchronous: Vec<Box<dyn Fn() -> Vec<AsyncCleanup> + Send + Sync>>,
}

impl Plugin for EffectsPlugin {
    fn name(&self) -> &'static str {
        "effects"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        for effect in &self.sync {
            ctx.effect(effect)?;
        }
        for effect in &self.asynchronous {
            ctx.effect_async(effect)?;
        }
        Ok(())
    }
}
