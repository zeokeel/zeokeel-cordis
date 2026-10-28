//! Round-6 adversarial attacks on CORE-049 (quiescence) and CORE-050
//! (cooperative cancellation). Each test asserts the round-6 brief semantics
//! and runs under a watchdog thread so a kernel hang fails the test.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use serde_json::Value;
use zeokeel_cordis::{
    block_on, cleanup, cleanup_async, AsyncPlugin, BoxFuture, Context, CordisError, EventResult,
    Fiber, FiberState, Plugin, ServiceKey,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
        Err(_) => panic!("WATCHDOG: `{label}` hung for {timeout:?}"),
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::noop();
    let mut cx = TaskContext::from_waker(waker);
    future.poll(&mut cx)
}

fn multi_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime")
}

/// Waker that only counts wakeups (used to observe retained waiters).
struct CountingWake(Arc<AtomicUsize>);

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Waker that polls its stored future inline from `wake()` — it reproduces the
/// exact "woken → re-check → re-register" sequence an executor performs when a
/// `notify_idle` happens to hit an already-registered waiter.
struct InlineWaker {
    future: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    done: Arc<AtomicUsize>,
    polls: Arc<AtomicUsize>,
}

impl Wake for InlineWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let waker = Waker::from(Arc::clone(self));
        let mut cx = TaskContext::from_waker(&waker);
        let mut slot = self.future.lock().unwrap().take();
        if let Some(mut future) = slot.take() {
            self.polls.fetch_add(1, Ordering::SeqCst);
            if future.as_mut().poll(&mut cx).is_ready() {
                self.done.fetch_add(1, Ordering::SeqCst);
            } else {
                *self.future.lock().unwrap() = Some(future);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Plugins
// ---------------------------------------------------------------------------

struct GatedWork {
    label: &'static str,
    started: Arc<AtomicUsize>,
    release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
}

impl Plugin for GatedWork {
    fn name(&self) -> &'static str {
        self.label
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        let started = self.started.clone();
        let release = self.release.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                started.fetch_add(1, Ordering::SeqCst);
                let rx = release.lock().unwrap().take();
                if let Some(rx) = rx {
                    let _ = rx.recv_timeout(Duration::from_secs(5));
                }
            })]
        })?;
        Ok(())
    }
}

/// Plugin whose activation blocks until released (used for slow-activation waits).
struct GatedApply {
    label: &'static str,
    started: Arc<AtomicUsize>,
    release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
}

impl Plugin for GatedApply {
    fn name(&self) -> &'static str {
        self.label
    }

    fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let rx = self.release.lock().unwrap().take();
        if let Some(rx) = rx {
            let _ = rx.recv_timeout(Duration::from_secs(5));
        }
        Ok(())
    }
}

struct Capturing {
    cell: Arc<Mutex<Option<(Context, Fiber)>>>,
}

impl Plugin for Capturing {
    fn name(&self) -> &'static str {
        "r7-capturing"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        *self.cell.lock().unwrap() = Some((ctx.clone(), ctx.fiber()));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 1. when_idle correctness (passing cases)
// ---------------------------------------------------------------------------

/// A waiter registered while a slow sync activation is in flight does not
/// resolve early and resolves once the activation settled.
#[test]
fn t01_when_idle_waits_for_slow_activation() {
    watchdog("t01", Duration::from_secs(20), || {
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let plugin = Arc::new(GatedApply {
            label: "r7-slow",
            started: started.clone(),
            release: Arc::new(Mutex::new(Some(release_rx))),
        });
        let root_for_activation = root.clone();
        let activation =
            std::thread::spawn(move || root_for_activation.plugin(plugin, Value::Null));
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let ctx = root.clone();
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            block_on(ctx.when_idle());
            let _ = done_tx.send(());
        });
        std::thread::sleep(Duration::from_millis(20));
        assert!(
            done_rx.try_recv().is_err(),
            "when_idle resolved while the activation was still in flight"
        );
        release_tx.send(()).expect("release");
        assert!(
            done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "when_idle did not resolve after the activation finished"
        );
        let fiber = activation.join().expect("activation").expect("plugin");
        assert_eq!(fiber.state(), FiberState::Active);
        assert!(root.registry().is_idle());
        let _ = waiter.join();
    });
}

/// `when_idle` waits for a slow async activation.
#[test]
fn t02_when_idle_waits_for_async_activation() {
    watchdog("t02", Duration::from_secs(20), || {
        struct AsyncGated {
            started: Arc<AtomicUsize>,
            release: Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
        }
        impl AsyncPlugin for AsyncGated {
            fn name(&self) -> &'static str {
                "r7-async-slow"
            }
            fn apply_async<'a>(
                &'a self,
                _ctx: Context,
                _config: Value,
            ) -> BoxFuture<'a, zeokeel_cordis::Result<()>> {
                let started = self.started.clone();
                let release = self.release.lock().unwrap().take();
                Box::pin(async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    if let Some(rx) = release {
                        let _ = rx.await;
                    }
                    Ok(())
                })
            }
        }
        let runtime = multi_runtime();
        runtime.block_on(async {
            let root = Context::root();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
            let started = Arc::new(AtomicUsize::new(0));
            let task_started = started.clone();
            let root_for_activation = root.clone();
            let activation = tokio::spawn(async move {
                root_for_activation
                    .plugin_async(
                        Arc::new(AsyncGated {
                            started: task_started,
                            release: Arc::new(Mutex::new(Some(release_rx))),
                        }),
                        Value::Null,
                    )
                    .await
                    .map(|_| ())
            });
            while started.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let ctx = root.clone();
            let waiter = tokio::spawn(async move { ctx.when_idle().await });
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(
                !waiter.is_finished(),
                "when_idle resolved during async activation"
            );
            let _ = release_tx.send(());
            let _ = tokio::time::timeout(Duration::from_secs(5), waiter)
                .await
                .expect("when_idle must resolve after the async activation");
            let _ = activation.await;
            assert!(root.registry().is_idle());
        });
    });
}

/// `when_idle` waits for a restart in flight.
#[test]
fn t03_when_idle_waits_for_restart() {
    watchdog("t03", Duration::from_secs(20), || {
        struct Slow {
            started: Arc<AtomicUsize>,
            release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
            applies: Arc<AtomicUsize>,
        }
        impl Plugin for Slow {
            fn name(&self) -> &'static str {
                "r7-restart-slow"
            }
            fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                let call = self.applies.fetch_add(1, Ordering::SeqCst);
                if call >= 1 {
                    self.started.fetch_add(1, Ordering::SeqCst);
                    let rx = self.release.lock().unwrap().take();
                    if let Some(rx) = rx {
                        let _ = rx.recv_timeout(Duration::from_secs(5));
                    }
                }
                Ok(())
            }
        }
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let applies = Arc::new(AtomicUsize::new(0));
        let fiber = root
            .plugin(
                Arc::new(Slow {
                    started: started.clone(),
                    release: Arc::new(Mutex::new(Some(release_rx))),
                    applies: applies.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let restarting = fiber.clone();
        let restarter = std::thread::spawn(move || restarting.restart());
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let ctx = root.clone();
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            block_on(ctx.when_idle());
            let _ = done_tx.send(());
        });
        std::thread::sleep(Duration::from_millis(20));
        assert!(
            done_rx.try_recv().is_err(),
            "when_idle resolved during restart"
        );
        release_tx.send(()).expect("release");
        assert!(done_rx.recv_timeout(Duration::from_secs(5)).is_ok());
        restarter.join().expect("restart thread").expect("restart");
        assert_eq!(fiber.state(), FiberState::Active);
        let _ = waiter.join();
    });
}

/// Failed / Pending / Disposed fibers are idle; repeated waits after everything
/// is disposed are immediate.
#[test]
fn t04_idle_with_terminal_and_pending_fibers() {
    watchdog("t04", Duration::from_secs(20), || {
        struct PendingChild;
        impl Plugin for PendingChild {
            fn name(&self) -> &'static str {
                "r7-pending"
            }
            fn inject(&self) -> &'static [&'static str] {
                &["r7.missing"]
            }
            fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                Ok(())
            }
        }
        struct Failing;
        impl Plugin for Failing {
            fn name(&self) -> &'static str {
                "r7-failing"
            }
            fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                Err(CordisError::InvalidPlugin("boom".into()))
            }
        }
        let root = Context::root();
        let pending = root.plugin(Arc::new(PendingChild), Value::Null).unwrap();
        let failed = root.plugin(Arc::new(Failing), Value::Null).unwrap();
        assert_eq!(pending.state(), FiberState::Pending);
        assert_eq!(failed.state(), FiberState::Failed);
        let started = Instant::now();
        block_on(root.when_idle());
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(root.registry().is_idle());
        let disposed = root.plugin(Arc::new(Failing), Value::Null).unwrap();
        disposed.dispose().unwrap();
        for _ in 0..100 {
            block_on(root.when_idle());
        }
        assert!(root.registry().is_idle());
    });
}

/// Concurrent waiters are all woken when the activation settles.
#[test]
fn t05_concurrent_waiters_all_wake() {
    watchdog("t05", Duration::from_secs(20), || {
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let plugin = Arc::new(GatedApply {
            label: "r7-multi-waiters",
            started: started.clone(),
            release: Arc::new(Mutex::new(Some(release_rx))),
        });
        let root_for_activation = root.clone();
        let activation =
            std::thread::spawn(move || root_for_activation.plugin(plugin, Value::Null));
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let (done_tx, done_rx) = mpsc::channel();
        let mut waiters = Vec::new();
        for _ in 0..8 {
            let ctx = root.clone();
            let done_tx = done_tx.clone();
            waiters.push(std::thread::spawn(move || {
                block_on(ctx.when_idle());
                let _ = done_tx.send(());
            }));
        }
        std::thread::sleep(Duration::from_millis(20));
        release_tx.send(()).expect("release");
        let mut woken = 0;
        for _ in 0..8 {
            if done_rx.recv_timeout(Duration::from_secs(5)).is_ok() {
                woken += 1;
            }
        }
        assert_eq!(woken, 8, "all concurrent when_idle callers must resolve");
        let _ = activation.join().unwrap().unwrap();
        for waiter in waiters {
            let _ = waiter.join();
        }
    });
}

/// Dropping a waiter future must not hang the kernel; later waits still work.
#[test]
fn t06_dropped_waiter_does_not_hang() {
    watchdog("t06", Duration::from_secs(20), || {
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let plugin = Arc::new(GatedApply {
            label: "r7-dropped-waiter",
            started: started.clone(),
            release: Arc::new(Mutex::new(Some(release_rx))),
        });
        let root_for_activation = root.clone();
        let activation =
            std::thread::spawn(move || root_for_activation.plugin(plugin, Value::Null));
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let ctx = root.clone();
        for _ in 0..100 {
            let mut future = Box::pin(ctx.when_idle());
            let _ = poll_once(future.as_mut());
            drop(future);
        }
        release_tx.send(()).expect("release");
        let _ = activation.join().unwrap().unwrap();
        block_on(root.when_idle());
        assert!(root.registry().is_idle());
    });
}

/// `is_idle`/`when_idle` stay consistent under 150 slow-activation races.
#[test]
fn t07_is_idle_when_idle_stress() {
    // 150 sequential boot/dispose cycles: generous watchdog so CPU contention
    // from other cargo jobs cannot masquerade as a kernel hang.
    watchdog("t07", Duration::from_secs(180), || {
        struct Sleepy;
        impl Plugin for Sleepy {
            fn name(&self) -> &'static str {
                "r7-sleepy"
            }
            fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                std::thread::sleep(Duration::from_millis(1));
                Ok(())
            }
        }
        let root = Context::root();
        for iteration in 0..150 {
            let root_for_activation = root.clone();
            let activation = std::thread::spawn(move || {
                root_for_activation.plugin(Arc::new(Sleepy), Value::Null)
            });
            // The activation may complete before this observer sees it become
            // non-idle (a 1ms window under CPU load); bound the spin instead of
            // spinning forever, then let when_idle/join observe the outcome.
            let spin_deadline = std::time::Instant::now() + Duration::from_secs(5);
            while root.registry().is_idle() && std::time::Instant::now() < spin_deadline {
                std::thread::sleep(Duration::from_micros(50));
            }
            block_on(root.when_idle());
            let fiber = activation.join().unwrap().expect("plugin");
            assert_eq!(fiber.state(), FiberState::Active, "iteration {iteration}");
            fiber.dispose().unwrap();
            block_on(root.when_idle());
            assert!(root.registry().is_idle());
        }
    });
}

/// Inject-gate flapping while `when_idle` is awaited terminates.
#[test]
fn t08_gate_flapping_terminates() {
    watchdog("t08", Duration::from_secs(30), || {
        static KEY: ServiceKey<u32> = ServiceKey::new("r7.gated");
        struct Dependent;
        impl Plugin for Dependent {
            fn name(&self) -> &'static str {
                "r7-gate-dependent"
            }
            fn inject(&self) -> &'static [&'static str] {
                &["r7.gated"]
            }
            fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                Ok(())
            }
        }
        let root = Context::root();
        let gate = zeokeel_cordis::ServiceGate::new(false);
        let _provided = root
            .provide_gated(KEY, Arc::new(1u32), &gate)
            .expect("provide gated");
        let dependent = root.plugin(Arc::new(Dependent), Value::Null).unwrap();
        let flapper = std::thread::spawn(move || {
            for round in 0..300 {
                gate.set(round % 2 == 0);
                std::thread::sleep(Duration::from_micros(100));
            }
            gate.set(false);
        });
        let started = Instant::now();
        let mut waits = 0usize;
        while !flapper.is_finished() && started.elapsed() < Duration::from_secs(5) {
            block_on(root.when_idle());
            waits += 1;
        }
        flapper.join().expect("flapper");
        block_on(root.when_idle());
        assert!(root.registry().is_idle());
        assert_eq!(dependent.state(), FiberState::Pending);
        assert!(waits > 0);
    });
}

// ---------------------------------------------------------------------------
// 2. Cooperative cancellation (passing cases)
// ---------------------------------------------------------------------------

/// `checkpoint` observes the flag before/after `cancel`; cancel is idempotent.
#[test]
fn t09_checkpoint_before_and_after_cancel() {
    watchdog("t09", Duration::from_secs(20), || {
        let root = Context::root();
        let cell = Arc::new(Mutex::new(None));
        let fiber = root
            .plugin(Arc::new(Capturing { cell: cell.clone() }), Value::Null)
            .unwrap();
        let (ctx, handle) = cell.lock().unwrap().clone().expect("captured");
        assert!(ctx.checkpoint().is_ok());
        assert!(!handle.is_cancelled());
        handle.cancel();
        handle.cancel();
        assert!(handle.is_cancelled());
        assert!(matches!(ctx.checkpoint(), Err(CordisError::Cancelled)));
        assert_eq!(
            fiber.state(),
            FiberState::Active,
            "cancel is cooperative only"
        );
    });
}

/// Cancelling during a slow checkpointing activation fails the activation and
/// rolls back its registrations; `when_idle` resolves afterwards.
#[test]
fn t10_cancel_slow_sync_apply_rolls_back() {
    watchdog("t10", Duration::from_secs(20), || {
        static SVC: ServiceKey<u32> = ServiceKey::new("r7.cancel.svc");
        struct Looper {
            started: Arc<AtomicUsize>,
            cleanups: Arc<AtomicUsize>,
        }
        impl Plugin for Looper {
            fn name(&self) -> &'static str {
                "r7-looper"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                ctx.provide(SVC, Arc::new(1u32))?;
                ctx.on("r7.cancel.event", |_args| EventResult::Continue)?;
                let cleanups = self.cleanups.clone();
                ctx.effect(move || {
                    vec![cleanup(move || {
                        cleanups.fetch_add(1, Ordering::SeqCst);
                    })]
                })?;
                self.started.fetch_add(1, Ordering::SeqCst);
                loop {
                    ctx.checkpoint()?;
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        let root = Context::root();
        let started = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let root_for_activation = root.clone();
        let plugin = Arc::new(Looper {
            started: started.clone(),
            cleanups: cleanups.clone(),
        });
        let activation =
            std::thread::spawn(move || root_for_activation.plugin(plugin, Value::Null));
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let fiber = root
            .registry()
            .fibers()
            .into_iter()
            .find(|fiber| fiber.name() == "r7-looper")
            .expect("fiber");
        fiber.cancel();
        let created = activation
            .join()
            .expect("activation thread")
            .expect("plugin creation");
        assert_eq!(created.state(), FiberState::Failed);
        assert_eq!(
            created.error().as_deref(),
            Some("operation cancelled"),
            "cancellation must surface through the failure path"
        );
        assert_eq!(root.get(SVC), None, "service rolled back");
        assert_eq!(root.events().listener_count(), 0, "listener rolled back");
        assert_eq!(cleanups.load(Ordering::SeqCst), 1, "effect rolled back");
        block_on(root.when_idle());
        assert!(root.registry().is_idle());
    });
}

/// Cancelling an async plugin after its await is observed by a later checkpoint
/// and rolls the activation back.
#[test]
fn t11_cancel_async_plugin_after_await_rolls_back() {
    watchdog("t11", Duration::from_secs(20), || {
        static SVC: ServiceKey<u32> = ServiceKey::new("r7.async.cancel.svc");
        struct AsyncLooper {
            started: Arc<AtomicUsize>,
            release: Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
            cleanups: Arc<AtomicUsize>,
        }
        impl AsyncPlugin for AsyncLooper {
            fn name(&self) -> &'static str {
                "r7-async-looper"
            }
            fn apply_async<'a>(
                &'a self,
                ctx: Context,
                _config: Value,
            ) -> BoxFuture<'a, zeokeel_cordis::Result<()>> {
                let started = self.started.clone();
                let release = self.release.lock().unwrap().take();
                let cleanups = self.cleanups.clone();
                Box::pin(async move {
                    ctx.provide(SVC, Arc::new(2u32))?;
                    let cleanups = cleanups.clone();
                    ctx.effect(move || {
                        vec![cleanup(move || {
                            cleanups.fetch_add(1, Ordering::SeqCst);
                        })]
                    })?;
                    started.fetch_add(1, Ordering::SeqCst);
                    if let Some(rx) = release {
                        let _ = rx.await;
                    }
                    ctx.checkpoint()?;
                    Ok(())
                })
            }
        }
        let runtime = multi_runtime();
        runtime.block_on(async {
            let root = Context::root();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
            let started = Arc::new(AtomicUsize::new(0));
            let cleanups = Arc::new(AtomicUsize::new(0));
            let root_for_activation = root.clone();
            let plugin = Arc::new(AsyncLooper {
                started: started.clone(),
                release: Arc::new(Mutex::new(Some(release_rx))),
                cleanups: cleanups.clone(),
            });
            let activation = tokio::spawn(async move {
                root_for_activation
                    .plugin_async(plugin, Value::Null)
                    .await
                    .map(|_| ())
            });
            while started.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let fiber = root
                .registry()
                .fibers()
                .into_iter()
                .find(|fiber| fiber.name() == "r7-async-looper")
                .expect("fiber");
            fiber.cancel();
            let _ = release_tx.send(());
            let _ = tokio::time::timeout(Duration::from_secs(5), activation)
                .await
                .expect("activation must settle");
            assert_eq!(fiber.state(), FiberState::Failed);
            assert_eq!(root.get(SVC), None);
            assert_eq!(cleanups.load(Ordering::SeqCst), 1);
        });
    });
}

/// Cancelling a Pending fiber before its first activation makes the eventual
/// activation fail (checkpoint) and roll back.
#[test]
fn t12_cancel_pending_fiber_before_activation() {
    watchdog("t12", Duration::from_secs(20), || {
        static SVC: ServiceKey<u32> = ServiceKey::new("r7.pending.cancel.svc");
        struct Needs {
            started: Arc<AtomicUsize>,
            cleanups: Arc<AtomicUsize>,
        }
        impl Plugin for Needs {
            fn name(&self) -> &'static str {
                "r7-pending-cancel"
            }
            fn inject(&self) -> &'static [&'static str] {
                &["r7.pending.dep"]
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                ctx.provide(SVC, Arc::new(1u32))?;
                let cleanups = self.cleanups.clone();
                ctx.effect(move || {
                    vec![cleanup(move || {
                        cleanups.fetch_add(1, Ordering::SeqCst);
                    })]
                })?;
                self.started.fetch_add(1, Ordering::SeqCst);
                ctx.checkpoint()?;
                Ok(())
            }
        }
        let root = Context::root();
        let started = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let fiber = root
            .plugin(
                Arc::new(Needs {
                    started: started.clone(),
                    cleanups: cleanups.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(fiber.state(), FiberState::Pending);
        fiber.cancel();
        let dependency: ServiceKey<u32> = ServiceKey::new("r7.pending.dep");
        let _provided = root.provide(dependency, Arc::new(1u32)).expect("provide");
        root.refresh();
        assert_eq!(fiber.state(), FiberState::Failed);
        assert_eq!(root.get(SVC), None);
        assert_eq!(cleanups.load(Ordering::SeqCst), 1);
    });
}

/// Cancellation is per-fiber: siblings and the root context stay un-cancelled.
#[test]
fn t13_cancel_is_per_fiber() {
    watchdog("t13", Duration::from_secs(20), || {
        let root = Context::root();
        let cell_a = Arc::new(Mutex::new(None));
        let cell_b = Arc::new(Mutex::new(None));
        let a = root
            .plugin(
                Arc::new(Capturing {
                    cell: cell_a.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let b = root
            .plugin(
                Arc::new(Capturing {
                    cell: cell_b.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let ctx_a = cell_a.lock().unwrap().clone().unwrap().0;
        let ctx_b = cell_b.lock().unwrap().clone().unwrap().0;
        let root_ctx = root.fiber().context().expect("root context");
        a.cancel();
        assert!(matches!(ctx_a.checkpoint(), Err(CordisError::Cancelled)));
        assert!(ctx_b.checkpoint().is_ok(), "sibling must not be cancelled");
        assert!(root_ctx.checkpoint().is_ok(), "root must not be cancelled");
        assert!(!b.is_cancelled());
        assert!(!root.fiber().is_cancelled());
    });
}

/// `checkpoint` is cheap enough for a hot loop and does not grow state.
#[test]
fn t14_checkpoint_hot_loop() {
    watchdog("t14", Duration::from_secs(20), || {
        let root = Context::root();
        let cell = Arc::new(Mutex::new(None));
        let fiber = root
            .plugin(Arc::new(Capturing { cell: cell.clone() }), Value::Null)
            .unwrap();
        let ctx = cell.lock().unwrap().clone().unwrap().0;
        let effects_before = fiber.get_effects().len();
        let started = Instant::now();
        let mut ok = 0usize;
        for _ in 0..200_000 {
            if ctx.checkpoint().is_ok() {
                ok += 1;
            }
        }
        let elapsed = started.elapsed();
        assert_eq!(ok, 200_000);
        assert!(
            elapsed < Duration::from_secs(5),
            "checkpoint hot loop too slow: {elapsed:?}"
        );
        assert_eq!(fiber.get_effects().len(), effects_before);
        assert!(root.registry().is_idle());
        fiber.cancel();
        assert!(matches!(ctx.checkpoint(), Err(CordisError::Cancelled)));
    });
}

/// Cancel + dispose interplay: no deadlock, final Disposed, checkpoint flag
/// remains observable on the handle after disposal.
#[test]
fn t15_cancel_dispose_interplay() {
    watchdog("t15", Duration::from_secs(20), || {
        let root = Context::root();
        let cell = Arc::new(Mutex::new(None));
        let fiber = root
            .plugin(Arc::new(Capturing { cell: cell.clone() }), Value::Null)
            .unwrap();
        let ctx = cell.lock().unwrap().clone().unwrap().0;
        fiber.cancel();
        let disposing = fiber.clone();
        let thread = std::thread::spawn(move || disposing.dispose());
        thread.join().expect("dispose thread").expect("dispose");
        assert_eq!(fiber.state(), FiberState::Disposed);
        fiber.cancel();
        assert!(fiber.is_cancelled());
        assert!(matches!(ctx.checkpoint(), Err(CordisError::Cancelled)));
        block_on(root.when_idle());
        assert!(root.registry().is_idle());
        assert_eq!(root.registry().size(), 0);
    });
}

/// Cancel during a slow checkpointing activation while another thread disposes
/// the fiber: no deadlock, final Disposed, rollback clean.
#[test]
fn t16_cancel_and_concurrent_dispose_is_safe() {
    watchdog("t16", Duration::from_secs(20), || {
        static SVC: ServiceKey<u32> = ServiceKey::new("r7.cancel.dispose.svc");
        struct Looper {
            started: Arc<AtomicUsize>,
            cleanups: Arc<AtomicUsize>,
        }
        impl Plugin for Looper {
            fn name(&self) -> &'static str {
                "r7-cancel-dispose-looper"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                ctx.provide(SVC, Arc::new(1u32))?;
                let cleanups = self.cleanups.clone();
                ctx.effect(move || {
                    vec![cleanup(move || {
                        cleanups.fetch_add(1, Ordering::SeqCst);
                    })]
                })?;
                self.started.fetch_add(1, Ordering::SeqCst);
                loop {
                    ctx.checkpoint()?;
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        let root = Context::root();
        let started = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let root_for_activation = root.clone();
        let plugin = Arc::new(Looper {
            started: started.clone(),
            cleanups: cleanups.clone(),
        });
        let activation =
            std::thread::spawn(move || root_for_activation.plugin(plugin, Value::Null));
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let fiber = root
            .registry()
            .fibers()
            .into_iter()
            .find(|fiber| fiber.name() == "r7-cancel-dispose-looper")
            .expect("fiber");
        fiber.cancel();
        let disposing = fiber.clone();
        let dispose = std::thread::spawn(move || disposing.dispose());
        let _ = activation.join().expect("activation thread");
        let _ = dispose.join().expect("dispose thread");
        assert!(
            matches!(fiber.state(), FiberState::Disposed | FiberState::Failed),
            "final state must be terminal, got {:?}",
            fiber.state()
        );
        assert_eq!(root.get(SVC), None);
        assert_eq!(cleanups.load(Ordering::SeqCst), 1);
        block_on(root.when_idle());
    });
}

// ---------------------------------------------------------------------------
// 3. Failing attacks
// ---------------------------------------------------------------------------

/// A waiter registered while a dispose is in flight must be woken when the
/// operation finally completes (pending_ops reaches 0), even for a Disposed
/// fiber whose ctx link was already cleared.
#[test]
fn f01_when_idle_waiter_is_woken_after_dispose_completes() {
    watchdog("f01", Duration::from_secs(20), || {
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let fiber = root
            .plugin(
                Arc::new(GatedWork {
                    label: "r7-f01",
                    started: started.clone(),
                    release: Arc::new(Mutex::new(Some(release_rx))),
                }),
                Value::Null,
            )
            .unwrap();
        let disposing = fiber.clone();
        let dispose_thread = std::thread::spawn(move || disposing.dispose());
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        // Register a waiter exactly as an executor would: poll it, then let
        // notifications poll it inline.
        let ctx = root.clone();
        let done = Arc::new(AtomicUsize::new(0));
        let polls = Arc::new(AtomicUsize::new(0));
        let state = Arc::new(InlineWaker {
            future: Mutex::new(Some(Box::pin(async move { ctx.when_idle().await }))),
            done: done.clone(),
            polls: polls.clone(),
        });
        let waker = Waker::from(Arc::clone(&state));
        let mut task_ctx = TaskContext::from_waker(&waker);
        let mut future = state.future.lock().unwrap().take().expect("future");
        assert!(
            future.as_mut().poll(&mut task_ctx).is_pending(),
            "waiter must not be ready while disposing"
        );
        state.future.lock().unwrap().replace(future);
        release_tx.send(()).expect("release cleanup");
        dispose_thread
            .join()
            .expect("dispose thread")
            .expect("dispose");
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert!(root.registry().is_idle());
        assert!(
            done.load(Ordering::SeqCst) > 0,
            "MISSED-WAKEUP: when_idle stayed pending although the registry is idle \
             after the disposal (polls={}, pending_future={})",
            polls.load(Ordering::SeqCst),
            state.future.lock().unwrap().is_some()
        );
        let _ = state.future.lock().unwrap().take();
    });
}

/// Dropping a `when_idle` future before the registry becomes idle must not
/// leave its waker registered: the dropped waiter must not be woken later.
#[test]
fn f02_dropped_waiter_is_not_retained() {
    watchdog("f02", Duration::from_secs(20), || {
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let plugin = Arc::new(GatedApply {
            label: "r7-f02",
            started: started.clone(),
            release: Arc::new(Mutex::new(Some(release_rx))),
        });
        let root_for_activation = root.clone();
        let activation =
            std::thread::spawn(move || root_for_activation.plugin(plugin, Value::Null));
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountingWake(wakes.clone())));
        let mut task_ctx = TaskContext::from_waker(&waker);
        for _ in 0..100 {
            let mut future = Box::pin(root.when_idle());
            assert!(future.as_mut().poll(&mut task_ctx).is_pending());
            drop(future);
        }
        release_tx.send(()).expect("release");
        let _ = activation.join().unwrap().unwrap();
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            0,
            "WAITER-LEAK: {} wakers of dropped when_idle futures were retained and \
             woken later",
            wakes.load(Ordering::SeqCst)
        );
    });
}

/// `when_idle` must include the root fiber's own lifecycle work: disposing the
/// root fiber is a lifecycle operation in flight too.
#[test]
fn f03_when_idle_includes_root_fiber_lifecycle() {
    watchdog("f03", Duration::from_secs(20), || {
        let runtime = multi_runtime();
        runtime.block_on(async {
            let root = Context::root();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
            let (started_tx, started_rx) = mpsc::channel::<()>();
            let started = Arc::new(AtomicUsize::new(0));
            let release = Arc::new(Mutex::new(Some(release_rx)));
            root.effect_async(move || {
                let release = release.clone();
                let started = started.clone();
                let started_tx = started_tx.clone();
                vec![cleanup_async(move || {
                    let rx = release.lock().unwrap().take();
                    let started = started.clone();
                    let started_tx = started_tx.clone();
                    async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        let _ = started_tx.send(());
                        if let Some(rx) = rx {
                            let _ = rx.await;
                        }
                    }
                })]
            })
            .expect("root effect");
            let root_for_dispose = root.clone();
            let dispose =
                tokio::spawn(async move { root_for_dispose.fiber().dispose_async().await });
            started_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("root cleanup started");
            let when = tokio::time::timeout(Duration::from_millis(150), root.when_idle()).await;
            let resolved_early = when.is_ok();
            let _ = release_tx.send(());
            let _ = dispose.await;
            assert!(
                !resolved_early,
                "ROOT-QUIESCENCE: when_idle resolved while the root fiber disposal was \
                 still in flight (root state was Unloading)"
            );
        });
    });
}
