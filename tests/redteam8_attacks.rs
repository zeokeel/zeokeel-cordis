//! Round-7 adversarial attacks: single scope-id namespace (R17), quiescence /
//! cancellation edges after the round-6 fixes, and residual-leak reclamation.
//! Every test runs under a watchdog.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context as TaskContext, Wake, Waker};
use std::time::Duration;

use cordis::{
    block_on, cleanup, cleanup_async, AsyncPlugin, BoxFuture, Context, Fiber, FiberState, Plugin,
    ServiceKey,
};
use serde_json::Value;

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

fn multi_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime")
}

struct CountingWake(Arc<AtomicUsize>);

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

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

struct Capture {
    cell: Arc<Mutex<Option<(Context, Fiber)>>>,
}

impl Plugin for Capture {
    fn name(&self) -> &'static str {
        "r8-capture"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        *self.cell.lock().unwrap() = Some((ctx.clone(), ctx.fiber()));
        Ok(())
    }
}

struct GatedApply {
    label: &'static str,
    started: Arc<AtomicUsize>,
    release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
}

impl Plugin for GatedApply {
    fn name(&self) -> &'static str {
        self.label
    }

    fn apply(&self, _ctx: Context, _config: Value) -> cordis::Result<()> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let rx = self.release.lock().unwrap().take();
        if let Some(rx) = rx {
            let _ = rx.recv_timeout(Duration::from_secs(5));
        }
        Ok(())
    }
}

struct GatedChild {
    label: &'static str,
    started: Arc<AtomicUsize>,
    release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
    cleanups: Arc<AtomicUsize>,
}

impl Plugin for GatedChild {
    fn name(&self) -> &'static str {
        self.label
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        let started = self.started.clone();
        let release = self.release.clone();
        let cleanups = self.cleanups.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                started.fetch_add(1, Ordering::SeqCst);
                let rx = release.lock().unwrap().take();
                if let Some(rx) = rx {
                    let _ = rx.recv_timeout(Duration::from_secs(5));
                }
                cleanups.fetch_add(1, Ordering::SeqCst);
            })]
        })?;
        Ok(())
    }
}

struct ParentLoads {
    child: Arc<dyn Plugin>,
}

impl Plugin for ParentLoads {
    fn name(&self) -> &'static str {
        "r8-parent"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        ctx.plugin(self.child.clone(), Value::Null)?;
        Ok(())
    }
}

struct AsyncLeaf {
    cleanups: Arc<AtomicUsize>,
}

impl Plugin for AsyncLeaf {
    fn name(&self) -> &'static str {
        "r8-async-leaf"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        let cleanups = self.cleanups.clone();
        ctx.effect_async(move || {
            vec![cleanup_async(move || {
                let cleanups = cleanups.clone();
                async move {
                    cleanups.fetch_add(1, Ordering::SeqCst);
                }
            })]
        })?;
        Ok(())
    }
}

struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// 1. Scope / isolation id semantics (R17)
// ---------------------------------------------------------------------------

static SVC: ServiceKey<u32> = ServiceKey::new("r8.svc");

/// Two contexts with the same label join one service scope (sibling subtrees).
#[test]
fn t01_same_label_joins_scope() {
    watchdog("t01", Duration::from_secs(20), || {
        let root = Context::root();
        let left = root.isolate_labeled("r8.svc", "r8.scope-L");
        let right = root.isolate_labeled("r8.svc", "r8.scope-L");
        left.provide(SVC, Arc::new(41u32)).expect("provide in left");
        assert_eq!(
            right.get(SVC).map(|value| *value),
            Some(41),
            "same label joins"
        );
        assert_eq!(left.get(SVC).map(|value| *value), Some(41));
        let other = root.isolate_labeled("r8.svc", "r8.scope-other");
        assert_eq!(other.get(SVC), None, "different labels never join");
        assert_eq!(root.get(SVC), None, "parent does not see the labeled scope");
    });
}

/// Different labels and fresh `isolate` scopes never join, and a labeled scope
/// never collides with a fresh isolate scope (either order).
#[test]
fn t02_labels_and_fresh_isolates_never_join() {
    watchdog("t02", Duration::from_secs(20), || {
        let root = Context::root();
        let labeled = root.isolate_labeled("r8.svc", "r8.scope-A");
        labeled.provide(SVC, Arc::new(1u32)).unwrap();
        let fresh = root.isolate("r8.svc");
        assert_eq!(
            fresh.get(SVC),
            None,
            "fresh isolate must not see labeled value"
        );
        fresh.provide(SVC, Arc::new(2u32)).unwrap();
        assert_eq!(labeled.get(SVC).map(|value| *value), Some(1));
        assert_eq!(fresh.get(SVC).map(|value| *value), Some(2));
        // fresh first, then labeled
        let root2 = Context::root();
        let fresh2 = root2.isolate("r8.svc");
        fresh2.provide(SVC, Arc::new(3u32)).unwrap();
        let labeled2 = root2.isolate_labeled("r8.svc", "r8.scope-B");
        assert_eq!(
            labeled2.get(SVC),
            None,
            "labeled must not collide with fresh scope"
        );
        labeled2.provide(SVC, Arc::new(4u32)).unwrap();
        assert_eq!(fresh2.get(SVC).map(|value| *value), Some(3));
        assert_eq!(labeled2.get(SVC).map(|value| *value), Some(4));
    });
}

/// Nested isolates and isolating after a labeled scope: each layer gets its own
/// scope and label reuse across subtrees still joins.
#[test]
fn t03_nested_and_isolate_after_labeled() {
    watchdog("t03", Duration::from_secs(20), || {
        let root = Context::root();
        let outer = root.isolate_labeled("r8.svc", "r8.scope-outer");
        outer.provide(SVC, Arc::new(10u32)).unwrap();
        let inner = outer.isolate("r8.svc");
        inner.provide(SVC, Arc::new(20u32)).unwrap();
        assert_eq!(outer.get(SVC).map(|value| *value), Some(10));
        assert_eq!(inner.get(SVC).map(|value| *value), Some(20));
        let reuse = inner.isolate_labeled("r8.svc", "r8.scope-outer");
        assert_eq!(
            reuse.get(SVC).map(|value| *value),
            Some(10),
            "reusing the outer label joins the outer scope again"
        );
        let sibling = root.isolate_labeled("r8.svc", "r8.scope-outer");
        assert_eq!(sibling.get(SVC).map(|value| *value), Some(10));
        let sibling_other = root.isolate_labeled("r8.svc", "r8.scope-other");
        assert_eq!(sibling_other.get(SVC), None);
    });
}

/// 500 labeled scopes interleaved with 500 fresh isolates: the visibility matrix
/// must be exactly one-value-per-labeled-scope and empty for fresh scopes.
#[test]
fn t04_scope_matrix_stress_500_500() {
    watchdog("t04", Duration::from_secs(60), || {
        let root = Context::root();
        let mut labeled_contexts = Vec::new();
        let mut fresh_contexts = Vec::new();
        for index in 0..500u32 {
            let label: &'static str = Box::leak(format!("r8.matrix-{index}").into_boxed_str());
            let labeled = root.isolate_labeled("r8.svc", label);
            labeled
                .provide(SVC, Arc::new(index))
                .expect("unique label scope");
            labeled_contexts.push((labeled, index));
            fresh_contexts.push(root.isolate("r8.svc"));
        }
        for (labeled, index) in &labeled_contexts {
            assert_eq!(
                labeled.get(SVC).map(|value| *value),
                Some(*index),
                "labeled scope {index} must resolve only its own value"
            );
        }
        for (position, fresh) in fresh_contexts.iter().enumerate() {
            assert_eq!(
                fresh.get(SVC),
                None,
                "fresh isolate #{position} must not see any labeled value"
            );
        }
        // same-label sharing at scale
        let shared_a = root.isolate_labeled("r8.svc", "r8.matrix-shared");
        let shared_b = root.isolate_labeled("r8.svc", "r8.matrix-shared");
        shared_a.provide(SVC, Arc::new(999u32)).unwrap();
        assert_eq!(shared_b.get(SVC).map(|value| *value), Some(999));
    });
}

/// Concurrent `isolate_labeled` calls with the same label join the same scope
/// (no duplicate/fresh scope allocation race).
#[test]
fn t05_labeled_scope_concurrent_same_label() {
    watchdog("t05", Duration::from_secs(20), || {
        let root = Context::root();
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let root = root.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                root.isolate_labeled("r8.svc", "r8.concurrent-L")
            }));
        }
        let contexts: Vec<Context> = handles
            .into_iter()
            .map(|handle| handle.join().expect("thread"))
            .collect();
        let provided = contexts[0]
            .provide(SVC, Arc::new(7u32))
            .expect("first provide");
        assert_eq!(contexts[0].get(SVC).map(|value| *value), Some(7));
        for context in &contexts[1..] {
            assert_eq!(
                context.get(SVC).map(|value| *value),
                Some(7),
                "all same-label contexts must share one scope"
            );
        }
        provided.dispose();
        assert_eq!(contexts[0].get(SVC), None);
    });
}

/// Labels are scoped per registry (tree): two roots using the same label do not
/// share service state, and a labeled scope is freed with its root.
#[test]
fn t06_cross_root_labels_do_not_share_or_leak() {
    watchdog("t06", Duration::from_secs(30), || {
        let drops = Arc::new(AtomicUsize::new(0));
        for _ in 0..200 {
            let root = Context::root();
            let labeled = root.isolate_labeled("r8.svc", "r8.cross-root-L");
            labeled
                .provide(SVC, Arc::new(5u32))
                .expect("provide per root");
            assert_eq!(labeled.get(SVC).map(|value| *value), Some(5));
            labeled
                .provide(
                    ServiceKey::<DropProbe>::new("r8.cross.probe"),
                    Arc::new(DropProbe(drops.clone())),
                )
                .expect("probe");
            drop(labeled);
            drop(root);
        }
        assert_eq!(
            drops.load(Ordering::SeqCst),
            200,
            "labeled-scope payloads must be released with their root"
        );
        let fresh_root = Context::root();
        let labeled = fresh_root.isolate_labeled("r8.svc", "r8.cross-root-L");
        assert_eq!(labeled.get(SVC), None, "no cross-root sharing");
    });
}

// ---------------------------------------------------------------------------
// 2. Quiescence / cancellation edges
// ---------------------------------------------------------------------------

/// `when_idle` waits for a child-cascade dispose and resolves afterwards.
#[test]
fn t07_when_idle_waits_child_cascade() {
    watchdog("t07", Duration::from_secs(20), || {
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let parent = root
            .plugin(
                Arc::new(ParentLoads {
                    child: Arc::new(GatedChild {
                        label: "r8-gated-child",
                        started: started.clone(),
                        release: Arc::new(Mutex::new(Some(release_rx))),
                        cleanups: cleanups.clone(),
                    }),
                }),
                Value::Null,
            )
            .unwrap();
        let disposing = parent.clone();
        let dispose_thread = std::thread::spawn(move || disposing.dispose());
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
        assert!(!root.registry().is_idle(), "cascade is in flight");
        release_tx.send(()).expect("release child cleanup");
        let resolved = done_rx.recv_timeout(Duration::from_secs(5));
        dispose_thread
            .join()
            .expect("dispose thread")
            .expect("dispose");
        assert!(
            resolved.is_ok(),
            "when_idle did not resolve after the cascade"
        );
        assert_eq!(cleanups.load(Ordering::SeqCst), 1);
        assert!(root.registry().is_idle());
        assert_eq!(root.registry().size(), 0);
        let _ = waiter.join();
    });
}

/// `when_idle` waits for a `replace` whose new implementation activates slowly.
#[test]
fn t08_when_idle_waits_replace() {
    watchdog("t08", Duration::from_secs(20), || {
        struct Old;
        impl Plugin for Old {
            fn name(&self) -> &'static str {
                "r8-replace-old"
            }
            fn apply(&self, _ctx: Context, _config: Value) -> cordis::Result<()> {
                Ok(())
            }
        }
        let root = Context::root();
        let fiber = root.plugin(Arc::new(Old), Value::Null).unwrap();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let new_plugin: Arc<dyn Plugin> = Arc::new(GatedApply {
            label: "r8-replace-new",
            started: started.clone(),
            release: Arc::new(Mutex::new(Some(release_rx))),
        });
        let registry = root.registry();
        let replace_thread = std::thread::spawn(move || {
            registry.replace(cordis::PluginKey::Named("r8-replace-old"), new_plugin)
        });
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            !root.registry().is_idle(),
            "replace activation is in flight"
        );
        let ctx = root.clone();
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            block_on(ctx.when_idle());
            let _ = done_tx.send(());
        });
        std::thread::sleep(Duration::from_millis(20));
        release_tx.send(()).expect("release replace activation");
        let resolved = done_rx.recv_timeout(Duration::from_secs(5));
        let replaced = replace_thread.join().expect("replace thread");
        assert!(
            replaced.is_ok(),
            "replace: {}",
            match &replaced {
                Ok(fibers) => format!("Ok({})", fibers.len()),
                Err(e) => format!("Err({e:?})"),
            }
        );
        assert!(resolved.is_ok(), "when_idle did not resolve after replace");
        assert_eq!(fiber.state(), FiberState::Active);
        assert!(root.registry().is_idle());
        let _ = waiter.join();
    });
}

/// The root fiber's own `dispose_async` counts as in-flight lifecycle work.
#[test]
fn t09_root_fiber_dispose_counts_for_quiescence() {
    watchdog("t09", Duration::from_secs(20), || {
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
            let waiter = tokio::spawn({
                let ctx = root.clone();
                async move { ctx.when_idle().await }
            });
            tokio::time::sleep(Duration::from_millis(30)).await;
            assert!(
                !waiter.is_finished(),
                "when_idle resolved during root disposal"
            );
            let _ = release_tx.send(());
            let waited = tokio::time::timeout(Duration::from_secs(5), waiter).await;
            assert!(waited.is_ok(), "when_idle must resolve after root disposal");
            let _ = dispose.await;
            assert!(root.registry().is_idle());
        });
    });
}

/// Dropping a `when_idle` future unregisters its waker (no retention/wakeups).
#[test]
fn t10_dropped_waiter_not_retained() {
    watchdog("t10", Duration::from_secs(20), || {
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let plugin: Arc<dyn Plugin> = Arc::new(GatedApply {
            label: "r8-dropped-waiter",
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
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            0,
            "{} wakers of dropped when_idle futures were retained",
            wakes.load(Ordering::SeqCst)
        );
    });
}

/// A waiter registered during a disposal is woken when the terminal operation
/// finishes (disposed fiber has no ctx link any more).
#[test]
fn t11_terminal_dispose_wakes_waiter() {
    watchdog("t11", Duration::from_secs(20), || {
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let fiber = root
            .plugin(
                Arc::new(GatedChild {
                    label: "r8-terminal",
                    started: started.clone(),
                    release: Arc::new(Mutex::new(Some(release_rx))),
                    cleanups: Arc::new(AtomicUsize::new(0)),
                }),
                Value::Null,
            )
            .unwrap();
        let disposing = fiber.clone();
        let dispose_thread = std::thread::spawn(move || disposing.dispose());
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
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
        assert!(future.as_mut().poll(&mut task_ctx).is_pending());
        state.future.lock().unwrap().replace(future);
        release_tx.send(()).expect("release");
        dispose_thread
            .join()
            .expect("dispose thread")
            .expect("dispose");
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert!(root.registry().is_idle());
        assert!(
            done.load(Ordering::SeqCst) > 0,
            "MISSED-WAKEUP: waiter never resolved after terminal disposal (polls={})",
            polls.load(Ordering::SeqCst)
        );
        let _ = state.future.lock().unwrap().take();
    });
}

/// Multiple waiters all resolve after a root-fiber operation; a dropped waiter
/// does not disturb the live ones.
#[test]
fn t12_multiple_waiters_with_root_op() {
    watchdog("t12", Duration::from_secs(20), || {
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let effect_started = started.clone();
        root.effect(move || {
            vec![cleanup(move || {
                effect_started.fetch_add(1, Ordering::SeqCst);
                let _ = release_rx.recv_timeout(Duration::from_secs(5));
            })]
        })
        .expect("root effect");
        let disposing_root = root.clone();
        let dispose_thread =
            std::thread::spawn(move || block_on(disposing_root.fiber().dispose_async()));
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        // dropped waiter first
        drop(Box::pin(root.when_idle()));
        let (done_tx, done_rx) = mpsc::channel();
        let mut waiters = Vec::new();
        for _ in 0..4 {
            let ctx = root.clone();
            let done_tx = done_tx.clone();
            waiters.push(std::thread::spawn(move || {
                block_on(ctx.when_idle());
                let _ = done_tx.send(());
            }));
        }
        std::thread::sleep(Duration::from_millis(20));
        release_tx.send(()).expect("release");
        let mut resolved = 0;
        for _ in 0..4 {
            if done_rx.recv_timeout(Duration::from_secs(5)).is_ok() {
                resolved += 1;
            }
        }
        dispose_thread
            .join()
            .expect("root dispose thread")
            .expect("root dispose");
        assert_eq!(resolved, 4, "all live waiters must resolve");
        assert!(root.registry().is_idle());
        for waiter in waiters {
            let _ = waiter.join();
        }
    });
}

/// `is_idle` is false while a child cascade is in flight, true after.
#[test]
fn t13_is_idle_false_during_child_disposal() {
    watchdog("t13", Duration::from_secs(20), || {
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let parent = root
            .plugin(
                Arc::new(ParentLoads {
                    child: Arc::new(GatedChild {
                        label: "r8-idle-child",
                        started: started.clone(),
                        release: Arc::new(Mutex::new(Some(release_rx))),
                        cleanups: Arc::new(AtomicUsize::new(0)),
                    }),
                }),
                Value::Null,
            )
            .unwrap();
        assert!(root.registry().is_idle());
        let disposing = parent.clone();
        let dispose_thread = std::thread::spawn(move || disposing.dispose());
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            !root.registry().is_idle(),
            "cascade must be observable as non-idle"
        );
        release_tx.send(()).expect("release");
        dispose_thread
            .join()
            .expect("dispose thread")
            .expect("dispose");
        assert!(root.registry().is_idle());
        assert_eq!(root.registry().size(), 0);
    });
}

/// Cancellation is per-fiber; an async plugin that checkpoints after its await
/// returns Cancelled → Failed + complete rollback.
#[test]
fn t14_cancel_siblings_and_async_checkpoint_rollback() {
    watchdog("t14", Duration::from_secs(20), || {
        static ASVC: ServiceKey<u32> = ServiceKey::new("r8.async.cancel.svc");
        struct AsyncCancel {
            started: Arc<AtomicUsize>,
            release: Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
            cleanups: Arc<AtomicUsize>,
        }
        impl AsyncPlugin for AsyncCancel {
            fn name(&self) -> &'static str {
                "r8-async-cancel"
            }
            fn apply_async<'a>(
                &'a self,
                ctx: Context,
                _config: Value,
            ) -> BoxFuture<'a, cordis::Result<()>> {
                let started = self.started.clone();
                let release = self.release.lock().unwrap().take();
                let cleanups = self.cleanups.clone();
                Box::pin(async move {
                    ctx.provide(ASVC, Arc::new(1u32))?;
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
            let cell_sibling = Arc::new(Mutex::new(None));
            let sibling = root
                .plugin(
                    Arc::new(Capture {
                        cell: cell_sibling.clone(),
                    }),
                    Value::Null,
                )
                .unwrap();
            let sibling_ctx = cell_sibling.lock().unwrap().clone().unwrap().0;
            let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
            let started = Arc::new(AtomicUsize::new(0));
            let cleanups = Arc::new(AtomicUsize::new(0));
            let root_for_activation = root.clone();
            let task_started = started.clone();
            let task_cleanups = cleanups.clone();
            let activation = tokio::spawn(async move {
                root_for_activation
                    .plugin_async(
                        Arc::new(AsyncCancel {
                            started: task_started,
                            release: Arc::new(Mutex::new(Some(release_rx))),
                            cleanups: task_cleanups,
                        }),
                        Value::Null,
                    )
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
                .find(|fiber| fiber.name() == "r8-async-cancel")
                .expect("fiber");
            fiber.cancel();
            assert!(
                sibling_ctx.checkpoint().is_ok(),
                "sibling must not be cancelled"
            );
            assert!(root.fiber().context().unwrap().checkpoint().is_ok());
            let _ = release_tx.send(());
            let _ = tokio::time::timeout(Duration::from_secs(5), activation)
                .await
                .expect("activation must settle");
            assert_eq!(fiber.state(), FiberState::Failed);
            assert_eq!(fiber.error().as_deref(), Some("operation cancelled"));
            assert_eq!(root.get(ASVC), None, "service rolled back");
            assert_eq!(cleanups.load(Ordering::SeqCst), 1);
            assert_eq!(sibling.state(), FiberState::Active);
            assert!(root.registry().is_idle());
        });
    });
}

/// Checkpoint hot loop under concurrent cancellation load: the loop observes
/// the cancellation, exits, and leaves no growing state.
#[test]
fn t15_checkpoint_hot_loop_under_cancel_load() {
    watchdog("t15", Duration::from_secs(30), || {
        let root = Context::root();
        let cell = Arc::new(Mutex::new(None));
        let fiber = root
            .plugin(Arc::new(Capture { cell: cell.clone() }), Value::Null)
            .unwrap();
        let ctx = cell.lock().unwrap().clone().unwrap().0;
        let canceler_fiber = fiber.clone();
        let canceler = std::thread::spawn(move || {
            for _ in 0..50 {
                std::thread::sleep(Duration::from_micros(200));
            }
            canceler_fiber.cancel();
        });
        let mut observed = 0usize;
        let mut total = 0usize;
        loop {
            total += 1;
            if ctx.checkpoint().is_err() {
                observed += 1;
                break;
            }
            if total > 20_000_000 {
                break;
            }
        }
        canceler.join().expect("canceler");
        assert_eq!(observed, 1, "hot loop must observe cancellation");
        assert_eq!(
            fiber.state(),
            FiberState::Active,
            "cancel is cooperative only"
        );
        assert!(root.registry().is_idle());
        assert!(fiber.get_effects().is_empty());
    });
}

// ---------------------------------------------------------------------------
// 3. Residual leaks
// ---------------------------------------------------------------------------

/// Dispose a tree with async children and labeled-scope payloads: registry is
/// empty, quiescent, and everything is reclaimed when the root is dropped.
#[test]
fn t16_async_tree_dispose_and_drop_reclaims() {
    watchdog("t16", Duration::from_secs(30), || {
        let drops = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        {
            let root = Context::root();
            let parent = root
                .plugin(
                    Arc::new(ParentLoads {
                        child: Arc::new(AsyncLeaf {
                            cleanups: cleanups.clone(),
                        }),
                    }),
                    Value::Null,
                )
                .unwrap();
            let labeled = root.isolate_labeled("r8.svc", "r8.leak-L");
            labeled
                .provide(
                    ServiceKey::<DropProbe>::new("r8.leak.probe"),
                    Arc::new(DropProbe(drops.clone())),
                )
                .expect("probe");
            parent.dispose().expect("dispose parent");
            assert_eq!(cleanups.load(Ordering::SeqCst), 1);
            assert_eq!(root.registry().size(), 0);
            assert!(root.registry().is_idle());
        }
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "labeled-scope payload must be freed with the root"
        );
    });
}

/// 50 replacement cycles do not leak runtimes or fibers; the registry stays at
/// one runtime and one fiber, and a final dispose empties everything.
#[test]
fn t17_replace_cycles_no_runtime_leak() {
    watchdog("t17", Duration::from_secs(30), || {
        struct Versioned {
            label: &'static str,
            value: u32,
            applies: Arc<AtomicUsize>,
            cleanups: Arc<AtomicUsize>,
        }
        impl Plugin for Versioned {
            fn name(&self) -> &'static str {
                self.label
            }
            fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
                self.applies.fetch_add(1, Ordering::SeqCst);
                ctx.provide(SVC, Arc::new(self.value))?;
                let cleanups = self.cleanups.clone();
                ctx.effect(move || {
                    vec![cleanup(move || {
                        cleanups.fetch_add(1, Ordering::SeqCst);
                    })]
                })?;
                Ok(())
            }
        }
        let root = Context::root();
        let applies = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let fiber = root
            .plugin(
                Arc::new(Versioned {
                    label: "r8-cycle-0",
                    value: 0,
                    applies: applies.clone(),
                    cleanups: cleanups.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let mut key = cordis::PluginKey::Named("r8-cycle-0");
        for cycle in 1..=50u32 {
            let label: &'static str = Box::leak(format!("r8-cycle-{cycle}").into_boxed_str());
            let new_plugin: Arc<dyn Plugin> = Arc::new(Versioned {
                label,
                value: cycle,
                applies: applies.clone(),
                cleanups: cleanups.clone(),
            });
            let replaced = root
                .registry()
                .replace(key, new_plugin)
                .unwrap_or_else(|error| panic!("cycle {cycle}: {error:?}"));
            assert_eq!(replaced.len(), 1);
            key = cordis::PluginKey::Named(label);
            assert_eq!(
                root.registry().size(),
                1,
                "runtime count stable at cycle {cycle}"
            );
            assert_eq!(root.registry().fiber_count(), 1);
        }
        assert_eq!(fiber.state(), FiberState::Active);
        assert_eq!(root.get(SVC).map(|value| *value), Some(50));
        assert_eq!(applies.load(Ordering::SeqCst), 51);
        assert_eq!(
            cleanups.load(Ordering::SeqCst),
            50,
            "old effects cleaned per cycle"
        );
        let keys = root.registry().keys();
        assert_eq!(keys.len(), 1, "no leaked runtimes: {keys:?}");
        fiber.dispose().expect("dispose");
        assert_eq!(cleanups.load(Ordering::SeqCst), 51);
        assert_eq!(root.registry().size(), 0);
        assert_eq!(root.registry().fiber_count(), 0);
        assert!(root.registry().is_idle());
    });
}

/// 1000 roots with labeled scopes: payloads are freed per root (no cross-root
/// map retention) and the same label never shares state across roots.
#[test]
fn t18_labeled_scope_maps_do_not_grow_across_roots() {
    watchdog("t18", Duration::from_secs(60), || {
        let drops = Arc::new(AtomicUsize::new(0));
        for round in 0..1000usize {
            let root = Context::root();
            let labeled = root.isolate_labeled("r8.svc", "r8.growth-L");
            labeled
                .provide(
                    ServiceKey::<DropProbe>::new("r8.growth.probe"),
                    Arc::new(DropProbe(drops.clone())),
                )
                .expect("probe");
            assert_eq!(
                labeled.get(SVC),
                None,
                "round {round}: no cross-root sharing"
            );
            drop(labeled);
            drop(root);
            assert_eq!(
                drops.load(Ordering::SeqCst),
                round + 1,
                "round {round}: labeled-scope payload not released"
            );
        }
        assert_eq!(drops.load(Ordering::SeqCst), 1000);
    });
}
