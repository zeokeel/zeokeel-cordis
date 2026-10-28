//! Round-4 adversarial attacks on CORE-047 (parent-owned child fibers).
//!
//! Covers cascade correctness, failed-activation rollback, deep/wide nesting,
//! recursion cycles, interaction with lifecycle serialization, and leak
//! reclamation. Each test asserts the semantics from the round-4 brief and runs
//! under a watchdog thread so a kernel hang fails the test instead of blocking
//! `cargo test`. Process-aborting cases (recursive cross-loading, >1200-level
//! chains) run in the `child_probe` subprocess.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};

use cordis::{
    block_on, cleanup, cleanup_async, AsyncPlugin, BoxFuture, Context, CordisError, EventResult,
    Fiber, FiberState, Plugin, ServiceKey,
};
use serde_json::Value;

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

fn fiber_by_name(root: &Context, name: &str) -> Fiber {
    let start = Instant::now();
    loop {
        if let Some(fiber) = root
            .registry()
            .fibers()
            .into_iter()
            .find(|fiber| fiber.name() == name && fiber.state() != FiberState::Disposed)
        {
            return fiber;
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("fiber `{name}` not found");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn active_fiber_count(root: &Context) -> usize {
    root.registry()
        .fibers()
        .iter()
        .filter(|fiber| fiber.state() == FiberState::Active)
        .count()
}

fn cleanup_labels(root: &Context) -> Vec<String> {
    root.registry()
        .fibers()
        .iter()
        .flat_map(|fiber| {
            fiber
                .get_effects()
                .into_iter()
                .map(|effect| effect.label.clone())
                .collect::<Vec<_>>()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Child plugins
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Leaf {
    cleanups: Arc<AtomicUsize>,
    with_listener: bool,
}

impl Plugin for Leaf {
    fn name(&self) -> &'static str {
        "r5-leaf"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        let cleanups = self.cleanups.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                cleanups.fetch_add(1, Ordering::SeqCst);
            })]
        })?;
        if self.with_listener {
            ctx.on("r5.leaf.event", |_args| EventResult::Continue)?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct AsyncLeaf {
    cleanups: Arc<AtomicUsize>,
    with_listener: bool,
}

impl Plugin for AsyncLeaf {
    fn name(&self) -> &'static str {
        "r5-async-leaf"
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
        if self.with_listener {
            ctx.on("r5.async-leaf.event", |_args| EventResult::Continue)?;
        }
        Ok(())
    }
}

struct Loader {
    children: Vec<Arc<dyn Plugin>>,
}

impl Plugin for Loader {
    fn name(&self) -> &'static str {
        "r5-loader"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        for child in &self.children {
            ctx.plugin(child.clone(), Value::Null)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 1. Cascade correctness
// ---------------------------------------------------------------------------

/// Parent dispose cascades through mixed sync+async children exactly once and
/// releases registry entries, listeners and effects.
#[test]
fn c01_parent_dispose_cascades_mixed_children_exactly_once() {
    watchdog("c01", Duration::from_secs(30), || {
        let root = Context::root();
        let leaf_cleanups = Arc::new(AtomicUsize::new(0));
        let async_cleanups = Arc::new(AtomicUsize::new(0));
        let children: Vec<Arc<dyn Plugin>> = vec![
            Arc::new(Leaf {
                cleanups: leaf_cleanups.clone(),
                with_listener: true,
            }),
            Arc::new(Leaf {
                cleanups: leaf_cleanups.clone(),
                with_listener: true,
            }),
            Arc::new(AsyncLeaf {
                cleanups: async_cleanups.clone(),
                with_listener: true,
            }),
            Arc::new(AsyncLeaf {
                cleanups: async_cleanups.clone(),
                with_listener: true,
            }),
        ];
        let parent = root
            .plugin(Arc::new(Loader { children }), Value::Null)
            .expect("parent");
        assert_eq!(active_fiber_count(&root), 5);
        assert_eq!(root.events().listener_count(), 4);

        parent.dispose().expect("parent dispose");

        assert_eq!(parent.state(), FiberState::Disposed);
        assert_eq!(
            leaf_cleanups.load(Ordering::SeqCst),
            2,
            "each sync child cleanup must run exactly once"
        );
        assert_eq!(
            async_cleanups.load(Ordering::SeqCst),
            2,
            "each async child cleanup must run exactly once"
        );
        assert_eq!(active_fiber_count(&root), 0);
        assert_eq!(root.registry().size(), 0);
        assert_eq!(root.registry().fiber_count(), 0);
        assert_eq!(root.events().listener_count(), 0);
        assert!(parent.get_effects().is_empty());
    });
}

/// Disposing a child first and the parent afterwards is idempotent: no double
/// cleanup, the parent still cascades to its remaining children.
#[test]
fn c02_child_first_then_parent_is_idempotent() {
    watchdog("c02", Duration::from_secs(20), || {
        let root = Context::root();
        let cleanups = Arc::new(AtomicUsize::new(0));
        let children: Vec<Arc<dyn Plugin>> = (0..2)
            .map(|_| {
                Arc::new(Leaf {
                    cleanups: cleanups.clone(),
                    with_listener: false,
                }) as Arc<dyn Plugin>
            })
            .collect();
        let parent = root
            .plugin(Arc::new(Loader { children }), Value::Null)
            .unwrap();
        let child_ids: Vec<u64> = root
            .registry()
            .fibers()
            .iter()
            .filter(|fiber| fiber.name() == "r5-leaf")
            .map(|fiber| fiber.id())
            .collect();
        assert_eq!(child_ids.len(), 2);
        for id in &child_ids {
            let child = root
                .registry()
                .fibers()
                .into_iter()
                .find(|fiber| fiber.id() == *id)
                .expect("child handle");
            child.dispose().expect("dispose child first");
        }
        assert_eq!(cleanups.load(Ordering::SeqCst), 2);
        parent.dispose().expect("dispose parent after children");
        assert_eq!(
            cleanups.load(Ordering::SeqCst),
            2,
            "already-disposed children must not be cleaned twice"
        );
        assert_eq!(root.registry().size(), 0);
        assert_eq!(active_fiber_count(&root), 0);
    });
}

/// Parent restart disposes the old children and activates replacements.
#[test]
fn c03_parent_restart_recreates_children() {
    watchdog("c03", Duration::from_secs(20), || {
        let root = Context::root();
        let cleanups = Arc::new(AtomicUsize::new(0));
        let children: Vec<Arc<dyn Plugin>> = (0..2)
            .map(|_| {
                Arc::new(Leaf {
                    cleanups: cleanups.clone(),
                    with_listener: true,
                }) as Arc<dyn Plugin>
            })
            .collect();
        let parent = root
            .plugin(Arc::new(Loader { children }), Value::Null)
            .unwrap();
        let old_ids: Vec<u64> = root
            .registry()
            .fibers()
            .iter()
            .filter(|fiber| fiber.name() == "r5-leaf")
            .map(|fiber| fiber.id())
            .collect();
        parent.restart().expect("restart parent");
        assert_eq!(
            cleanups.load(Ordering::SeqCst),
            2,
            "old children must be disposed on restart"
        );
        let new_ids: Vec<u64> = root
            .registry()
            .fibers()
            .iter()
            .filter(|fiber| fiber.name() == "r5-leaf" && fiber.state() == FiberState::Active)
            .map(|fiber| fiber.id())
            .collect();
        assert_eq!(new_ids.len(), 2, "restart must load two new children");
        assert!(
            old_ids.iter().all(|id| !new_ids.contains(id)),
            "restart must create fresh child fibers"
        );
        assert_eq!(parent.state(), FiberState::Active);
        assert_eq!(root.events().listener_count(), 2);
        parent.dispose().expect("dispose");
        assert_eq!(cleanups.load(Ordering::SeqCst), 4);
        assert_eq!(root.registry().size(), 0);
        assert_eq!(root.events().listener_count(), 0);
    });
}

/// Parent update (restart with new config) disposes old children and loads new.
#[test]
fn c04_parent_update_recreates_children() {
    watchdog("c04", Duration::from_secs(20), || {
        let root = Context::root();
        let cleanups = Arc::new(AtomicUsize::new(0));
        let children: Vec<Arc<dyn Plugin>> = (0..2)
            .map(|_| {
                Arc::new(Leaf {
                    cleanups: cleanups.clone(),
                    with_listener: false,
                }) as Arc<dyn Plugin>
            })
            .collect();
        let parent = root
            .plugin(Arc::new(Loader { children }), Value::Null)
            .unwrap();
        parent
            .update(serde_json::json!({"round": 4}))
            .expect("update");
        assert_eq!(cleanups.load(Ordering::SeqCst), 2);
        assert_eq!(active_fiber_count(&root), 3, "parent + two new children");
        parent.dispose().expect("dispose");
        assert_eq!(cleanups.load(Ordering::SeqCst), 4);
        assert_eq!(root.registry().size(), 0);
    });
}

struct Chain {
    remaining: usize,
}

impl Plugin for Chain {
    fn name(&self) -> &'static str {
        "r5-chain"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        if self.remaining > 0 {
            ctx.plugin(
                Arc::new(Chain {
                    remaining: self.remaining - 1,
                }),
                Value::Null,
            )?;
        }
        Ok(())
    }
}

/// A 100-level parent→child chain disposes without stack overflow; every level
/// is gone from the registry afterwards.
#[test]
fn c05_deep_chain_100_levels_disposes() {
    watchdog("c05", Duration::from_secs(30), || {
        let root = Context::root();
        let top = root
            .plugin(Arc::new(Chain { remaining: 100 }), Value::Null)
            .expect("deep chain");
        assert_eq!(root.registry().fiber_count(), 101);
        top.dispose().expect("dispose chain root");
        assert_eq!(root.registry().size(), 0);
        assert_eq!(root.registry().fiber_count(), 0);
        assert_eq!(active_fiber_count(&root), 0);
    });
}

/// A 500-child fan-out disposes completely with one cleanup per child.
#[test]
fn c06_wide_fanout_500_disposes() {
    watchdog("c06", Duration::from_secs(60), || {
        let root = Context::root();
        let cleanups = Arc::new(AtomicUsize::new(0));
        let children: Vec<Arc<dyn Plugin>> = (0..500)
            .map(|_| {
                Arc::new(Leaf {
                    cleanups: cleanups.clone(),
                    with_listener: false,
                }) as Arc<dyn Plugin>
            })
            .collect();
        let parent = root
            .plugin(Arc::new(Loader { children }), Value::Null)
            .unwrap();
        assert_eq!(root.registry().fiber_count(), 501);
        parent.dispose().expect("dispose wide parent");
        assert_eq!(cleanups.load(Ordering::SeqCst), 500);
        assert_eq!(root.registry().size(), 0);
        assert_eq!(active_fiber_count(&root), 0);
    });
}

// ---------------------------------------------------------------------------
// 2. Rollback of a failed parent activation
// ---------------------------------------------------------------------------

struct FailingLoader {
    children: Vec<Arc<dyn Plugin>>,
    panic: bool,
}

impl Plugin for FailingLoader {
    fn name(&self) -> &'static str {
        "r5-failing-loader"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        for child in &self.children {
            ctx.plugin(child.clone(), Value::Null)?;
        }
        if self.panic {
            panic!("parent apply boom");
        }
        Err(CordisError::InvalidPlugin("parent boom".into()))
    }
}

fn failing_loader_case(panic: bool) {
    let root = Context::root();
    let cleanups = Arc::new(AtomicUsize::new(0));
    let children: Vec<Arc<dyn Plugin>> = (0..2)
        .map(|_| {
            Arc::new(Leaf {
                cleanups: cleanups.clone(),
                with_listener: true,
            }) as Arc<dyn Plugin>
        })
        .collect();
    let parent = root
        .plugin(Arc::new(FailingLoader { children, panic }), Value::Null)
        .expect("failed activation still returns the fiber");
    assert_eq!(parent.state(), FiberState::Failed);
    assert_eq!(
        cleanups.load(Ordering::SeqCst),
        2,
        "children loaded by a failed activation must be rolled back"
    );
    assert_eq!(active_fiber_count(&root), 0, "no orphan active children");
    assert_eq!(root.events().listener_count(), 0, "no leaked listeners");
    assert!(parent.get_effects().is_empty());
    assert_eq!(
        root.registry().size(),
        1,
        "the Failed parent stays registered"
    );
    parent.dispose().expect("dispose failed parent");
    assert_eq!(root.registry().size(), 0);
}

/// A sync parent that returns Err after loading children rolls them back.
#[test]
fn c07_sync_parent_error_rolls_back_children() {
    watchdog("c07", Duration::from_secs(20), || {
        failing_loader_case(false)
    });
}

/// A sync parent that panics after loading children rolls them back.
#[test]
fn c08_sync_parent_panic_rolls_back_children() {
    watchdog("c08", Duration::from_secs(20), || failing_loader_case(true));
}

struct AsyncFailingLoader {
    children: Vec<Arc<dyn Plugin>>,
}

impl AsyncPlugin for AsyncFailingLoader {
    fn name(&self) -> &'static str {
        "r5-async-failing-loader"
    }

    fn apply_async<'a>(
        &'a self,
        ctx: Context,
        _config: Value,
    ) -> BoxFuture<'a, cordis::Result<()>> {
        let children = self.children.clone();
        Box::pin(async move {
            for child in &children {
                ctx.plugin(child.clone(), Value::Null)?;
            }
            Err(CordisError::InvalidPlugin("async parent boom".into()))
        })
    }
}

/// An async parent that returns Err after loading children rolls them back.
#[test]
fn c09_async_parent_error_rolls_back_children() {
    watchdog("c09", Duration::from_secs(20), || {
        let runtime = current_runtime();
        runtime.block_on(async {
            let root = Context::root();
            let cleanups = Arc::new(AtomicUsize::new(0));
            let children: Vec<Arc<dyn Plugin>> = (0..2)
                .map(|_| {
                    Arc::new(Leaf {
                        cleanups: cleanups.clone(),
                        with_listener: true,
                    }) as Arc<dyn Plugin>
                })
                .collect();
            let parent = root
                .plugin_async(Arc::new(AsyncFailingLoader { children }), Value::Null)
                .await
                .expect("plugin_async");
            assert_eq!(parent.state(), FiberState::Failed);
            assert_eq!(cleanups.load(Ordering::SeqCst), 2);
            assert_eq!(active_fiber_count(&root), 0);
            assert_eq!(root.events().listener_count(), 0);
            parent.dispose_async().await.expect("dispose");
            assert_eq!(root.registry().size(), 0);
        });
    });
}

struct AsyncCancelLoader {
    cleanups: Arc<AtomicUsize>,
}

impl AsyncPlugin for AsyncCancelLoader {
    fn name(&self) -> &'static str {
        "r5-async-cancel-loader"
    }

    fn apply_async<'a>(
        &'a self,
        ctx: Context,
        _config: Value,
    ) -> BoxFuture<'a, cordis::Result<()>> {
        let cleanups = self.cleanups.clone();
        Box::pin(async move {
            ctx.plugin(
                Arc::new(Leaf {
                    cleanups,
                    with_listener: true,
                }),
                Value::Null,
            )?;
            std::future::pending::<()>().await;
            Ok(())
        })
    }
}

/// Cancelling a `plugin_async` mid-activation after it loaded children must roll
/// the children back (brief: "children rolled back (deferred path too)"); the
/// current implementation only defers the ownership effect, leaving an Active
/// child owned by a Failed parent.
#[test]
fn c10_cancelled_async_parent_rolls_back_children() {
    watchdog("c10", Duration::from_secs(30), || {
        let runtime = current_runtime();
        runtime.block_on(async {
            let root = Context::root();
            let cleanups = Arc::new(AtomicUsize::new(0));
            let mut creation = Box::pin(root.plugin_async(
                Arc::new(AsyncCancelLoader {
                    cleanups: cleanups.clone(),
                }),
                Value::Null,
            ));
            assert!(poll_once(creation.as_mut()).is_pending());
            drop(creation); // activate-cancel guard runs
            let parent = root
                .registry()
                .fibers()
                .into_iter()
                .find(|fiber| fiber.name() == "r5-async-cancel-loader")
                .expect("parent fiber");
            assert_eq!(parent.state(), FiberState::Failed);
            let orphans: Vec<(String, FiberState)> = root
                .registry()
                .fibers()
                .iter()
                .filter(|fiber| fiber.id() != parent.id())
                .map(|fiber| (fiber.name(), fiber.state()))
                .collect();
            assert!(
                orphans
                    .iter()
                    .all(|(_, state)| *state == FiberState::Disposed),
                "CANCEL-ORPHAN: cancelled async parent left children {orphans:?} \
                 (cleanups ran {}, registry {})",
                cleanups.load(Ordering::SeqCst),
                root.registry().size()
            );
            parent
                .dispose_async()
                .await
                .expect("dispose cancelled parent");
            assert_eq!(root.registry().size(), 0);
        });
    });
}

/// Deferred-path companion: the children left behind by a cancelled async parent
/// are reclaimed when the Failed parent is disposed.
#[test]
fn c10b_cancelled_async_parent_children_reclaimed_on_parent_dispose() {
    watchdog("c10b", Duration::from_secs(30), || {
        let runtime = current_runtime();
        runtime.block_on(async {
            let root = Context::root();
            let cleanups = Arc::new(AtomicUsize::new(0));
            let mut creation = Box::pin(root.plugin_async(
                Arc::new(AsyncCancelLoader {
                    cleanups: cleanups.clone(),
                }),
                Value::Null,
            ));
            assert!(poll_once(creation.as_mut()).is_pending());
            drop(creation);
            let parent = root
                .registry()
                .fibers()
                .into_iter()
                .find(|fiber| fiber.name() == "r5-async-cancel-loader")
                .expect("parent fiber");
            parent.dispose_async().await.expect("dispose");
            assert_eq!(
                cleanups.load(Ordering::SeqCst),
                1,
                "the deferred child cleanup must run exactly once on parent dispose"
            );
            assert_eq!(root.registry().size(), 0);
            assert_eq!(active_fiber_count(&root), 0);
        });
    });
}

struct FailChild;

impl Plugin for FailChild {
    fn name(&self) -> &'static str {
        "r5-fail-child"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> cordis::Result<()> {
        Err(CordisError::InvalidPlugin("child boom".into()))
    }
}

/// A child whose activation fails does not fail the parent (actual semantics):
/// parent Active, child Failed, and the cascade still reclaims the Failed child.
#[test]
fn c11_failing_child_does_not_fail_parent_and_is_reclaimed() {
    watchdog("c11", Duration::from_secs(20), || {
        let root = Context::root();
        let parent = root
            .plugin(
                Arc::new(Loader {
                    children: vec![Arc::new(FailChild)],
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(parent.state(), FiberState::Active);
        let child = root
            .registry()
            .fibers()
            .into_iter()
            .find(|fiber| fiber.name() == "r5-fail-child")
            .expect("failed child fiber");
        assert_eq!(child.state(), FiberState::Failed);
        parent.dispose().expect("dispose parent");
        assert_eq!(child.state(), FiberState::Disposed);
        assert_eq!(root.registry().size(), 0);
        assert_eq!(active_fiber_count(&root), 0);
    });
}

// ---------------------------------------------------------------------------
// 3. Nesting / recursion
// ---------------------------------------------------------------------------

/// Cross-loading (A loads B, B loads A) must be contained (one side
/// Pending/Failed), not recurse forever. Runs in a subprocess because v0.3
/// overflows the stack and aborts the process.
#[test]
fn c12_cross_loading_cycle_is_contained() {
    watchdog("c12", Duration::from_secs(30), || {
        let probe = env!("CARGO_BIN_EXE_child_probe");
        let output = std::process::Command::new(probe)
            .arg("cycle")
            .output()
            .expect("spawn child probe");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "RECURSION-CYCLE: process aborted (status {:?}); stderr tail: {}",
            output.status,
            stderr.lines().rev().take(3).collect::<Vec<_>>().join(" | ")
        );
        assert!(
            stdout.contains("CYCLE_SURVIVED"),
            "cross-loading cycle was not contained: stdout={stdout} stderr={stderr}"
        );
    });
}

/// Chains up to 1000 levels dispose in the subprocess (1000 measured OK; 1500
/// overflows the default stack — see RESULTS).
#[test]
fn c13_deep_chain_subprocess_1000_levels() {
    watchdog("c13", Duration::from_secs(60), || {
        let probe = env!("CARGO_BIN_EXE_child_probe");
        for depth in [100usize, 1000] {
            let output = std::process::Command::new(probe)
                .arg("depth")
                .arg(depth.to_string())
                .output()
                .expect("spawn child probe");
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success(),
                "depth {depth} aborted: status {:?}, stdout={stdout}",
                output.status
            );
            assert!(
                stdout.contains("RESULT=OK") && stdout.contains("registry=0"),
                "depth {depth} did not dispose cleanly: {stdout}"
            );
        }
    });
}

// ---------------------------------------------------------------------------
// 4. Interaction with lifecycle serialization
// ---------------------------------------------------------------------------

struct RestartBomb {
    started: Arc<AtomicUsize>,
    release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
    applies: Arc<AtomicUsize>,
    cleanups: Arc<AtomicUsize>,
}

impl Plugin for RestartBomb {
    fn name(&self) -> &'static str {
        "r5-restart-bomb"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        let call = self.applies.fetch_add(1, Ordering::SeqCst);
        let cleanups = self.cleanups.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                cleanups.fetch_add(1, Ordering::SeqCst);
            })]
        })?;
        if call == 1 {
            self.started.fetch_add(1, Ordering::SeqCst);
            let rx = self.release.lock().unwrap().take();
            if let Some(rx) = rx {
                let _ = rx.recv_timeout(Duration::from_secs(5));
            }
        }
        Ok(())
    }
}

/// Parent dispose while a child restart owns the child's lifecycle slot: the
/// cascade's `dispose_async` gets `LifecycleBusy`; that must not leave an
/// Active orphan child behind.
#[test]
fn c14_parent_dispose_vs_child_restart_does_not_orphan_child() {
    watchdog("c14", Duration::from_secs(20), || {
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let applies = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let parent = root
            .plugin(
                Arc::new(Loader {
                    children: vec![Arc::new(RestartBomb {
                        started: started.clone(),
                        release: Arc::new(Mutex::new(Some(release_rx))),
                        applies: applies.clone(),
                        cleanups: cleanups.clone(),
                    })],
                }),
                Value::Null,
            )
            .unwrap();
        let child = fiber_by_name(&root, "r5-restart-bomb");
        let restart_child = child.clone();
        let restarter = std::thread::spawn(move || restart_child.restart());
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        parent.dispose().expect("parent dispose");
        release_tx.send(()).expect("release child restart");
        let restart = restarter.join().expect("restart thread");
        assert!(restart.is_ok(), "child restart: {restart:?}");
        assert_eq!(parent.state(), FiberState::Disposed);
        assert_eq!(
            child.state(),
            FiberState::Disposed,
            "ORPHAN: parent disposed but child is {:?} (applies={}, cleanups={}, registry={})",
            child.state(),
            applies.load(Ordering::SeqCst),
            cleanups.load(Ordering::SeqCst),
            root.registry().size()
        );
        assert_eq!(root.registry().size(), 0);
    });
}

struct GatedChild {
    started: Arc<AtomicUsize>,
    release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
    cleanups: Arc<AtomicUsize>,
}

impl Plugin for GatedChild {
    fn name(&self) -> &'static str {
        "r5-gated-child"
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

/// Parent dispose while the child's own dispose is already in progress from
/// another thread: both finalize, the cleanup runs exactly once.
#[test]
fn c15_parent_dispose_vs_child_dispose_in_progress() {
    watchdog("c15", Duration::from_secs(20), || {
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let parent = root
            .plugin(
                Arc::new(Loader {
                    children: vec![Arc::new(GatedChild {
                        started: started.clone(),
                        release: Arc::new(Mutex::new(Some(release_rx))),
                        cleanups: cleanups.clone(),
                    })],
                }),
                Value::Null,
            )
            .unwrap();
        let child = fiber_by_name(&root, "r5-gated-child");
        let disposing_child = child.clone();
        let child_disposer = std::thread::spawn(move || disposing_child.dispose());
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        parent.dispose().expect("parent dispose");
        release_tx.send(()).expect("release child cleanup");
        child_disposer
            .join()
            .expect("child dispose thread")
            .expect("child dispose");
        assert_eq!(parent.state(), FiberState::Disposed);
        assert_eq!(child.state(), FiberState::Disposed);
        assert_eq!(cleanups.load(Ordering::SeqCst), 1, "cleanup exactly once");
        assert_eq!(root.registry().size(), 0);
    });
}

#[derive(Default)]
struct GatedAsyncChild {
    gate: Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
    started: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
}

impl Plugin for GatedAsyncChild {
    fn name(&self) -> &'static str {
        "r5-gated-async-child"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        if let Some(gate) = self.gate.lock().unwrap().take() {
            let started = self.started.clone();
            let completed = self.completed.clone();
            ctx.effect_async(move || {
                vec![cleanup_async(move || {
                    let started = started.clone();
                    let completed = completed.clone();
                    async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        let _ = gate.await;
                        completed.fetch_add(1, Ordering::SeqCst);
                    }
                })]
            })?;
        }
        Ok(())
    }
}

/// Cancel a parent's `dispose_async` mid-cascade and retry: every child must be
/// disposed exactly once (the in-flight ownership effect must not be lost).
#[test]
fn c16_cancelled_parent_dispose_async_retry_disposes_all_children() {
    watchdog("c16", Duration::from_secs(30), || {
        let root = Context::root();
        let mut gates = Vec::new();
        let mut children = Vec::new();
        let started = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
            gates.push(gate_tx);
            children.push(GatedAsyncChild {
                gate: Arc::new(Mutex::new(Some(gate_rx))),
                started: started.clone(),
                completed: completed.clone(),
            });
        }
        let child_plugins: Vec<Arc<dyn Plugin>> = children
            .into_iter()
            .map(|child| Arc::new(child) as Arc<dyn Plugin>)
            .collect();
        let parent = root
            .plugin(
                Arc::new(Loader {
                    children: child_plugins,
                }),
                Value::Null,
            )
            .unwrap();
        let mut future = Box::pin(parent.dispose_async());
        assert!(poll_once(future.as_mut()).is_pending());
        drop(future); // cancel mid-cascade
        assert_eq!(parent.state(), FiberState::Unloading);
        for gate in gates {
            let _ = gate.send(());
        }
        let retry = block_on(parent.dispose_async());
        assert!(retry.is_ok(), "retry dispose: {retry:?}");
        let undelivered: Vec<(String, FiberState)> = root
            .registry()
            .fibers()
            .iter()
            .filter(|fiber| fiber.state() != FiberState::Disposed)
            .map(|fiber| (fiber.name(), fiber.state()))
            .collect();
        assert!(
            undelivered.is_empty(),
            "CANCEL-CASCADE: retry did not dispose every child: {undelivered:?} \
             (registry={}, started={}, completed={})",
            root.registry().size(),
            started.load(Ordering::SeqCst),
            completed.load(Ordering::SeqCst)
        );
        assert_eq!(root.registry().size(), 0);
        // Cancellation may lose at most the single cleanup future that was
        // in flight (documented bound); every other cleanup must run once.
        let completed_count = completed.load(Ordering::SeqCst);
        assert!(
            completed_count >= 1 && completed_count <= started.load(Ordering::SeqCst),
            "cleanups after retry: completed={completed_count} started={}",
            started.load(Ordering::SeqCst)
        );
    });
}

/// Disposing a child from the parent's own cleanup is allowed (different
/// lifecycle) and the ownership effect then sees an already-disposed child
/// (idempotent, no double cleanup).
#[test]
fn c17_dispose_child_from_parent_cleanup() {
    watchdog("c17", Duration::from_secs(20), || {
        struct DisposesChild {
            child_cleanups: Arc<AtomicUsize>,
        }
        impl Plugin for DisposesChild {
            fn name(&self) -> &'static str {
                "r5-parent-disposes-child"
            }
            fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
                let child = ctx.plugin(
                    Arc::new(Leaf {
                        cleanups: self.child_cleanups.clone(),
                        with_listener: false,
                    }),
                    Value::Null,
                )?;
                ctx.effect(move || {
                    vec![cleanup(move || {
                        child.dispose().expect("dispose child from parent cleanup");
                    })]
                })?;
                Ok(())
            }
        }
        let root = Context::root();
        let child_cleanups = Arc::new(AtomicUsize::new(0));
        let parent = root
            .plugin(
                Arc::new(DisposesChild {
                    child_cleanups: child_cleanups.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        parent.dispose().expect("parent dispose");
        assert_eq!(child_cleanups.load(Ordering::SeqCst), 1);
        assert_eq!(root.registry().size(), 0);
    });
}

/// Disposing the root fiber cascades through every plugin and its children.
#[test]
fn c18_root_fiber_dispose_cascades_tree() {
    watchdog("c18", Duration::from_secs(30), || {
        let root = Context::root();
        let cleanups = Arc::new(AtomicUsize::new(0));
        let children: Vec<Arc<dyn Plugin>> = (0..2)
            .map(|_| {
                Arc::new(Leaf {
                    cleanups: cleanups.clone(),
                    with_listener: true,
                }) as Arc<dyn Plugin>
            })
            .collect();
        let _parent = root
            .plugin(Arc::new(Loader { children }), Value::Null)
            .unwrap();
        assert_eq!(root.registry().fiber_count(), 3);
        root.fiber().dispose().expect("dispose root fiber");
        assert_eq!(cleanups.load(Ordering::SeqCst), 2);
        assert_eq!(root.registry().size(), 0);
        assert_eq!(root.registry().fiber_count(), 0);
        assert_eq!(root.events().listener_count(), 0);
    });
}

struct PendingChild;

impl Plugin for PendingChild {
    fn name(&self) -> &'static str {
        "r5-pending-child"
    }

    fn inject(&self) -> &'static [&'static str] {
        &["r5.missing.service"]
    }

    fn apply(&self, _ctx: Context, _config: Value) -> cordis::Result<()> {
        Ok(())
    }
}

/// A child that never activated (Pending, missing inject) is still owned and
/// disposed with the parent.
#[test]
fn c19_pending_child_disposed_with_parent() {
    watchdog("c19", Duration::from_secs(20), || {
        let root = Context::root();
        let parent = root
            .plugin(
                Arc::new(Loader {
                    children: vec![Arc::new(PendingChild)],
                }),
                Value::Null,
            )
            .unwrap();
        let child = fiber_by_name(&root, "r5-pending-child");
        assert_eq!(child.state(), FiberState::Pending);
        parent.dispose().expect("parent dispose");
        assert_eq!(child.state(), FiberState::Disposed);
        assert_eq!(root.registry().size(), 0);
    });
}

// ---------------------------------------------------------------------------
// 5. Leak reclamation
// ---------------------------------------------------------------------------

struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

static PROBE_SERVICE: ServiceKey<DropProbe> = ServiceKey::new("r5.probe.service");

struct ProbeChild {
    drops: Arc<AtomicUsize>,
}

impl Plugin for ProbeChild {
    fn name(&self) -> &'static str {
        "r5-probe-child"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        ctx.provide(PROBE_SERVICE, Arc::new(DropProbe(self.drops.clone())))?;
        ctx.on("r5.probe.event", |_args| EventResult::Continue)?;
        Ok(())
    }
}

/// After the cascade the child payload is released (no parent↔child strong
/// cycle), and registry/listeners/services are all clean.
#[test]
fn c20_child_payload_released_after_cascade() {
    watchdog("c20", Duration::from_secs(20), || {
        let root = Context::root();
        let drops = Arc::new(AtomicUsize::new(0));
        let parent = root
            .plugin(
                Arc::new(Loader {
                    children: vec![Arc::new(ProbeChild {
                        drops: drops.clone(),
                    })],
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(root.get(PROBE_SERVICE).map(|_| ()), Some(()));
        assert_eq!(root.events().listener_count(), 1);
        parent.dispose().expect("parent dispose");
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "LEAK: the child payload was not released after the cascade"
        );
        assert!(root.get(PROBE_SERVICE).is_none());
        assert_eq!(root.events().listener_count(), 0);
        assert_eq!(root.registry().size(), 0);
        assert!(cleanup_labels(&root).is_empty());
    });
}

/// An async child with a reactor-free cleanup is disposed by the parent's
/// blocking cascade path exactly once.
#[test]
fn c21_parent_sync_dispose_handles_async_child_cleanup() {
    watchdog("c21", Duration::from_secs(20), || {
        let root = Context::root();
        let cleanups = Arc::new(AtomicUsize::new(0));
        let parent = root
            .plugin(
                Arc::new(Loader {
                    children: vec![Arc::new(AsyncLeaf {
                        cleanups: cleanups.clone(),
                        with_listener: true,
                    })],
                }),
                Value::Null,
            )
            .unwrap();
        parent.dispose().expect("sync parent dispose");
        assert_eq!(cleanups.load(Ordering::SeqCst), 1);
        assert_eq!(root.registry().size(), 0);
        assert_eq!(root.events().listener_count(), 0);
    });
}
