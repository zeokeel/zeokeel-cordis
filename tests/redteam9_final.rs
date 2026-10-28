//! Final sweep: `Context::dispose_all()` / `Registry::dispose_all()` (CORE-051)
//! shutdown-path correctness, leak sanity, and one last panic edge.
//! Every test runs under a watchdog.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use zeokeel_cordis::{
    cleanup, cleanup_async, Context, EventResult, FiberState, Plugin, ServiceKey,
};

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

fn rss_kb() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    pages * 4
}

static SVC: ServiceKey<u32> = ServiceKey::new("r9.svc");
static PROBE: ServiceKey<u32> = ServiceKey::new("r9.probe");

struct Leaf {
    label: &'static str,
    cleanups: Arc<AtomicUsize>,
    with_listener: bool,
    with_service: bool,
}

impl Plugin for Leaf {
    fn name(&self) -> &'static str {
        self.label
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        let cleanups = self.cleanups.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                cleanups.fetch_add(1, Ordering::SeqCst);
            })]
        })?;
        if self.with_listener {
            ctx.on("r9.event", |_args| EventResult::Continue)?;
        }
        if self.with_service {
            ctx.provide(SVC, Arc::new(7u32))?;
        }
        Ok(())
    }
}

struct AsyncLeaf {
    cleanups: Arc<AtomicUsize>,
}

impl Plugin for AsyncLeaf {
    fn name(&self) -> &'static str {
        "r9-async-leaf"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
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

struct ParentLoads {
    child: Arc<dyn Plugin>,
}

impl Plugin for ParentLoads {
    fn name(&self) -> &'static str {
        "r9-parent"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        ctx.plugin(self.child.clone(), Value::Null)?;
        Ok(())
    }
}

/// A tree with children, an async leaf, listeners, services and labeled scopes:
/// one `dispose_all` reclaims everything exactly once; the second call is a
/// no-op; the root fiber itself ends Disposed.
#[test]
fn t01_dispose_all_reclaims_composite_tree() {
    watchdog("t01", Duration::from_secs(30), || {
        let root = Context::root();
        let sync_cleanups = Arc::new(AtomicUsize::new(0));
        let async_cleanups = Arc::new(AtomicUsize::new(0));
        let parent = root
            .plugin(
                Arc::new(ParentLoads {
                    child: Arc::new(Leaf {
                        label: "r9-child",
                        cleanups: sync_cleanups.clone(),
                        with_listener: true,
                        with_service: true,
                    }),
                }),
                Value::Null,
            )
            .unwrap();
        let _sibling = root
            .plugin(
                Arc::new(AsyncLeaf {
                    cleanups: async_cleanups.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let labeled = root.isolate_labeled("r9.svc", "r9.dispose-all-L");
        labeled
            .provide(PROBE, Arc::new(1u32))
            .expect("labeled service");
        assert!(root.events().listener_count() >= 1);

        let result = root.dispose_all();
        assert!(result.is_ok(), "dispose_all: {result:?}");
        assert_eq!(sync_cleanups.load(Ordering::SeqCst), 1);
        assert_eq!(async_cleanups.load(Ordering::SeqCst), 1);
        assert_eq!(root.registry().size(), 0);
        assert_eq!(root.registry().fiber_count(), 0);
        assert_eq!(root.events().listener_count(), 0);
        assert!(root.get(SVC).is_none());
        assert!(labeled.get(PROBE).is_none() || root.get(PROBE).is_none());
        assert!(root.registry().is_idle());
        assert_eq!(parent.state(), FiberState::Disposed);
        assert_eq!(root.fiber().state(), FiberState::Disposed);
        let second = root.dispose_all();
        assert!(second.is_ok(), "idempotent dispose_all: {second:?}");
        assert_eq!(sync_cleanups.load(Ordering::SeqCst), 1, "no double cleanup");
    });
}

/// `dispose_all` called from inside a cleanup (reentrant) does not deadlock and
/// still empties the registry.
#[test]
fn t02_reentrant_dispose_all_from_cleanup() {
    watchdog("t02", Duration::from_secs(20), || {
        struct Reentrant {
            root: Arc<Mutex<Option<Context>>>,
            results: Arc<Mutex<Vec<String>>>,
        }
        impl Plugin for Reentrant {
            fn name(&self) -> &'static str {
                "r9-reentrant"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                *self.root.lock().unwrap() = Some(ctx.clone());
                let root = self.root.clone();
                let results = self.results.clone();
                ctx.effect(move || {
                    vec![cleanup(move || {
                        let ctx = root.lock().unwrap().clone().expect("root");
                        results.lock().unwrap().push(format!(
                            "{:?}",
                            ctx.dispose_all().map_err(|e| e.to_string())
                        ));
                    })]
                })?;
                Ok(())
            }
        }
        let root = Context::root();
        let cell = Arc::new(Mutex::new(None));
        let results = Arc::new(Mutex::new(Vec::new()));
        let _fiber = root
            .plugin(
                Arc::new(Reentrant {
                    root: cell,
                    results: results.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let outer = root.dispose_all();
        assert!(outer.is_ok(), "outer dispose_all: {outer:?}");
        assert_eq!(results.lock().unwrap().len(), 1);
        assert!(root.registry().is_idle());
        assert_eq!(root.registry().size(), 0);
    });
}

/// Concurrent `dispose_all` from two threads: no panic, every cleanup runs
/// exactly once, registry empty/idle afterwards.
#[test]
fn t03_concurrent_dispose_all() {
    watchdog("t03", Duration::from_secs(30), || {
        let root = Context::root();
        let cleanups = Arc::new(AtomicUsize::new(0));
        for index in 0..8 {
            let label: &'static str = Box::leak(format!("r9-concurrent-{index}").into_boxed_str());
            root.plugin(
                Arc::new(Leaf {
                    label,
                    cleanups: cleanups.clone(),
                    with_listener: false,
                    with_service: false,
                }),
                Value::Null,
            )
            .unwrap();
        }
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let first = {
            let root = root.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                root.dispose_all().map_err(|error| error.to_string())
            })
        };
        let second = {
            let root = root.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                root.dispose_all().map_err(|error| error.to_string())
            })
        };
        let first = first.join().expect("first thread");
        let second = second.join().expect("second thread");
        assert_eq!(
            cleanups.load(Ordering::SeqCst),
            8,
            "each cleanup exactly once"
        );
        assert_eq!(root.registry().size(), 0);
        assert!(root.registry().is_idle());
        eprintln!("concurrent dispose_all outcomes: {first:?} / {second:?}");
    });
}

/// `dispose_all` while a restart owns a fiber's lifecycle: it may report Busy,
/// but after the restart settles the shutdown path must still reclaim
/// everything on retry (no zombie/orphan).
#[test]
fn t04_dispose_all_during_restart_then_retry() {
    watchdog("t04", Duration::from_secs(20), || {
        struct Slow {
            started: Arc<AtomicUsize>,
            release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
            applies: Arc<AtomicUsize>,
        }
        impl Plugin for Slow {
            fn name(&self) -> &'static str {
                "r9-restart-slow"
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
        let restarter = fiber.clone();
        let restart_thread = std::thread::spawn(move || restarter.restart());
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let first = root.dispose_all();
        release_tx.send(()).expect("release restart");
        restart_thread
            .join()
            .expect("restart thread")
            .expect("restart");
        // Whatever happened to the in-flight restart, a retry must finalize.
        let retry = root.dispose_all();
        assert!(
            retry.is_ok(),
            "retry dispose_all after Busy: {retry:?} (first={first:?})"
        );
        assert_eq!(root.registry().size(), 0);
        assert_eq!(root.registry().fiber_count(), 0);
        assert!(root.registry().is_idle());
        assert_eq!(root.fiber().state(), FiberState::Disposed);
    });
}

/// `dispose_all` while a dispose is in flight on another thread: no panic, and
/// after settling both paths agree on a fully disposed, empty registry.
#[test]
fn t05_dispose_all_during_dispose_inflight() {
    watchdog("t05", Duration::from_secs(20), || {
        struct Gated {
            started: Arc<AtomicUsize>,
            release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
        }
        impl Plugin for Gated {
            fn name(&self) -> &'static str {
                "r9-gated-dispose"
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
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let fiber = root
            .plugin(
                Arc::new(Gated {
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
        let first = root.dispose_all();
        release_tx.send(()).expect("release dispose");
        dispose_thread
            .join()
            .expect("dispose thread")
            .expect("dispose");
        let retry = root.dispose_all();
        assert!(retry.is_ok(), "retry: {retry:?} (first={first:?})");
        assert_eq!(root.registry().size(), 0);
        assert!(root.registry().is_idle());
    });
}

/// After `dispose_all` the root is terminal: creating a plugin fails with
/// "fiber already disposed" consistently, and another dispose_all is a no-op.
#[test]
fn t06_after_dispose_all_root_is_terminal() {
    watchdog("t06", Duration::from_secs(20), || {
        let root = Context::root();
        root.plugin(
            Arc::new(Leaf {
                label: "r9-before",
                cleanups: Arc::new(AtomicUsize::new(0)),
                with_listener: false,
                with_service: false,
            }),
            Value::Null,
        )
        .unwrap();
        root.dispose_all().expect("dispose_all");
        assert_eq!(root.fiber().state(), FiberState::Disposed);
        let reuse = root.plugin(
            Arc::new(Leaf {
                label: "r9-after",
                cleanups: Arc::new(AtomicUsize::new(0)),
                with_listener: false,
                with_service: false,
            }),
            Value::Null,
        );
        let message = match reuse {
            Ok(_) => panic!("root after dispose_all must reject new plugins"),
            Err(error) => error.to_string(),
        };
        assert!(
            message.contains("disposed") || message.contains("inactive"),
            "unexpected terminal error: {message}"
        );
        assert_eq!(root.registry().size(), 0);
        assert_eq!(root.registry().fiber_count(), 0);
        assert!(root.registry().is_idle());
        assert!(root.dispose_all().is_ok());
    });
}

/// A never-disposed fiber with an async cleanup is reclaimed by the synchronous
/// `dispose_all` via the blocking executor (no reactor needed).
#[test]
fn t07_never_disposed_async_cleanup_runs_on_dispose_all() {
    watchdog("t07", Duration::from_secs(20), || {
        let root = Context::root();
        let cleanups = Arc::new(AtomicUsize::new(0));
        let fiber = root
            .plugin(
                Arc::new(AsyncLeaf {
                    cleanups: cleanups.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(fiber.state(), FiberState::Active);
        assert_eq!(cleanups.load(Ordering::SeqCst), 0, "not disposed yet");
        let result = root.dispose_all();
        assert!(result.is_ok(), "dispose_all: {result:?}");
        assert_eq!(cleanups.load(Ordering::SeqCst), 1, "async cleanup must run");
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert_eq!(root.registry().size(), 0);
        assert!(root.registry().is_idle());
    });
}

/// One panicking cleanup must not abort the whole shutdown: the other fibers
/// and the root still have to be attempted (the panic may surface after the
/// attempts, like `drain_effects_blocking`).
#[test]
fn t08_panicking_cleanup_does_not_abort_shutdown() {
    watchdog("t08", Duration::from_secs(20), || {
        struct PanicLeaf {
            label: &'static str,
            cleanups: Arc<AtomicUsize>,
        }
        impl Plugin for PanicLeaf {
            fn name(&self) -> &'static str {
                self.label
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                let cleanups = self.cleanups.clone();
                let label = self.label;
                ctx.effect(move || {
                    vec![cleanup(move || {
                        cleanups.fetch_add(1, Ordering::SeqCst);
                        if label == "r9-panic" {
                            panic!("cleanup boom");
                        }
                    })]
                })?;
                Ok(())
            }
        }
        let root = Context::root();
        let cleanups = Arc::new(AtomicUsize::new(0));
        let _panicking = root
            .plugin(
                Arc::new(PanicLeaf {
                    label: "r9-panic",
                    cleanups: cleanups.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let _healthy = root
            .plugin(
                Arc::new(PanicLeaf {
                    label: "r9-healthy",
                    cleanups: cleanups.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let first = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| root.dispose_all()));
        let size = root.registry().size();
        let fiber_count = root.registry().fiber_count();
        let root_state = root.fiber().state();
        let idle = root.registry().is_idle();
        // finish the job if the first call was aborted (retry must be safe)
        let retry = root.dispose_all();
        assert!(
            size == 0 && fiber_count == 0 && root_state == FiberState::Disposed,
            "PANIC-ABORT: a panicking cleanup aborted dispose_all (registry={size}, \
             fibers={fiber_count}, root={root_state:?}, idle={idle}, cleanups={}); \
             remaining fibers were not attempted",
            cleanups.load(Ordering::SeqCst)
        );
        assert!(first.is_err() || retry.is_ok());
        assert_eq!(root.registry().size(), 0);
        assert_eq!(
            cleanups.load(Ordering::SeqCst),
            2,
            "each cleanup exactly once"
        );
    });
}

/// 2000 boot→(plugin+child+labeled scope)→dispose_all→drop cycles keep RSS
/// flat; 50 replace cycles followed by dispose_all leave an empty/idle registry.
#[test]
fn t09_leak_sanity_boot_cycles_and_replace() {
    watchdog("t09", Duration::from_secs(120), || {
        struct Versioned {
            label: &'static str,
            value: u32,
            cleanups: Arc<AtomicUsize>,
        }
        impl Plugin for Versioned {
            fn name(&self) -> &'static str {
                self.label
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
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
        // 50 replace cycles, then dispose_all.
        {
            let root = Context::root();
            let cleanups = Arc::new(AtomicUsize::new(0));
            let fiber = root
                .plugin(
                    Arc::new(Versioned {
                        label: "r9-replace-0",
                        value: 0,
                        cleanups: cleanups.clone(),
                    }),
                    Value::Null,
                )
                .unwrap();
            let mut key = zeokeel_cordis::PluginKey::Named("r9-replace-0");
            for cycle in 1..=50u32 {
                let label: &'static str = Box::leak(format!("r9-replace-{cycle}").into_boxed_str());
                let replaced = root
                    .registry()
                    .replace(
                        key,
                        Arc::new(Versioned {
                            label,
                            value: cycle,
                            cleanups: cleanups.clone(),
                        }),
                    )
                    .unwrap_or_else(|error| panic!("replace {cycle}: {error:?}"));
                assert_eq!(replaced.len(), 1);
                key = zeokeel_cordis::PluginKey::Named(label);
                assert_eq!(root.registry().size(), 1);
                assert_eq!(root.registry().fiber_count(), 1);
            }
            assert_eq!(cleanups.load(Ordering::SeqCst), 50);
            assert!(root.dispose_all().is_ok());
            assert_eq!(root.registry().size(), 0);
            assert_eq!(fiber.state(), FiberState::Disposed);
            assert_eq!(cleanups.load(Ordering::SeqCst), 51);
        }
        // 2000 boot/dispose_all/drop cycles.
        let before = rss_kb();
        for cycle in 0..2000usize {
            let root = Context::root();
            let cleanups = Arc::new(AtomicUsize::new(0));
            let _parent = root
                .plugin(
                    Arc::new(ParentLoads {
                        child: Arc::new(Leaf {
                            label: "r9-cycle-child",
                            cleanups: cleanups.clone(),
                            with_listener: true,
                            with_service: true,
                        }),
                    }),
                    Value::Null,
                )
                .unwrap();
            let _labeled = root.isolate_labeled("r9.svc", "r9.cycle-L");
            root.dispose_all()
                .unwrap_or_else(|error| panic!("cycle {cycle}: {error:?}"));
            assert_eq!(root.registry().size(), 0, "cycle {cycle}");
            assert!(root.registry().is_idle(), "cycle {cycle}");
            assert_eq!(root.events().listener_count(), 0, "cycle {cycle}");
            assert_eq!(cleanups.load(Ordering::SeqCst), 1, "cycle {cycle}");
            drop(root);
        }
        let after = rss_kb();
        let delta = after as i64 - before as i64;
        eprintln!("rss before={before}kB after={after}kB delta={delta}kB");
        assert!(
            delta < 32_768,
            "RSS grew by {delta}kB over 2000 boot cycles (possible leak)"
        );
    });
}
