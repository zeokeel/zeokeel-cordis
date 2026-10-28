//! Round-5 adversarial attacks on CORE-048 (in-place plugin replacement).
//!
//! Covers identity/config retention, lifecycle interactions, registry
//! invariants, failure paths and dependents. Each test asserts the semantics
//! from the round-5 brief and runs under a watchdog thread.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use cordis::{
    block_on, cleanup, AsyncPlugin, BoxFuture, ConfigSchema, Context, CordisError, EventResult,
    Fiber, FiberState, Plugin, PluginKey, ServiceKey,
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

fn named(key: &'static str) -> PluginKey {
    PluginKey::Named(key)
}

fn fmt_replace(result: &cordis::Result<Vec<Fiber>>) -> String {
    match result {
        Ok(fibers) => format!("Ok({} fibers)", fibers.len()),
        Err(error) => format!("Err({error:?})"),
    }
}

static SVC: ServiceKey<u32> = ServiceKey::new("r6.svc");

/// A service-providing plugin with an optional listener and cleanup counter.
struct SvcPlugin {
    label: &'static str,
    value: u32,
    provides: bool,
    with_listener: bool,
    cleanups: Arc<AtomicUsize>,
}

impl Plugin for SvcPlugin {
    fn name(&self) -> &'static str {
        self.label
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        if self.provides {
            ctx.provide(SVC, Arc::new(self.value))?;
        }
        if self.with_listener {
            ctx.on("r6.event", |_args| EventResult::Continue)?;
        }
        let cleanups = self.cleanups.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                cleanups.fetch_add(1, Ordering::SeqCst);
            })]
        })?;
        Ok(())
    }
}

struct Noop {
    label: &'static str,
    applies: Arc<AtomicUsize>,
}

impl Plugin for Noop {
    fn name(&self) -> &'static str {
        self.label
    }

    fn apply(&self, _ctx: Context, _config: Value) -> cordis::Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct EchoConfig {
    label: &'static str,
    seen: Arc<Mutex<Vec<Value>>>,
    schema: Option<Arc<dyn ConfigSchema>>,
}

impl Plugin for EchoConfig {
    fn name(&self) -> &'static str {
        self.label
    }

    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>> {
        self.schema.clone()
    }

    fn apply(&self, _ctx: Context, config: Value) -> cordis::Result<()> {
        self.seen.lock().unwrap().push(config);
        Ok(())
    }
}

fn config_marker_hook(root: &Context) -> cordis::Disposer {
    root.on_internal_config(Arc::new(
        |_fiber: &cordis::Fiber, config: Value, next: &cordis::ConfigNext| {
            let mut value = next.next(config);
            if let Some(text) = value.as_str() {
                value = serde_json::json!(format!("{text}|R"));
            }
            value
        },
    ))
    .expect("internal/config hook")
}

struct AppendSchema;

impl ConfigSchema for AppendSchema {
    fn validate(&self, value: Value) -> std::result::Result<Value, Vec<String>> {
        match value.as_str() {
            Some(text) => Ok(serde_json::json!(format!("{text}|S"))),
            None => Err(vec!["expected a string".to_string()]),
        }
    }

    fn merge(&self, base: Value, _overrides: &[Value]) -> Value {
        base
    }
}

struct Dependent {
    label: &'static str,
    applies: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<u32>>>,
}

impl Plugin for Dependent {
    fn name(&self) -> &'static str {
        self.label
    }

    fn inject(&self) -> &'static [&'static str] {
        &["r6.svc"]
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        self.seen
            .lock()
            .unwrap()
            .push(ctx.get(SVC).map(|value| *value).unwrap_or(u32::MAX));
        Ok(())
    }
}

fn active_fibers(root: &Context) -> usize {
    root.registry()
        .fibers()
        .iter()
        .filter(|fiber| fiber.state() == FiberState::Active)
        .count()
}

// ---------------------------------------------------------------------------
// 1. Identity / config
// ---------------------------------------------------------------------------

/// Replacement keeps fiber identity, retains the raw config and resolves it
/// exactly once per activation through the new schema.
#[test]
fn t01_replace_preserves_identity_and_renormalizes_config() {
    watchdog("t01", Duration::from_secs(20), || {
        let root = Context::root();
        let _hook = config_marker_hook(&root);
        let first_seen = Arc::new(Mutex::new(Vec::new()));
        let fiber = root
            .plugin(
                Arc::new(EchoConfig {
                    label: "r6-echo",
                    seen: first_seen.clone(),
                    schema: None,
                }),
                serde_json::json!("x"),
            )
            .unwrap();
        let id = fiber.id();
        let uid = fiber.uid();
        assert!(fiber.parent().is_some());

        let second_seen = Arc::new(Mutex::new(Vec::new()));
        let replaced = root
            .registry()
            .replace(
                named("r6-echo"),
                Arc::new(EchoConfig {
                    label: "r6-echo-v2",
                    seen: second_seen.clone(),
                    schema: Some(Arc::new(AppendSchema)),
                }),
            )
            .expect("replace");
        assert_eq!(replaced.len(), 1);
        assert_eq!(fiber.id(), id, "fiber id preserved");
        assert_eq!(fiber.uid(), uid, "fiber uid preserved");
        assert!(fiber.parent().is_some(), "parent link preserved");
        assert_eq!(
            *second_seen.lock().unwrap(),
            vec![serde_json::json!("x|R|S")],
            "raw config resolved once through internal/config + new schema"
        );
        assert_eq!(fiber.raw_config(), serde_json::json!("x"));
        assert_eq!(fiber.config(), serde_json::json!("x|R|S"));
        assert!(!root.registry().has(named("r6-echo")));
        assert!(root.registry().has(named("r6-echo-v2")));

        fiber.restart().expect("restart after replace");
        assert_eq!(
            *second_seen.lock().unwrap(),
            vec![serde_json::json!("x|R|S"), serde_json::json!("x|R|S")],
            "restart must not compound the resolved config"
        );
        assert_eq!(fiber.raw_config(), serde_json::json!("x"));
    });
}

/// Replacement keeps the raw config that a previous `update` stored.
#[test]
fn t02_replace_keeps_raw_config_after_update() {
    watchdog("t02", Duration::from_secs(20), || {
        let root = Context::root();
        let _hook = config_marker_hook(&root);
        let first_seen = Arc::new(Mutex::new(Vec::new()));
        let fiber = root
            .plugin(
                Arc::new(EchoConfig {
                    label: "r6-update-echo",
                    seen: first_seen.clone(),
                    schema: None,
                }),
                serde_json::json!("x"),
            )
            .unwrap();
        fiber.update(serde_json::json!("y")).expect("update");
        assert_eq!(fiber.raw_config(), serde_json::json!("y"));
        assert_eq!(fiber.config(), serde_json::json!("y|R"));

        let second_seen = Arc::new(Mutex::new(Vec::new()));
        root.registry()
            .replace(
                named("r6-update-echo"),
                Arc::new(EchoConfig {
                    label: "r6-update-echo-v2",
                    seen: second_seen.clone(),
                    schema: None,
                }),
            )
            .expect("replace");
        assert_eq!(*second_seen.lock().unwrap(), vec![serde_json::json!("y|R")]);
        assert_eq!(fiber.raw_config(), serde_json::json!("y"));
        assert_eq!(fiber.config(), serde_json::json!("y|R"));
        fiber.restart().expect("restart");
        assert_eq!(
            *second_seen.lock().unwrap(),
            vec![serde_json::json!("y|R"), serde_json::json!("y|R")]
        );
    });
}

struct LeafPlugin {
    label: &'static str,
    cleanups: Arc<AtomicUsize>,
}

impl Plugin for LeafPlugin {
    fn name(&self) -> &'static str {
        self.label
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        let cleanups = self.cleanups.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                cleanups.fetch_add(1, Ordering::SeqCst);
            })]
        })?;
        Ok(())
    }
}

struct ParentV1 {
    label: &'static str,
    cleanups: Arc<AtomicUsize>,
    leaf: Arc<LeafPlugin>,
}

impl Plugin for ParentV1 {
    fn name(&self) -> &'static str {
        self.label
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        ctx.plugin(self.leaf.clone(), Value::Null)?;
        let cleanups = self.cleanups.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                cleanups.fetch_add(1, Ordering::SeqCst);
            })]
        })?;
        Ok(())
    }
}

struct ParentV2 {
    label: &'static str,
    leaves: usize,
    child_cleanups: Arc<AtomicUsize>,
}

impl Plugin for ParentV2 {
    fn name(&self) -> &'static str {
        self.label
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        for _ in 0..self.leaves {
            ctx.plugin(
                Arc::new(LeafPlugin {
                    label: "r6-leaf-v2",
                    cleanups: self.child_cleanups.clone(),
                }),
                Value::Null,
            )?;
        }
        Ok(())
    }
}

/// Replacement keeps identity and reloads the owned child subtree.
#[test]
fn t03_replace_preserves_identity_and_reloads_children() {
    watchdog("t03", Duration::from_secs(20), || {
        let root = Context::root();
        let old_cleanups = Arc::new(AtomicUsize::new(0));
        let old_leaf_cleanups = Arc::new(AtomicUsize::new(0));
        let parent = root
            .plugin(
                Arc::new(ParentV1 {
                    label: "r6-parent",
                    cleanups: old_cleanups.clone(),
                    leaf: Arc::new(LeafPlugin {
                        label: "r6-leaf-v1",
                        cleanups: old_leaf_cleanups.clone(),
                    }),
                }),
                Value::Null,
            )
            .unwrap();
        let id = parent.id();
        let uid = parent.uid();
        assert_eq!(active_fibers(&root), 2);

        let new_child_cleanups = Arc::new(AtomicUsize::new(0));
        root.registry()
            .replace(
                named("r6-parent"),
                Arc::new(ParentV2 {
                    label: "r6-parent-v2",
                    leaves: 2,
                    child_cleanups: new_child_cleanups.clone(),
                }),
            )
            .expect("replace parent");
        assert_eq!(parent.id(), id);
        assert_eq!(parent.uid(), uid);
        assert!(parent.parent().is_some());
        assert_eq!(parent.state(), FiberState::Active);
        assert_eq!(
            old_leaf_cleanups.load(Ordering::SeqCst),
            1,
            "old child subtree disposed"
        );
        assert_eq!(old_cleanups.load(Ordering::SeqCst), 1);
        assert_eq!(active_fibers(&root), 3, "parent + two replacement children");
        assert!(!root.registry().has(named("r6-parent")));
        assert!(root.registry().has(named("r6-parent-v2")));

        parent.dispose().expect("dispose parent");
        assert_eq!(new_child_cleanups.load(Ordering::SeqCst), 2);
        assert_eq!(root.registry().size(), 0);
        assert_eq!(active_fibers(&root), 0);
    });
}

// ---------------------------------------------------------------------------
// 2. Lifecycle interactions
// ---------------------------------------------------------------------------

struct BlockSecondApply {
    started: Arc<AtomicUsize>,
    release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
    applies: Arc<AtomicUsize>,
}

impl Plugin for BlockSecondApply {
    fn name(&self) -> &'static str {
        "r6-block-a"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        let call = self.applies.fetch_add(1, Ordering::SeqCst);
        ctx.provide(SVC, Arc::new(1u32))?;
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

/// Replacing while a restart owns the lifecycle slot must fail with Busy and
/// must not partially rebind the fiber to the rejected implementation.
#[test]
fn t04_replace_during_restart_busy_does_not_partially_rebind() {
    watchdog("t04", Duration::from_secs(20), || {
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let applies = Arc::new(AtomicUsize::new(0));
        let fiber = root
            .plugin(
                Arc::new(BlockSecondApply {
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
        let replaced = root.registry().replace(
            named("r6-block-a"),
            Arc::new(SvcPlugin {
                label: "r6-block-b",
                value: 2,
                provides: true,
                with_listener: false,
                cleanups: Arc::new(AtomicUsize::new(0)),
            }),
        );
        assert!(
            matches!(replaced, Err(CordisError::LifecycleBusy)),
            "replace must report Busy: {}",
            fmt_replace(&replaced)
        );
        release_tx.send(()).expect("release restart");
        restart_thread
            .join()
            .expect("restart thread")
            .expect("restart");
        // The rejected replace must leave the old implementation wholly intact:
        // the fiber still runs the old plugin and the old runtime key survives.
        assert_eq!(
            fiber.name(),
            "r6-block-a",
            "PARTIAL-REBIND: fiber claims the rejected implementation while still \
             running the old service (service={:?}, keys={:?})",
            root.get(SVC).map(|value| *value),
            root.registry().keys()
        );
        assert_eq!(root.get(SVC).map(|value| *value), Some(1));
        assert!(
            root.registry().has(named("r6-block-a")),
            "old runtime key disappeared after a Busy reject: {:?}",
            root.registry().keys()
        );
        assert!(!root.registry().has(named("r6-block-b")));
    });
}

/// Replacing a Pending fiber keeps it Pending (inject gate) and the new
/// implementation activates once the dependency appears.
#[test]
fn t05_replace_pending_fiber_waits_for_inject() {
    watchdog("t05", Duration::from_secs(20), || {
        struct NeedsMissing {
            label: &'static str,
        }
        impl Plugin for NeedsMissing {
            fn name(&self) -> &'static str {
                self.label
            }
            fn inject(&self) -> &'static [&'static str] {
                &["r6.missing"]
            }
            fn apply(&self, _ctx: Context, _config: Value) -> cordis::Result<()> {
                Ok(())
            }
        }
        let root = Context::root();
        let fiber = root
            .plugin(
                Arc::new(NeedsMissing {
                    label: "r6-pending",
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(fiber.state(), FiberState::Pending);
        root.registry()
            .replace(
                named("r6-pending"),
                Arc::new(NeedsMissing {
                    label: "r6-pending-v2",
                }),
            )
            .expect("replace pending");
        assert_eq!(fiber.state(), FiberState::Pending);
        assert_eq!(fiber.name(), "r6-pending-v2");
        assert!(!root.registry().has(named("r6-pending")));
        assert!(root.registry().has(named("r6-pending-v2")));
        let dependency: ServiceKey<u32> = ServiceKey::new("r6.missing");
        let _provided = root.provide(dependency, Arc::new(1u32)).expect("provide");
        root.refresh();
        assert_eq!(fiber.state(), FiberState::Active);
    });
}

/// Replacing a Failed fiber reloads the new implementation and activates it.
#[test]
fn t06_replace_failed_fiber_reloads() {
    watchdog("t06", Duration::from_secs(20), || {
        struct FailOnCreate;
        impl Plugin for FailOnCreate {
            fn name(&self) -> &'static str {
                "r6-initially-failed"
            }
            fn apply(&self, _ctx: Context, _config: Value) -> cordis::Result<()> {
                Err(CordisError::InvalidPlugin("first boom".into()))
            }
        }
        let root = Context::root();
        let fiber = root.plugin(Arc::new(FailOnCreate), Value::Null).unwrap();
        assert_eq!(fiber.state(), FiberState::Failed);
        root.registry()
            .replace(
                named("r6-initially-failed"),
                Arc::new(SvcPlugin {
                    label: "r6-recovered",
                    value: 2,
                    provides: true,
                    with_listener: false,
                    cleanups: Arc::new(AtomicUsize::new(0)),
                }),
            )
            .expect("replace failed fiber");
        assert_eq!(fiber.state(), FiberState::Active);
        assert_eq!(root.get(SVC).map(|value| *value), Some(2));
    });
}

/// Replacing a Disposed fiber must fail safely and never resurrect it.
#[test]
fn t07_replace_disposed_fiber_fails_safely() {
    watchdog("t07", Duration::from_secs(20), || {
        let root = Context::root();
        let fiber = root
            .plugin(
                Arc::new(Noop {
                    label: "r6-disposed",
                    applies: Arc::new(AtomicUsize::new(0)),
                }),
                Value::Null,
            )
            .unwrap();
        fiber.dispose().expect("dispose");
        assert_eq!(fiber.state(), FiberState::Disposed);
        let registry_result = root.registry().replace(
            named("r6-disposed"),
            Arc::new(Noop {
                label: "r6-disposed-v2",
                applies: Arc::new(AtomicUsize::new(0)),
            }),
        );
        assert!(
            matches!(
                registry_result,
                Err(CordisError::InvalidPlugin(_)) | Err(CordisError::Disposed)
            ),
            "replace of a disposed runtime must fail safely: {}",
            fmt_replace(&registry_result)
        );
        let fiber_result = fiber.replace_plugin(Arc::new(Noop {
            label: "r6-disposed-v3",
            applies: Arc::new(AtomicUsize::new(0)),
        }));
        assert!(
            fiber_result.is_err(),
            "replace_plugin on disposed fiber must Err"
        );
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert_eq!(root.registry().size(), 0);
        assert!(!root.registry().has(named("r6-disposed-v2")));
    });
}

/// Replacing during a dispose in progress reports Busy; after the owner
/// finishes the registry is consistent (no zombie runtime).
#[test]
fn t08_replace_during_dispose_busy_settles_consistent() {
    watchdog("t08", Duration::from_secs(20), || {
        struct Gated {
            started: Arc<AtomicUsize>,
            release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
        }
        impl Plugin for Gated {
            fn name(&self) -> &'static str {
                "r6-gated-dispose"
            }
            fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
                ctx.provide(SVC, Arc::new(1u32))?;
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
        let disposer = std::thread::spawn(move || disposing.dispose());
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let replaced = root.registry().replace(
            named("r6-gated-dispose"),
            Arc::new(Noop {
                label: "r6-gated-new",
                applies: Arc::new(AtomicUsize::new(0)),
            }),
        );
        assert!(
            matches!(replaced, Err(CordisError::LifecycleBusy)),
            "dispose owns the slot: {}",
            fmt_replace(&replaced)
        );
        release_tx.send(()).expect("release cleanup");
        disposer.join().expect("dispose thread").expect("dispose");
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert_eq!(root.registry().size(), 0);
        assert!(root.registry().keys().is_empty());
        assert_eq!(active_fibers(&root), 0);
    });
}

// ---------------------------------------------------------------------------
// 3. Registry invariants
// ---------------------------------------------------------------------------

/// Replacing two fibers of one runtime re-keys both and reloads both.
#[test]
fn t09_replace_multiple_fibers_rekeys_and_reloads() {
    watchdog("t09", Duration::from_secs(20), || {
        let root = Context::root();
        let applies = Arc::new(AtomicUsize::new(0));
        let first = root
            .plugin(
                Arc::new(Noop {
                    label: "r6-multi-a",
                    applies: applies.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let second = root
            .plugin(
                Arc::new(Noop {
                    label: "r6-multi-a",
                    applies: applies.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let (id1, id2) = (first.id(), second.id());
        let replaced = root
            .registry()
            .replace(
                named("r6-multi-a"),
                Arc::new(Noop {
                    label: "r6-multi-b",
                    applies: applies.clone(),
                }),
            )
            .expect("replace multiple");
        assert_eq!(replaced.len(), 2);
        assert_eq!(first.id(), id1);
        assert_eq!(second.id(), id2);
        assert_eq!(first.name(), "r6-multi-b");
        assert_eq!(second.name(), "r6-multi-b");
        assert!(!root.registry().has(named("r6-multi-a")));
        let runtime = root
            .registry()
            .get(named("r6-multi-b"))
            .expect("new runtime");
        assert_eq!(runtime.fiber_count(), 2);
        assert_eq!(root.registry().size(), 1);
        assert_eq!(root.registry().fiber_count(), 2);
        assert_eq!(
            applies.load(Ordering::SeqCst),
            4,
            "two initial + two reloads"
        );
    });
}

/// `Fiber::replace_plugin` must keep the registry invariants of CORE-048:
/// the old key disappears, the new key is registered and owns the fiber.
#[test]
fn t10_fiber_replace_plugin_rekeys_the_runtime() {
    watchdog("t10", Duration::from_secs(20), || {
        let root = Context::root();
        let fiber = root
            .plugin(
                Arc::new(SvcPlugin {
                    label: "r6-fiber-a",
                    value: 1,
                    provides: true,
                    with_listener: false,
                    cleanups: Arc::new(AtomicUsize::new(0)),
                }),
                Value::Null,
            )
            .unwrap();
        fiber
            .replace_plugin(Arc::new(SvcPlugin {
                label: "r6-fiber-b",
                value: 2,
                provides: true,
                with_listener: false,
                cleanups: Arc::new(AtomicUsize::new(0)),
            }))
            .expect("fiber replace_plugin");
        assert_eq!(fiber.state(), FiberState::Active);
        assert_eq!(root.get(SVC).map(|value| *value), Some(2));
        assert!(
            !root.registry().has(named("r6-fiber-a")),
            "STALE-KEY: old runtime key still registered after Fiber::replace_plugin \
             (keys={:?}, new_runtime={:?})",
            root.registry().keys(),
            root.registry()
                .get(named("r6-fiber-b"))
                .map(|rt| rt.fiber_count())
        );
        assert!(
            root.registry().has(named("r6-fiber-b")),
            "NEW-KEY-MISSING: Fiber::replace_plugin did not register the new runtime \
             (keys={:?})",
            root.registry().keys()
        );
        let new_runtime = root
            .registry()
            .get(named("r6-fiber-b"))
            .expect("new runtime present");
        assert_eq!(new_runtime.fiber_count(), 1);
        let deleted = root.registry().delete(named("r6-fiber-b"));
        assert!(
            deleted.is_some(),
            "registry.delete(new key) must find the replaced fiber: {:?}",
            root.registry().keys()
        );
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert_eq!(root.registry().size(), 0);
    });
}

/// Replacing onto an already-existing key merges the fiber lists; deleting the
/// merged runtime disposes every fiber.
#[test]
fn t11_replace_onto_existing_key_merges() {
    watchdog("t11", Duration::from_secs(20), || {
        let root = Context::root();
        let existing = root
            .plugin(
                Arc::new(Noop {
                    label: "r6-merge-b",
                    applies: Arc::new(AtomicUsize::new(0)),
                }),
                Value::Null,
            )
            .unwrap();
        let moved = root
            .plugin(
                Arc::new(Noop {
                    label: "r6-merge-a",
                    applies: Arc::new(AtomicUsize::new(0)),
                }),
                Value::Null,
            )
            .unwrap();
        let (existing_id, moved_id) = (existing.id(), moved.id());
        root.registry()
            .replace(
                named("r6-merge-a"),
                Arc::new(Noop {
                    label: "r6-merge-b",
                    applies: Arc::new(AtomicUsize::new(0)),
                }),
            )
            .expect("replace onto existing key");
        assert_eq!(existing.id(), existing_id);
        assert_eq!(moved.id(), moved_id);
        assert!(!root.registry().has(named("r6-merge-a")));
        let runtime = root.registry().get(named("r6-merge-b")).expect("runtime");
        assert_eq!(
            runtime.fiber_count(),
            2,
            "fibers merged into the existing runtime"
        );
        assert_eq!(root.registry().size(), 1);
        assert_eq!(root.registry().fiber_count(), 2);
        assert!(root.registry().delete(named("r6-merge-b")).is_some());
        assert_eq!(existing.state(), FiberState::Disposed);
        assert_eq!(moved.state(), FiberState::Disposed);
        assert_eq!(root.registry().size(), 0);
    });
}

/// Unknown keys and same-key replacement are handled safely.
#[test]
fn t12_replace_unknown_and_same_key() {
    watchdog("t12", Duration::from_secs(20), || {
        struct SameKey {
            value: u32,
        }
        impl Plugin for SameKey {
            fn name(&self) -> &'static str {
                "r6-same"
            }
            fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
                ctx.provide(SVC, Arc::new(self.value))?;
                Ok(())
            }
        }
        let root = Context::root();
        let unknown = root.registry().replace(
            named("r6-unknown"),
            Arc::new(Noop {
                label: "r6-nowhere",
                applies: Arc::new(AtomicUsize::new(0)),
            }),
        );
        assert!(
            matches!(unknown, Err(CordisError::InvalidPlugin(_))),
            "unknown key must be InvalidPlugin: {}",
            fmt_replace(&unknown)
        );
        assert_eq!(root.registry().size(), 0);
        assert!(!root.registry().has(named("r6-nowhere")));

        let fiber = root
            .plugin(Arc::new(SameKey { value: 1 }), Value::Null)
            .unwrap();
        root.registry()
            .replace(named("r6-same"), Arc::new(SameKey { value: 2 }))
            .expect("same-key replace");
        assert_eq!(fiber.state(), FiberState::Active);
        assert_eq!(root.get(SVC).map(|value| *value), Some(2));
        assert!(root.registry().has(named("r6-same")));
        assert_eq!(root.registry().size(), 1);
    });
}

/// A host-held `Fiber` handle stays valid across replacement (id/uid, dispose).
#[test]
fn t13_replace_keeps_host_handle_usable() {
    watchdog("t13", Duration::from_secs(20), || {
        let root = Context::root();
        let fiber = root
            .plugin(
                Arc::new(SvcPlugin {
                    label: "r6-handle-a",
                    value: 1,
                    provides: true,
                    with_listener: false,
                    cleanups: Arc::new(AtomicUsize::new(0)),
                }),
                Value::Null,
            )
            .unwrap();
        let id = fiber.id();
        root.registry()
            .replace(
                named("r6-handle-a"),
                Arc::new(SvcPlugin {
                    label: "r6-handle-b",
                    value: 2,
                    provides: true,
                    with_listener: false,
                    cleanups: Arc::new(AtomicUsize::new(0)),
                }),
            )
            .expect("replace");
        assert_eq!(fiber.id(), id);
        assert_eq!(fiber.state(), FiberState::Active);
        assert_eq!(root.get(SVC).map(|value| *value), Some(2));
        fiber.dispose().expect("dispose via old handle");
        assert_eq!(fiber.state(), FiberState::Disposed);
        assert_eq!(root.registry().size(), 0);
        assert_eq!(active_fibers(&root), 0);
    });
}

// ---------------------------------------------------------------------------
// 4. Failure paths
// ---------------------------------------------------------------------------

struct FailingReplacement {
    label: &'static str,
    value: u32,
    cleanups: Arc<AtomicUsize>,
    panic: bool,
}

impl Plugin for FailingReplacement {
    fn name(&self) -> &'static str {
        self.label
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        ctx.provide(SVC, Arc::new(self.value))?;
        ctx.on("r6.fail.event", |_args| EventResult::Continue)?;
        let cleanups = self.cleanups.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                cleanups.fetch_add(1, Ordering::SeqCst);
            })]
        })?;
        if self.panic {
            panic!("replacement apply boom");
        }
        Err(CordisError::InvalidPlugin("replacement boom".into()))
    }
}

fn replacement_failure_case(panic: bool) {
    let root = Context::root();
    let old_cleanups = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(
            Arc::new(SvcPlugin {
                label: "r6-fail-a",
                value: 1,
                provides: true,
                with_listener: true,
                cleanups: old_cleanups.clone(),
            }),
            Value::Null,
        )
        .unwrap();
    let new_cleanups = Arc::new(AtomicUsize::new(0));
    let replaced = root
        .registry()
        .replace(
            named("r6-fail-a"),
            Arc::new(FailingReplacement {
                label: "r6-fail-b",
                value: 2,
                cleanups: new_cleanups.clone(),
                panic,
            }),
        )
        .expect("activation failure is reported on the fiber, not as replace Err");
    assert_eq!(replaced.len(), 1);
    assert_eq!(fiber.state(), FiberState::Failed);
    assert!(fiber.error().is_some());
    assert_eq!(
        root.get(SVC),
        None,
        "failed replacement rolled its service back"
    );
    assert_eq!(
        root.events().listener_count(),
        0,
        "failed replacement rolled its listeners back"
    );
    assert_eq!(old_cleanups.load(Ordering::SeqCst), 1);
    assert_eq!(new_cleanups.load(Ordering::SeqCst), 1);
    assert!(!root.registry().has(named("r6-fail-a")));
    assert!(root.registry().has(named("r6-fail-b")));
    assert_eq!(root.registry().size(), 1);
    assert_eq!(root.registry().fiber_count(), 1);
    fiber.dispose().expect("dispose failed fiber");
    assert_eq!(root.registry().size(), 0);
}

/// A replacement whose `apply` returns Err leaves the fiber Failed with a full
/// rollback of the new registrations and a consistent registry.
#[test]
fn t14_replace_apply_error_rolls_back() {
    watchdog("t14", Duration::from_secs(20), || {
        replacement_failure_case(false)
    });
}

/// A replacement whose `apply` panics is contained and rolled back the same way.
#[test]
fn t15_replace_apply_panic_rolls_back() {
    watchdog("t15", Duration::from_secs(20), || {
        replacement_failure_case(true)
    });
}

/// `replace_async` with a failing async implementation behaves like the sync
/// failure path.
#[test]
fn t16_replace_async_apply_error_rolls_back() {
    watchdog("t16", Duration::from_secs(20), || {
        struct AsyncFail {
            label: &'static str,
            applies: Arc<AtomicUsize>,
        }
        impl AsyncPlugin for AsyncFail {
            fn name(&self) -> &'static str {
                self.label
            }
            fn apply_async<'a>(
                &'a self,
                ctx: Context,
                _config: Value,
            ) -> BoxFuture<'a, cordis::Result<()>> {
                let applies = self.applies.clone();
                Box::pin(async move {
                    applies.fetch_add(1, Ordering::SeqCst);
                    ctx.provide(SVC, Arc::new(2u32))?;
                    Err(CordisError::InvalidPlugin("async replacement boom".into()))
                })
            }
        }
        let root = Context::root();
        let fiber = root
            .plugin(
                Arc::new(SvcPlugin {
                    label: "r6-async-fail-a",
                    value: 1,
                    provides: true,
                    with_listener: false,
                    cleanups: Arc::new(AtomicUsize::new(0)),
                }),
                Value::Null,
            )
            .unwrap();
        let applies = Arc::new(AtomicUsize::new(0));
        block_on(root.registry().replace_async(
            named("r6-async-fail-a"),
            Arc::new(AsyncFail {
                label: "r6-async-fail-b",
                applies: applies.clone(),
            }),
        ))
        .expect("replace_async");
        assert_eq!(applies.load(Ordering::SeqCst), 1);
        assert_eq!(fiber.state(), FiberState::Failed);
        assert_eq!(root.get(SVC), None);
        assert!(root.registry().has(named("r6-async-fail-b")));
        assert!(!root.registry().has(named("r6-async-fail-a")));
        fiber.dispose().expect("dispose");
        assert_eq!(root.registry().size(), 0);
    });
}

/// One of several fibers failing to activate under the new implementation keeps
/// the registry consistent (Failed + Active, re-keyed, then disposable).
#[test]
fn t17_multi_fiber_replace_one_apply_failure_is_consistent() {
    watchdog("t17", Duration::from_secs(20), || {
        struct FailOnSecond {
            label: &'static str,
            calls: Arc<AtomicUsize>,
        }
        impl Plugin for FailOnSecond {
            fn name(&self) -> &'static str {
                self.label
            }
            fn apply(&self, _ctx: Context, _config: Value) -> cordis::Result<()> {
                let call = self.calls.fetch_add(1, Ordering::SeqCst);
                if call >= 1 {
                    return Err(CordisError::InvalidPlugin("second fiber boom".into()));
                }
                Ok(())
            }
        }
        let root = Context::root();
        let first = root
            .plugin(
                Arc::new(Noop {
                    label: "r6-partial-a",
                    applies: Arc::new(AtomicUsize::new(0)),
                }),
                Value::Null,
            )
            .unwrap();
        let second = root
            .plugin(
                Arc::new(Noop {
                    label: "r6-partial-a",
                    applies: Arc::new(AtomicUsize::new(0)),
                }),
                Value::Null,
            )
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        root.registry()
            .replace(
                named("r6-partial-a"),
                Arc::new(FailOnSecond {
                    label: "r6-partial-b",
                    calls: calls.clone(),
                }),
            )
            .expect("replace");
        let active = [first.state(), second.state()]
            .iter()
            .filter(|state| **state == FiberState::Active)
            .count();
        let failed = [first.state(), second.state()]
            .iter()
            .filter(|state| **state == FiberState::Failed)
            .count();
        assert_eq!(
            (active, failed),
            (1, 1),
            "one fiber failed, one stayed active"
        );
        assert!(root.registry().has(named("r6-partial-b")));
        assert!(!root.registry().has(named("r6-partial-a")));
        assert_eq!(root.registry().fiber_count(), 2);
        assert!(root.registry().delete(named("r6-partial-b")).is_some());
        assert_eq!(first.state(), FiberState::Disposed);
        assert_eq!(second.state(), FiberState::Disposed);
        assert_eq!(root.registry().size(), 0);
    });
}

/// Mixed sync/async replacement APIs are safe in both directions.
#[test]
fn t18_mixed_api_replace_is_safe() {
    watchdog("t18", Duration::from_secs(20), || {
        struct AsyncProv {
            label: &'static str,
            value: u32,
            applies: Arc<AtomicUsize>,
        }
        impl AsyncPlugin for AsyncProv {
            fn name(&self) -> &'static str {
                self.label
            }
            fn apply_async<'a>(
                &'a self,
                ctx: Context,
                _config: Value,
            ) -> BoxFuture<'a, cordis::Result<()>> {
                let value = self.value;
                let applies = self.applies.clone();
                Box::pin(async move {
                    applies.fetch_add(1, Ordering::SeqCst);
                    ctx.provide(SVC, Arc::new(value))?;
                    Ok(())
                })
            }
        }
        struct SyncProv {
            label: &'static str,
            value: u32,
        }
        impl Plugin for SyncProv {
            fn name(&self) -> &'static str {
                self.label
            }
            fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
                ctx.provide(SVC, Arc::new(self.value))?;
                Ok(())
            }
        }
        let root = Context::root();
        let fiber = root
            .plugin(
                Arc::new(SyncProv {
                    label: "r6-mixed-a",
                    value: 1,
                }),
                Value::Null,
            )
            .unwrap();
        let async_applies = Arc::new(AtomicUsize::new(0));
        block_on(fiber.replace_plugin_async(Arc::new(AsyncProv {
            label: "r6-mixed-async",
            value: 2,
            applies: async_applies.clone(),
        })))
        .expect("sync -> async replacement");
        assert_eq!(fiber.state(), FiberState::Active);
        assert_eq!(async_applies.load(Ordering::SeqCst), 1);
        assert_eq!(root.get(SVC).map(|value| *value), Some(2));

        fiber
            .replace_plugin(Arc::new(SyncProv {
                label: "r6-mixed-sync-2",
                value: 3,
            }))
            .expect("async -> sync replacement");
        assert_eq!(fiber.state(), FiberState::Active);
        assert_eq!(root.get(SVC).map(|value| *value), Some(3));
        // Registry re-keying for the single-fiber helper is covered by t10.
        assert_eq!(root.registry().size(), 1);
    });
}

// ---------------------------------------------------------------------------
// 5. Dependents / provider identity epoch
// ---------------------------------------------------------------------------

/// Replacement reactivates dependents exactly once with the new provider value,
/// including a chain A→B→A.
#[test]
fn t19_replace_reactivates_dependents_exactly_once() {
    watchdog("t19", Duration::from_secs(20), || {
        let root = Context::root();
        let _provider = root
            .plugin(
                Arc::new(SvcPlugin {
                    label: "r6-dep-a",
                    value: 1,
                    provides: true,
                    with_listener: false,
                    cleanups: Arc::new(AtomicUsize::new(0)),
                }),
                Value::Null,
            )
            .unwrap();
        let applies = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let dependent = root
            .plugin(
                Arc::new(Dependent {
                    label: "r6-dependent",
                    applies: applies.clone(),
                    seen: seen.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(dependent.state(), FiberState::Active);
        assert_eq!(*seen.lock().unwrap(), vec![1]);

        root.registry()
            .replace(
                named("r6-dep-a"),
                Arc::new(SvcPlugin {
                    label: "r6-dep-b",
                    value: 2,
                    provides: true,
                    with_listener: false,
                    cleanups: Arc::new(AtomicUsize::new(0)),
                }),
            )
            .expect("replace to b");
        assert_eq!(*seen.lock().unwrap(), vec![1, 2]);
        assert_eq!(
            applies.load(Ordering::SeqCst),
            2,
            "exactly one reactivation"
        );
        assert_eq!(root.get(SVC).map(|value| *value), Some(2));

        root.registry()
            .replace(
                named("r6-dep-b"),
                Arc::new(SvcPlugin {
                    label: "r6-dep-c",
                    value: 3,
                    provides: true,
                    with_listener: false,
                    cleanups: Arc::new(AtomicUsize::new(0)),
                }),
            )
            .expect("replace to c");
        assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
        assert_eq!(applies.load(Ordering::SeqCst), 3);
        assert_eq!(dependent.state(), FiberState::Active);
    });
}

/// When the replacement provides no injected service the dependent unloads to
/// Pending (no stale service value).
#[test]
fn t20_replace_without_service_unloads_dependent() {
    watchdog("t20", Duration::from_secs(20), || {
        let root = Context::root();
        let _provider = root
            .plugin(
                Arc::new(SvcPlugin {
                    label: "r6-nosvc-a",
                    value: 1,
                    provides: true,
                    with_listener: false,
                    cleanups: Arc::new(AtomicUsize::new(0)),
                }),
                Value::Null,
            )
            .unwrap();
        let applies = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let dependent = root
            .plugin(
                Arc::new(Dependent {
                    label: "r6-nosvc-dependent",
                    applies: applies.clone(),
                    seen: seen.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(dependent.state(), FiberState::Active);
        root.registry()
            .replace(
                named("r6-nosvc-a"),
                Arc::new(SvcPlugin {
                    label: "r6-nosvc-b",
                    value: 2,
                    provides: false,
                    with_listener: false,
                    cleanups: Arc::new(AtomicUsize::new(0)),
                }),
            )
            .expect("replace without service");
        assert_eq!(
            dependent.state(),
            FiberState::Pending,
            "dependent must unload when the replacement stops providing the service"
        );
        assert_eq!(root.get(SVC), None);
        assert_eq!(applies.load(Ordering::SeqCst), 1);
    });
}

/// Replacing a child fiber (owned by a parent) reloads it and the parent
/// cascade still reclaims it.
#[test]
fn t21_replace_child_fiber_under_parent() {
    watchdog("t21", Duration::from_secs(20), || {
        struct Parent {
            child: Arc<LeafPlugin>,
        }
        impl Plugin for Parent {
            fn name(&self) -> &'static str {
                "r6-child-parent"
            }
            fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
                ctx.plugin(self.child.clone(), Value::Null)?;
                Ok(())
            }
        }
        let root = Context::root();
        let old_cleanups = Arc::new(AtomicUsize::new(0));
        let new_cleanups = Arc::new(AtomicUsize::new(0));
        let parent = root
            .plugin(
                Arc::new(Parent {
                    child: Arc::new(LeafPlugin {
                        label: "r6-child-a",
                        cleanups: old_cleanups.clone(),
                    }),
                }),
                Value::Null,
            )
            .unwrap();
        let child = root
            .registry()
            .fibers()
            .into_iter()
            .find(|fiber| fiber.name() == "r6-child-a")
            .expect("child");
        root.registry()
            .replace(
                named("r6-child-a"),
                Arc::new(LeafPlugin {
                    label: "r6-child-b",
                    cleanups: new_cleanups.clone(),
                }),
            )
            .expect("replace child");
        assert_eq!(child.state(), FiberState::Active);
        assert_eq!(child.name(), "r6-child-b");
        assert_eq!(old_cleanups.load(Ordering::SeqCst), 1);
        assert_eq!(parent.state(), FiberState::Active);
        parent.dispose().expect("dispose parent");
        assert_eq!(child.state(), FiberState::Disposed);
        assert_eq!(new_cleanups.load(Ordering::SeqCst), 1);
        assert_eq!(root.registry().size(), 0);
    });
}

// ---------------------------------------------------------------------------
// 6. Lifecycle busy during replace (registry-level partial migration)
// ---------------------------------------------------------------------------

/// A rejected (Busy) replacement while one of two fibers is restarting must not
/// migrate the registry to the new runtime key.
#[test]
fn t22_multi_fiber_replace_busy_does_not_migrate_registry() {
    watchdog("t22", Duration::from_secs(20), || {
        struct Gate {
            started: Arc<AtomicUsize>,
            release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
            applies: Arc<AtomicUsize>,
        }
        impl Plugin for Gate {
            fn name(&self) -> &'static str {
                "r6-multi-busy-a"
            }
            fn apply(&self, _ctx: Context, _config: Value) -> cordis::Result<()> {
                let call = self.applies.fetch_add(1, Ordering::SeqCst);
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
        let root = Context::root();
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(AtomicUsize::new(0));
        let applies = Arc::new(AtomicUsize::new(0));
        let busy_fiber = root
            .plugin(
                Arc::new(Gate {
                    started: started.clone(),
                    release: Arc::new(Mutex::new(Some(release_rx))),
                    applies: applies.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let other_fiber = root
            .plugin(
                Arc::new(Noop {
                    label: "r6-multi-busy-a",
                    applies: Arc::new(AtomicUsize::new(0)),
                }),
                Value::Null,
            )
            .unwrap();
        let restarter = busy_fiber.clone();
        let restart_thread = std::thread::spawn(move || restarter.restart());
        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let replaced = root.registry().replace(
            named("r6-multi-busy-a"),
            Arc::new(Noop {
                label: "r6-multi-busy-b",
                applies: Arc::new(AtomicUsize::new(0)),
            }),
        );
        assert!(
            matches!(replaced, Err(CordisError::LifecycleBusy)),
            "replace must report Busy: {}",
            fmt_replace(&replaced)
        );
        release_tx.send(()).expect("release restart");
        restart_thread
            .join()
            .expect("restart thread")
            .expect("restart");
        assert!(
            root.registry().has(named("r6-multi-busy-a")),
            "PARTIAL-MIGRATION: a Busy-rejected replace removed the old runtime key \
             (keys={:?}, names={}/{}",
            root.registry().keys(),
            busy_fiber.name(),
            other_fiber.name()
        );
        assert!(!root.registry().has(named("r6-multi-busy-b")));
        assert_eq!(busy_fiber.name(), "r6-multi-busy-a");
        assert_eq!(other_fiber.name(), "r6-multi-busy-a");
    });
}
