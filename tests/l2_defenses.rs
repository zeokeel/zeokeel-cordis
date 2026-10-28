//! L2 runtime defenses: capability introspection, per-fiber quotas and the
//! cooperative watchdog/heartbeat API.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cordis::{cleanup, Context, FiberQuota, FiberState, IsolationCapabilities, Plugin, Result};
use serde_json::Value;

#[test]
fn isolation_capabilities_are_reported() {
    let caps = IsolationCapabilities::detect();
    assert_eq!(caps.panic_strategy, "unwind");
    assert!(caps.panic_containment, "unwind build contains panics");
    assert!(caps.unsafe_forbidden, "kernel itself forbids unsafe");
    assert!(
        !caps.process_isolation,
        "process isolation is an embedder layer"
    );
    assert!(!caps.wasm_isolation, "wasm isolation is an embedder layer");
    let summary = caps.summary();
    assert!(summary.contains("panic=unwind"), "{summary}");
    assert!(summary.contains("process=false"), "{summary}");
}

struct TooManyEffects {
    cleanups: Arc<AtomicUsize>,
}

impl Plugin for TooManyEffects {
    fn name(&self) -> &'static str {
        "l2-many-effects"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        for _ in 0..3 {
            let cleanups = self.cleanups.clone();
            ctx.effect(move || {
                vec![cleanup(move || {
                    cleanups.fetch_add(1, Ordering::SeqCst);
                })]
            })?;
        }
        Ok(())
    }
}

#[test]
fn effect_quota_rejects_and_rolls_back() {
    let root = Context::root();
    root.set_quota(FiberQuota::unlimited().max_effects(2));
    let cleanups = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(
            Arc::new(TooManyEffects {
                cleanups: cleanups.clone(),
            }),
            Value::Null,
        )
        .expect("a Failed fiber is still returned");

    assert_eq!(fiber.state(), FiberState::Failed);
    let error = fiber.error().expect("activation failed");
    assert!(error.contains("quota"), "error: {error}");
    // Two effects were accepted and rolled back; the rejected third effect's
    // disposer runs its cleanup through the Disposer Drop safety net.
    assert_eq!(
        cleanups.load(Ordering::SeqCst),
        3,
        "accepted effects roll back; the rejected disposer is dropped safely"
    );
    assert!(root.registry().quota().max_effects == Some(2));
    root.dispose_all().expect("cleanup");
}

struct TooManyListeners;

impl Plugin for TooManyListeners {
    fn name(&self) -> &'static str {
        "l2-many-listeners"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.on("l2", |_| cordis::EventResult::Continue)?;
        ctx.on("l2", |_| cordis::EventResult::Continue)?;
        Ok(())
    }
}

#[test]
fn listener_quota_rejects_and_releases_the_slot() {
    let root = Context::root();
    root.set_quota(FiberQuota::unlimited().max_listeners(1));
    let fiber = root
        .plugin(Arc::new(TooManyListeners), Value::Null)
        .expect("fiber");
    assert_eq!(fiber.state(), FiberState::Failed);
    assert!(fiber.error().unwrap().contains("quota"));
    assert_eq!(
        root.events().listener_count(),
        0,
        "the rejected listener slot must be released"
    );
    root.dispose_all().expect("cleanup");
}

struct ParentWithChildren;

impl Plugin for ParentWithChildren {
    fn name(&self) -> &'static str {
        "l2-parent"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        // The second child exceeds max_child_fibers(1).
        ctx.plugin(Arc::new(ChildPlugin), Value::Null)?;
        ctx.plugin(Arc::new(ChildPlugin), Value::Null)?;
        Ok(())
    }
}

struct ChildPlugin;

impl Plugin for ChildPlugin {
    fn name(&self) -> &'static str {
        "l2-child"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        Ok(())
    }
}

#[test]
fn child_quota_rejects_and_cascades_cleanup() {
    let root = Context::root();
    root.set_quota(FiberQuota::unlimited().max_child_fibers(1));
    let fiber = root
        .plugin(Arc::new(ParentWithChildren), Value::Null)
        .expect("fiber");
    assert_eq!(fiber.state(), FiberState::Failed);
    assert!(fiber.error().unwrap().contains("quota"));
    root.dispose_all().expect("cleanup");
    assert_eq!(root.registry().size(), 0);
    assert_eq!(root.registry().fiber_count(), 0);
}

struct Sleepy;

impl Plugin for Sleepy {
    fn name(&self) -> &'static str {
        "l2-sleepy"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        std::thread::sleep(Duration::from_millis(200));
        Ok(())
    }
}

#[test]
fn watchdog_flags_a_stalled_activation() {
    let root = Context::root();
    let ctx = root.clone();
    let handle = std::thread::spawn(move || ctx.plugin(Arc::new(Sleepy), Value::Null));

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut stalled = Vec::new();
    while Instant::now() < deadline {
        stalled = root.stalled_fibers(Duration::from_millis(40));
        if !stalled.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        !stalled.is_empty(),
        "a 200ms activation must look stalled after 40ms of quiet"
    );
    assert_eq!(stalled[0].name(), "l2-sleepy");
    assert!(stalled[0].idle_for >= Duration::from_millis(40));

    let fiber = handle.join().expect("thread").expect("fiber");
    assert_eq!(fiber.state(), FiberState::Active);
    assert!(
        root.stalled_fibers(Duration::from_millis(40)).is_empty(),
        "a settled fiber is not stalled"
    );
    root.dispose_all().expect("cleanup");
}

struct Heartbeating {
    started: Arc<AtomicUsize>,
}

impl Plugin for Heartbeating {
    fn name(&self) -> &'static str {
        "l2-heartbeat"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        self.started.fetch_add(1, Ordering::SeqCst);
        for _ in 0..15 {
            ctx.heartbeat();
            std::thread::sleep(Duration::from_millis(15));
        }
        Ok(())
    }
}

#[test]
fn heartbeat_prevents_false_stall_reports() {
    let root = Context::root();
    let started = Arc::new(AtomicUsize::new(0));
    let ctx = root.clone();
    let plugin = Heartbeating {
        started: started.clone(),
    };
    let handle = std::thread::spawn(move || ctx.plugin(Arc::new(plugin), Value::Null));

    let deadline = Instant::now() + Duration::from_secs(5);
    while started.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    std::thread::sleep(Duration::from_millis(120));
    assert!(
        root.stalled_fibers(Duration::from_millis(50)).is_empty(),
        "heartbeats must keep the fiber out of the stalled set"
    );

    handle.join().expect("thread").expect("fiber");
    root.dispose_all().expect("cleanup");
}
