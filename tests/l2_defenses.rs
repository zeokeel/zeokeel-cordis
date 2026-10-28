#![cfg(feature = "supervision")]
//! L2 runtime defenses: capability introspection, per-fiber quotas and the
//! cooperative watchdog/heartbeat API.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use std::sync::Mutex;
use zeokeel_cordis::{
    cleanup, Clock, Context, CordisError, Disposer, EventResult, FiberQuota, FiberState,
    IsolationCapabilities, Plugin, Result, ServiceKey, TracingExporter,
};

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
        ctx.on("l2", |_| zeokeel_cordis::EventResult::Continue)?;
        ctx.on("l2", |_| zeokeel_cordis::EventResult::Continue)?;
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

// ---------------------------------------------------------------------------
// Audit R3B P2-1: supervision quotas count *live* resources
// ---------------------------------------------------------------------------

struct EffectRecycler {
    ctx_slot: Arc<Mutex<Option<Context>>>,
    kept: Arc<Mutex<Vec<zeokeel_cordis::Disposer>>>,
}

impl Plugin for EffectRecycler {
    fn name(&self) -> &'static str {
        "l2-effect-recycler"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        *self.ctx_slot.lock().unwrap() = Some(ctx.clone());
        // Audit R3B P2-1a: registering and releasing effects repeatedly must not
        // accumulate against `max_effects` (disposed entries are not live).
        for _ in 0..10 {
            let disposer = ctx.effect(|| vec![cleanup(|| {})])?;
            disposer.dispose();
        }
        // Exactly `max_effects` live effects are allowed.
        for _ in 0..2 {
            let disposer = ctx.effect(|| vec![cleanup(|| {})])?;
            self.kept.lock().unwrap().push(disposer);
        }
        assert!(
            ctx.effect(|| vec![cleanup(|| {})]).is_err(),
            "a third *live* effect exceeds max_effects(2)"
        );
        Ok(())
    }
}

#[test]
fn disposed_effects_free_the_effect_quota_slot() {
    let root = Context::root();
    root.set_quota(FiberQuota::unlimited().max_effects(2));
    let ctx_slot = Arc::new(Mutex::new(None));
    let kept = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(
            Arc::new(EffectRecycler {
                ctx_slot: ctx_slot.clone(),
                kept: kept.clone(),
            }),
            Value::Null,
        )
        .expect("fiber");
    assert_eq!(
        fiber.state(),
        FiberState::Active,
        "released effects must not trip the quota: {:?}",
        fiber.error()
    );

    // Releasing the kept effects frees their slots for new registrations.
    for disposer in kept.lock().unwrap().drain(..) {
        disposer.dispose();
    }
    let ctx = ctx_slot.lock().unwrap().clone().expect("plugin context");
    ctx.effect(|| vec![cleanup(|| {})])
        .expect("the recovered slot is reusable");
    root.dispose_all().expect("cleanup");
}

struct ListenerRecycler {
    ctx_slot: Arc<Mutex<Option<Context>>>,
    kept: Arc<Mutex<Option<zeokeel_cordis::Disposer>>>,
}

impl Plugin for ListenerRecycler {
    fn name(&self) -> &'static str {
        "l2-listener-recycler"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        *self.ctx_slot.lock().unwrap() = Some(ctx.clone());
        // Audit R3B P2-1b: disposing the listener disposer must free the slot
        // immediately (not only when the fiber unloads).
        for _ in 0..10 {
            let disposer = ctx.on("l2.recycle", |_| zeokeel_cordis::EventResult::Continue)?;
            disposer.dispose();
            // Double dispose is idempotent and must not double-decrement.
            disposer.dispose();
        }
        // One live listener is exactly the quota limit.
        let kept = ctx.on("l2.recycle", |_| zeokeel_cordis::EventResult::Continue)?;
        assert!(
            ctx.on("l2.recycle", |_| zeokeel_cordis::EventResult::Continue)
                .is_err(),
            "a second *live* listener exceeds max_listeners(1)"
        );
        *self.kept.lock().unwrap() = Some(kept);
        Ok(())
    }
}

#[test]
fn disposed_listeners_free_the_listener_quota_slot() {
    let root = Context::root();
    root.set_quota(FiberQuota::unlimited().max_listeners(1));
    let ctx_slot = Arc::new(Mutex::new(None));
    let kept_slot = Arc::new(Mutex::new(None));
    let fiber = root
        .plugin(
            Arc::new(ListenerRecycler {
                ctx_slot: ctx_slot.clone(),
                kept: kept_slot.clone(),
            }),
            Value::Null,
        )
        .expect("fiber");
    assert_eq!(
        fiber.state(),
        FiberState::Active,
        "released listeners must not trip the quota: {:?}",
        fiber.error()
    );
    assert_eq!(
        root.events().listener_count(),
        1,
        "the kept listener is registered, the recycled ones are gone"
    );

    // Disposing the last listener frees its slot for a new listener, and the
    // quota is still enforced for *live* listeners afterwards.
    let ctx = ctx_slot.lock().unwrap().clone().expect("plugin context");
    let kept = kept_slot
        .lock()
        .unwrap()
        .take()
        .expect("kept listener disposer");
    kept.dispose();
    assert_eq!(root.events().listener_count(), 0);
    let disposer = ctx
        .on("l2.recycle2", |_| zeokeel_cordis::EventResult::Continue)
        .expect("the freed slot is reusable");
    assert!(
        ctx.on("l2.recycle2", |_| zeokeel_cordis::EventResult::Continue)
            .is_err(),
        "the live-listener quota is still enforced"
    );
    disposer.dispose();
    root.dispose_all().expect("cleanup");
}

struct OnceListenerRecycler;

impl Plugin for OnceListenerRecycler {
    fn name(&self) -> &'static str {
        "l2-once-recycler"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        // Audit R3B P2-1b: a `once` listener is removed by the event bus after
        // its first invocation, without disposing the user-facing disposer, so
        // the quota slot has to be released then as well.
        for _ in 0..5 {
            ctx.once("l2.once", |_| zeokeel_cordis::EventResult::Continue)?;
            ctx.emit("l2.once", &[]);
        }
        Ok(())
    }
}

#[test]
fn once_listener_frees_its_quota_slot_when_it_auto_removes() {
    let root = Context::root();
    root.set_quota(FiberQuota::unlimited().max_listeners(1));
    let fiber = root
        .plugin(Arc::new(OnceListenerRecycler), Value::Null)
        .expect("fiber");
    assert_eq!(
        fiber.state(),
        FiberState::Active,
        "auto-removed once listeners must free their slot: {:?}",
        fiber.error()
    );
    assert_eq!(
        root.events().listener_count(),
        0,
        "the event bus removed every once listener"
    );
    root.dispose_all().expect("cleanup");
}

/// Concurrent disposal of one listener disposer releases its slot exactly once
/// and keeps the slot reusable afterwards (audit R3B P2-1b: idempotent +
/// concurrency-safe bookkeeping, no double-decrement with the unload path).
#[test]
fn listener_quota_release_is_idempotent_and_concurrency_safe() {
    struct HolderPlugin {
        ctx_slot: Arc<Mutex<Option<Context>>>,
        disposer: Arc<Mutex<Option<zeokeel_cordis::Disposer>>>,
    }
    impl Plugin for HolderPlugin {
        fn name(&self) -> &'static str {
            "l2-listener-holder"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            *self.ctx_slot.lock().unwrap() = Some(ctx.clone());
            let disposer = ctx.on("l2.race", |_| zeokeel_cordis::EventResult::Continue)?;
            *self.disposer.lock().unwrap() = Some(disposer);
            Ok(())
        }
    }

    let root = Context::root();
    root.set_quota(FiberQuota::unlimited().max_listeners(1));
    let ctx_slot = Arc::new(Mutex::new(None));
    let disposer_slot = Arc::new(Mutex::new(None));
    let fiber = root
        .plugin(
            Arc::new(HolderPlugin {
                ctx_slot: ctx_slot.clone(),
                disposer: disposer_slot.clone(),
            }),
            Value::Null,
        )
        .expect("fiber");
    assert_eq!(fiber.state(), FiberState::Active);

    let disposer = disposer_slot
        .lock()
        .unwrap()
        .take()
        .expect("registered listener disposer");
    let first = disposer.clone();
    let second = disposer.clone();
    let a = std::thread::spawn(move || first.dispose());
    let b = std::thread::spawn(move || second.dispose());
    a.join().expect("dispose thread");
    b.join().expect("dispose thread");
    assert_eq!(root.events().listener_count(), 0);

    let ctx = ctx_slot.lock().unwrap().clone().expect("plugin context");
    let reuse = ctx
        .on("l2.race2", |_| zeokeel_cordis::EventResult::Continue)
        .expect("the slot is free exactly once after concurrent release");
    reuse.dispose();

    // The fiber unload path must not decrement again (saturating bookkeeping).
    root.dispose_all().expect("cleanup");
}

struct ChildRecycler {
    ctx_slot: Arc<Mutex<Option<Context>>>,
    kept: Arc<Mutex<Option<zeokeel_cordis::Fiber>>>,
}

impl Plugin for ChildRecycler {
    fn name(&self) -> &'static str {
        "l2-child-recycler"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        *self.ctx_slot.lock().unwrap() = Some(ctx.clone());
        // Audit R3B P2-1c: a disposed child used to stay in the parent's weak
        // child list and keep consuming `max_child_fibers` forever, so "unload
        // the old child plugin, load a new one" failed. Churn 100 children: the
        // quota must never be consumed by *historical* creations.
        for _ in 0..100 {
            let child = ctx.plugin(Arc::new(ChildPlugin), Value::Null)?;
            child.dispose()?;
        }
        let kept = ctx.plugin(Arc::new(ChildPlugin), Value::Null)?;
        assert!(
            ctx.plugin(Arc::new(ChildPlugin), Value::Null).is_err(),
            "a second *live* child exceeds max_child_fibers(1)"
        );
        *self.kept.lock().unwrap() = Some(kept);
        Ok(())
    }
}

#[test]
fn disposed_child_fibers_free_the_child_quota_slot() {
    let root = Context::root();
    root.set_quota(FiberQuota::unlimited().max_child_fibers(1));
    let ctx_slot = Arc::new(Mutex::new(None));
    let kept_slot = Arc::new(Mutex::new(None));
    let fiber = root
        .plugin(
            Arc::new(ChildRecycler {
                ctx_slot: ctx_slot.clone(),
                kept: kept_slot.clone(),
            }),
            Value::Null,
        )
        .expect("fiber");
    assert_eq!(
        fiber.state(),
        FiberState::Active,
        "disposed children must not trip the quota: {:?}",
        fiber.error()
    );

    // Disposing the last live child (which also detaches its ownership effect /
    // weak handle, R3-A4) lowers the live count: a new child is allowed, the
    // quota is still enforced for two live children.
    let ctx = ctx_slot.lock().unwrap().clone().expect("plugin context");
    let kept = kept_slot.lock().unwrap().take().expect("kept child");
    kept.dispose().expect("dispose kept child");
    let replacement = ctx
        .plugin(Arc::new(ChildPlugin), Value::Null)
        .expect("the freed child slot is reusable");
    assert!(
        ctx.plugin(Arc::new(ChildPlugin), Value::Null).is_err(),
        "the live-child quota is still enforced"
    );
    replacement.dispose().expect("dispose replacement");

    root.dispose_all().expect("cleanup");
    assert_eq!(root.registry().fiber_count(), 0);
}

// ---------------------------------------------------------------------------
// Q11: `max_effects` counts *user* effects only
// ---------------------------------------------------------------------------

/// A plugin that registers one user effect per call through `ctx.effect`.
struct UserEffects {
    runs: Arc<AtomicUsize>,
    outcomes: Arc<Mutex<Vec<Option<String>>>>,
}

impl Plugin for UserEffects {
    fn name(&self) -> &'static str {
        "l2-user-effects"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let runs = self.runs.clone();
        // First user effect: allowed under max_effects(1).
        let first = ctx.effect(move || {
            let runs = runs.clone();
            vec![cleanup(move || {
                runs.fetch_add(1, Ordering::SeqCst);
            })]
        });
        self.outcomes
            .lock()
            .unwrap()
            .push(first.as_ref().err().map(ToString::to_string));
        // Second live user effect: must be rejected (QuotaExceeded("effects")).
        let counter = self.runs.clone();
        let second = ctx.effect(move || {
            let counter = counter.clone();
            vec![cleanup(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            })]
        });
        self.outcomes
            .lock()
            .unwrap()
            .push(second.as_ref().err().map(ToString::to_string));
        Ok(())
    }
}

struct NoopPlugin;

impl Plugin for NoopPlugin {
    fn name(&self) -> &'static str {
        "l2-noop"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        Ok(())
    }
}

/// A plugin whose bookkeeping uses only kernel-internal effects: providing a
/// service, a log exporter, an event listener and an internal hook.
struct InternalOnly {
    disposers: Arc<Mutex<Vec<Disposer>>>,
}

impl Plugin for InternalOnly {
    fn name(&self) -> &'static str {
        "l2-internal-only"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let service = ctx.provide(ServiceKey::new("l2.internal"), Arc::new(7u32))?;
        let exporter = ctx.log_exporter(Arc::new(TracingExporter))?;
        let listener = ctx.on("l2.internal-event", |_args: &[Value]| EventResult::Continue)?;
        ctx.on_internal_status(Arc::new(|_fiber, _old| {}))?;
        self.disposers
            .lock()
            .unwrap()
            .extend([service, exporter, listener]);
        Ok(())
    }
}

#[test]
fn max_effects_one_still_loads_a_plugin() {
    let root = Context::root();
    root.set_quota(FiberQuota::unlimited().max_effects(1));
    let fiber = root
        .plugin(
            Arc::new(InternalOnly {
                disposers: Arc::new(Mutex::new(Vec::new())),
            }),
            Value::Null,
        )
        .expect("max_effects(1) must not block a plugin whose bookkeeping is internal");
    assert_eq!(fiber.state(), FiberState::Active);
    assert!(
        !fiber.get_effects().is_empty(),
        "the internal effects are still registered on the fiber"
    );
    root.dispose_all().expect("cleanup");
}

#[test]
fn user_effects_consume_the_max_effects_quota() {
    let root = Context::root();
    root.set_quota(FiberQuota::unlimited().max_effects(1));
    let runs = Arc::new(AtomicUsize::new(0));
    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(
            Arc::new(UserEffects {
                runs: runs.clone(),
                outcomes: outcomes.clone(),
            }),
            Value::Null,
        )
        .expect("plugin");
    assert_eq!(fiber.state(), FiberState::Active);
    let recorded = outcomes.lock().unwrap().clone();
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0], None, "the first user effect is allowed");
    assert!(
        recorded[1]
            .as_deref()
            .is_some_and(|error| error.contains("effects")),
        "the second live user effect is rejected: {recorded:?}"
    );
    root.dispose_all().expect("cleanup");
}

#[test]
fn disposed_user_effect_frees_the_quota_slot_under_max_effects_one() {
    let root = Context::root();
    root.set_quota(FiberQuota::unlimited().max_effects(1));
    let fiber = root
        .plugin(Arc::new(NoopPlugin), Value::Null)
        .expect("plugin");
    let ctx = fiber.context().expect("ctx");
    let first = ctx
        .effect(|| vec![cleanup(|| {})])
        .expect("first user effect");
    let error = match ctx.effect(|| vec![cleanup(|| {})]) {
        Err(error) => error,
        Ok(disposer) => {
            disposer.dispose();
            panic!("the second live user effect must exceed max_effects(1)");
        }
    };
    assert!(
        matches!(&error, CordisError::QuotaExceeded(kind) if kind == "effects"),
        "unexpected error: {error:?}"
    );
    first.dispose();
    // The freed slot is reusable (disposed entries are not live).
    let second = ctx
        .effect(|| vec![cleanup(|| {})])
        .expect("the slot is freed after disposal");
    drop(second);
    root.dispose_all().expect("cleanup");
}

// ---------------------------------------------------------------------------
// Injectable clock: L2 activity tracking uses the host clock
// ---------------------------------------------------------------------------

struct FakeClock {
    unix_ms: AtomicUsize,
    monotonic_ms: AtomicUsize,
}

impl FakeClock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            unix_ms: AtomicUsize::new(1_700_000_000_000),
            monotonic_ms: AtomicUsize::new(0),
        })
    }

    fn advance(&self, millis: usize) {
        self.monotonic_ms.fetch_add(millis, Ordering::SeqCst);
    }
}

impl Clock for FakeClock {
    fn now_unix_ms(&self) -> u64 {
        self.unix_ms.load(Ordering::SeqCst) as u64
    }

    fn now_monotonic_ms(&self) -> u64 {
        self.monotonic_ms.load(Ordering::SeqCst) as u64
    }
}

#[test]
fn l2_activity_uses_the_injected_clock() {
    let root = Context::root();
    let fake = FakeClock::new();
    root.set_clock(fake.clone())
        .expect("root installs the clock");

    // Fibers created after the clock is installed start their activity stamp
    // from the host clock.
    let fiber = root
        .plugin(Arc::new(NoopPlugin), Value::Null)
        .expect("plugin");
    assert_eq!(fiber.last_activity_ms(), 0, "created at fake monotonic 0");

    fake.advance(2_500);
    fiber.heartbeat();
    assert_eq!(
        fiber.last_activity_ms(),
        2_500,
        "heartbeat writes the host monotonic clock"
    );

    fake.advance(7_500);
    assert!(
        fiber.idle_for() >= Duration::from_secs(7),
        "idle duration comes from the host clock: {:?}",
        fiber.idle_for()
    );

    // The root fiber is updated together with the tree clock (it predates it).
    assert!(root.fiber().last_activity_ms() <= fake.now_monotonic_ms());
    root.fiber().heartbeat();
    fake.advance(3_000);
    assert!(
        root.fiber().idle_for() >= Duration::from_secs(3),
        "the root fiber follows the injected clock too"
    );
    root.dispose_all().expect("cleanup");
}
