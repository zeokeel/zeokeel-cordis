//! Round-2 adversarial attacks on the v0.3 semantics
//! (provider epochs, revertible `ctx.set`, raw config, withdrawal order,
//! async-disposer joining). Each test asserts the semantics documented in
//! `spec/CORDIS-CORE.md` (CORE-040…CORE-043) or stated in the round-2 brief.
//!
//! All tests run under a watchdog thread, so a kernel hang fails the test
//! instead of blocking `cargo test`.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use zeokeel_cordis::{
    block_on, cleanup, cleanup_async, AsyncDisposer, AsyncPlugin, BoxFuture, ConfigSchema, Context,
    CordisError, Fiber, FiberState, Plugin, ServiceKey, ServiceValue,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type Trace = Arc<Mutex<Vec<String>>>;

fn trace() -> Trace {
    Arc::new(Mutex::new(Vec::new()))
}

fn snap(trace: &Trace) -> Vec<String> {
    trace.lock().unwrap().clone()
}

fn watchdog<F, T>(label: &str, timeout: Duration, body: F) -> T
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
    let waker = Waker::noop();
    let mut task_ctx = TaskContext::from_waker(waker);
    future.poll(&mut task_ctx)
}

/// Last-known handle of a provider fiber, for "is the provider still active?"
/// probes from a dependent's `apply`.
#[derive(Clone, Default)]
struct ProviderCell(Arc<Mutex<Option<Fiber>>>);

impl ProviderCell {
    fn set(&self, fiber: &Fiber) {
        *self.0.lock().unwrap() = Some(fiber.clone());
    }
    fn state(&self) -> Option<FiberState> {
        self.0.lock().unwrap().as_ref().map(|fiber| fiber.state())
    }
}

// ---------------------------------------------------------------------------
// Generic provider / dependent plugins
// ---------------------------------------------------------------------------

struct Provider {
    key: ServiceKey<u32>,
    value: u32,
    label: &'static str,
    trace: Trace,
    set_after: Option<u32>,
}

impl Plugin for Provider {
    fn name(&self) -> &'static str {
        "at-provider"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        ctx.provide(self.key, Arc::new(self.value))?;
        if let Some(next) = self.set_after {
            ctx.set(self.key.name(), Arc::new(next))?;
        }
        let trace = self.trace.clone();
        let label = self.label;
        ctx.effect(move || {
            vec![cleanup(move || {
                trace.lock().unwrap().push(format!("{label}-cleanup"));
            })]
        })?;
        Ok(())
    }
}

struct Dependent {
    key: ServiceKey<u32>,
    inject: &'static [&'static str],
    label: &'static str,
    trace: Trace,
    applies: Arc<AtomicUsize>,
    cleanups: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Option<u32>>>>,
    provider: ProviderCell,
    provider_states: Arc<Mutex<Vec<Option<FiberState>>>>,
}

impl Plugin for Dependent {
    fn name(&self) -> &'static str {
        "at-dependent"
    }

    fn inject(&self) -> &'static [&'static str] {
        self.inject
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        self.seen
            .lock()
            .unwrap()
            .push(ctx.get(self.key).map(|value| *value));
        self.provider_states
            .lock()
            .unwrap()
            .push(self.provider.state());
        let trace = self.trace.clone();
        let cleanups = self.cleanups.clone();
        let label = self.label;
        let key = self.key;
        let cleanup_ctx = ctx.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                cleanups.fetch_add(1, Ordering::SeqCst);
                let available = cleanup_ctx.get(key).is_some();
                trace.lock().unwrap().push(format!(
                    "{label}-cleanup:{}",
                    if available { "live" } else { "withdrawn" }
                ));
            })]
        })?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 1. Provider-identity epoch
// ---------------------------------------------------------------------------

static S1: ServiceKey<u32> = ServiceKey::new("at1.svc");

/// `ctx.set` bumps the entry version: dependents reactivate exactly once per
/// mutation (no duplicate cleanup, no extra reactivation loop).
#[test]
fn at01_set_reactivates_dependent_once_per_version() {
    watchdog("at01", Duration::from_secs(20), || {
        let root = Context::root();
        let tr = trace();
        let provider = root
            .plugin(
                Arc::new(Provider {
                    key: S1,
                    value: 1,
                    label: "provider",
                    trace: tr.clone(),
                    set_after: None,
                }),
                Value::Null,
            )
            .unwrap();
        let applies = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let dep = root
            .plugin(
                Arc::new(Dependent {
                    key: S1,
                    inject: &["at1.svc"],
                    label: "dep",
                    trace: tr.clone(),
                    applies: applies.clone(),
                    cleanups: cleanups.clone(),
                    seen: seen.clone(),
                    provider: ProviderCell::default(),
                    provider_states: Arc::new(Mutex::new(Vec::new())),
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(dep.state(), FiberState::Active);
        let provider_ctx = provider.context().unwrap();
        provider_ctx.set("at1.svc", Arc::new(2u32)).unwrap();
        provider_ctx.set("at1.svc", Arc::new(3u32)).unwrap();
        assert_eq!(
            *seen.lock().unwrap(),
            vec![Some(1), Some(2), Some(3)],
            "each set must re-activate the dependent with the new value"
        );
        assert_eq!(
            applies.load(Ordering::SeqCst),
            3,
            "no extra reactivation loop: one apply per version"
        );
        assert_eq!(
            cleanups.load(Ordering::SeqCst),
            2,
            "no duplicate cleanup: one unload per superseded version"
        );
        assert_eq!(dep.state(), FiberState::Active);
    });
}

// --- set_any lane ---

trait AtGreeter: Send + Sync + 'static {
    fn greet(&self) -> String;
}

struct Greeter(&'static str);

impl AtGreeter for Greeter {
    fn greet(&self) -> String {
        self.0.to_string()
    }
}

static ANY: ServiceKey<dyn AtGreeter> = ServiceKey::new("at2.greeter");

struct AnyProvider {
    key: ServiceKey<dyn AtGreeter>,
}

impl Plugin for AnyProvider {
    fn name(&self) -> &'static str {
        "at2-provider"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        ctx.provide_any(self.key, Arc::new(Greeter("v1")))?;
        Ok(())
    }
}

struct AnyDependent {
    key: ServiceKey<dyn AtGreeter>,
    applies: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Option<String>>>>,
}

impl Plugin for AnyDependent {
    fn name(&self) -> &'static str {
        "at2-dependent"
    }

    fn inject(&self) -> &'static [&'static str] {
        &["at2.greeter"]
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        self.seen
            .lock()
            .unwrap()
            .push(ctx.get_any(self.key).map(|value| value.greet()));
        Ok(())
    }
}

/// `set_any` is a classified mutation too: it bumps the version and
/// reactivates trait-object dependents.
#[test]
fn at02_set_any_reactivates_any_lane_dependent() {
    watchdog("at02", Duration::from_secs(20), || {
        let root = Context::root();
        let provider = root
            .plugin(Arc::new(AnyProvider { key: ANY }), Value::Null)
            .unwrap();
        let applies = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let dep = root
            .plugin(
                Arc::new(AnyDependent {
                    key: ANY,
                    applies: applies.clone(),
                    seen: seen.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(dep.state(), FiberState::Active);
        provider
            .context()
            .unwrap()
            .set_any(ANY, Arc::new(Greeter("v2")))
            .expect("set_any");
        assert_eq!(
            *seen.lock().unwrap(),
            vec![Some("v1".to_string()), Some("v2".to_string())],
            "set_any must reactivate the dependent with the new trait-object value"
        );
        assert_eq!(applies.load(Ordering::SeqCst), 2);
        assert_eq!(dep.state(), FiberState::Active);
    });
}

// --- isolate shadow switch ---

static S3: ServiceKey<u32> = ServiceKey::new("at3.svc");

struct PlainProvider {
    key: ServiceKey<u32>,
    value: u32,
}

impl Plugin for PlainProvider {
    fn name(&self) -> &'static str {
        "at3-provider"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        ctx.provide(self.key, Arc::new(self.value))?;
        Ok(())
    }
}

/// Isolated-scope shadow switch: dependents resolve the child-scope provider,
/// deactivate when it disappears (no silent fallback to the parent entry),
/// and reactivate when a new child-scope provider is installed.
#[test]
fn at03_isolate_shadow_provider_switch() {
    watchdog("at03", Duration::from_secs(20), || {
        let root = Context::root();
        let _parent = root
            .plugin(Arc::new(PlainProvider { key: S3, value: 1 }), Value::Null)
            .unwrap();
        let scope = root.isolate("at3.svc");
        let child = scope
            .plugin(Arc::new(PlainProvider { key: S3, value: 2 }), Value::Null)
            .unwrap();
        let applies = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let dep = scope
            .plugin(
                Arc::new(Dependent {
                    key: S3,
                    inject: &["at3.svc"],
                    label: "dep",
                    trace: trace(),
                    applies: applies.clone(),
                    cleanups: Arc::new(AtomicUsize::new(0)),
                    seen: seen.clone(),
                    provider: ProviderCell::default(),
                    provider_states: Arc::new(Mutex::new(Vec::new())),
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(dep.state(), FiberState::Active);
        assert_eq!(*seen.lock().unwrap(), vec![Some(2)]);

        child.dispose().expect("dispose child-scope provider");
        assert_eq!(
            dep.state(),
            FiberState::Pending,
            "dependent must deactivate when its isolated-scope provider vanishes"
        );
        assert!(
            scope.get(S3).is_none(),
            "an isolated scope must not fall back to the parent provider (CORE-014)"
        );
        assert_eq!(root.get(S3).map(|value| *value), Some(1));

        let _child2 = scope
            .plugin(Arc::new(PlainProvider { key: S3, value: 3 }), Value::Null)
            .unwrap();
        assert_eq!(dep.state(), FiberState::Active);
        assert_eq!(
            *seen.lock().unwrap(),
            vec![Some(2), Some(3)],
            "dependent must reactivate against the new shadow provider"
        );
        assert!(
            !seen.lock().unwrap().contains(&Some(1)),
            "parent value leaked"
        );
    });
}

// --- provider restart / dispose / re-provide ---

static S4: ServiceKey<u32> = ServiceKey::new("at4.svc");

/// Restarting a provider reloads dependents; every unload cleanup runs exactly
/// once (no duplicate cleanup on the epoch refresh).
#[test]
fn at04_provider_restart_reloads_dependents_without_duplicate_cleanup() {
    watchdog("at04", Duration::from_secs(20), || {
        let root = Context::root();
        let provider = root
            .plugin(
                Arc::new(Provider {
                    key: S4,
                    value: 1,
                    label: "provider",
                    trace: trace(),
                    set_after: None,
                }),
                Value::Null,
            )
            .unwrap();
        let applies = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let dep = root
            .plugin(
                Arc::new(Dependent {
                    key: S4,
                    inject: &["at4.svc"],
                    label: "dep",
                    trace: trace(),
                    applies: applies.clone(),
                    cleanups: cleanups.clone(),
                    seen: seen.clone(),
                    provider: ProviderCell::default(),
                    provider_states: Arc::new(Mutex::new(Vec::new())),
                }),
                Value::Null,
            )
            .unwrap();
        provider.restart().expect("restart 1");
        provider.restart().expect("restart 2");
        provider.restart().expect("restart 3");
        assert_eq!(dep.state(), FiberState::Active);
        assert_eq!(
            applies.load(Ordering::SeqCst),
            4,
            "one activation initially plus one per provider restart"
        );
        assert_eq!(
            cleanups.load(Ordering::SeqCst),
            3,
            "exactly one dependent unload per provider restart"
        );
        assert_eq!(*seen.lock().unwrap(), vec![Some(1); 4]);
        dep.dispose().expect("dispose dependent");
        assert_eq!(cleanups.load(Ordering::SeqCst), 4);
    });
}

/// Dispose + re-provide is an identity change: dependents go Pending and come
/// back against the replacement provider.
#[test]
fn at05_provider_dispose_and_reprovide_reactivates_dependents() {
    watchdog("at05", Duration::from_secs(20), || {
        let root = Context::root();
        let tr = trace();
        let provider = root
            .plugin(
                Arc::new(Provider {
                    key: S4,
                    value: 1,
                    label: "provider",
                    trace: tr.clone(),
                    set_after: None,
                }),
                Value::Null,
            )
            .unwrap();
        let applies = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let dep = root
            .plugin(
                Arc::new(Dependent {
                    key: S4,
                    inject: &["at4.svc"],
                    label: "dep",
                    trace: tr.clone(),
                    applies: applies.clone(),
                    cleanups: Arc::new(AtomicUsize::new(0)),
                    seen: seen.clone(),
                    provider: ProviderCell::default(),
                    provider_states: Arc::new(Mutex::new(Vec::new())),
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(dep.state(), FiberState::Active);
        provider.dispose().expect("dispose provider");
        assert_eq!(dep.state(), FiberState::Pending);
        assert!(root.get(S4).is_none());

        let _provider2 = root
            .plugin(Arc::new(PlainProvider { key: S4, value: 5 }), Value::Null)
            .unwrap();
        assert_eq!(dep.state(), FiberState::Active);
        assert_eq!(*seen.lock().unwrap(), vec![Some(1), Some(5)]);
        dep.dispose().unwrap();
    });
}

// --- cycles and the fixed point ---

static CA: ServiceKey<u32> = ServiceKey::new("at6.a");
static CB: ServiceKey<u32> = ServiceKey::new("at6.b");

struct Cyclic {
    applies: Arc<AtomicUsize>,
    inject: &'static [&'static str],
    name: &'static str,
}

impl Plugin for Cyclic {
    fn name(&self) -> &'static str {
        self.name
    }

    fn inject(&self) -> &'static [&'static str] {
        self.inject
    }

    fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Cyclic dependency graph (A injects B, B injects A) terminates: both stay
/// Pending until their injected services exist, activate with bounded applies
/// once they do, and deactivate correctly when one disappears.
#[test]
fn at06_cyclic_dependencies_terminate_without_deadlock() {
    watchdog("at06", Duration::from_secs(20), || {
        let root = Context::root();
        let a_applies = Arc::new(AtomicUsize::new(0));
        let b_applies = Arc::new(AtomicUsize::new(0));
        let a = root
            .plugin(
                Arc::new(Cyclic {
                    applies: a_applies.clone(),
                    inject: &["at6.b"],
                    name: "at6-cyc-a",
                }),
                Value::Null,
            )
            .unwrap();
        let b = root
            .plugin(
                Arc::new(Cyclic {
                    applies: b_applies.clone(),
                    inject: &["at6.a"],
                    name: "at6-cyc-b",
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(a.state(), FiberState::Pending);
        assert_eq!(b.state(), FiberState::Pending);

        let da = root.provide(CA, Arc::new(1u32)).unwrap();
        let _db = root.provide(CB, Arc::new(2u32)).unwrap();
        assert_eq!(a.state(), FiberState::Active);
        assert_eq!(b.state(), FiberState::Active);
        assert!(
            a_applies.load(Ordering::SeqCst) <= 2 && b_applies.load(Ordering::SeqCst) <= 2,
            "cyclic refresh must be bounded (a={}, b={})",
            a_applies.load(Ordering::SeqCst),
            b_applies.load(Ordering::SeqCst)
        );

        da.dispose();
        root.refresh();
        assert_eq!(b.state(), FiberState::Pending, "B loses its injected A");
        assert_eq!(a.state(), FiberState::Active, "A still has its injected B");
    });
}

struct Link {
    key: ServiceKey<u32>,
    inject: &'static [&'static str],
    applies: Arc<AtomicUsize>,
}

impl Plugin for Link {
    fn name(&self) -> &'static str {
        "at7-link"
    }

    fn inject(&self) -> &'static [&'static str] {
        self.inject
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        ctx.provide(self.key, Arc::new(1u32))?;
        Ok(())
    }
}

/// A chain longer than the 64-iteration refresh bound still reaches the fixed
/// point: the bounded loop must make progress across refresh calls, and every
/// link activates exactly once.
#[test]
fn at07_long_chain_reaches_fixed_point_within_bound() {
    watchdog("at07", Duration::from_secs(30), || {
        const N: u32 = 80;
        let root = Context::root();
        let first: &'static str = "at7.svc0";
        root.provide(ServiceKey::new(first), Arc::new(0u32))
            .expect("bootstrap service");
        let mut applies = Vec::new();
        let mut prev = first;
        for i in (1..=N).rev() {
            let name: &'static str = Box::leak(format!("at7.svc{i}").into_boxed_str());
            let inject: &'static [&'static str] = Box::leak(vec![prev].into_boxed_slice());
            let key: ServiceKey<u32> = ServiceKey::new(name);
            let counter = Arc::new(AtomicUsize::new(0));
            applies.push(counter.clone());
            root.plugin(
                Arc::new(Link {
                    key,
                    inject,
                    applies: counter,
                }),
                Value::Null,
            )
            .unwrap();
            prev = name;
        }
        for _ in 0..10 {
            if root
                .registry()
                .fibers()
                .iter()
                .all(|fiber| fiber.state() == FiberState::Active)
            {
                break;
            }
            root.refresh();
        }
        let states: Vec<FiberState> = root
            .registry()
            .fibers()
            .iter()
            .map(|fiber| fiber.state())
            .collect();
        assert!(
            states.iter().all(|state| *state == FiberState::Active),
            "chain of {N} dependents did not reach the fixed point (states: {states:?})"
        );
        for (index, counter) in applies.iter().enumerate() {
            assert_eq!(
                counter.load(Ordering::SeqCst),
                1,
                "link {} activated more than once (duplicate activation loop)",
                index + 1
            );
        }
    });
}

/// A re-entrant `ctx.set` issued from an `internal/status` hook cannot livelock:
/// the refresh loop is bounded and the kernel reaches a fixed point afterwards.
#[test]
fn at08_reentrant_set_from_status_hook_is_bounded() {
    watchdog("at08", Duration::from_secs(20), || {
        let root = Context::root();
        let tr = trace();
        let provider = root
            .plugin(
                Arc::new(Provider {
                    key: S1,
                    value: 1,
                    label: "provider",
                    trace: tr.clone(),
                    set_after: None,
                }),
                Value::Null,
            )
            .unwrap();
        let applies = Arc::new(AtomicUsize::new(0));
        let dep = root
            .plugin(
                Arc::new(Dependent {
                    key: S1,
                    inject: &["at1.svc"],
                    label: "dep",
                    trace: tr.clone(),
                    applies: applies.clone(),
                    cleanups: Arc::new(AtomicUsize::new(0)),
                    seen: Arc::new(Mutex::new(Vec::new())),
                    provider: ProviderCell::default(),
                    provider_states: Arc::new(Mutex::new(Vec::new())),
                }),
                Value::Null,
            )
            .unwrap();
        let dep_id = dep.id();
        let provider_ctx = provider.context().unwrap();
        let counter = Arc::new(AtomicUsize::new(2));
        let ctx_for_hook = provider_ctx.clone();
        let counter_for_hook = counter.clone();
        let hook = root
            .on_internal_status(Arc::new(move |fiber: &Fiber, old: FiberState| {
                if fiber.id() == dep_id && old == FiberState::Active {
                    let value = counter_for_hook.fetch_add(1, Ordering::SeqCst) as u32;
                    let _ = ctx_for_hook.set("at1.svc", Arc::new(value));
                }
            }))
            .unwrap();

        let started = Instant::now();
        provider_ctx.set("at1.svc", Arc::new(100u32)).unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "re-entrant set livelocked the refresh loop"
        );
        let applies_during = applies.load(Ordering::SeqCst);
        assert!(
            applies_during <= 128,
            "refresh loop must be bounded (applies={applies_during})"
        );
        assert_eq!(dep.state(), FiberState::Active);

        drop(hook);
        root.refresh();
        let applies_settled = applies.load(Ordering::SeqCst);
        root.refresh();
        assert_eq!(
            applies.load(Ordering::SeqCst),
            applies_settled,
            "fixed point not reached after removing the re-entrant hook"
        );
    });
}

// --- no activation while the provider is unloading ---

static S9: ServiceKey<u32> = ServiceKey::new("at9.svc");

struct SetThenProvide {
    trace: Trace,
}

impl Plugin for SetThenProvide {
    fn name(&self) -> &'static str {
        "at9-provider"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        ctx.provide(S9, Arc::new(1u32))?;
        // Registered after provide: the revertible set's inverse effect is
        // drained before the provide cleanup, i.e. while the fiber is Unloading.
        ctx.set("at9.svc", Arc::new(2u32))?;
        let trace = self.trace.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                trace.lock().unwrap().push("provider-cleanup".to_string());
            })]
        })?;
        Ok(())
    }
}

struct StateProbe {
    provider: ProviderCell,
    applies: Arc<AtomicUsize>,
    cleanups: Arc<AtomicUsize>,
    trace: Trace,
}

impl Plugin for StateProbe {
    fn name(&self) -> &'static str {
        "at9-dependent"
    }

    fn inject(&self) -> &'static [&'static str] {
        &["at9.svc"]
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        let state = self.provider.state();
        self.trace
            .lock()
            .unwrap()
            .push(format!("dep-apply:provider={state:?}"));
        let cleanups = self.cleanups.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                cleanups.fetch_add(1, Ordering::SeqCst);
            })]
        })?;
        Ok(())
    }
}

/// A revertible `set` inverse must not restore a non-withdrawn entry while the
/// provider is draining: dependents may not activate against an `Unloading`
/// provider (CORE-042), and the lost activation must not run twice.
#[test]
fn at09_set_inverse_during_provider_unload_does_not_reactivate() {
    watchdog("at09", Duration::from_secs(20), || {
        let root = Context::root();
        let tr = trace();
        let provider = root
            .plugin(Arc::new(SetThenProvide { trace: tr.clone() }), Value::Null)
            .unwrap();
        let cell = ProviderCell::default();
        cell.set(&provider);
        let applies = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let dep = root
            .plugin(
                Arc::new(StateProbe {
                    provider: cell,
                    applies: applies.clone(),
                    cleanups: cleanups.clone(),
                    trace: tr.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(dep.state(), FiberState::Active);
        tr.lock().unwrap().clear();

        provider.dispose().expect("dispose provider");

        let applies = applies.load(Ordering::SeqCst);
        let cleanups = cleanups.load(Ordering::SeqCst);
        assert_eq!(dep.state(), FiberState::Pending);
        assert_eq!(
            applies,
            1,
            "dependent activated again during provider unload (apply trace: {:?})",
            snap(&tr)
        );
        assert_eq!(
            cleanups,
            1,
            "dependent unloaded more than once for one provider disposal (trace: {:?})",
            snap(&tr)
        );
        assert!(
            !snap(&tr).iter().any(|entry| entry.contains("Unloading")),
            "activation observed while the provider was Unloading: {:?}",
            snap(&tr)
        );
        assert!(root.get(S9).is_none());
    });
}

// ---------------------------------------------------------------------------
// 2. Revertible / classified `ctx.set`
// ---------------------------------------------------------------------------

static S10: ServiceKey<u32> = ServiceKey::new("at10.svc");

/// Nested sets restore in reverse registration order; the inverse disposer is
/// single-shot.
#[test]
fn at10_nested_sets_restore_in_reverse_order() {
    let root = Context::root();
    root.provide(S10, Arc::new(1u32)).expect("provide");
    let second = root
        .set_revertible("at10.svc", Arc::new(2u32))
        .expect("set v2");
    let third = root
        .set_revertible("at10.svc", Arc::new(3u32))
        .expect("set v3");
    assert_eq!(root.get(S10).map(|value| *value), Some(3));
    third.dispose();
    assert_eq!(root.get(S10).map(|value| *value), Some(2), "LIFO restore");
    third.dispose();
    assert_eq!(
        root.get(S10).map(|value| *value),
        Some(2),
        "inverse is single-shot"
    );
    second.dispose();
    assert_eq!(root.get(S10).map(|value| *value), Some(1));
    second.dispose();
    assert_eq!(root.get(S10).map(|value| *value), Some(1));
}

/// The inverse disposer can be called twice without panicking or re-running.
#[test]
fn at11_set_revertible_disposer_is_single_shot() {
    let root = Context::root();
    root.provide(S10, Arc::new(1u32)).expect("provide");
    let disposer = root
        .set_revertible("at10.svc", Arc::new(7u32))
        .expect("set");
    disposer.dispose();
    disposer.dispose();
    assert_eq!(root.get(S10).map(|value| *value), Some(1));
}

/// Only the providing fiber may mutate a service: `set` **and**
/// `set_revertible` must reject a non-owner before touching the store.
#[test]
fn at12_set_from_non_owner_fails() {
    watchdog("at12", Duration::from_secs(20), || {
        let root = Context::root();
        let provided = root.provide(S10, Arc::new(1u32)).expect("provide");
        struct Thief {
            set_forbidden: Arc<AtomicBool>,
            revertible_forbidden: Arc<AtomicBool>,
        }
        impl Plugin for Thief {
            fn name(&self) -> &'static str {
                "at12-thief"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                self.set_forbidden.store(
                    matches!(
                        ctx.set("at10.svc", Arc::new(2u32)),
                        Err(CordisError::ServiceWriteForbidden(_))
                    ),
                    Ordering::SeqCst,
                );
                self.revertible_forbidden.store(
                    matches!(
                        ctx.set_revertible("at10.svc", Arc::new(3u32)),
                        Err(CordisError::ServiceWriteForbidden(_))
                    ),
                    Ordering::SeqCst,
                );
                Ok(())
            }
        }
        let set_forbidden = Arc::new(AtomicBool::new(false));
        let revertible_forbidden = Arc::new(AtomicBool::new(false));
        let _thief = root
            .plugin(
                Arc::new(Thief {
                    set_forbidden: set_forbidden.clone(),
                    revertible_forbidden: revertible_forbidden.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        assert!(
            set_forbidden.load(Ordering::SeqCst),
            "ctx.set from a non-owner must return ServiceWriteForbidden"
        );
        assert!(
            revertible_forbidden.load(Ordering::SeqCst),
            "ctx.set_revertible from a non-owner must return ServiceWriteForbidden"
        );
        assert_eq!(
            root.get(S10).map(|value| *value),
            Some(1),
            "a non-owner must not mutate the provided value"
        );
        provided.dispose();
    });
}

/// `set` on a read-only accessor is rejected (CORE-025).
#[test]
fn at13_set_on_readonly_accessor_fails() {
    let root = Context::root();
    root.accessor("at13.acc", |_ctx| -> ServiceValue { Arc::new(7u32) })
        .expect("accessor");
    let result = root.set("at13.acc", Arc::new(8u32));
    assert!(
        matches!(result, Err(CordisError::AccessorReadOnly(_))),
        "set on a read-only accessor must be rejected: {result:?}"
    );
    assert_eq!(
        root.get(ServiceKey::<u32>::new("at13.acc")).map(|v| *v),
        Some(7)
    );
}

/// A vetoed `internal/set` hook leaves the store untouched and registers no
/// inverse effect; a later allowed set works normally.
#[test]
fn at14_vetoed_set_registers_no_inverse() {
    let root = Context::root();
    let provider = root
        .plugin(
            Arc::new(Provider {
                key: S10,
                value: 1,
                label: "provider",
                trace: trace(),
                set_after: None,
            }),
            Value::Null,
        )
        .unwrap();
    let hook = root
        .on_internal_set(Arc::new(
            |_ctx: &Context,
             _name: &str,
             _value: &ServiceValue,
             _next: &zeokeel_cordis::SetNext| false,
        ))
        .unwrap();
    let provider_ctx = provider.context().unwrap();
    provider_ctx
        .set("at10.svc", Arc::new(2u32))
        .expect("a veto is Ok (Cordis waterfall semantics)");
    assert_eq!(root.get(S10).map(|value| *value), Some(1));
    let labels: Vec<String> = provider
        .get_effects()
        .iter()
        .map(|effect| effect.label.clone())
        .collect();
    assert!(
        !labels.iter().any(|label| label == "ctx.set"),
        "a vetoed set must not register an inverse effect: {labels:?}"
    );
    hook.dispose();
    provider_ctx
        .set("at10.svc", Arc::new(2u32))
        .expect("allowed set");
    assert_eq!(root.get(S10).map(|value| *value), Some(2));
}

/// `set` from an effect cleanup that runs during unload is rejected without
/// panicking; the fiber still finalizes and the service is removed.
#[test]
fn at15_set_during_unload_fails_cleanly() {
    watchdog("at15", Duration::from_secs(20), || {
        struct SetInCleanup {
            result: Arc<Mutex<Option<bool>>>,
        }
        impl Plugin for SetInCleanup {
            fn name(&self) -> &'static str {
                "at15-provider"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                ctx.provide(S10, Arc::new(1u32))?;
                let result = self.result.clone();
                let cleanup_ctx = ctx.clone();
                ctx.effect(move || {
                    vec![cleanup(move || {
                        *result.lock().unwrap() =
                            Some(cleanup_ctx.set("at10.svc", Arc::new(2u32)).is_err());
                    })]
                })?;
                Ok(())
            }
        }
        let root = Context::root();
        let result = Arc::new(Mutex::new(None));
        let provider = root
            .plugin(
                Arc::new(SetInCleanup {
                    result: result.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        provider.dispose().expect("dispose");
        assert_eq!(
            *result.lock().unwrap(),
            Some(true),
            "set during Unloading must return an error"
        );
        assert_eq!(provider.state(), FiberState::Disposed);
        assert!(root.get(S10).is_none());
        assert_eq!(root.registry().size(), 0);
    });
}

/// A `set` issued while a dependent is mid-activation must not deadlock and the
/// dependent must settle on the newest value.
#[test]
fn at16_set_while_dependent_is_running_settles() {
    watchdog("at16", Duration::from_secs(20), || {
        static T: ServiceKey<u32> = ServiceKey::new("at16.t");
        struct Watcher {
            applies: Arc<AtomicUsize>,
            seen: Arc<Mutex<Vec<Option<u32>>>>,
        }
        impl Plugin for Watcher {
            fn name(&self) -> &'static str {
                "at16-watcher"
            }
            fn inject(&self) -> &'static [&'static str] {
                &["at16.t"]
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                self.applies.fetch_add(1, Ordering::SeqCst);
                self.seen
                    .lock()
                    .unwrap()
                    .push(ctx.get(T).map(|value| *value));
                Ok(())
            }
        }
        struct SelfSetter;
        impl Plugin for SelfSetter {
            fn name(&self) -> &'static str {
                "at16-provider"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                ctx.provide(T, Arc::new(1u32))?;
                ctx.set("at16.t", Arc::new(2u32))?;
                Ok(())
            }
        }
        let root = Context::root();
        let applies = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let watcher = root
            .plugin(
                Arc::new(Watcher {
                    applies: applies.clone(),
                    seen: seen.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(watcher.state(), FiberState::Pending);
        let provider = root.plugin(Arc::new(SelfSetter), Value::Null).unwrap();
        assert_eq!(provider.state(), FiberState::Active);
        assert_eq!(watcher.state(), FiberState::Active);
        assert_eq!(
            seen.lock().unwrap().last().cloned(),
            Some(Some(2)),
            "dependent must settle on the post-set value (seen: {:?})",
            seen.lock().unwrap()
        );
        let applies = applies.load(Ordering::SeqCst);
        assert!(applies <= 2, "bounded reactivation (applies={applies})");
    });
}

// ---------------------------------------------------------------------------
// 3. Raw config
// ---------------------------------------------------------------------------

fn add_marker_hook(root: &Context) -> zeokeel_cordis::Disposer {
    root.on_internal_config(Arc::new(
        |_fiber: &Fiber, config: Value, next: &zeokeel_cordis::ConfigNext| {
            let mut value = next.next(config);
            if let Some(text) = value.as_str() {
                value = json!(format!("{text}|R"));
            }
            value
        },
    ))
    .expect("internal/config hook")
}

struct EchoConfig {
    seen: Arc<Mutex<Vec<Value>>>,
}

impl Plugin for EchoConfig {
    fn name(&self) -> &'static str {
        "at17-echo"
    }

    fn apply(&self, _ctx: Context, config: Value) -> zeokeel_cordis::Result<()> {
        self.seen.lock().unwrap().push(config);
        Ok(())
    }
}

/// `update` must not store the *resolved* config as the new raw config: the
/// next activation resolves from the caller value exactly once (CORE-040).
#[test]
fn at17_update_does_not_compound_config() {
    let root = Context::root();
    let _hook = add_marker_hook(&root);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(Arc::new(EchoConfig { seen: seen.clone() }), json!("x"))
        .expect("plugin");
    assert_eq!(*seen.lock().unwrap(), vec![json!("x|R")]);
    assert_eq!(fiber.raw_config(), json!("x"));
    assert_eq!(fiber.config(), json!("x|R"));

    fiber.update(json!("y")).expect("update");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![json!("x|R"), json!("y|R")],
        "update must re-resolve from the raw caller config once"
    );
    assert_eq!(
        fiber.raw_config(),
        json!("y"),
        "raw config must be the caller value"
    );
    assert_eq!(fiber.config(), json!("y|R"));

    fiber.restart().expect("restart");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![json!("x|R"), json!("y|R"), json!("y|R")],
        "restart after update must not compound the already-resolved config"
    );
}

/// An `internal/update` hook that replaces the config sets the raw value; the
/// next activation resolves the replacement once.
#[test]
fn at18_update_hook_replacement_is_the_new_raw_config() {
    let root = Context::root();
    let _hook = add_marker_hook(&root);
    let _replacer = root
        .on_internal_update(Arc::new(
            |_fiber: &Fiber, _config: Value, no_save: bool, next: &zeokeel_cordis::UpdateNext| {
                let _ = next.next(json!("z"), no_save);
            },
        ))
        .expect("internal/update hook");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(Arc::new(EchoConfig { seen: seen.clone() }), json!("x"))
        .expect("plugin");
    fiber.update(json!("y")).expect("update");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![json!("x|R"), json!("z|R")],
        "the replaced config must be resolved once"
    );
    assert_eq!(fiber.raw_config(), json!("z"));
    assert_eq!(fiber.config(), json!("z|R"));
    fiber.restart().expect("restart");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![json!("x|R"), json!("z|R"), json!("z|R")]
    );
}

struct RejectNonString;

impl ConfigSchema for RejectNonString {
    fn validate(&self, value: Value) -> std::result::Result<Value, Vec<String>> {
        if value.is_string() {
            Ok(value)
        } else {
            Err(vec!["expected a string".to_string()])
        }
    }

    fn merge(&self, base: Value, _overrides: &[Value]) -> Value {
        base
    }
}

struct SchemaEcho {
    seen: Arc<Mutex<Vec<Value>>>,
}

impl Plugin for SchemaEcho {
    fn name(&self) -> &'static str {
        "at19-schema-echo"
    }

    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>> {
        Some(Arc::new(RejectNonString))
    }

    fn apply(&self, _ctx: Context, config: Value) -> zeokeel_cordis::Result<()> {
        self.seen.lock().unwrap().push(config);
        Ok(())
    }
}

/// A failed `update` must not corrupt the raw config: validation happens before
/// the raw value is committed, so a later restart still uses the old config.
#[test]
fn at19_failed_update_keeps_raw_config() {
    let root = Context::root();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(Arc::new(SchemaEcho { seen: seen.clone() }), json!("ok"))
        .expect("plugin");
    assert_eq!(fiber.raw_config(), json!("ok"));
    let failed = fiber.update(json!(42));
    assert!(failed.is_err(), "schema must reject a non-string config");
    assert_eq!(
        fiber.raw_config(),
        json!("ok"),
        "a rejected update must not replace the raw config"
    );
    fiber.restart().expect("restart with the old config");
    assert_eq!(*seen.lock().unwrap(), vec![json!("ok"), json!("ok")]);
}

struct AsyncEcho {
    seen: Arc<Mutex<Vec<Value>>>,
}

impl AsyncPlugin for AsyncEcho {
    fn name(&self) -> &'static str {
        "at20-async-echo"
    }

    fn apply_async<'a>(
        &'a self,
        _ctx: Context,
        config: Value,
    ) -> BoxFuture<'a, zeokeel_cordis::Result<()>> {
        let seen = self.seen.clone();
        Box::pin(async move {
            seen.lock().unwrap().push(config);
            Ok(())
        })
    }
}

/// `update_async` shares the raw-config contract of `update`.
#[test]
fn at20_async_update_does_not_compound_config() {
    watchdog("at20", Duration::from_secs(30), || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let root = Context::root();
            let _hook = add_marker_hook(&root);
            let seen = Arc::new(Mutex::new(Vec::new()));
            let fiber = root
                .plugin_async(Arc::new(AsyncEcho { seen: seen.clone() }), json!("x"))
                .await
                .expect("plugin_async");
            fiber.update_async(json!("y")).await.expect("update_async");
            assert_eq!(
                *seen.lock().unwrap(),
                vec![json!("x|R"), json!("y|R")],
                "update_async must resolve the raw caller config exactly once"
            );
            assert_eq!(fiber.raw_config(), json!("y"));
            assert_eq!(fiber.config(), json!("y|R"));
        });
    });
}

struct CounterSchema {
    counter: Arc<AtomicUsize>,
}

impl ConfigSchema for CounterSchema {
    fn validate(&self, value: Value) -> std::result::Result<Value, Vec<String>> {
        match value.as_str() {
            Some(text) => Ok(json!(format!(
                "{text}|S{}",
                self.counter.load(Ordering::SeqCst)
            ))),
            None => Err(vec!["expected a string".to_string()]),
        }
    }

    fn merge(&self, base: Value, _overrides: &[Value]) -> Value {
        base
    }
}

struct ChangingSchemaPlugin {
    seen: Arc<Mutex<Vec<Value>>>,
    schema: Arc<CounterSchema>,
}

impl Plugin for ChangingSchemaPlugin {
    fn name(&self) -> &'static str {
        "at21-changing-schema"
    }

    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>> {
        Some(self.schema.clone())
    }

    fn apply(&self, _ctx: Context, config: Value) -> zeokeel_cordis::Result<()> {
        self.seen.lock().unwrap().push(config);
        Ok(())
    }
}

/// The schema is re-applied on every activation: changing its behavior is
/// visible on restart, and the raw config is not compounded by the new schema.
#[test]
fn at21_schema_change_between_activations_renormalizes() {
    let root = Context::root();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let counter = Arc::new(AtomicUsize::new(1));
    let schema = Arc::new(CounterSchema {
        counter: counter.clone(),
    });
    let fiber = root
        .plugin(
            Arc::new(ChangingSchemaPlugin {
                seen: seen.clone(),
                schema: schema.clone(),
            }),
            json!("x"),
        )
        .expect("plugin");
    assert_eq!(*seen.lock().unwrap(), vec![json!("x|S1")]);
    assert_eq!(fiber.raw_config(), json!("x"));
    counter.store(2, Ordering::SeqCst);
    fiber.restart().expect("restart");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![json!("x|S1"), json!("x|S2")],
        "the schema must re-normalize the raw config on every activation"
    );
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

struct BoomSchemaPlugin;

impl Plugin for BoomSchemaPlugin {
    fn name(&self) -> &'static str {
        "at22-boom-schema"
    }

    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>> {
        Some(Arc::new(BoomSchema))
    }

    fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        Ok(())
    }
}

/// A panicking schema is contained and the fiber ends in a definite state.
#[test]
fn at22_config_schema_panic_is_contained() {
    let root = Context::root();
    let created = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        root.plugin(Arc::new(BoomSchemaPlugin), Value::Null)
    }));
    assert!(
        created.is_ok(),
        "CONTAINMENT: panicking schema escaped ctx.plugin"
    );
    let fiber = created.unwrap().expect("plugin creation must succeed");
    assert_ne!(fiber.state(), FiberState::Loading);
    let usable: ServiceKey<u8> = ServiceKey::new("at22.usable");
    root.provide(usable, Arc::new(1u8)).expect("kernel usable");
}

// ---------------------------------------------------------------------------
// 4. Withdrawal ordering
// ---------------------------------------------------------------------------

static W_A: ServiceKey<u32> = ServiceKey::new("at23.a");
static W_B: ServiceKey<u32> = ServiceKey::new("at23.b");

struct ChainLink {
    label: &'static str,
    provide: Option<ServiceKey<u32>>,
    inject_key: Option<ServiceKey<u32>>,
    inject: &'static [&'static str],
    trace: Trace,
}

impl Plugin for ChainLink {
    fn name(&self) -> &'static str {
        "at23-chain"
    }

    fn inject(&self) -> &'static [&'static str] {
        self.inject
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        if let Some(key) = self.provide {
            ctx.provide(key, Arc::new(1u32))?;
        }
        let trace = self.trace.clone();
        let label = self.label;
        let inject_key = self.inject_key;
        let provide = self.provide;
        let cleanup_ctx = ctx.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                let mut message = format!("{label}-cleanup");
                if let Some(key) = inject_key {
                    let withdrawn = cleanup_ctx.get(key).is_none();
                    let still_stored = matches!(
                        cleanup_ctx.provide(key, Arc::new(0u32)),
                        Err(CordisError::DuplicateService(_))
                    );
                    message =
                        format!("{message}:dep-withdrawn={withdrawn}:dep-stored={still_stored}");
                }
                if let Some(key) = provide {
                    let own_withdrawn = cleanup_ctx.get(key).is_none();
                    message = format!("{message}:own-withdrawn={own_withdrawn}");
                }
                trace.lock().unwrap().push(message);
            })]
        })?;
        Ok(())
    }
}

/// Nested provider chain A→B→C: C (the leaf dependent) must be torn down first
/// while its binding is still in the store but withdrawn, then B, then A.
#[test]
fn at23_nested_chain_unloads_dependents_first() {
    watchdog("at23", Duration::from_secs(20), || {
        let root = Context::root();
        let tr = trace();
        let a = root
            .plugin(
                Arc::new(ChainLink {
                    label: "A",
                    provide: Some(W_A),
                    inject_key: None,
                    inject: &[],
                    trace: tr.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let b = root
            .plugin(
                Arc::new(ChainLink {
                    label: "B",
                    provide: Some(W_B),
                    inject_key: Some(W_A),
                    inject: &["at23.a"],
                    trace: tr.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        let c = root
            .plugin(
                Arc::new(ChainLink {
                    label: "C",
                    provide: None,
                    inject_key: Some(W_B),
                    inject: &["at23.b"],
                    trace: tr.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(
            (b.state(), c.state()),
            (FiberState::Active, FiberState::Active)
        );
        a.dispose().expect("dispose A");

        assert_eq!(
            snap(&tr),
            vec![
                "C-cleanup:dep-withdrawn=true:dep-stored=true".to_string(),
                "B-cleanup:dep-withdrawn=true:dep-stored=true:own-withdrawn=true".to_string(),
                "A-cleanup:own-withdrawn=true".to_string(),
            ],
            "CORE-042: dependents must unload before their provider's cleanup, leaf first"
        );
        assert_eq!(b.state(), FiberState::Pending);
        assert_eq!(c.state(), FiberState::Pending);
    });
}

static S24: ServiceKey<u32> = ServiceKey::new("at24.svc");

/// A dependent's cleanup runs while the binding is still present but withdrawn
/// (CORE-042): `get` fails, re-providing reports `DuplicateService`.
#[test]
fn at24_dependent_cleanup_sees_withdrawn_but_present() {
    watchdog("at24", Duration::from_secs(20), || {
        let root = Context::root();
        let provider = root
            .plugin(
                Arc::new(Provider {
                    key: S24,
                    value: 1,
                    label: "provider",
                    trace: trace(),
                    set_after: None,
                }),
                Value::Null,
            )
            .unwrap();
        let observations = Arc::new(Mutex::new(Vec::<String>::new()));
        struct Probe {
            key: ServiceKey<u32>,
            observations: Arc<Mutex<Vec<String>>>,
        }
        impl Plugin for Probe {
            fn name(&self) -> &'static str {
                "at24-probe"
            }
            fn inject(&self) -> &'static [&'static str] {
                &["at24.svc"]
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                let observations = self.observations.clone();
                let key = self.key;
                let cleanup_ctx = ctx.clone();
                ctx.effect(move || {
                    vec![cleanup(move || {
                        let withdrawn = cleanup_ctx.get(key).is_none();
                        let still_stored = matches!(
                            cleanup_ctx.provide(key, Arc::new(0u32)),
                            Err(CordisError::DuplicateService(_))
                        );
                        observations
                            .lock()
                            .unwrap()
                            .push(format!("withdrawn={withdrawn}:stored={still_stored}"));
                    })]
                })?;
                Ok(())
            }
        }
        let _dep = root
            .plugin(
                Arc::new(Probe {
                    key: S24,
                    observations: observations.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        provider.dispose().expect("dispose provider");
        assert_eq!(
            *observations.lock().unwrap(),
            vec!["withdrawn=true:stored=true".to_string()],
            "dependent cleanup must observe the withdrawn-but-present binding"
        );
        assert!(root.get(S24).is_none());
    });
}

/// Withdrawal works for the `*_any` lane too, and re-providing restores the
/// dependent.
#[test]
fn at25_any_lane_withdrawal_and_reprovide() {
    watchdog("at25", Duration::from_secs(20), || {
        struct AnyProvider2;
        impl Plugin for AnyProvider2 {
            fn name(&self) -> &'static str {
                "at25-provider"
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                ctx.provide_any(ANY, Arc::new(Greeter("v1")))?;
                Ok(())
            }
        }
        struct AnyDep {
            applies: Arc<AtomicUsize>,
            seen: Arc<Mutex<Vec<Option<String>>>>,
        }
        impl Plugin for AnyDep {
            fn name(&self) -> &'static str {
                "at25-dependent"
            }
            fn inject(&self) -> &'static [&'static str] {
                &["at2.greeter"]
            }
            fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
                self.applies.fetch_add(1, Ordering::SeqCst);
                self.seen
                    .lock()
                    .unwrap()
                    .push(ctx.get_any(ANY).map(|value| value.greet()));
                Ok(())
            }
        }
        let root = Context::root();
        let provider = root.plugin(Arc::new(AnyProvider2), Value::Null).unwrap();
        let applies = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let dep = root
            .plugin(
                Arc::new(AnyDep {
                    applies: applies.clone(),
                    seen: seen.clone(),
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(dep.state(), FiberState::Active);
        provider.dispose().expect("dispose any provider");
        assert_eq!(dep.state(), FiberState::Pending);
        assert!(root.get_any(ANY).is_none());
        let _provider2 = root.plugin(Arc::new(AnyProvider2), Value::Null).unwrap();
        assert_eq!(dep.state(), FiberState::Active);
        assert_eq!(
            *seen.lock().unwrap(),
            vec![Some("v1".to_string()), Some("v1".to_string())]
        );
    });
}

/// A normal restart must not leave `withdrawn = true` entries behind: the
/// re-provided service is available to existing and new dependents.
#[test]
fn at26_restart_leaves_no_withdrawn_entry() {
    watchdog("at26", Duration::from_secs(20), || {
        let root = Context::root();
        let provider = root
            .plugin(
                Arc::new(Provider {
                    key: S24,
                    value: 1,
                    label: "provider",
                    trace: trace(),
                    set_after: None,
                }),
                Value::Null,
            )
            .unwrap();
        for _ in 0..3 {
            provider.restart().expect("restart");
            assert_eq!(
                root.get(S24).map(|value| *value),
                Some(1),
                "service must be available (not withdrawn) after restart"
            );
        }
        let applies = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let _dep = root
            .plugin(
                Arc::new(Dependent {
                    key: S24,
                    inject: &["at24.svc"],
                    label: "late-dep",
                    trace: trace(),
                    applies: applies.clone(),
                    cleanups: Arc::new(AtomicUsize::new(0)),
                    seen: seen.clone(),
                    provider: ProviderCell::default(),
                    provider_states: Arc::new(Mutex::new(Vec::new())),
                }),
                Value::Null,
            )
            .unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![Some(1)]);
        assert_eq!(applies.load(Ordering::SeqCst), 1);
        assert!(root.get(S24).is_some());
    });
}

/// Sync provider disposal cannot await async dependents; the documented flow is
/// `dispose()` + `refresh_async()`, after which the async dependent unloads.
#[test]
fn at27_async_dependent_unloads_after_refresh_async() {
    watchdog("at27", Duration::from_secs(30), || {
        struct AsyncDep {
            cleanups: Arc<AtomicUsize>,
        }
        impl AsyncPlugin for AsyncDep {
            fn name(&self) -> &'static str {
                "at27-async-dep"
            }
            fn inject(&self) -> &'static [&'static str] {
                &["at24.svc"]
            }
            fn apply_async<'a>(
                &'a self,
                ctx: Context,
                _config: Value,
            ) -> BoxFuture<'a, zeokeel_cordis::Result<()>> {
                let cleanups = self.cleanups.clone();
                Box::pin(async move {
                    ctx.effect(move || {
                        vec![cleanup(move || {
                            cleanups.fetch_add(1, Ordering::SeqCst);
                        })]
                    })?;
                    Ok(())
                })
            }
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let root = Context::root();
            let provider = root
                .plugin(
                    Arc::new(Provider {
                        key: S24,
                        value: 1,
                        label: "provider",
                        trace: trace(),
                        set_after: None,
                    }),
                    Value::Null,
                )
                .unwrap();
            let cleanups = Arc::new(AtomicUsize::new(0));
            let dep = root
                .plugin_async(
                    Arc::new(AsyncDep {
                        cleanups: cleanups.clone(),
                    }),
                    Value::Null,
                )
                .await
                .unwrap();
            assert_eq!(dep.state(), FiberState::Active);
            provider.dispose().expect("dispose provider");
            root.refresh_async().await;
            assert_eq!(dep.state(), FiberState::Pending);
            assert_eq!(cleanups.load(Ordering::SeqCst), 1);
            assert!(root.get(S24).is_none());
        });
    });
}

/// Over a set/restart/dispose sequence, every dependent activation is matched by
/// exactly one cleanup (no duplicate or skipped cleanups).
#[test]
fn at28_activation_and_cleanup_counts_match() {
    watchdog("at28", Duration::from_secs(20), || {
        let root = Context::root();
        let provider = root
            .plugin(
                Arc::new(Provider {
                    key: S24,
                    value: 1,
                    label: "provider",
                    trace: trace(),
                    set_after: None,
                }),
                Value::Null,
            )
            .unwrap();
        let applies = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let dep = root
            .plugin(
                Arc::new(Dependent {
                    key: S24,
                    inject: &["at24.svc"],
                    label: "dep",
                    trace: trace(),
                    applies: applies.clone(),
                    cleanups: cleanups.clone(),
                    seen: Arc::new(Mutex::new(Vec::new())),
                    provider: ProviderCell::default(),
                    provider_states: Arc::new(Mutex::new(Vec::new())),
                }),
                Value::Null,
            )
            .unwrap();
        provider
            .context()
            .unwrap()
            .set("at24.svc", Arc::new(2u32))
            .unwrap();
        provider.restart().unwrap();
        provider
            .context()
            .unwrap()
            .set("at24.svc", Arc::new(3u32))
            .unwrap();
        provider.restart().unwrap();
        dep.dispose().unwrap();
        assert_eq!(
            applies.load(Ordering::SeqCst),
            cleanups.load(Ordering::SeqCst),
            "every activation must have exactly one matching cleanup"
        );
    });
}

// ---------------------------------------------------------------------------
// 5. AsyncDisposer join
// ---------------------------------------------------------------------------

/// A concurrent `dispose` joins the in-flight teardown instead of returning
/// while cleanups are still running.
#[test]
fn at29_concurrent_dispose_joins_inflight() {
    watchdog("at29", Duration::from_secs(30), || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let runs = Arc::new(AtomicUsize::new(0));
            let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
            let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
            let counter = runs.clone();
            let disposer = AsyncDisposer::new(vec![cleanup_async(move || async move {
                let _ = started_tx.send(());
                let _ = gate_rx.await;
                counter.fetch_add(1, Ordering::SeqCst);
            })]);
            let first_handle = disposer.clone();
            let first = tokio::spawn(async move { first_handle.dispose().await });
            started_rx.await.expect("cleanup started");
            // The joiner runs on a dedicated thread: a joiner polled on the same
            // thread as the drainer is the documented R2-1 self-join shortcut
            // (it must return instead of waiting for itself), and tokio may
            // schedule a second task on the same worker thread. A *concurrent*
            // joiner from another thread is what must wait here.
            let second_handle = disposer.clone();
            let (joiner_tx, joiner_rx) = std::sync::mpsc::channel::<()>();
            let joiner = std::thread::spawn(move || {
                let _ = joiner_tx.send(());
                block_on(second_handle.dispose());
            });
            joiner_rx.recv().expect("joiner started");
            std::thread::sleep(Duration::from_millis(30));
            assert!(!joiner.is_finished(), "the second dispose must wait");
            assert_eq!(runs.load(Ordering::SeqCst), 0);
            let _ = gate_tx.send(());
            tokio::time::timeout(Duration::from_secs(10), first)
                .await
                .expect("first dispose")
                .expect("first task");
            joiner.join().expect("joiner must not hang");
            assert_eq!(runs.load(Ordering::SeqCst), 1);
            let started = Instant::now();
            tokio::time::timeout(Duration::from_secs(2), disposer.dispose())
                .await
                .expect("third dispose after completion");
            assert!(started.elapsed() < Duration::from_millis(300));
            assert_eq!(runs.load(Ordering::SeqCst), 1);
        });
    });
}

/// A third dispose after completion is an immediate no-op.
#[test]
fn at30_third_dispose_after_completion_is_immediate() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let runs = Arc::new(AtomicUsize::new(0));
        let counter = runs.clone();
        let disposer = AsyncDisposer::new(vec![cleanup_async(move || async move {
            counter.fetch_add(1, Ordering::SeqCst);
        })]);
        disposer.dispose().await;
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        let started = Instant::now();
        disposer.dispose().await;
        assert!(started.elapsed() < Duration::from_millis(300));
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert!(disposer.is_disposed());
    });
}

/// Cancelling the first disposer future mid-await releases joiners (new Drop
/// guard) instead of leaving them parked forever.
#[test]
fn at31_cancelled_first_dispose_releases_joiners() {
    watchdog("at31", Duration::from_secs(20), || {
        let runs = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = mpsc::channel::<()>();
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let counter = runs.clone();
        let disposer = AsyncDisposer::new(vec![cleanup_async(move || async move {
            let _ = started_tx.send(());
            let _ = gate_rx.await;
            counter.fetch_add(1, Ordering::SeqCst);
        })]);
        let first_handle = disposer.clone();
        let mut first = Box::pin(first_handle.dispose());
        assert!(poll_once(first.as_mut()).is_pending());
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("cleanup started");
        drop(first);

        let started = Instant::now();
        block_on(disposer.clone().dispose());
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "joiner hung after the first disposer was cancelled"
        );
        assert_eq!(
            runs.load(Ordering::SeqCst),
            0,
            "cancelled cleanup must not complete"
        );
        assert!(disposer.is_disposed());
        drop(disposer);
        drop(first_handle);
        drop(gate_tx);
    });
}

/// A panic in one cleanup reaches only the disposing caller; a concurrent
/// joiner completes instead of hanging.
#[test]
fn at32_panic_cleanup_releases_concurrent_joiner() {
    watchdog("at32", Duration::from_secs(30), || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let disposer = AsyncDisposer::new(vec![cleanup_async(|| async {
                tokio::time::sleep(Duration::from_millis(60)).await;
                panic!("cleanup boom");
            })]);
            let first_handle = disposer.clone();
            let first = tokio::spawn(async move { first_handle.dispose().await });
            tokio::time::sleep(Duration::from_millis(20)).await;
            let joiner_handle = disposer.clone();
            let joiner = tokio::spawn(async move { joiner_handle.dispose().await });
            let joined = tokio::time::timeout(Duration::from_secs(10), joiner)
                .await
                .expect("joiner must not hang");
            assert!(
                joined.is_ok(),
                "a cleanup panic must not propagate to the joining disposer"
            );
            let disposed = tokio::time::timeout(Duration::from_secs(10), first)
                .await
                .expect("first disposer must not hang");
            assert!(
                disposed.is_err(),
                "the cleanup panic must reach the disposing caller"
            );
        });
    });
}

/// Dropping the handles around a waiting joiner neither deadlocks nor aborts;
/// the in-flight cleanup still completes exactly once.
#[test]
fn at33_drop_of_handles_while_joiner_waits_is_safe() {
    watchdog("at33", Duration::from_secs(20), || {
        let runs = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = mpsc::channel::<()>();
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let counter = runs.clone();
        let disposer = AsyncDisposer::new(vec![cleanup_async(move || async move {
            let _ = started_tx.send(());
            let _ = gate_rx.await;
            counter.fetch_add(1, Ordering::SeqCst);
        })]);
        let worker_handle = disposer.clone();
        let worker = std::thread::spawn(move || {
            block_on(worker_handle.dispose());
        });
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("cleanup started");

        // A joiner future that parks while the teardown is in flight.
        let joiner_handle = disposer.clone();
        let mut joiner = Box::pin(joiner_handle.dispose());
        assert!(poll_once(joiner.as_mut()).is_pending());

        // Drop one handle while the joiner waits, then finish the cleanup.
        drop(disposer);
        let _ = gate_tx.send(());
        worker.join().expect("worker thread must not panic");
        assert!(
            matches!(poll_once(joiner.as_mut()), Poll::Ready(())),
            "joiner did not complete after the teardown finished"
        );
        // Dropping the last handle after completion must not abort.
        drop(joiner);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    });
}
