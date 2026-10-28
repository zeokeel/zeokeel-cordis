//! v0.3 semantic guarantees.
//!
//! Covers: raw-config re-resolution (no compounding), provider-identity epoch
//! (dependents reactivate when the provider changes even while the service
//! stays available), revertible/classified `ctx.set`, provider-withdrawal
//! ordering, and async-disposer joining.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cordis::{
    cleanup, cleanup_async, AsyncDisposer, Context, Disposer, FiberState, Plugin, ServiceKey,
    ServiceValue,
};
use serde_json::{json, Value};

static SVC: ServiceKey<u32> = ServiceKey::new("svc");

type Trace = Arc<Mutex<Vec<&'static str>>>;

fn trace() -> Trace {
    Arc::new(Mutex::new(Vec::new()))
}

fn snapshot(trace: &Trace) -> Vec<&'static str> {
    trace.lock().unwrap().clone()
}

// ---------------------------------------------------------------------------
// raw config: resolved once per activation, never compounded
// ---------------------------------------------------------------------------

struct EchoConfig {
    seen: Arc<Mutex<Vec<String>>>,
}

impl Plugin for EchoConfig {
    fn name(&self) -> &'static str {
        "echo-config"
    }

    fn apply(&self, _ctx: Context, config: Value) -> cordis::Result<()> {
        self.seen
            .lock()
            .unwrap()
            .push(config.as_str().unwrap_or("<non-string>").to_string());
        Ok(())
    }
}

#[test]
fn raw_config_is_re_resolved_from_raw_on_every_activation() {
    let root = Context::root();
    // An `internal/config` hook that appends a marker each time it runs.
    let _hook = root
        .on_internal_config(Arc::new(|_fiber, config, next| {
            let mut value = next.next(config);
            if let Some(text) = value.as_str() {
                value = json!(format!("{text}|R"));
            }
            value
        }))
        .expect("config hook");

    let seen = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(Arc::new(EchoConfig { seen: seen.clone() }), json!("x"))
        .expect("plugin");
    fiber.restart().expect("restart");
    // v0.2 compounded resolved values (`x|R|R`); v0.3 resolves from the raw
    // caller config on every activation.
    assert_eq!(*seen.lock().unwrap(), vec!["x|R", "x|R"]);
    assert_eq!(fiber.config(), json!("x|R"));
    assert_eq!(fiber.raw_config(), json!("x"));
}

// ---------------------------------------------------------------------------
// provider-identity epoch: dependents reactivate when the version changes
// ---------------------------------------------------------------------------

struct Provider {
    set_to: Option<u32>,
    trace: Trace,
}

impl Plugin for Provider {
    fn name(&self) -> &'static str {
        "provider"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        ctx.provide(SVC, Arc::new(1u32))?;
        if let Some(value) = self.set_to {
            let service: ServiceValue = Arc::new(value);
            ctx.set("svc", service)?;
        }
        let trace = self.trace.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                trace.lock().unwrap().push("provider-cleanup")
            })]
        })?;
        Ok(())
    }
}

struct Dependent {
    applies: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<u32>>>,
    trace: Trace,
}

impl Plugin for Dependent {
    fn name(&self) -> &'static str {
        "dependent"
    }

    fn inject(&self) -> &'static [&'static str] {
        &["svc"]
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        if let Some(value) = ctx.get(SVC) {
            self.seen.lock().unwrap().push(*value);
        }
        let trace = self.trace.clone();
        let cleanup_ctx = ctx.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                // The provider must already be withdrawn while dependents
                // deactivate (withdrawal ordering).
                let withdrawn = cleanup_ctx.get(SVC).is_none();
                trace.lock().unwrap().push(if withdrawn {
                    "dep-cleanup-withdrawn"
                } else {
                    "dep-cleanup-live"
                });
            })]
        })?;
        Ok(())
    }
}

#[test]
fn provider_version_change_reactivates_dependents() {
    let root = Context::root();
    let trace = trace();
    let provider = root
        .plugin(
            Arc::new(Provider {
                set_to: Some(2),
                trace: trace.clone(),
            }),
            Value::Null,
        )
        .expect("provider");
    let applies = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let _dep = root
        .plugin(
            Arc::new(Dependent {
                applies: applies.clone(),
                seen: seen.clone(),
                trace: trace.clone(),
            }),
            Value::Null,
        )
        .expect("dependent");

    assert_eq!(*seen.lock().unwrap(), vec![2]);
    assert_eq!(applies.load(Ordering::SeqCst), 1);

    // A value change through `ctx.set` bumps the entry version; the dependent
    // must deactivate and activate again.
    let ctx = provider.context().expect("provider context");
    let service: ServiceValue = Arc::new(3u32);
    ctx.set("svc", service).expect("set");
    assert_eq!(*seen.lock().unwrap(), vec![2, 3]);
    assert_eq!(applies.load(Ordering::SeqCst), 2);
    assert_eq!(ctx.get(SVC).map(|value| *value), Some(3));
}

#[test]
fn set_is_revertible_and_classified() {
    let root = Context::root();
    let _provided = root.provide(SVC, Arc::new(1u32)).expect("provide");
    assert_eq!(root.get(SVC).map(|value| *value), Some(1));

    let service: ServiceValue = Arc::new(2u32);
    let disposer: Disposer = root.set_revertible("svc", service).expect("set");
    assert_eq!(root.get(SVC).map(|value| *value), Some(2));

    // The inverse restores the previous value.
    disposer.dispose();
    assert_eq!(root.get(SVC).map(|value| *value), Some(1));
}

#[test]
fn provider_withdrawal_precedes_binding_removal() {
    let root = Context::root();
    let trace = trace();
    let provider = root
        .plugin(
            Arc::new(Provider {
                set_to: None,
                trace: trace.clone(),
            }),
            Value::Null,
        )
        .expect("provider");
    let applies = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let dependent = root
        .plugin(
            Arc::new(Dependent {
                applies,
                seen,
                trace: trace.clone(),
            }),
            Value::Null,
        )
        .expect("dependent");
    assert_eq!(dependent.state(), FiberState::Active);

    provider.dispose().expect("provider dispose");

    // Dependents deactivate while the binding is still present but withdrawn,
    // before the provider's own cleanup removes it.
    assert_eq!(
        snapshot(&trace),
        vec!["dep-cleanup-withdrawn", "provider-cleanup"]
    );
    assert_eq!(dependent.state(), FiberState::Pending);
    assert!(root.get(SVC).is_none());
}

// ---------------------------------------------------------------------------
// async disposer joining
// ---------------------------------------------------------------------------

#[test]
fn concurrent_async_dispose_joins_the_inflight_cleanup() {
    let runs = Arc::new(AtomicUsize::new(0));
    let counter = runs.clone();
    let disposer = AsyncDisposer::new(vec![cleanup_async(move || {
        let counter = counter.clone();
        async move {
            tokio::time::sleep(std::time::Duration::from_millis(80)).await;
            counter.fetch_add(1, Ordering::SeqCst);
        }
    })]);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let started = std::time::Instant::now();
    runtime.block_on(async {
        let first = disposer.dispose();
        let second = disposer.dispose();
        futures_util::future::join(first, second).await;
    });
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(70),
        "the second dispose must wait for the in-flight cleanup"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}
