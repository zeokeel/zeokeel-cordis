//! `Registry::replace` / `Fiber::replace_plugin` (CORE-048): swap a plugin
//! implementation in place while keeping fiber identity and reloading.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;
use zeokeel_cordis::{
    cleanup, cleanup_async, AsyncPlugin, BoxFuture, Context, FiberState, Plugin, PluginKey,
    ServiceKey,
};

static SVC: ServiceKey<u32> = ServiceKey::new("replace-svc");

type Trace = Arc<Mutex<Vec<&'static str>>>;

fn trace() -> Trace {
    Arc::new(Mutex::new(Vec::new()))
}

struct ProviderA {
    trace: Trace,
}

impl Plugin for ProviderA {
    fn name(&self) -> &'static str {
        "replace-a"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        ctx.provide(SVC, Arc::new(1u32))?;
        let trace = self.trace.clone();
        ctx.effect(move || vec![cleanup(move || trace.lock().unwrap().push("a-cleanup"))])?;
        Ok(())
    }
}

struct ProviderB {
    trace: Trace,
    value: u32,
}

impl Plugin for ProviderB {
    fn name(&self) -> &'static str {
        "replace-b"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        ctx.provide(SVC, Arc::new(self.value))?;
        let trace = self.trace.clone();
        ctx.effect(move || vec![cleanup(move || trace.lock().unwrap().push("b-cleanup"))])?;
        Ok(())
    }
}

struct ProviderBAsync {
    value: u32,
    applied: Arc<AtomicUsize>,
}

impl AsyncPlugin for ProviderBAsync {
    fn name(&self) -> &'static str {
        "replace-b-async"
    }

    fn apply_async<'a>(
        &'a self,
        ctx: Context,
        _config: Value,
    ) -> BoxFuture<'a, zeokeel_cordis::Result<()>> {
        let value = self.value;
        let applied = self.applied.clone();
        Box::pin(async move {
            applied.fetch_add(1, Ordering::SeqCst);
            ctx.provide_any(SVC, Arc::new(value))?;
            let counter = applied.clone();
            ctx.effect_async(move || {
                vec![cleanup_async(move || async move {
                    counter.fetch_add(100, Ordering::SeqCst);
                })]
            })?;
            Ok(())
        })
    }
}

struct Dependent {
    applies: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<u32>>>,
}

impl Plugin for Dependent {
    fn name(&self) -> &'static str {
        "replace-dependent"
    }

    fn inject(&self) -> &'static [&'static str] {
        &["replace-svc"]
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        if let Some(value) = ctx.get(SVC) {
            self.seen.lock().unwrap().push(*value);
        }
        Ok(())
    }
}

fn named(key: &'static str) -> PluginKey {
    PluginKey::Named(key)
}

#[test]
fn replace_rebinds_fibers_and_reloads() {
    let root = Context::root();
    let trace = trace();
    let fiber = root
        .plugin(
            Arc::new(ProviderA {
                trace: trace.clone(),
            }),
            Value::Null,
        )
        .expect("provider a");
    let fiber_id = fiber.id();
    let applies = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let dependent = root
        .plugin(
            Arc::new(Dependent {
                applies: applies.clone(),
                seen: seen.clone(),
            }),
            Value::Null,
        )
        .expect("dependent");
    assert_eq!(*seen.lock().unwrap(), vec![1]);

    let replaced = root
        .registry()
        .replace(
            named("replace-a"),
            Arc::new(ProviderB {
                trace: trace.clone(),
                value: 2,
            }),
        )
        .expect("replace");
    assert_eq!(replaced.len(), 1);
    assert_eq!(replaced[0].id(), fiber_id, "fiber identity is preserved");
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(runner_value(&root), Some(2), "new implementation is active");
    // The dependent was re-evaluated against the new provider.
    assert_eq!(applies.load(Ordering::SeqCst), 2);
    assert_eq!(*seen.lock().unwrap(), vec![1, 2]);
    // Old effects were cleaned up and the runtime was re-keyed.
    assert!(snapshot(&trace).contains(&"a-cleanup"));
    assert!(!root.registry().has(named("replace-a")));
    assert!(root.registry().has(named("replace-b")));
    assert_eq!(dependent.state(), FiberState::Active);
}

fn snapshot(trace: &Trace) -> Vec<&'static str> {
    trace.lock().unwrap().clone()
}

fn runner_value(root: &Context) -> Option<u32> {
    root.get(SVC).map(|value| *value)
}

#[test]
fn replace_unknown_runtime_errors() {
    let root = Context::root();
    let error = root
        .registry()
        .replace(
            named("nope"),
            Arc::new(ProviderB {
                trace: trace(),
                value: 9,
            }),
        )
        .err()
        .expect("unknown key must fail");
    assert!(matches!(
        error,
        zeokeel_cordis::CordisError::InvalidPlugin(_)
    ));
}

#[test]
fn replace_async_plugin_via_registry() {
    let root = Context::root();
    let _a = root
        .plugin(Arc::new(ProviderA { trace: trace() }), Value::Null)
        .expect("provider a");
    let applied = Arc::new(AtomicUsize::new(0));
    let replaced = zeokeel_cordis::block_on(root.registry().replace_async(
        named("replace-a"),
        Arc::new(ProviderBAsync {
            value: 3,
            applied: applied.clone(),
        }),
    ))
    .expect("async replace");
    assert_eq!(replaced.len(), 1);
    assert_eq!(applied.load(Ordering::SeqCst), 1);
    assert_eq!(root.get_any(SVC).map(|value| *value), Some(3));
    assert_eq!(replaced[0].state(), FiberState::Active);
}

#[test]
fn replace_on_disposed_fiber_fails_safely() {
    let root = Context::root();
    let fiber = root
        .plugin(Arc::new(ProviderA { trace: trace() }), Value::Null)
        .expect("provider a");
    fiber.dispose().expect("dispose");
    let error = root
        .registry()
        .replace(
            named("replace-a"),
            Arc::new(ProviderB {
                trace: trace(),
                value: 4,
            }),
        )
        .err()
        .expect("replace must not resurrect a disposed fiber");
    // The disposed fiber's runtime is already unregistered, so replace reports
    // the missing runtime (or Disposed if it raced the finalization).
    assert!(matches!(
        error,
        zeokeel_cordis::CordisError::InvalidPlugin(_) | zeokeel_cordis::CordisError::Disposed
    ));
}

#[test]
fn replace_keeps_the_raw_config() {
    let root = Context::root();
    let _hook = root
        .on_internal_config(Arc::new(|_fiber, config, next| {
            let mut value = next.next(config);
            if let Some(text) = value.as_str() {
                value = serde_json::json!(format!("{text}|R"));
            }
            value
        }))
        .expect("config hook");
    struct EchoConfig {
        seen: Arc<Mutex<Vec<String>>>,
    }
    impl Plugin for EchoConfig {
        fn name(&self) -> &'static str {
            "replace-echo-config"
        }
        fn apply(&self, _ctx: Context, config: Value) -> zeokeel_cordis::Result<()> {
            self.seen
                .lock()
                .unwrap()
                .push(config.as_str().unwrap_or("?").to_string());
            Ok(())
        }
    }
    let first_seen = Arc::new(Mutex::new(Vec::new()));
    let _first = root
        .plugin(
            Arc::new(EchoConfig {
                seen: first_seen.clone(),
            }),
            serde_json::json!("x"),
        )
        .expect("first");
    let second_seen = Arc::new(Mutex::new(Vec::new()));
    root.registry()
        .replace(
            named("replace-echo-config"),
            Arc::new(EchoConfig {
                seen: second_seen.clone(),
            }),
        )
        .expect("replace");
    // The raw caller config is retained and resolved once per activation.
    assert_eq!(*second_seen.lock().unwrap(), vec!["x|R"]);
}

#[test]
fn fiber_replace_plugin_swaps_one_fiber() {
    let root = Context::root();
    let fiber = root
        .plugin(Arc::new(ProviderA { trace: trace() }), Value::Null)
        .expect("provider a");
    fiber
        .replace_plugin(Arc::new(ProviderB {
            trace: trace(),
            value: 7,
        }))
        .expect("fiber replace");
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(runner_value(&root), Some(7));
}
