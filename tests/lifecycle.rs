use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use cordis::*;

static GREETING: ServiceKey<String> = ServiceKey::new("greeting");

struct NeedsGreeting {
    hits: Arc<AtomicUsize>,
}

impl Plugin for NeedsGreeting {
    fn name(&self) -> &'static str {
        "needs-greeting"
    }

    fn inject(&self) -> &'static [&'static str] {
        &["greeting"]
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let greeting = ctx.require(GREETING)?;
        assert_eq!(greeting.as_str(), "hello");
        self.hits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct EffectPlugin {
    order: Arc<Mutex<Vec<&'static str>>>,
}

impl Plugin for EffectPlugin {
    fn name(&self) -> &'static str {
        "effects"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let order = self.order.clone();
        ctx.effect(move || {
            vec![
                Box::new({
                    let order = order.clone();
                    move || order.lock().unwrap().push("first")
                }),
                Box::new({
                    let order = order.clone();
                    move || order.lock().unwrap().push("second")
                }),
            ]
        })?;
        Ok(())
    }
}

struct EventPlugin {
    owner: Arc<AtomicUsize>,
}

impl Plugin for EventPlugin {
    fn name(&self) -> &'static str {
        "events"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let owner = self.owner.clone();
        ctx.on("demo", move |_args| {
            owner.fetch_add(1, Ordering::SeqCst);
            EventResult::Bail(json!("stop"))
        })?;
        Ok(())
    }
}

struct RestartPlugin {
    runs: Arc<AtomicUsize>,
}

impl Plugin for RestartPlugin {
    fn name(&self) -> &'static str {
        "restart"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn plugin_waits_for_injected_service() {
    let root = Context::root();
    let hits = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(Arc::new(NeedsGreeting { hits: hits.clone() }), Value::Null)
        .expect("plugin fiber");
    assert_eq!(fiber.state(), FiberState::Pending);
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    let service = root
        .provide(GREETING, Arc::new("hello".to_string()))
        .expect("provide");
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    fiber.dispose().expect("dispose");
    assert_eq!(fiber.state(), FiberState::Disposed);
    assert!(
        root.get(GREETING).is_some(),
        "service was provided by the root, not the plugin fiber"
    );
    service.dispose();
    assert!(root.get(GREETING).is_none());
}

#[test]
fn effects_run_in_reverse_order_once() {
    let root = Context::root();
    let order = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(
            Arc::new(EffectPlugin {
                order: order.clone(),
            }),
            Value::Null,
        )
        .unwrap();
    assert_eq!(fiber.state(), FiberState::Active);
    fiber.dispose().unwrap();
    assert_eq!(*order.lock().unwrap(), vec!["second", "first"]);
    fiber.dispose().unwrap();
    assert_eq!(order.lock().unwrap().len(), 2);
}

#[test]
fn event_bail_and_owner_disposal() {
    let root = Context::root();
    let owner = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(
            Arc::new(EventPlugin {
                owner: owner.clone(),
            }),
            Value::Null,
        )
        .unwrap();
    assert_eq!(root.bail("demo", &[]), Some(json!("stop")));
    assert_eq!(owner.load(Ordering::SeqCst), 1);
    fiber.dispose().unwrap();
    assert_eq!(
        root.bail("demo", &[]),
        None,
        "listeners must unload with fiber"
    );
}

#[test]
fn restart_reapplies_plugin_and_update_config() {
    let root = Context::root();
    let runs = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(
            Arc::new(RestartPlugin { runs: runs.clone() }),
            json!({"a": 1}),
        )
        .unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    fiber.restart().unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    fiber.update(json!({"a": 2})).unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 3);
}

#[test]
fn isolate_shadows_parent_service() {
    let root = Context::root();
    let root_service = root
        .provide(GREETING, Arc::new("root".to_string()))
        .unwrap();
    let isolated = root.isolate("greeting");
    let child_service = isolated
        .provide(GREETING, Arc::new("child".to_string()))
        .unwrap();
    assert_eq!(root.get(GREETING).unwrap().as_str(), "root");
    assert_eq!(isolated.get(GREETING).unwrap().as_str(), "child");
    child_service.dispose();
    // Isolated scope has no provider, and must not fall back to the parent.
    assert!(isolated.get(GREETING).is_none());
    root_service.dispose();
}
