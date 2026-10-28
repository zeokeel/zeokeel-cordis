//! Phase A conformance: config schema validation, defaults and fiber updates.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use cordis::{
    ConfigSchema, Context, CordisError, FiberState, JsonConfigSchema, JsonType, Plugin, Result,
};

#[derive(Clone)]
struct ConfigPlugin {
    runs: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Value>>>,
}

impl ConfigPlugin {
    fn new() -> Self {
        Self {
            runs: Arc::new(AtomicUsize::new(0)),
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn runs(&self) -> usize {
        self.runs.load(Ordering::SeqCst)
    }

    fn seen(&self) -> Vec<Value> {
        self.seen.lock().unwrap().clone()
    }
}

impl Plugin for ConfigPlugin {
    fn name(&self) -> &'static str {
        "config-plugin"
    }

    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>> {
        Some(Arc::new(
            JsonConfigSchema::new()
                .required("model")
                .property("model", JsonType::String)
                .default_value("temperature", json!(0.2)),
        ))
    }

    fn apply(&self, _ctx: Context, config: Value) -> Result<()> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        self.seen.lock().unwrap().push(config);
        Ok(())
    }
}

#[test]
fn config_schema_validates_and_merges() {
    let root = Context::root();
    let plugin = Arc::new(ConfigPlugin::new());
    let fiber = root
        .plugin(plugin.clone(), json!({"model": "a"}))
        .expect("plugin fiber");
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(plugin.runs(), 1);

    let first = plugin.seen()[0].clone();
    assert_eq!(first["model"], json!("a"));
    assert_eq!(first["temperature"], json!(0.2), "defaults are injected");
    assert_eq!(fiber.config(), first, "fiber stores the validated config");

    // Missing required field: update fails, fiber keeps the old config and is
    // not restarted.
    let error = fiber
        .update(json!({"temperature": 0.9}))
        .expect_err("invalid update must fail");
    match error {
        CordisError::Validation(validation) => {
            assert!(
                validation
                    .issues
                    .iter()
                    .any(|issue| issue.contains("model")),
                "issues: {:?}",
                validation.issues
            );
        }
        other => panic!("expected Validation, got {other:?}"),
    }
    assert_eq!(plugin.runs(), 1, "invalid update does not restart");
    assert_eq!(fiber.config()["model"], json!("a"));

    // Valid update: normalized config is stored and the plugin is restarted.
    fiber.update(json!({"model": "b"})).expect("valid update");
    assert_eq!(plugin.runs(), 2);
    assert_eq!(fiber.config()["model"], json!("b"));
    assert_eq!(fiber.config()["temperature"], json!(0.2));
    let second = plugin.seen()[1].clone();
    assert_eq!(second["model"], json!("b"));
}

#[test]
fn invalid_initial_config_fails_fiber() {
    let root = Context::root();
    let plugin = Arc::new(ConfigPlugin::new());
    let fiber = root
        .plugin(plugin.clone(), json!({"temperature": 0.1}))
        .expect("fiber is still returned");
    assert_eq!(fiber.state(), FiberState::Failed);
    assert_eq!(plugin.runs(), 0, "plugin never runs with invalid config");
    let error = fiber.error().expect("error message");
    assert!(error.contains("model"), "error: {error}");
}

struct SchemalessPlugin {
    seen: Arc<Mutex<Vec<Value>>>,
}

impl Plugin for SchemalessPlugin {
    fn name(&self) -> &'static str {
        "schemaless"
    }

    fn apply(&self, _ctx: Context, config: Value) -> Result<()> {
        self.seen.lock().unwrap().push(config);
        Ok(())
    }
}

#[test]
fn plugin_without_schema_keeps_raw_config() {
    let root = Context::root();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let raw = json!({"anything": [1, 2, 3]});
    let fiber = root
        .plugin(
            Arc::new(SchemalessPlugin {
                seen: Arc::clone(&seen),
            }),
            raw.clone(),
        )
        .expect("plugin fiber");
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(seen.lock().unwrap()[0], raw);
    assert_eq!(fiber.config(), raw);
    // No schema means every value passes update validation.
    fiber.update(json!(null)).expect("schemaless update");
    assert_eq!(fiber.config(), json!(null));
}

#[test]
fn resolve_config_without_schema_shallow_merges() {
    let root = Context::root();
    let child = root.intercept("svc", json!({"a": 1, "b": 1}));
    let leaf = child.intercept("svc", json!({"b": 2}));

    let merged = leaf.resolve_config("svc", Some(json!({"z": 0})), Some(json!({"c": 3})));
    assert_eq!(merged, json!({"z": 0, "a": 1, "b": 2, "c": 3}));
    assert!(root
        .resolve_config("svc", None, None)
        .as_object()
        .unwrap()
        .is_empty());
    assert_eq!(
        child.resolve_config("svc", None, None),
        json!({"a": 1, "b": 1}),
        "parent intercepts stay untouched by children"
    );
}

/// A hook that replaces the config with a schema-invalid value must reject the
/// whole update (and restore the retained raw config) instead of storing it and
/// failing later on activation.
#[test]
fn update_hook_replacement_is_validated() {
    let root = Context::root();
    let plugin = Arc::new(ConfigPlugin::new());
    let fiber = root
        .plugin(plugin.clone(), json!({"model": "a"}))
        .expect("plugin fiber");
    let _hook = root
        .on_internal_update(Arc::new(
            |_fiber: &cordis::Fiber, _config: Value, _no_save: bool, next: &cordis::UpdateNext| {
                // Replace with a value that violates the plugin's schema.
                next.next(json!({"model": 42}), false);
            },
        ))
        .expect("update hook");

    let error = fiber
        .update(json!({"model": "b"}))
        .expect_err("invalid hook replacement must reject the update");
    assert!(matches!(error, CordisError::Validation(_)), "{error:?}");
    assert_eq!(fiber.state(), FiberState::Active, "fiber was not reloaded");
    assert_eq!(plugin.runs(), 1, "activation count unchanged");
    assert_eq!(
        fiber.raw_config(),
        json!({"model": "a"}),
        "raw config restored"
    );
    assert_eq!(fiber.config()["model"], json!("a"));
}
