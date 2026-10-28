//! Phase C conformance: Registry runtime dedupe and the map-like inspection API.

use std::any::TypeId;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;

use zeokeel_cordis::{cleanup, Context, FiberState, Plugin, PluginKey, Result};

struct Incrementer {
    name: &'static str,
    runs: Arc<AtomicUsize>,
    cleanups: Arc<Mutex<Vec<&'static str>>>,
}

impl Incrementer {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            runs: Arc::new(AtomicUsize::new(0)),
            cleanups: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Plugin for Incrementer {
    fn name(&self) -> &'static str {
        self.name
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        let cleanups = Arc::clone(&self.cleanups);
        let label = self.name;
        ctx.effect_labeled("ctx.effect(incrementer)", move || {
            vec![cleanup(move || cleanups.lock().unwrap().push(label))]
        })?;
        Ok(())
    }
}

#[test]
fn registry_reuses_runtime_for_same_plugin() {
    let root = Context::root();
    let plugin = Arc::new(Incrementer::new("plugin-a"));
    let first = root
        .plugin(plugin.clone(), Value::Null)
        .expect("first fiber");
    let second = root.plugin(plugin, Value::Null).expect("second fiber");
    assert_eq!(first.state(), FiberState::Active);
    assert_eq!(second.state(), FiberState::Active);

    let registry = root.registry();
    let key = PluginKey::Named("plugin-a");
    assert_eq!(registry.size(), 1, "one runtime for both fibers");
    let runtime = registry.get(key).expect("runtime");
    assert_eq!(runtime.name, "plugin-a");
    assert_eq!(runtime.fiber_count(), 2);

    first.dispose().expect("dispose first");
    assert_eq!(
        registry.size(),
        1,
        "runtime survives while a fiber is alive"
    );
    assert_eq!(registry.get(key).unwrap().fiber_count(), 1);

    second.dispose().expect("dispose second");
    assert_eq!(registry.size(), 0, "runtime is removed with its last fiber");
    assert!(!registry.has(key));
}

#[test]
fn registry_typed_plugin_identity() {
    let root = Context::root();
    let plug = Arc::new(Incrementer::new("typed"));
    let first = root
        .plugin_typed(plug.clone(), Value::Null)
        .expect("first typed fiber");
    let second = root
        .plugin_typed(plug, Value::Null)
        .expect("second typed fiber");
    let registry = root.registry();
    let type_key = PluginKey::Type(TypeId::of::<Incrementer>());

    assert_eq!(
        registry.size(),
        1,
        "TypeId identity dedupes even separate instances"
    );
    assert_eq!(registry.get(type_key).unwrap().fiber_count(), 2);
    assert!(!registry.has(PluginKey::Named("typed")));

    first.dispose().expect("dispose first");
    second.dispose().expect("dispose second");
    assert_eq!(registry.size(), 0);
}

#[test]
fn registry_map_api() {
    let root = Context::root();
    let alpha = Arc::new(Incrementer::new("alpha"));
    let beta = Arc::new(Incrementer::new("beta"));
    let fiber_alpha = root.plugin(alpha.clone(), Value::Null).expect("alpha");
    let _fiber_beta = root.plugin(beta.clone(), Value::Null).expect("beta");
    let registry = root.registry();

    assert_eq!(registry.size(), 2);
    assert!(registry.counter() >= 2, "fiber ids are allocated");
    assert!(registry.fiber_count() >= 2);

    let mut keys = registry.keys();
    keys.sort_by_key(|key| format!("{key:?}"));
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&PluginKey::Named("alpha")));

    let values = registry.values();
    assert_eq!(values.len(), 2);
    let entries = registry.entries();
    assert_eq!(entries.len(), 2);
    assert!(entries
        .iter()
        .any(|(key, runtime)| *key == PluginKey::Named("beta") && runtime.name == "beta"));

    let visited = Arc::new(AtomicUsize::new(0));
    registry.for_each(|_key, _runtime| {
        visited.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(visited.load(Ordering::SeqCst), 2);

    // delete disposes every fiber of the runtime and returns it.
    let removed = registry.delete(PluginKey::Named("alpha")).expect("runtime");
    assert_eq!(removed.name, "alpha");
    assert_eq!(registry.size(), 1);
    assert!(!registry.has(PluginKey::Named("alpha")));
    assert_eq!(alpha.runs.load(Ordering::SeqCst), 1);
    assert_eq!(
        alpha.cleanups.lock().unwrap().as_slice(),
        &["alpha"],
        "delete runs the fiber cleanup"
    );
    assert_eq!(fiber_alpha.state(), FiberState::Disposed);
    assert!(registry.delete(PluginKey::Named("missing")).is_none());
}

#[test]
fn delete_runtime_with_no_fibers_is_rejected_gracefully() {
    let root = Context::root();
    let registry = root.registry();
    assert_eq!(registry.size(), 0);
    assert!(registry.keys().is_empty());
    assert!(registry.values().is_empty());
    assert!(registry.entries().is_empty());
    assert!(registry.delete(PluginKey::Named("nothing")).is_none());
}
