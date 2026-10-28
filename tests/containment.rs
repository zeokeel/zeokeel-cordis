//! Panic-containment conformance tests.
//!
//! Every boundary that invokes embedder/plugin code must contain a panic: the
//! kernel call returns normally and the kernel stays usable afterwards. These
//! tests cover the `internal/*` hooks, the logger exporters, the async event
//! lanes, the async waterfall dispatch, accessors and config schemas.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::Value;
use zeokeel_cordis::{
    ConfigSchema, Context, EventResult, FiberState, LogExporter, LogRecord, Plugin, ServiceKey,
    ServiceValue,
};

static PROBE: ServiceKey<u32> = ServiceKey::new("containment.probe");
static ACC: ServiceKey<u32> = ServiceKey::new("containment.acc");

struct Noop;

impl Plugin for Noop {
    fn name(&self) -> &'static str {
        "containment-noop"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        Ok(())
    }
}

/// The kernel still works after a contained panic: plugin loading and a fresh
/// service registration both succeed and the service resolves.
fn assert_kernel_usable(root: &Context) {
    assert!(
        root.plugin(Arc::new(Noop), Value::Null).is_ok(),
        "plugin loading must still work"
    );
    let key = ServiceKey::<u8>::new("containment.usable");
    assert!(root.provide(key, Arc::new(1u8)).is_ok());
    assert_eq!(root.get(key).map(|value| *value), Some(1));
}

#[test]
fn internal_plugin_hook_panic_is_contained() {
    let root = Context::root();
    let ran = Arc::new(AtomicUsize::new(0));
    root.on_internal_plugin(Arc::new(|_fiber: &zeokeel_cordis::Fiber| {
        panic!("plugin hook boom")
    }))
    .unwrap();
    let ran_after = ran.clone();
    root.on_internal_plugin(Arc::new(move |_fiber: &zeokeel_cordis::Fiber| {
        ran_after.fetch_add(1, Ordering::SeqCst);
    }))
    .unwrap();

    let fiber = root
        .plugin(Arc::new(Noop), Value::Null)
        .expect("a panicking internal/plugin hook must not escape ctx.plugin");
    assert_eq!(fiber.state(), FiberState::Active);
    assert!(ran.load(Ordering::SeqCst) >= 1, "remaining hooks still run");
    assert_kernel_usable(&root);
}

#[test]
fn internal_status_hook_panic_is_contained() {
    let root = Context::root();
    let ran = Arc::new(AtomicUsize::new(0));
    root.on_internal_status(Arc::new(
        |_fiber: &zeokeel_cordis::Fiber, _old: FiberState| panic!("status hook boom"),
    ))
    .unwrap();
    let ran_after = ran.clone();
    root.on_internal_status(Arc::new(
        move |_fiber: &zeokeel_cordis::Fiber, _old: FiberState| {
            ran_after.fetch_add(1, Ordering::SeqCst);
        },
    ))
    .unwrap();

    let fiber = root
        .plugin(Arc::new(Noop), Value::Null)
        .expect("a panicking internal/status hook must not escape activation");
    assert_eq!(fiber.state(), FiberState::Active);
    assert!(ran.load(Ordering::SeqCst) >= 1, "remaining hooks still run");
    assert_kernel_usable(&root);
}

#[test]
fn internal_service_hook_panic_is_contained() {
    let root = Context::root();
    let ran = Arc::new(AtomicUsize::new(0));
    root.on_internal_service(Arc::new(
        |_ctx: &Context, _name: &str, _value: Option<&ServiceValue>| panic!("service hook boom"),
    ))
    .unwrap();
    let ran_after = ran.clone();
    root.on_internal_service(Arc::new(
        move |_ctx: &Context, _name: &str, _value: Option<&ServiceValue>| {
            ran_after.fetch_add(1, Ordering::SeqCst);
        },
    ))
    .unwrap();

    root.provide(PROBE, Arc::new(11u32))
        .expect("a panicking internal/service hook must not escape ctx.provide");
    assert_eq!(root.get(PROBE).map(|value| *value), Some(11));
    assert!(ran.load(Ordering::SeqCst) >= 1, "remaining hooks still run");
    assert_kernel_usable(&root);
}

#[test]
fn internal_dispatch_hook_panic_is_contained() {
    let root = Context::root();
    let ran = Arc::new(AtomicUsize::new(0));
    root.on_internal_dispatch(Arc::new(|_info: &zeokeel_cordis::DispatchInfo| {
        panic!("dispatch hook boom")
    }))
    .unwrap();
    let ran_after = ran.clone();
    root.on_internal_dispatch(Arc::new(move |_info: &zeokeel_cordis::DispatchInfo| {
        ran_after.fetch_add(1, Ordering::SeqCst);
    }))
    .unwrap();

    root.emit("containment.event", &[]);
    assert!(ran.load(Ordering::SeqCst) >= 1, "remaining hooks still run");
    assert_kernel_usable(&root);
}

#[test]
fn internal_get_hook_panic_falls_through() {
    let root = Context::root();
    root.on_internal_get(Arc::new(
        |_ctx: &Context,
         _name: &str,
         _next: &zeokeel_cordis::GetNext<'_>|
         -> Option<ServiceValue> { panic!("get hook boom") },
    ))
    .unwrap();

    root.provide(PROBE, Arc::new(13u32)).unwrap();
    assert_eq!(
        root.get(PROBE).map(|value| *value),
        Some(13),
        "a panicking internal/get hook falls through to the terminal"
    );
    assert_kernel_usable(&root);
}

#[test]
fn internal_config_hook_panic_falls_through() {
    let root = Context::root();
    root.on_internal_config(Arc::new(
        |_fiber: &zeokeel_cordis::Fiber,
         _config: Value,
         _next: &zeokeel_cordis::ConfigNext<'_>|
         -> Value { panic!("config hook boom") },
    ))
    .unwrap();

    root.plugin(Arc::new(Noop), Value::Null)
        .expect("a panicking internal/config hook must not escape ctx.plugin");
    assert_kernel_usable(&root);
}

struct BoomExporter;

impl LogExporter for BoomExporter {
    fn export(&self, _record: &LogRecord) {
        panic!("exporter boom")
    }
}

struct CountExporter(Arc<AtomicUsize>);

impl LogExporter for CountExporter {
    fn export(&self, _record: &LogRecord) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn logger_exporter_panic_is_contained() {
    let root = Context::root();
    let hits = Arc::new(AtomicUsize::new(0));
    root.log_exporter(Arc::new(BoomExporter)).unwrap();
    root.log_exporter(Arc::new(CountExporter(hits.clone())))
        .unwrap();

    root.logger().info("contained", Value::Null);
    assert_eq!(hits.load(Ordering::SeqCst), 1, "other exporters still run");
    assert_eq!(
        root.logger_service().records().len(),
        1,
        "the buffer write still happens"
    );
    assert_kernel_usable(&root);
}

#[test]
fn parallel_async_listener_panic_is_contained() {
    let root = Context::root();
    let hits = Arc::new(AtomicUsize::new(0));
    root.on_async("containment.par", |_args| async {
        panic!("async listener boom")
    })
    .unwrap();
    let hits_after = hits.clone();
    root.on_async("containment.par", move |_args| {
        let hits_after = hits_after.clone();
        async move {
            hits_after.fetch_add(1, Ordering::SeqCst);
            EventResult::Continue
        }
    })
    .unwrap();

    zeokeel_cordis::block_on(root.events().parallel_async("containment.par", &[], None));
    assert_eq!(hits.load(Ordering::SeqCst), 1, "other listeners still run");
    assert_kernel_usable(&root);
}

#[test]
fn serial_async_listener_panic_is_contained() {
    let root = Context::root();
    let hits = Arc::new(AtomicUsize::new(0));
    root.on_async("containment.ser", |_args| async {
        panic!("async listener boom")
    })
    .unwrap();
    let hits_after = hits.clone();
    root.on_async("containment.ser", move |_args| {
        let hits_after = hits_after.clone();
        async move {
            hits_after.fetch_add(1, Ordering::SeqCst);
            EventResult::Continue
        }
    })
    .unwrap();

    let bailed = zeokeel_cordis::block_on(root.events().serial_async("containment.ser", &[], None));
    assert!(bailed.is_none(), "a panicking listener does not bail");
    assert_eq!(hits.load(Ordering::SeqCst), 1, "other listeners still run");
    assert_kernel_usable(&root);
}

#[test]
fn accessor_getter_panic_is_contained() {
    let root = Context::root();
    root.accessor(ACC.name(), |_ctx| -> ServiceValue {
        panic!("accessor boom")
    })
    .unwrap();

    assert!(
        root.get(ACC).is_none(),
        "a panicking getter degrades to an unavailable value"
    );
    let _ = root.get_named(ACC.name());
    assert_kernel_usable(&root);
}

struct BoomSchema;

impl ConfigSchema for BoomSchema {
    fn validate(&self, _value: Value) -> std::result::Result<Value, Vec<String>> {
        panic!("schema validate boom")
    }

    fn merge(&self, _base: Value, _overrides: &[Value]) -> Value {
        panic!("schema merge boom")
    }
}

struct SchemaPlugin;

impl Plugin for SchemaPlugin {
    fn name(&self) -> &'static str {
        "containment-schema"
    }

    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>> {
        Some(Arc::new(BoomSchema))
    }

    fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        Ok(())
    }
}

#[test]
fn config_schema_panic_is_contained() {
    let root = Context::root();
    root.plugin(Arc::new(SchemaPlugin), Value::Null)
        .expect("a panicking ConfigSchema::validate must not escape ctx.plugin");
    assert_kernel_usable(&root);
}
