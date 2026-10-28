//! Phase A conformance: service lifecycle, availability checks and schema
//! aware `resolveConfig`.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use zeokeel_cordis::{
    AvailabilityCheck, ConfigSchema, Context, CordisError, FiberState, JsonConfigSchema, Plugin,
    Result, Service, ServiceKey,
};

static GREETER: ServiceKey<Greeter> = ServiceKey::new("greeter");
static GATED: ServiceKey<GatedService> = ServiceKey::new("gated");

struct Greeter;

impl Service for Greeter {
    fn name(&self) -> &'static str {
        "greeter"
    }
}

struct ProvidesGreeter {
    ctx_slot: Arc<Mutex<Option<Context>>>,
}

impl Plugin for ProvidesGreeter {
    fn name(&self) -> &'static str {
        "provides-greeter"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        *self.ctx_slot.lock().unwrap() = Some(ctx.clone());
        ctx.provide_service(Arc::new(Greeter))?;
        Ok(())
    }
}

#[test]
fn service_auto_register_and_dispose() {
    let root = Context::root();
    let slot = Arc::new(Mutex::new(None));
    let fiber = root
        .plugin(
            Arc::new(ProvidesGreeter {
                ctx_slot: Arc::clone(&slot),
            }),
            Value::Null,
        )
        .expect("plugin fiber");
    assert_eq!(fiber.state(), FiberState::Active);

    let plugin_ctx = slot.lock().unwrap().clone().expect("plugin context");
    assert!(
        plugin_ctx.get(GREETER).is_some(),
        "provide_service registers the instance for typed lookups"
    );

    fiber.dispose().expect("dispose");
    assert!(
        plugin_ctx.get(GREETER).is_none(),
        "service unregisters with its owning fiber"
    );
}

#[test]
fn provided_service_disposer_removes_service() {
    let root = Context::root();
    let disposer = root
        .provide_service(Arc::new(Greeter))
        .expect("provide service");
    assert!(root.get(GREETER).is_some());
    disposer.dispose();
    assert!(root.get(GREETER).is_none());
}

#[test]
fn duplicate_service_is_rejected() {
    let root = Context::root();
    root.provide_service(Arc::new(Greeter))
        .expect("first provide");
    let error = root
        .provide_service(Arc::new(Greeter))
        .err()
        .expect("second provide must fail");
    match error {
        CordisError::DuplicateService(name) => assert_eq!(name, "greeter"),
        other => panic!("expected DuplicateService, got {other:?}"),
    }
}

struct GatedService {
    open: Arc<AtomicBool>,
}

impl Service for GatedService {
    fn name(&self) -> &'static str {
        "gated"
    }

    fn check(&self) -> Option<AvailabilityCheck> {
        let open = Arc::clone(&self.open);
        Some(Arc::new(move || open.load(Ordering::SeqCst)))
    }
}

struct DependsOnGated {
    hits: Arc<AtomicUsize>,
}

impl Plugin for DependsOnGated {
    fn name(&self) -> &'static str {
        "depends-on-gated"
    }

    fn inject(&self) -> &'static [&'static str] {
        &["gated"]
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.require(GATED)?;
        self.hits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn service_check_gates_dependents() {
    let root = Context::root();
    let open = Arc::new(AtomicBool::new(false));
    let _service = root
        .provide_service(Arc::new(GatedService {
            open: Arc::clone(&open),
        }))
        .expect("provide gated service");

    let hits = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(
            Arc::new(DependsOnGated {
                hits: Arc::clone(&hits),
            }),
            Value::Null,
        )
        .expect("dependent fiber");
    assert_eq!(fiber.state(), FiberState::Pending);
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    open.store(true, Ordering::SeqCst);
    root.refresh();
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    open.store(false, Ordering::SeqCst);
    root.refresh();
    assert_eq!(
        fiber.state(),
        FiberState::Pending,
        "closing the check unloads dependents"
    );

    open.store(true, Ordering::SeqCst);
    root.refresh();
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "reopening the check re-applies the plugin"
    );
}

struct SchemaService;

impl Service for SchemaService {
    fn name(&self) -> &'static str {
        "schema-svc"
    }

    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>> {
        Some(Arc::new(
            JsonConfigSchema::new()
                .required("model")
                .default_value("temperature", json!(0.2)),
        ))
    }
}

#[test]
fn service_resolve_config_merges_intercepts() {
    let root = Context::root();
    let child = root.intercept("schema-svc", json!({"top_p": 0.9}));
    let leaf = child.intercept("schema-svc", json!({"model": "served"}));
    let service = Arc::new(SchemaService);
    let _disposer = leaf.provide_service(Arc::clone(&service)).expect("provide");

    let merged = service.resolve_config(
        &leaf,
        Some(json!({"model": "base", "temperature": 1.0})),
        Some(json!({"top_p": 0.5})),
    );
    assert_eq!(
        merged["model"],
        json!("served"),
        "child intercept beats base"
    );
    assert_eq!(merged["top_p"], json!(0.5), "head beats every intercept");
    assert_eq!(
        merged["temperature"],
        json!(1.0),
        "schema defaults never override explicit values"
    );

    // Defaults are injected by the schema merge.
    let with_defaults = service.resolve_config(&leaf, None, None);
    assert_eq!(with_defaults["temperature"], json!(0.2));
}

static SIBLING: ServiceKey<String> = ServiceKey::new("sibling-service");

struct ProvidesSibling;

impl Plugin for ProvidesSibling {
    fn name(&self) -> &'static str {
        "provides-sibling"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.provide(SIBLING, Arc::new("from-sibling".to_string()))?;
        Ok(())
    }
}

struct ConsumesSibling {
    hits: Arc<AtomicUsize>,
}

impl Plugin for ConsumesSibling {
    fn name(&self) -> &'static str {
        "consumes-sibling"
    }

    fn inject(&self) -> &'static [&'static str] {
        &["sibling-service"]
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        assert_eq!(ctx.require(SIBLING)?.as_str(), "from-sibling");
        self.hits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn service_is_visible_across_sibling_fibers() {
    // Official Cordis stores implementations on the root store: a service
    // provided by one plugin fiber is visible to sibling fibers and the root.
    let root = Context::root();
    let hits = Arc::new(AtomicUsize::new(0));
    let consumer = root
        .plugin(
            Arc::new(ConsumesSibling {
                hits: Arc::clone(&hits),
            }),
            Value::Null,
        )
        .expect("consumer fiber");
    assert_eq!(consumer.state(), FiberState::Pending);

    let provider = root
        .plugin(Arc::new(ProvidesSibling), Value::Null)
        .expect("provider fiber");
    assert_eq!(provider.state(), FiberState::Active);
    assert_eq!(
        consumer.state(),
        FiberState::Active,
        "sibling-provided service unblocks the dependent"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(root.get(SIBLING).unwrap().as_str(), "from-sibling");

    provider.dispose().expect("dispose provider");
    assert!(root.get(SIBLING).is_none());
    assert_eq!(
        consumer.state(),
        FiberState::Pending,
        "removing the service unloads dependents"
    );
}

#[test]
fn default_scope_rejects_duplicates_but_isolate_shadows() {
    let root = Context::root();
    root.provide(GREETER, Arc::new(Greeter))
        .expect("default scope");
    let error = root
        .provide(GREETER, Arc::new(Greeter))
        .err()
        .expect("default scope duplicate");
    assert!(matches!(error, CordisError::DuplicateService(name) if name == "greeter"));

    // The same name is legal in an isolation scope (shadows the default scope).
    let scoped = root.isolate("greeter");
    scoped
        .provide(GREETER, Arc::new(Greeter))
        .expect("isolated scope provider");
    assert!(scoped.get(GREETER).is_some());
}

#[test]
fn availability_check_may_read_other_services() {
    // A Service.check that re-enters the context (reads another service) must
    // not deadlock: the store lock is released before the check runs.
    static INNER: ServiceKey<String> = ServiceKey::new("inner-svc");
    static OUTER: ServiceKey<String> = ServiceKey::new("outer-svc");

    let root = Context::root();
    root.provide(INNER, Arc::new("inner".to_string()))
        .expect("provide inner");

    let probe = root.clone();
    root.provide_checked(OUTER, Arc::new("outer".to_string()), move || {
        // Re-enters lookup while OUTER's lookup is being evaluated.
        probe.service_available("inner-svc")
    })
    .expect("provide checked outer");

    let (tx, rx) = std::sync::mpsc::channel();
    let worker = root.clone();
    std::thread::spawn(move || {
        let _ = tx.send(worker.get(OUTER).map(|value| value.as_str().to_string()));
    });
    let resolved = rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("availability check must not deadlock on the service store");
    assert_eq!(resolved.as_deref(), Some("outer"));
}
