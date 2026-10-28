//! Phase D conformance: computed accessors, context metadata and labeled
//! isolation scopes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::Value;

use cordis::{Context, CordisError, Disposer, Plugin, Result, ServiceKey, ServiceValue};

static GREETING: ServiceKey<String> = ServiceKey::new("greeting");
static SCOPED: ServiceKey<String> = ServiceKey::new("scoped");

#[test]
fn context_accessor_and_set() {
    let root = Context::root();
    assert!(root
        .accessor("computed", |_ctx| Arc::new("from-accessor".to_string())
            as ServiceValue)
        .is_ok());
    assert_eq!(
        root.get_named("computed")
            .and_then(|value| value.downcast::<String>().ok())
            .map(|value| value.as_str().to_string()),
        Some("from-accessor".to_string())
    );
    // Typed lookups consult accessors before services.
    static COMPUTED: ServiceKey<String> = ServiceKey::new("computed");
    assert_eq!(
        root.get(COMPUTED).map(|value| value.as_str().to_string()),
        Some("from-accessor".to_string())
    );

    // A read-only accessor rejects writes.
    match root.set("computed", Arc::new("x".to_string()) as ServiceValue) {
        Err(CordisError::AccessorReadOnly(name)) => assert_eq!(name, "computed"),
        other => panic!("expected AccessorReadOnly, got {other:?}"),
    }

    // Settable accessor: writes go through the setter hook.
    let writes = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&writes);
    let disposer = root
        .accessor_with_setter(
            "sink",
            |_ctx| Arc::new("initial".to_string()) as ServiceValue,
            move |_ctx, _value| {
                counter.fetch_add(1, Ordering::SeqCst);
                true
            },
        )
        .expect("settable accessor");
    root.set("sink", Arc::new("updated".to_string()) as ServiceValue)
        .expect("set through accessor");
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    disposer.dispose();
    assert!(
        root.get_named("sink").is_none(),
        "disposer removes the accessor"
    );
}

#[test]
fn accessor_is_scoped_to_its_fiber() {
    struct AccessorPlugin {
        disposer: Arc<std::sync::Mutex<Option<Disposer>>>,
    }

    impl Plugin for AccessorPlugin {
        fn name(&self) -> &'static str {
            "accessor-plugin"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            let disposer = ctx.accessor("plugin-value", |_ctx| {
                Arc::new("owned".to_string()) as ServiceValue
            })?;
            *self.disposer.lock().unwrap() = Some(disposer);
            Ok(())
        }
    }

    let root = Context::root();
    let slot = Arc::new(std::sync::Mutex::new(None));
    let fiber = root
        .plugin(
            Arc::new(AccessorPlugin {
                disposer: Arc::clone(&slot),
            }),
            Value::Null,
        )
        .expect("fiber");
    assert!(root.get_named("plugin-value").is_none());
    assert!(fiber.context().unwrap().get_named("plugin-value").is_some());
    fiber.dispose().expect("dispose");
    assert!(fiber
        .context()
        .map(|ctx| ctx.get_named("plugin-value").is_none())
        .unwrap_or(true));
}

#[test]
fn extend_meta_attaches_metadata() {
    let root = Context::root();
    let mut meta = cordis::ContextMeta::new();
    meta.insert("workspace", Arc::new("/tmp/project".to_string()));
    let child = root.extend_meta(meta);
    assert_eq!(
        child
            .meta::<String>("workspace")
            .map(|value| value.as_str().to_string()),
        Some("/tmp/project".to_string())
    );
    assert!(root.meta::<String>("workspace").is_none());

    let grandchild = child.extend();
    assert_eq!(
        grandchild
            .meta::<String>("workspace")
            .map(|value| value.as_str().to_string()),
        Some("/tmp/project".to_string()),
        "metadata is inherited through the context chain"
    );
}

#[test]
fn isolate_labeled_shares_scope() {
    let root = Context::root();
    let first = root.isolate_labeled("scoped", "shared");
    let second = root.isolate_labeled("scoped", "shared");
    let other = root.isolate_labeled("scoped", "other");

    let disposer = first
        .provide(SCOPED, Arc::new("shared-value".to_string()))
        .expect("provide in shared scope");
    assert_eq!(
        second.get(SCOPED).map(|value| value.as_str().to_string()),
        Some("shared-value".to_string()),
        "same label joins the service scope"
    );
    assert!(other.get(SCOPED).is_none(), "other labels stay isolated");
    assert!(root.get(SCOPED).is_none(), "default scope is untouched");

    // Providing the same name inside one scope is a duplicate.
    let error = second
        .provide(SCOPED, Arc::new("again".to_string()))
        .err()
        .expect("duplicate provide");
    assert!(matches!(error, CordisError::DuplicateService(name) if name == "scoped"));

    disposer.dispose();
    assert!(second.get(SCOPED).is_none());
}

#[test]
fn isolate_without_label_creates_fresh_scopes() {
    let root = Context::root();
    let first = root.isolate("scoped");
    let second = root.isolate("scoped");
    first
        .provide(SCOPED, Arc::new("first".to_string()))
        .expect("provide");
    assert!(
        second.get(SCOPED).is_none(),
        "unlabeled isolates do not share"
    );
}

#[test]
fn accessor_shadows_service_of_same_name() {
    let root = Context::root();
    root.provide(GREETING, Arc::new("service".to_string()))
        .expect("provide");
    root.accessor("greeting", |_ctx| {
        Arc::new("accessor".to_string()) as ServiceValue
    })
    .expect("accessor");
    assert_eq!(
        root.get(GREETING).map(|value| value.as_str().to_string()),
        Some("accessor".to_string()),
        "accessors are resolved before provided services"
    );
}
