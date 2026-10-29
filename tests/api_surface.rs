//! Direct coverage for public APIs that previously had no dedicated test
//! (0.5.0 "zero leftover" batch) plus the general extension primitives added in
//! the same batch.
//!
//! - `config::JsonConfigSchema::required_keys`
//! - `context::Context::{attach_effect, effect_future_async, on_lifecycle}`
//! - `fiber::Fiber::is_active`
//! - `logger::LoggerService::{is_enabled, exporter_count, clear_buffer}`
//! - `reflect::ContextMeta::insert_any`
//! - `registry::PluginKind::as_async` — **removed in 0.5.0** (the `PluginKind`
//!   enum is now crate-internal; use the `Plugin` / `AsyncPlugin` traits)
//! - `CordisError::plugin` (structured plugin error constructor/Display)
//! - `Events::internal` / `InternalBus` raw-bus ownership

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use zeokeel_cordis::{
    block_on, cleanup, cleanup_async, AsyncPlugin, Clock, ConfigSchema, Context, CordisError,
    Disposer, FiberState, InternalBus, JsonConfigSchema, LifecycleEvent, LogExporter, LogLevel,
    LogRecord, LoggerService, Plugin, Result, SystemClock,
};

// ---------------------------------------------------------------------------
// config::JsonConfigSchema::required_keys
// ---------------------------------------------------------------------------

#[test]
fn api_required_keys_reports_declared_required_fields() {
    let schema = JsonConfigSchema::new()
        .required("model")
        .required("endpoint");
    assert_eq!(
        schema.required_keys(),
        &["model".to_string(), "endpoint".to_string()]
    );

    let permissive = JsonConfigSchema::new();
    assert!(permissive.required_keys().is_empty());
    assert!(permissive.validate(json!({})).is_ok());

    // A required key injected by a default is satisfied, not reported missing.
    let defaults = JsonConfigSchema::new()
        .required("model")
        .default_value("model", json!("default-model"));
    assert_eq!(defaults.required_keys(), &["model".to_string()]);
    assert_eq!(
        defaults
            .validate(json!({}))
            .expect("a default satisfies required"),
        json!({ "model": "default-model" })
    );
}

// ---------------------------------------------------------------------------
// fiber::Fiber::is_active
// ---------------------------------------------------------------------------

struct Noop;

impl Plugin for Noop {
    fn name(&self) -> &'static str {
        "api-noop"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        Ok(())
    }
}

struct NeedsMissing {
    started: Arc<AtomicUsize>,
}

impl Plugin for NeedsMissing {
    fn name(&self) -> &'static str {
        "api-needs-missing"
    }

    fn inject(&self) -> &'static [&'static str] {
        &["api.never-provided"]
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn api_fiber_is_active_tracks_lifecycle_states() {
    let root = Context::root();
    assert!(root.fiber().is_active(), "root fiber starts Active");

    let fiber = root.plugin(Arc::new(Noop), Value::Null).expect("plugin");
    assert!(fiber.is_active());
    assert_eq!(fiber.state(), FiberState::Active);
    fiber.dispose().expect("dispose");
    assert!(!fiber.is_active(), "a Disposed fiber is not active");

    let started = Arc::new(AtomicUsize::new(0));
    let pending = root
        .plugin(
            Arc::new(NeedsMissing {
                started: started.clone(),
            }),
            Value::Null,
        )
        .expect("fiber");
    assert_eq!(pending.state(), FiberState::Pending);
    assert!(!pending.is_active());
    assert_eq!(started.load(Ordering::SeqCst), 0, "apply must not run");
    root.dispose_all().expect("cleanup");
}

// ---------------------------------------------------------------------------
// context::Context::attach_effect
// ---------------------------------------------------------------------------

struct AttachTarget {
    attached: Arc<AtomicUsize>,
}

impl Plugin for AttachTarget {
    fn name(&self) -> &'static str {
        "api-attach-target"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let counter = self.attached.clone();
        let disposer = Disposer::new(vec![cleanup(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        })]);
        ctx.attach_effect(disposer, "api.attached")?;
        Ok(())
    }
}

#[test]
fn api_attach_effect_owns_an_external_disposer() {
    let root = Context::root();
    let attached = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(
            Arc::new(AttachTarget {
                attached: attached.clone(),
            }),
            Value::Null,
        )
        .expect("plugin");
    let effects = fiber.get_effects();
    assert!(
        effects.iter().any(|meta| meta.label == "api.attached"),
        "the attached effect is reported under its label: {effects:?}"
    );

    // The fiber owns the attached effect: unloading runs the cleanup exactly once.
    let ctx = fiber.context().expect("fiber context");
    fiber.dispose().expect("dispose");
    assert_eq!(attached.load(Ordering::SeqCst), 1);

    // Attaching to the (now unloaded) plugin context is rejected.
    let error = ctx
        .attach_effect(Disposer::new(Vec::new()), "api.late")
        .expect_err("an unloading/disposed fiber must reject new effects");
    assert!(
        matches!(error, CordisError::InactiveEffect),
        "unexpected error: {error:?}"
    );
    root.dispose_all().expect("cleanup");
}

// ---------------------------------------------------------------------------
// context::Context::effect_future_async
// ---------------------------------------------------------------------------

#[test]
fn api_effect_future_async_registers_async_cleanups() {
    let root = Context::root();
    let runs = Arc::new(AtomicUsize::new(0));
    let fiber = root.plugin(Arc::new(Noop), Value::Null).expect("plugin");
    let ctx = fiber.context().expect("fiber context");
    let counter = runs.clone();
    let effect = block_on(ctx.effect_future_async(
        move || async move {
            Ok(vec![cleanup_async(move || {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            })])
        },
        "api.effect_future_async",
    ))
    .expect("effect");
    assert_eq!(runs.load(Ordering::SeqCst), 0);

    let owned = fiber.get_effects();
    assert!(
        owned
            .iter()
            .any(|meta| meta.label == "api.effect_future_async"),
        "the async effect is owned by the fiber: {owned:?}"
    );
    block_on(effect.dispose());
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    // Idempotent: the fiber unload does not run the cleanup again.
    fiber.dispose().expect("dispose");
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    root.dispose_all().expect("cleanup");
}

// ---------------------------------------------------------------------------
// logger::LoggerService::{is_enabled, exporter_count, clear_buffer}
// ---------------------------------------------------------------------------

#[derive(Default)]
struct CountingExporter {
    records: AtomicUsize,
}

impl LogExporter for CountingExporter {
    fn level(&self) -> Option<LogLevel> {
        None
    }

    fn export(&self, _record: &LogRecord) {
        self.records.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn api_logger_is_enabled_follows_the_service_threshold() {
    let service = LoggerService::with_buffer_size(LogLevel::Warn, 4);
    assert!(service.is_enabled(LogLevel::Error));
    assert!(service.is_enabled(LogLevel::Warn));
    assert!(!service.is_enabled(LogLevel::Info));
    assert!(!service.is_enabled(LogLevel::Debug));

    service.set_level(LogLevel::Debug);
    assert!(service.is_enabled(LogLevel::Debug));
    service.set_level(LogLevel::Error);
    assert!(!service.is_enabled(LogLevel::Warn));
}

#[test]
fn api_logger_exporter_count_and_clear_buffer() {
    let service = LoggerService::with_buffer_size(LogLevel::Info, 8);
    let exporter = Arc::new(CountingExporter::default());
    assert_eq!(service.exporter_count(), 0);

    let disposer = service.exporter(exporter.clone());
    assert_eq!(service.exporter_count(), 1);
    service.log(LogLevel::Info, "api", "hello", Value::Null);
    assert_eq!(exporter.records.load(Ordering::SeqCst), 1);
    assert_eq!(service.records().len(), 1);

    // clear_buffer drops the buffer only: exporters still receive new records.
    service.clear_buffer();
    assert!(service.records().is_empty());
    service.log(LogLevel::Info, "api", "again", Value::Null);
    assert_eq!(exporter.records.load(Ordering::SeqCst), 2);
    assert_eq!(service.records().len(), 1);

    disposer.dispose();
    assert_eq!(service.exporter_count(), 0);
    // Single-shot disposer: a second dispose is a no-op.
    disposer.dispose();
    assert_eq!(service.exporter_count(), 0);
}

// ---------------------------------------------------------------------------
// reflect::ContextMeta::insert_any
// ---------------------------------------------------------------------------

#[test]
fn api_context_meta_insert_any_round_trips() {
    let mut meta = zeokeel_cordis::ContextMeta::new();
    let raw: Arc<dyn Any + Send + Sync> = Arc::new(42u32);
    meta.insert_any("answer", raw);
    assert!(meta.contains("answer"));
    assert_eq!(meta.len(), 1);
    assert_eq!(meta.get::<u32>("answer").map(|value| *value), Some(42));
    assert!(meta.get::<String>("answer").is_none());
    assert!(!meta.contains("missing"));
    assert!(meta.get::<u32>("missing").is_none());
    assert!(!meta.is_empty());

    // The typed `insert` and the untyped `insert_any` share one namespace, and
    // insertion overrides (last write wins).
    meta.insert::<u32>("answer", Arc::new(7));
    assert_eq!(meta.get::<u32>("answer").map(|value| *value), Some(7));
    // The parent chain sees the meta; children inherit it.
    let root = Context::root();
    let child = root.extend_meta(meta);
    assert_eq!(
        child.meta::<u32>("answer").map(|value| *value),
        Some(7),
        "meta is inherited by derived contexts"
    );
}

// ---------------------------------------------------------------------------
// CordisError::plugin (structured plugin errors)
// ---------------------------------------------------------------------------

#[test]
fn api_structured_plugin_error_constructor_and_display() {
    let error = CordisError::plugin("my-plugin", "config", "missing api key");
    assert_eq!(
        error,
        CordisError::Plugin {
            plugin: "my-plugin".to_string(),
            kind: "config".to_string(),
            message: "missing api key".to_string(),
        },
        "the structured variant round-trips through PartialEq"
    );
    assert_eq!(
        error.to_string(),
        "plugin error: my-plugin (config): missing api key"
    );
    assert!(!matches!(error, CordisError::PluginFailed(_)));
}

// ---------------------------------------------------------------------------
// context::Context::on_lifecycle (general lifecycle broadcast)
// ---------------------------------------------------------------------------

#[test]
fn api_on_lifecycle_reports_transitions_and_supports_multiple_subscribers() {
    let root = Context::root();
    let events: Arc<Mutex<Vec<LifecycleEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = events.clone();
    let first = root
        .on_lifecycle(Arc::new(move |event| {
            sink.lock().unwrap().push(event.clone());
        }))
        .expect("first subscriber");
    let second_hits = Arc::new(AtomicUsize::new(0));
    let hits = second_hits.clone();
    let _second = root
        .on_lifecycle(Arc::new(move |_event| {
            hits.fetch_add(1, Ordering::SeqCst);
        }))
        .expect("second subscriber");

    let fiber = root.plugin(Arc::new(Noop), Value::Null).expect("plugin");
    let fiber_id = fiber.id();
    fiber.dispose().expect("dispose");

    let recorded = events.lock().unwrap().clone();
    assert!(
        recorded
            .iter()
            .any(|event| event.fiber_id == fiber_id && event.to == FiberState::Active),
        "loading -> active transition observed: {recorded:?}"
    );
    assert!(
        recorded
            .iter()
            .any(|event| event.fiber_id == fiber_id && event.to == FiberState::Disposed),
        "* -> Disposed transition observed: {recorded:?}"
    );
    assert!(
        recorded
            .iter()
            .all(|event| event.fiber_id != 0 || event.name == "root"),
        "events carry the fiber id and display name"
    );
    assert!(
        second_hits.load(Ordering::SeqCst) >= 2,
        "all subscribers run"
    );

    // Disposing a subscription stops delivery for it only.
    first.dispose();
    let before = events.lock().unwrap().len();
    let later = root.plugin(Arc::new(Noop), Value::Null).expect("plugin");
    later.dispose().expect("dispose");
    assert_eq!(
        events.lock().unwrap().len(),
        before,
        "a disposed subscriber must not receive further events"
    );
    assert!(
        second_hits.load(Ordering::SeqCst) > 2,
        "the other subscriber keeps receiving"
    );
    root.dispose_all().expect("cleanup");
}

#[test]
fn api_on_lifecycle_contains_listener_panics() {
    let root = Context::root();
    let seen = Arc::new(AtomicUsize::new(0));
    let _panicking = root
        .on_lifecycle(Arc::new(|_event| panic!("lifecycle listener boom")))
        .expect("panicking subscriber");
    let counter = seen.clone();
    let _counting = root
        .on_lifecycle(Arc::new(move |_event| {
            counter.fetch_add(1, Ordering::SeqCst);
        }))
        .expect("counting subscriber");

    let fiber = root.plugin(Arc::new(Noop), Value::Null).expect("plugin");
    fiber.dispose().expect("dispose");
    assert!(
        seen.load(Ordering::SeqCst) > 0,
        "a panicking subscriber must not stop the others"
    );
    // The kernel stays usable after the contained panic.
    let again = root.plugin(Arc::new(Noop), Value::Null).expect("plugin");
    again.dispose().expect("dispose");
    root.dispose_all().expect("cleanup");
}

// ---------------------------------------------------------------------------
// Events::internal / InternalBus ownership
// ---------------------------------------------------------------------------

/// The raw `InternalBus` entry point and `Context::on_internal_*` share one
/// ordered hook list; the raw one is not fiber-owned, so it must be disposed
/// explicitly (see the `Events::internal` / `InternalBus` docs).
#[test]
fn api_raw_internal_bus_is_not_fiber_owned() {
    let root = Context::root();
    let hits = Arc::new(AtomicUsize::new(0));
    let raw = {
        let hits = hits.clone();
        let bus: Arc<InternalBus> = root.events().internal().clone();
        bus.on_plugin(Arc::new(move |_fiber| {
            hits.fetch_add(1, Ordering::SeqCst);
        }))
    };
    {
        let fiber = root.plugin(Arc::new(Noop), Value::Null).expect("plugin");
        fiber.dispose().expect("dispose");
    }
    assert!(hits.load(Ordering::SeqCst) >= 1, "raw hook fired");

    // A raw hook is not tied to a fiber: it survives the fibers it observed.
    raw.dispose();
    let before = hits.load(Ordering::SeqCst);
    {
        let fiber = root.plugin(Arc::new(Noop), Value::Null).expect("plugin");
        fiber.dispose().expect("dispose");
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        before,
        "a disposed raw hook no longer fires"
    );
    root.dispose_all().expect("cleanup");
}

// ---------------------------------------------------------------------------
// Structured plugin errors propagate through the lifecycle APIs
// ---------------------------------------------------------------------------

struct FailingSync;

impl Plugin for FailingSync {
    fn name(&self) -> &'static str {
        "api-failing-sync"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        Err(CordisError::plugin("api-failing-sync", "config", "boom"))
    }
}

struct FailingAsync;

impl AsyncPlugin for FailingAsync {
    fn name(&self) -> &'static str {
        "api-failing-async"
    }

    fn apply_async<'a>(
        &'a self,
        _ctx: Context,
        _config: Value,
    ) -> zeokeel_cordis::BoxFuture<'a, Result<()>> {
        zeokeel_cordis::boxed(
            async move { Err(CordisError::plugin("api-failing-async", "io", "boom")) },
        )
    }
}

/// A `Plugin::apply` failure keeps the structured [`CordisError::Plugin`]
/// variant instead of being flattened into `PluginFailed(String)`.
#[test]
fn api_structured_plugin_error_survives_sync_activation() {
    let root = Context::root();
    let fiber = root
        .plugin(Arc::new(FailingSync), Value::Null)
        .expect("a failing plugin still yields a fiber");
    assert_eq!(fiber.state(), FiberState::Failed);
    let error = block_on(fiber.await_settled()).expect_err("activation failed");
    match error {
        CordisError::Plugin {
            plugin,
            kind,
            message,
        } => {
            assert_eq!(plugin, "api-failing-sync");
            assert_eq!(kind, "config");
            assert_eq!(message, "boom");
        }
        other => panic!("expected CordisError::Plugin, got {other:?}"),
    }
    // `Fiber::error()` keeps the compatibility string form.
    assert_eq!(
        fiber.error().as_deref(),
        Some("plugin error: api-failing-sync (config): boom")
    );
    root.dispose_all().expect("cleanup");
}

#[test]
fn api_structured_plugin_error_survives_async_activation() {
    let root = Context::root();
    let fiber = block_on(root.plugin_async(Arc::new(FailingAsync), Value::Null))
        .expect("a failing async plugin still yields a fiber");
    assert_eq!(fiber.state(), FiberState::Failed);
    let error = block_on(fiber.await_settled()).expect_err("activation failed");
    assert!(
        matches!(&error, CordisError::Plugin { kind, .. } if kind == "io"),
        "expected CordisError::Plugin, got {error:?}"
    );
    root.dispose_all().expect("cleanup");
}

#[test]
fn api_structured_plugin_error_survives_replace() {
    let root = Context::root();
    let fiber = root.plugin(Arc::new(Noop), Value::Null).expect("plugin");
    // A reload failure is fiber-level (like `restart`): `replace_plugin` reports
    // it through the fiber state/error instead of an `Err`, and the structured
    // variant must survive that path unchanged.
    fiber
        .replace_plugin(Arc::new(FailingSync))
        .expect("replace itself completes; the reload fails on the fiber");
    assert_eq!(fiber.state(), FiberState::Failed);
    match fiber.error_detail().expect("structured replace error") {
        CordisError::Plugin {
            plugin,
            kind,
            message,
        } => {
            assert_eq!(plugin, "api-failing-sync");
            assert_eq!(kind, "config");
            assert_eq!(message, "boom");
        }
        other => panic!("expected CordisError::Plugin, got {other:?}"),
    }
    assert_eq!(
        fiber.error().as_deref(),
        Some("plugin error: api-failing-sync (config): boom"),
        "the compatibility string form is not wrapped into `plugin failed: ...`"
    );
    root.dispose_all().expect("cleanup");
}

// ---------------------------------------------------------------------------
// Injectable clock (Context::set_clock / Clock / SystemClock)
// ---------------------------------------------------------------------------

/// Deterministic clock: wall time and monotonic time are independent counters.
struct FakeClock {
    unix_ms: AtomicUsize,
    monotonic_ms: AtomicUsize,
}

impl FakeClock {
    fn new(unix_ms: usize) -> Arc<Self> {
        Arc::new(Self {
            unix_ms: AtomicUsize::new(unix_ms),
            monotonic_ms: AtomicUsize::new(0),
        })
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
fn api_injectable_clock_drives_log_timestamps() {
    let root = Context::root();
    let fake = FakeClock::new(1_700_000_000_123);
    root.set_clock(fake.clone())
        .expect("root installs the clock");

    // Child contexts share the tree clock.
    let child = root.isolate("api.clock");
    assert_eq!(child.clock().now_unix_ms(), 1_700_000_000_123);

    let service = root.logger_service();
    let _exporter = service.exporter(Arc::new(CountingExporter::default()));
    root.logger().info("clocked", Value::Null);
    let records = service.records();
    assert_eq!(
        records.last().map(|record| record.ts_unix_ms),
        Some(1_700_000_000_123),
        "log timestamps come from the injected clock"
    );

    fake.unix_ms.store(1_700_000_000_999, Ordering::SeqCst);
    root.logger().info("again", Value::Null);
    assert_eq!(
        service.records().last().map(|record| record.ts_unix_ms),
        Some(1_700_000_000_999),
        "later records observe the advanced clock"
    );
    root.dispose_all().expect("cleanup");
}

#[test]
fn api_clock_can_only_be_set_on_the_root_context() {
    let root = Context::root();
    // A plugin context belongs to a non-root fiber (fiber_id != 0).
    let fiber = root.plugin(Arc::new(Noop), Value::Null).expect("plugin");
    let plugin_ctx = fiber.context().expect("plugin context");
    assert_ne!(plugin_ctx.fiber_id(), 0);
    let error = plugin_ctx
        .set_clock(FakeClock::new(1))
        .expect_err("non-root contexts must not install a clock");
    assert!(
        matches!(&error, CordisError::InvalidPlugin(message) if message.contains("root")),
        "unexpected error: {error:?}"
    );
    // Any root-derived scope writes the same per-tree slot (fiber id 0).
    let derived = root.extend();
    derived
        .set_clock(FakeClock::new(7))
        .expect("root-derived scopes share the tree clock slot");
    assert_eq!(
        root.clock().now_unix_ms(),
        7,
        "the derived scope installed the root tree clock"
    );
    assert_eq!(derived.clock().now_unix_ms(), 7);
    root.dispose_all().expect("cleanup");
}

#[test]
fn api_system_clock_is_monotonic() {
    let clock = SystemClock;
    let first = clock.now_monotonic_ms();
    let second = clock.now_monotonic_ms();
    assert!(second >= first, "monotonic time never goes backwards");
    assert!(
        clock.now_unix_ms() > 1_600_000_000_000,
        "wall clock is a plausible unix millisecond timestamp"
    );
}

/// `Disposer` is a single-owner handle: dropping the **last** handle runs the
/// cleanup (its `Drop` safety net), so a dropped `on_lifecycle` subscription
/// stops firing exactly like an explicit `dispose()`.
#[test]
fn api_on_lifecycle_dropping_the_disposer_unsubscribes() {
    let root = Context::root();
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    {
        let disposer = root
            .on_lifecycle(Arc::new(move |_event| {
                counter.fetch_add(1, Ordering::SeqCst);
            }))
            .expect("subscriber");
        let fiber = root.plugin(Arc::new(Noop), Value::Null).expect("plugin");
        fiber.dispose().expect("dispose");
        assert!(hits.load(Ordering::SeqCst) >= 1, "subscription was active");
        // Last handle dropped here: Drop disposes the subscription.
        drop(disposer);
    }
    let before = hits.load(Ordering::SeqCst);
    let fiber = root.plugin(Arc::new(Noop), Value::Null).expect("plugin");
    fiber.dispose().expect("dispose");
    assert_eq!(
        hits.load(Ordering::SeqCst),
        before,
        "dropping the last disposer handle unsubscribes (Drop == dispose)"
    );
    root.dispose_all().expect("cleanup");
}

/// Logger facades snapshot the clock at creation: `set_clock` only affects
/// facades acquired afterwards (documented on `Context::set_clock` / `Logger`).
#[test]
fn api_logger_facade_snapshots_the_clock() {
    let root = Context::root();
    let old_facade = root.logger();
    let old_clock_before = old_facade.clock().now_unix_ms();

    let fake = FakeClock::new(1_234_567_890_000);
    root.set_clock(fake.clone())
        .expect("root installs the clock");

    // The old facade keeps its snapshot; a new facade uses the injected clock.
    // SystemClock is a live wall clock, so compare behaviourally (it must not
    // switch to the injected test clock) instead of requiring an identical
    // millisecond timestamp — that assertion was timing-flaky on CI.
    let old_clock_after = old_facade.clock().now_unix_ms();
    assert!(
        old_clock_after >= old_clock_before && old_clock_after != 1_234_567_890_000,
        "an existing facade keeps its clock snapshot: before={before}, after={after}",
        before = old_clock_before,
        after = old_clock_after,
    );
    let new_facade = root.logger();
    assert_eq!(new_facade.clock().now_unix_ms(), 1_234_567_890_000);

    let service = root.logger_service();
    service.clear_buffer();
    old_facade.info("old-clock", Value::Null);
    new_facade.info("new-clock", Value::Null);
    let records = service.records();
    assert_eq!(records.len(), 2);
    assert_ne!(
        records[0].ts_unix_ms, 1_234_567_890_000,
        "the pre-existing facade still stamps with its snapshot clock"
    );
    assert_eq!(
        records[1].ts_unix_ms, 1_234_567_890_000,
        "the new facade stamps with the injected clock"
    );
    root.dispose_all().expect("cleanup");
}
