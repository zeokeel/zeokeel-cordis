//! v0.4.2 semantic regression tests (FIX-12 … FIX-21).
//!
//! One test per fixed defect; each test states the pre-fix behavior it pins so
//! a future regression is recognizable without reading the fix commit.

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use zeokeel_cordis::{
    async_waterfall_terminal, block_on, cleanup_async, AsyncDisposer, ConfigSchema, Context,
    CordisError, Fiber, FiberState, GetNext, JsonConfigSchema, JsonType, Plugin, PluginKey, Result,
    ServiceKey, ServiceValue, UpdateNext,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct Noop;

impl Plugin for Noop {
    fn name(&self) -> &'static str {
        "noop"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        Ok(())
    }
}

struct Configurable {
    applies: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Value>>>,
}

impl Plugin for Configurable {
    fn name(&self) -> &'static str {
        "configurable"
    }

    fn apply(&self, _ctx: Context, config: Value) -> Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        self.seen.lock().unwrap().push(config);
        Ok(())
    }
}

fn configurable() -> (Arc<AtomicUsize>, Arc<Mutex<Vec<Value>>>, Arc<Configurable>) {
    let applies = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let plugin = Arc::new(Configurable {
        applies: applies.clone(),
        seen: seen.clone(),
    });
    (applies, seen, plugin)
}

// ---------------------------------------------------------------------------
// FIX-12: provider-ACTIVE injection gate (injection only, reads stay non-strict)
// ---------------------------------------------------------------------------

static GREETING: ServiceKey<String> = ServiceKey::new("fix12.greeting");

/// Provides `GREETING` and then blocks while its fiber is still `Loading`.
struct ProvidesGreeting {
    provided: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl Plugin for ProvidesGreeting {
    fn name(&self) -> &'static str {
        "fix12-provides-greeting"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.provide(GREETING, Arc::new("hello".to_string()))?;
        self.provided.send(()).expect("signal provide");
        // Stay `Loading` until the test releases us.
        self.release.lock().unwrap().recv().expect("wait release");
        Ok(())
    }
}

struct NeedsGreeting {
    applied: Arc<AtomicUsize>,
}

impl Plugin for NeedsGreeting {
    fn name(&self) -> &'static str {
        "fix12-needs-greeting"
    }

    fn inject(&self) -> &'static [&'static str] {
        &["fix12.greeting"]
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        self.applied.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Pre-fix: `insert_service` refreshed dependents immediately, so a dependent
/// activated while the provider fiber was still `Loading`; a provider that then
/// failed had already allowed the dependent's `apply` to run.
#[test]
fn fix12_dependent_waits_for_active_provider_while_reads_stay_non_strict() {
    let root = Context::root();
    let applied = Arc::new(AtomicUsize::new(0));
    let consumer = root
        .plugin(
            Arc::new(NeedsGreeting {
                applied: applied.clone(),
            }),
            Value::Null,
        )
        .expect("consumer fiber");
    assert_eq!(consumer.state(), FiberState::Pending);

    let (provided_tx, provided_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let provider = ProvidesGreeting {
        provided: provided_tx,
        release: Mutex::new(release_rx),
    };
    let provider_root = root.clone();
    let handle = std::thread::spawn(move || provider_root.plugin(Arc::new(provider), Value::Null));
    provided_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("provider provided the service");

    // The provider fiber is `Loading` (its `apply` is blocked) while the
    // service entry already exists.
    assert_eq!(
        consumer.state(),
        FiberState::Pending,
        "FIX-12: a dependent must not activate against a Loading provider"
    );
    assert_eq!(
        applied.load(Ordering::SeqCst),
        0,
        "the dependent's apply must never run for a non-Active provider"
    );
    // Non-strict read path (Cordis property read): the value is visible.
    assert_eq!(
        root.get(GREETING).map(|value| value.as_str().to_string()),
        Some("hello".to_string()),
        "Context::get keeps the non-strict (property read) semantics"
    );
    assert!(
        !root.service_available("fix12.greeting"),
        "service_available is the strict injection query"
    );

    release_tx.send(()).expect("release provider");
    let provider_fiber = handle
        .join()
        .expect("provider thread")
        .expect("provider fiber");
    assert_eq!(provider_fiber.state(), FiberState::Active);
    assert_eq!(
        consumer.state(),
        FiberState::Active,
        "the Active transition wakes the dependent"
    );
    assert_eq!(applied.load(Ordering::SeqCst), 1);
    assert!(root.service_available("fix12.greeting"));
}

struct FailingProvider;

impl Plugin for FailingProvider {
    fn name(&self) -> &'static str {
        "fix12-failing-provider"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.provide(GREETING, Arc::new("never".to_string()))?;
        Err(CordisError::InvalidPlugin(
            "provider fails after providing".to_string(),
        ))
    }
}

/// Pre-fix: the dependent activated as soon as the failing provider inserted
/// the service, so its `apply` ran even though the provider never committed.
#[test]
fn fix12_failed_provider_never_activates_dependents() {
    let root = Context::root();
    let applied = Arc::new(AtomicUsize::new(0));
    let consumer = root
        .plugin(
            Arc::new(NeedsGreeting {
                applied: applied.clone(),
            }),
            Value::Null,
        )
        .expect("consumer fiber");
    assert_eq!(consumer.state(), FiberState::Pending);

    let provider = root
        .plugin(Arc::new(FailingProvider), Value::Null)
        .expect("provider fiber");
    assert_eq!(provider.state(), FiberState::Failed);
    assert_eq!(
        consumer.state(),
        FiberState::Pending,
        "a failed provider must not wake dependents"
    );
    assert_eq!(
        applied.load(Ordering::SeqCst),
        0,
        "the dependent's apply must never run for a failed provider"
    );
    assert!(!root.service_available("fix12.greeting"));
}

// ---------------------------------------------------------------------------
// FIX-13: `ctx.set` type check
// ---------------------------------------------------------------------------

static COUNTER: ServiceKey<String> = ServiceKey::new("fix13.counter");

/// Pre-fix: `ServiceEntry` had no `TypeId`, so a `u32` silently replaced a
/// `String` service (the next typed `get` then failed to downcast).
#[test]
fn fix13_set_rejects_type_mismatch_and_keeps_the_original_value() {
    let root = Context::root();
    root.provide(COUNTER, Arc::new("one".to_string()))
        .expect("provide");

    let error = root
        .set("fix13.counter", Arc::new(7u32) as ServiceValue)
        .expect_err("a different type must be rejected");
    match error {
        CordisError::ServiceTypeMismatch(message) => {
            assert!(
                message.contains("fix13.counter"),
                "the diagnostic must name the service: {message}"
            );
        }
        other => panic!("expected ServiceTypeMismatch, got {other:?}"),
    }
    assert_eq!(
        root.get(COUNTER).expect("original value").as_str(),
        "one",
        "a rejected set must not mutate the store"
    );

    // Same type still succeeds.
    root.set("fix13.counter", Arc::new("two".to_string()) as ServiceValue)
        .expect("a same-type set succeeds");
    assert_eq!(root.get(COUNTER).expect("updated value").as_str(), "two");
}

/// Pre-fix: the untyped `set` could also corrupt a `*_any` (trait-object)
/// service, whose stored type is `ArcWrap<T>`.
#[test]
fn fix13_set_rejects_typed_vs_any_wrapper_mismatch() {
    let root = Context::root();
    root.provide_any(COUNTER, Arc::new("any".to_string()))
        .expect("provide_any");
    let error = root
        .set(
            "fix13.counter",
            Arc::new("plain".to_string()) as ServiceValue,
        )
        .expect_err("set() must not accept a plain Arc<T> for an ArcWrap<T> entry");
    assert!(matches!(error, CordisError::ServiceTypeMismatch(_)));
    // set_any with the matching wrapper type succeeds.
    root.set_any(COUNTER, Arc::new("wrapped".to_string()))
        .expect("same ArcWrap<T> type");
    assert_eq!(root.get_any(COUNTER).expect("updated").as_str(), "wrapped");
}

// ---------------------------------------------------------------------------
// FIX-14: update ordering / veto / no_save (fiber.rs implemented by impl-core)
// ---------------------------------------------------------------------------

/// Pre-fix: the raw config was stored *before* `internal/update`, so a vetoing
/// hook left the rejected config in `raw_config` (the next restart used it).
#[test]
fn fix14_vetoed_update_keeps_raw_config_and_does_not_restart() {
    let root = Context::root();
    let (applies, seen, plugin) = configurable();
    let fiber = root.plugin(plugin, json!({ "v": 1 })).expect("fiber");
    assert_eq!(applies.load(Ordering::SeqCst), 1);

    let veto = root
        .on_internal_update(Arc::new(
            |_fiber, _config: Value, _no_save: bool, _next: &UpdateNext<'_>| {
                // Never calls `next`: veto the whole update.
            },
        ))
        .expect("veto hook");

    fiber
        .update(json!({ "v": 2 }))
        .expect("a pure veto is Ok(())");
    assert_eq!(
        fiber.raw_config(),
        json!({ "v": 1 }),
        "FIX-14: a veto must not retain the rejected raw config"
    );
    assert_eq!(fiber.config(), json!({ "v": 1 }));
    assert_eq!(
        applies.load(Ordering::SeqCst),
        1,
        "a veto must not restart the fiber"
    );

    veto.dispose();
    fiber.update(json!({ "v": 2 })).expect("update after veto");
    assert_eq!(fiber.raw_config(), json!({ "v": 2 }));
    assert_eq!(applies.load(Ordering::SeqCst), 2);
    assert_eq!(seen.lock().unwrap().last(), Some(&json!({ "v": 2 })));
}

/// TS parity (`fiber.update(config, noSave)`): `no_save` is only a **hint**
/// forwarded through the `internal/update` chain (e.g. for persistence hooks
/// not to write the change back). It never changes kernel state: a non-vetoed
/// update always retains the new raw config and restarts with it (the TS
/// implementation writes `_config` unconditionally and passes `noSave` to the
/// hooks only).
///
/// Pre-fix: the terminal hard-coded `false` and ignored the hook's hint.
#[test]
fn fix14_no_save_is_forwarded_as_a_hint_but_the_update_still_commits() {
    let root = Context::root();
    let (applies, seen, plugin) = configurable();
    let fiber = root.plugin(plugin, json!({ "v": 1 })).expect("fiber");
    assert_eq!(applies.load(Ordering::SeqCst), 1);

    // First hook forwards `no_save = true`; the second one runs later in the
    // chain and observes the forwarded hint.
    let _hint_hook = root
        .on_internal_update(Arc::new(
            |_fiber, config: Value, _no_save: bool, next: &UpdateNext<'_>| {
                next.next(config, true);
            },
        ))
        .expect("hint hook");
    let observed = Arc::new(AtomicBool::new(false));
    let flag = observed.clone();
    let _observer_hook = root
        .on_internal_update(Arc::new(
            move |_fiber, config: Value, no_save: bool, next: &UpdateNext<'_>| {
                flag.store(no_save, Ordering::SeqCst);
                next.next(config, no_save);
            },
        ))
        .expect("observer hook");

    fiber.update(json!({ "v": 2 })).expect("update");
    assert!(
        observed.load(Ordering::SeqCst),
        "the no_save hint must be forwarded to later update hooks (TS parity)"
    );
    assert_eq!(
        fiber.raw_config(),
        json!({ "v": 2 }),
        "no_save is only a hint: the update still retains the new raw config"
    );
    assert_eq!(
        applies.load(Ordering::SeqCst),
        2,
        "the update still restarts (apply runs again)"
    );
    assert_eq!(
        seen.lock().unwrap().last(),
        Some(&json!({ "v": 2 })),
        "the restart applies the new config, not the old raw config"
    );
}

/// A hook may replace the config before the terminal; the replacement must be
/// schema-validated first and then become the new raw config.
#[test]
fn fix14_replacement_config_is_validated_and_becomes_the_new_raw() {
    let root = Context::root();
    let (applies, seen, plugin) = configurable();
    let fiber = root.plugin(plugin, json!({ "v": 1 })).expect("fiber");

    let _hook = root
        .on_internal_update(Arc::new(
            |_fiber, _config: Value, _no_save: bool, next: &UpdateNext<'_>| {
                next.next(json!({ "v": 42 }), false);
            },
        ))
        .expect("replacement hook");

    fiber.update(json!({ "v": 2 })).expect("update");
    assert_eq!(
        fiber.raw_config(),
        json!({ "v": 42 }),
        "FIX-14: a validated replacement becomes the new raw config"
    );
    assert_eq!(applies.load(Ordering::SeqCst), 2);
    assert_eq!(seen.lock().unwrap().last(), Some(&json!({ "v": 42 })));
}

struct SchemaPlugin {
    applies: Arc<AtomicUsize>,
}

impl Plugin for SchemaPlugin {
    fn name(&self) -> &'static str {
        "fix14-schema-plugin"
    }

    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>> {
        Some(Arc::new(
            JsonConfigSchema::new()
                .required("v")
                .property("v", JsonType::Number),
        ))
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// An invalid replacement is rejected as a whole: `update` returns
/// `CordisError::Validation`, the raw config is unchanged and no restart ran.
#[test]
fn fix14_invalid_replacement_is_rejected_without_mutating_state() {
    let root = Context::root();
    let applies = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(
            Arc::new(SchemaPlugin {
                applies: applies.clone(),
            }),
            json!({ "v": 1 }),
        )
        .expect("fiber");
    assert_eq!(applies.load(Ordering::SeqCst), 1);

    let _hook = root
        .on_internal_update(Arc::new(
            |_fiber, _config: Value, _no_save: bool, next: &UpdateNext<'_>| {
                next.next(json!({ "v": "not-a-number" }), false);
            },
        ))
        .expect("invalid replacement hook");

    let error = fiber
        .update(json!({ "v": 2 }))
        .expect_err("an invalid replacement must fail the update");
    assert!(
        matches!(error, CordisError::Validation(_)),
        "expected Validation, got {error:?}"
    );
    assert_eq!(fiber.raw_config(), json!({ "v": 1 }));
    assert_eq!(fiber.config(), json!({ "v": 1 }));
    assert_eq!(applies.load(Ordering::SeqCst), 1, "no restart on rejection");
}

// ---------------------------------------------------------------------------
// FIX-15: `dispose_blocking` joins an in-flight teardown
// ---------------------------------------------------------------------------

/// Pre-fix: `dispose_blocking` returned as soon as the cleanup list was taken
/// (`is_disposed()`), i.e. while another thread was still running the cleanup.
#[test]
fn fix15_dispose_blocking_joins_an_in_flight_teardown() {
    let (started_tx, started_rx) = mpsc::channel();
    let disposer = AsyncDisposer::new(vec![cleanup_async(move || {
        let started_tx = started_tx.clone();
        async move {
            started_tx.send(()).expect("signal cleanup start");
            std::thread::sleep(Duration::from_millis(400));
        }
    })]);

    let owner = disposer.clone();
    let thread = std::thread::spawn(move || block_on(owner.dispose()));
    started_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("cleanup started on the owning thread");

    let started = Instant::now();
    disposer.dispose_blocking();
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(200),
        "FIX-15: dispose_blocking returned after {waited:?} without joining the in-flight cleanup"
    );
    thread.join().expect("disposing thread");
}

// ---------------------------------------------------------------------------
// FIX-16: async waterfall panics are fail-closed (re-raised)
// ---------------------------------------------------------------------------

/// Pre-fix: a panicking async middleware was logged and the dispatch degraded to
/// `Value::Null` (fail-open); the sync lane re-raised.
#[test]
fn fix16_async_waterfall_panic_reaches_the_caller_and_skips_the_terminal() {
    let root = Context::root();
    let terminal_ran = Arc::new(AtomicBool::new(false));
    root.on_waterfall_async("fix16.io", |_args, _next| {
        zeokeel_cordis::boxed(async { panic!("fix16 async middleware boom") })
    })
    .expect("register panicking async middleware");
    let flag = terminal_ran.clone();
    let terminal = async_waterfall_terminal(move |_args| {
        flag.store(true, Ordering::SeqCst);
        std::future::ready(json!("allowed"))
    });

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        block_on(root.waterfall_async("fix16.io", &[], Some(terminal)))
    }));
    assert!(
        outcome.is_err(),
        "FIX-16: a panicking async middleware must re-raise to the dispatcher"
    );
    assert!(
        !terminal_ran.load(Ordering::SeqCst),
        "the terminal must not run after a middleware panic"
    );
}

#[test]
fn fix16_normal_async_waterfall_is_unchanged() {
    let root = Context::root();
    root.on_waterfall_async("fix16.ok", |_args, next| {
        zeokeel_cordis::boxed(async move { next.next().await })
    })
    .expect("middleware");
    let value = block_on(root.waterfall_async(
        "fix16.ok",
        &[],
        Some(async_waterfall_terminal(|_args| async { json!("base") })),
    ));
    assert_eq!(value, json!("base"));
}

// ---------------------------------------------------------------------------
// FIX-17: nullable JSON config defaults (no new public enum variant)
// ---------------------------------------------------------------------------

/// Pre-fix: `default_value(name, Value::Null)` declared the property as
/// `JsonType::Object`, so `validate({})` rejected the injected `null`.
///
/// Post-fix contract: a `null` default is injected but leaves the property
/// **untyped** (no `properties` entry) — `JsonType` keeps its five variants so
/// downstream exhaustive `match`es stay source-compatible in a patch release.
#[test]
fn fix17_null_defaults_are_injected_without_a_type_constraint() {
    let schema = JsonConfigSchema::new().default_value("maybe", Value::Null);
    assert_eq!(
        schema.validate(json!({})).expect("null default injects"),
        json!({ "maybe": null })
    );
    assert_eq!(
        schema
            .validate(json!({ "maybe": null }))
            .expect("explicit null passes"),
        json!({ "maybe": null })
    );
    // No declared kind for a null default: an explicit non-null value is
    // accepted too (declaring a constraint is `property()`'s job).
    assert_eq!(
        schema
            .validate(json!({ "maybe": 5 }))
            .expect("untyped defaulted key accepts any value"),
        json!({ "maybe": 5 })
    );

    // A defaulted key is still a *declared* key: `deny_unknown` must not
    // reject the injected `null`.
    let strict = JsonConfigSchema::new()
        .default_value("maybe", Value::Null)
        .deny_unknown();
    assert_eq!(
        strict
            .validate(json!({}))
            .expect("deny_unknown accepts a defaulted null key"),
        json!({ "maybe": null })
    );
    assert!(
        strict.validate(json!({ "other": 1 })).is_err(),
        "deny_unknown still rejects genuinely unknown keys"
    );

    // An explicit `property()` declaration is enforced regardless.
    let typed = JsonConfigSchema::new()
        .default_value("maybe", Value::Null)
        .property("maybe", JsonType::Object);
    assert!(
        typed.validate(json!({ "maybe": null })).is_err(),
        "an explicit property() declaration takes precedence"
    );

    // Non-null default inference is unchanged.
    let strings = JsonConfigSchema::new().default_value("name", json!("x"));
    assert_eq!(
        strings.validate(json!({})).expect("string default"),
        json!({ "name": "x" })
    );
    assert!(
        strings.validate(json!({ "name": 1 })).is_err(),
        "a string default still declares a string property"
    );
}

/// Mutual exclusion between "default `null`" and "declared type" (audit B2):
/// a `null` default for a field with an explicit type means "optional, absent
/// by default", so `validate({})` must not inject a `null` that contradicts the
/// declared type. Untyped `null` defaults keep the 0.4.1-injected behaviour.
#[test]
fn fix17_optional_typed_fields_with_null_default_stay_absent() {
    // (1) `null` default + explicit `Object` type, both declaration orders.
    let schemas = [
        JsonConfigSchema::new()
            .default_value("maybe", Value::Null)
            .property("maybe", JsonType::Object),
        JsonConfigSchema::new()
            .property("maybe", JsonType::Object)
            .default_value("maybe", Value::Null),
    ];
    for schema in schemas {
        assert_eq!(
            schema
                .validate(json!({}))
                .expect("optional typed field is absent"),
            json!({}),
            "the null default must not be injected for a typed field"
        );
        assert_eq!(
            schema
                .validate(json!({ "maybe": {} }))
                .expect("declared type passes"),
            json!({ "maybe": {} })
        );
        assert!(
            schema.validate(json!({ "maybe": 5 })).is_err(),
            "the declared type is enforced when the caller provides a value"
        );
        assert!(
            schema.validate(json!({ "maybe": null })).is_err(),
            "explicit null is not an object"
        );
        // `merge` follows the same rule, so resolved configs do not gain a
        // phantom `null` either.
        assert_eq!(
            schema.merge(Value::Null, &[]),
            json!({}),
            "merge must not materialize a typed null default"
        );
    }

    // (2) two defaults, non-null first then `null`: the declared kind stays
    // `Object` (defaults never re-declare a type) and the `null` default means
    // "absent". The retained default *value* is the last one (normal
    // last-write-wins), so the reverse order keeps the object default.
    let null_last = JsonConfigSchema::new()
        .default_value("maybe", json!({}))
        .default_value("maybe", Value::Null);
    assert_eq!(
        null_last.validate(json!({})).expect("null default wins"),
        json!({})
    );
    assert!(null_last.validate(json!({ "maybe": 5 })).is_err());
    let object_last = JsonConfigSchema::new()
        .default_value("maybe", Value::Null)
        .default_value("maybe", json!({}));
    assert_eq!(
        object_last
            .validate(json!({}))
            .expect("object default wins"),
        json!({ "maybe": {} })
    );

    // (3) `deny_unknown`: an absent optional key is not "unknown"; unknown keys
    // are still rejected.
    let strict = JsonConfigSchema::new()
        .default_value("maybe", Value::Null)
        .property("maybe", JsonType::Object)
        .deny_unknown();
    assert_eq!(
        strict
            .validate(json!({}))
            .expect("absent optional key is fine"),
        json!({})
    );
    assert_eq!(
        strict
            .validate(json!({ "maybe": {} }))
            .expect("provided typed value"),
        json!({ "maybe": {} })
    );
    assert!(
        strict.validate(json!({ "other": 1 })).is_err(),
        "deny_unknown still rejects genuinely unknown keys"
    );

    // (4) `required`: the typed field is absent by default, so `required` now
    // demands an explicit value; the untyped `null` default still satisfies it.
    let required_typed = JsonConfigSchema::new()
        .required("maybe")
        .default_value("maybe", Value::Null)
        .property("maybe", JsonType::Object);
    let issues = required_typed
        .validate(json!({}))
        .expect_err("a required typed field must be provided");
    assert!(
        issues.iter().any(|issue| issue.contains("maybe")),
        "{issues:?}"
    );
    assert_eq!(
        required_typed
            .validate(json!({ "maybe": {} }))
            .expect("provided required value"),
        json!({ "maybe": {} })
    );
    let required_untyped = JsonConfigSchema::new()
        .required("maybe")
        .default_value("maybe", Value::Null);
    assert_eq!(
        required_untyped
            .validate(json!({}))
            .expect("an untyped null default satisfies required"),
        json!({ "maybe": null })
    );
}

// ---------------------------------------------------------------------------
// FIX-18: panicking accessor reads as absent
// ---------------------------------------------------------------------------

static PANICKING_ACCESSOR: ServiceKey<String> = ServiceKey::new("fix18.panicking-accessor");

/// Pre-fix: `get_named` returned `Some(Arc<()>)` (a placeholder) instead of
/// `None`, contradicting the documented "absent property → None" contract.
#[test]
fn fix18_panicking_accessor_reads_as_absent() {
    let root = Context::root();
    root.accessor("fix18.panicking-accessor", |_ctx| -> ServiceValue {
        panic!("fix18 accessor boom")
    })
    .expect("accessor");
    assert!(
        root.get_named("fix18.panicking-accessor").is_none(),
        "FIX-18: a panicking getter must read as None, not a placeholder value"
    );
    assert!(root.get(PANICKING_ACCESSOR).is_none());
    assert!(!root.service_available("fix18.panicking-accessor"));
    // The kernel stays usable after the contained panic.
    root.plugin(Arc::new(Noop), Value::Null).expect("noop");
}

// ---------------------------------------------------------------------------
// FIX-19: MAX_PLUGIN_DEPTH counts fiber depth, not context depth
// ---------------------------------------------------------------------------

/// Pre-fix: a 300-level `intercept()` context chain exhausted
/// `MAX_PLUGIN_DEPTH`, so loading a single shallow plugin failed.
#[test]
fn fix19_deep_context_chain_does_not_consume_the_loading_depth_budget() {
    let root = Context::root();
    let mut ctx = root.clone();
    for _ in 0..300 {
        ctx = ctx.intercept("fix19.svc", json!({ "v": 1 }));
    }
    let fiber = ctx
        .plugin(Arc::new(Noop), Value::Null)
        .expect("FIX-19: context depth must not count as plugin loading depth");
    assert_eq!(fiber.state(), FiberState::Active);
}

struct DeepChain {
    remaining: usize,
}

impl Plugin for DeepChain {
    fn name(&self) -> &'static str {
        "fix19-deep-chain"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        if self.remaining > 0 {
            ctx.plugin(
                Arc::new(DeepChain {
                    remaining: self.remaining - 1,
                }),
                Value::Null,
            )?;
        }
        Ok(())
    }
}

/// The fiber-depth guard must still contain genuinely recursive loading.
#[test]
fn fix19_nested_plugin_chain_is_still_rejected() {
    let root = Context::root();
    let _top = root
        .plugin(Arc::new(DeepChain { remaining: 300 }), Value::Null)
        .expect("top fiber loads");
    let failed = root
        .registry()
        .fibers()
        .into_iter()
        .find(|fiber| {
            fiber.state() == FiberState::Failed
                && fiber.error().unwrap_or_default().contains("depth limit")
        })
        .expect("a 300-level plugin chain must be rejected by the depth guard");
    assert!(failed.error().unwrap_or_default().contains("depth limit"));
    root.dispose_all().expect("cleanup");
}

// ---------------------------------------------------------------------------
// FIX-20: `ctx.inject` identity granularity
// ---------------------------------------------------------------------------

/// Pre-fix: every `inject` shared `PluginKey::Named("injected")`, so the
/// registry under-counted runtimes and `replace(Named("injected"))` replaced
/// every injected plugin at once.
#[test]
fn fix20_inject_plugins_have_distinct_registry_identities() {
    let root = Context::root();
    let hits = Arc::new(AtomicUsize::new(0));

    let first = hits.clone();
    root.inject(&[], move |_ctx| {
        first.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .expect("first inject");
    let second = hits.clone();
    root.inject(&[], move |_ctx| {
        second.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .expect("second inject");
    let third = hits.clone();
    root.inject(&[], move |_ctx| {
        third.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .expect("third inject");

    assert_eq!(hits.load(Ordering::SeqCst), 3);
    assert_eq!(
        root.registry().size(),
        3,
        "FIX-20: three injects are three registry runtimes"
    );
    assert!(
        !root
            .registry()
            .keys()
            .contains(&PluginKey::Named("injected")),
        "injects must not share the `injected` name key"
    );
    let replaced = root
        .registry()
        .replace(PluginKey::Named("injected"), Arc::new(Noop));
    let error = match replaced {
        Ok(_) => panic!("replace(Named(\"injected\")) must no longer match"),
        Err(error) => error,
    };
    assert!(
        matches!(error, CordisError::InvalidPlugin(_)),
        "expected InvalidPlugin(no runtime), got {error:?}"
    );
    // The injected fibers are untouched by the failed replace.
    assert_eq!(root.registry().size(), 3);
}

// ---------------------------------------------------------------------------
// FIX-21: virtual services are readable but never injectable
// ---------------------------------------------------------------------------

/// Accessors and `internal/get` overrides are *properties*, not provided
/// services: `get`/`get_named` resolve them, while `inject` and
/// `service_available` (strict injection queries) never see them. This split is
/// the documented Cordis semantics (TS: `ctx.name` vs `inject`).
#[test]
fn fix21_virtual_services_are_readable_but_never_satisfy_inject() {
    let root = Context::root();
    root.accessor("fix21.virtual", |_ctx| {
        Arc::new("virtual".to_string()) as ServiceValue
    })
    .expect("accessor");
    let _get_hook = root
        .on_internal_get(Arc::new(
            |_ctx: &Context, name: &str, next: &GetNext<'_>| {
                if name == "fix21.hooked" {
                    Some(Arc::new("hooked".to_string()) as ServiceValue)
                } else {
                    next.next()
                }
            },
        ))
        .expect("internal/get hook");

    struct NeedsVirtual {
        hits: Arc<AtomicUsize>,
    }
    impl Plugin for NeedsVirtual {
        fn name(&self) -> &'static str {
            "fix21-needs-virtual"
        }

        fn inject(&self) -> &'static [&'static str] {
            &["fix21.virtual", "fix21.hooked"]
        }

        fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
            self.hits.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let hits = Arc::new(AtomicUsize::new(0));
    let consumer = root
        .plugin(Arc::new(NeedsVirtual { hits: hits.clone() }), Value::Null)
        .expect("consumer");
    assert_eq!(
        consumer.state(),
        FiberState::Pending,
        "FIX-21: virtual services must never satisfy inject"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    assert!(
        root.get_named("fix21.virtual").is_some(),
        "property reads still resolve the accessor"
    );
    assert!(
        root.get_named("fix21.hooked").is_some(),
        "property reads still dispatch internal/get"
    );
    assert!(!root.service_available("fix21.virtual"));
    assert!(!root.service_available("fix21.hooked"));
}

// ---------------------------------------------------------------------------
// S-1/S-2: cancelled async teardown retry + self-join (audit B-1/B-2)
// ---------------------------------------------------------------------------

/// S-1 (audit B-1): a cancelled teardown restores its unstarted cleanups and
/// marks the state cancelled/settled. `dispose_blocking` used to early-return on
/// that "done" state, silently leaking the restored cleanups, while `dispose()`
/// retried them (asymmetric). Both mirrors must retry now.
#[test]
fn fix15_cancelled_teardown_is_retried_by_dispose_blocking() {
    let restored_runs = Arc::new(AtomicUsize::new(0));
    let runs = restored_runs.clone();
    let (started_tx, started_rx) = mpsc::channel::<()>();
    // Vec order: [restored, pending]; the drain pops the pending one first, so
    // the first-stored cleanup stays unstarted and is restored on cancellation.
    let disposer = AsyncDisposer::new(vec![
        cleanup_async(move || {
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
            }
        }),
        cleanup_async(move || {
            let started_tx = started_tx.clone();
            async move {
                started_tx.send(()).expect("signal");
                std::future::pending::<()>().await;
            }
        }),
    ]);

    // Poll the dispose future once (starts the pending cleanup), then drop it:
    // the unstarted cleanup is put back and the state marked cancelled.
    block_on(std::future::poll_fn(|cx| {
        let mut future = std::pin::pin!(disposer.dispose());
        let _ = future.as_mut().poll(cx);
        std::task::Poll::Ready(())
    }));
    started_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("pending cleanup started");
    assert!(
        !disposer.is_disposed(),
        "cancellation restored the unstarted cleanup to the slot"
    );

    disposer.dispose_blocking();
    assert_eq!(
        restored_runs.load(Ordering::SeqCst),
        1,
        "S-1: dispose_blocking must retry cleanups restored by a cancelled teardown"
    );
    // The retry completed the ordered teardown: a later async dispose is a no-op.
    block_on(disposer.dispose());
    assert_eq!(restored_runs.load(Ordering::SeqCst), 1);
}

/// S-1 fiber level (audit `s4_fiber_leak`): after a `dispose_async` future is
/// cancelled mid-teardown, the sync `Fiber::dispose()` safety net must run the
/// restored async cleanups (0.4.1 did; the FIX-15 early return dropped them).
#[test]
fn fix15_cancelled_dispose_async_is_retried_by_fiber_dispose() {
    struct EffectPlugin {
        restored_runs: Arc<AtomicUsize>,
        started: Mutex<Option<mpsc::Sender<()>>>,
    }
    impl Plugin for EffectPlugin {
        fn name(&self) -> &'static str {
            "fix15-cancel-effect"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            let restored = self.restored_runs.clone();
            let started = self.started.lock().unwrap().take();
            ctx.effect_async(move || {
                vec![
                    cleanup_async(move || {
                        let restored = restored.clone();
                        async move {
                            restored.fetch_add(1, Ordering::SeqCst);
                        }
                    }),
                    cleanup_async(move || {
                        let started = started;
                        async move {
                            if let Some(started) = started {
                                let _ = started.send(());
                            }
                            std::future::pending::<()>().await;
                        }
                    }),
                ]
            })?;
            Ok(())
        }
    }

    let root = Context::root();
    let restored_runs = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = mpsc::channel::<()>();
    let fiber = root
        .plugin(
            Arc::new(EffectPlugin {
                restored_runs: restored_runs.clone(),
                started: Mutex::new(Some(started_tx)),
            }),
            Value::Null,
        )
        .expect("plugin");
    block_on(std::future::poll_fn(|cx| {
        let mut future = std::pin::pin!(fiber.dispose_async());
        let _ = future.as_mut().poll(cx);
        std::task::Poll::Ready(())
    }));
    started_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("pending cleanup started");

    fiber
        .dispose()
        .expect("sync dispose after a cancelled dispose_async");
    assert_eq!(
        restored_runs.load(Ordering::SeqCst),
        1,
        "S-1: the fiber-level sync dispose must retry the restored async cleanup"
    );
}

/// S-2 (audit B-2): a cleanup that calls `dispose_blocking()` on its *own*
/// disposer must return instead of waiting for a teardown it is part of.
#[test]
fn fix15_self_join_from_cleanup_returns_instead_of_deadlocking() {
    let slot: Arc<Mutex<Option<AsyncDisposer>>> = Arc::new(Mutex::new(None));
    let slot_for_cleanup = slot.clone();
    let disposer = AsyncDisposer::new(vec![cleanup_async(move || {
        let slot = slot_for_cleanup.clone();
        async move {
            if let Some(inner) = slot.lock().unwrap().clone() {
                inner.dispose_blocking();
            }
        }
    })]);
    *slot.lock().unwrap() = Some(disposer.clone());

    let (done_tx, done_rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        disposer.dispose_blocking();
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("S-2: reentrant dispose_blocking must not self-join");
    handle.join().expect("disposing thread");
}

/// R2-1: a cleanup that `await`s `dispose()` on its **own** disposer must return
/// instead of joining a teardown it is itself part of (the async mirror of the
/// S-2 `dispose_blocking` shortcut).
#[test]
fn fix15_self_join_from_cleanup_via_async_dispose_returns() {
    let slot: Arc<Mutex<Option<AsyncDisposer>>> = Arc::new(Mutex::new(None));
    let slot_for_cleanup = slot.clone();
    let disposer = AsyncDisposer::new(vec![cleanup_async(move || {
        let slot = slot_for_cleanup.clone();
        async move {
            // Drop the lock guard before awaiting (the future must be `Send`).
            let inner = slot.lock().unwrap().clone();
            if let Some(inner) = inner {
                // R2-1: nested await on the disposer whose drain is running us.
                inner.dispose().await;
            }
        }
    })]);
    *slot.lock().unwrap() = Some(disposer.clone());

    let (done_tx, done_rx) = mpsc::channel();
    let running = disposer.clone();
    let handle = std::thread::spawn(move || {
        block_on(running.dispose());
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("R2-1: awaiting dispose() on the disposer being drained must not self-join");
    handle.join().expect("disposing thread");
    // The *outer* drain completed the teardown; the nested call did not.
    assert!(disposer.is_disposed());
    block_on(disposer.dispose());
}

/// S-2 fiber level (audit `s4_fiber_selfjoin`): the same reentrancy through
/// `Fiber::dispose()` must not deadlock.
#[test]
fn fix15_self_join_from_fiber_cleanup_returns_instead_of_deadlocking() {
    struct SelfJoinPlugin {
        slot: Arc<Mutex<Option<AsyncDisposer>>>,
    }
    impl Plugin for SelfJoinPlugin {
        fn name(&self) -> &'static str {
            "fix15-self-join"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            let slot = self.slot.clone();
            let disposer = ctx.effect_async(move || {
                let slot = slot.clone();
                vec![cleanup_async(move || {
                    let slot = slot.clone();
                    async move {
                        if let Some(inner) = slot.lock().unwrap().clone() {
                            inner.dispose_blocking();
                        }
                    }
                })]
            })?;
            *self.slot.lock().unwrap() = Some(disposer);
            Ok(())
        }
    }

    let root = Context::root();
    let fiber = root
        .plugin(
            Arc::new(SelfJoinPlugin {
                slot: Arc::new(Mutex::new(None)),
            }),
            Value::Null,
        )
        .expect("plugin");
    let (done_tx, done_rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let _ = fiber.dispose();
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("S-2: fiber dispose with a self-joining cleanup must not deadlock");
    handle.join().expect("disposing thread");
}

// ---------------------------------------------------------------------------
// S-3: a poisoned provider lock must not disable the service
// ---------------------------------------------------------------------------

/// C2 + S-3 (audit B-3): a panicking `Plugin::inject()` is contained (it must
/// not escape, and it must not poison any kernel lock). The replacement still
/// activates with "no declared dependencies", and services keep working.
///
/// The S-3 fallback (`provider_is_active` treats a poisoned lock as active) is
/// no longer reachable through the public API now that C2 contains embedder
/// metadata panics; it stays as defence in depth and is covered by a
/// `service.rs` unit test.
#[test]
fn fix12b_panicking_inject_is_contained_and_keeps_services_working() {
    struct Provider;
    impl Plugin for Provider {
        fn name(&self) -> &'static str {
            "fix12b-provider"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            ctx.provide(GREETING, Arc::new("first".to_string()))?;
            Ok(())
        }
    }

    /// Replacement plugin: `inject()` panics (C2 must contain it), `apply`
    /// provides the service again so the dependent can reactivate.
    struct PanickingInjectProvider;
    impl Plugin for PanickingInjectProvider {
        fn name(&self) -> &'static str {
            "fix12b-panic-inject-provider"
        }

        fn inject(&self) -> &'static [&'static str] {
            panic!("fix12b inject boom")
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            ctx.provide(GREETING, Arc::new("second".to_string()))?;
            Ok(())
        }
    }

    let root = Context::root();
    let provider = root
        .plugin(Arc::new(Provider), Value::Null)
        .expect("provider");
    let consumer = root
        .plugin(
            Arc::new(NeedsGreeting {
                applied: Arc::new(AtomicUsize::new(0)),
            }),
            Value::Null,
        )
        .expect("consumer");
    assert_eq!(consumer.state(), FiberState::Active);

    // C2: the embedder panic is contained, so this is an ordinary successful
    // replace (no panic escapes this `Result` API).
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        provider.replace_plugin(Arc::new(PanickingInjectProvider))
    }));
    assert!(
        outcome.is_ok(),
        "C2: Plugin::inject panics are contained, not propagated"
    );
    let replaced = outcome.unwrap();
    assert!(
        replaced.is_ok(),
        "the replacement activates without declared dependencies: {replaced:?}"
    );
    assert_eq!(provider.state(), FiberState::Active);
    assert!(
        root.service_available("fix12.greeting"),
        "the service is provided again by the replacement"
    );
    assert_eq!(
        root.get(GREETING).expect("readable").as_str(),
        "second",
        "the replacement's value is live"
    );
    assert_eq!(
        consumer.state(),
        FiberState::Active,
        "the dependent reactivated"
    );

    // The fiber stays fully usable after the contained metadata panic.
    consumer.dispose().expect("dispose consumer");
    provider.dispose().expect("dispose provider");
    assert_eq!(
        root.registry().size(),
        0,
        "both plugin runtimes are released after disposal"
    );
}

// ---------------------------------------------------------------------------
// B-4': a default that contradicts its declared kind is a clear schema issue
// ---------------------------------------------------------------------------

/// Audit B-4': `default_value` + a conflicting `property()` used to inject the
/// default and only report "field must be X, got Y" after the fact. The schema
/// now reports the authoring error up front and does not inject the value.
#[test]
fn fix17_conflicting_default_and_declared_type_reports_a_clear_issue() {
    let schemas = [
        JsonConfigSchema::new()
            .default_value("port", json!(8080))
            .property("port", JsonType::String),
        JsonConfigSchema::new()
            .property("port", JsonType::String)
            .default_value("port", json!(8080)),
    ];
    for schema in schemas {
        let issues = schema
            .validate(json!({}))
            .expect_err("a mis-declared default is rejected");
        assert_eq!(issues.len(), 1, "exactly one clear issue, got {issues:?}");
        assert!(
            issues[0].contains("default value for `port`")
                && issues[0].contains("string")
                && issues[0].contains("number"),
            "the issue must name the key, the declared kind and the actual kind: {issues:?}"
        );
        // The authoring error is reported even when the caller supplies a valid
        // value (it is a schema defect, not a user-input defect).
        assert!(schema.validate(json!({ "port": "8080" })).is_err());
    }

    // A matching default still injects and validates normally.
    let ok = JsonConfigSchema::new()
        .default_value("port", json!(8080))
        .property("port", JsonType::Number);
    assert_eq!(
        ok.validate(json!({})).expect("matching default"),
        json!({ "port": 8080 })
    );
}

// ---------------------------------------------------------------------------
// FIX-11b: the registry registration window blocks `when_idle`
// ---------------------------------------------------------------------------

/// Pre-fix: the quiescence window only started once `create_fiber_handle`
/// returned (fiber-level `pending_ops`), so an `internal/plugin` create hook
/// blocking inside `ctx.plugin(..)` left the registry reporting idle:
/// `when_idle()` returned while a plugin was still being registered.
#[test]
fn fix11_registration_window_keeps_when_idle_blocked() {
    let root = Context::root();
    let (hooked_tx, hooked_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Mutex::new(release_rx);

    let _hook = root
        .on_internal_plugin(Arc::new(move |_fiber: &Fiber| {
            hooked_tx.send(()).expect("signal create hook");
            // Never block the test forever if an assertion below fails.
            let _ = release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(20));
        }))
        .expect("internal/plugin hook");

    let load_root = root.clone();
    let loader = std::thread::spawn(move || load_root.plugin(Arc::new(Noop), Value::Null));
    hooked_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("create hook runs inside ctx.plugin");

    // Another thread waiting for quiescence must stay blocked while the
    // registration window is open.
    let (idle_tx, idle_rx) = mpsc::channel();
    let idle_root = root.clone();
    let waiter = std::thread::spawn(move || {
        block_on(idle_root.when_idle());
        let _ = idle_tx.send(());
    });
    assert!(
        idle_rx.recv_timeout(Duration::from_millis(300)).is_err(),
        "FIX-11b: when_idle() must not return while a plugin registration is in progress"
    );

    release_tx.send(()).expect("release create hook");
    let fiber = loader.join().expect("loader thread").expect("plugin fiber");
    assert_eq!(fiber.state(), FiberState::Active);
    idle_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("when_idle wakes once the registration window closes");
    waiter.join().expect("idle waiter");
}

// ---------------------------------------------------------------------------
// C4 (0.4.3): `when_idle` inside a registration frame does not wait for itself
// ---------------------------------------------------------------------------

/// Audit R3-A/C4: calling `when_idle()` from inside an `internal/plugin` create
/// hook used to wait for the registration that the calling thread itself holds
/// (a self-deadlock). The registry now excludes the caller's own registrations
/// from the quiescence check; external waiters still wait (FIX-11b).
#[test]
fn c4_when_idle_inside_a_registration_frame_does_not_wait_for_itself() {
    let root = Context::root();
    let calls = Arc::new(AtomicUsize::new(0));
    let hook_calls = calls.clone();
    let _hook = root
        .on_internal_plugin(Arc::new(move |fiber: &Fiber| {
            hook_calls.fetch_add(1, Ordering::SeqCst);
            let ctx = fiber.context().expect("create hook sees the fiber context");
            // Must return promptly: waiting for our own registration can never
            // complete while this hook is running.
            block_on(ctx.when_idle());
        }))
        .expect("internal/plugin hook");

    let (tx, rx) = mpsc::channel();
    let load_root = root.clone();
    let loader = std::thread::spawn(move || {
        let result = load_root.plugin(Arc::new(Noop), Value::Null).map(|_| ());
        let _ = tx.send(result);
    });
    rx.recv_timeout(Duration::from_secs(10))
        .expect("C4: when_idle inside a create hook must not wait for its own registration")
        .expect("plugin load");
    loader.join().expect("loader thread");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// F1/F4/F5 (0.4.3): cancelled async cascades park frames in `Unloading`;
// quiescence entry points recover them while waiting, and a cancelled recovery
// must not latch
// ---------------------------------------------------------------------------

/// Child plugin with two async cleanups:
///
/// - the first registered runs **last** and is counted (optionally gated on
///   `restored_gate`): it stays unstarted when the cascade is cancelled, is
///   restored by the cancel-safe unload, and is re-run by the recovery pass;
/// - the last registered runs **first**: it signals `started` and waits on
///   `inflight_gate`, so the first poll of the dispose future returns `Pending`.
///   That cleanup is in flight when the future is cancelled and cannot be
///   recovered (Rust has no cancel-safe await), which is the documented A-9
///   limitation.
struct F1Child {
    runs: Arc<AtomicUsize>,
    started: Mutex<Option<mpsc::Sender<()>>>,
    inflight_gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    restored_gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl Plugin for F1Child {
    fn name(&self) -> &'static str {
        "f1-child"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let restored_runs = self.runs.clone();
        let started = self.started.lock().unwrap().take();
        let inflight_gate = self.inflight_gate.lock().unwrap().take();
        let restored_gate = self.restored_gate.lock().unwrap().take();
        ctx.effect_async(move || {
            vec![
                cleanup_async(move || {
                    let runs = restored_runs.clone();
                    let gate = restored_gate;
                    async move {
                        if let Some(gate) = gate {
                            let _ = gate.await;
                        }
                        runs.fetch_add(1, Ordering::SeqCst);
                    }
                }),
                cleanup_async(move || {
                    let started = started;
                    let gate = inflight_gate;
                    async move {
                        if let Some(started) = started {
                            let _ = started.send(());
                        }
                        if let Some(gate) = gate {
                            let _ = gate.await;
                        }
                    }
                }),
            ]
        })?;
        Ok(())
    }
}

/// Parent plugin: loads the gated child, so disposing the parent cascades into
/// the child's async teardown.
struct F1Parent {
    runs: Arc<AtomicUsize>,
    started: Mutex<Option<mpsc::Sender<()>>>,
    inflight_gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    restored_gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl Plugin for F1Parent {
    fn name(&self) -> &'static str {
        "f1-parent"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.plugin(
            Arc::new(F1Child {
                runs: self.runs.clone(),
                started: Mutex::new(self.started.lock().unwrap().take()),
                inflight_gate: Mutex::new(self.inflight_gate.lock().unwrap().take()),
                restored_gate: Mutex::new(self.restored_gate.lock().unwrap().take()),
            }),
            Value::Null,
        )?;
        Ok(())
    }
}

/// Fixture for one cascade that can be cancelled mid-teardown.
struct F1Cascade {
    root: Context,
    parent: Fiber,
    runs: Arc<AtomicUsize>,
    started: mpsc::Receiver<()>,
    inflight_gate: Option<tokio::sync::oneshot::Sender<()>>,
    restored_gate: Option<tokio::sync::oneshot::Sender<()>>,
}

fn start_f1_cascade_on(root: &Context) -> F1Cascade {
    let runs = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = mpsc::channel();
    let (inflight_tx, inflight_rx) = tokio::sync::oneshot::channel();
    let (restored_tx, restored_rx) = tokio::sync::oneshot::channel();
    let parent = root
        .plugin(
            Arc::new(F1Parent {
                runs: runs.clone(),
                started: Mutex::new(Some(started_tx)),
                inflight_gate: Mutex::new(Some(inflight_rx)),
                restored_gate: Mutex::new(Some(restored_rx)),
            }),
            Value::Null,
        )
        .expect("parent fiber");
    assert_eq!(parent.state(), FiberState::Active);
    F1Cascade {
        root: root.clone(),
        parent,
        runs,
        started: started_rx,
        inflight_gate: Some(inflight_tx),
        restored_gate: Some(restored_tx),
    }
}

fn start_f1_cascade() -> F1Cascade {
    start_f1_cascade_on(&Context::root())
}

impl F1Cascade {
    /// Cancel the parent `dispose_async` mid-cascade (the child's in-flight
    /// cleanup is still gated).
    fn cancel(&mut self) {
        let parent = self.parent.clone();
        block_on(std::future::poll_fn(|cx| {
            let mut future = std::pin::pin!(parent.dispose_async());
            let _ = future.as_mut().poll(cx);
            std::task::Poll::Ready(())
        }));
        self.started
            .recv_timeout(Duration::from_secs(10))
            .expect("the child async cleanup started");
    }

    fn release_inflight(&mut self) {
        if let Some(gate) = self.inflight_gate.take() {
            let _ = gate.send(());
        }
    }

    fn release_restored(&mut self) {
        if let Some(gate) = self.restored_gate.take() {
            let _ = gate.send(());
        }
    }

    fn release_all(&mut self) {
        self.release_inflight();
        self.release_restored();
    }

    /// The cancelled cascade must actually leave a frame parked, otherwise the
    /// test would not exercise the recovery path at all.
    fn assert_parked(&self) {
        assert!(
            unloading_fibers(&self.root) >= 1,
            "expected a parked Unloading frame after cancelling the cascade"
        );
    }
}

fn unloading_fibers(root: &Context) -> usize {
    root.registry()
        .fibers()
        .into_iter()
        .filter(|fiber| fiber.state() == FiberState::Unloading)
        .count()
}

/// Poll a future exactly once with a no-op waker (used to start a recovery and
/// then cancel it by dropping the future).
fn poll_once<F: Future>(future: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    future.poll(&mut context)
}

/// F1 (A-9): a cancelled cascade may leave non-root frames parked in
/// `Unloading`. `when_idle()` must recover them (bounded, contained) instead of
/// waiting forever, and the restored cleanup must run exactly once.
#[test]
fn f1_when_idle_recovers_parked_cascade_after_cancel() {
    let mut cascade = start_f1_cascade();
    cascade.cancel();
    cascade.assert_parked();
    cascade.release_all();

    let (done_tx, done_rx) = mpsc::channel();
    let idle_root = cascade.root.clone();
    let waiter = std::thread::spawn(move || {
        block_on(idle_root.when_idle());
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("F1: when_idle must recover parked unloads and return");
    waiter.join().expect("when_idle waiter");
    assert_eq!(
        unloading_fibers(&cascade.root),
        0,
        "no fiber may stay parked in Unloading after when_idle"
    );
    assert_eq!(
        cascade.runs.load(Ordering::SeqCst),
        1,
        "the restored (unstarted) cleanup runs exactly once during recovery"
    );
    cascade.root.dispose_all().expect("cleanup");
}

/// F1: `refresh_async` runs the same recovery before its refresh pass.
#[test]
fn f1_refresh_async_recovers_parked_cascade_after_cancel() {
    let mut cascade = start_f1_cascade();
    cascade.cancel();
    cascade.assert_parked();
    cascade.release_all();

    let (done_tx, done_rx) = mpsc::channel();
    let refresh_root = cascade.root.clone();
    let waiter = std::thread::spawn(move || {
        block_on(refresh_root.refresh_async());
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("F1: refresh_async must recover parked unloads and return");
    waiter.join().expect("refresh waiter");
    assert_eq!(
        unloading_fibers(&cascade.root),
        0,
        "no fiber may stay parked in Unloading after refresh_async"
    );
    assert_eq!(cascade.runs.load(Ordering::SeqCst), 1);
    cascade.root.dispose_all().expect("cleanup");
}

/// F1 fast path: with nothing parked, `when_idle()` still resolves immediately
/// (repeatedly), so recovery adds no observable cost.
#[test]
fn f1_when_idle_without_parked_unloads_is_immediate() {
    let root = Context::root();
    let (done_tx, done_rx) = mpsc::channel();
    let idle_root = root.clone();
    let waiter = std::thread::spawn(move || {
        for _ in 0..5 {
            block_on(idle_root.when_idle());
        }
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("when_idle without parked unloads must be immediate");
    waiter.join().expect("waiter");
    root.dispose_all().expect("cleanup");
}

/// F5 (a): parked frames plus an *active* unload at entry. `when_idle()` must
/// wait for the in-flight unload body to end (recovery would otherwise be
/// skipped forever) and then recover the parked frames.
#[test]
fn f5_when_idle_waits_for_active_unload_then_recovers_parked_frames() {
    let root = Context::root();

    // Independent fiber whose `dispose_async` is in flight (gated cleanup).
    // Created *before* the cascade: while an unload body is active, sync
    // dependency refreshes skip parked recovery (they must not block on async
    // cleanups), so cancelling afterwards really parks a frame for `when_idle`.
    let blocker_runs = Arc::new(AtomicUsize::new(0));
    let (blocker_started_tx, blocker_started_rx) = mpsc::channel();
    let (blocker_gate_tx, blocker_gate_rx) = tokio::sync::oneshot::channel();
    let blocker = root
        .plugin(
            Arc::new(F1Child {
                runs: blocker_runs.clone(),
                started: Mutex::new(Some(blocker_started_tx)),
                inflight_gate: Mutex::new(Some(blocker_gate_rx)),
                restored_gate: Mutex::new(None),
            }),
            Value::Null,
        )
        .expect("blocker fiber");
    let blocker_thread = std::thread::spawn(move || block_on(blocker.dispose_async()));
    blocker_started_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("blocker cleanup started");

    let mut cascade = start_f1_cascade_on(&root);
    cascade.cancel();
    cascade.assert_parked();

    let (done_tx, done_rx) = mpsc::channel();
    let idle_root = cascade.root.clone();
    let waiter = std::thread::spawn(move || {
        block_on(idle_root.when_idle());
        let _ = done_tx.send(());
    });
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        done_rx.try_recv().is_err(),
        "when_idle must wait while an unload body is in flight"
    );

    let _ = blocker_gate_tx.send(());
    cascade.release_all();
    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("F5: when_idle must recover parked frames once the active unload ends");
    waiter.join().expect("waiter");
    blocker_thread
        .join()
        .expect("blocker thread")
        .expect("blocker dispose");
    assert_eq!(unloading_fibers(&cascade.root), 0);
    assert_eq!(cascade.runs.load(Ordering::SeqCst), 1);
    cascade.root.dispose_all().expect("cleanup");
}

/// F5 (b): a concurrent cancellation parks a **new** frame while `when_idle()`
/// is already waiting; the wait must observe it (park wakes the waiter) and
/// recover it instead of returning/hanging without it.
#[test]
fn f5_when_idle_recovers_parked_frames_that_appear_while_waiting() {
    let root = Context::root();

    // Keep the registry busy so `when_idle()` genuinely waits.
    let blocker_runs = Arc::new(AtomicUsize::new(0));
    let (blocker_started_tx, blocker_started_rx) = mpsc::channel();
    let (blocker_gate_tx, blocker_gate_rx) = tokio::sync::oneshot::channel();
    let blocker = root
        .plugin(
            Arc::new(F1Child {
                runs: blocker_runs.clone(),
                started: Mutex::new(Some(blocker_started_tx)),
                inflight_gate: Mutex::new(Some(blocker_gate_rx)),
                restored_gate: Mutex::new(None),
            }),
            Value::Null,
        )
        .expect("blocker fiber");
    let blocker_thread = std::thread::spawn(move || block_on(blocker.dispose_async()));
    blocker_started_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("blocker cleanup started");

    let (done_tx, done_rx) = mpsc::channel();
    let idle_root = root.clone();
    let waiter = std::thread::spawn(move || {
        block_on(idle_root.when_idle());
        let _ = done_tx.send(());
    });
    std::thread::sleep(Duration::from_millis(100));
    assert!(done_rx.try_recv().is_err(), "when_idle must be waiting");

    // A cancellation during the wait parks a new frame.
    let mut cascade = start_f1_cascade_on(&root);
    cascade.cancel();
    cascade.assert_parked();

    let _ = blocker_gate_tx.send(());
    cascade.release_all();
    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("F5: when_idle must recover frames parked during the wait");
    waiter.join().expect("waiter");
    blocker_thread
        .join()
        .expect("blocker thread")
        .expect("blocker dispose");
    assert_eq!(unloading_fibers(&root), 0);
    assert_eq!(cascade.runs.load(Ordering::SeqCst), 1);
    root.dispose_all().expect("cleanup");
}

/// F5 (c): 20 consecutive cancel → `when_idle()` rounds complete without a
/// hang and each restores exactly its own cleanup.
#[test]
fn f5_twenty_cancel_and_when_idle_rounds_do_not_hang() {
    let (done_tx, done_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let root = Context::root();
        for round in 0..20 {
            let mut cascade = start_f1_cascade_on(&root);
            cascade.cancel();
            cascade.assert_parked();
            cascade.release_all();
            block_on(root.when_idle());
            assert_eq!(
                unloading_fibers(&root),
                0,
                "round {round} must fully recover parked frames"
            );
            assert_eq!(
                cascade.runs.load(Ordering::SeqCst),
                1,
                "round {round} runs the restored cleanup exactly once"
            );
        }
        let _ = done_tx.send(());
        root
    });
    done_rx
        .recv_timeout(Duration::from_secs(40))
        .expect("F5: 20 cancel -> when_idle rounds must not hang");
    let root = worker.join().expect("round worker");
    root.dispose_all().expect("cleanup");
}

/// F4: cancelling a `when_idle()` future in the middle of recovery must not
/// latch the recovery section. The old manual `recovering_parked` flag stayed
/// `true` forever in that case, so every later `when_idle()` skipped recovery
/// and hung on the permanently `Unloading` frame.
///
/// A-9 note: the cleanup that was in flight when the recovery was cancelled is
/// lost (Rust has no cancel-safe await), so the retried recovery may complete the
/// frame without re-running that cleanup; the discriminating assertion is that
/// the retried `when_idle()` returns (watchdog) and leaves no `Unloading` frame.
#[test]
fn f4_cancelled_recovery_does_not_latch_the_next_when_idle() {
    let mut cascade = start_f1_cascade();
    cascade.cancel();
    cascade.assert_parked();

    // Start recovery and cancel it while it is awaiting the gated cleanup.
    let idle_ctx = cascade.root.clone();
    let mut future = Box::pin(idle_ctx.when_idle());
    assert!(
        poll_once(future.as_mut()).is_pending(),
        "recovery must be pending (awaiting the gated cleanup)"
    );
    drop(future);

    cascade.release_all();
    let (done_tx, done_rx) = mpsc::channel();
    let idle_root = cascade.root.clone();
    let waiter = std::thread::spawn(move || {
        block_on(idle_root.when_idle());
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("F4: a cancelled recovery must not latch the next when_idle");
    waiter.join().expect("waiter");
    assert_eq!(
        unloading_fibers(&cascade.root),
        0,
        "no frame may stay parked after the retried recovery"
    );
    assert!(
        cascade.runs.load(Ordering::SeqCst) <= 1,
        "the restored cleanup must never run twice"
    );
    cascade.root.dispose_all().expect("cleanup");
}

// ---------------------------------------------------------------------------
// XT-02: refresh-driven unloads are serialized with lifecycle ops, and an
// activation never publishes `Active` when an injected service was withdrawn
// while `apply` ran.
// ---------------------------------------------------------------------------

static XT02_SERVICE: ServiceKey<u32> = ServiceKey::new("xt02.service");

struct Xt02Provider {
    self_handle: Arc<Mutex<Option<Fiber>>>,
}

impl Plugin for Xt02Provider {
    fn name(&self) -> &'static str {
        "xt02-provider"
    }
    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.provide(XT02_SERVICE, Arc::new(7u32))?;
        *self.self_handle.lock().unwrap() = Some(ctx.fiber());
        Ok(())
    }
}

struct Xt02Consumer {
    provider: Arc<Mutex<Option<Fiber>>>,
    applies: Arc<AtomicUsize>,
}

impl Plugin for Xt02Consumer {
    fn name(&self) -> &'static str {
        "xt02-consumer"
    }
    fn inject(&self) -> &'static [&'static str] {
        &["xt02.service"]
    }
    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        assert!(
            ctx.get(XT02_SERVICE).is_some(),
            "consumer injected dependency"
        );
        self.applies.fetch_add(1, Ordering::SeqCst);
        // Withdraw the provider *during* the consumer's activation: after this
        // apply returns, the dependency is gone and the activation must not
        // publish `Active`.
        if let Some(provider) = self.provider.lock().unwrap().take() {
            provider.dispose().expect("provider dispose");
        }
        Ok(())
    }
}

/// XT-02 regression: a dependency withdrawn while the consumer's `apply` runs
/// must roll the activation back to `Pending` instead of publishing `Active`
/// with a missing injected service.
#[test]
fn xt02_activation_rolls_back_when_injection_is_withdrawn_mid_apply() {
    let root = Context::root();
    let provider_slot: Arc<Mutex<Option<Fiber>>> = Arc::new(Mutex::new(None));
    let applies = Arc::new(AtomicUsize::new(0));

    let provider = root
        .plugin(
            Arc::new(Xt02Provider {
                self_handle: provider_slot.clone(),
            }),
            Value::Null,
        )
        .expect("provider");
    assert_eq!(provider.state(), FiberState::Active);
    assert!(root.get(XT02_SERVICE).is_some());

    let consumer = root
        .plugin(
            Arc::new(Xt02Consumer {
                provider: provider_slot.clone(),
                applies: applies.clone(),
            }),
            Value::Null,
        )
        .expect("consumer");

    assert_eq!(
        applies.load(Ordering::SeqCst),
        1,
        "consumer applied exactly once"
    );
    assert_eq!(
        consumer.state(),
        FiberState::Pending,
        "XT-02: activation must roll back to Pending when an injected service disappears mid-apply"
    );
    assert!(
        root.get(XT02_SERVICE).is_none(),
        "provider withdrawal removed the service"
    );
    root.dispose_all().expect("cleanup");
}
