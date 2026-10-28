//! Consolidation cases added in v0.2.0 (CORE-028..CORE-036):
//!
//! * failed activation rolls back effects/services and isolates apply panics;
//! * cleanup and listener panics are isolated (and still observable);
//! * trait-object services are supported through the `*_any` APIs;
//! * async plugins activate/roll back/restart through the `*_async` APIs;
//! * the root context/fiber graph has no strong reference cycle.

use std::sync::{Arc, Mutex};

use serde_json::Value;

use cordis::*;

type Trace = Arc<Mutex<Vec<&'static str>>>;

fn trace() -> Trace {
    Arc::new(Mutex::new(Vec::new()))
}

fn snapshot(trace: &Trace) -> Vec<&'static str> {
    trace.lock().unwrap().clone()
}

static TEXT: ServiceKey<String> = ServiceKey::new("hardening.text");

// ---------------------------------------------------------------------------
// CORE-028 / CORE-029: failed activation rollback + panic isolation
// ---------------------------------------------------------------------------

struct FailsAfterRegistering {
    trace: Trace,
    panic: bool,
}

impl Plugin for FailsAfterRegistering {
    fn name(&self) -> &'static str {
        "fails-after-registering"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let trace = self.trace.clone();
        ctx.provide(TEXT, Arc::new("stale".to_string()))?;
        ctx.effect(move || vec![cleanup(move || trace.lock().unwrap().push("rolled-back"))])?;
        if self.panic {
            panic!("apply boom");
        }
        Err(CordisError::PluginFailed("apply failed".to_string()))
    }
}

#[test]
fn failed_apply_rolls_back_effects_and_services() {
    let root = Context::root();
    let trace = trace();
    let fiber = root
        .plugin(
            Arc::new(FailsAfterRegistering {
                trace: trace.clone(),
                panic: false,
            }),
            Value::Null,
        )
        .expect("plugin call itself succeeds");

    assert_eq!(fiber.state(), FiberState::Failed);
    assert!(fiber.error().is_some());
    assert!(
        root.get(TEXT).is_none(),
        "failed activation leaked a service"
    );
    assert_eq!(snapshot(&trace), vec!["rolled-back"]);

    // The kernel keeps running: a healthy sibling still activates.
    let sibling = root
        .plugin(Arc::new(Noop), Value::Null)
        .expect("sibling plugin");
    assert_eq!(sibling.state(), FiberState::Active);

    // C1 regression: a failed provider must not remain injectable, so a
    // dependent stays Pending instead of activating against a dead service.
    let dependent = root
        .plugin(Arc::new(NeedsText), Value::Null)
        .expect("dependent plugin");
    assert_eq!(dependent.state(), FiberState::Pending);
}

#[test]
fn panicking_apply_is_isolated_and_rolled_back() {
    let root = Context::root();
    let trace = trace();
    let fiber = root
        .plugin(
            Arc::new(FailsAfterRegistering {
                trace: trace.clone(),
                panic: true,
            }),
            Value::Null,
        )
        .expect("panic must not escape ctx.plugin");

    assert_eq!(fiber.state(), FiberState::Failed);
    assert!(fiber.error().unwrap_or_default().contains("panic"));
    assert!(root.get(TEXT).is_none());
    assert_eq!(snapshot(&trace), vec!["rolled-back"]);
}

struct NeedsText;

impl Plugin for NeedsText {
    fn name(&self) -> &'static str {
        "needs-text"
    }

    fn inject(&self) -> &'static [&'static str] {
        &["hardening.text"]
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        Ok(())
    }
}

struct Noop;

impl Plugin for Noop {
    fn name(&self) -> &'static str {
        "noop"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// CORE-030: cleanup / listener panic isolation
// ---------------------------------------------------------------------------

#[test]
fn cleanup_panic_does_not_skip_remaining_cleanups() {
    let root = Context::root();
    let trace = trace();
    let fiber = root
        .plugin(
            Arc::new(EffectPlugin {
                trace: trace.clone(),
            }),
            Value::Null,
        )
        .unwrap();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fiber.dispose()));
    assert!(
        outcome.is_err(),
        "the first captured cleanup panic is re-raised"
    );
    assert_eq!(
        snapshot(&trace),
        vec!["second", "first"],
        "every cleanup ran in reverse registration order"
    );
}

struct EffectPlugin {
    trace: Trace,
}

impl Plugin for EffectPlugin {
    fn name(&self) -> &'static str {
        "effect-plugin"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let first = self.trace.clone();
        let second = self.trace.clone();
        ctx.effect(move || {
            vec![
                cleanup(move || first.lock().unwrap().push("first")),
                cleanup(move || second.lock().unwrap().push("second")),
                cleanup(|| panic!("cleanup boom")),
            ]
        })?;
        // Reorder: registration order is first, second, panic -> disposal is
        // panic, second, first; assert below uses that order.
        Ok(())
    }
}

#[test]
fn event_listener_panic_does_not_abort_dispatch() {
    let root = Context::root();
    let trace = trace();
    root.on("evt", |_args: &[Value]| -> EventResult {
        panic!("listener boom")
    })
    .unwrap();
    let later = trace.clone();
    root.on("evt", move |_args: &[Value]| -> EventResult {
        later.lock().unwrap().push("later");
        EventResult::Continue
    })
    .unwrap();

    root.emit("evt", &[]);
    assert_eq!(snapshot(&trace), vec!["later"]);
}

// ---------------------------------------------------------------------------
// CORE-031: trait-object services through `*_any`
// ---------------------------------------------------------------------------

trait Greeter: Send + Sync + 'static {
    fn greet(&self) -> String;
}

struct English;

impl Greeter for English {
    fn greet(&self) -> String {
        "hello".to_string()
    }
}

static GREETER: ServiceKey<dyn Greeter> = ServiceKey::new("hardening.greeter");

struct ProvidesGreeter;

impl Plugin for ProvidesGreeter {
    fn name(&self) -> &'static str {
        "provides-greeter"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.provide_any(GREETER, Arc::new(English))?;
        Ok(())
    }
}

#[test]
fn trait_object_service_keys_work() {
    let root = Context::root();
    let fiber = root.plugin(Arc::new(ProvidesGreeter), Value::Null).unwrap();
    assert_eq!(fiber.state(), FiberState::Active);

    let greeter = root.require_any(GREETER).expect("greeter service");
    assert_eq!(greeter.greet(), "hello");

    fiber.dispose().unwrap();
    assert!(root.get_any(GREETER).is_none());
}

// ---------------------------------------------------------------------------
// CORE-034..CORE-036: async plugins
// ---------------------------------------------------------------------------

struct AsyncInit {
    trace: Trace,
    fail: bool,
}

impl AsyncPlugin for AsyncInit {
    fn name(&self) -> &'static str {
        "async-init"
    }

    fn apply_async<'a>(&'a self, ctx: Context, _config: Value) -> BoxFuture<'a, Result<()>> {
        let trace = self.trace.clone();
        let fail = self.fail;
        boxed(async move {
            ctx.provide(TEXT, Arc::new("async".to_string()))?;
            let async_trace = trace.clone();
            ctx.effect_async(move || {
                vec![cleanup_async(move || {
                    let trace = async_trace.clone();
                    async move {
                        trace.lock().unwrap().push("async-cleanup");
                    }
                })]
            })?;
            yield_now().await;
            if fail {
                return Err(CordisError::PluginFailed("async failed".to_string()));
            }
            Ok(())
        })
    }
}

#[test]
fn async_plugin_activates_and_disposes() {
    let root = Context::root();
    let trace = trace();
    let fiber = block_on(root.plugin_async(
        Arc::new(AsyncInit {
            trace: trace.clone(),
            fail: false,
        }),
        Value::Null,
    ))
    .unwrap();

    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(root.get(TEXT).as_deref().map(String::as_str), Some("async"));

    block_on(fiber.dispose_async()).unwrap();
    assert!(root.get(TEXT).is_none());
    assert_eq!(snapshot(&trace), vec!["async-cleanup"]);
}

#[test]
fn async_plugin_failure_rolls_back() {
    let root = Context::root();
    let trace = trace();
    let fiber = block_on(root.plugin_async(
        Arc::new(AsyncInit {
            trace: trace.clone(),
            fail: true,
        }),
        Value::Null,
    ))
    .unwrap();

    assert_eq!(fiber.state(), FiberState::Failed);
    assert!(
        root.get(TEXT).is_none(),
        "failed async plugin leaked a service"
    );
    assert_eq!(snapshot(&trace), vec!["async-cleanup"]);

    // Sync lifecycle APIs reject async plugins instead of blocking.
    assert!(matches!(
        fiber.restart(),
        Err(CordisError::AsyncPluginNeedsAsync(_))
    ));
}

struct AsyncWaitsForDep {
    fiber_out: Arc<Mutex<Option<Fiber>>>,
}

impl AsyncPlugin for AsyncWaitsForDep {
    fn name(&self) -> &'static str {
        "async-waits"
    }

    fn inject(&self) -> &'static [&'static str] {
        &["hardening.gate"]
    }

    fn apply_async<'a>(&'a self, ctx: Context, _config: Value) -> BoxFuture<'a, Result<()>> {
        let out = self.fiber_out.clone();
        boxed(async move {
            let _ = &out;
            ctx.provide(TEXT, Arc::new("gated".to_string()))?;
            Ok(())
        })
    }
}

#[test]
fn async_plugin_waits_for_inject_and_refresh_async() {
    static GATE: ServiceKey<String> = ServiceKey::new("hardening.gate");
    let root = Context::root();
    let out = Arc::new(Mutex::new(None));
    let fiber = block_on(root.plugin_async(
        Arc::new(AsyncWaitsForDep {
            fiber_out: out.clone(),
        }),
        Value::Null,
    ))
    .unwrap();
    assert_eq!(fiber.state(), FiberState::Pending);
    assert!(root.get(TEXT).is_none());

    root.provide(GATE, Arc::new("ready".to_string())).unwrap();
    // Sync refresh intentionally cannot activate async fibers.
    root.refresh();
    assert_eq!(fiber.state(), FiberState::Pending);
    block_on(root.refresh_async());
    assert_eq!(fiber.state(), FiberState::Active);
    assert!(root.get(TEXT).is_some());
}

struct SyncFailsWithAsyncCleanup {
    trace: Trace,
}

impl Plugin for SyncFailsWithAsyncCleanup {
    fn name(&self) -> &'static str {
        "sync-fails-with-async-cleanup"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let trace = self.trace.clone();
        ctx.effect_async(move || {
            vec![cleanup_async(move || {
                let trace = trace.clone();
                async move {
                    trace.lock().unwrap().push("deferred");
                }
            })]
        })?;
        Err(CordisError::PluginFailed("sync failure".to_string()))
    }
}

#[test]
fn failed_sync_activation_drains_async_cleanup_inline() {
    let root = Context::root();
    let trace = trace();
    let fiber = root
        .plugin(
            Arc::new(SyncFailsWithAsyncCleanup {
                trace: trace.clone(),
            }),
            Value::Null,
        )
        .unwrap();

    assert_eq!(fiber.state(), FiberState::Failed);
    // v0.3: a failed sync activation drains async cleanups during rollback
    // (through the blocking executor), so the reverse registration order holds
    // across lanes and an undisposed Failed fiber cannot leak them.
    assert_eq!(snapshot(&trace), vec!["deferred"]);

    block_on(fiber.dispose_async()).unwrap();
    assert_eq!(snapshot(&trace), vec!["deferred"]);
}

struct GateUser;

impl Plugin for GateUser {
    fn name(&self) -> &'static str {
        "gate-user"
    }

    fn inject(&self) -> &'static [&'static str] {
        &["hardening.gate2"]
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.provide(TEXT, Arc::new("gated".to_string()))?;
        Ok(())
    }
}

#[test]
fn restart_rechecks_inject_gate() {
    static GATE2: ServiceKey<String> = ServiceKey::new("hardening.gate2");
    let root = Context::root();
    let gate = root.provide(GATE2, Arc::new("on".to_string())).unwrap();
    let fiber = root.plugin(Arc::new(GateUser), Value::Null).unwrap();
    assert_eq!(fiber.state(), FiberState::Active);
    assert!(root.get(TEXT).is_some());

    // Removing the injected service unloads the dependent back to Pending.
    gate.dispose();
    assert_eq!(fiber.state(), FiberState::Pending);

    // CORE-039: restart must re-check `inject` instead of activating anyway.
    fiber.restart().expect("restart");
    assert_eq!(fiber.state(), FiberState::Pending);
    assert!(root.get(TEXT).is_none());
}
