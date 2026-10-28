//! Conformance tests for the async effect / dispatch / waterfall / service
//! check layer.
//!
//! The sync lifecycle tests live in `tests/lifecycle.rs`; this
//! module adds the behaviours the async layer is defined by:
//!
//! - async cleanup runs in reverse registration order, exactly once, and the
//!   synchronous disposal path still awaits it (no fire-and-forget);
//! - waterfall middlewares run outermost-first around an explicit `next`
//!   chain, a middleware that skips `next` blocks the rest, and both hold for
//!   the async chain;
//! - async serial/parallel dispatch keeps registration order across the sync
//!   and async lanes;
//! - an availability predicate / gate hides a service from dependents and
//!   unloads them while it is closed.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use cordis::yield_now;
use cordis::*;

type Trace = Arc<Mutex<Vec<&'static str>>>;

fn trace() -> Trace {
    Arc::new(Mutex::new(Vec::new()))
}

fn snapshot(trace: &Trace) -> Vec<&'static str> {
    trace.lock().unwrap().clone()
}

static GREETING: ServiceKey<String> = ServiceKey::new("greeting");

// ---------------------------------------------------------------------------
// Async effects / cleanup
// ---------------------------------------------------------------------------

struct AsyncEffectPlugin {
    trace: Trace,
}

impl Plugin for AsyncEffectPlugin {
    fn name(&self) -> &'static str {
        "async-effects"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let first = self.trace.clone();
        let second = self.trace.clone();
        ctx.effect_async(move || {
            vec![
                cleanup_async(move || {
                    let trace = first.clone();
                    async move {
                        trace.lock().unwrap().push("async-first");
                    }
                }),
                cleanup_async(move || {
                    let trace = second.clone();
                    async move {
                        trace.lock().unwrap().push("async-second");
                    }
                }),
            ]
        })?;
        Ok(())
    }
}

#[test]
fn async_cleanup_runs_in_reverse_order_once() {
    let root = Context::root();
    let trace = trace();
    let fiber = root
        .plugin(
            Arc::new(AsyncEffectPlugin {
                trace: trace.clone(),
            }),
            Value::Null,
        )
        .expect("plugin fiber");
    assert_eq!(fiber.state(), FiberState::Active);

    block_on(fiber.dispose_async()).expect("dispose");
    assert_eq!(snapshot(&trace), vec!["async-second", "async-first"]);

    block_on(fiber.dispose_async()).expect("dispose is idempotent");
    assert_eq!(
        snapshot(&trace),
        vec!["async-second", "async-first"],
        "cleanups must run exactly once"
    );
}

#[test]
fn sync_dispose_awaits_async_cleanups_before_returning() {
    let root = Context::root();
    let trace = trace();
    let fiber = root
        .plugin(
            Arc::new(AsyncEffectPlugin {
                trace: trace.clone(),
            }),
            Value::Null,
        )
        .unwrap();

    // The sync path must complete the cleanups, not spawn them and return.
    fiber.dispose().expect("sync dispose");
    assert_eq!(snapshot(&trace), vec!["async-second", "async-first"]);
}

struct MixedEffectPlugin {
    trace: Trace,
}

impl Plugin for MixedEffectPlugin {
    fn name(&self) -> &'static str {
        "mixed-effects"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let sync_a = self.trace.clone();
        ctx.effect(move || vec![cleanup(move || sync_a.lock().unwrap().push("sync-a"))])?;
        let asynchronous = self.trace.clone();
        ctx.effect_async(move || {
            vec![cleanup_async(move || {
                let trace = asynchronous.clone();
                async move {
                    trace.lock().unwrap().push("async-b");
                }
            })]
        })?;
        let sync_c = self.trace.clone();
        ctx.effect(move || vec![cleanup(move || sync_c.lock().unwrap().push("sync-c"))])?;
        Ok(())
    }
}

#[test]
fn sync_and_async_effects_share_reverse_registration_order() {
    let root = Context::root();
    let trace = trace();
    let fiber = root
        .plugin(
            Arc::new(MixedEffectPlugin {
                trace: trace.clone(),
            }),
            Value::Null,
        )
        .unwrap();

    block_on(fiber.dispose_async()).unwrap();
    assert_eq!(snapshot(&trace), vec!["sync-c", "async-b", "sync-a"]);
}

#[test]
fn async_disposer_is_single_shot() {
    let trace = trace();
    let recorded = trace.clone();
    let disposer = AsyncDisposer::new(vec![cleanup_async(move || {
        let trace = recorded.clone();
        async move {
            trace.lock().unwrap().push("only");
        }
    })]);

    block_on(disposer.dispose());
    assert!(disposer.is_disposed());
    block_on(disposer.dispose());
    assert_eq!(snapshot(&trace), vec!["only"]);
}

#[test]
fn async_cleanup_panic_does_not_skip_remaining_cleanups() {
    let root = Context::root();
    let trace = trace();
    let panicking = trace.clone();
    let surviving = trace.clone();
    let fiber = root
        .inject(&[], move |ctx| {
            let panicking = panicking.clone();
            let surviving = surviving.clone();
            ctx.effect_async(move || {
                vec![
                    // Registered first => disposed last, after the other one.
                    cleanup_async(move || {
                        let trace = panicking.clone();
                        async move {
                            trace.lock().unwrap().push("panicking");
                            panic!("cleanup boom");
                        }
                    }),
                    cleanup_async(move || {
                        let trace = surviving.clone();
                        async move {
                            trace.lock().unwrap().push("surviving");
                        }
                    }),
                ]
            })?;
            Ok(())
        })
        .unwrap();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fiber.dispose()));
    assert!(result.is_err(), "the first captured panic is resumed");
    assert_eq!(snapshot(&trace), vec!["surviving", "panicking"]);
}

struct CaptureContext {
    slot: Arc<Mutex<Option<Context>>>,
}

impl Plugin for CaptureContext {
    fn name(&self) -> &'static str {
        "capture-context"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        *self.slot.lock().unwrap() = Some(ctx);
        Ok(())
    }
}

#[test]
fn async_effect_on_inactive_fiber_is_rejected() {
    let root = Context::root();
    let slot = Arc::new(Mutex::new(None));
    let fiber = root
        .plugin(Arc::new(CaptureContext { slot: slot.clone() }), Value::Null)
        .unwrap();
    fiber.dispose().unwrap();

    let ctx = slot.lock().unwrap().clone().expect("captured ctx");
    assert_eq!(
        ctx.effect_async(Vec::new).err(),
        Some(CordisError::InactiveEffect)
    );
    assert_eq!(
        ctx.effect(Vec::new).err(),
        Some(CordisError::InactiveEffect)
    );
}

// ---------------------------------------------------------------------------
// Waterfall: explicit next chain
// ---------------------------------------------------------------------------

#[test]
fn waterfall_runs_middleware_in_order_with_explicit_next() {
    let root = Context::root();
    let trace = trace();

    let outer = trace.clone();
    root.on_waterfall("request", move |_args, next| {
        outer.lock().unwrap().push("outer:in");
        let value = next.next();
        outer.lock().unwrap().push("outer:out");
        json!({ "outer": value })
    })
    .expect("outer middleware");

    let inner = trace.clone();
    root.on_waterfall("request", move |_args, next| {
        inner.lock().unwrap().push("inner:in");
        assert_eq!(next.remaining(), 0, "inner middleware is the last one");
        let value = next.next();
        inner.lock().unwrap().push("inner:out");
        json!({ "inner": value })
    })
    .expect("inner middleware");

    let terminal_calls = Arc::new(AtomicUsize::new(0));
    let calls = terminal_calls.clone();
    let value = root.waterfall(
        "request",
        &[json!(1)],
        Some(waterfall_terminal(move |args| {
            calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(args, &[json!(1)], "middleware must pass the dispatch args");
            json!("terminal")
        })),
    );

    assert_eq!(
        snapshot(&trace),
        vec!["outer:in", "inner:in", "inner:out", "outer:out"]
    );
    assert_eq!(value, json!({"outer": {"inner": "terminal"}}));
    assert_eq!(terminal_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn waterfall_middleware_that_skips_next_blocks_the_chain() {
    let root = Context::root();
    let trace = trace();

    // First middleware vetoes: returns without calling `next`.
    root.on_waterfall("guard", |_args, _next| json!("vetoed"))
        .expect("vetoing middleware");

    let later = trace.clone();
    root.on_waterfall("guard", move |_args, next| {
        later.lock().unwrap().push("later");
        next.next()
    })
    .expect("later middleware");

    let terminal = trace.clone();
    let value = root.waterfall(
        "guard",
        &[],
        Some(waterfall_terminal(move |_args| {
            terminal.lock().unwrap().push("terminal");
            json!("terminal")
        })),
    );

    assert_eq!(value, json!("vetoed"));
    assert!(
        snapshot(&trace).is_empty(),
        "a middleware that skips next blocks later middlewares and the terminal"
    );
}

#[test]
fn waterfall_next_may_be_called_more_than_once() {
    let root = Context::root();
    let calls = Arc::new(AtomicUsize::new(0));
    root.on_waterfall("retry", |_args, next| {
        let first = next.next();
        let second = next.next();
        json!([first, second])
    })
    .expect("retry middleware");

    let counter = calls.clone();
    let value = root.waterfall(
        "retry",
        &[],
        Some(waterfall_terminal(move |_args| {
            let call = counter.fetch_add(1, Ordering::SeqCst) + 1;
            json!(call)
        })),
    );

    assert_eq!(value, json!([1, 2]), "next re-runs the remaining chain");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

struct WaterfallPlugin {
    trace: Trace,
}

impl Plugin for WaterfallPlugin {
    fn name(&self) -> &'static str {
        "waterfall-plugin"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let trace = self.trace.clone();
        ctx.on_waterfall("hook", move |_args, next| {
            trace.lock().unwrap().push("plugin");
            next.next()
        })?;
        Ok(())
    }
}

#[test]
fn waterfall_listeners_unload_with_their_fiber() {
    let root = Context::root();
    let trace = trace();
    let fiber = root
        .plugin(
            Arc::new(WaterfallPlugin {
                trace: trace.clone(),
            }),
            Value::Null,
        )
        .unwrap();

    assert_eq!(root.waterfall("hook", &[], None), Value::Null);
    assert_eq!(snapshot(&trace), vec!["plugin"]);
    assert_eq!(root.events().waterfall_listener_count(), 1);

    fiber.dispose().unwrap();
    assert_eq!(root.events().waterfall_listener_count(), 0);
    assert_eq!(
        root.waterfall("hook", &[], Some(waterfall_terminal(|_args| json!("base")))),
        json!("base")
    );
    assert_eq!(snapshot(&trace), vec!["plugin"]);
}

#[test]
fn async_waterfall_awaits_chain_in_order() {
    let root = Context::root();
    let trace = trace();

    let outer = trace.clone();
    root.on_waterfall_async("io", move |_args, next| {
        let trace = outer.clone();
        boxed(async move {
            trace.lock().unwrap().push("outer:in");
            // Force an await point: the chain must still be ordered.
            yield_now().await;
            let value = next.next().await;
            trace.lock().unwrap().push("outer:out");
            json!({ "outer": value })
        })
    })
    .expect("outer async middleware");

    let inner = trace.clone();
    root.on_waterfall_async("io", move |_args, next| {
        let trace = inner.clone();
        boxed(async move {
            trace.lock().unwrap().push("inner:in");
            let value = next.next().await;
            trace.lock().unwrap().push("inner:out");
            value
        })
    })
    .expect("inner async middleware");

    let value = block_on(root.waterfall_async(
        "io",
        &[],
        Some(async_waterfall_terminal(|_args| async {
            json!("terminal")
        })),
    ));

    assert_eq!(
        snapshot(&trace),
        vec!["outer:in", "inner:in", "inner:out", "outer:out"]
    );
    assert_eq!(value, json!({"outer": "terminal"}));
}

#[test]
fn async_waterfall_middleware_that_skips_next_blocks_the_chain() {
    let root = Context::root();
    let ran = Arc::new(AtomicBool::new(false));

    root.on_waterfall_async("guard", |_args, _next| {
        boxed(async move { json!("vetoed") })
    })
    .expect("vetoing async middleware");

    let later = ran.clone();
    root.on_waterfall_async("guard", move |_args, next| {
        let later = later.clone();
        boxed(async move {
            later.store(true, Ordering::SeqCst);
            next.next().await
        })
    })
    .expect("later async middleware");

    let value = block_on(root.waterfall_async("guard", &[], None));
    assert_eq!(value, json!("vetoed"));
    assert!(!ran.load(Ordering::SeqCst));
}

// ---------------------------------------------------------------------------
// Async event dispatch: serial / parallel across lanes
// ---------------------------------------------------------------------------

#[test]
fn serial_async_stops_at_first_bail_across_lanes() {
    let root = Context::root();
    let hits = Arc::new(AtomicUsize::new(0));

    let first = hits.clone();
    root.on("demo", move |_args| {
        first.fetch_add(1, Ordering::SeqCst);
        EventResult::Continue
    })
    .unwrap();

    let second = hits.clone();
    root.on_async("demo", move |_args| {
        let second = second.clone();
        async move {
            second.fetch_add(1, Ordering::SeqCst);
            EventResult::Bail(json!("stop"))
        }
    })
    .unwrap();

    let third = hits.clone();
    root.on_async("demo", move |_args| {
        let third = third.clone();
        async move {
            third.fetch_add(1, Ordering::SeqCst);
            EventResult::Continue
        }
    })
    .unwrap();

    let value = block_on(root.serial_async("demo", &[]));
    assert_eq!(value, Some(json!("stop")));
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "listeners after the first bail must not run"
    );
}

#[test]
fn parallel_async_interleaves_listeners_and_ignores_bail() {
    let root = Context::root();
    let trace = trace();

    let a = trace.clone();
    root.on_async("demo", move |_args| {
        let trace = a.clone();
        boxed(async move {
            trace.lock().unwrap().push("a:in");
            yield_now().await;
            trace.lock().unwrap().push("a:out");
            EventResult::Bail(json!("ignored"))
        })
    })
    .unwrap();

    let b = trace.clone();
    root.on_async("demo", move |_args| {
        let trace = b.clone();
        boxed(async move {
            trace.lock().unwrap().push("b:in");
            yield_now().await;
            trace.lock().unwrap().push("b:out");
            EventResult::Continue
        })
    })
    .unwrap();

    let guard = Arc::new(AtomicUsize::new(0));
    let guard_counter = guard.clone();
    root.on("demo", move |_args| {
        guard_counter.fetch_add(1, Ordering::SeqCst);
        EventResult::Continue
    })
    .unwrap();

    block_on(root.parallel_async("demo", &[]));

    // Interleaved, not one-after-another: both listeners reach their first
    // await point before either resumes.
    assert_eq!(snapshot(&trace), vec!["a:in", "b:in", "a:out", "b:out"]);
    assert_eq!(
        guard.load(Ordering::SeqCst),
        1,
        "the sync lane participates in parallel dispatch"
    );
}

#[test]
fn async_listeners_unload_with_their_fiber() {
    let root = Context::root();
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    let fiber = root
        .inject(&[], move |ctx| {
            let counter = counter.clone();
            ctx.on_async("demo", move |_args| {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    EventResult::Continue
                }
            })?;
            Ok(())
        })
        .unwrap();

    block_on(root.parallel_async("demo", &[]));
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(root.events().async_listener_count(), 1);

    fiber.dispose().unwrap();
    assert_eq!(root.events().async_listener_count(), 0);
    block_on(root.parallel_async("demo", &[]));
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// Service::check availability predicate
// ---------------------------------------------------------------------------

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

#[test]
fn service_gate_controls_dependent_activation() {
    let root = Context::root();
    let gate = ServiceGate::new(false);
    let hits = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(Arc::new(NeedsGreeting { hits: hits.clone() }), Value::Null)
        .unwrap();
    assert_eq!(fiber.state(), FiberState::Pending);

    let service = root
        .provide_gated(GREETING, Arc::new("hello".to_string()), &gate)
        .expect("provide");
    assert_eq!(fiber.state(), FiberState::Pending, "gate is closed");
    assert!(
        root.get(GREETING).is_none(),
        "an unavailable provider must not be readable"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    gate.set(true);
    assert_eq!(
        fiber.state(),
        FiberState::Active,
        "gate flip wakes dependents"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(root.get(GREETING).unwrap().as_str(), "hello");

    gate.set(false);
    assert_eq!(
        fiber.state(),
        FiberState::Pending,
        "dependents unload while the gate is closed"
    );
    assert!(root.get(GREETING).is_none());

    service.dispose();
    assert!(!root.service_available("greeting"));
}

#[test]
fn checked_service_predicate_controls_lookups() {
    let root = Context::root();
    let available = Arc::new(AtomicBool::new(false));
    let flag = available.clone();
    let service = root
        .provide_checked(GREETING, Arc::new("hi".to_string()), move || {
            flag.load(Ordering::SeqCst)
        })
        .expect("provide");

    assert!(root.get(GREETING).is_none());
    assert!(!root.service_available("greeting"));
    assert_eq!(
        root.require(GREETING).unwrap_err(),
        CordisError::UnknownService("greeting".to_string())
    );

    available.store(true, Ordering::SeqCst);
    root.refresh();
    assert_eq!(root.get(GREETING).unwrap().as_str(), "hi");
    assert!(root.service_available("greeting"));

    service.dispose();
    assert!(root.get(GREETING).is_none());
}

#[test]
fn closed_gate_does_not_fall_back_to_an_ancestor_provider() {
    let root = Context::root();
    root.provide(GREETING, Arc::new("root".to_string()))
        .expect("root provider");

    // A scoped provider lives in its own isolation scope. (Official Cordis
    // stores impls on the root store keyed by isolation label: shadowing a name
    // requires `isolate()`, providing it twice in the default scope is a
    // DuplicateService.)
    let gate = ServiceGate::new(false);
    let scoped = root.isolate("greeting");
    scoped
        .provide_gated(GREETING, Arc::new("scoped".to_string()), &gate)
        .expect("scoped provider");

    // The scoped provider shadows the default one even while it is unavailable.
    assert!(scoped.get(GREETING).is_none());
    assert_eq!(root.get(GREETING).unwrap().as_str(), "root");

    gate.set(true);
    assert_eq!(scoped.get(GREETING).unwrap().as_str(), "scoped");
    assert_eq!(
        root.get(GREETING).unwrap().as_str(),
        "root",
        "the scoped provider does not leak into the default scope"
    );
}

// ---------------------------------------------------------------------------
// Runtime edges: restart and spawnability of the async futures
// ---------------------------------------------------------------------------

struct RestartAsyncPlugin {
    trace: Trace,
}

impl Plugin for RestartAsyncPlugin {
    fn name(&self) -> &'static str {
        "restart-async"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let trace = self.trace.clone();
        ctx.effect_async(move || {
            vec![cleanup_async(move || {
                let trace = trace.clone();
                async move {
                    trace.lock().unwrap().push("cleanup");
                }
            })]
        })?;
        Ok(())
    }
}

#[test]
fn restart_async_drains_and_reapplies_async_effects() {
    let root = Context::root();
    let trace = trace();
    let fiber = root
        .plugin(
            Arc::new(RestartAsyncPlugin {
                trace: trace.clone(),
            }),
            Value::Null,
        )
        .unwrap();

    block_on(fiber.restart_async()).expect("restart");
    assert_eq!(
        snapshot(&trace),
        vec!["cleanup"],
        "restart awaits the old async effect before re-applying"
    );
    assert_eq!(fiber.state(), FiberState::Active);

    block_on(fiber.update_async(json!({ "n": 1 }))).expect("update");
    assert_eq!(snapshot(&trace), vec!["cleanup", "cleanup"]);

    block_on(fiber.dispose_async()).unwrap();
    assert_eq!(snapshot(&trace), vec!["cleanup", "cleanup", "cleanup"]);
}

struct NoopPlugin;

impl Plugin for NoopPlugin {
    fn name(&self) -> &'static str {
        "noop"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        Ok(())
    }
}

fn assert_send<T: Send>(_: T) {}

#[test]
fn async_apis_produce_send_futures() {
    // Tokio (or any executor) must be able to spawn the kernel's futures.
    let root = Context::root();
    let fiber = root.plugin(Arc::new(NoopPlugin), Value::Null).unwrap();
    assert_send(fiber.dispose_async());
    assert_send(fiber.restart_async());
    assert_send(fiber.update_async(json!({})));
    assert_send(root.parallel_async("demo", &[]));
    assert_send(root.serial_async("demo", &[]));
    assert_send(root.waterfall_async("demo", &[], None));
}

#[test]
fn waterfall_terminal_skips_remaining_middlewares() {
    let root = Context::root();
    let ran = Arc::new(AtomicBool::new(false));

    // The first middleware jumps straight to the built-in behavior.
    root.on_waterfall("skip", |_args, next| next.terminal())
        .expect("skipping middleware");

    let later = ran.clone();
    root.on_waterfall("skip", move |_args, next| {
        later.store(true, Ordering::SeqCst);
        next.next()
    })
    .expect("later middleware");

    let value = root.waterfall("skip", &[], Some(waterfall_terminal(|_args| json!("base"))));
    assert_eq!(value, json!("base"));
    assert!(!ran.load(Ordering::SeqCst));
}
