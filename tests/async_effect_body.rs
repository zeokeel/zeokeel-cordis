//! Phase E conformance: async effect bodies (`ctx.effect(async …)`) and streams
//! (`ctx.effect(async function* …)`) plus restart teardown.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;

use zeokeel_cordis::{
    block_on, cleanup, cleanup_async, Context, CordisError, EffectMeta, FiberState, Plugin, Result,
};

#[test]
fn async_effect_body_and_stream() {
    struct AsyncBodies {
        trace: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Plugin for AsyncBodies {
        fn name(&self) -> &'static str {
            "async-bodies"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            let future_trace = Arc::clone(&self.trace);
            ctx.effect_future(
                move || async move {
                    Ok(vec![cleanup_async(move || async move {
                        future_trace.lock().unwrap().push("future");
                    })])
                },
                "future-body",
            )?;

            let stream_trace = Arc::clone(&self.trace);
            let first = Arc::clone(&stream_trace);
            let second = Arc::clone(&stream_trace);
            ctx.effect_stream(
                futures_util::stream::iter(vec![
                    cleanup_async(move || async move {
                        first.lock().unwrap().push("stream-1");
                    }),
                    cleanup_async(move || async move {
                        second.lock().unwrap().push("stream-2");
                    }),
                ]),
                "stream-body",
            )?;
            Ok(())
        }
    }

    let root = Context::root();
    let trace = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(
            Arc::new(AsyncBodies {
                trace: Arc::clone(&trace),
            }),
            Value::Null,
        )
        .expect("fiber");
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(
        fiber
            .get_effects()
            .iter()
            .map(|meta| meta.label.as_str())
            .collect::<Vec<_>>(),
        vec!["future-body", "stream-body"]
    );

    fiber.dispose().expect("dispose awaits async cleanups");
    assert_eq!(
        trace.lock().unwrap().as_slice(),
        &["stream-2", "stream-1", "future"],
        "async cleanups run in reverse registration order"
    );
}

#[test]
fn async_effect_body_error_fails_fiber() {
    struct FailingBody;

    impl Plugin for FailingBody {
        fn name(&self) -> &'static str {
            "failing-body"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            ctx.effect_future(
                || async { Err(CordisError::PluginFailed("async body failed".to_string())) },
                "failing-body",
            )?;
            Ok(())
        }
    }

    let root = Context::root();
    let fiber = root
        .plugin(Arc::new(FailingBody), Value::Null)
        .expect("fiber handle");
    assert_eq!(fiber.state(), FiberState::Failed);
    let error = fiber.error().expect("fiber error");
    assert!(error.contains("async body failed"), "error: {error}");
}

#[test]
fn restart_awaits_async_effect_cleanups() {
    struct Restartable {
        cleanups: Arc<AtomicUsize>,
        runs: Arc<AtomicUsize>,
    }

    impl Plugin for Restartable {
        fn name(&self) -> &'static str {
            "restartable"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            let cleanups = Arc::clone(&self.cleanups);
            ctx.effect_future(
                move || async move {
                    Ok(vec![cleanup_async(move || async move {
                        cleanups.fetch_add(1, Ordering::SeqCst);
                    })])
                },
                "restartable-effect",
            )?;
            Ok(())
        }
    }

    let root = Context::root();
    let cleanups = Arc::new(AtomicUsize::new(0));
    let runs = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(
            Arc::new(Restartable {
                cleanups: Arc::clone(&cleanups),
                runs: Arc::clone(&runs),
            }),
            Value::Null,
        )
        .expect("fiber");
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    block_on(fiber.restart_async()).expect("restart");
    assert_eq!(
        cleanups.load(Ordering::SeqCst),
        1,
        "restart awaited the previous async cleanup"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    assert_eq!(fiber.state(), FiberState::Active);
}

#[test]
fn async_effect_body_nests_labeled_effects() {
    struct Nested;

    impl Plugin for Nested {
        fn name(&self) -> &'static str {
            "nested-async"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            let inner = ctx.clone();
            ctx.effect_future(
                move || async move {
                    inner.effect_labeled("inner", || vec![cleanup(|| {})])?;
                    Ok(vec![cleanup_async(|| async {})])
                },
                "outer-future",
            )?;
            Ok(())
        }
    }

    let root = Context::root();
    let fiber = root.plugin(Arc::new(Nested), Value::Null).expect("fiber");
    assert_eq!(
        fiber.get_effects(),
        vec![EffectMeta {
            label: "outer-future".to_string(),
            children: vec![EffectMeta {
                label: "inner".to_string(),
                children: Vec::new(),
            }],
        }]
    );
}
