//! Phase C conformance: fiber diagnostics (`uid`, `parent`, `getEffects`,
//! `await_settled`) and labeled effect trees.

use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use cordis::{
    block_on, cleanup, cleanup_async, ConfigSchema, Context, CordisError, EffectMeta, FiberState,
    JsonConfigSchema, Plugin, Result,
};

#[test]
fn fiber_reports_effects_and_awaits_settlement() {
    struct EffectsPlugin {
        ctx_slot: Arc<Mutex<Option<Context>>>,
    }

    impl Plugin for EffectsPlugin {
        fn name(&self) -> &'static str {
            "effects-plugin"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            *self.ctx_slot.lock().unwrap() = Some(ctx.clone());
            let nested = ctx.clone();
            ctx.effect_labeled("outer", move || {
                nested
                    .effect_labeled("inner", || vec![cleanup(|| {})])
                    .expect("nested effect registers while outer setup runs");
                vec![cleanup(|| {})]
            })?;
            Ok(())
        }
    }

    let root = Context::root();
    let slot = Arc::new(Mutex::new(None));
    let fiber = root
        .plugin(
            Arc::new(EffectsPlugin {
                ctx_slot: Arc::clone(&slot),
            }),
            Value::Null,
        )
        .expect("fiber");
    block_on(fiber.await_settled()).expect("settled");

    assert_eq!(
        fiber.get_effects(),
        vec![EffectMeta {
            label: "outer".to_string(),
            children: vec![EffectMeta {
                label: "inner".to_string(),
                children: Vec::new(),
            }],
        }]
    );

    fiber.dispose().expect("dispose");
    assert!(
        fiber.get_effects().is_empty(),
        "disposed effects disappear from the tree"
    );
    block_on(fiber.await_settled()).expect("disposal settles");
}

#[test]
fn effect_disposer_removes_its_meta_node() {
    struct OneEffectPlugin {
        disposer: Arc<Mutex<Option<cordis::Disposer>>>,
    }

    impl Plugin for OneEffectPlugin {
        fn name(&self) -> &'static str {
            "one-effect"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            let disposer = ctx.effect_labeled("owned", || vec![cleanup(|| {})])?;
            *self.disposer.lock().unwrap() = Some(disposer);
            Ok(())
        }
    }

    let root = Context::root();
    let slot = Arc::new(Mutex::new(None));
    let fiber = root
        .plugin(
            Arc::new(OneEffectPlugin {
                disposer: Arc::clone(&slot),
            }),
            Value::Null,
        )
        .expect("fiber");
    assert_eq!(fiber.get_effects().len(), 1);
    assert_eq!(fiber.get_effects()[0].label, "owned");

    slot.lock().unwrap().take().expect("disposer").dispose();
    assert!(
        fiber.get_effects().is_empty(),
        "disposing one effect removes only that node"
    );
}

#[test]
fn fiber_uid_parent_and_assert_active() {
    struct Simple;
    impl Plugin for Simple {
        fn name(&self) -> &'static str {
            "simple"
        }

        fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
            Ok(())
        }
    }

    let root = Context::root();
    let fiber = root.plugin(Arc::new(Simple), Value::Null).expect("fiber");
    assert_eq!(fiber.uid(), Some(fiber.id()));
    assert!(fiber.uid().is_some());
    assert!(fiber.assert_active().is_ok());
    assert_eq!(
        fiber.parent().expect("parent context").fiber_id(),
        root.fiber_id(),
        "fiber.parent is the context the plugin was loaded from"
    );

    fiber.dispose().expect("dispose");
    assert_eq!(fiber.uid(), None, "uid is cleared on disposal");
    assert!(matches!(
        fiber.assert_active(),
        Err(CordisError::InactiveEffect)
    ));
}

#[test]
fn await_settled_rethrows_startup_errors() {
    struct NeedsModel;
    impl Plugin for NeedsModel {
        fn name(&self) -> &'static str {
            "needs-model"
        }

        fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>> {
            Some(Arc::new(JsonConfigSchema::new().required("model")))
        }

        fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
            Ok(())
        }
    }

    let root = Context::root();
    let fiber = root
        .plugin(Arc::new(NeedsModel), json!({}))
        .expect("fiber handle");
    assert_eq!(fiber.state(), FiberState::Failed);
    match block_on(fiber.await_settled()) {
        Err(CordisError::PluginFailed(message)) => {
            assert!(message.contains("model"), "message: {message}")
        }
        other => panic!("expected PluginFailed, got {other:?}"),
    }
}

#[test]
fn await_settled_waits_for_async_effect_cleanup() {
    struct AsyncCleanupPlugin;

    impl Plugin for AsyncCleanupPlugin {
        fn name(&self) -> &'static str {
            "async-cleanup"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            ctx.effect_async_labeled("async-effect", move || vec![cleanup_async(|| async {})])?;
            Ok(())
        }
    }

    let root = Context::root();
    let fiber = root
        .plugin(Arc::new(AsyncCleanupPlugin), Value::Null)
        .expect("fiber");
    assert_eq!(fiber.get_effects()[0].label, "async-effect");
    fiber.dispose().expect("dispose");
    block_on(fiber.await_settled()).expect("settled");
    assert!(fiber.get_effects().is_empty());
    assert_eq!(fiber.state(), FiberState::Disposed);
}
