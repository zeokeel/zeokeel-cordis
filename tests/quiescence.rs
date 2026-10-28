//! Quiescence (CORE-049) and cooperative cancellation checkpoints (CORE-050).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;
use zeokeel_cordis::{
    block_on, cleanup_async, yield_now, Context, CordisError, FiberState, Plugin,
};

struct Slow {
    delay: Duration,
}

impl Plugin for Slow {
    fn name(&self) -> &'static str {
        "quiescence-slow"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        std::thread::sleep(self.delay);
        Ok(())
    }
}

#[test]
fn fresh_registry_is_idle() {
    let root = Context::root();
    assert!(root.registry().is_idle());
    block_on(root.when_idle());
}

#[test]
fn when_idle_waits_for_a_slow_activation() {
    let root = Context::root();
    let ctx = root.clone();
    let handle = std::thread::spawn(move || {
        ctx.plugin(
            Arc::new(Slow {
                delay: Duration::from_millis(150),
            }),
            Value::Null,
        )
        .expect("plugin")
    });
    // Wait until the activation is observable as in-flight.
    while root.registry().is_idle() {
        std::thread::sleep(Duration::from_millis(1));
    }
    let started = Instant::now();
    block_on(root.when_idle());
    assert!(
        started.elapsed() >= Duration::from_millis(50),
        "when_idle returned before the activation finished"
    );
    let fiber = handle.join().expect("activation thread");
    assert_eq!(fiber.state(), FiberState::Active);
}

struct AsyncCleanupPlugin {
    runs: Arc<AtomicUsize>,
}

impl Plugin for AsyncCleanupPlugin {
    fn name(&self) -> &'static str {
        "quiescence-async-cleanup"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        let runs = self.runs.clone();
        ctx.effect_async(move || {
            vec![cleanup_async(move || {
                let runs = runs.clone();
                async move {
                    yield_now().await;
                    runs.fetch_add(1, Ordering::SeqCst);
                }
            })]
        })?;
        Ok(())
    }
}

#[test]
fn when_idle_waits_for_an_async_disposal() {
    let root = Context::root();
    let runs = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(
            Arc::new(AsyncCleanupPlugin { runs: runs.clone() }),
            Value::Null,
        )
        .expect("plugin");
    let disposer = fiber.clone();
    let handle = std::thread::spawn(move || disposer.dispose());
    block_on(root.when_idle());
    handle.join().expect("dispose thread").expect("dispose");
    assert_eq!(fiber.state(), FiberState::Disposed);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert!(root.registry().is_idle());
}

struct CheckpointPlugin {
    captured: Arc<Mutex<Option<Context>>>,
}

impl Plugin for CheckpointPlugin {
    fn name(&self) -> &'static str {
        "quiescence-checkpoint"
    }

    fn apply(&self, ctx: Context, _config: Value) -> zeokeel_cordis::Result<()> {
        *self.captured.lock().unwrap() = Some(ctx);
        Ok(())
    }
}

#[test]
fn checkpoint_observes_cooperative_cancellation() {
    let root = Context::root();
    let captured = Arc::new(Mutex::new(None));
    let fiber = root
        .plugin(
            Arc::new(CheckpointPlugin {
                captured: captured.clone(),
            }),
            Value::Null,
        )
        .expect("plugin");
    let plugin_ctx = captured.lock().unwrap().clone().expect("plugin context");
    assert!(plugin_ctx.checkpoint().is_ok());
    assert!(!fiber.is_cancelled());
    fiber.cancel();
    assert!(fiber.is_cancelled());
    assert!(matches!(
        plugin_ctx.checkpoint(),
        Err(CordisError::Cancelled)
    ));
    // The fiber itself is unaffected until the plugin acts on the checkpoint.
    assert_eq!(fiber.state(), FiberState::Active);
}
