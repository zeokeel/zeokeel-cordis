//! CORE-033: synchronous disposal honors the embedder-provided blocking
//! executor (the `BlockingExecutor` extension, not part of the JS reference).
//!
//! This lives in its own integration-test binary because the executor is a
//! process-wide installation.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::Value;

use cordis::*;

struct CountingExecutor {
    runs: Arc<AtomicUsize>,
}

impl BlockingExecutor for CountingExecutor {
    fn block_on(&self, future: BoxFuture<'static, ()>) {
        self.runs.fetch_add(1, Ordering::SeqCst);
        block_on(future);
    }
}

struct AsyncCleanupPlugin;

impl Plugin for AsyncCleanupPlugin {
    fn name(&self) -> &'static str {
        "async-cleanup"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.effect_async(|| vec![cleanup_async(|| async {})])?;
        Ok(())
    }
}

#[test]
fn sync_disposal_uses_embedder_blocking_executor() {
    let runs = Arc::new(AtomicUsize::new(0));
    assert!(
        set_blocking_executor(Arc::new(CountingExecutor { runs: runs.clone() })),
        "fresh process installs the executor exactly once"
    );

    let root = Context::root();
    let fiber = root
        .plugin(Arc::new(AsyncCleanupPlugin), Value::Null)
        .unwrap();
    fiber.dispose().expect("sync dispose");
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}
