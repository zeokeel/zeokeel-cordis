//! Subprocess probe for register item 10: a panicking `BlockingExecutor`.
//!
//! A double panic during unwinding aborts the whole process, so this must run
//! in a child process: the parent test asserts that the child survived and that
//! `dispose()` still finalized the fiber after the executor panic.
//!
//! Run as: `redteam_probe blocking-executor-panic`

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use cordis::{cleanup_async, BlockingExecutor, BoxFuture, Context, FiberState, Plugin};
use serde_json::Value;

struct PanicExecutor;

impl BlockingExecutor for PanicExecutor {
    fn block_on(&self, _future: BoxFuture<'static, ()>) {
        panic!("embedder blocking executor rejects synchronous disposal on this thread");
    }
}

struct AsyncEffectPlugin;

impl Plugin for AsyncEffectPlugin {
    fn name(&self) -> &'static str {
        "rt20-async-effect"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        // A live async effect makes `dispose()` go through
        // `AsyncDisposer::dispose_blocking` -> the embedder executor.
        ctx.effect_async(|| vec![cleanup_async(|| async {})])?;
        Ok(())
    }
}

fn blocking_executor_panic() {
    // First install wins; this is a fresh process, so it is ours.
    assert!(
        cordis::set_blocking_executor(Arc::new(PanicExecutor)),
        "fresh process must accept the executor"
    );

    let root = Context::root();
    let fiber = root
        .plugin(Arc::new(AsyncEffectPlugin), Value::Null)
        .expect("plugin activation");
    let dispose = catch_unwind(AssertUnwindSafe(|| {
        let _ = fiber.dispose();
    }));
    let state = fiber.state();
    let registry = root.registry().size();

    println!(
        "dispose_escaped_panic={} state={:?} registry={}",
        dispose.is_err(),
        state,
        registry
    );
    if state == FiberState::Disposed && registry == 0 {
        println!("VERDICT=OK");
    } else {
        println!("VERDICT=BAD");
    }
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("blocking-executor-panic") => blocking_executor_panic(),
        other => {
            eprintln!("unknown mode: {other:?}");
            std::process::exit(2);
        }
    }
}
