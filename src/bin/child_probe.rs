//! Out-of-process probes for child-fiber ownership edge cases that can abort
//! the host process (recursive cross-loading cycle, very deep nesting).
//!
//! Modes:
//!   cycle                  A loads B, B loads A (recursion-cycle containment)
//!   depth `<N>`            parent→child chain N levels deep, then dispose root
//!   wide `<N>`             parent loads N children, then dispose parent

use std::sync::Arc;

use cordis::{Context, FiberState, Plugin};
use serde_json::Value;

struct Chain {
    remaining: usize,
}

impl Plugin for Chain {
    fn name(&self) -> &'static str {
        "probe-chain"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        if self.remaining > 0 {
            ctx.plugin(
                Arc::new(Chain {
                    remaining: self.remaining - 1,
                }),
                Value::Null,
            )?;
        }
        Ok(())
    }
}

struct CycleA;
struct CycleB;

impl Plugin for CycleA {
    fn name(&self) -> &'static str {
        "probe-cycle-a"
    }
    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        ctx.plugin(Arc::new(CycleB), Value::Null)?;
        Ok(())
    }
}

impl Plugin for CycleB {
    fn name(&self) -> &'static str {
        "probe-cycle-b"
    }
    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        ctx.plugin(Arc::new(CycleA), Value::Null)?;
        Ok(())
    }
}

struct FanOut {
    children: usize,
}

impl Plugin for FanOut {
    fn name(&self) -> &'static str {
        "probe-fanout"
    }
    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        for _ in 0..self.children {
            ctx.plugin(Arc::new(Chain { remaining: 0 }), Value::Null)?;
        }
        Ok(())
    }
}

fn run_chain(depth: usize) {
    let root = Context::root();
    let top = match root.plugin(Arc::new(Chain { remaining: depth }), Value::Null) {
        Ok(fiber) => fiber,
        Err(error) => {
            println!("DEPTH={depth} CREATE_ERR={error:?}");
            return;
        }
    };
    let before = root.registry().fibers().len();
    let dispose = top.dispose();
    let disposed = root
        .registry()
        .fibers()
        .iter()
        .filter(|fiber| fiber.state() == FiberState::Disposed)
        .count();
    let active = root
        .registry()
        .fibers()
        .iter()
        .filter(|fiber| fiber.state() == FiberState::Active)
        .count();
    println!(
        "DEPTH={depth} RESULT=OK dispose={:?} fibers_before={before} disposed={disposed} active={active} registry={}",
        dispose.map_err(|e| e.to_string()),
        root.registry().size()
    );
}

fn run_cycle() {
    let root = Context::root();
    let result = root.plugin(Arc::new(CycleA), Value::Null);
    println!(
        "CYCLE_RESULT={:?}",
        result.map(|fiber| fiber.state()).map_err(|e| e.to_string())
    );
    println!("CYCLE_SURVIVED registry={}", root.registry().size());
}

fn run_wide(children: usize) {
    let root = Context::root();
    let parent = match root.plugin(Arc::new(FanOut { children }), Value::Null) {
        Ok(fiber) => fiber,
        Err(error) => {
            println!("WIDE={children} CREATE_ERR={error:?}");
            return;
        }
    };
    let before = root.registry().fibers().len();
    let dispose = parent.dispose();
    println!(
        "WIDE={children} RESULT=OK dispose={:?} fibers_before={before} registry={}",
        dispose.map_err(|e| e.to_string()),
        root.registry().size()
    );
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    match mode.as_str() {
        "cycle" => run_cycle(),
        "depth" => {
            let depth: usize = std::env::args()
                .nth(2)
                .and_then(|value| value.parse().ok())
                .unwrap_or(10);
            run_chain(depth);
        }
        "wide" => {
            let children: usize = std::env::args()
                .nth(2)
                .and_then(|value| value.parse().ok())
                .unwrap_or(10);
            run_wide(children);
        }
        other => {
            eprintln!("unknown mode: {other:?}");
            std::process::exit(2);
        }
    }
}
