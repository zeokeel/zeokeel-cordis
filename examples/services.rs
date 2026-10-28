//! 服务提供 + 依赖注入（inject）示例。
//!
//! cargo run --example services

use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;
use zeokeel_cordis::{Context, FiberState, Plugin, Result, ServiceKey};

struct Clock {
    started: Instant,
}

static CLOCK: ServiceKey<Clock> = ServiceKey::new("clock");

struct ClockPlugin;

impl Plugin for ClockPlugin {
    fn name(&self) -> &'static str {
        "clock"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.provide(
            CLOCK,
            Arc::new(Clock {
                started: Instant::now(),
            }),
        )?;
        Ok(())
    }
}

struct UptimePlugin;

impl Plugin for UptimePlugin {
    fn name(&self) -> &'static str {
        "uptime"
    }

    // 依赖服务 "clock"：就绪前 fiber 停在 Pending，就绪后自动激活。
    fn inject(&self) -> &'static [&'static str] {
        &["clock"]
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let clock = ctx.require(CLOCK)?;
        println!("uptime: {:?}", clock.started.elapsed());
        Ok(())
    }
}

fn main() -> Result<()> {
    let root = Context::root();
    let provider = root.plugin(Arc::new(ClockPlugin), Value::Null)?;
    let consumer = root.plugin(Arc::new(UptimePlugin), Value::Null)?;
    assert!(consumer.is_active());

    // 提供者卸载 → 依赖方回到 Pending
    provider.dispose()?;
    assert_eq!(consumer.state(), FiberState::Pending);
    println!("consumer is now pending");

    // 重新提供 → 依赖方自动重新激活
    let _provider = root.plugin(Arc::new(ClockPlugin), Value::Null)?;
    assert!(consumer.is_active());

    root.dispose_all()
}
