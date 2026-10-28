//! 事件监听、bail 与 waterfall 中间件示例。
//!
//! cargo run --example events

use std::sync::Arc;

use cordis::{waterfall_terminal, Context, EventResult, Plugin, Result};
use serde_json::{json, Value};

struct Listeners;

impl Plugin for Listeners {
    fn name(&self) -> &'static str {
        "listeners"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        // 监听器归当前 fiber 所有，fiber 卸载时自动移除。
        ctx.on("demo", |args| {
            println!("sync listener got {}", args[0]);
            EventResult::Continue
        })?;

        // once：只触发一次
        ctx.once("demo", |_args| {
            println!("once listener");
            EventResult::Continue
        })?;

        // 返回 Bail：bail 派发时由它决定结果（emit 会忽略）
        ctx.on("demo", |_args| EventResult::Bail(json!("vetoed")))?;

        // 中间件：在返回途上包装终端结果
        ctx.on_waterfall("request", |_args, next| {
            let value = next.next();
            json!({ "wrapped": value })
        })?;
        Ok(())
    }
}

fn main() -> Result<()> {
    let root = Context::root();
    let _fiber = root.plugin(Arc::new(Listeners), Value::Null)?;

    root.emit("demo", &[json!(1)]);
    root.emit("demo", &[json!(2)]);

    // bail：第一个返回 Bail 的监听器决定结果
    let verdict = root.bail("demo", &[json!(3)]);
    println!("bail verdict: {verdict:?}");

    // waterfall：中间件链 + 调用方提供的终端行为
    let out = root.waterfall(
        "request",
        &[json!("payload")],
        Some(waterfall_terminal(|args| json!({ "base": args[0] }))),
    );
    println!("waterfall: {out}");

    root.dispose_all()
}
