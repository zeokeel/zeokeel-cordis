//! 最小可运行示例：加载一个同步插件，注册清理逻辑，然后卸载。
//!
//! cargo run --example hello

use std::sync::Arc;

use cordis::{cleanup, Context, Plugin, Result};
use serde_json::Value;

struct Greeter;

impl Plugin for Greeter {
    fn name(&self) -> &'static str {
        "greeter"
    }

    fn apply(&self, ctx: Context, config: Value) -> Result<()> {
        let who = config
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("world")
            .to_string();
        println!("hello, {who}");
        // 清理逻辑归 fiber 所有：fiber 卸载时逆序执行，恰好一次。
        ctx.effect(move || vec![cleanup(move || println!("bye, {who}"))])?;
        Ok(())
    }
}

fn main() -> Result<()> {
    let root = Context::root();
    let fiber = root.plugin(Arc::new(Greeter), serde_json::json!({ "name": "Cordis" }))?;
    assert!(fiber.is_active());
    fiber.dispose()?;
    root.dispose_all()?;
    Ok(())
}
