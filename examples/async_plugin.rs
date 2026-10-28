//! 异步插件（AsyncPlugin）示例：注册异步清理，用 *_async API 卸载。
//!
//! cargo run --example async_plugin

use std::sync::Arc;

use cordis::{block_on, cleanup_async, AsyncPlugin, BoxFuture, Context, Result};
use serde_json::Value;

struct Ticker;

impl AsyncPlugin for Ticker {
    fn name(&self) -> &'static str {
        "ticker"
    }

    fn apply_async<'a>(&'a self, ctx: Context, _config: Value) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            ctx.effect_async(|| {
                vec![cleanup_async(|| async {
                    // 这里 await 关闭后台任务/连接
                    println!("async cleanup ran");
                })]
            })?;
            println!("async plugin active");
            Ok(())
        })
    }
}

fn main() -> Result<()> {
    let root = Context::root();
    let fiber = block_on(root.plugin_async(Arc::new(Ticker), Value::Null))?;
    assert!(fiber.is_active());

    block_on(fiber.dispose_async())?;
    root.dispose_all()
}
