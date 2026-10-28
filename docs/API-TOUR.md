# Cordis API tour (Rust)

Pure-Rust implementation of the Cordis paradigm. See the [repository README](../README.md), [`spec/CORDIS-CORE.md`](../spec/CORDIS-CORE.md)
for the contract and [`CORDIS-RUST.md`](CORDIS-RUST.md) for the
Rust-port notes; this file is a compact API tour.

```rust
use std::sync::Arc;
use cordis::{cleanup, Context, Plugin, Result, ServiceKey};

struct FsService; // embedder-provided implementation

static FS: ServiceKey<FsService> = ServiceKey::new("fs");

struct FsPlugin;

impl Plugin for FsPlugin {
    fn name(&self) -> &'static str { "fs" }

    fn apply(&self, ctx: Context, _config: serde_json::Value) -> Result<()> {
        // Provide a typed service; the providing fiber owns it. The key is
        // passed by value and registering returns Result<Disposer>.
        ctx.provide(FS, Arc::new(FsService))?;
        // Register cleanup; it runs when the fiber unloads, in reverse order.
        ctx.effect(|| {
            vec![cleanup(|| {
                // listeners, background tasks, nested effects...
            })]
        })?;
        Ok(())
    }
}

fn main() -> Result<()> {
    let root = Context::root();
    let child = root.extend();
    let fiber = child.plugin(Arc::new(FsPlugin), serde_json::Value::Null)?;
    let fs = child.require(FS).expect("fs service is provided by FsPlugin");
    let _ = fs; // use the service...
    fiber.dispose()?; // effects cleaned up, services released
    Ok(())
}
```

Main modules:

| File | Content |
| --- | --- |
| `context.rs` | root/extend/isolate/plugin/effect/on/accessor entry points |
| `service.rs` | typed service store, `ServiceKey`, availability checks, `ServiceGate` |
| `fiber.rs` / `effect.rs` | fiber lifecycle, effect registration and reverse cleanup |
| `registry.rs` | plugin registry map API, runtime dedupe, quiescence (`is_idle`/`when_idle`), in-place `replace`, `PluginKind`/`AsyncPlugin` |
| `events.rs` / `waterfall.rs` | publish/subscribe, once/prepend/global, waterfall chain (listener panics contained on both lanes; waterfall panics abort and re-raise) |
| `internal.rs` | typed `internal/*` hooks |
| `config.rs` / `logger.rs` / `reflect.rs` | config schema, logger service, accessors/meta |

Async plugins implement `AsyncPlugin` and are activated through
`ctx.plugin_async(...)`; they must use the `*_async` lifecycle APIs
(`restart_async`/`update_async`/`refresh_async`), and the sync APIs return
`CordisError::AsyncPluginNeedsAsync` for them. Trait-object services use
`ServiceKey<dyn Trait>` together with `provide_any`/`get_any`/`require_any`/`set_any`.

The v0.3 API adds raw-config retention (`Fiber::raw_config`), revertible
`ctx.set` (`set_revertible`), lifecycle exclusivity (`LifecycleBusy`),
cancel-safe disposal, child-fiber ownership, `Registry`/`Fiber` in-place replace,
tree-wide quiescence (`Registry::is_idle`/`when_idle`, `Context::when_idle`) and
cooperative cancellation (`Fiber::cancel`/`is_cancelled`, `Context::checkpoint`).

`cargo test` at `v0.3.0` runs 311 tests (lib unit + 266
integration + 7 doctests); `spec/CORDIS-CORE.md` §4 maps every case ID to the
tests that assert it. Embedders should read the root README's
"Known limitations (v0.3.0)" before relying on lifecycle or teardown ordering.
