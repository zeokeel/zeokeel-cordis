# Cordis

A small, framework-agnostic **pluggable kernel** written in pure Rust, implementing
the Cordis paradigm: contexts own scoped service stores, plugins run on fibers,
effects clean up in reverse order, dependency injection is declarative, and events
cross plugin boundaries. It is deliberately **only the core** — no loader, no
timer, no UI framework, no product contracts.

Any embedder (desktop host, server, agent runtime, test harness) depends on this
crate and plugs its own plugins in: a plugin is a value implementing `Plugin`
(sync) or `AsyncPlugin` (async), and everything it registers (services, listeners,
effects) is released when its fiber unloads; child fibers loaded with `ctx.plugin`
are owned effects of the loading fiber.

## Scope

| Area | Module |
| --- | --- |
| Context / scope isolation (`extend`, `isolate`, `isolate_labeled`) | `src/context.rs` |
| Service provide/get/set, availability checks, root-level store | `service.rs` |
| Fiber lifecycle, effect registration/cleanup, diagnostics | `fiber.rs`, `effect.rs` |
| Plugin loading, `inject` dependency gating, registry map API | `registry.rs` |
| Events, waterfall, typed internal hooks | `events.rs`, `internal.rs`, `waterfall.rs` |
| Logger service / config schema / reflect accessors | `logger.rs`, `config.rs`, `reflect.rs` |

Deliberately out of scope: module loader, timers, agent/tool contracts, permissions,
sandboxing, UI slots, dynamic plugin loading, and process/WASM isolation hosts (see
[`docs/CORDIS-RUST.md`](docs/CORDIS-RUST.md); isolation layers are embedder guidance
in [`docs/KERNEL-ISOLATION.md`](docs/KERNEL-ISOLATION.md)). Those belong to the
consuming application.

## Usage

```toml
[dependencies]
cordis = { git = "https://github.com/zeokeel/Cordis", tag = "v0.3.0" }
serde_json = "1"   # plugin config values
```

```rust
use std::sync::Arc;
use cordis::{cleanup, Context, Plugin, Result};

struct MyPlugin;

impl Plugin for MyPlugin {
    fn name(&self) -> &'static str { "my-plugin" }

    fn apply(&self, ctx: Context, _config: serde_json::Value) -> Result<()> {
        // Everything registered here is owned by the fiber: effects are cleaned
        // up in reverse order when the fiber unloads.
        ctx.effect(|| {
            vec![cleanup(|| {
                // release a listener, a file handle, a background task...
            })]
        })?;
        Ok(())
    }
}

fn main() -> Result<()> {
    let root = Context::root();
    let ctx = root.extend();
    let fiber = ctx.plugin(Arc::new(MyPlugin), serde_json::Value::Null)?;
    fiber.dispose()?;
    Ok(())
}
```

Async plugins implement `AsyncPlugin` and are activated with `ctx.plugin_async`;
their lifecycle APIs are the `*_async` variants (`restart_async`, `update_async`,
`refresh_async`). Trait-object services (`ServiceKey<dyn Trait>`) use the
`provide_any` / `get_any` / `require_any` / `set_any` methods.

See [`docs/API-TOUR.md`](docs/API-TOUR.md) for an API tour,
[`spec/CORDIS-CORE.md`](spec/CORDIS-CORE.md) for the normative semantics and
numbered conformance cases, and [`docs/CORDIS-RUST.md`](docs/CORDIS-RUST.md) for
the Rust-port notes (deliberate differences from the TypeScript reference,
Rust-only extensions, panic/isolation policy and support boundary).

## Contract and conformance

- `spec/CORDIS-CORE.md` — normative semantics, lifecycle states, API surface and
  numbered cases (`CORE-001`…`CORE-051`). §4 maps every case to the integration
  test(s) that assert it.
- `cargo test` — 311 tests at v0.3.0 (lib unit + conformance/containment/
  red-team integration suites + doctests).
- `cargo fmt --check && cargo clippy --all-targets -- -D warnings`

A behavior change is a breaking change: update `spec/` and bump the version.

## Status

`v0.3.x` kernel-hardening line: fault containment, semantic parity and
provider-identity epochs on top of the v0.2 async-host work. Extracted from
It is an independent pure-Rust implementation written against the Cordis
paradigm; see
`spec/CORDIS-CORE.md` §5 for the deliberate differences from the JavaScript
reference and §2.8 for the Rust-only embedder extensions added in v0.2.0
(asynchronous plugins and the blocking-disposal executor).

v0.3.0 (CORE-040…CORE-050) adds: raw-config retention with one-shot resolution,
provider-identity epochs and provider-withdrawal ordering, revertible/classified
`ctx.set`, async-disposer joining, lifecycle exclusivity (`LifecycleBusy`),
cancel-safe disposal, child-fiber ownership, in-place plugin replacement,
tree-wide quiescence (`Registry::is_idle`/`when_idle`) and cooperative
cancellation (`Fiber::cancel` + `ctx.checkpoint`).

v0.2.0 (CORE-028…CORE-039) adds: failure rollback + panic isolation on activation,
cleanup/listener panic isolation, `ServiceKey<?Sized>` with the `*_any` service
APIs, async plugins (`AsyncPlugin`/`plugin_async`/`refresh_async`), the
`BlockingExecutor` hook for sync disposal, and removes the root Context↔FiberData
reference cycle.

### Known limitations (v0.3.0)

Measured/documented so embedders do not rely on guarantees the kernel does not
make. `docs/KERNEL-ISOLATION.md` describes the failure-mode evidence behind the
panic/isolation items.

- **Undisposed fibers keep their graph alive.** Disposing a fiber now reclaims it
  completely (0 leaked / 10 000 boot+dispose+drop cycles measured), but a fiber
  that is never disposed still pins its context/plugin graph (about 9 KB/boot in
  our 10 000-boot drop probe; workload-dependent) because the fiber↔context links
  stay strong until unload. Dispose everything on shutdown.
- **Synchronous disposal on an async thread needs the embedder executor.**
  `Fiber::dispose()`/`restart`/`update` are blocking edges. The default executor
  parks the calling thread and can deadlock on a current-thread runtime; install
  `exec::set_blocking_executor` (first installer wins) or use the `*_async` APIs.
  `effect_future`/`effect_stream` registration still uses the raw thread-park
  `block_on`, so register them from a non-async context or prefer
  `effect_future_async`.
- **Lifecycle calls are exclusive, not queued.** `dispose`/`restart`/`update`
  claim a per-fiber slot; a concurrent call from another thread returns
  `CordisError::LifecycleBusy` (reentrant calls on the owning thread are
  allowed), so hosts must retry or serialize themselves. Activation racing a
  dispose is fenced and rolls back; a disposed fiber cannot be restarted.
- **`update` validates before the `internal/update` waterfall.** The payload is
  resolved and schema-validated first; a hook that replaces it sets the next
  activation's raw config, which is resolved and validated once on that activation.
  An invalid replacement therefore fails the activation after `update` has already
  returned `Ok`; hook authors should return schema-valid config.
- **`ctx.effect` has an ownership window.** The body runs before the effect is
  attached to the fiber: the stack is restored on panic (v0.3), but registrations
  made inside the body are only owned by the fiber, not by the effect being
  created; a `dispose` issued from inside the body runs the cleanup once and then
  rejects the registration.
- **Cancelling a disposal loses only the in-flight cleanup.** Dropping the future
  returned by `dispose_async()` restores the cleanups that had not started (a
  later dispose runs them in reverse order); the one cleanup future that was
  being awaited cannot be recovered and is logged.
- **Panic containment is bounded.** Contained: sync/async `apply`, cleanup,
  listener, internal-hook, exporter, accessor and schema panics (each logged and
  isolated). A panicking waterfall middleware aborts the chain and re-raises
  (fail-closed; inside `apply` this becomes a failed activation). Not contained,
  because they are not unwinding panics: `panic="abort"` builds, stack overflow,
  OOM/allocator abort, `process::abort/exit`, `unsafe` UB, deadlocks and infinite
  loops. Use process or WASM isolation for untrusted plugins (see
  `docs/KERNEL-ISOLATION.md`).
- **No dynamic plugin loading / stable ABI.** Plugins are statically linked Rust
  trait objects (`Arc<dyn Plugin>`); there is no cdylib ABI, so "drop in a new
  plugin binary" requires process isolation or WASM.
- **Event scope filtering is isolation-map equality**, not "own subtree": a
  listener passes when the dispatch and registration isolate maps are equal (or
  `global`). JSON payloads are `serde_json::Value`; there is no typed event map.
- **No preemption, watchdog or resource quotas.** Quiescence (`Registry::is_idle` /
  `when_idle`) and cancellation (`Fiber::cancel` + `ctx.checkpoint`) are cooperative:
  the kernel never interrupts, force-kills or times out plugin work. There is no
  heartbeat/deadline supervisor and no per-fiber thread/fd/effect quota.
