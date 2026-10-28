# Cordis-Rust port notes

Companion to `spec/CORDIS-CORE.md`. This file records how the Rust port relates to
the JavaScript Cordis reference, what the Rust-only extensions add, which failures
the kernel contains, and what the crate deliberately does not support. It is
descriptive; the specification stays normative. Baseline: crate version `0.4.0`
(tag `v0.4.0`).

## 1. Deliberate differences from the TypeScript reference

The semantics of the Rust port follow `spec/CORDIS-CORE.md`; these differences are
language-driven and permanent unless the spec says otherwise:

- **Typed, explicit services.** A service is registered and read through a
  `ServiceKey<T>` passed by value (`ctx.provide(KEY, value)`, `ctx.get(KEY)`); there
  is no `ctx.foo` property proxy and no runtime property interception.
- **No callable services, `Service` subclassing or `ctx.mixin`.** Rust uses plain
  values plus typed traits, and `.extend()` / `.isolate()` to derive context
  scopes. Service accessors are the explicit `ctx.accessor` /
  `ctx.accessor_with_setter` API.
- **Typed internal hooks.** `internal/*` is a set of typed hook methods, not
  string-keyed metadata; names and ordering match the JS reference. The only
  lifecycle broadcast today is `internal/plugin` on **create** — there is no
  dispose-time broadcast yet.
- **Logger severity is numeric order** (`Error=0 … Debug=3`), the same ordering used
  for thresholds; the facade exposes `error`/`warn`/`info`/`debug`.
- **Two listener lanes.** A Rust closure has one fixed return type, so sync
  listeners (`on`/`on_with`/`once`) are dispatched by `emit`/`bail`/`waterfall`
  and async listeners (`on_async`/`on_async_with`) by `parallel_async` /
  `serial_async` / `waterfall_async`. Sync dispatch never polls async listeners.
  Listeners return `EventResult` instead of raising exceptions, and events carry
  `serde_json::Value` payloads — there is no typed event map and no
  `AggregateError` channel.
- **`inject` is a name list** (`&'static [&'static str]`), not a name → intercept
  config map.
- **Event scope filtering is isolate-map equality**, not a subtree walk: a listener
  receives an event only when the dispatching context's isolation map equals its
  own, unless the listener is `global`.
- **No dynamic plugin loading / stable ABI** (see §5).

## 2. Rust-only extensions

### 2.1 Added in v0.2.0

- **Async plugins.** `AsyncPlugin` mirrors `Plugin` metadata, but activation is a
  host-awaited future (`apply_async`). Entry points: `ctx.plugin_async`,
  `ctx.plugin_async_typed`, `ctx.refresh_async`, `Fiber::restart_async`,
  `Fiber::update_async`. The synchronous lifecycle APIs return
  `CordisError::AsyncPluginNeedsAsync` for async fibers instead of blocking.
- **Blocking executor seam.** Sync disposal edges (`Fiber::dispose`,
  `Fiber::restart`, `Fiber::update`, `AsyncDisposer::dispose_blocking`) drive
  deferred async cleanups through the executor installed with
  `exec::set_blocking_executor`. The first installer wins (`OnceLock`; later calls
  return `false`). Without an installation the kernel uses a dependency-free
  thread-park `block_on` that only makes progress when the future wakes itself.
  `effect_future` / `effect_stream` *registration* uses that raw `block_on`
  directly and ignores the installed executor.
- **Unsized services.** `ServiceKey<T: ?Sized>` admits `ServiceKey<dyn Trait>`;
  the unsized lane is `provide_any` / `get_any` / `require_any` / `set_any`, and
  mixing the two lanes for one key returns `None` on the mismatched lane.

### 2.2 Added in v0.3.0 (CORE-040…CORE-050)

- **Raw config, resolved once per activation (CORE-040).** The fiber retains the
  caller-supplied config (`Fiber::raw_config`) and resolves it through
  `internal/config` + schema once per activation; `Fiber::config` is the resolved
  value. Restart/update therefore cannot compound derived values. `update`
  validates before the `internal/update` waterfall; a hook that replaces the
  config becomes the next activation's raw config (validated when that activation
  runs), a pass-through is not stored, and a rejected update keeps the old raw
  config.
- **Provider identity epochs and withdrawal order (CORE-041, CORE-042).** Service
  entries carry a monotonic version (a provider swap bumps it; the dependent
  reactivation is asserted under CORE-041), and unloading a provider marks its
  services withdrawn before draining, so dependents deactivate while the binding is
  still present and removal follows (CORE-042).
- **Revertible, classified `ctx.set` (CORE-041).** A set captures the previous
  value and registers an effect owned by the providing fiber (restored in reverse
  order on unload) and notifies dependents of the version bump.
  `ctx.set_revertible` exposes the inverse's `Disposer`; a hook that vetoes the
  write leaves the store untouched.
- **Async-disposer joining (CORE-043).** A second concurrent async disposal joins
  the in-flight cleanup instead of racing it.
- **Lifecycle exclusivity and "disposed is final" (CORE-044, CORE-045).**
  `dispose`/`restart`/`update` (sync and async) claim a per-fiber slot; another
  thread gets `CordisError::LifecycleBusy`, while a reentrant call on the owning
  thread (a cleanup disposing its own fiber, `update` calling `restart`) is
  allowed. A disposed fiber cannot be restarted or resurrected; a reentrant
  dispose during activation rolls the activation back.
- **Cancel-safe disposal (CORE-046).** Dropping the future returned by
  `dispose_async` / `AsyncDisposer::dispose` mid-await restores the cleanups that
  had not started so the next dispose attempt runs them in the same reverse order.
  At most the single cleanup future that was being awaited is lost (and logged).
- **Child-fiber ownership (CORE-047).** `ctx.plugin` registers the child fiber as
  an effect of the caller's fiber (a weak child list plus an ownership effect), so
  unloading the parent disposes the subtree in reverse order and a failed parent
  activation rolls its children back. A child that is busy while the parent unloads
  flag-disposes itself when its operation finishes, and a cancelled cascade is
  retried by the next dispose. Loading depth is bounded
  (`MAX_PLUGIN_DEPTH = 256`) so recursive loading fails with
  `CordisError::InvalidPlugin` instead of overflowing the stack.
- **In-place replacement (CORE-048).** `Registry::replace` / `replace_async`
  re-bind every fiber of a runtime to a new implementation, keeping fiber
  identity/uid and raw config, then reload through the normal activation pipeline;
  `Fiber::replace_plugin` / `replace_plugin_async` do the same for one fiber. A
  reload rejected as busy rolls the re-binding and runtime move back.
- **Quiescence (CORE-049).** `Registry::is_idle` reports whether any lifecycle
  operation is in flight (no `Loading`/`Unloading` fiber, `pending_ops == 0`), and
  `Registry::when_idle` / `Context::when_idle` resolve once the tree is quiescent.
- **Cooperative cancellation (CORE-050).** `Fiber::cancel` sets a flag and
  `Context::checkpoint` returns `Err(CordisError::Cancelled)` once set, letting
  plugin work unwind through the normal failure/rollback path. The kernel never
  forcibly interrupts plugin code.

## 3. Panic and isolation policy (v0.3.0)

The kernel contains **unwinding panics raised by plugin/embedder code it calls
through one of its own dispatch points**. Each contained failure is logged via
`tracing`; the kernel state machine continues.

| Boundary | Behavior |
| --- | --- |
| Sync `apply` returns `Err` or panics | activation fails, effects/services/listeners rolled back, fiber `Failed`; panic does not escape `ctx.plugin` (CORE-028/029) |
| Async activation returns `Err` or panics | same rollback, async cleanups awaited (CORE-035/036) |
| Sync/async cleanup panics | remaining cleanups and effects still run; the first panic is re-raised after the drain (CORE-030/011). During a failed-activation rollback it is logged instead of escaping |
| Sync event listener panics | contained, treated as `EventResult::Continue`; dispatch continues (CORE-031) |
| Async listener panic (future construction or poll) | contained and logged; dispatch continues |
| Internal hooks (`internal/*`) | contained with the hook's fallback behavior (default config, skipped listener, continue) |
| Logger `LogExporter::level` / `export` | contained per exporter; remaining exporters and the buffer write continue |
| Reflect accessor read | contained; lookup treats the accessor as absent (`None`) |
| `ConfigSchema::validate` / `::merge` | contained; a panic becomes a validation failure / lowest-precedence fallback |
| `AvailabilityCheck` predicate | contained; treated as `false` (service unavailable) |
| Waterfall middleware panic | logs and **aborts the chain, re-raising** (fail-closed; inside `apply` this becomes a failed activation). It never lets the guarded operation proceed |

Not contained, because these are not unwinding panics: `panic = "abort"` builds,
stack overflow, OOM/allocator abort, `process::abort`/`exit`, `unsafe` code with
undefined behavior, deadlocks and infinite loops. Use an external process or WASM
sandbox for untrusted plugins; that layer is embedder guidance, not a crate feature
(see `docs/KERNEL-ISOLATION.md`).

`Disposer`/`AsyncDisposer` also run cleanup from `Drop` when they are detached. A
cleanup panic re-raised from `Drop` aborts the process when it happens during an
unwind, and `AsyncDisposer::drop` can block; prefer explicit `dispose` /
`dispose_async` (never dispose from `Drop` on an async worker thread). Cancelling
an async disposal restores only the cleanups that had not started; the in-flight
one is not recoverable (CORE-046).

## 4. Runtime and lifecycle boundaries

- The crate is async-runtime-agnostic (`#![forbid(unsafe_code)]`; dependencies are
  `serde_json`, `tracing`, `futures-util`, no runtime executor) and its contexts
  are `Send + Sync`.
- Lifecycle calls on one fiber are exclusive, not queued: concurrent calls from
  other threads return `LifecycleBusy` and hosts must retry or serialize
  themselves. There is no preemption, forced thread kill, watchdog or timeout.
- Undisposed fibers keep their context/plugin graph alive (the links stay strong
  until unload). Dispose fibers on shutdown; v0.3.0 reclaims a disposed fiber's
  graph.

## 5. Support boundary

- **Static linking only.** A plugin is an in-process `Arc<dyn Plugin>` /
  `Arc<dyn AsyncPlugin>` value compiled into the host binary. The crate provides no
  ABI/FFI, no cdylib/dylib loading, no module-URL resolution and no
  configuration-file loader, and the Rust ABI is not stable — plugins cannot be
  shipped as shared libraries across compiler or crate versions. The kernel also
  ships no process or WASM host; isolation of untrusted plugins is an embedder
  concern (guidance in `docs/KERNEL-ISOLATION.md`).
- **Plugin identity is a `PluginKey`**, not a module URL: `Named(name)` by default,
  `Type(TypeId)` with `plugin_typed` / `plugin_async_typed`.
- **Out of scope for the crate**: module/hot-reload loaders, timers, agent and tool
  contracts, permissions, sandboxing, UI composition (see the root README "Scope").
- **Kernel guarantees are in-process fault containment only**, not a security
  sandbox; see `docs/KERNEL-ISOLATION.md` for the measured failure modes and the
  suggested external isolation layers.
