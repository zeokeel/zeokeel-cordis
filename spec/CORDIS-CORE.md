# Cordis Core — normative specification

Status: **v0.3.0**. The Rust crate (repository root) is the reference implementation of
this document. A behavior that is not written here is not part of the contract.

Editorial revision: the prose and the API surface below were corrected against the
`v0.3.0` crate; no case ID, ordering guarantee or normative behavior changed.

## 1. Scope

The core owns exactly five things:

1. **contexts** — scoped dependency containers with parent/child relations,
2. **services** — typed values registered into an isolation scope, with optional
   availability checks,
3. **fibers** — one lifecycle unit per loaded plugin, owning effects and services,
4. **effects** — registrations whose cleanup is owned by a fiber (listeners, derived
   services, background tasks, and child fibers created through `ctx.plugin`,
   CORE-047),
5. **events** — synchronous publish/subscribe and waterfall hooks crossing plugin
   boundaries.

A sixth, explicitly Rust-only extension is part of the contract since v0.2.0:
**asynchronous plugins** (activation is a host-awaited future, §2.8) and the
**blocking executor** used by synchronous disposal (§2.8). Everything else below
is core semantics.

The core does *not* own: module loading, timers, configuration file formats, agent
or tool contracts, permissions, sandboxing, UI composition. These are embedder
concerns built on top of the core.

## 2. Core model

### 2.1 Context

- `Context::root()` creates the root context and its sole `root` fiber (active,
  no `apply` body).
- `ctx.extend()` returns a child context sharing parent state and scopes.
- `ctx.isolate(name)` returns a child context with a **new scope** for one service
  name. Reads/writes of `name` below the child resolve in that scope; all other
  services keep resolving in their inherited scopes.
- `ctx.isolate_labeled(name, label)` joins a previously created named scope
  (label ids are FNV-1a hashes of the label; identical labels share a scope).
- Service resolution walks the child chain outward and stops at the first scope
  that defines the name. There is **no fallback to an ancestor scope** when a
  closer scope defines the name but its availability check is false (CORE-014).

### 2.2 Services

- `ctx.provide(key, value)` registers a typed service under a `ServiceKey<T>`,
  passed **by value**, and returns `Result<Disposer>`. The provider is the fiber of
  the context used for the call; the disposer unregisters immediately and
  unloading the providing fiber unregisters too (CORE-010).
  - `ctx.provide_checked(key, value, check)` attaches an `AvailabilityCheck`
    predicate. Dependents only activate while it returns `true`; re-evaluate them
    with `ctx.refresh()` after its backing state changes (CORE-013).
  - `ctx.provide_gated(key, value, &gate)` is `provide_checked` plus automatic
    dependent refresh when the `ServiceGate` flips (CORE-006).
- Registering a name twice in the same scope fails with
  `CordisError::DuplicateService` (CORE-004).
- Typed reads: `ctx.get(key)` returns `Option<Arc<T>>` (`None` when unprovided or
  unavailable); `ctx.require(key)` returns `Err(CordisError::UnknownService)` in
  the same cases. Both consult accessors first (CORE-025). The untyped lane is
  `ctx.get_named(name)`.
- A provider's `check` predicate is evaluated **outside** the service-store lock;
  it may re-enter the context (CORE-015). A closed predicate in a closer scope
  shadows an ancestor provider instead of falling back to it (CORE-014).
- `ctx.set(name, value)` overwrites a provided value; only the providing fiber may
  set it (`CordisError::ServiceWriteForbidden` otherwise), and computed accessors
  reject writes with `CordisError::AccessorReadOnly`. Since v0.3.0 a `set` is a
  **revertible, classified mutation** (CORE-041): the previous entry is captured
  and restored by an effect owned by the providing fiber (reverse order on
  unload), and the entry version bump re-evaluates every dependent even while
  the service stays available. `ctx.set_revertible` exposes the inverse's
  disposer directly; a hook that vetoes the write leaves the store untouched.
- **Unsized service keys (v0.2.0)**: `ServiceKey<T>` accepts `T: ?Sized`, so
  `ServiceKey<dyn Trait>` is a legal static. Its value is registered and read
  through `provide_any` / `get_any` / `require_any` / `set_any`, because
  `Arc<dyn Trait>` cannot be unsize-coerced into the type-erased store; the store
  keeps a sized wrapper and recovers `Arc<T>` by type id (CORE-032). The sized
  `provide`/`get`/`require`/`set` lane is unchanged.

### 2.3 Fiber lifecycle

```
plugin() ──► Loading ──(apply ok)──► Active ──(dep missing)──► Pending ──(dep returns)──► Active
                 │                        │
                 └──(apply err)──► Failed  └──(dispose)──► Unloading ──► Disposed
```

- A plugin implements [`crate::Plugin`] (sync) or [`crate::AsyncPlugin`] (async,
  §2.8); `ctx.plugin`/`ctx.plugin_async` create fibers.
- `inject` names gate activation: while any is missing the fiber is `Pending`;
  when all resolve it activates; when one disappears it unloads back to `Pending`
  (CORE-005, CORE-006, CORE-016).
- **Failure semantics (v0.2.0)**: when `apply` returns `Err` **or panics**, the
  activation is failed: every effect registered during the failed activation is
  rolled back (sync cleanups immediately and async cleanups through the blocking executor
  (each panic isolated), so reverse registration order holds across lanes and
  an undisposed Failed fiber cannot leak its cleanups, CORE-028/CORE-038), services/listeners are removed, and only this fiber
  transitions to `Failed`. The panic never escapes `ctx.plugin` (CORE-029).
- `fiber.dispose()` (or `fiber.dispose_async()` for async cleanup) runs registered
  effects **in reverse registration order** and releases owned services and
  listeners (CORE-007, CORE-010, CORE-017).
- **Config re-resolution (v0.3.0)**: the fiber keeps the caller-supplied raw
  config and resolves it (internal/config waterfall + schema) once **per
  activation**; restart/update therefore never compound derived values
  (CORE-040). `Fiber::config` returns the resolved value, `Fiber::raw_config` the
  input. `update` validates *before* the `internal/update` waterfall; an update
  hook that returns a different config replaces the raw config (resolved once on
  the following activation), a pass-through does not (no compounding). A
  rejected update leaves the retained raw config untouched.
- **Provider-withdrawal order (v0.3.0)**: unloading a provider marks its services
  withdrawn *before* draining, so dependents deactivate while the binding is
  still present and only then is it removed (CORE-042).
- **Disposed is final (v0.3.0)**: restart/update/activate on a disposed fiber
  returns `CordisError::Disposed`, and a reentrant dispose during activation
  rolls the activation back instead of resurrecting the fiber (CORE-044).
- **Child-fiber ownership (v0.3.0)**: `ctx.plugin` registers the new fiber as an
  effect of the caller's fiber, so unloading the parent disposes the whole
  subtree; a failed parent activation rolls its children back too. The ownership
  effect holds a `Weak` fiber handle (no parent↔child cycle) and is disposed in
  reverse registration order like any other effect (CORE-047). A child whose
  lifecycle is busy while the parent unloads is flagged and disposes itself when
  its current operation finishes; a cancelled cascade is retried by the next
  `dispose` (the parent keeps a weak child list for this).
  **Loading depth is bounded** (`MAX_PLUGIN_DEPTH = 256`): recursive loading
  (A→B→A or a plugin that keeps loading itself) fails with
  `CordisError::InvalidPlugin` instead of overflowing the stack.
- **Lifecycle operations are exclusive (v0.3.0)**: `dispose`/`restart`/`update`
  (sync and async) claim a per-fiber lifecycle slot. A concurrent call from
  another thread returns `CordisError::LifecycleBusy` instead of racing; a
  reentrant call on the owning thread (a cleanup disposing its own fiber,
  `update` calling `restart`) is allowed (CORE-045).
- **Cancel-safe disposal (v0.3.0)**: if the future returned by
  `dispose_async`/`AsyncDisposer::dispose` is dropped mid-await, the cleanups
  that had not started are restored and run by the next dispose attempt in the
  same reverse order; the single in-flight cleanup future is lost and logged
  (CORE-046).
- `fiber.await_settled()` resolves once the fiber is stable (Active/Failed).
- Diagnostics: `fiber.uid`, `fiber.parent`, `fiber.get_effects()` returning the
  `EffectMeta` tree (CORE-012).

### 2.4 Effects

- `ctx.effect(body)` runs `body` immediately and owns its result. The body is
  `FnOnce() -> Effect` with `Effect = Vec<Cleanup>`: an empty vector means nothing
  to clean up, and `cordis::cleanup(closure)` wraps one sync cleanup. The call
  returns `Result<Disposer>`; the disposer runs the cleanups in reverse order and
  is single-shot.
- `ctx.effect_labeled(label, body)` is the same with an explicit diagnostics label.
  The async forms return `Result<AsyncDisposer>` over `AsyncEffect =
  Vec<AsyncCleanup>` (`cleanup_async(fut)` wraps one future): `effect_async`,
  `effect_async_labeled`, `effect_future`, `effect_future_async` and
  `effect_stream`. Async cleanups are awaited in reverse order during unload; the
  fiber settles only after they finish (CORE-027), and a concurrent async disposal
  joins the in-flight cleanup instead of racing it (CORE-043).
- Registering an effect on an `Unloading`/`Disposed` fiber fails with
  `CordisError::InactiveEffect`.
- An effect is disposed exactly once (CORE-008); disposing a fiber disposes its
  effects exactly once even if the disposer is also called (CORE-009).
- **Cleanup panic isolation (v0.2.0)**: a panicking cleanup does not skip the
  remaining cleanups of that effect, nor the remaining effects of the fiber. The
  fiber still drains fully; the **first** captured panic is re-raised after the
  drain, so callers of `dispose`/`dispose_async` can observe it (CORE-030).
- An effect *body* panic that happens inside `apply` fails that activation and is
  rolled back (CORE-029); a body panic outside activation is the embedder's
  problem. Panic containment for the remaining boundaries is documented in
  `docs/CORDIS-RUST.md`.

### 2.5 Events

- `ctx.on(event, listener)` registers a listener owned by the current fiber and
  returns `Result<Disposer>`; it is removed with the fiber (CORE-017). Sync
  listeners return `EventResult` (`Continue`, or `Bail(value)`).
- `ctx.on_with_options(event, options, listener)` takes
  `EventOptions { prepend, global }`; `ctx.on_with(event, prepend, global,
  listener)` is the positional form; `ctx.once` removes the listener after its
  first call (CORE-018).
- Dispatch modes are part of the contract:
  - sync lane — `ctx.emit` (registration order, return value ignored), `ctx.bail`
    (registration order until the first non-null, non-`false` `Bail`), and
    `ctx.waterfall` (middleware around an explicit `next`);
  - async lane — `ctx.parallel_async` (await every listener concurrently),
    `ctx.serial_async` (await in registration order until one bails), and
    `ctx.waterfall_async`.
  Listeners registered with `ctx.on_async` are dispatched by the async lane only;
  sync dispatch never polls them.
- Scope filters: a listener receives an event only when the dispatching context's
  isolation map equals the listener's own, unless `global` is set (CORE-019).
- Waterfall hooks (`waterfall.rs`) pass a mutable value through listeners; the
  first listener returning a non-null, non-`false` `Bail` stops the chain
  (CORE-020).

### 2.6 Registry

- `ctx.plugin(Arc<dyn Plugin>, config)` registers a fiber under a `PluginKey`:
  `Named(name)` by default, `Type(TypeId)` with `plugin_typed`. `plugin_async` /
  `plugin_async_typed` are the `AsyncPlugin` counterparts (CORE-035).
- `Registry::size/get/has/delete/keys/values/entries/for_each/fibers/counter`
  expose the live plugin map (CORE-022).
- **Shutdown helper (v0.3.0)**: `Context::dispose_all()` /
  `Registry::dispose_all()` dispose every registered fiber and the root fiber,
  collecting the first cleanup error after attempting all of them; after it
  returns the registry is empty and `is_idle()` is true. This is the supported
  shutdown path (dispose+drop is the only path that reclaims the graph; a
  never-disposed fiber keeps its context/plugin graph alive) (CORE-051).
- **Quiescence (v0.3.0)**: `Registry::is_idle()` reports whether any lifecycle
  operation is in flight (no `Loading`/`Unloading` fiber, `pending_ops == 0`),
  and `Registry::when_idle()` / `Context::when_idle()` resolve once the tree is
  quiescent (CORE-049). This is the pure-Rust building block a supervisor needs
  for shutdown/health; it does not provide preemption or forced cancellation.
- **Cooperative cancellation (v0.3.0)**: `Fiber::cancel()`/`is_cancelled()` set
  a flag and `Context::checkpoint()` returns `Err(CordisError::Cancelled)` once
  set, so long-running plugin work can unwind through the normal failure/rollback
  path (CORE-050). The kernel never forcibly interrupts a plugin.
- **In-place replacement (v0.3.0)**: `Registry::replace(old_key, new_plugin)`
  (and `replace_async` for async plugins) re-binds every fiber of the old
  runtime to the new implementation, keeping fiber identity/uid and the retained
  raw config, moves the runtime to the new plugin's key and reloads each fiber
  through the normal activation pipeline (inject gate, config resolution,
  failure rollback). `Fiber::replace_plugin` / `replace_plugin_async` do the
  same for a single fiber (CORE-048). A reload rejected by a busy fiber
  (`LifecycleBusy`) rolls the re-binding and the runtime move back, so a failed
  replace leaves the registry and the fiber binding untouched.
- Registering the same key again returns the existing runtime (dedupe) until the
  last fiber of that key is disposed; then the runtime is removed (CORE-022).
- Child fibers created through `ctx.plugin` are effects of the loading fiber and
  are disposed with it; §2.3 gives the cascade, rollback and busy-child rules
  (CORE-047).

### 2.7 Logger / config / reflect (services owned by the core)

- `LOGGER` service (CORE-023): level threshold, exporters, bounded ring buffer
  (1000), monotonic sequence, plus the `LoggerService` facade
  (`level`/`set_level`/`is_enabled`/`logger`/`logger_with`/`exporter`/`log`/
  `records`/`clear_buffer`) and the per-plugin `Logger` facade
  (`log`/`error`/`warn`/`info`/`debug`).
- `Context::resolve_config` merges `ConfigSchema` layers base → root intercept →
  child intercept → head; `Plugin::config_schema` validation failures surface as
  `CordisError::Validation` and never replace the previous config (CORE-024).
- `ctx.accessor` / `ctx.accessor_with_setter` expose computed context properties;
  reads of services first consult accessors, and a setter-less accessor rejects
  writes with `CordisError::AccessorReadOnly`. `ctx.extend_meta` / `ctx.meta` /
  `ctx.meta_any` store and read metadata on context scopes (CORE-025).

### 2.8 Async plugins and blocking disposal (v0.2.0, Rust-only extensions)

**Async plugins.** [`crate::AsyncPlugin`] has the same shape as `Plugin`
(`name`/`inject`/`config_schema`) but activation is a future:

```rust
fn apply_async<'a>(&'a self, ctx: Context, config: Value) -> BoxFuture<'a, Result<()>>;
```

- `ctx.plugin_async(Arc<dyn AsyncPlugin>, Value)` awaits activation when `inject`
  dependencies are already available; otherwise the fiber stays `Pending`.
- `ctx.refresh_async()` re-evaluates fibers (including awaiting async
  activation/unload). The synchronous `ctx.refresh()` intentionally only handles
  sync plugins.
- `Fiber::restart_async` / `Fiber::update_async` are the async lifecycle entry
  points; the synchronous `restart`/`update` return
  `CordisError::AsyncPluginNeedsAsync` for async fibers instead of blocking.
- Failure rollback follows §2.3; async cleanups are awaited (CORE-036).

**Blocking disposal.** The synchronous disposal edges (`Fiber::dispose`,
`restart`, `update`, `AsyncDisposer::dispose_blocking`) run async cleanups through
`exec::set_blocking_executor` when the embedder installed a `BlockingExecutor`;
the default is the dependency-free thread-park `block_on`. An async host should
install an executor that either drives its runtime or rejects blocking calls from
async threads (CORE-034). This is the supported way to make sync disposal safe in
a multi-threaded server host.

## 3. Public API surface (Rust)

The main public entry points and types are listed below; the full set of `pub`
items is the crate API documentation. They are stable for the `0.y` line;
`pub(crate)` helpers and private fields are not part of the contract.

Core types:

```rust
pub struct Context;
pub struct Fiber;                 // wraps `pub type FiberRef = Arc<Mutex<FiberData>>`
pub struct Registry;              // PluginKey → PluginRuntime map
pub struct Events;                // event bus; `Context` forwards to it
pub struct LoggerService;         // ring buffer + exporters
pub struct Logger;                // named facade
pub struct ServiceKey<T: ?Sized + Send + Sync + 'static>;
pub struct ServiceGate;           // availability gate with dependent refresh
pub type ServiceValue = Arc<dyn Any + Send + Sync>;
pub type AvailabilityCheck = Arc<dyn Fn() -> bool + Send + Sync>;
pub enum FiberState { Pending, Loading, Active, Failed, Unloading, Disposed }
pub enum EventResult { Continue, Bail(Value) }  // Value = serde_json::Value
pub type Effect = Vec<Cleanup>;            // `cleanup(closure)` wraps one cleanup
pub type AsyncEffect = Vec<AsyncCleanup>;  // `cleanup_async(fut)` wraps one future
pub struct Disposer;              // sync, single-shot
pub struct AsyncDisposer;         // async cleanup handle

pub trait Plugin: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn key(&self) -> PluginKey;                       // default: PluginKey::Named(name)
    fn inject(&self) -> &'static [&'static str];      // default: []
    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>>;
    fn apply(&self, ctx: Context, config: serde_json::Value) -> Result<()>;
}
pub trait AsyncPlugin: Send + Sync + 'static {
    // same attributes as Plugin, but activation is a future
    fn name(&self) -> &'static str;
    fn key(&self) -> PluginKey;
    fn inject(&self) -> &'static [&'static str];
    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>>;
    fn apply_async<'a>(&'a self, ctx: Context, config: serde_json::Value)
        -> BoxFuture<'a, Result<()>>;
}
pub trait BlockingExecutor: Send + Sync {
    fn block_on(&self, future: BoxFuture<'static, ()>);
}
```

`CordisError` is `#[non_exhaustive]`; the v0.3.0 variants include `Cancelled`
(cooperative checkpoint observed cancellation) and `LifecycleBusy` (another thread
already runs a lifecycle operation on the fiber).

`impl Context` (grouped):

- scoping: `root()`, `extend()`, `isolate(name)`, `isolate_labeled(name, label)`,
  `intercept(name, config)`, `fiber()`, `registry()`, `events()`, `fiber_id()`,
  `plugin_name()`;
- services: `provide(key, value)`, `provide_checked(key, value, check)`,
  `provide_gated(key, value, &gate)`, `get(key)`, `require(key)`,
  `provide_any` / `get_any` / `require_any` / `set_any` (unsized lane),
  `get_named(name)`, `set(name, value)`, `set_revertible(name, value)`,
  `service_available(name)`,
  `resolve_config(name, base, head)`, `find_schema(name)`;
- effects: `effect(body)`, `effect_labeled(label, body)`, `effect_async(body)`,
  `effect_async_labeled(label, body)`, `effect_future(body, label)`,
  `effect_future_async(body, label)`, `effect_stream(stream, label)`,
  `attach_effect(disposer, label)`;
- plugins: `plugin(Arc<dyn Plugin>, config)`, `plugin_typed(Arc<P>, config)`,
  `plugin_async(Arc<dyn AsyncPlugin>, config)`, `plugin_async_typed(Arc<P>, config)`,
  `inject(deps, callback)`;
- lifecycle: `refresh()`, `refresh_async()`, `when_idle()`;
- cooperative cancellation: `checkpoint()`;
- events: `on`, `on_with(name, prepend, global, listener)`,
  `on_with_options(name, options, listener)`, `once`, `on_async`, `on_async_with`,
  `on_waterfall`, `on_waterfall_async`, `emit`, `bail`, `parallel_async`,
  `serial_async`, `waterfall`, `waterfall_async`;
- typed internal hooks: `on_internal_plugin`, `on_internal_status`,
  `on_internal_service`, `on_internal_config`, `on_internal_update`,
  `on_internal_get`, `on_internal_set`, `on_internal_listener`,
  `on_internal_dispatch`;
- logger / reflect: `logger()`, `logger_named(name)`, `logger_service()`,
  `log_exporter(exporter)`, `accessor(name, get)`,
  `accessor_with_setter(name, get, set)`, `extend_meta(meta)`, `meta::<T>(key)`,
  `meta_any(key)`.

`impl Fiber`: `dispose`, `dispose_async`, `restart`, `restart_async`,
`update(config)`, `update_async(config)`, `replace_plugin(Arc<dyn Plugin>)`,
`replace_plugin_async(Arc<dyn AsyncPlugin>)`, `cancel`, `is_cancelled`,
`await_settled`, `settled`, `state`, `is_active`, `assert_active`, `context`,
`config`, `raw_config`, `error`, `get_effects`, `uid`, `id`, `name`, `parent`.
`impl Registry`: `new`, `counter`, `size`, `get(key)`, `has(key)`, `delete(key)`,
`keys`, `values`, `entries`, `for_each`, `fibers`, `fiber_count`, `is_idle`,
`when_idle`, `replace(old_key, new_plugin)`, `replace_async(old_key, new_plugin)`.
`impl PluginRuntime`: `name`, `key`, `fibers`, `fiber_count`, `is_empty`.
`exec`: `block_on`, `boxed`, `set_blocking_executor`, `yield_now`.

## 4. Conformance cases

Every case below is asserted by the Rust test-suite in `tests/`
(`cargo test`). Case IDs are stable: a failing case is a contract
violation, not a test bug. Test names are listed per ID where the mapping is 1:1;
group IDs point at the file that owns the cases.

| ID | Requirement | Test(s) |
| --- | --- | --- |
| CORE-001 | Plugin activates, disposes and cleans up | `lifecycle::effects_run_in_reverse_order_once`, `lifecycle::event_bail_and_owner_disposal` |
| CORE-002 | Child contexts inherit provider registrations | `registry::service_is_visible_across_sibling_fibers` |
| CORE-003 | `isolate` shadows a service for the child subtree only | `lifecycle::isolate_shadows_parent_service`, `context::isolate_without_label_creates_fresh_scopes` |
| CORE-004 | Duplicate provide in one scope is rejected | `service::duplicate_service_is_rejected`, `service::default_scope_rejects_duplicates_but_isolate_shadows` |
| CORE-005 | Missing `inject` keeps the fiber Pending | `lifecycle::plugin_waits_for_injected_service` |
| CORE-006 | Gate flips activate and unload the dependent fiber | `service::service_gate_controls_dependents`, `service::service_check_gates_dependents` |
| CORE-007 | Effects clean up in reverse registration order, exactly once | `async_conformance::sync_and_async_effects_share_reverse_registration_order`, `async_conformance::async_cleanup_runs_in_reverse_order_once` |
| CORE-008 | A disposer runs exactly once | `async_conformance::async_disposer_is_single_shot` |
| CORE-009 | Fiber dispose awaits async cleanups before returning | `async_conformance::sync_dispose_awaits_async_cleanups_before_returning` |
| CORE-010 | Provider unload unregisters its services | `service::service_auto_register_and_dispose`, `service::provided_service_disposer_removes_service` |
| CORE-011 | Cleanup panic does not skip remaining cleanups (async lane; same policy as CORE-030) | `async_conformance::async_cleanup_panic_does_not_skip_remaining_cleanups` |
| CORE-012 | Effect diagnostics tree reflects registrations | `fiber::fiber_reports_effects_and_awaits_settlement`, `fiber::effect_disposer_removes_its_meta_node` |
| CORE-013 | Availability check gates lookups and dependents | `service::checked_service_predicate_controls_lookups` |
| CORE-014 | Closed gate does not fall back to an ancestor provider | `async_conformance::closed_gate_does_not_fall_back_to_an_ancestor_provider` |
| CORE-015 | Availability check may re-enter the context | `service::availability_check_may_read_other_services` |
| CORE-016 | Dependency removal unloads an active fiber | `service::service_gate_controls_dependents` |
| CORE-017 | Listeners are removed with their fiber | `lifecycle::event_bail_and_owner_disposal`, `async_conformance::async_listeners_unload_with_their_fiber` |
| CORE-018 | `prepend`/`global`/`once` event options | `events::event_options_prepend_and_global`, `events::event_once_fires_exactly_once`, `events::bail_ignores_null_and_false_values` |
| CORE-019 | Context scope filter for events | `events::event_context_filter_matches_scopes`, `events::event_waterfall_respects_context_filter` |
| CORE-020 | Waterfall middleware order and bail semantics | `events::*` + `async_conformance::waterfall_*` (8 cases) |
| CORE-021 | Internal hooks (plugin/service/config/get/set/update/listener/dispatch) | `internal::*` (8 cases) |
| CORE-022 | Registry map API + runtime dedupe/reclaim | `registry::registry_map_api`, `registry::registry_reuses_runtime_for_same_plugin`, `registry::registry_typed_plugin_identity`, `registry::delete_runtime_with_no_fibers_is_rejected_gracefully` |
| CORE-023 | Logger buffer/seq/threshold/exporters/intercept | `logger::*` (5 cases) |
| CORE-024 | Config schema validation and merge precedence | `config::*` (4 cases) + `service::service_resolve_config_merges_intercepts` |
| CORE-025 | Accessors, metadata, labeled isolates | `context::context_accessor_and_set`, `context::accessor_is_scoped_to_its_fiber`, `context::accessor_shadows_service_of_same_name`, `context::extend_meta_attaches_metadata`, `context::isolate_labeled_shares_scope` |
| CORE-026 | Fiber diagnostics (`uid`/`parent`/`assert_active`/`await_settled`) | `fiber::fiber_uid_parent_and_assert_active`, `fiber::await_settled_rethrows_startup_errors`, `fiber::await_settled_waits_for_async_effect_cleanup` |
| CORE-027 | Async effect bodies/streams and restart cleanup | `async_effect_body::*` (4 cases), `async_conformance::restart_async_drains_and_reapplies_async_effects`, `lifecycle::restart_reapplies_plugin_and_update_config` |
| CORE-028 | Failed activation rolls back effects and services | `hardening::failed_apply_rolls_back_effects_and_services` |
| CORE-029 | `apply` panic is isolated; fiber fails, caller does not panic | `hardening::panicking_apply_is_isolated_and_rolled_back` |
| CORE-030 | Cleanup panic does not skip remaining cleanups (sync + async) | `hardening::cleanup_panic_does_not_skip_remaining_cleanups`, `async_conformance::async_cleanup_panic_does_not_skip_remaining_cleanups` |
| CORE-031 | Listener panic does not abort sync dispatch | `hardening::event_listener_panic_does_not_abort_dispatch` |
| CORE-032 | Unsized `ServiceKey<dyn Trait>` via `*_any` services | `hardening::trait_object_service_keys_work` |
| CORE-033 | Root context/fiber graph has no strong reference cycle | `context::link_cycle_tests::root_context_graph_has_no_strong_cycle` |
| CORE-034 | Sync disposal uses the embedder blocking executor | `executor::sync_disposal_uses_embedder_blocking_executor` |
| CORE-035 | Async plugin activates and disposes through `*_async` APIs | `hardening::async_plugin_activates_and_disposes` |
| CORE-036 | Async plugin failure rolls back (awaits async cleanups) | `hardening::async_plugin_failure_rolls_back` |
| CORE-037 | Async plugin `inject` gating re-evaluated by `refresh_async` | `hardening::async_plugin_waits_for_inject_and_refresh_async` |
| CORE-038 | Failed sync activation drains async cleanups during rollback (reverse order across lanes) | `hardening::failed_sync_activation_drains_async_cleanup_inline` |
| CORE-039 | `restart`/`restart_async` re-check the `inject` gate | `hardening::restart_rechecks_inject_gate` |
| CORE-040 | Raw config is re-resolved once per activation (no compounding) | `semantics::raw_config_is_re_resolved_from_raw_on_every_activation` |
| CORE-041 | `ctx.set` is a revertible, classified mutation (dependents reactivate) | `semantics::set_is_revertible_and_classified`, `semantics::provider_version_change_reactivates_dependents` |
| CORE-042 | Provider withdrawal precedes binding removal | `semantics::provider_withdrawal_precedes_binding_removal` |
| CORE-043 | Concurrent async dispose joins the in-flight cleanup | `semantics::concurrent_async_dispose_joins_the_inflight_cleanup` |
| CORE-044 | A disposed fiber is final (no restart/activation resurrection) | `redteam::rt12_restart_after_dispose_fails_safely`, `redteam::rt01_sync_dispose_during_apply_must_not_end_active` |
| CORE-045 | Concurrent lifecycle calls are exclusive (`LifecycleBusy`) | `concurrency::concurrent_dispose_returns_lifecycle_busy`, `concurrency::reentrant_lifecycle_calls_are_allowed`, `redteam::rt13_dispose_during_restart_apply_cannot_leave_a_zombie` |
| CORE-046 | Cancelling an async disposal restores unstarted cleanups | `concurrency::cancelling_dispose_async_restores_unstarted_effects`, `concurrency::cancelling_async_disposer_restores_unstarted_cleanups` |
| CORE-047 | Child fibers are owned by their parent (cascade + rollback) | `child_ownership::*` (4 cases) |
| CORE-048 | In-place plugin replacement keeps fiber identity and reloads | `replace::*` (6 cases) |
| CORE-049 | Quiescence: `when_idle` resolves when no lifecycle work is in flight | `quiescence::fresh_registry_is_idle`, `quiescence::when_idle_waits_for_a_slow_activation`, `quiescence::when_idle_waits_for_an_async_disposal` |
| CORE-050 | Cooperative cancellation via `checkpoint` | `quiescence::checkpoint_observes_cooperative_cancellation` |
| CORE-051 | `dispose_all` shutdown path reclaims the whole tree | `child_ownership::dispose_all_reclaims_the_whole_tree` |

## 5. Deliberate differences from the JavaScript Cordis reference

1. **No Proxy-based property reads.** Services live behind explicit
   `ServiceKey<T>` lookups (`ctx.get(KEY)`) rather than `ctx.foo` proxies; Rust has
   no runtime property interception.
2. **No callable services / `Service` class inheritance / `ctx.mixin`.** These are
   JS language features; Rust expresses the same intent with typed traits and
   `.extend()`/`.isolate()`.
3. **Internal events are typed hooks**, not JSON strings (`internal.rs`), so
   embedders get compile-time checks; the event names and ordering match the JS
   reference.
4. **Logger severity order is numeric threshold order** (`Error=0 … Debug=3`), the
   same ordering used for thresholds; call sites use the facade names.
5. **`internal/plugin` broadcasts on create only** (no dispose-time broadcast yet);
   see `docs/CORDIS-RUST.md` for the current status of that edge.
6. **Async plugins (v0.2.0)** — the JS reference is promise-native at the plugin
   level; Rust adds the explicit `AsyncPlugin`/`plugin_async` entry points plus
   `refresh_async`, and rejects sync lifecycle APIs on async fibers instead of
   blocking (CORE-035..037).
7. **Blocking executor (v0.2.0)** — the JS reference never blocks a thread on
   disposal; Rust's sync disposal mirror accepts an embedder `BlockingExecutor`
   so server hosts can route it through their runtime or fail loudly (CORE-034).
8. **No dynamic plugin loading / stable ABI (v0.2.0)** — plugins are in-process
   `Arc<dyn Plugin>` / `Arc<dyn AsyncPlugin>` values linked into the host binary.
   There is no ABI/FFI boundary, no cdylib/dylib loader, module-URL resolution or
   configuration-file loader, and no process/WASM isolation host; the Rust ABI is
   not stable, so a loader with hot replacement and any sandboxing layer are
   embedder concerns (see `docs/CORDIS-RUST.md`).

## 6. Versioning

- `0.y.z` — breaking changes may land in `y`.
- Any change to a CORE case ID or to a documented ordering guarantees requires a
  `spec/` change in the same commit and a minor bump.
- Embedders should pin a tag (`tag = "v0.3.0"`) and update deliberately.
- v0.3.0 added CORE-040…CORE-050: raw-config retention and one-shot resolution,
  provider-identity epochs and withdrawal ordering, revertible/classified
  `ctx.set`, async-disposer joining, lifecycle exclusivity, cancel-safe disposal,
  child-fiber ownership, in-place plugin replacement, quiescence, and cooperative
  cancellation.
- v0.2.0 added CORE-028…CORE-039 and changed failure/cleanup semantics
  (rollback + panic isolation); it is a behavioral change and therefore a minor
  bump per the rule above.
