# Cordis Core — normative specification

Status: **v0.5.0**. The Rust crate (repository root) is the reference implementation of
this document. A behavior that is not written here is not part of the contract.

Editorial revision v0.5.0: the prose and the API surface below were re-checked against
the `v0.5.0` crate. No CORE case is added, renumbered or removed — **all 51 cases
(CORE-001…CORE-051) remain the contract**. v0.5.0 is a **minor release**: it converges
the public API (internal state types leave the public contract; `Fiber::last_activity`
becomes `last_activity_ms`, Q11 clarifies that `FiberQuota::max_effects` counts user
effects only) and adds three general extension APIs — `Clock`/`SystemClock` injection,
`LifecycleEvent`/`LifecycleListener`/`Context::on_lifecycle`, and
`CordisError::Plugin`/`Fiber::error_detail`. v0.4.2–v0.4.4 remain patch releases that
fixed the implementation against requirements this document already implied; their
behavioral deltas are listed in `CHANGELOG.md` and summarised in §6. Where a release
makes a previously unstated edge explicit it is marked "(v0.4.x)"/"(v0.5.0)" below;
such a mark is a clarification of an existing case, not a new case.

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

> **Editorial note (v0.4.4): kernel vs plugin boundary.** The kernel/plugin/embedder
> layering, the "never in the kernel" list (business domain logic; I/O, network,
> persistence, HTTP, databases, caches, filesystem, environment, process control;
> domain config schemas; business metrics; business policy), the dependency
> direction (`src/**` may use only `serde_json`, `tracing`, `futures-util`) and the
> extension points are documented in
> [`docs/KERNEL-BOUNDARY.md`](../docs/KERNEL-BOUNDARY.md) and enforced by
> `tests/kernel_boundary.rs` plus a CI step. This note adds **no CORE case**: it
> only states where implementers and embedders are expected to put business
> capabilities (plugins/examples/separate crates or the host, not the kernel).

## 2. Core model

### 2.1 Context

- `Context::root()` creates the root context and its sole `root` fiber (active,
  no `apply` body).
- `ctx.extend()` returns a child context sharing parent state and scopes.
- `ctx.isolate(name)` returns a child context with a **new scope** for one service
  name. Reads/writes of `name` below the child resolve in that scope; all other
  services keep resolving in their inherited scopes.
- `ctx.isolate_labeled(name, label)` joins a previously created named scope.
  Label ids are **allocated by a registry-level counter**, not hashed: the first
  use of a label inserts it into the registry's `label_scopes` map with a fresh
  counter value, and every later context using the same label joins that scope
  (the `labeled_scope_id` helper in `src/registry.rs`). The label is a
  `&'static str`, so the map key has a stable identity for the process lifetime.
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
  **Type-checked writes (v0.4.2, enforces DESIGN I5):** `set` and `set_any` compare
  the value's `TypeId` with the registered entry's type and return
  `CordisError::ServiceTypeMismatch(name)` on a mismatch, leaving the store
  untouched; the terminal write (`write_service`) re-checks the `TypeId` as
  defence in depth, so a store entry can never change its concrete type. An
  `internal/set` hook can only veto the write, it cannot substitute a value.
  A wrong-typed overwrite can no longer silently replace a service.
  **Cost (XT-07):** every successful `set`/`set_any` registers one inverse effect
  on the providing fiber (that is what makes it revertible), so a hot loop of
  `set` grows that fiber's effect list linearly — O(n) memory and O(n) unload
  work. Prefer mutating the service value in place for high-frequency updates,
  reserve `set` for configuration-style changes, and use `set_revertible` when
  the caller wants to collapse the inverse history.
  **Accessor write rejection (v0.4.3)**: when a computed accessor has a setter and
  that setter returns `false` (the owner inspected the value and rejected it), the
  write returns `CordisError::AccessorWriteRejected(name)` — distinct from
  `ServiceWriteForbidden` (another fiber owns the service) and
  `AccessorReadOnly` (the accessor has no setter).
- **Injection gate vs. read lane (v0.4.2)**: `ctx.service_available(name)` and the
  `inject` gate use the **strict** query — the service must be stored, its
  availability check must pass, **and** the providing fiber must be `Active`.
  Unloading a provider withdraws its services before draining (CORE-042), so a
  dependent never activates against a provider whose `apply` has not returned
  (CORE-006/016). The read lane (`ctx.get`/`get_named`/`get_any`) stays
  non-strict, matching the TypeScript property-read semantics: it can observe a
  value while the provider is still `Loading`.
- **Accessors are read-only for injection.** Accessors, `internal/get` virtual
  services and other read-lane-only values are visible to `ctx.get`/`get_named`
  but **never satisfy `inject` or `service_available`** (v0.4.2, mirrors the TS
  reference). A fiber that requires such a value must resolve it in `apply`
  instead of declaring it in `inject`.
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
  (CORE-005, CORE-006, CORE-016). The gate is the **strict** query of §2.2: a
  provider that exists but is not `Active` yet (v0.4.2) and a read-lane-only
  accessor do not satisfy it.
- **Failure semantics (v0.2.0)**: when `apply` returns `Err` **or panics**, the
  activation is failed: every effect registered during the failed activation is
  rolled back (sync cleanups immediately and async cleanups through the blocking executor
  (each panic isolated), so reverse registration order holds across lanes and
  an undisposed Failed fiber cannot leak its cleanups, CORE-028/CORE-038), services/listeners are removed, and only this fiber
  transitions to `Failed`. The panic never escapes `ctx.plugin` (CORE-029).
- `fiber.dispose()` (or `fiber.dispose_async()` for async cleanup) runs registered
  effects **in reverse registration order** and releases owned services and
  listeners (CORE-007, CORE-010, CORE-017).
- **Config re-resolution (v0.3.0, ordering made explicit in v0.4.2)**: the fiber
  keeps the caller-supplied raw config and resolves it (internal/config waterfall
  + schema) once **per activation**; restart/update therefore never compound
  derived values (CORE-040). `Fiber::config` returns the resolved value,
  `Fiber::raw_config` the input. `update` runs in this order: `internal/config`
  on the caller config → schema validation → `internal/update`. Only the terminal
  of `internal/update` retains a raw config and restarts, and it runs at most
  once. Consequently:
  - an invalid candidate returns `CordisError::Validation`, does not restart and
    leaves the retained raw config untouched;
  - an update hook that returns a different config has that replacement validated
    before it becomes the new raw config (resolved once on the following
    activation);
  - a pass-through hook keeps the caller's raw config (the resolved/derived
    value is never stored, so nothing compounds);
  - a hook that never calls `next` **vetoes** the update: `update` returns
    `Ok(())`, the fiber does not restart and the retained raw config is unchanged;
  - `Fiber::update` is `update_with_no_save(config, false)`. The additive
    entries `Fiber::update_with_no_save(config, no_save)` and
    `update_async_with_no_save(config, no_save)` expose `no_save`, which is a
    **hint for persistence hooks** (Cordis `update(config, noSave)`): the kernel
    forwards it through the `internal/update` hook chain, but a successful
    (non-vetoed) update always commits the new raw config and restarts —
    `no_save` never suppresses the in-memory raw-config update (v0.4.2, TS
    parity).
- **Provider-withdrawal order (v0.3.0)**: unloading a provider marks its services
  withdrawn *before* draining, so dependents deactivate while the binding is
  still present and only then is it removed (CORE-042).
- **Disposed is final (v0.3.0)**: restart/update/activate on a disposed fiber
  returns `CordisError::Disposed`, and a reentrant dispose during activation
  rolls the activation back instead of resurrecting the fiber (CORE-044).
- **Child-fiber ownership (v0.3.0, hardened in v0.4.2)**: `ctx.plugin` registers
  the new fiber as an effect of the caller's fiber, so unloading the parent
  disposes the whole subtree; a failed parent activation rolls its children back
  too. The ownership effect holds a `Weak` fiber handle (no parent↔child cycle)
  and is disposed in reverse registration order like any other effect (CORE-047).
  A child whose lifecycle is busy while the parent unloads is flagged and
  disposes itself when its current operation finishes; a cancelled cascade is
  retried by the next `dispose` (the parent keeps a weak child list for this).
  **Loading depth is bounded** (`MAX_PLUGIN_DEPTH = 256`): recursive loading
  (A→B→A or a plugin that keeps loading itself) fails with
  `CordisError::InvalidPlugin` instead of overflowing the stack. Since v0.4.2 the
  bound is counted along the **fiber parent chain** (`Fiber::parent()` → that
  fiber's `context().fiber()`), not the context chain: the root fiber counts as
  one level, a plugin loaded from the root context starts at depth 1, and every
  nested `ctx.plugin` inside `apply` adds one level — so roughly 254 nested
  loads below a root-level plugin are accepted and the next one fails.
  Context-only nesting (`extend`/`intercept`/`extend_meta`, e.g. 300 levels)
  consumes no budget at all. A dependency chain longer than the bound cannot be
  constructed through `ctx.plugin`: e.g. a 300-level chain is truncated at 255
  loaded levels, so only 255 cleanups run (XT-11).
  **Dynamic child bookkeeping is bounded (v0.4.3, R3-A4)**: a child fiber that
  disposes itself detaches its ownership effect from the parent's effect list
  (and its effect-node / weak-child-list entries), and a parent effect list that
  grows past a threshold amortizes away already-disposed ownership effects, so
  repeated dynamic load/unload cycles cannot accumulate an unbounded effect list
  on the parent.
  **Deep dependency chains unload iteratively (v0.4.3, R3-A1)**: unloading uses a
  two-phase, order-preserving cascade — every transitively-unready fiber is
  *begun* provider-before-dependent (marked `Unloading`, services withdrawn,
  effects taken) and the frames are then drained in reverse order (an equivalent
  dependents-before-provider cleanup order); ownership descendants are collected
  with an explicit DFS stack. The call stack is therefore O(1) in the chain depth:
  on a 2 MiB thread, depth 800 passes in both debug and release builds in v0.4.3
  (v0.4.2 comparison: debug aborted at about 480 levels, release at about 2200),
  far beyond anything the 256-level loading limit can create. The cascade has a
  `fiber_count + 2` fixed-point bound;
  any leftover dependent stays `Active` and can be retried by a later
  dispose/refresh. This supersedes the former "deep chain dispose can overflow
  the stack" limitation.
- **Lifecycle broadcasts (v0.5.0, event snapshot in v0.5.1/XT-03)**: `Context::on_lifecycle(listener)`
  subscribes a **non-fiber-bound** listener (`LifecycleListener`) that receives one
  `LifecycleEvent { fiber_id, name, from, to, seq }` per fiber state transition.
  `from`/`to` are captured **under the state lock** at transition time (never
  re-read from the live fiber when the listener runs), so a reentrant or
  concurrent transition cannot make an event claim an impossible
  `from`/`to` pair. `seq` is a per-fiber monotonic counter assigned in the same
  critical section: delivery order across threads is not guaranteed, but sorting
  a fiber's events by `seq` reconstructs the exact transition chain.
  Multiple subscribers run independently and listener panics are contained and
  logged. The subscription is **not fiber-owned**: the returned `Disposer` is its
  single owner — dropping the `Disposer` unsubscribes exactly like `dispose`
  (single-owner safety net); to keep observing across the installing fiber's
  lifetime, store the `Disposer` and dispose it explicitly when done. It fires
  at the same point as the `internal/status` hook (CORE-021) from a dedicated
  internal slot and does not alter the state-machine guarantees of
  CORE-044/045/047.
- **Pending-dispose finalisation (v0.4.2)**: when a parent unload flags a busy
  child for disposal, every lifecycle entry point (`restart`, `restart_async`,
  `update`, `update_async`) finalises the pending dispose on **all** exit paths —
  early `Err`, `?` and panics — so a flagged child cannot remain `Active` after
  its operation returns. The guard also covers the `Disposed`/`Unloading`/
  `AsyncPluginNeedsAsync`/bind-failure/validation/veto exits.
- **Refresh containment (v0.4.2)**: the dependent-refresh latch is released by an
  RAII guard, so a panic that escapes one refresh pass can never disable refresh
  permanently (retrying `ctx.refresh()` or re-providing the service recovers).
  Cleanup panics raised while a refresh/retire path unloads dependents are logged
  and contained — the provider still reaches its target state. A direct
  `Fiber::dispose` keeps the CORE-030 contract and re-raises after the drain.
- **Unload containment (v0.4.2)**: disposing a parent sweeps its children one by
  one; a child panic is captured per child, the sweep **continues** with the
  remaining children, and the first captured panic is re-raised only after the
  parent reached its target state and its effects were taken. A failed activation
  rollback and stale-activation discard log sweep panics instead of propagating
  them.
- **Plugin metadata panics are contained (v0.4.3, C2)**: `Plugin::name` /
  `::key` / `::inject` / `::config_schema` (and the `AsyncPlugin` counterparts)
  are embedder code invoked during creation, replacement, restart, diagnostics
  and dependency resolution. A panic in them is caught, logged and degraded
  deterministically: `name()` → `"<panicked plugin>"`,
  `key()` → `PluginKey::Named("<panicked plugin>")` (such plugins share one
  runtime), `inject()` → `&[]` (no dependency gating), `config_schema()` →
  `None` (raw config accepted, no validation). It never unwinds out of
  `ctx.plugin`/replace or poisons a kernel lock; the affected plugin is still a
  broken plugin (best-effort isolation, not a repair).
- **Activation epochs (v0.4.2)**: effects registered during an activation attempt
  are tagged with that attempt's epoch (a thread-local activation frame, carried
  across executor migration for async activation), so a concurrent restart
  discards exactly the stale attempt's effects and cannot leave a stale tail
  attached to the new activation (CORE-045).
- **Observable lifecycle transitions (v0.4.2)**: `restart`/`restart_async` change
  state through the same transition helper as every other state change, so an
  `internal/status` observer sees `Active → Unloading`; no raw state write bypasses
  the hook.
- **Lifecycle operations are exclusive (v0.3.0)**: `dispose`/`restart`/`update`
  (sync and async) claim a per-fiber lifecycle slot. A concurrent call from
  another thread returns `CordisError::LifecycleBusy` instead of racing; a
  reentrant call on the owning thread (a cleanup disposing its own fiber,
  `update` calling `restart`) is allowed (CORE-045).
- **Cancel-safe disposal (v0.3.0; fiber-level completion clarified in v0.4.2)**:
  cancelling is defined per granularity (CORE-046):
  - **`AsyncDisposer` handle** (one effect's async cleanup batch): dropping
    `AsyncDisposer::dispose` mid-await restores the cleanups that had not
    started; the next `dispose()`/`dispose_blocking()` retries them in the same
    reverse order (S-1), and the single in-flight cleanup future is lost (logged
    when other cleanups were restored; a fully consumed list loses that last
    future silently).
  - **fiber unload** (`dispose_async`/`refresh_async`): a cancelled unload now
    **completes the teardown instead of restoring the old state** (A-9,
    v0.4.2): sync cleanups run immediately, async cleanups are parked in the
    fiber's deferred list and completed by the next dispose/drain, and
    `finish_unload` lands the fiber in its target state (`Pending` for a
    reload/retire unload such as `restart_async`/`refresh_async`, `Disposed` for
    a dispose), removes the event owner and notifies quiescence — never a permanent `Unloading` and never a
    fake-`Active` half-torn-down fiber.
  A cleanup that calls `dispose_blocking` on its **own** disposer returns
  immediately (self-join short-circuit, prevents deadlock, same as
  v0.3.0/0.4.1); joiners on other threads or other disposers still block until
  the teardown completes. The async mirror (`await`ing its own
  `AsyncDisposer::dispose` from inside a cleanup of that disposer) short-circuits
  the same way. The check is a *drain frame* (are we currently polling that
  disposer's cleanup on this call stack?), not a thread id: independent
  joiners on the same worker thread still wait for the teardown (S-2/R2-1).
- `fiber.await_settled()` resolves once the fiber is stable (Active/Failed).
  Inside the `internal/plugin` create hook the new fiber is still `Pending`
  with no in-flight fiber-level operation, so `await_settled()` resolves
  immediately there (v0.4.2 boundary; the registry-level registration guard is
  not visible to it).
- Diagnostics: `fiber.uid`, `fiber.parent`, `fiber.get_effects()` returning the
  `EffectMeta` tree (CORE-012). Since v0.4.3 the snapshot is filtered to **live**
  effect nodes: a nested effect disposed early by the user no longer leaves a
  ghost node under its parent's metadata (C3).

### 2.4 Effects

- `ctx.effect(body)` runs `body` immediately and owns its result. The body is
  `FnOnce() -> Effect` with `Effect = Vec<Cleanup>`: an empty vector means nothing
  to clean up, and `zeokeel_cordis::cleanup(closure)` wraps one sync cleanup. The call
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
  `EventOptions { prepend, global, once }`; `ctx.on_with(event, prepend, global,
  listener)` is the positional form; `ctx.once` removes the listener after its
  first call (CORE-018). `once` is part of the options struct as well, so
  `EventOptions::new().once()` expresses a one-shot listener without the
  convenience method.
- Dispatch modes are part of the contract:
  - sync lane — `ctx.emit` (registration order, return value ignored), `ctx.bail`
    (registration order until the first non-null, non-`false` `Bail`), and
    `ctx.waterfall` (middleware around an explicit `next`);
  - async lane — `ctx.parallel_async` (await every listener concurrently),
    `ctx.serial_async` (await in registration order until one bails), and
    `ctx.waterfall_async`.
  Listeners registered with `ctx.on_async` are dispatched by the async lane only;
  sync dispatch never polls them. Within the async lane, `parallel_async`
  invokes sync listeners inline first and then awaits the async futures
  concurrently, while `serial_async` walks the **registration order** and
  runs/awaits each listener in that order (sync and async listeners interleave
  by registration).
- **Async registration bypasses `internal/listener` (XT-06)**: `ctx.on_async` /
  `ctx.on_async_with` are registered directly on the async lane and are **not**
  passed through the `internal/listener` hook (sync `on`/`on_once`
  registrations are); the async lane also has no `once`/`global` options. A host
  that uses `internal/listener` for policy (audit, deny-list, rewriting) must
  enforce it on the sync lane only or wrap async callbacks itself.
- Scope filters: a listener receives an event only when the dispatching context's
  isolation map equals the listener's own, unless `global` is set (CORE-019).
- Waterfall hooks (`src/waterfall.rs`) are **middleware around an explicit
  continuation, not a bail lane**: each listener receives the dispatch args plus a
  `next` handle. Calling `next.next()` runs the remaining middlewares and
  finally the caller-supplied terminal; returning a rewritten value on the way
  out is the middleware pattern. A listener that returns **without** calling
  `next` vetoes the rest of the chain, terminal included — there is no
  `EventResult::Bail` in the waterfall lane (CORE-020). `next.remaining()` and
  `next.terminal()` let a middleware inspect or skip to the terminal.
- **Waterfall panic policy (v0.4.2, both lanes)**: a panicking waterfall
  middleware aborts the chain (the remaining middlewares and the terminal do not
  run), is logged, and the panic is re-raised to the dispatch caller —
  fail-closed, never fail-open. The sync lane always behaved this way; the async
  lane swallowed the panic into `Value::Null` until v0.4.2 and now re-raises
  identically. Inside `apply`/`apply_async` the caller is the activation path, so
  the re-raised panic becomes a failed, rolled-back activation (CORE-029). This
  is the only dispatch boundary whose panics are not contained; see
  `docs/CORDIS-RUST.md` §3.
- **Listener ownership is released on drop (v0.4.2)**: disposing a fiber removes
  its listeners as effects, and a `FiberData` dropped without an explicit dispose
  still removes its event owner entry as a last resort, so an undisposed handle
  cannot keep listeners registered forever.

### 2.6 Registry

- `ctx.plugin(Arc<dyn Plugin>, config)` registers a fiber under a `PluginKey`:
  `Named(name)` by default, `Type(TypeId)` with `plugin_typed`. `plugin_async` /
  `plugin_async_typed` are the `AsyncPlugin` counterparts (CORE-035).
- `Registry::size/get/has/delete/keys/values/entries/for_each/fibers`
  expose the live plugin map (CORE-022). These are **per-structure snapshots**:
  each call locks one map/list, so during concurrent lifecycle operations
  different calls can be transiently inconsistent (e.g. `keys()` lists a
  runtime that `size()` no longer counts). Treat them as diagnostics; use
  `is_idle()`/`when_idle()` when a consistent view is required.
- **Shutdown helper (v0.3.0, panic priority v0.4.2)**: `Context::dispose_all()` /
  `Registry::dispose_all()` dispose every registered fiber and the root fiber,
  collecting the first cleanup error after attempting all of them. **On
  `Ok`** the registry is empty and `is_idle()` is true. If a fiber's lifecycle
  operation is busy during the attempt, the call stops that fiber with the first
  error (`Err(LifecycleBusy)`) after attempting the others; the busy fiber may
  leave the registry non-empty or non-idle, so the caller must retry after its
  lifecycle operation finishes (XT-10). This is the supported
  shutdown path (dispose+drop is the only path that reclaims the graph; a
  never-disposed fiber keeps its context/plugin graph alive) (CORE-051). When a
  cleanup **panics** and another fiber also returns an error, the panic wins: the
  error is logged with `tracing::error!` and the first panic payload is
  re-raised to the caller after every fiber was attempted (v0.4.2; previously
  the panic payload was dropped and only the error surfaced).
- **Quiescence (v0.3.0)**: `Registry::is_idle()` reports whether any lifecycle
  operation is in flight (no `Loading`/`Unloading` fiber, `pending_ops == 0`),
  and `Registry::when_idle()` / `Context::when_idle()` resolve once the tree is
  quiescent (CORE-049). This is the pure-Rust building block a supervisor needs
  for shutdown/health; it does not provide preemption or forced cancellation.
  `Registry::when_idle()` returns the crate-root `IdleWait<'_>` future
  (`zeokeel_cordis::IdleWait`); `Context::when_idle()` is the async convenience
  wrapper that awaits it. Since v0.4.2 the whole registration→activation window of `ctx.plugin*` counts
  as in-flight work: from the context entry point through the `internal/plugin`
  create hook and up to the end of activation. Since v0.4.3 (R3-A3/C4) the
  registry records which **thread created** each in-flight registration:
  `when_idle()` subtracts the caller's own registrations, so a create hook can
  await it without waiting for itself, while external waiters keep the full
  registration-window guarantee. `is_idle()` stays **strict** (it excludes
  nothing, so the window is non-idle for every observer). **Boundary:** two
  threads whose registrations mutually await each other's window can still
  deadlock; the kernel does not detect cyclic registration dependencies.
  **The exemption covers only the `internal/plugin` create hook** (XT-04): while
  `apply`/`apply_async` runs, the fiber's own activation is an in-flight
  fiber-level operation (`pending_ops > 0`), so synchronously awaiting
  `when_idle()` from activation code waits for itself. Do not block on
  quiescence inside `apply`/`apply_async`; external waiters are unaffected.
- **Cancelled-cascade recovery (v0.4.3, F1/F4/F5)**: a cancelled async unload
  cascade may temporarily park non-root dependents in `Unloading`.
  - **Recovery entries (F1)**: `Registry::when_idle()`/`Context::when_idle()`,
    `refresh_async()`, `dispose_all()`, and a retried root
    `Fiber::dispose()`/`dispose_async()` recover such parked frames to their
    **original cascade target** (`Pending` for dependency dependents,
    `Disposed` for ownership children); unstarted deferred async cleanups run
    exactly once during recovery.
  - **Latched safely (F4)**: the single-recoverer latch is an RAII guard (a
    cancelled or panicking recovery cannot leave it set and disable later
    recovery), and the batch of taken frames is re-parked on cancellation:
    recovery interrupted mid-batch loses no frame.
  - **Waits keep recovering (F5)**: `when_idle()` is recovery-aware on **both**
    the `Context` and the raw public `Registry` API — the wait loop retries
    recovery whenever parked frames are recoverable (bounded, with a fallback),
    so frames parked while waiting cannot hang it. **Synchronous `refresh()`
    deliberately does not recover parked frames**: it may run on a reactor
    thread, where draining async cleanups synchronously would deadlock; it
    skips `Unloading` frames and leaves recovery to the entries above.
  - Recovery is idempotent, deduplicated and bounded by the number of
    concurrently parked frames; it is deferred to the next root operation while
    a cancellation unwind or another unload is in progress. If the cancellation
    lands while an async cleanup is executing, that in-flight future is not
    recoverable (the A-9 boundary) but the fiber never stays permanently
    `Unloading`; `is_idle()` is strict and remains `false` until recovery runs.
- **Quotas count live resources (v0.4.3, S1, `supervision` feature)**: per-fiber
  `FiberQuota` limits apply to live effects/listeners/child fibers. Effects
  whose disposer has run are pruned from the quota check, a listener slot is
  released exactly once by whichever path comes first (user disposer, a `once`
  listener's auto-removal, or fiber unload), and child fibers are counted by live
  weak handles. Released slots are reusable immediately; exceeding a limit still
  returns `CordisError::QuotaExceeded` and rolls the activation back. Since
  v0.5.0 (Q11) **`max_effects` counts user effects only** — internal kernel
  effects (the root logger provide effect, the ownership effect of each
  dynamically loaded child fiber) are excluded, so `max_effects(1)` still loads
  plugins and only limits effects registered through `ctx.effect*` by
  hosts/plugins.
- **Cooperative cancellation (v0.3.0)**: `Fiber::cancel()`/`is_cancelled()` set
  a flag and `Context::checkpoint()` returns `Err(CordisError::Cancelled)` once
  set, so long-running plugin work can unwind through the normal failure/rollback
  path (CORE-050). The kernel never forcibly interrupts a plugin.
- **In-place replacement (v0.3.0, full rollback in v0.4.2)**: `Registry::replace(old_key, new_plugin)`
  (and `replace_async` for async plugins) re-binds every fiber of the old
  runtime to the new implementation, keeping fiber identity/uid and the retained
  raw config, moves the runtime to the new plugin's key and reloads each fiber
  through the normal activation pipeline (inject gate, config resolution,
  failure rollback). `Fiber::replace_plugin` / `replace_plugin_async` do the
  same for a single fiber (CORE-048). A reload rejected by a busy fiber
  (`LifecycleBusy`) rolls the re-binding and the runtime move back, so a failed
  replace leaves the registry and the fiber binding untouched. **`Ok` does not
  mean `Active` (v0.5.0 clarification)**: a reload/activation failure after the
  swap is contained at the fiber level — `Fiber::replace_plugin` (and
  `restart`/`update`) returns `Ok(())` while the fiber lands in `Failed`, with the
  structured error retained in `Fiber::error_detail()`; callers must check
  `Fiber::state()`/`error_detail()` rather than only the return value.
  Validation/veto/lifecycle errors still return `Err`. **The same rule applies
  to the multi-fiber `Registry::replace`/`replace_async` (XT-05)**: a reload or
  activation failure of the new implementation is contained at the fiber level —
  the fibers land in `Failed` and the runtime stays bound to the new plugin key —
  and the previous implementation/service is **not restored**. Only a lifecycle
  rejection (`LifecycleBusy`) or a validation error triggers the rollback
  described here; hosts doing hot-swap must check `Fiber::state()` /
  `error_detail()` and keep their own recovery path. Since v0.4.2 the
  rollback also **restores the fibers that had already been reloaded**: every
  fiber switched to the new implementation is reloaded back with the old plugin
  before `replace` returns, so a failed replace leaves no mixed
  runtime/effects. If that rollback itself fails, the call returns
  `CordisError::Rollback(message)` — the message carries the original error and
  the rollback error — and still restores the binding as far as possible.
  **Reload panics are contained (v0.4.2)**: a cleanup panic raised while the
  reload drains the previous implementation is caught and reported as
  `CordisError::PluginFailed("<op>: reload panicked: …")`, then the rollback
  above runs; these `Result` APIs never let the panic escape (the single-fiber
  `replace_plugin` path applies the same policy and restores the old
  plugin/runtime).
  **Lifecycle exclusivity of a single-fiber replacement (v0.4.2)**:
  `Fiber::replace_plugin` / `replace_plugin_async` claim the fiber's lifecycle
  slot (and count as an in-flight operation) for the swap/re-bind prefix, so a
  concurrent lifecycle call gets `LifecycleBusy`; when the root-fiber special
  case fails, the temporary runtime and root binding are rolled back and no
  entry is left in the registry (CORE-045/048).
- **`ctx.inject` identity (v0.4.2)**: `ctx.inject(deps, callback)` installs one
  fiber per call, keyed by `PluginKey::Type(TypeId)` of its `'static` callback
  type, so independent injections are independent runtimes; `Fiber::name()` still
  reports `"injected"` for diagnostics but `Registry::replace(Named("injected"))`
  no longer matches them. Before v0.4.2 all injections shared a single
  `Named("injected")` runtime and a replace re-bound all of them at once.
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
  (`log`/`error`/`warn`/`info`/`debug`). The root context installs the service
  but **no exporter**: records only reach the ring buffer until the embedder
  installs one with `ctx.log_exporter(...)` (v0.4.2 clarification —
  `TracingExporter` is the adapter that forwards records to `tracing`; the
  kernel's own diagnostic `tracing` events are an independent channel). An
  **invalid numeric level** in logger config is clamped to the safe end
  (`Error`) since v0.4.3 (S2); string levels are unchanged. Since v0.5.0
  `LogRecord::ts_unix_ms` comes from the tree's injectable clock; a `ctx.logger()`
  facade snapshots the clock **at creation** (so a facade obtained before
  `set_clock` keeps the old clock — re-acquire it after changing the clock),
  while the raw `LoggerService` API always timestamps with `SystemClock`.
  Only new observations are affected by a clock change; the clock should be set
  once, right after `Context::root` and before loading plugins (not swapped at
  runtime).
- `Context::resolve_config` merges `ConfigSchema` layers base → root intercept →
  child intercept → head; `Plugin::config_schema` validation failures surface as
  `CordisError::Validation` and never replace the previous config (CORE-024).
  `ctx.intercept(name, config)` only influences `resolve_config`, i.e. the
  configuration of services that resolve through it (the logger and
  `Service::resolve_config` implementors); **plugin** config goes through the
  `internal/config` waterfall and is not affected by intercepts (v0.4.2
  clarification).
- `Context::resolve_config` schema layers are typed: `JsonConfigSchema` accepts
  `JsonType::{String, Number, Bool, Object, Array}` (the enum is unchanged in
  v0.4.2, so downstream exhaustive matches stay source-compatible). A non-null
  `default_value` also declares the property's kind. A **`null` default declares
  no type** (v0.4.2 fixes the old behaviour that inferred `Object` and therefore
  rejected the injected `null`); whether it is injected depends on the declared
  kind (`property(name, JsonType::…)` or an earlier non-null default):
  - **untyped** — a missing key is injected as `null`; the key is declared (so
    `deny_unknown` accepts it) and its value is not type-checked;
  - **typed** — the key stays **absent** by default (injecting `null` would
    contradict the declared kind), which models an optional field whose default
    is `null`: a caller-provided value is type-checked as declared, and
    `required(name)` demands an explicit value.
- `ctx.accessor` / `ctx.accessor_with_setter` expose computed context properties;
  reads of services first consult accessors, and a setter-less accessor rejects
  writes with `CordisError::AccessorReadOnly`. Since v0.4.3 a **writeable**
  accessor whose setter returns `false` rejects the write with the dedicated
  `CordisError::AccessorWriteRejected(name)` (S3). A panicking accessor getter is
  contained and the read is treated as **absent**: `ctx.get` / `get_named` /
  `get_any` return `None` (v0.4.2; the old placeholder value is gone).
  `ctx.extend_meta` / `ctx.meta` / `ctx.meta_any` store and read metadata on
  context scopes (CORE-025).

### 2.8 Async plugins and blocking disposal (v0.2.0, Rust-only extensions)

**Async plugins.** [`crate::AsyncPlugin`] has the same shape as `Plugin`
(`name`/`inject`/`config_schema`) but activation is a future:

```rust
fn apply_async<'a>(&'a self, ctx: Context, config: Value) -> BoxFuture<'a, Result<()>>;
```

- `ctx.plugin_async(Arc<dyn AsyncPlugin>, Value)` awaits activation when `inject`
  dependencies are already available; otherwise the fiber stays `Pending`.
  `AsyncPlugin` metadata callbacks are contained with the same degradation as the
  sync trait (v0.4.3, C2).
- `ctx.refresh_async()` re-evaluates fibers (including awaiting async
  activation/unload). The synchronous `ctx.refresh()` intentionally only handles
  sync plugins. Both run a **bounded fixed point**: at most 64 passes, each
  advancing in the registry's snapshot order. A forward-ordered dependency chain
  (provider registered before consumer) settles in one pass; a reverse-ordered
  chain advances about one level per pass, so an extremely deep chain may need
  repeated `refresh()` calls before it is fully settled. An **explicit**
  `refresh()`/`refresh_async()` re-evaluates the whole registry snapshot,
  including availability-gate flips; the internal dependents refresh performed
  after provide/withdraw/replace/update uses the reverse-dependency-index fast
  path and is semantically equivalent to the full evaluation.
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
a multi-threaded server host. Since v0.4.2 `AsyncDisposer::dispose_blocking`
**joins** the cleanup: if another disposer already drained the slot it blocks
until that cleanup finished (previously it returned as soon as `is_disposed()`
reported the slot stolen), so "blocking" really means "all cleanups ran". It
also **retries a cancelled teardown** (the unstarted cleanups restored when a
dispose future was dropped run on the next call; S-1), while a cleanup that calls
`dispose_blocking` on its **own** disposer returns immediately (self-join
short-circuit; S-2 — waiting would deadlock and v0.4.1 returned immediately too).
`AsyncDisposer::dispose` short-circuits its own self-await identically; the
decision uses a per-call-stack *drain frame*, so a plain joiner task on the same
worker thread still blocks until the teardown completes (R2-1).

## 3. Public API surface (Rust)

The main public entry points and types are listed below; the full set of `pub`
items is the crate API documentation. They are stable for the `0.y` line;
`pub(crate)` helpers and private fields are not part of the contract.

Core types:

```rust
pub struct Context;
pub struct Fiber;                 // opaque handle (internals are `pub(crate)`)
pub struct Registry;              // PluginKey → PluginRuntime map
pub struct Events;                // event bus; `Context` forwards to it
pub struct LoggerService;         // ring buffer + exporters
pub struct Logger;                // named facade
pub trait Clock;                  // now_unix_ms() / now_monotonic_ms()
pub struct SystemClock;           // default Clock (wall time + process-monotonic)
pub struct LifecycleEvent { pub fiber_id: u64, pub name: String,
                            pub from: FiberState, pub to: FiberState,
                            pub seq: u64 }
pub type LifecycleListener = Arc<dyn Fn(&LifecycleEvent) + Send + Sync>;
pub struct IdleWait<'a>;          // `Registry::when_idle()` future, re-exported at the crate root
pub struct ServiceKey<T: ?Sized + Send + Sync + 'static>;
pub struct ServiceGate;           // availability gate with dependent refresh
pub type ServiceValue = Arc<dyn Any + Send + Sync>;
pub type AvailabilityCheck = Arc<dyn Fn() -> bool + Send + Sync>;
pub enum FiberState { Pending, Loading, Active, Failed, Unloading, Disposed }
pub enum EventResult { Continue, Bail(Value) }  // Value = serde_json::Value
pub struct EventOptions { pub prepend: bool, pub global: bool, pub once: bool }
pub enum JsonType { String, Number, Bool, Object, Array }
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
already runs a lifecycle operation on the fiber). v0.4.2 adds
`ServiceTypeMismatch(name)` (a `ctx.set`/`set_any` write with the wrong type) and
`Rollback(message)`
(a `Registry::replace` rollback did not complete; the message carries the
original error and the rollback error). v0.4.3 adds
`AccessorWriteRejected(name)` (a computed accessor's setter inspected the write
and returned `false`; distinct from `ServiceWriteForbidden` and
`AccessorReadOnly`). v0.5.0 adds
`Plugin { plugin, kind, message }` (structured plugin failure; construct with
`CordisError::plugin(plugin, kind, message)` and read it back from
`Fiber::error_detail()`); it is propagated unchanged through activation,
`Fiber::await_settled`, restart/update and `Registry::replace`.

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
- lifecycle: `refresh()`, `refresh_async()`, `when_idle()`, `on_lifecycle(listener)`
  (non-fiber-bound broadcast, returns `Disposer`);
- time source: `clock()`, `set_clock(Arc<dyn Clock>)` (only callable from the
  **root fiber's** context; root-derived scopes share the same tree clock slot;
  install once right after `Context::root`, before loading plugins; only new
  observations use it; `ctx.logger()` facades snapshot the clock at creation, the
  raw `LoggerService` uses `SystemClock`);
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
`update(config)` (equivalent to `update_with_no_save(config, false)`),
`update_async(config)`, `update_with_no_save(config, no_save)`,
`update_async_with_no_save(config, no_save)`, `replace_plugin(Arc<dyn Plugin>)`,
`replace_plugin_async(Arc<dyn AsyncPlugin>)`, `cancel`, `is_cancelled`,
`await_settled`, `settled`, `state`, `is_active`, `assert_active`, `context`,
`config`, `raw_config`, `error`, `error_detail`, `get_effects`, `uid`, `id`, `name`,
`parent`; with the **`supervision` feature** also `heartbeat`, `last_activity_ms`,
`idle_for` (L2 activity/watchdog surface, unavailable in the default build).
`impl Registry`: `new`, `size`, `get(key)`, `has(key)`, `delete(key)`,
`keys`, `values`, `entries`, `for_each`, `fibers`, `fiber_count`, `is_idle`,
`when_idle() -> IdleWait<'_>` (the future type is re-exported as
`zeokeel_cordis::IdleWait`), `replace(old_key, new_plugin)`,
`replace_async(old_key, new_plugin)`; with the **`supervision` feature** also
`stalled_fibers(timeout) -> Vec<StalledFiber>`.
`impl PluginRuntime`: `name`, `key`, `fibers`, `fiber_count`, `is_empty`.
`exec`: `block_on`, `boxed`, `set_blocking_executor`, `yield_now`.

## 4. Conformance cases

Every case below is asserted by the Rust test-suite in `tests/`
(`cargo test`). Case IDs are stable: a failing case is a contract
violation, not a test bug. Test names are listed per ID where the mapping is 1:1;
group IDs point at the file that owns the cases.

The v0.4.2/v0.4.3 fixes (no new cases, see §6) are additionally covered by
`tests/regression_core.rs` (lifecycle/registry hardening: FIX-1…FIX-9, FIX-14,
concurrent replace/dispose `D-1`, replace reload panic `D-2`, and the v0.4.3
audit set `r1_*` / `c1_*` / `c2_*` / `c3_*` / `c4_*` / `r3a4_*`), by
`tests/regression_semantics.rs` (injection/config/reflect/waterfall hardening:
FIX-12…FIX-21, async-teardown retry/self-join S-1/S-2, `c4_when_idle_*`), and by
`tests/l2_defenses.rs` (live-resource quota slots, S1). Fixes that need internal
state which the public API cannot construct are asserted by `#[cfg(test)]` unit
tests inside `src/fiber.rs`.

v0.5.0 adds **no CORE case**; its extension APIs and API convergence are covered by
`tests/api_surface.rs` (19 public-API contract cases: clock injection, lifecycle
broadcasts, structured plugin errors, `InternalBus` ownership, logger/meta/effect
surface) plus `tests/property.rs` (deterministic randomized lifecycle state machine)
and `tests/miri_leaks.rs` (leak-checked create/dispose scenario). The new test names
are listed under the affected CORE rows above where they exercise that case.

| ID | Requirement | Test(s) |
| --- | --- | --- |
| CORE-001 | Plugin activates, disposes and cleans up | `lifecycle::effects_run_in_reverse_order_once`, `lifecycle::event_bail_and_owner_disposal`, `api_surface::api_fiber_is_active_tracks_lifecycle_states` |
| CORE-002 | Child contexts inherit provider registrations | `service::service_is_visible_across_sibling_fibers` |
| CORE-003 | `isolate` shadows a service for the child subtree only | `lifecycle::isolate_shadows_parent_service`, `context::isolate_without_label_creates_fresh_scopes` |
| CORE-004 | Duplicate provide in one scope is rejected | `service::duplicate_service_is_rejected`, `service::default_scope_rejects_duplicates_but_isolate_shadows` |
| CORE-005 | Missing `inject` keeps the fiber Pending | `lifecycle::plugin_waits_for_injected_service`, `regression_semantics::fix12b_panicking_inject_is_contained_and_keeps_services_working` |
| CORE-006 | Gate flips activate and unload the dependent fiber | `service::service_check_gates_dependents` |
| CORE-007 | Effects clean up in reverse registration order, exactly once | `async_conformance::sync_and_async_effects_share_reverse_registration_order`, `async_conformance::async_cleanup_runs_in_reverse_order_once`, `api_surface::api_attach_effect_owns_an_external_disposer`, `api_surface::api_effect_future_async_registers_async_cleanups` |
| CORE-008 | A disposer runs exactly once | `async_conformance::async_disposer_is_single_shot` |
| CORE-009 | Fiber dispose awaits async cleanups before returning | `async_conformance::sync_dispose_awaits_async_cleanups_before_returning` |
| CORE-010 | Provider unload unregisters its services | `service::service_auto_register_and_dispose`, `service::provided_service_disposer_removes_service` |
| CORE-011 | Cleanup panic does not skip remaining cleanups (async lane; same policy as CORE-030) | `async_conformance::async_cleanup_panic_does_not_skip_remaining_cleanups` |
| CORE-012 | Effect diagnostics tree reflects registrations (live nodes only since v0.4.3, C3) | `fiber::fiber_reports_effects_and_awaits_settlement`, `fiber::effect_disposer_removes_its_meta_node`, `regression_core::c3_disposed_nested_effect_is_not_reported_in_the_tree` |
| CORE-013 | Availability check gates lookups and dependents | `async_conformance::checked_service_predicate_controls_lookups` |
| CORE-014 | Closed gate does not fall back to an ancestor provider | `async_conformance::closed_gate_does_not_fall_back_to_an_ancestor_provider` |
| CORE-015 | Availability check may re-enter the context | `service::availability_check_may_read_other_services` |
| CORE-016 | Dependency removal unloads an active fiber | `service::service_check_gates_dependents` |
| CORE-017 | Listeners are removed with their fiber | `lifecycle::event_bail_and_owner_disposal`, `async_conformance::async_listeners_unload_with_their_fiber` |
| CORE-018 | `prepend`/`global`/`once` event options | `events::event_options_prepend_and_global`, `events::event_once_fires_exactly_once`, `events::bail_ignores_null_and_false_values` |
| CORE-019 | Context scope filter for events | `events::event_context_filter_matches_scopes`, `events::event_waterfall_respects_context_filter` |
| CORE-020 | Waterfall middleware order and veto semantics (no `Bail` in this lane) | `events::event_waterfall_respects_context_filter`, `async_conformance::waterfall_*` (5 sync-lane cases), `async_conformance::async_waterfall_*` (2 async-lane cases) — 8 cases |
| CORE-021 | Internal hooks (plugin/service/config/get/set/update/listener/dispatch; 9 hooks) | `internal::*`, `api_surface::api_raw_internal_bus_is_not_fiber_owned` (raw `InternalBus` registrations are caller-owned, not fiber-owned) |
| CORE-022 | Registry map API + runtime dedupe/reclaim | `registry::registry_map_api`, `registry::registry_reuses_runtime_for_same_plugin`, `registry::registry_typed_plugin_identity`, `registry::delete_runtime_with_no_fibers_is_rejected_gracefully` |
| CORE-023 | Logger buffer/seq/threshold/exporters/intercept; clock-driven timestamps (v0.5.0) | `logger::*` (6 cases: incl. `logger::invalid_numeric_logger_level_clamps_to_error`, S2), `api_surface::api_logger_is_enabled_follows_the_service_threshold`, `api_surface::api_logger_exporter_count_and_clear_buffer`, `api_surface::api_injectable_clock_drives_log_timestamps` |
| CORE-024 | Config schema validation and merge precedence | `config::*` (5 cases) + `service::service_resolve_config_merges_intercepts` |
| CORE-025 | Accessors, metadata, labeled isolates | `context::context_accessor_and_set`, `context::accessor_is_scoped_to_its_fiber`, `context::accessor_shadows_service_of_same_name`, `context::extend_meta_attaches_metadata`, `context::isolate_labeled_shares_scope`, `service::accessor_setter_rejection_uses_a_dedicated_error`, `api_surface::api_context_meta_insert_any_round_trips` |
| CORE-026 | Fiber diagnostics (`uid`/`parent`/`assert_active`/`await_settled`) | `fiber::fiber_uid_parent_and_assert_active`, `fiber::await_settled_rethrows_startup_errors`, `fiber::await_settled_waits_for_async_effect_cleanup` |
| CORE-027 | Async effect bodies/streams and restart cleanup | `async_effect_body::*` (4 cases), `async_conformance::restart_async_drains_and_reapplies_async_effects`, `lifecycle::restart_reapplies_plugin_and_update_config` |
| CORE-028 | Failed activation rolls back effects and services | `hardening::failed_apply_rolls_back_effects_and_services`, `api_surface::api_structured_plugin_error_constructor_and_display`, `api_surface::api_structured_plugin_error_survives_sync_activation` |
| CORE-029 | `apply` panic is isolated; fiber fails, caller does not panic | `hardening::panicking_apply_is_isolated_and_rolled_back` |
| CORE-030 | Cleanup panic does not skip remaining cleanups (sync + async) | `hardening::cleanup_panic_does_not_skip_remaining_cleanups`, `async_conformance::async_cleanup_panic_does_not_skip_remaining_cleanups` |
| CORE-031 | Listener panic does not abort sync dispatch | `hardening::event_listener_panic_does_not_abort_dispatch` |
| CORE-032 | Unsized `ServiceKey<dyn Trait>` via `*_any` services | `hardening::trait_object_service_keys_work` |
| CORE-033 | Root context/fiber graph has no strong reference cycle | `context::link_cycle_tests::root_context_graph_has_no_strong_cycle` |
| CORE-034 | Sync disposal uses the embedder blocking executor | `executor::sync_disposal_uses_embedder_blocking_executor` |
| CORE-035 | Async plugin activates and disposes through `*_async` APIs | `hardening::async_plugin_activates_and_disposes`, `api_surface::api_structured_plugin_error_survives_async_activation` |
| CORE-036 | Async plugin failure rolls back (awaits async cleanups) | `hardening::async_plugin_failure_rolls_back` |
| CORE-037 | Async plugin `inject` gating re-evaluated by `refresh_async` | `hardening::async_plugin_waits_for_inject_and_refresh_async` |
| CORE-038 | Failed sync activation drains async cleanups during rollback (reverse order across lanes) | `hardening::failed_sync_activation_drains_async_cleanup_inline` |
| CORE-039 | `restart`/`restart_async` re-check the `inject` gate | `hardening::restart_rechecks_inject_gate` |
| CORE-040 | Raw config is re-resolved once per activation (no compounding) | `semantics::raw_config_is_re_resolved_from_raw_on_every_activation` |
| CORE-041 | `ctx.set` is a revertible, classified mutation (dependents reactivate) | `semantics::set_is_revertible_and_classified`, `semantics::provider_version_change_reactivates_dependents` |
| CORE-042 | Provider withdrawal precedes binding removal | `semantics::provider_withdrawal_precedes_binding_removal` |
| CORE-043 | Concurrent async dispose joins the in-flight cleanup | `semantics::concurrent_async_dispose_joins_the_inflight_cleanup` |
| CORE-044 | A disposed fiber is final (no restart/activation resurrection); lifecycle broadcasts report transitions | `redteam::rt12_restart_after_dispose_fails_safely`, `redteam::rt01_sync_dispose_during_apply_must_not_end_active`, `api_surface::api_on_lifecycle_reports_transitions_and_supports_multiple_subscribers`, `api_surface::api_on_lifecycle_contains_listener_panics` |
| CORE-045 | Concurrent lifecycle calls are exclusive (`LifecycleBusy`) | `concurrency::concurrent_dispose_returns_lifecycle_busy`, `concurrency::reentrant_lifecycle_calls_are_allowed`, `redteam::rt13_dispose_during_restart_apply_cannot_leave_a_zombie`, `property::randomized_lifecycle_interactions_preserve_invariants` |
| CORE-046 | Cancelling an async disposal: an `AsyncDisposer` restores unstarted cleanups for the next dispose (S-1); a cancelled fiber unload completes the teardown (sync inline, async deferred) and lands in its target state (A-9); a same-disposer cleanup's `dispose_blocking` **and** its own async `dispose()` return immediately (drain-frame self-join, R2-1), while same-thread independent joiners still wait | `concurrency::cancelling_dispose_async_restores_unstarted_effects`, `concurrency::cancelling_async_disposer_restores_unstarted_cleanups`, `redteam3_attacks::at29_concurrent_dispose_joins_inflight`, `regression_semantics::fix15_cancelled_teardown_is_retried_by_dispose_blocking`, `regression_semantics::fix15_cancelled_dispose_async_is_retried_by_fiber_dispose`, `regression_semantics::fix15_self_join_from_cleanup_returns_instead_of_deadlocking`, `regression_semantics::fix15_self_join_from_fiber_cleanup_returns_instead_of_deadlocking`, `regression_semantics::fix15_self_join_from_cleanup_via_async_dispose_returns`, `regression_core::a2_cancelled_refresh_async_finishes_the_teardown`, `regression_core::a9_cancelled_dispose_async_completes_the_teardown`, `regression_core::f1_retried_root_dispose_async_recovers_parked_cascade`, `regression_core::f1_dispose_all_recovers_parked_cascade`, `regression_core::f1_refresh_async_recovers_parked_cascade`, `regression_core::f4_cancelled_recovery_releases_the_latch_for_the_next_when_idle`, `regression_core::f4_cancelled_refresh_recovery_is_retried_by_when_idle`, `regression_semantics::f1_when_idle_recovers_parked_cascade_after_cancel`, `regression_semantics::f4_cancelled_recovery_does_not_latch_the_next_when_idle` |
| CORE-047 | Child fibers are owned by their parent (cascade + rollback); deep chains unload iteratively and ownership bookkeeping stays bounded | `child_ownership::*` (5 cases), `regression_core::c1_deep_chain_dispose_is_iterative_and_ordered`, `regression_core::c1_three_level_cascade_order_is_dependents_first`, `regression_core::c1_cancelled_deep_cascade_still_completes`, `regression_core::c1_dependent_cleanup_panic_is_contained`, `regression_core::r1_deep_plugin_chain_activates_on_a_2mib_thread`, `regression_core::r3a4_dynamic_plugin_churn_keeps_parent_bookkeeping_bounded`, `regression_core::r3a4_long_churn_keeps_memory_and_effects_bounded`, `regression_core::r3a4_parent_dispose_then_child_dispose_is_idempotent`, `regression_core::r3a4_parent_sweep_races_child_self_dispose`, `regression_core::r3a4_cancelled_child_dispose_still_detaches`, `redteam5_attacks::c16_cancelled_parent_dispose_async_retry_disposes_all_children` |
| CORE-048 | In-place plugin replacement keeps fiber identity and reloads | `replace::*` (6 cases), `api_surface::api_structured_plugin_error_survives_replace` |
| CORE-049 | Quiescence: `when_idle` resolves when no lifecycle work is in flight; it excludes only the caller's own in-flight registrations (v0.4.3) while `is_idle` stays strict, and it is recovery-aware (v0.4.3, F5) — both `Context` and raw `Registry` APIs retry recovery of parked cancelled-cascade frames while waiting; sync `refresh()` deliberately does not recover | `quiescence::fresh_registry_is_idle`, `quiescence::when_idle_waits_for_a_slow_activation`, `quiescence::when_idle_waits_for_an_async_disposal`, `regression_semantics::c4_when_idle_inside_a_registration_frame_does_not_wait_for_itself`, `regression_core::c4_when_idle_inside_registration_frame_does_not_wait_for_itself`, `regression_semantics::fix11_registration_window_keeps_when_idle_blocked`, `regression_core::f5_when_idle_recovers_parked_frames_that_appear_while_waiting`, `regression_core::f5_repeated_cancel_then_when_idle_does_not_hang`, `regression_core::f5_raw_registry_when_idle_recovers_parked_cascade`, `regression_core::f5_when_idle_sees_parked_frames_parked_after_entry`, `regression_semantics::f5_when_idle_waits_for_active_unload_then_recovers_parked_frames`, `regression_semantics::f5_when_idle_recovers_parked_frames_that_appear_while_waiting`, `regression_semantics::f5_twenty_cancel_and_when_idle_rounds_do_not_hang` |
| CORE-050 | Cooperative cancellation via `checkpoint` | `quiescence::checkpoint_observes_cooperative_cancellation` |
| CORE-051 | `dispose_all` shutdown path reclaims the whole tree (leak-clean under Miri) | `child_ownership::dispose_all_reclaims_the_whole_tree`, `miri_leaks::contexts_and_plugins_do_not_leak` |

The optional `supervision` feature is not part of the CORE cases; its quota
semantics (live-resource accounting, S1) are asserted by
`tests/l2_defenses.rs` (`disposed_effects_free_the_effect_quota_slot`,
`disposed_listeners_free_the_listener_quota_slot`,
`once_listener_frees_its_quota_slot_when_it_auto_removes`,
`listener_quota_release_is_idempotent_and_concurrency_safe`,
`disposed_child_fibers_free_the_child_quota_slot`). The Q11 change (v0.5.0:
`max_effects` counts **user** effects only) is asserted by
`tests/l2_defenses.rs` (`max_effects_one_still_loads_a_plugin`,
`user_effects_consume_the_max_effects_quota`,
`disposed_user_effect_frees_the_quota_slot_under_max_effects_one`,
`l2_activity_uses_the_injected_clock`).

The v0.4.3 audit fixes add **no CORE ID**; they are assertions about the cases
above, and their regressions are: `regression_core::c1_*` / `r1_*` / `r3a4_*`
(CORE-047, iterative deep-chain teardown and bounded child bookkeeping);
`regression_core::c4_when_idle_inside_registration_frame_does_not_wait_for_itself`
and `regression_semantics::c4_when_idle_inside_a_registration_frame_does_not_wait_for_itself`
(CORE-049, self-registration exclusion);
`regression_core::c3_disposed_nested_effect_is_not_reported_in_the_tree`
(CORE-012, live effect diagnostics); `regression_core::c2_panicking_key_degrades_to_a_placeholder_runtime`
and `redteam::rt21_fiber_name_does_not_call_user_code_under_the_lock` (§2.3,
Plugin metadata containment); `service::accessor_setter_rejection_uses_a_dedicated_error`
(CORE-025, `AccessorWriteRejected`); `logger::invalid_numeric_logger_level_clamps_to_error`
(CORE-023, clamped levels).

## 5. Deliberate differences from the JavaScript Cordis reference

1. **No Proxy-based property reads.** Services live behind explicit
   `ServiceKey<T>` lookups (`ctx.get(KEY)`) rather than `ctx.foo` proxies; Rust has
   no runtime property interception.
2. **No callable services / `Service` class inheritance / `ctx.mixin`.** These are
   JS language features; Rust expresses the same intent with typed traits and
   `.extend()`/`.isolate()`.
3. **Internal events are typed hooks**, not JSON strings (`src/internal.rs`), so
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
- Embedders should pin a tag (`tag = "v0.5.0"`) and update deliberately.
- v0.5.0 is a **minor release**: no CORE case was added, renumbered or removed
  (still 51 cases). It converges the public API — internal types
  `FiberRef`, `FiberData`, `PluginKind`, `IdleOrRecoverableWait` and
  `Registry::counter()` become `pub(crate)`;
  `Fiber::last_activity() -> Instant` is replaced by `last_activity_ms() -> u64`;
  Q11: `max_effects` counts user effects only — and adds three general extension
  APIs: injectable `Clock`, `LifecycleEvent`/`Context::on_lifecycle` and
  structured `CordisError::Plugin` + `Fiber::error_detail`. Performance: full
  reverse-dependency index (S1–S4; internal only, no semantic change). See
  `CHANGELOG.md` for the migration list and
  [`docs/KERNEL-BOUNDARY.md`](../docs/KERNEL-BOUNDARY.md) §8 for the public-API
  policy.
- v0.4.2 is a **patch release**: no CORE case was added, renumbered or removed.
  It fixes defects in the implementation of existing cases and makes previously
  unstated edges explicit. Behavioral deltas are listed in `CHANGELOG.md`
  (compatibility notes) and summarised here:
  - refresh and unload containment (CORE-049, CORE-030): the refresh latch is
    RAII-guarded; cleanup panics raised by refresh/retire driven unloads are
    logged and contained; a parent sweep contains per-child panics and re-raises
    only after its own unload;
  - pending-dispose finalisation on every lifecycle exit path and activation
    epoch tagging (CORE-044/045/047);
  - `restart` emits `Active → Unloading` through `internal/status`
    (CORE-021, §2.3);
  - complete `Registry::replace` rollback plus `CordisError::Rollback`, and
    lifecycle exclusivity for `Fiber::replace_plugin` (CORE-045/048); a panic
    raised by the reload is contained and reported as
    `CordisError::PluginFailed` before the rollback runs (D-2);
  - `dispose_all` panic priority over collected errors (CORE-051);
  - `is_idle`/`when_idle` cover the registration→activation window,
    including the `internal/plugin` create hook (CORE-049); in 0.4.2 a create
    hook waiting synchronously on `when_idle()` therefore waited on itself
    (v0.4.3 fixes this by excluding the caller's own registrations, see below);
  - strict provider-`Active` injection gate and read-lane/strict-lane split
    (CORE-005/006/016);
  - type-checked `ctx.set`/`set_any` with `CordisError::ServiceTypeMismatch`
    (CORE-041, DESIGN I5);
  - `update` ordering/veto semantics plus the additive `update_with_no_save` /
    `update_async_with_no_save` entries (existing `update` == `no_save = false`)
    and the persistence hint they forward to hooks (CORE-040);
  - `AsyncDisposer::dispose_blocking` joins the in-flight cleanup, retries a
    cancelled teardown's restored cleanups, and returns immediately when called
    on its own disposer from within a cleanup (self-join; S-1/S-2, §2.8);
  - a cancelled **fiber unload** (`dispose_async`/`refresh_async` future
    dropped) completes the teardown instead of restoring the old state: sync
    cleanups run inline, async cleanups are deferred to the next dispose/drain,
    and the fiber lands in its target state (`Pending`/`Disposed`) (A-9, §2.3);
  - async waterfall middleware panics re-raise (fail-closed) like the sync lane
    (CORE-020);
  - `null` `JsonConfigSchema` defaults are injected without declaring a type
    (CORE-024);
  - contained panicking accessors degrade to `None` (CORE-025, §2.7);
  - `MAX_PLUGIN_DEPTH` counted along the fiber parent chain (CORE-047);
  - one fiber per `ctx.inject` call, keyed by callback type (§2.6).
- v0.4.3 is a **patch release**: no CORE case was added, renumbered or removed.
  It fixes audit findings and documents the resulting behavior. Deltas
  (compatibility notes in `CHANGELOG.md`):
  - **deep dependency-chain teardown is iterative** (R3-A1, CORE-047): a
    two-phase order-preserving cascade (begin provider-before-dependent, drain
    in reverse; ownership descendants via explicit DFS stack) keeps the call
    stack O(1) in chain depth — on a 2 MiB thread depth 800 passes in 0.4.3
    debug and release (0.4.2 comparison: debug ≈480 levels → abort, release
    ≈2200); the former "very deep chains can overflow the stack" limitation is
    gone, and `MAX_PLUGIN_DEPTH = 256` still bounds loading only;
  - **dynamic child bookkeeping is bounded** (R3-A4, CORE-047): a disposing
    child detaches its ownership effect from the parent (plus effect-node /
    weak-list entries) and parent effect lists amortize away disposed ownership
    entries, so churn cannot grow the parent unboundedly;
  - **`when_idle` excludes the caller's own registrations** (R3-A3/C4,
    CORE-049): a create hook can await it without self-deadlock; `is_idle()`
    stays strict; cross-thread cyclic registration dependencies remain a known
    deadlock boundary;
  - **Plugin metadata callbacks are contained** (C2, §2.3): panics in
    `name`/`key`/`inject`/`config_schema` are logged and degraded
    (`"<panicked plugin>"` / empty inject / no schema), never escaping the
    `Result` APIs;
  - **nested-effect diagnostics are filtered** (C3, §2.3 diagnostics);
  - **supervision quotas count live resources** (S1, §2.6): effect/listener/
    child slots released by unload, `once` auto-removal or concurrent dispose
    are immediately reusable;
  - **accessor write rejection has its own error** (S3, §2.2/§2.7):
    `CordisError::AccessorWriteRejected`;
  - **invalid numeric logger levels clamp to `Error`** (S2, §2.7).
- v0.3.0 added CORE-040…CORE-050: raw-config retention and one-shot resolution,
  provider-identity epochs and withdrawal ordering, revertible/classified
  `ctx.set`, async-disposer joining, lifecycle exclusivity, cancel-safe disposal,
  child-fiber ownership, in-place plugin replacement, quiescence, and cooperative
  cancellation.
- v0.2.0 added CORE-028…CORE-039 and changed failure/cleanup semantics
  (rollback + panic isolation); it is a behavioral change and therefore a minor
  bump per the rule above.
