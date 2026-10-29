# Cordis-Rust port notes

Companion to `spec/CORDIS-CORE.md`. This file records how the Rust port relates to
the JavaScript Cordis reference, what the Rust-only extensions add, which failures
the kernel contains, and what the crate deliberately does not support. It is
descriptive; the specification stays normative. Baseline: crate version `0.5.0`
(tag `v0.5.0`).

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
  for thresholds; the facade exposes `error`/`warn`/`info`/`debug`. Since v0.4.3 an
  **invalid numeric level** in config (e.g. `{"level": 99}`) is clamped to the
  safe end (`Error`) instead of failing open to the most verbose `Debug` (S2;
  string levels are unchanged).
- **Two listener lanes.** A Rust closure has one fixed return type, so sync
  listeners (`on`/`on_with`/`once`) are dispatched by `emit`/`bail`/`waterfall`
  and async listeners (`on_async`/`on_async_with`) by `parallel_async` /
  `serial_async` / `waterfall_async`. Sync dispatch never polls async listeners.
  Listeners return `EventResult` instead of raising exceptions, and events carry
  `serde_json::Value` payloads — there is no typed event map and no
  `AggregateError` channel. Async registrations (`on_async*`) bypass the
  `internal/listener` hook that sync registrations pass through, and the async
  lane has no `once`/`global` options (XT-06).
- **`inject` is a name list** (`&'static [&'static str]`), not a name → intercept
  config map.
- **Event scope filtering is isolate-map equality**, not a subtree walk: a listener
  receives an event only when the dispatching context's isolation map equals its
  own, unless the listener is `global`.
- **No dynamic plugin loading / stable ABI** (see §5).
- **Structured plugin errors, lifecycle broadcasts and an injectable clock are
  Rust-only extension points (v0.5.0).** The TS reference handles failures by
  throwing (no typed `plugin/kind/message` payload), has no generic lifecycle
  broadcast, and time is read directly (no `Clock` seam). The Rust kernel keeps
  those as explicit APIs so hosts can classify failures, observe transitions and
  make time deterministic — all optional, none required by a plugin.

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
  directly and ignores the installed executor. Since v0.4.2
  `AsyncDisposer::dispose_blocking` really **joins** the cleanup: when another
  disposer already took the slot it blocks until that cleanup finished instead of
  returning early (0.4.1 checked `is_disposed()` and returned). It also
  **retries a cancelled teardown** — the unstarted cleanups restored when a
  `dispose`/`dispose_async` future was dropped run on the next
  `dispose_blocking()`/`dispose()` (S-1) — while a cleanup that calls
  `dispose_blocking` on its **own** disposer returns immediately (self-join
  short-circuit, S-2: waiting for the teardown it is part of could never finish,
  and 0.4.1 returned immediately there too).
- **Unsized services.** `ServiceKey<T: ?Sized>` admits `ServiceKey<dyn Trait>`;
  the unsized lane is `provide_any` / `get_any` / `require_any` / `set_any`, and
  mixing the two lanes for one key returns `None` on the mismatched lane.

### 2.2 Added in v0.3.0 (CORE-040…CORE-050)

- **Raw config, resolved once per activation (CORE-040, ordering clarified in
  v0.4.2).** The fiber retains the caller-supplied config
  (`Fiber::raw_config`) and resolves it through `internal/config` + schema once
  per activation; `Fiber::config` is the resolved value. Restart/update therefore
  cannot compound derived values. `update` runs in the order
  `internal/config` → schema validation → `internal/update`; only the terminal
  of `internal/update` stores a raw config and restarts (at most once). An
  invalid candidate returns `Validation` and changes nothing; a hook that returns
  a different config has the replacement validated before it becomes the new raw
  config; a pass-through keeps the caller's raw config (never the resolved one);
  a hook that never calls `next` vetoes the update (`Ok(())`, no restart, raw
  config unchanged). The `internal/update` hook chain also receives a `no_save`
  hint (Cordis `update(config, noSave)`): it is forwarded to persistence hooks
  and never suppresses the kernel-side raw-config commit — a successful update
  always keeps the new raw config. The additive entries
  `Fiber::update_with_no_save(config, no_save)` /
  `update_async_with_no_save(config, no_save)` expose the hint publicly;
  `Fiber::update` / `update_async` are exactly the `no_save = false` forms.
- **Provider identity epochs and withdrawal order (CORE-041, CORE-042).** Service
  entries carry a monotonic version (a provider swap bumps it; the dependent
  reactivation is asserted under CORE-041), and unloading a provider marks its
  services withdrawn before draining, so dependents deactivate while the binding is
  still present and removal follows (CORE-042). Since v0.4.2 the inject gate is
  also **provider-`Active`-strict** (see §2.3).
- **Revertible, classified `ctx.set` (CORE-041, type-checked in v0.4.2).** A set
  captures the previous value and registers an effect owned by the providing fiber
  (restored in reverse order on unload) and notifies dependents of the version
  bump. `ctx.set_revertible` exposes the inverse's `Disposer`; a hook that vetoes
  the write leaves the store untouched. `set`/`set_any` must match the registered
  entry's type (the terminal write re-checks as defence in depth), otherwise
  `CordisError::ServiceTypeMismatch(name)` is returned and the store is unchanged;
  an `internal/set` hook can veto a write but cannot substitute a value. A
  **computed accessor's setter returning `false`** (the owner inspected the value
  and rejected it) reports the dedicated `CordisError::AccessorWriteRejected(name)`
  since v0.4.3 (S3), distinct from `ServiceWriteForbidden` (another fiber owns the
  service) and `AccessorReadOnly` (no setter at all).
- **Async-disposer joining (CORE-043).** A second concurrent async disposal joins
  the in-flight cleanup instead of racing it.
- **Lifecycle exclusivity and "disposed is final" (CORE-044, CORE-045).**
  `dispose`/`restart`/`update` (sync and async) claim a per-fiber slot; another
  thread gets `CordisError::LifecycleBusy`, while a reentrant call on the owning
  thread (a cleanup disposing its own fiber, `update` calling `restart`) is
  allowed. A disposed fiber cannot be restarted or resurrected; a reentrant
  dispose during activation rolls the activation back.
- **Cancel-safe disposal (CORE-046), two granularities.** Dropping an
  `AsyncDisposer::dispose` future mid-await restores the cleanups that had not
  started; the next `dispose()`/`dispose_blocking()` retries them in the same
  reverse order (S-1), and at most the single in-flight cleanup future is lost
  (logged when other cleanups were restored; a fully consumed list loses that
  last future silently). Dropping a **fiber** unload future (`dispose_async` /
  `refresh_async`) instead *completes* the teardown as of v0.4.2 (A-9): sync
  cleanups run immediately, async cleanups are parked in the fiber's deferred
  list and finished by the next dispose/drain, and the fiber lands in its target
  state (`Pending` for a reload/retire unload such as `restart_async`/refresh,
  `Disposed` for a dispose) with its event owner removed and quiescence notified — never a
  permanent `Unloading` or a fake-`Active` half-torn-down fiber.
- **Child-fiber ownership (CORE-047).** `ctx.plugin` registers the child fiber as
  an effect of the caller's fiber (a weak child list plus an ownership effect), so
  unloading the parent disposes the subtree in reverse order and a failed parent
  activation rolls its children back. A child that is busy while the parent unloads
  flag-disposes itself when its operation finishes, and a cancelled cascade is
  retried by the next dispose. Loading depth is bounded
  (`MAX_PLUGIN_DEPTH = 256`) so recursive loading fails with
  `CordisError::InvalidPlugin` instead of overflowing the stack; v0.4.2 counts
  that bound along the fiber parent chain, not the context chain (a root-level
  plugin starts at depth 1 and about 254 nested loads below it are accepted).
- **In-place replacement (CORE-048).** `Registry::replace` / `replace_async`
  re-bind every fiber of a runtime to a new implementation, keeping fiber
  identity/uid and raw config, then reload through the normal activation pipeline;
  `Fiber::replace_plugin` / `replace_plugin_async` do the same for one fiber. A
  reload rejected as busy rolls the re-binding and runtime move back; v0.4.2 also
  reloads the already-switched fibers back to the old plugin and
  reports `CordisError::Rollback` if that rollback fails.
- **Quiescence (CORE-049).** `Registry::is_idle` reports whether any lifecycle
  operation is in flight (no `Loading`/`Unloading` fiber, `pending_ops == 0`), and
  `Registry::when_idle()` returns the crate-root `zeokeel_cordis::IdleWait<'_>`
  future (`Context::when_idle()` is its async convenience wrapper); both resolve
  once the tree is quiescent. The whole registration→activation window of `ctx.plugin*` (including the
  `internal/plugin` create hook) counts as in-flight. Since **v0.4.3** `when_idle`
  excludes the calling thread's **own** in-flight registrations (R3-A3), so a
  create hook can wait for the rest of the tree without deadlocking; `is_idle`
  stays strict and other threads' waiters keep the FIX-11b guarantee (§2.3).
- **Cooperative cancellation (CORE-050).** `Fiber::cancel` sets a flag and
  `Context::checkpoint` returns `Err(CordisError::Cancelled)` once set, letting
  plugin work unwind through the normal failure/rollback path. The kernel never
  forcibly interrupts plugin code.

### 2.3 Patched in v0.4.2 / v0.4.3 (no new CORE cases)

v0.4.2 and v0.4.3 are hardening releases: they fix the implementation against
existing contracts and make the previously unstated edges below explicit. See
`CHANGELOG.md` for the compatibility notes.

- **Refresh latch is RAII-guarded.** A panic that escapes one `refresh` pass can
  no longer leave the dependent-refresh latch set forever (0.4.1: one panicking
  cleanup permanently disabled reactive re-activation). The latch is also
  released when a refresh fails partway and sets the dirty flag again.
- **Contained unloads on refresh/retire paths.** Cleanup panics raised while
  `refresh_all`/`refresh_all_async`/`retire_unready` unload dependents are logged
  and contained, so a provider cannot be left stuck in `Unloading` (and
  `when_idle` cannot block forever). A **direct** `Fiber::dispose` keeps the
  CORE-030 contract: drain fully, then re-raise the first panic.
- **Per-child sweep containment.** Disposing a parent sweeps children one by one;
  a child panic is captured, the sweep continues with the remaining children, and
  the first payload is re-raised only after the parent reached its target state.
  Activation-failure rollback and stale-activation discard only log sweep panics.
- **No orphaned flagged children.** `restart`/`restart_async`/`update`/
  `update_async` finalise a parent-requested dispose on **every** exit path
  (early `Err`, `?` and panic), so a flagged child cannot stay `Active`.
- **Activation epochs are exact.** Effects are tagged with the epoch of the
  activation attempt that registered them (thread-local frame; carried across
  executor migration for async activation), so a concurrent restart discards
  exactly the stale attempt's tail.
- **`restart` emits status events.** `restart`/`restart_async` transition through
  the normal helper, so `internal/status` sees `Active → Unloading` (no raw state
  write, no sourceless `Unloading → Pending`).
- **`Registry::replace` rolls back completely** (every already-switched fiber is
  reloaded with the old plugin) and returns
  `CordisError::Rollback` when the rollback itself fails. `Fiber::replace_plugin`
  claims the lifecycle slot for its swap/re-bind prefix and cleans up the
  temporary runtime of the root-fiber special case.
- **Replace/reload panics become errors (v0.4.2).** The reload drains the old
  implementation's cleanups (embedder code), and a cleanup panic used to unwind
  out of the `Result` API after the runtime had already been re-keyed. Both
  `Registry::replace`/`replace_async` and
  `Fiber::replace_plugin`/`replace_plugin_async` now wrap the reload in
  `catch_unwind` and report
  `Err(CordisError::PluginFailed("<op>: reload panicked: …"))`, then roll back
  (the registry path reloads already-reloaded fibers with the old
  implementation; the single-fiber path restores the old plugin/runtime). Only a
  failing rollback on the registry path yields `CordisError::Rollback`. Callers
  never receive the panic (observable change vs 0.4.1).
- **`refresh` is a bounded fixed point.** `Context::refresh`/`refresh_async`
  run at most **64 passes**, each advancing in the registry's snapshot order.
  A forward-ordered dependency chain (provider registered before its consumer)
  settles in one pass; a reverse-ordered chain advances about one level per
  pass, so a chain deeper than the pass budget may need repeated `refresh()`
  calls before it is fully settled.
- **`dispose_all` keeps the panic.** When a cleanup panics and another fiber also
  returns an error, the panic wins: the error is logged and the first panic
  payload is re-raised after every fiber was attempted.
- **Event owner is released on drop.** A `FiberData` dropped without an explicit
  dispose removes its event owner entry as a last resort.
- **`is_idle`/`when_idle` cover the registration→activation window (R3-A3/C4).**
  A registry-level registration guard spans the whole `ctx.plugin*` call — through
  the `internal/plugin` create hook and the activation that follows — so the tree
  is not idle while a plugin is being created/activated (`is_idle()` is strict:
  it excludes nothing, v0.4.2 behavior kept). Since v0.4.3 `when_idle()` records
  which **thread created** each in-flight registration and subtracts the caller's
  own registrations: a hook can `await when_idle()` without waiting for its own
  registration (returns in ~4 ms in the regression test), while external waiters
  keep the FIX-11b guarantee. **Remaining boundary:** two threads whose
  registrations mutually await each other's window can still deadlock — the
  kernel does not detect cyclic registration dependencies. This thread-exclusion
  rule is a Rust-side design choice (the TS reference has no equivalent
  registration guard); `is_idle()` stays strict for supervisors.
- **Plugin metadata callbacks are contained (C2, v0.4.3).** `Plugin::name` /
  `::key` / `::inject` / `::config_schema` (and `AsyncPlugin` counterparts) are
  embedder code called during creation, replacement, restart, diagnostics and
  dependency resolution. A panic no longer escapes `ctx.plugin`/replace or poisons
  kernel locks: it is logged and degrades deterministically — `name()` →
  `"<panicked plugin>"`, `key()` → `Named("<panicked plugin>")` (such plugins
  share one runtime), `inject()` → `&[]` (no dependency gating),
  `config_schema()` → `None` (raw config accepted, no validation). This is a
  Rust kernel contract: the TS reference would propagate the exception.
- **Companion boundaries of the registration window.** Inside the
  `internal/plugin` create hook the new fiber is still `Pending` and has no
  in-flight fiber-level operation, so `fiber.await_settled()` resolves
  immediately (`Ok(())`), and `Registry::stalled_fibers()`/`Context::stalled_fibers()`
  do not report the registration window (they inspect fiber-level `pending_ops`
  and `Loading`/`Unloading` states only). `when_idle()` is the right primitive
  there (v0.4.3 excludes the caller's own registration, see above).
- **Deep dependency chains unload iteratively (R3-A1, v0.4.3).** Unloading no
  longer recurses through `take_effects → retire_unready → unload`: a two-phase,
  order-preserving cascade *begins* every transitively-unready fiber
  provider-before-dependent (marks `Unloading`, withdraws services, takes
  effects) and then drains the frames in reverse order (an equivalent
  dependents-before-provider cleanup order). Ownership descendants are collected
  with an explicit DFS stack, so the call stack is O(1) in the chain depth: on a
  2 MiB thread depth 800 passes in **both debug and release** builds in 0.4.3
  (0.4.2 comparison: debug aborted at ≈480 levels, release at ≈2200; the actual
  `MAX_PLUGIN_DEPTH = 256` load limit is far below either). The cascade has a
  `fiber_count + 2` fixed-point bound; leftovers
  stay `Active` and can be retried by a later dispose/refresh. This removes the
  former "deep dependency-chain teardown can overflow the stack" limitation.
- **Dynamic child bookkeeping is bounded (R3-A4, v0.4.3).** A child fiber
  disposing itself detaches its `ctx.plugin` ownership effect from the parent's
  effect list (and its effect-node/weak-list entries), and a parent effect list
  that grows past a threshold amortizes away disposed ownership effects. A 60k
  churn used to accumulate 60001 effects / +54.7 MB on the parent; 10k churn now
  peaks at one ownership effect and RSS stays essentially flat (+≈1 MB measured,
  +996 KB at 10k churn).
- **Nested effect metadata is filtered (C3, v0.4.3).** `Fiber::get_effects()`
  snapshots the live tree: a nested effect disposed by the user no longer leaves
  a ghost node under its parent's `EffectMeta`.
- **Supervision quotas count live resources (S1, v0.4.3).** With the
  `supervision` feature the per-fiber `FiberQuota` counts *live* effects (dead
  entries are pruned when pushing), listeners (one-shot `ListenerSlot` released
  exactly once by whichever path comes first: user disposer, `once`
  auto-removal, or fiber unload), and child fibers (live weak handles). Released
  slots are reusable immediately; exceeding a limit returns
  `CordisError::QuotaExceeded` and rolls the activation back. Anti-starvation:
  **since v0.5.0 (Q11) `max_effects` counts user effects only** — internal kernel
  effects (root logger provide, dynamic child ownership effects) are excluded, so
  `max_effects(1)` still loads plugins and only limits `ctx.effect*` callers.
- **Strict injection gate.** `inject` and `ctx.service_available` require the
  provider to be `Active`; `ctx.get`/`get_named`/`get_any` stay non-strict (TS
  property-read semantics). Accessors and `internal/get` virtual services only
  serve the read lane and never satisfy `inject`. A dependent therefore never
  activates against a provider whose `apply` has not returned, and a provider that
  provided and then failed cannot leave an already-applied consumer.
- **Type-checked `ctx.set`** (`ServiceTypeMismatch`) — see CORE-041 above.
- **`update` ordering/veto, and the additive `update_with_no_save` entries** —
  see CORE-040 above.
- **`dispose_blocking`: join, cancel-retry, self-join short-circuit (v0.4.2)**
  — see §2.1. It blocks until an in-flight teardown finishes, retries the
  unstarted cleanups restored by a **cancelled** `AsyncDisposer` teardown on the
  next `dispose_blocking()`/`dispose()` (S-1; 0.4.2's first FIX-15
  implementation early-returned and lost them), and returns immediately when
  called on the same disposer from inside its own cleanup (S-2, deadlock-free
  and 0.4.1-compatible).
- **Cancelled fiber unloads complete (A-9).** Dropping a `dispose_async` /
  `refresh_async` future no longer restores the pre-unload state with a partially
  drained effect list (which could leave a permanently `Unloading` fiber or a
  fake-`Active` half-torn-down one in 0.4.1/early-0.4.2). The cancellation hook
  now finishes the teardown: sync cleanups run immediately, async cleanups are
  deferred to the fiber (`deferred_async`) and completed by the next
  dispose/drain, `finish_unload` reaches the target state (`Pending`/`Disposed`),
  and the event owner is removed plus quiescence notified. The in-flight
  cleanup future itself is still lost (Rust has no cancel-safe await).
- **Cancelled cascades are recovered by recovery-aware entry points (F1/F4/F5,
  v0.4.3).** Cancelling an async unload cascade (`dispose_async`/`refresh_async`
  future dropped) can temporarily park non-root dependents in `Unloading`.
  - **F1:** `when_idle()`, `refresh_async()`, `dispose_all()` and a retried root
    `dispose()`/`dispose_async()` recover parked frames to the **original
    cascade target** (`Pending` for dependency dependents, `Disposed` for
    ownership children), with unstarted deferred async cleanups running exactly
    once. A cancel that lands while an async cleanup is already executing still
    loses that in-flight future (the A-9 boundary) but never leaves a permanent
    `Unloading`; `is_idle()` stays strict (`false`) until recovery runs.
  - **F4:** the single-recoverer latch is an **RAII guard** (cancel/panic cannot
    leave it set and disable later recovery), and the batch of taken frames is
    owned by a **`ReparkGuard`/`RecoveryBatch`**: a recovery cancelled mid-batch
    re-parks every unprocessed/failed frame instead of losing it.
  - **S7 (v0.4.4):** a recovery cancelled while finishing the cascade **root**
    can leave the fiber on its target state (`Pending`) with
    `CancelSafeDrain`-restored cleanups in `deferred_async`. `ReparkGuard` now
    also re-parks frames that are `Unloading` **or** non-`Disposed` with a
    non-empty deferred list, `take_parked_unloads` re-admits them, and
    `when_idle()` returns only when idle **and** no parked unloads remain — the
    restored cleanup runs exactly once before it returns (0.4.3 could report
    idle early and defer them to an explicit dispose).
  - **F5:** `when_idle()` is recovery-aware in **both** `Context::when_idle()`
    and the raw public `Registry::when_idle()`: the wait loop retries recovery
    whenever parked frames are recoverable (woken on idle or parked-recoverable,
    bounded with a fallback), so frames parked *while waiting* cannot hang it.
    **Sync `refresh()` does not recover parked frames** — it can be called from
    a reactor thread and draining reactor-backed cleanups synchronously would
    deadlock; recovery belongs to the awaitable/blocking entries above.
  - Recovery is idempotent, deduplicated and bounded (by concurrently parked
    frames), and deferred to the next root operation while a cancellation unwind
    or another unload is in progress. This is the Rust-side completion contract
    for a cancelled cascade; the TS reference has no equivalent parking state.
- **Async waterfall panics re-raise.** `waterfall_async` no longer swallows a
  middleware panic into `Value::Null`; both lanes abort the chain, log, and
  re-raise (fail-closed).
- **`null` config defaults model optional fields.** `default_value(name,
  Value::Null)` never declares a type (0.4.1 inferred `Object`, so the injected
  `null` then failed validation), and its injection depends on the declared kind:
  untyped keys are injected as `null` (value not type-checked); a key with an
  explicit `property(name, JsonType::…)` keeps the null default **absent** by
  default, so the field is optional, a provided value is type-checked as
  declared, and `required(name)` demands an explicit value. `JsonType` keeps its
  five variants — the patch release stays source-compatible for exhaustive
  matches.
- **Accessor panics degrade to absent.** A panicking accessor getter is contained
  and the read yields `None` (`get`/`get_any`/`get_named`), not a placeholder
  value.
- **`MAX_PLUGIN_DEPTH` counts fibers.** The depth limit walks the fiber parent
  chain (root fiber counts as one level, so a root-level plugin starts at depth
  1 and roughly 254 nested loads below it are accepted), not the context chain:
  300 nested `intercept()`/`extend_meta()` scopes no longer exhaust it, while a
  genuinely recursive `plugin()` chain still fails with `InvalidPlugin`.
- **`ctx.inject` has per-call identity.** Each `inject` creates its own fiber keyed
  by the type id of its callback, so `registry.size()` counts them individually
  and `Registry::replace(Named("injected"))` no longer re-binds all of them;
  `Fiber::name()` still reports `"injected"`.
- **Cancelled async activation reclaims its children (A-10).** If an in-flight
  `plugin_async` future is dropped, the activation rollback now releases the
  event owner and sweeps the child fibers created by that attempt, so a `Failed`
  parent does not keep `Active` children while `is_idle()` reports quiet. On an
  unwinding path (the activation itself panicked) children are only flagged for
  disposal and reclaimed by the next `dispose`/`dispose_all`; the normal
  cancellation path sweeps them immediately.
- **A poisoned provider lock keeps the service injectable (B-3).** `Plugin::inject`
  is evaluated before the fiber lock is taken (an embedder `inject` panic can no
  longer poison it), and the strict injection gate treats a poisoned provider lock
  as available — the same lenient rule as a dropped provider weak. Real
  `Loading`/`Failed` providers are still gated (FIX-12 unchanged).
- **Schema authoring errors surface clearly (B-4').** A non-null
  `default_value` that contradicts an explicitly declared `property` type is
  reported by `validate` as
  `default value for \`name\` must be <kind>, got <kind>` and the default is not
  injected (previously the injected value failed the field check with a
  misleading message). `merge` performs no authoring validation and still
  materializes defaults as-is.
- **Concurrent `Disposed` fibers are skipped during replace rollback (A-6).** A
  failed `Registry::replace` no longer reports a spurious
  `Rollback("… fiber already disposed …")` for a fiber that a concurrent
  `dispose` already removed; the rollback heals and continues.

### 2.4 Added in v0.5.0 (API convergence + extensions)

- **Reverse-dependency index (S1–S4).** Internal only: the kernel keeps a
  provider → dependent index and reuses it in refresh/unload/cascade paths instead
  of scanning all fibers. An **explicit** `refresh()`/`refresh_async()` still
  re-evaluates the whole registry snapshot, including availability-gate flips;
  internal `refresh_dependents` (after provide/withdraw/replace/update) takes the
  reverse-index fast path and is semantically equivalent. No public API or
  semantic change. Measured vs 0.4.x:
  fanout 5k `create+dispose` −54% / `dispose` −74%, 1500-level deep-chain cascade
  −97% / `dispose` −90%, fanout 1k `create+dispose` −50% (see CHANGELOG).
- **Injectable clock.** `Clock` (`now_unix_ms` / `now_monotonic_ms`) with default
  `SystemClock`; `Context::clock()` reads the tree clock. `Context::set_clock` can
  only be called from a context whose fiber is the **root fiber** (root-derived
  scopes share the same tree clock slot); install it once, right after
  `Context::root` and before loading plugins — **do not swap the clock at
  runtime**. Only new observations use it. `ctx.logger()` facades snapshot the
  clock at creation, so a facade obtained before the swap keeps the old clock:
  re-acquire the facade after changing it. The raw `LoggerService` API always
  timestamps with `SystemClock`; L2 activity/`idle_for` (**`supervision` feature**
  only) use the tree clock.
- **Lifecycle broadcast.** `LifecycleEvent { fiber_id, name, from, to }`,
  `LifecycleListener`, `Context::on_lifecycle` (thin wrapper over the
  `internal/status` hook): one event per state transition, multiple subscribers,
  listener panics contained. The subscription is **not fiber-owned**; the
  returned `Disposer` is its single owner — **dropping the `Disposer`
  unsubscribes just like `dispose`** (safety net). To keep observing across the
  lifetime of the installing fiber, store the `Disposer` and dispose it
  explicitly when done. Designed for embedder metrics/shutdown coordination.
- **Structured plugin errors.** `CordisError::Plugin { plugin, kind, message }`
  (+ `CordisError::plugin(...)`) lets plugins return classified failures
  (`"config"`/`"network"`/…); propagated unchanged through activation,
  `Fiber::await_settled`, restart/update and `Registry::replace`.
  `Fiber::error_detail()` exposes the retained structured error.
- **Public API convergence (breaking, migration in CHANGELOG).** Internal state
  types `fiber::FiberRef`, `fiber::FiberData`, `registry::PluginKind`,
  `registry::IdleOrRecoverableWait` and `Registry::counter()` became `pub(crate)`;
  `Fiber::last_activity() -> Instant` was replaced by `last_activity_ms() -> u64`
  (**`supervision` feature** only, like `Fiber::{heartbeat,idle_for}` and
  `Registry::stalled_fibers`). Use `Fiber`/`Context`/`Registry`/`PluginRuntime`
  methods and program against the `Plugin`/`AsyncPlugin` traits.
- **`InternalBus` ownership is explicit.** `Events::internal()`/`InternalBus` is an
  advanced surface: callbacks are caller-owned and **not** removed when a fiber
  unloads; use `Context::on_internal_*` for fiber-owned hooks and
  `Context::on_lifecycle` for a non-fiber-bound status observer.
- **`Ok` from `restart`/`update`/`replace_plugin` does not mean `Active`.** Those
  methods contain a post-operation reload/activation failure: the call returns
  `Ok(())` while the fiber ends `Failed`, with the (optionally structured) error
  retained in `Fiber::error_detail()` (`Fiber::error()` keeps the display
  string). Check `Fiber::state()`/`error_detail()` rather than only the return
  value. Validation/veto/lifecycle errors still return `Err`.

## 3. Panic and isolation policy (v0.4.2)

The kernel contains **unwinding panics raised by plugin/embedder code it calls
through one of its own dispatch points**. Each contained failure is logged via
`tracing`; the kernel state machine continues. The waterfall lane is the single
deliberate exception: it is **fail-closed** — a panicking middleware aborts the
chain and the panic is re-raised to the dispatcher (both sync and async since
v0.4.2).

| Boundary | Behavior |
| --- | --- |
| Sync `apply` returns `Err` or panics | activation fails, effects/services/listeners rolled back, fiber `Failed`; panic does not escape `ctx.plugin` (CORE-028/029) |
| Async activation returns `Err` or panics | same rollback, async cleanups awaited (CORE-035/036) |
| Sync/async cleanup panics | remaining cleanups and effects still run; the first panic is re-raised after the drain (CORE-030/011). During a failed-activation rollback it is logged instead of escaping |
| Cleanup panic while a **refresh/retire path** unloads dependents | contained and logged (v0.4.2); the drained fiber still reaches its target state so a provider cannot stick in `Unloading`. A direct `dispose` still re-raises |
| Child sweep panic while a parent unloads | per-child `catch_unwind` (v0.4.2); the sweep continues with the remaining children, and the first payload is re-raised only after the parent reached its target state |
| Reload panic during `Registry::replace` / `Fiber::replace_plugin` | contained (v0.4.2): converted to `Err(CordisError::PluginFailed("<op>: reload panicked: …"))` with a full rollback; only a failing rollback returns `Rollback`. The `Result` APIs never unwind |
| Plugin metadata callbacks (`name` / `key` / `inject` / `config_schema`, sync + async) | contained and degraded (v0.4.3, C2): `name` → `"<panicked plugin>"`, `key` → `Named("<panicked plugin>")`, `inject` → `&[]`, `config_schema` → `None`; logged, never escapes `ctx.plugin`/replace |
| `dispose_all` with both an error and a panic | the panic wins (v0.4.2): the error is logged and the first payload is re-raised after every fiber was attempted (CORE-051) |
| Sync event listener panics | contained, treated as `EventResult::Continue`; dispatch continues (CORE-031) |
| Async listener panic (future construction or poll) | contained and logged; dispatch continues |
| Internal hooks (`internal/*`) | contained with the hook's fallback behavior (default config, skipped listener, continue) |
| Logger `LogExporter::level` / `export` | contained per exporter; remaining exporters and the buffer write continue |
| Reflect accessor read | contained; lookup treats the accessor as absent (`None`) — v0.4.2 returns `None` instead of a placeholder value |
| `ConfigSchema::validate` / `::merge` | contained; a panic becomes a validation failure / lowest-precedence fallback |
| `AvailabilityCheck` predicate | contained; treated as `false` (service unavailable) |
| Waterfall middleware panic (sync **and** async) | logs and **aborts the chain, re-raising** (fail-closed; inside `apply` this becomes a failed activation). It never lets the guarded operation proceed or falls back to `Value::Null` |

Not contained, because these are not unwinding panics: `panic = "abort"` builds,
stack overflow, OOM/allocator abort, `process::abort`/`exit`, `unsafe` code with
undefined behavior, deadlocks and infinite loops. Use an external process or WASM
sandbox for untrusted plugins; that layer is embedder guidance, not a crate feature
(see `docs/KERNEL-ISOLATION.md`).

`Disposer`/`AsyncDisposer` also run cleanup from `Drop` when they are detached. A
cleanup panic re-raised from `Drop` aborts the process when it happens during an
unwind, and `AsyncDisposer::drop` can block; prefer explicit `dispose` /
`dispose_async` (never dispose from `Drop` on an async worker thread).
**`Fiber` drop does not block on async cleanups (XT-01)**: dropping the last
`Fiber` handle without an explicit disposal runs the sync cleanups, but async
cleanups (including a deferred tail left by a cancelled `dispose_async`) are
**skipped** with a `tracing::error!` unless the embedder installed a
`BlockingExecutor` with `exec::set_blocking_executor`. This trades a possible
resource leak for not deadlocking the caller's thread: the default thread-park
`block_on` cannot drive a reactor, so draining a reactor-backed cleanup from
`Drop` on a `current_thread` runtime would hang forever. Dispose fibers
explicitly (`dispose` / `dispose_async`) on async worker threads.
Cancellation semantics depend on the granularity (CORE-046): an
`AsyncDisposer` restores only the cleanups that had not started and the next
explicit `dispose()` / `dispose_blocking()` retries them (S-1), while a cancelled
**fiber** unload completes the teardown (sync cleanups inline, async ones
deferred) and lands the fiber in its target state (A-9) — the in-flight cleanup
future is not recoverable either way. A cleanup that calls `dispose_blocking()`
on its own disposer returns immediately (self-join, S-2) instead of blocking on
the teardown it belongs to; the async mirror — `await`ing its own
`AsyncDisposer::dispose` inside one of that disposer's cleanups — short-circuits
the same way (R2-1). The check is a *drain frame* on the current call stack, not
a thread id, so an unrelated joiner task scheduled on the same worker thread
still waits for the teardown.

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
- **Registry snapshots are per-structure, not atomic.** `Registry::size`,
  `fiber_count`, `keys`, `values`, `entries` and the runtime member lists each
  lock their own structure; while another thread runs a lifecycle operation one
  call can observe a runtime or fiber that a later call no longer sees. Treat
  cross-call snapshots as **diagnostics**, not a transactional view; call
  `is_idle()`/`when_idle()` first when you need a consistent picture (e.g. in
  tests or shutdown code).
- **Deep dependency-chain teardown is iterative (v0.4.3).** The unload cascade is
  a two-phase, order-preserving iteration (begin provider-before-dependent, drain
  in reverse), so the call stack is O(1) in the chain depth; on a 2 MiB thread
  depth 800 passes in 0.4.3 debug **and** release builds (0.4.2 comparison: debug
  aborted at ≈480 levels, release at ≈2200), while `MAX_PLUGIN_DEPTH = 256` still
  bounds plugin **loading** only. The former "dispose very deep chains in
  layers/batches" limitation no longer applies.
- **Cyclic registration waits are not detected.** `when_idle()` excludes only the
  caller's own in-flight registrations (R3-A3). If two threads each wait for the
  other's registration/activation window, they can still wait forever — avoid
  synchronous registration cycles in hosts.
- **Supervision quotas count live resources (v0.4.3; Q11 fixed in v0.5.0).**
  `FiberQuota` limits apply to live effects/listeners/children; released slots
  (unload, `once` auto-removal, concurrent dispose) are reusable immediately.
  Since v0.5.0 `max_effects` counts **user** effects only — internal kernel
  effects are excluded, so `max_effects(1)` still loads plugins.

- **Cancelled-cascade recovery (F1/F4/F5, v0.4.3).** A cancelled async cascade
  can park non-root dependents in `Unloading`; recovery happens through
  `when_idle()` (both `Context` and the raw `Registry` API), `refresh_async()`,
  `dispose_all()` and retried root `dispose()`/`dispose_async()`, to the
  original target with deferred async cleanups running exactly once. The
  recovery latch is RAII (no stuck latch after cancel/panic) and taken frames
  are re-parked if recovery is cancelled mid-batch. **Sync `refresh()` does not
  recover** (it may run on a reactor thread; a synchronous drain would
  deadlock) — it simply skips `Unloading` frames. Recovery is
  idempotent/deduped/bounded and may be deferred; an async cleanup already in
  flight at cancellation time stays unrecoverable (A-9), while `is_idle()`
  remains strictly `false` until recovery runs.
- **Deferred cleanups outliving a cancelled recovery — fixed in v0.4.4 (S7).**
  In 0.4.3, cancelling a recovery while it finished the cascade *root* could
  leave `CancelSafeDrain`-restored cleanups in `deferred_async` until an
  explicit `dispose`/`dispose_all` (the fiber was on its target state, so
  `when_idle()` could report idle first). v0.4.4 re-parks frames that are
  `Unloading` **or** non-`Disposed` with non-empty `deferred_async`
  (`ReparkGuard`), re-admits them in `take_parked_unloads`, and makes
  `when_idle()` return only when idle **and** `!has_parked_unloads()`; each
  restored cleanup runs exactly once before the wait completes.
- **Reverse-dependency index (v0.5.0).** The 0.4.3 fanout regression (5000-fiber
  `create`/`dispose_all` ~25–35% slower than 0.4.2) is eliminated by the full
  reverse-dependency index (S1–S4, internal only, no public API or semantic
  change): fanout 5k `create+dispose` −54% / `dispose` −74%, 1500-level deep
  chain cascade −97% / `dispose` −90%, fanout 1k `create+dispose` −50%.
- **Degraded plugin keys are not individually addressable (C2).** Every plugin
  whose `Plugin::key()` panics shares one
  `PluginKey::Named("<panicked plugin>")` runtime, so `keys`/`replace`/`delete`
  see a single entry and cannot target one such plugin. This is best-effort
  isolation for already-broken plugins — not a supported configuration; fix the
  plugin's `key()` instead.

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
