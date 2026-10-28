//! The `Plugin` trait and the process-wide plugin/fiber registry.
//!
//! Cordis keeps one mutable `Plugin.Runtime` per plugin entrypoint: every
//! `ctx.plugin(plugin)` call creates a new fiber but they share the runtime, and
//! the registry exposes a map-like API over runtimes (`counter`, `size`,
//! `get`/`has`/`delete`, `keys`/`values`/`entries`/`forEach`).
//!
//! Rust identifies a plugin by [`PluginKey`]: an explicit
//! [`PluginKey::Type`] (`TypeId` of the concrete plugin type, used by
//! [`crate::Context::plugin_typed`]) or the default [`PluginKey::Named`] taken
//! from [`Plugin::name`].

use std::any::TypeId;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
#[cfg(feature = "supervision")]
use std::time::Duration;

use serde_json::Value;

use crate::context::Context;
use crate::error::Result;
use crate::fiber::{
    activate_fiber, activate_fiber_async, dependencies_ready, dependencies_satisfied,
    recover_parked_unloads_async, recover_parked_unloads_blocking, unload_contained,
    unload_contained_async, Fiber, FiberData, FiberRef, FiberState,
};
#[cfg(feature = "supervision")]
use crate::l2::{FiberQuota, StalledFiber};

static IDLE_SEQ: AtomicU64 = AtomicU64::new(1);

// ---------------------------------------------------------------------------
// C4: per-thread plugin-registration ownership
// ---------------------------------------------------------------------------
//
// `ctx.plugin(...)` holds a `RegistrationGuard` for the whole
// "create → internal/plugin hook → activate" window (FIX-11b). A host that
// calls `when_idle()` *from inside that same window* (typically from an
// `internal/plugin` hook) can never make progress by waiting: the registration
// that keeps the registry busy is blocked on the very thread that is waiting.
// The registry therefore records which **thread created** each in-flight
// registration; the quiescence check used by `when_idle` excludes this thread's
// own registrations (and only those), so a hook can observe "everything except
// my own registration is quiet" instead of deadlocking. Other threads'
// registrations are still counted, so external waiters keep the FIX-11b
// guarantee.
//
// The owner is recorded on the `Registry` (not in a thread-local) because the
// guard is held across `.await` in `plugin_async` and a multi-thread runtime may
// poll/drop the future on a different worker thread: the decrement must target
// the *creating* thread's count, not the dropping thread's.

/// Stable identity of one plugin runtime.
///
/// `Type` wins over `Named` when a typed plugin is registered through
/// [`crate::Context::plugin_typed`]; otherwise the plugin's name is the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PluginKey {
    /// Concrete plugin type (`std::any::TypeId`), shared by every instance of
    /// the same type.
    Type(TypeId),
    /// Plugin display name (`Plugin::name()`), shared by every plugin with the
    /// same name.
    Named(&'static str),
}

/// One plugin entrypoint and every live fiber started from it (Cordis
/// `Plugin.Runtime`).
pub struct PluginRuntime {
    /// Display name copied from the first registration.
    pub name: String,
    /// Identity key of this runtime.
    pub key: PluginKey,
    fibers: Mutex<Vec<Fiber>>,
}

impl PluginRuntime {
    fn new(name: String, key: PluginKey) -> Arc<Self> {
        Arc::new(Self {
            name,
            key,
            fibers: Mutex::new(Vec::new()),
        })
    }

    /// Live fibers of this runtime (diagnostics).
    pub fn fibers(&self) -> Vec<Fiber> {
        self.fibers
            .lock()
            .map(|fibers| fibers.clone())
            .unwrap_or_default()
    }

    /// Number of live fibers of this runtime.
    pub fn fiber_count(&self) -> usize {
        self.fibers.lock().map(|fibers| fibers.len()).unwrap_or(0)
    }

    /// Whether every fiber of this runtime was disposed.
    pub fn is_empty(&self) -> bool {
        self.fiber_count() == 0
    }
}

impl std::fmt::Debug for PluginRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginRuntime")
            .field("name", &self.name)
            .field("key", &self.key)
            .field("fibers", &self.fiber_count())
            .finish()
    }
}

/// A plugin is a named activation function with declared service dependencies.
pub trait Plugin: Send + Sync + 'static {
    /// Stable identity used by the registry and diagnostics.
    fn name(&self) -> &'static str;

    /// Registry identity (`TypeId` for [`crate::Context::plugin_typed`], the
    /// plugin name otherwise).
    fn key(&self) -> PluginKey {
        PluginKey::Named(self.name())
    }

    /// Services that must be available before `apply` runs.
    fn inject(&self) -> &'static [&'static str] {
        &[]
    }

    /// Optional config schema (Cordis `Plugin.Config`): validated before the
    /// plugin runs and on every `Fiber::update`.
    fn config_schema(&self) -> Option<Arc<dyn crate::config::ConfigSchema>> {
        None
    }

    /// Run the plugin. Effects registered through `ctx.effect` are removed on
    /// unload/restart; returning `Err` puts the fiber into `Failed`.
    fn apply(&self, ctx: Context, config: Value) -> Result<()>;
}

/// Placeholder plugin for the root fiber.
pub(crate) struct RootPlugin;

impl Plugin for RootPlugin {
    fn name(&self) -> &'static str {
        "root"
    }

    fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
        Ok(())
    }
}

/// An **async plugin**: same contract as [`Plugin`] except that activation is a
/// future awaited by the host (`Context::plugin_async`), so plugins may
/// perform asynchronous I/O-bound initialization without
/// blocking a thread or using a sync bridge.
///
/// Async plugins are activated, restarted and updated through the `*_async`
/// lifecycle APIs (`plugin_async`, `Fiber::restart_async`,
/// `Fiber::update_async`, `Context::refresh_async`).
pub trait AsyncPlugin: Send + Sync + 'static {
    /// Stable identity used by the registry and diagnostics.
    fn name(&self) -> &'static str;

    /// Registry identity (`TypeId` for [`crate::Context::plugin_async_typed`]).
    fn key(&self) -> PluginKey {
        PluginKey::Named(self.name())
    }

    /// Services that must be available before `apply_async` runs.
    fn inject(&self) -> &'static [&'static str] {
        &[]
    }

    /// Optional config schema (validated before activation / on `update`).
    fn config_schema(&self) -> Option<Arc<dyn crate::config::ConfigSchema>> {
        None
    }

    /// Asynchronous activation body. Effects registered through `ctx` are
    /// removed on unload/restart; returning `Err` puts the fiber into `Failed`
    /// and rolls back the effects registered so far.
    fn apply_async<'a>(
        &'a self,
        ctx: Context,
        config: Value,
    ) -> crate::exec::BoxFuture<'a, Result<()>>;
}

/// The plugin implementation a fiber hosts (sync [`Plugin`] or [`AsyncPlugin`]).
#[derive(Clone)]
pub enum PluginKind {
    /// Synchronous plugin (`Plugin::apply`).
    Sync(Arc<dyn Plugin>),
    /// Asynchronous plugin (`AsyncPlugin::apply_async`).
    Async(Arc<dyn AsyncPlugin>),
}

// ---------------------------------------------------------------------------
// C2: contained plugin metadata callbacks
// ---------------------------------------------------------------------------
//
// `Plugin::name/key/inject/config_schema` are **embedder** code. The kernel calls
// them from plugin creation, replacement, restart, diagnostics and dependency
// resolution, so a panicking metadata callback must not unwind through a
// `Result` API or poison kernel locks. Contract (documented on the methods):
// a panic is logged and degrades deterministically:
//   * `name()`  -> `"<panicked plugin>"` (diagnostics/runtime display name),
//   * `key()`   -> `PluginKey::Named("<panicked plugin>")` (all panicking-key
//                  plugins then share one runtime; they are broken anyway),
//   * `inject()` -> `&[]` (the plugin activates without dependency gating),
//   * `config_schema()` -> `None` (raw config accepted, no validation).
// The degradation is logged at `error!` level; it is best-effort isolation, not
// a replacement for fixing the plugin.

/// Display name used when a plugin's `name()` callback panics (C2).
pub(crate) const PANICKED_PLUGIN_NAME: &str = "<panicked plugin>";

/// Contained `Plugin::name` / `AsyncPlugin::name` (C2).
pub(crate) fn plugin_name_contained(plugin: &dyn Plugin) -> &'static str {
    match catch_unwind(AssertUnwindSafe(|| plugin.name())) {
        Ok(name) => name,
        Err(payload) => {
            crate::fiber::log_cleanup_panic("Plugin::name metadata callback", payload.as_ref());
            PANICKED_PLUGIN_NAME
        }
    }
}

/// Contained `AsyncPlugin::name` (C2).
pub(crate) fn async_plugin_name_contained(plugin: &dyn AsyncPlugin) -> &'static str {
    match catch_unwind(AssertUnwindSafe(|| plugin.name())) {
        Ok(name) => name,
        Err(payload) => {
            crate::fiber::log_cleanup_panic(
                "AsyncPlugin::name metadata callback",
                payload.as_ref(),
            );
            PANICKED_PLUGIN_NAME
        }
    }
}

/// Contained `Plugin::key` (C2): a panicking key degrades to the shared
/// panicked-plugin runtime instead of unwinding out of `ctx.plugin`.
///
/// C2 caveat: every plugin whose `Plugin::key()` panics is registered under the
/// same shared `PluginKey::Named("<panicked plugin>")` runtime. Per-runtime
/// registry operations (`keys`, `replace`, `delete`, size accounting) therefore
/// treat all such degraded plugins as ONE runtime: `replace`/`delete` affect
/// them together and no individual targeting is possible. This is deliberate
/// best-effort isolation of already-broken plugins, not a supported setup.
pub(crate) fn plugin_key_contained(plugin: &dyn Plugin) -> PluginKey {
    match catch_unwind(AssertUnwindSafe(|| plugin.key())) {
        Ok(key) => key,
        Err(payload) => {
            crate::fiber::log_cleanup_panic("Plugin::key metadata callback", payload.as_ref());
            PluginKey::Named(PANICKED_PLUGIN_NAME)
        }
    }
}

/// Contained `AsyncPlugin::key` (C2). Same shared-degraded-runtime caveat as
/// [`plugin_key_contained`].
pub(crate) fn async_plugin_key_contained(plugin: &dyn AsyncPlugin) -> PluginKey {
    match catch_unwind(AssertUnwindSafe(|| plugin.key())) {
        Ok(key) => key,
        Err(payload) => {
            crate::fiber::log_cleanup_panic("AsyncPlugin::key metadata callback", payload.as_ref());
            PluginKey::Named(PANICKED_PLUGIN_NAME)
        }
    }
}

/// Contained `Plugin::inject` (C2): a panicking dependency declaration degrades
/// to "no dependencies" (logged) instead of failing the lifecycle operation.
pub(crate) fn plugin_inject_contained(plugin: &dyn Plugin) -> &'static [&'static str] {
    match catch_unwind(AssertUnwindSafe(|| plugin.inject())) {
        Ok(inject) => inject,
        Err(payload) => {
            crate::fiber::log_cleanup_panic("Plugin::inject metadata callback", payload.as_ref());
            &[]
        }
    }
}

/// Contained `AsyncPlugin::inject` (C2).
pub(crate) fn async_plugin_inject_contained(plugin: &dyn AsyncPlugin) -> &'static [&'static str] {
    match catch_unwind(AssertUnwindSafe(|| plugin.inject())) {
        Ok(inject) => inject,
        Err(payload) => {
            crate::fiber::log_cleanup_panic(
                "AsyncPlugin::inject metadata callback",
                payload.as_ref(),
            );
            &[]
        }
    }
}

/// Contained `Plugin::config_schema` (C2): a panicking schema accessor degrades
/// to "no schema" (raw config accepted), logged.
pub(crate) fn plugin_schema_contained(
    plugin: &dyn Plugin,
) -> Option<Arc<dyn crate::config::ConfigSchema>> {
    match catch_unwind(AssertUnwindSafe(|| plugin.config_schema())) {
        Ok(schema) => schema,
        Err(payload) => {
            crate::fiber::log_cleanup_panic(
                "Plugin::config_schema metadata callback",
                payload.as_ref(),
            );
            None
        }
    }
}

/// Contained `AsyncPlugin::config_schema` (C2).
pub(crate) fn async_plugin_schema_contained(
    plugin: &dyn AsyncPlugin,
) -> Option<Arc<dyn crate::config::ConfigSchema>> {
    match catch_unwind(AssertUnwindSafe(|| plugin.config_schema())) {
        Ok(schema) => schema,
        Err(payload) => {
            crate::fiber::log_cleanup_panic(
                "AsyncPlugin::config_schema metadata callback",
                payload.as_ref(),
            );
            None
        }
    }
}

impl PluginKind {
    /// Display/diagnostic name (C2: a panicking callback degrades to
    /// `PANICKED_PLUGIN_NAME`, logged).
    pub fn name(&self) -> &'static str {
        match self {
            Self::Sync(plugin) => plugin_name_contained(plugin.as_ref()),
            Self::Async(plugin) => async_plugin_name_contained(plugin.as_ref()),
        }
    }

    /// Registry identity (C2: a panicking callback degrades to
    /// [`PluginKey::Named`] with `PANICKED_PLUGIN_NAME`, logged).
    pub fn key(&self) -> PluginKey {
        match self {
            Self::Sync(plugin) => plugin_key_contained(plugin.as_ref()),
            Self::Async(plugin) => async_plugin_key_contained(plugin.as_ref()),
        }
    }

    /// Declared service dependencies (C2: a panicking callback degrades to no
    /// dependencies, logged).
    pub fn inject(&self) -> &'static [&'static str] {
        match self {
            Self::Sync(plugin) => plugin_inject_contained(plugin.as_ref()),
            Self::Async(plugin) => async_plugin_inject_contained(plugin.as_ref()),
        }
    }

    /// Optional config schema (C2: a panicking callback degrades to no schema,
    /// logged).
    pub fn config_schema(&self) -> Option<Arc<dyn crate::config::ConfigSchema>> {
        match self {
            Self::Sync(plugin) => plugin_schema_contained(plugin.as_ref()),
            Self::Async(plugin) => async_plugin_schema_contained(plugin.as_ref()),
        }
    }

    /// Whether this is an async plugin (must use `*_async` lifecycle APIs).
    pub fn is_async(&self) -> bool {
        matches!(self, Self::Async(_))
    }

    /// Borrow the sync plugin when this is a [`PluginKind::Sync`].
    pub fn as_sync(&self) -> Option<&Arc<dyn Plugin>> {
        match self {
            Self::Sync(plugin) => Some(plugin),
            Self::Async(_) => None,
        }
    }

    /// Borrow the async plugin when this is a [`PluginKind::Async`].
    pub fn as_async(&self) -> Option<&Arc<dyn AsyncPlugin>> {
        match self {
            Self::Async(plugin) => Some(plugin),
            Self::Sync(_) => None,
        }
    }
}

/// The registry owned by the root context (Cordis `RegistryService`).
#[derive(Default)]
pub struct Registry {
    counter: Mutex<u64>,
    scope_counter: Mutex<u64>,
    fibers: Mutex<Vec<Fiber>>,
    runtimes: Mutex<HashMap<PluginKey, Arc<PluginRuntime>>>,
    /// Wakers of `when_idle` callers, tagged with their future id so a dropped
    /// future can unregister (no leak, no spurious wakeups).
    idle_waiters: Mutex<Vec<(u64, std::task::Waker)>>,
    /// Named isolation scopes (`isolate_labeled`): label -> scope id, allocated
    /// from the same counter as fresh `isolate()` scopes so the two namespaces
    /// can never collide.
    label_scopes: Mutex<HashMap<&'static str, u64>>,
    /// The root fiber (weak): it is not part of `fibers`, but its lifecycle
    /// operations count for quiescence.
    root_fiber: Mutex<Option<std::sync::Weak<Mutex<crate::fiber::FiberData>>>>,
    /// Per-fiber resource limits enforced when resources are registered
    /// (only with the `supervision` feature; unlimited otherwise).
    #[cfg(feature = "supervision")]
    quota: Mutex<FiberQuota>,
    /// Re-entrancy guard for the refresh fixed point: a dependent evaluation
    /// triggered from inside a refresh marks the registry dirty instead of
    /// recursing (prevents exponential re-entrant scans).
    refresh_depth: AtomicUsize,
    refresh_dirty: AtomicBool,
    /// Plugin registrations in flight before their fiber-level lifecycle
    /// operation exists (`ctx.plugin` → `internal/plugin` hook →
    /// `activate`). While this is non-zero the registry is not quiescent
    /// (FIX-11b), so `when_idle` cannot return before activation starts.
    pending_registrations: AtomicUsize,
    /// C4: in-flight registrations per **creating** thread. Used by
    /// `when_idle` to exclude the caller's own registrations (waiting for them
    /// would self-deadlock). Kept on the registry, not in a thread-local, so a
    /// guard that is dropped on another thread decrements the right owner.
    registration_owners: Mutex<HashMap<std::thread::ThreadId, usize>>,
    /// F1: fibers left `Unloading` by a **cancelled** async cascade (their
    /// remaining frames were parked instead of blocking on reactor cleanups).
    /// `dispose_all` / `refresh_all(_async)` / a root `dispose(_async)` / the
    /// next `when_idle` recover them, so a cancel can never leave a permanent
    /// `Unloading` that blocks quiescence (A-9). Entries are weak and pruned, so
    /// the list stays proportional to concurrently parked fibers.
    parked_unloads: Mutex<Vec<(Weak<Mutex<FiberData>>, FiberState)>>,
    /// F1: single-recoverer guard. A second recoverer (another thread, or a
    /// nested call from a cleanup) returns immediately; the first one finishes
    /// the recovery and any leftover parked fiber is picked up by the next call.
    recovering_parked: AtomicBool,
    /// F1: number of unload/cascade bodies currently running on any thread.
    /// Recovery is skipped while one is active: a cancellation unwinds through
    /// `withdraw`/`refresh_dependents` on the cancelling thread, and recovering
    /// other fibers from that nested refresh would run reactor-backed cleanups
    /// inside `Drop` (deadlock). The parked fibers stay recoverable by the next
    /// root operation (`dispose_all`/`refresh_*`/`dispose(_async)`/`when_idle`).
    active_unloads: AtomicUsize,
}

/// Future returned by `Registry::wait_idle_or_recoverable` (F5).
///
/// Resolves when the registry is idle **or** there is a parked unload that can
/// be recovered right now (`parked && !active_unloads`). `when_idle` uses it
/// instead of the plain `IdleWait` so a cancel that produces new parked frames
/// while we are waiting is noticed and recovered, instead of being missed by a
/// single up-front recovery pass.
pub struct IdleOrRecoverableWait<'a> {
    registry: &'a Registry,
    id: u64,
}

impl Drop for IdleOrRecoverableWait<'_> {
    fn drop(&mut self) {
        if let Ok(mut waiters) = self.registry.idle_waiters.lock() {
            waiters.retain(|(id, _)| *id != self.id);
        }
    }
}

impl std::future::Future for IdleOrRecoverableWait<'_> {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        let this = self.get_mut();
        if this.registry.is_recoverable_or_idle() {
            return std::task::Poll::Ready(());
        }
        if let Ok(mut waiters) = this.registry.idle_waiters.lock() {
            waiters.retain(|(id, _)| *id != this.id);
            waiters.push((this.id, cx.waker().clone()));
        }
        if this.registry.is_recoverable_or_idle() {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    }
}

/// Future returned by [`Registry::when_idle`]; unregisters its waker on drop.
///
/// F5/raw-API fix: the *public* `Registry::when_idle` is recovery-aware too —
/// a cancelled cascade can park fibers as `Unloading`, and a raw waiter that
/// only re-polled the plain idle condition would hang forever. The future is a
/// small state machine: it first runs the recovery-aware idle loop
/// (`when_idle_recovering`), and that loop awaits the raw waiter above.
pub struct IdleWait<'a> {
    future: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>,
}

impl std::future::Future for IdleWait<'_> {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        let this = self.get_mut();
        this.future.as_mut().poll(cx)
    }
}

/// Internal form of per-fiber limits (only with the `supervision` feature).
#[cfg(feature = "supervision")]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct QuotaLimits {
    pub(crate) max_effects: Option<usize>,
    pub(crate) max_listeners: Option<usize>,
    pub(crate) max_child_fibers: Option<usize>,
}

/// Snapshot taken before a rebind so a rejected replace can be undone.
struct RebindUndo {
    old_key: PluginKey,
    old_runtime: Arc<PluginRuntime>,
    target_key: PluginKey,
    fibers: Vec<(Fiber, PluginKind, Option<PluginKey>)>,
}

/// RAII guard for the refresh fixed point (FIX-1).
///
/// The re-entrancy latch (`refresh_depth`) used to be reset by a plain store at
/// the end of `refresh_all`/`refresh_all_async`; a cleanup panic unwinding out
/// of the loop skipped it, latched the registry forever and left dependency
/// refresh dead (`is_idle` still reporting idle). Resetting in `Drop` also
/// covers the async path when the future is cancelled mid-refresh.
struct RefreshGuard<'a> {
    registry: &'a Registry,
}

impl Drop for RefreshGuard<'_> {
    fn drop(&mut self) {
        self.registry.refresh_depth.store(0, Ordering::SeqCst);
    }
}

/// RAII guard for the "fiber registered → activation started" window (FIX-11b).
///
/// `create_fiber_handle` publishes the fiber and fires the `internal/plugin`
/// create hook *before* any fiber-level operation guard exists, so a host
/// waiting in `when_idle()` could otherwise observe the moment as quiescent.
/// The context plugin entry points hold this guard for the whole
/// registration+activation window; `is_idle` reports busy while it is alive.
pub(crate) struct RegistrationGuard<'a> {
    registry: &'a Registry,
    /// C4: thread that started the registration. `plugin_async` keeps this guard
    /// across `.await`; a multi-thread runtime may drop (or poll) the future on
    /// another worker, so the decrement must target the creating thread's count.
    created_thread: std::thread::ThreadId,
}

impl Drop for RegistrationGuard<'_> {
    fn drop(&mut self) {
        // C4: release exactly the creating thread's count (idempotent / no
        // underflow), then decrement and wake waiters so a re-poll observes the
        // new state (the last registration may be the only thing keeping it
        // busy).
        self.registry
            .release_registration_owner(self.created_thread);
        self.registry
            .pending_registrations
            .fetch_sub(1, Ordering::SeqCst);
        self.registry.notify_idle();
    }
}

/// Outcome of a failed [`Registry::replace`] whose rollback also failed.
///
/// Contract (kept compatible with 0.4.1 tests): the caller sees the **original**
/// error unless a partial migration really happened (`reloaded > 0`) and the
/// rollback failed for a reason other than a busy fiber. `LifecycleBusy` during
/// the rollback only means another thread is finishing its own operation on one
/// of the fibers; the runtime table/bindings were restored structurally, so the
/// caller still gets the original error (the incomplete per-fiber restore is
/// logged). Only a genuine partial migration plus a non-busy rollback failure is
/// reported as [`crate::CordisError::Rollback`] with both errors.
fn replace_rollback_result(
    original: crate::CordisError,
    rollback: Option<crate::CordisError>,
    reloaded: usize,
) -> crate::CordisError {
    let Some(rollback) = rollback else {
        return original;
    };
    if reloaded == 0 || matches!(rollback, crate::CordisError::LifecycleBusy) {
        tracing::error!(
            target: "zeokeel_cordis::registry",
            "replace rollback incomplete (reloaded={reloaded}): {rollback}; returning the original error: {original}"
        );
        return original;
    }
    crate::CordisError::Rollback(format!(
        "original error: {original}; rollback error: {rollback}"
    ))
}

/// Turn a contained reload panic into the error reported by
/// [`Registry::replace`] / [`Registry::replace_async`] (D-2).
fn reload_panic(operation: &str, payload: &(dyn std::any::Any + Send)) -> crate::CordisError {
    crate::CordisError::PluginFailed(format!(
        "{operation}: reload panicked: {}",
        crate::fiber::panic_message(payload)
    ))
}

/// Indices the rollback must reload: every fiber that already ran the new
/// implementation plus the failed fiber itself (whose reload may have unloaded
/// it). The failed fiber is checked lazily by the caller, because an earlier
/// rollback reload can re-activate it through the dependent refresh (D-2).
fn rollback_targets(fibers: &[Fiber], reloaded: usize) -> Vec<usize> {
    let mut targets: Vec<usize> = (0..reloaded).collect();
    if reloaded < fibers.len() {
        targets.push(reloaded);
    }
    targets
}

/// Reload one fiber for the rollback, containing a cleanup panic (D-2).
fn restart_contained(fiber: &Fiber) -> Result<()> {
    match catch_unwind(AssertUnwindSafe(|| fiber.restart())) {
        Ok(result) => result,
        Err(payload) => Err(reload_panic("replace rollback", payload.as_ref())),
    }
}

/// Awaiting variant of [`restart_contained`].
async fn restart_contained_async(fiber: &Fiber) -> Result<()> {
    let outcome =
        futures_util::FutureExt::catch_unwind(AssertUnwindSafe(fiber.restart_async())).await;
    match outcome {
        Ok(result) => result,
        Err(payload) => Err(reload_panic("replace rollback", payload.as_ref())),
    }
}

impl Registry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Current fiber id counter (Cordis `registry.counter`).
    pub fn counter(&self) -> u64 {
        self.counter.lock().map(|counter| *counter).unwrap_or(0)
    }

    /// Set the per-fiber quota applied to every fiber in this registry.
    #[cfg(feature = "supervision")]
    pub fn set_quota(&self, quota: FiberQuota) {
        if let Ok(mut slot) = self.quota.lock() {
            *slot = quota;
        }
    }

    /// Current per-fiber quota (unlimited by default).
    #[cfg(feature = "supervision")]
    pub fn quota(&self) -> FiberQuota {
        self.quota.lock().map(|quota| *quota).unwrap_or_default()
    }

    /// Internal limits used by the enforcement hooks (`supervision` only).
    #[cfg(feature = "supervision")]
    pub(crate) fn quota_limits(&self) -> QuotaLimits {
        let quota = self.quota();
        QuotaLimits {
            max_effects: quota.max_effects,
            max_listeners: quota.max_listeners,
            max_child_fibers: quota.max_child_fibers,
        }
    }

    /// Fibers with a lifecycle operation in flight that have been quiet for at
    /// least `timeout`. This is the cooperative watchdog: the kernel never
    /// interrupts or kills the fiber, the embedder supervisor decides what to do
    /// (log, restart, cancel via `Fiber::cancel`, or isolate out-of-process).
    #[cfg(feature = "supervision")]
    pub fn stalled_fibers(&self, timeout: Duration) -> Vec<StalledFiber> {
        let mut fibers = self
            .fibers
            .lock()
            .map(|fibers| fibers.clone())
            .unwrap_or_default();
        if let Some(data) = self
            .root_fiber
            .lock()
            .ok()
            .and_then(|slot| slot.as_ref().and_then(|weak| weak.upgrade()))
        {
            fibers.push(Fiber { data });
        }
        let mut stalled = Vec::new();
        for fiber in fibers {
            let state = fiber.state();
            let pending = fiber
                .data
                .lock()
                .map(|data| data.pending_ops.load(Ordering::SeqCst))
                .unwrap_or(0);
            let in_flight =
                pending > 0 || matches!(state, FiberState::Loading | FiberState::Unloading);
            if !in_flight {
                continue;
            }
            let idle_for = fiber.idle_for();
            if idle_for >= timeout {
                stalled.push(StalledFiber { fiber, idle_for });
            }
        }
        stalled
    }

    /// Whether the whole registry is quiescent: no plugin registration is in
    /// flight and no fiber (including the root fiber) is loading/unloading.
    ///
    /// A registration counts from the moment the context entry point starts
    /// creating the fiber until its activation has begun (FIX-11b: this covers
    /// the `internal/plugin` create hook, which runs before the fiber-level
    /// operation guard exists), so `when_idle()` never reports a settled kernel
    /// for a plugin that has not started activating yet.
    pub fn is_idle(&self) -> bool {
        self.is_idle_excluding(0)
    }

    /// Quiescence with `own` registrations of the calling thread excluded (C4).
    ///
    /// Used only by [`IdleWait`]: a host waiting from inside its own plugin
    /// registration window must not wait for that registration. `is_idle()` is
    /// intentionally strict (the window is not idle for anybody else).
    fn is_idle_excluding(&self, own: usize) -> bool {
        let total = self.pending_registrations.load(Ordering::SeqCst);
        self.is_idle_excluding_with_total(own, total)
    }

    /// F3: quiescence check from one `(own, total)` snapshot (see
    /// [`Registry::quiescence_snapshot`]) so a concurrent registration cannot
    /// slip between the two reads.
    pub(crate) fn is_idle_excluding_with_total(&self, own: usize, total: usize) -> bool {
        if total.saturating_sub(own) != 0 {
            return false;
        }
        let fibers = self
            .fibers
            .lock()
            .map(|fibers| fibers.clone())
            .unwrap_or_default();
        let live = fibers.iter().all(|fiber| {
            let Ok(data) = fiber.data.lock() else {
                return true;
            };
            !matches!(data.state, FiberState::Loading | FiberState::Unloading)
                && data.pending_ops.load(Ordering::SeqCst) == 0
        }) && match self.root_fiber.lock() {
            Ok(slot) => match slot.as_ref().and_then(|weak| weak.upgrade()) {
                Some(data) => match data.lock() {
                    Ok(root) => {
                        !matches!(root.state, FiberState::Loading | FiberState::Unloading)
                            && root.pending_ops.load(Ordering::SeqCst) == 0
                    }
                    Err(_) => true,
                },
                None => true,
            },
            Err(_) => false,
        };
        live
    }

    /// Register the root fiber so its lifecycle operations count for
    /// quiescence (the root fiber is not part of `fibers`).
    pub(crate) fn set_root_fiber(&self, fiber: &Fiber) {
        if let Ok(mut slot) = self.root_fiber.lock() {
            *slot = Some(std::sync::Arc::downgrade(&fiber.data));
        }
    }

    /// Wake every `when_idle` waiter (called on lifecycle state changes).
    pub(crate) fn notify_idle(&self) {
        let waiters = self
            .idle_waiters
            .lock()
            .map(|mut waiters| std::mem::take(&mut *waiters))
            .unwrap_or_default();
        for (_, waker) in waiters {
            waker.wake();
        }
    }

    /// F5: `true` when the caller can make progress: either the registry is
    /// idle (own registrations excluded) or a parked unload can be recovered
    /// now. Used by the recovery-aware idle wait.
    pub(crate) fn is_recoverable_or_idle(&self) -> bool {
        let (own, total) = self.quiescence_snapshot();
        if self.is_idle_excluding_with_total(own, total) {
            return true;
        }
        !self.has_active_unload() && self.has_parked_unloads()
    }

    /// F5: resolve when idle **or** a parked unload became recoverable.
    pub(crate) fn wait_idle_or_recoverable(&self) -> IdleOrRecoverableWait<'_> {
        IdleOrRecoverableWait {
            registry: self,
            id: IDLE_SEQ.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Recovery-aware idle loop: returns when genuinely idle, recovers parked
    /// unloads whenever they become recoverable, and otherwise waits on the raw
    /// idle waiter (woken by lifecycle changes / parking). Livelock-bounded; on
    /// exhaustion it logs and falls back to the raw wait (a later operation
    /// still recovers).
    async fn when_idle_recovering(&self) {
        let limit = self.fiber_count().saturating_add(8).max(16);
        for _ in 0..limit {
            let (own, total) = self.quiescence_snapshot();
            // S7: a recovery pass cancelled while finishing a frame leaves the
            // fiber on its target state (`Pending`) with restored cleanups in
            // `deferred_async`. Such an entry is re-parked, and `is_idle()` alone
            // would be `true` for a `Pending` fiber — do not return until the
            // recoverable set is empty, so the restored cleanup runs exactly
            // once before `when_idle` completes.
            if self.is_idle_excluding_with_total(own, total) && !self.has_parked_unloads() {
                return;
            }
            if self.has_parked_unloads() {
                if self.has_active_unload() {
                    self.wait_idle_or_recoverable().await;
                    continue;
                }
                crate::fiber::recover_parked_unloads_async(self).await;
                continue;
            }
            // A raw idle wait here would miss a cancel that parks frames while
            // we sleep behind an in-flight unload (A finding: lost wakeup ->
            // permanent hang). Wait for "idle OR recoverable parked" instead so
            // the loop re-checks and recovers.
            self.wait_idle_or_recoverable().await;
        }
        tracing::error!(
            target: "zeokeel_cordis::registry",
            "when_idle recovery loop hit its bound; falling back to the recoverable idle wait"
        );
        let _ = crate::fiber::recover_parked_unloads_async(self).await;
        self.wait_idle_or_recoverable().await;
    }

    /// Resolve once every fiber (including the root fiber) is idle.
    ///
    /// F5 (raw API): recovery-aware. A cancelled async cascade can park fibers
    /// as `Unloading`; waiting for pure quiescence would never return. This
    /// future runs the same bounded recovery loop as `Context::when_idle`, so
    /// the public registry API cannot hang on a parked cascade either.
    pub fn when_idle(&self) -> IdleWait<'_> {
        IdleWait {
            future: Box::pin(self.when_idle_recovering()),
        }
    }

    pub(crate) fn next_id(&self) -> u64 {
        self.counter
            .lock()
            .map(|mut counter| {
                *counter += 1;
                *counter
            })
            .unwrap_or(1)
    }

    /// Allocate a fresh isolation scope id (Cordis `Symbol(name)` uniqueness).
    /// Scope id for a named isolation scope (created on first use).
    pub(crate) fn labeled_scope_id(&self, label: &'static str) -> u64 {
        if let Ok(mut labels) = self.label_scopes.lock() {
            if let Some(id) = labels.get(label) {
                return *id;
            }
            let id = self.next_scope_id();
            labels.insert(label, id);
            return id;
        }
        self.next_scope_id()
    }

    pub(crate) fn next_scope_id(&self) -> u64 {
        self.scope_counter
            .lock()
            .map(|mut counter| {
                *counter += 1;
                *counter
            })
            .unwrap_or(1)
    }

    /// Number of registered plugin runtimes (Cordis `registry.size`).
    ///
    /// This counts *runtimes*, not fibers: the same plugin started twice has one
    /// runtime and two fibers.
    pub fn size(&self) -> usize {
        self.runtimes
            .lock()
            .map(|runtimes| runtimes.len())
            .unwrap_or(0)
    }

    /// Number of live fibers across all runtimes (diagnostics).
    pub fn fiber_count(&self) -> usize {
        self.fibers
            .lock()
            .map(|fibers| {
                fibers
                    .iter()
                    .filter(|fiber| !matches!(fiber.state(), FiberState::Disposed))
                    .count()
            })
            .unwrap_or(0)
    }

    /// Get the runtime registered under `key`.
    pub fn get(&self, key: PluginKey) -> Option<Arc<PluginRuntime>> {
        self.runtimes
            .lock()
            .ok()
            .and_then(|runtimes| runtimes.get(&key).cloned())
    }

    /// Whether a runtime is registered under `key`.
    pub fn has(&self, key: PluginKey) -> bool {
        self.runtimes
            .lock()
            .map(|runtimes| runtimes.contains_key(&key))
            .unwrap_or(false)
    }

    /// Remove the runtime and dispose every fiber of it, returning the removed
    /// runtime (Cordis `registry.delete`).
    pub fn delete(&self, key: PluginKey) -> Option<Arc<PluginRuntime>> {
        let runtime = self.runtimes.lock().ok()?.remove(&key);
        if let Some(runtime) = &runtime {
            for fiber in runtime.fibers() {
                let _ = fiber.dispose();
            }
        }
        runtime
    }

    /// Replace a plugin runtime's implementation in place (Cordis
    /// `registry.replace`): every fiber keeps its identity, raw config and
    /// effect history, but is re-bound to `new` and reloaded.
    ///
    /// Returns the reloaded fibers. The old key is removed; the new
    /// implementation's key becomes the runtime identity.
    ///
    /// FIX-7: when a reload is rejected (e.g. `LifecycleBusy`), the rollback is
    /// complete — the binding is restored *and* every fiber that already ran the
    /// new implementation is reloaded with the old one, so no fiber is left with
    /// the new implementation's live effects. If the rollback itself fails, the
    /// error is reported as [`crate::CordisError::Rollback`] carrying both the
    /// original and the rollback error (the binding is still restored).
    pub fn replace(&self, old: PluginKey, new: Arc<dyn Plugin>) -> Result<Vec<Fiber>> {
        let (fibers, undo) = self.rebind_runtime(old, PluginKind::Sync(new))?;
        for index in 0..fibers.len() {
            // A-6: a fiber disposed by a concurrent operation is already final;
            // reloading it would only produce a bogus `Disposed` error and (via
            // the rollback) a misleading `Rollback` report.
            if fibers[index].state() == FiberState::Disposed {
                self.heal_disposed_registration(&fibers[index]);
                continue;
            }
            // D-2: the reload drains the previous implementation's cleanups,
            // i.e. embedder code. A panic must be handled exactly like an
            // `Err`; otherwise it escapes this `Result` API *after* the runtime
            // was re-keyed and leaves a half-migrated registry.
            match catch_unwind(AssertUnwindSafe(|| fibers[index].restart())) {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    return Err(self.rollback_replace(undo, &fibers, index, error));
                }
                Err(payload) => {
                    let error = reload_panic("replace", payload.as_ref());
                    return Err(self.rollback_replace(undo, &fibers, index, error));
                }
            }
        }
        self.heal_disposed_registrations(&fibers);
        self.prune_empty_runtimes();
        Ok(fibers)
    }

    /// Awaiting variant of [`Registry::replace`] for async plugins.
    pub async fn replace_async(
        &self,
        old: PluginKey,
        new: Arc<dyn AsyncPlugin>,
    ) -> Result<Vec<Fiber>> {
        let (fibers, undo) = self.rebind_runtime(old, PluginKind::Async(new))?;
        for index in 0..fibers.len() {
            if fibers[index].state() == FiberState::Disposed {
                self.heal_disposed_registration(&fibers[index]);
                continue;
            }
            let outcome = futures_util::FutureExt::catch_unwind(AssertUnwindSafe(
                fibers[index].restart_async(),
            ))
            .await;
            match outcome {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    return Err(self
                        .rollback_replace_async(undo, &fibers, index, error)
                        .await);
                }
                Err(payload) => {
                    let error = reload_panic("replace_async", payload.as_ref());
                    return Err(self
                        .rollback_replace_async(undo, &fibers, index, error)
                        .await);
                }
            }
        }
        self.heal_disposed_registrations(&fibers);
        self.prune_empty_runtimes();
        Ok(fibers)
    }

    /// Undo a rejected [`Registry::replace`] with the old implementation.
    ///
    /// The binding is restored first (so the reloads below run against the old
    /// runtime), then the fibers that already applied the new implementation are
    /// restarted, which drains its effects and re-runs the old `apply`.
    fn rollback_replace(
        &self,
        undo: RebindUndo,
        fibers: &[Fiber],
        reloaded: usize,
        original: crate::CordisError,
    ) -> crate::CordisError {
        self.undo_rebind(undo);
        let mut rollback_error = None;
        for index in rollback_targets(fibers, reloaded) {
            let fiber = &fibers[index];
            if fiber.state() == FiberState::Disposed {
                continue;
            }
            if index == reloaded && fiber.state() == FiberState::Active {
                // A previous rollback reload ran `apply` again, which refreshes
                // dependents and may already have re-activated the failed fiber
                // (it is `Active` again with the old implementation). Reloading
                // it a second time would drain those freshly registered effects
                // — and could re-raise their cleanup panic (D-2).
                continue;
            }
            if let Err(error) = restart_contained(fiber) {
                rollback_error.get_or_insert(error);
            }
        }
        self.heal_disposed_registrations(fibers);
        self.prune_empty_runtimes();
        replace_rollback_result(original, rollback_error, reloaded)
    }

    /// Awaiting variant of [`Registry::rollback_replace`].
    async fn rollback_replace_async(
        &self,
        undo: RebindUndo,
        fibers: &[Fiber],
        reloaded: usize,
        original: crate::CordisError,
    ) -> crate::CordisError {
        self.undo_rebind(undo);
        let mut rollback_error = None;
        for index in rollback_targets(fibers, reloaded) {
            let fiber = &fibers[index];
            if fiber.state() == FiberState::Disposed {
                continue;
            }
            if index == reloaded && fiber.state() == FiberState::Active {
                continue;
            }
            if let Err(error) = restart_contained_async(fiber).await {
                rollback_error.get_or_insert(error);
            }
        }
        self.heal_disposed_registrations(fibers);
        self.prune_empty_runtimes();
        replace_rollback_result(original, rollback_error, reloaded)
    }

    /// Re-key a single fiber's runtime (used by `Fiber::replace_plugin`).
    pub(crate) fn rebind_single_fiber(
        &self,
        fiber: &Fiber,
        old_key: Option<PluginKey>,
        new_key: PluginKey,
    ) -> Result<()> {
        let name = fiber.name();
        let id = fiber.id();
        let mut runtimes = self
            .runtimes
            .lock()
            .map_err(|_| crate::CordisError::Disposed)?;
        if let Some(old) = old_key {
            if let Some(runtime) = runtimes.get(&old) {
                let empty = match runtime.fibers.lock() {
                    Ok(mut fibers) => {
                        fibers.retain(|candidate| candidate.id() != id);
                        fibers.is_empty()
                    }
                    Err(_) => false,
                };
                if empty {
                    runtimes.remove(&old);
                }
            }
        }
        let target = match runtimes.get(&new_key) {
            Some(existing) => Arc::clone(existing),
            None => {
                let created = PluginRuntime::new(name.to_string(), new_key);
                runtimes.insert(new_key, Arc::clone(&created));
                created
            }
        };
        drop(runtimes);
        if let Ok(mut list) = target.fibers.lock() {
            list.push(fiber.clone());
        }
        match fiber.data.lock() {
            Ok(mut data) => data.runtime_key = Some(new_key),
            Err(_) => {
                // The push already happened; a poisoned fiber lock would leave
                // the membership dangling, so detach it before returning.
                self.unregister_runtime_fiber(id);
                return Err(crate::CordisError::Disposed);
            }
        }
        // D-1 self-heal: the fiber may have been disposed between the runtime
        // table update and this push (a concurrent `dispose_all` only saw the
        // old membership). Undo the new membership now so the registry cannot
        // keep a `Disposed` strong handle.
        self.heal_disposed_registration(fiber);
        Ok(())
    }

    fn undo_rebind(&self, undo: RebindUndo) {
        if let Ok(mut runtimes) = self.runtimes.lock() {
            let ids: Vec<u64> = undo.fibers.iter().map(|(fiber, _, _)| fiber.id()).collect();
            // D-1 residual (s28): remove the migrated fibers from the target —
            // whether it was created by this replace or shared with another
            // one — and prune it when it becomes empty in *both* cases. Leaving
            // an empty runtime behind is invisible to `dispose_all` (no fibers
            // to dispose) and keeps `keys()/size()` non-empty forever.
            let mut prune_target = false;
            if let Some(target) = runtimes.get(&undo.target_key) {
                if let Ok(mut fibers) = target.fibers.lock() {
                    fibers.retain(|candidate| !ids.contains(&candidate.id()));
                    prune_target = fibers.is_empty();
                }
            }
            if prune_target {
                runtimes.remove(&undo.target_key);
            }
            // Restore the old runtime unless every fiber of it is gone by now
            // (a concurrent disposal may have emptied it; the post-loop heal
            // would then prune it, but not inserting it at all is cheaper).
            let old_has_fibers = undo
                .old_runtime
                .fibers
                .lock()
                .map(|fibers| !fibers.is_empty())
                .unwrap_or(true);
            if old_has_fibers {
                runtimes.insert(undo.old_key, undo.old_runtime);
            }
        }
        for (fiber, kind, runtime_key) in &undo.fibers {
            let _ = fiber.swap_plugin(kind.clone());
            if let Ok(mut data) = fiber.data.lock() {
                data.runtime_key = *runtime_key;
            }
        }
        // D-1: a fiber that finished disposing while the rollback ran must not
        // stay attached to the restored runtime.
        for (fiber, _, _) in &undo.fibers {
            self.heal_disposed_registration(fiber);
        }
        self.prune_empty_runtimes();
    }

    fn rebind_runtime(&self, old: PluginKey, kind: PluginKind) -> Result<(Vec<Fiber>, RebindUndo)> {
        let new_key = kind.key();
        let name = kind.name();
        let mut runtimes = self
            .runtimes
            .lock()
            .map_err(|_| crate::CordisError::Disposed)?;
        let runtime = runtimes.remove(&old).ok_or_else(|| {
            crate::CordisError::InvalidPlugin(format!(
                "replace: no plugin runtime registered for {old:?}"
            ))
        })?;
        let fibers = runtime.fibers();
        // Merge into an existing runtime for the new key if present (the new
        // implementation was already loaded elsewhere).
        let target: Arc<PluginRuntime> = match runtimes.get(&new_key) {
            Some(existing) => Arc::clone(existing),
            None => {
                let created: Arc<PluginRuntime> = PluginRuntime::new(name.to_string(), new_key);
                runtimes.insert(new_key, Arc::clone(&created));
                created
            }
        };
        drop(runtimes);
        // Snapshot every old binding *before* mutating any fiber, so a failure
        // from here on can undo the whole rebind (FIX-7: the previous `?` path
        // left the runtime table and some fibers half-rewritten).
        let mut saved = Vec::with_capacity(fibers.len());
        for fiber in &fibers {
            let data = match fiber.data.lock() {
                Ok(data) => data,
                Err(_) => {
                    self.undo_rebind(RebindUndo {
                        old_key: old,
                        old_runtime: runtime,
                        target_key: new_key,
                        fibers: saved,
                    });
                    return Err(crate::CordisError::Disposed);
                }
            };
            saved.push((fiber.clone(), data.plugin.clone(), data.runtime_key));
        }
        let undo = RebindUndo {
            old_key: old,
            old_runtime: runtime,
            target_key: new_key,
            fibers: saved,
        };
        if let Ok(mut list) = target.fibers.lock() {
            for fiber in &fibers {
                list.push(fiber.clone());
            }
        }
        for fiber in &fibers {
            if let Err(error) = fiber.swap_plugin(kind.clone()) {
                self.undo_rebind(undo);
                return Err(error);
            }
            let Ok(mut data) = fiber.data.lock() else {
                self.undo_rebind(undo);
                return Err(crate::CordisError::Disposed);
            };
            data.runtime_key = Some(new_key);
        }
        // D-1 self-heal: fibers disposed while this rebind was migrating the
        // runtime table must not stay attached to the new runtime.
        self.heal_disposed_registrations(&fibers);
        Ok((fibers, undo))
    }

    /// Dispose every registered fiber and the root fiber (shutdown path).
    ///
    /// Child trees are reclaimed per fiber; the first error is returned after
    /// every attempt so a single broken cleanup cannot stop the shutdown.
    pub fn dispose_all(&self) -> Result<()> {
        // F1: finish fibers parked by a cancelled cascade before shutting down.
        recover_parked_unloads_blocking(self);
        let fibers = self
            .fibers
            .lock()
            .map(|fibers| fibers.clone())
            .unwrap_or_default();
        let mut first_error = None;
        let mut first_panic: Option<crate::exec::PanicPayload> = None;
        let mut attempt = |fiber: Fiber| {
            if fiber.state() == FiberState::Disposed {
                return;
            }
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fiber.dispose())) {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    first_error.get_or_insert(error);
                }
                Err(payload) => {
                    first_panic.get_or_insert(payload);
                }
            }
        };
        for fiber in fibers {
            attempt(fiber);
        }
        let root = self
            .root_fiber
            .lock()
            .ok()
            .and_then(|slot| slot.as_ref().and_then(|weak| weak.upgrade()));
        if let Some(data) = root {
            attempt(Fiber { data });
        }
        // D-1 residual: a runtime entry left empty by a rolled-back rebind has
        // no fiber to dispose; prune it so shutdown really empties the registry.
        self.prune_empty_runtimes();
        // Surface the outcome only after every fiber was attempted: one broken
        // cleanup must not abort shutdown. A panic wins over an error (only the
        // panic payload is observable to an unwinding caller), but the error is
        // logged instead of being silently dropped (FIX-9).
        match (first_error, first_panic) {
            (Some(error), Some(payload)) => {
                tracing::error!(
                    target: "zeokeel_cordis::registry",
                    "dispose_all: a fiber failed while another panicked: {error}"
                );
                std::panic::resume_unwind(payload);
            }
            (Some(error), None) => Err(error),
            (None, Some(payload)) => std::panic::resume_unwind(payload),
            (None, None) => Ok(()),
        }
    }

    /// Registered runtime keys (Cordis `registry.keys`).
    pub fn keys(&self) -> Vec<PluginKey> {
        self.runtimes
            .lock()
            .map(|runtimes| runtimes.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Registered runtimes (Cordis `registry.values`).
    pub fn values(&self) -> Vec<Arc<PluginRuntime>> {
        self.runtimes
            .lock()
            .map(|runtimes| runtimes.values().cloned().collect())
            .unwrap_or_default()
    }

    /// `(key, runtime)` pairs (Cordis `registry.entries`).
    pub fn entries(&self) -> Vec<(PluginKey, Arc<PluginRuntime>)> {
        self.runtimes
            .lock()
            .map(|runtimes| {
                runtimes
                    .iter()
                    .map(|(key, runtime)| (*key, Arc::clone(runtime)))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Visit every registered runtime (Cordis `registry.forEach`).
    pub fn for_each<F>(&self, mut callback: F)
    where
        F: FnMut(PluginKey, &Arc<PluginRuntime>),
    {
        let entries = self.entries();
        for (key, runtime) in entries {
            callback(key, &runtime);
        }
    }

    /// Every known fiber, including disposed ones still awaiting pruning
    /// (diagnostics; use [`Registry::fiber_count`] for a live count).
    pub fn fibers(&self) -> Vec<Fiber> {
        self.fibers
            .lock()
            .map(|fibers| fibers.clone())
            .unwrap_or_default()
    }

    pub(crate) fn register_fiber(&self, fiber: Fiber) {
        if let Ok(mut fibers) = self.fibers.lock() {
            fibers.retain(|entry| !matches!(entry.state(), FiberState::Disposed));
            fibers.push(fiber);
        }
    }

    /// Attach `fiber` to the runtime of `key`, creating the runtime on first
    /// registration.
    pub(crate) fn register_runtime_fiber(
        &self,
        key: PluginKey,
        name: &'static str,
        fiber: &Fiber,
    ) -> Result<Arc<PluginRuntime>> {
        let runtime = {
            let mut runtimes = self
                .runtimes
                .lock()
                .map_err(|_| crate::CordisError::Disposed)?;
            let runtime = runtimes
                .entry(key)
                .or_insert_with(|| PluginRuntime::new(name.to_string(), key));
            if let Ok(mut fibers) = runtime.fibers.lock() {
                fibers.push(fiber.clone());
            }
            Arc::clone(runtime)
        };
        // D-1 self-heal: `dispose_all` may have finalized this fiber between
        // `register_fiber` and this push (it only needs the registry list).
        self.heal_disposed_registration(fiber);
        Ok(runtime)
    }

    /// Detach a finished fiber from **every** runtime and from the registry
    /// list; runtimes that become empty are removed (Cordis deletes the runtime
    /// in the fiber disposer).
    ///
    /// D-1: matching by fiber id instead of by the fiber's current
    /// `runtime_key` is what makes disposal/rebind races leak-free. A concurrent
    /// rebind may have moved the fiber to another runtime (or may push it there
    /// *after* this scan started), so a per-key removal could miss it and leave
    /// a `Disposed` strong handle inside a runtime forever. Scanning every
    /// runtime makes the detach idempotent and key-independent; the rebind paths
    /// additionally self-heal a late push (`heal_disposed_registration`).
    ///
    /// Lock order: `runtimes` → each `runtime.fibers` (never the reverse), and
    /// `registry.fibers` is taken only after `runtimes` was released. Reading
    /// `fiber.id()` inside a `runtime.fibers` guard follows the existing
    /// `runtime.fibers` → `fiber.data` order.
    pub(crate) fn unregister_runtime_fiber(&self, fiber_id: u64) {
        let mut empty_keys = Vec::new();
        if let Ok(mut runtimes) = self.runtimes.lock() {
            for (key, runtime) in runtimes.iter() {
                let (detached, empty) = match runtime.fibers.lock() {
                    Ok(mut fibers) => {
                        let before = fibers.len();
                        fibers.retain(|fiber| fiber.id() != fiber_id);
                        (fibers.len() != before, fibers.is_empty())
                    }
                    Err(_) => (false, false),
                };
                // Only drop a runtime that this cleanup actually emptied: an
                // empty runtime created by a concurrent registration may still
                // receive its fiber.
                if detached && empty {
                    empty_keys.push(*key);
                }
            }
            for key in empty_keys {
                runtimes.remove(&key);
            }
        }
        // Drop the fiber from the registry list immediately. Keeping disposed
        // fibers here until the next registration pins their context graph
        // (measured ~16 KB leaked per boot in v0.2).
        if let Ok(mut fibers) = self.fibers.lock() {
            fibers.retain(|fiber| fiber.id() != fiber_id);
        }
    }

    /// D-1 self-heal for a fiber that was disposed while a topology change was
    /// in flight: if the fiber is already final, undo the membership that was
    /// published after its cleanup scan.
    fn heal_disposed_registration(&self, fiber: &Fiber) {
        if fiber.state() != FiberState::Disposed {
            return;
        }
        self.unregister_runtime_fiber(fiber.id());
        if let Ok(mut data) = fiber.data.lock() {
            data.runtime_key = None;
        }
    }

    /// [`Registry::heal_disposed_registration`] for a batch of fibers.
    fn heal_disposed_registrations(&self, fibers: &[Fiber]) {
        for fiber in fibers {
            self.heal_disposed_registration(fiber);
        }
    }

    /// Remove runtime entries whose fiber list is empty (D-1 residual).
    ///
    /// A rolled-back rebind can leave an empty runtime behind (s28); since
    /// `dispose_all` only walks fibers, such an entry would survive shutdown and
    /// keep `keys()/size()` non-empty forever. An empty runtime has no members,
    /// so dropping it cannot orphan a fiber; a concurrent registration that is
    /// about to push into it is not possible while it is empty-and-visible
    /// (registration holds the `runtimes` lock for its push).
    fn prune_empty_runtimes(&self) {
        let Ok(mut runtimes) = self.runtimes.lock() else {
            return;
        };
        let mut empty_keys = Vec::new();
        for (key, runtime) in runtimes.iter() {
            let is_empty = runtime
                .fibers
                .lock()
                .map(|fibers| fibers.is_empty())
                .unwrap_or(false);
            if is_empty {
                empty_keys.push(*key);
            }
        }
        for key in empty_keys {
            runtimes.remove(&key);
        }
    }

    /// Mark a plugin registration as in flight until the returned guard drops
    /// (FIX-11b: `is_idle`/`when_idle` must see the registration window).
    pub(crate) fn begin_registration(&self) -> RegistrationGuard<'_> {
        self.pending_registrations.fetch_add(1, Ordering::SeqCst);
        // C4: record which thread owns the registration, so a `when_idle()`
        // reached from inside the window (e.g. an `internal/plugin` hook)
        // excludes it instead of deadlocking, and a guard dropped on another
        // thread still decrements the right owner.
        let created_thread = std::thread::current().id();
        if let Ok(mut owners) = self.registration_owners.lock() {
            *owners.entry(created_thread).or_insert(0) += 1;
        }
        RegistrationGuard {
            registry: self,
            created_thread,
        }
    }

    /// C4: release one registration count owned by `thread` (no underflow).
    fn release_registration_owner(&self, thread: std::thread::ThreadId) {
        if let Ok(mut owners) = self.registration_owners.lock() {
            if let Some(count) = owners.get_mut(&thread) {
                if *count > 1 {
                    *count -= 1;
                } else {
                    owners.remove(&thread);
                }
            }
        }
    }

    /// C4/F3: snapshot `(own registration count of this thread, total pending
    /// registrations)` under **one** lock.
    ///
    /// Reading the two values separately had a TOCTOU window in `when_idle`:
    /// a registration created on this thread between the reads made the waiter
    /// exclude a registration it does not own (returning "idle" too early), or
    /// count one it does own (self-deadlock). The owner map lock is also held
    /// across the total read, so the pair is taken from one consistent epoch
    /// (`begin_registration`/`Drop` update the owner map and the atomic in a
    /// fixed order: the snapshot is conservative in the rare cross-update
    /// window because `saturating_sub` can only *under*-subtract).
    pub(crate) fn quiescence_snapshot(&self) -> (usize, usize) {
        let current = std::thread::current().id();
        let Ok(owners) = self.registration_owners.lock() else {
            return (0, self.pending_registrations.load(Ordering::SeqCst));
        };
        let own = owners.get(&current).copied().unwrap_or(0);
        let total = self.pending_registrations.load(Ordering::SeqCst);
        (own, total)
    }

    /// F1: remember a fiber whose cascade was cancelled mid-unload, together
    /// with the state its frame was targeting (dependents end `Pending`, owned
    /// children end `Disposed`). Deduplicated and pruned so repeated cancels
    /// cannot grow the list without bound.
    pub(crate) fn park_unload(&self, data: &FiberRef, target: FiberState) {
        let Ok(mut parked) = self.parked_unloads.lock() else {
            return;
        };
        parked.retain(|(weak, _)| weak.strong_count() > 0);
        let duplicate = parked.iter().any(|(weak, _)| {
            weak.upgrade()
                .map(|candidate| Arc::ptr_eq(&candidate, data))
                .unwrap_or(false)
        });
        if !duplicate {
            parked.push((Arc::downgrade(data), target));
        }
        drop(parked);
        // F5: wake `when_idle` waiters so they can run the recovery pass.
        self.notify_idle();
    }

    /// F5: whether any parked unload is waiting for recovery (dead entries are
    /// pruned). Used by the recovery-aware idle wait.
    pub(crate) fn has_parked_unloads(&self) -> bool {
        let Ok(mut parked) = self.parked_unloads.lock() else {
            return false;
        };
        parked.retain(|(weak, _)| weak.strong_count() > 0);
        !parked.is_empty()
    }

    /// F1: take every parked fiber that is still `Unloading` (deduplicated),
    /// with the state its frame was targeting. Dead weak entries and entries
    /// whose fiber already left `Unloading` are dropped, so the caller only sees
    /// work that still needs finishing.
    pub(crate) fn take_parked_unloads(&self) -> Vec<(Fiber, FiberState)> {
        let entries = match self.parked_unloads.lock() {
            Ok(mut parked) => std::mem::take(&mut *parked),
            Err(_) => return Vec::new(),
        };
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut fibers = Vec::new();
        for (weak, target) in entries {
            let Some(data) = weak.upgrade() else {
                continue;
            };
            let Ok(fiber) = data.lock() else {
                continue;
            };
            // F4/S7: a cancelled recovery can finish a frame on its target state
            // (`Pending`) while leaving restored async cleanups in
            // `deferred_async`. Those entries stay recoverable until the
            // deferred cleanups are drained; a `Disposed` fiber is left alone
            // (its deferred cleanups are explicit-retry work, A-9).
            let recoverable = fiber.state == FiberState::Unloading
                || (fiber.state != FiberState::Disposed && !fiber.deferred_async.is_empty());
            if !recoverable {
                continue;
            }
            let id = fiber.id;
            drop(fiber);
            if seen.insert(id) {
                fibers.push((Fiber { data }, target));
            }
        }
        fibers
    }

    /// F1: enter the single-recoverer section; `false` means another recoverer
    /// is already running (or the lock is poisoned), so the caller just skips.
    pub(crate) fn begin_recovery(&self) -> bool {
        !self.recovering_parked.swap(true, Ordering::SeqCst)
    }

    /// F1: leave the single-recoverer section.
    pub(crate) fn end_recovery(&self) {
        self.recovering_parked.store(false, Ordering::SeqCst);
    }

    /// F1: mark an unload/cascade body as running (see `active_unloads`).
    pub(crate) fn begin_unload_activity(&self) {
        self.active_unloads.fetch_add(1, Ordering::SeqCst);
    }

    /// F1: pair of [`Registry::begin_unload_activity`]; wakes the
    /// recovery-aware idle waiters once the last unload body finishes (F5).
    pub(crate) fn end_unload_activity(&self) {
        if self.active_unloads.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.notify_idle();
        }
    }

    /// F1: whether any unload/cascade body is currently running.
    pub(crate) fn has_active_unload(&self) -> bool {
        self.active_unloads.load(Ordering::SeqCst) != 0
    }

    /// Enter the refresh fixed point, or mark the registry dirty when another
    /// refresh is already running (the outer loop re-evaluates).
    ///
    /// The returned guard resets the latch on every exit path (FIX-1).
    fn begin_refresh(&self) -> Option<RefreshGuard<'_>> {
        if self
            .refresh_depth
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            self.refresh_dirty.store(true, Ordering::SeqCst);
            return None;
        }
        Some(RefreshGuard { registry: self })
    }

    pub(crate) fn refresh_all(&self) {
        // F1/F4: a **sync** refresh must not recover parked frames: recovery may
        // need to drain reactor-backed async cleanups, and blocking on them from
        // a synchronous caller (possibly a reactor thread) deadlocks. Recovery is
        // owned by the awaitable quiescence entry points (`when_idle`,
        // `refresh_all_async`) and by `dispose_all`; a parked fiber is simply not
        // scanned here (it is `Unloading`, not `Active`/`Pending`).
        let Some(_refresh) = self.begin_refresh() else {
            return;
        };
        for _ in 0..64 {
            let mut progressed = false;
            let fibers = self
                .fibers
                .lock()
                .map(|fibers| fibers.clone())
                .unwrap_or_default();
            for fiber in fibers {
                let state = fiber.state();
                let (ctx, plugin, config, deps, recorded) = {
                    let Ok(data) = fiber.data.lock() else {
                        continue;
                    };
                    let recorded = data
                        .dep_versions
                        .lock()
                        .map(|versions| versions.clone())
                        .unwrap_or_default();
                    (
                        data.ctx_ref(),
                        data.plugin.clone(),
                        data.raw_config.clone(),
                        data.inject,
                        recorded,
                    )
                };
                let Some(ctx) = ctx else { continue };
                match (&plugin, state) {
                    (PluginKind::Sync(_), FiberState::Pending)
                        if dependencies_ready(&ctx, deps) =>
                    {
                        let _ = activate_fiber(&fiber.data, &ctx, &plugin, &config);
                        progressed = true;
                    }
                    (PluginKind::Sync(_), FiberState::Active)
                        if !dependencies_satisfied(&ctx, deps, &recorded) =>
                    {
                        let _ = unload_contained(&fiber.data, FiberState::Pending);
                        progressed = true;
                    }
                    // Async fibers are re-evaluated by `refresh_all_async`; a sync
                    // refresh cannot await their activation/cleanup.
                    _ => {}
                }
            }
            if !progressed && !self.refresh_dirty.swap(false, Ordering::SeqCst) {
                break;
            }
        }
        // `RefreshGuard` resets `refresh_depth` on drop, including a panic path.
    }

    /// Async counterpart of [`Registry::refresh_all`]: also awaits async
    /// plugin activation/unload (called by [`Context::refresh_async`]).
    /// Unload every active sync fiber whose injected services are no longer
    /// satisfied; returns whether anything was retired.
    ///
    /// Used by provider withdrawal to retire dependents (leaf first) before the
    /// provider's own effects are drained.
    /// First `Active` sync fiber whose injected services are no longer
    /// satisfied (C1: iterative unload cascade candidate).
    ///
    /// The unload cascade begins every transitively-unready fiber (mark
    /// `Unloading` + withdraw its services + take its effects) in a fixed-point
    /// loop, then drains them in reverse begin order. Scanning for the first
    /// candidate each round keeps the begin order a topological order: a fiber
    /// can only become unready after one of its providers was begun, so
    /// providers always precede their dependents in the frame list.
    ///
    /// Async fibers are intentionally not returned: the previous
    /// `retire_unready_contained` also retired sync dependents only; async
    /// dependents are re-evaluated by `refresh_async` (parity).
    pub(crate) fn find_unready_active(&self) -> Option<Fiber> {
        let fibers = self.fibers.lock().ok()?;
        for fiber in fibers.iter() {
            if fiber.state() != FiberState::Active {
                continue;
            }
            let (ctx, plugin, deps, recorded) = {
                let Ok(data) = fiber.data.lock() else {
                    continue;
                };
                let recorded = data
                    .dep_versions
                    .lock()
                    .map(|versions| versions.clone())
                    .unwrap_or_default();
                (data.ctx_ref(), data.plugin.clone(), data.inject, recorded)
            };
            let Some(ctx) = ctx else { continue };
            if matches!(plugin, PluginKind::Sync(_))
                && !dependencies_satisfied(&ctx, deps, &recorded)
            {
                return Some(fiber.clone());
            }
        }
        None
    }

    pub(crate) async fn refresh_all_async(&self) {
        // F1/F5: recover parked unloads (awaiting reactor-backed deferred
        // cleanups). If an unload body is in flight, wait until it ends and try
        // again (bounded) instead of missing parked frames that appear later.
        for _ in 0..8 {
            if !self.has_parked_unloads() {
                break;
            }
            if self.has_active_unload() {
                self.wait_idle_or_recoverable().await;
                continue;
            }
            recover_parked_unloads_async(self).await;
        }
        let Some(_refresh) = self.begin_refresh() else {
            return;
        };
        for _ in 0..64 {
            let mut progressed = false;
            let fibers = self
                .fibers
                .lock()
                .map(|fibers| fibers.clone())
                .unwrap_or_default();
            for fiber in fibers {
                let state = fiber.state();
                let (ctx, plugin, config, deps, recorded) = {
                    let Ok(data) = fiber.data.lock() else {
                        continue;
                    };
                    let recorded = data
                        .dep_versions
                        .lock()
                        .map(|versions| versions.clone())
                        .unwrap_or_default();
                    (
                        data.ctx_ref(),
                        data.plugin.clone(),
                        data.raw_config.clone(),
                        data.inject,
                        recorded,
                    )
                };
                let Some(ctx) = ctx else { continue };
                match (&plugin, state) {
                    (_, FiberState::Pending) if dependencies_ready(&ctx, deps) => {
                        if plugin.is_async() {
                            let _ = activate_fiber_async(&fiber.data, &ctx, &plugin, &config).await;
                        } else {
                            let _ = activate_fiber(&fiber.data, &ctx, &plugin, &config);
                        }
                        progressed = true;
                    }
                    (_, FiberState::Active) if !dependencies_satisfied(&ctx, deps, &recorded) => {
                        let _ = unload_contained_async(&fiber.data, FiberState::Pending).await;
                        progressed = true;
                    }
                    _ => {}
                }
            }
            if !progressed && !self.refresh_dirty.swap(false, Ordering::SeqCst) {
                break;
            }
        }
        // `RefreshGuard` resets `refresh_depth` on drop (also if cancelled).
    }
}

#[cfg(test)]
mod registration_window_tests {
    use super::*;

    /// FIX-11b: a plugin registration keeps the registry busy until its guard
    /// drops, and `when_idle` waiters are woken up afterwards.
    #[test]
    fn registration_guard_keeps_the_registry_busy() {
        fn assert_send<T: Send>(_: &T) {}
        let registry = Registry::new();
        assert!(registry.is_idle(), "an empty registry is idle");
        let guard = registry.begin_registration();
        // Held across `.await` by the async plugin entry points.
        assert_send(&guard);
        assert!(
            !registry.is_idle(),
            "FIX-11b: a pending registration must not report idle"
        );
        drop(guard);
        assert!(
            registry.is_idle(),
            "the registry is idle again once the registration finished"
        );
    }

    fn poll_once<F: std::future::Future>(
        future: std::pin::Pin<&mut F>,
    ) -> std::task::Poll<F::Output> {
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        std::future::Future::poll(future, &mut cx)
    }

    /// C4: a registration guard created on thread A and dropped on thread B must
    /// decrement A's count (the `plugin_async` future may be dropped by another
    /// runtime worker). Before the Registry-level `ThreadId -> count` map this
    /// leaked the creating thread's count and could make that thread's later
    /// `when_idle` report idle while other threads still had registrations.
    #[test]
    fn cross_thread_drop_releases_the_creating_thread_registration() {
        // Leaked so the guard (which borrows the registry) can be moved to
        // another thread; the test process is short-lived.
        let registry: &'static Registry = Box::leak(Box::new(Registry::new()));
        let guard = registry.begin_registration();
        assert_eq!(registry.pending_registrations.load(Ordering::SeqCst), 1);
        let dropper = std::thread::spawn(move || {
            // Sanity: the dropping thread does not own the registration.
            assert_eq!(registry.quiescence_snapshot().0, 0);
            drop(guard);
        });
        dropper.join().expect("dropper");
        assert_eq!(
            registry.pending_registrations.load(Ordering::SeqCst),
            0,
            "the creating thread's count must be released from the dropping thread"
        );
        assert!(
            registry.registration_owners.lock().unwrap().is_empty(),
            "no per-thread owner entry may leak"
        );
        assert!(registry.is_idle());
    }

    /// C4: `when_idle` excludes only *this* thread's own registrations. A
    /// registration owned by **another** thread keeps the waiter pending.
    #[test]
    fn when_idle_excludes_only_the_callers_own_registrations() {
        let registry: &'static Registry = Box::leak(Box::new(Registry::new()));
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            let guard = registry.begin_registration();
            started_tx.send(()).expect("started");
            release_rx.recv().expect("release");
            drop(guard);
        });
        started_rx.recv().expect("registration started");
        let mut waiter = Box::pin(registry.when_idle());
        assert!(
            poll_once(waiter.as_mut()).is_pending(),
            "FIX-11b: another thread's registration must keep when_idle pending"
        );
        drop(waiter);
        release_tx.send(()).expect("release");
        holder.join().expect("holder");
        assert!(registry.is_idle());
        let mut ready = Box::pin(registry.when_idle());
        assert!(poll_once(ready.as_mut()).is_ready());
    }

    /// F3/C4 stress: 50 rounds of cross-thread registration ownership with a
    /// `when_idle` poll in each window (the snapshot must never exclude another
    /// thread's registration, and the per-thread map must never leak).
    #[test]
    fn idle_snapshot_excludes_only_others_under_cross_thread_churn() {
        let registry: &'static Registry = Box::leak(Box::new(Registry::new()));
        for _ in 0..50 {
            let (created_tx, created_rx) = std::sync::mpsc::channel();
            let (drop_tx, drop_rx) = std::sync::mpsc::channel();
            let creator = std::thread::spawn(move || {
                let guard = registry.begin_registration();
                created_tx.send(()).expect("created");
                // Hand the guard to a *different* thread, which drops it.
                let dropper = std::thread::spawn(move || {
                    drop_rx.recv().expect("release");
                    drop(guard);
                });
                drop(dropper);
            });
            created_rx.recv().expect("guard created");
            // Another thread owns the registration: an external waiter parks.
            let mut waiter = Box::pin(registry.when_idle());
            assert!(
                poll_once(waiter.as_mut()).is_pending(),
                "F3: another thread's registration must keep when_idle pending"
            );
            drop(waiter);
            drop_tx.send(()).expect("release");
            creator.join().expect("creator");
            // Let the dropper finish before asserting.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while !registry.is_idle() && std::time::Instant::now() < deadline {
                std::thread::yield_now();
            }
            assert!(
                registry.is_idle(),
                "F3: registry must be idle after the round"
            );
            assert!(
                registry.registration_owners.lock().unwrap().is_empty(),
                "F3: the per-thread owner map must not leak entries"
            );
        }
    }

    /// C4: a registration held across yields of a multi-thread runtime (so the
    /// guard may be polled/dropped on a different worker) still releases exactly
    /// once, and an external `when_idle` waits for the whole window.
    #[test]
    fn tokio_multi_thread_migration_does_not_leak_the_registration() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .expect("multi-thread runtime");
        let registry: &'static Registry = Box::leak(Box::new(Registry::new()));
        let started = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        runtime.block_on(async move {
            let started_task = started.clone();
            let release_task = release.clone();
            let task = tokio::spawn(async move {
                let _guard = registry.begin_registration();
                started_task.store(true, Ordering::SeqCst);
                // Yield enough times to let the scheduler move the task between
                // worker threads while the guard is held.
                while !release_task.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
                assert_eq!(registry.pending_registrations.load(Ordering::SeqCst), 1);
            });
            while !started.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            // While the (possibly migrated) registration is in flight, an
            // external waiter on this thread parks.
            let mut waiter = Box::pin(registry.when_idle());
            assert!(
                poll_once(waiter.as_mut()).is_pending(),
                "the migrated registration must still keep an external waiter pending"
            );
            drop(waiter);
            release.store(true, Ordering::SeqCst);
            task.await.expect("registration task");
        });
        assert_eq!(
            registry.pending_registrations.load(Ordering::SeqCst),
            0,
            "a migrated registration must not leak its count"
        );
        assert!(registry.registration_owners.lock().unwrap().is_empty());
        assert!(registry.is_idle());
    }
}
