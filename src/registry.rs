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
use std::sync::{Arc, Mutex};
#[cfg(feature = "supervision")]
use std::time::Duration;

use serde_json::Value;

use crate::context::Context;
use crate::error::Result;
use crate::fiber::{
    activate_fiber, activate_fiber_async, dependencies_ready, dependencies_satisfied,
    unload_contained, unload_contained_async, Fiber, FiberState,
};
#[cfg(feature = "supervision")]
use crate::l2::{FiberQuota, StalledFiber};

static IDLE_SEQ: AtomicU64 = AtomicU64::new(1);

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
/// perform asynchronous initialization (database/settings/network) without
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

impl PluginKind {
    /// Display/diagnostic name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Sync(plugin) => plugin.name(),
            Self::Async(plugin) => plugin.name(),
        }
    }

    /// Registry identity.
    pub fn key(&self) -> PluginKey {
        match self {
            Self::Sync(plugin) => plugin.key(),
            Self::Async(plugin) => plugin.key(),
        }
    }

    /// Declared service dependencies.
    pub fn inject(&self) -> &'static [&'static str] {
        match self {
            Self::Sync(plugin) => plugin.inject(),
            Self::Async(plugin) => plugin.inject(),
        }
    }

    /// Optional config schema.
    pub fn config_schema(&self) -> Option<Arc<dyn crate::config::ConfigSchema>> {
        match self {
            Self::Sync(plugin) => plugin.config_schema(),
            Self::Async(plugin) => plugin.config_schema(),
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
}

/// Future returned by [`Registry::when_idle`]; unregisters its waker on drop.
pub struct IdleWait<'a> {
    registry: &'a Registry,
    id: u64,
}

impl Drop for IdleWait<'_> {
    fn drop(&mut self) {
        if let Ok(mut waiters) = self.registry.idle_waiters.lock() {
            waiters.retain(|(id, _)| *id != self.id);
        }
    }
}

impl std::future::Future for IdleWait<'_> {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        let this = self.get_mut();
        if this.registry.is_idle() {
            return std::task::Poll::Ready(());
        }
        if let Ok(mut waiters) = this.registry.idle_waiters.lock() {
            waiters.retain(|(id, _)| *id != this.id);
            waiters.push((this.id, cx.waker().clone()));
        }
        if this.registry.is_idle() {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
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
}

impl Drop for RegistrationGuard<'_> {
    fn drop(&mut self) {
        // Decrement first, then wake waiters so a re-poll observes the new
        // state (the last registration may be the only thing keeping it busy).
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
        if self.pending_registrations.load(Ordering::SeqCst) != 0 {
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

    /// Resolve once every fiber (including the root fiber) is idle.
    pub fn when_idle(&self) -> IdleWait<'_> {
        IdleWait {
            registry: self,
            id: IDLE_SEQ.fetch_add(1, Ordering::Relaxed),
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
        RegistrationGuard { registry: self }
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
    pub(crate) fn retire_unready_contained(&self) -> bool {
        let fibers = self
            .fibers
            .lock()
            .map(|fibers| fibers.clone())
            .unwrap_or_default();
        let mut progressed = false;
        for fiber in fibers {
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
                let _ = unload_contained(&fiber.data, FiberState::Pending);
                progressed = true;
            }
        }
        progressed
    }

    pub(crate) async fn refresh_all_async(&self) {
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
}
