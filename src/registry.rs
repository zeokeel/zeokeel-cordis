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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::context::Context;
use crate::error::Result;
use crate::fiber::{
    activate_fiber, activate_fiber_async, dependencies_ready, dependencies_satisfied, unload_fiber,
    unload_fiber_async, Fiber, FiberState,
};

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
    /// Re-entrancy guard for the refresh fixed point: a dependent evaluation
    /// triggered from inside a refresh marks the registry dirty instead of
    /// recursing (prevents exponential re-entrant scans).
    refresh_depth: AtomicUsize,
    refresh_dirty: AtomicBool,
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

/// Snapshot taken before a rebind so a rejected replace can be undone.
struct RebindUndo {
    old_key: PluginKey,
    old_runtime: Arc<PluginRuntime>,
    target_key: PluginKey,
    target_created: bool,
    fibers: Vec<(Fiber, PluginKind, Option<PluginKey>)>,
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

    /// Whether the whole registry is quiescent: no lifecycle operation is in
    /// flight and no fiber (including the root fiber) is loading/unloading.
    pub fn is_idle(&self) -> bool {
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
    /// implementation's key becomes the runtime identity. A reload rejected by
    /// a busy fiber (`LifecycleBusy`) rolls the rebind back atomically.
    pub fn replace(&self, old: PluginKey, new: Arc<dyn Plugin>) -> Result<Vec<Fiber>> {
        let (fibers, undo) = self.rebind_runtime(old, PluginKind::Sync(new))?;
        for fiber in &fibers {
            if let Err(error) = fiber.restart() {
                self.undo_rebind(undo);
                return Err(error);
            }
        }
        Ok(fibers)
    }

    /// Awaiting variant of [`Registry::replace`] for async plugins.
    pub async fn replace_async(
        &self,
        old: PluginKey,
        new: Arc<dyn AsyncPlugin>,
    ) -> Result<Vec<Fiber>> {
        let (fibers, undo) = self.rebind_runtime(old, PluginKind::Async(new))?;
        for fiber in &fibers {
            if let Err(error) = fiber.restart_async().await {
                self.undo_rebind(undo);
                return Err(error);
            }
        }
        Ok(fibers)
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
        let mut data = fiber
            .data
            .lock()
            .map_err(|_| crate::CordisError::Disposed)?;
        data.runtime_key = Some(new_key);
        Ok(())
    }

    fn undo_rebind(&self, undo: RebindUndo) {
        if let Ok(mut runtimes) = self.runtimes.lock() {
            if undo.target_created {
                if let Some(target) = runtimes.get(&undo.target_key) {
                    if let Ok(mut fibers) = target.fibers.lock() {
                        let ids: Vec<u64> =
                            undo.fibers.iter().map(|(fiber, _, _)| fiber.id()).collect();
                        fibers.retain(|candidate| !ids.contains(&candidate.id()));
                    }
                    let empty = target
                        .fibers
                        .lock()
                        .map(|fibers| fibers.is_empty())
                        .unwrap_or(false);
                    if empty {
                        runtimes.remove(&undo.target_key);
                    }
                }
            } else if let Some(target) = runtimes.get(&undo.target_key) {
                if let Ok(mut fibers) = target.fibers.lock() {
                    let ids: Vec<u64> =
                        undo.fibers.iter().map(|(fiber, _, _)| fiber.id()).collect();
                    fibers.retain(|candidate| !ids.contains(&candidate.id()));
                }
            }
            runtimes.insert(undo.old_key, undo.old_runtime);
        }
        for (fiber, kind, runtime_key) in undo.fibers {
            let _ = fiber.swap_plugin(kind);
            if let Ok(mut data) = fiber.data.lock() {
                data.runtime_key = runtime_key;
            }
        }
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
        let target_was_created = !runtimes.contains_key(&new_key);
        let target: Arc<PluginRuntime> = match runtimes.get(&new_key) {
            Some(existing) => Arc::clone(existing),
            None => {
                let created: Arc<PluginRuntime> = PluginRuntime::new(name.to_string(), new_key);
                runtimes.insert(new_key, Arc::clone(&created));
                created
            }
        };
        drop(runtimes);
        if let Ok(mut list) = target.fibers.lock() {
            for fiber in &fibers {
                list.push(fiber.clone());
            }
        }
        let mut saved = Vec::with_capacity(fibers.len());
        for fiber in &fibers {
            let saved_kind = fiber
                .data
                .lock()
                .map_err(|_| crate::CordisError::Disposed)?
                .plugin
                .clone();
            let saved_key = fiber.data.lock().ok().and_then(|data| data.runtime_key);
            fiber.swap_plugin(kind.clone())?;
            let mut data = fiber
                .data
                .lock()
                .map_err(|_| crate::CordisError::Disposed)?;
            data.runtime_key = Some(new_key);
            saved.push((fiber.clone(), saved_kind, saved_key));
        }
        let target_created = target_was_created;
        Ok((
            fibers,
            RebindUndo {
                old_key: old,
                old_runtime: runtime,
                target_key: new_key,
                target_created,
                fibers: saved,
            },
        ))
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
        // Surface the outcome only after every fiber was attempted: one broken
        // cleanup must not abort shutdown.
        if let Some(error) = first_error {
            return Err(error);
        }
        if let Some(payload) = first_panic {
            std::panic::resume_unwind(payload);
        }
        Ok(())
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
        Ok(Arc::clone(runtime))
    }

    /// Detach a disposed fiber from its runtime; the runtime is removed when its
    /// last fiber is gone (Cordis deletes the runtime in the fiber disposer).
    pub(crate) fn unregister_runtime_fiber(&self, key: PluginKey, fiber_id: u64) {
        let mut runtimes = match self.runtimes.lock() {
            Ok(runtimes) => runtimes,
            Err(_) => return,
        };
        let empty = match runtimes.get(&key) {
            Some(runtime) => match runtime.fibers.lock() {
                Ok(mut fibers) => {
                    fibers.retain(|fiber| fiber.id() != fiber_id);
                    fibers.is_empty()
                }
                Err(_) => false,
            },
            None => false,
        };
        if empty {
            runtimes.remove(&key);
        }
        drop(runtimes);
        // Drop the fiber from the registry list immediately. Keeping disposed
        // fibers here until the next registration pins their context graph
        // (measured ~16 KB leaked per boot in v0.2).
        if let Ok(mut fibers) = self.fibers.lock() {
            fibers.retain(|fiber| fiber.id() != fiber_id);
        }
    }

    pub(crate) fn refresh_all(&self) {
        if self
            .refresh_depth
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            self.refresh_dirty.store(true, Ordering::SeqCst);
            return;
        }
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
                        let _ = unload_fiber(&fiber.data, FiberState::Pending);
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
        self.refresh_depth.store(0, Ordering::SeqCst);
    }

    /// Async counterpart of [`Registry::refresh_all`]: also awaits async
    /// plugin activation/unload (called by [`Context::refresh_async`]).
    /// Unload every active sync fiber whose injected services are no longer
    /// satisfied; returns whether anything was retired.
    ///
    /// Used by provider withdrawal to retire dependents (leaf first) before the
    /// provider's own effects are drained.
    pub(crate) fn retire_unready(&self) -> bool {
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
                let _ = unload_fiber(&fiber.data, FiberState::Pending);
                progressed = true;
            }
        }
        progressed
    }

    pub(crate) async fn refresh_all_async(&self) {
        if self
            .refresh_depth
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            self.refresh_dirty.store(true, Ordering::SeqCst);
            return;
        }
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
                        let _ = unload_fiber_async(&fiber.data, FiberState::Pending).await;
                        progressed = true;
                    }
                    _ => {}
                }
            }
            if !progressed && !self.refresh_dirty.swap(false, Ordering::SeqCst) {
                break;
            }
        }
        self.refresh_depth.store(0, Ordering::SeqCst);
    }
}
