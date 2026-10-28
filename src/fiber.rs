//! Plugin fibers: lifecycle state machine, effect ownership and unloading.

use std::cell::RefCell;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Poll, Waker};

use serde_json::Value;

use crate::async_effects::AsyncDisposer;
use crate::context::{Context, ContextInner};
use crate::effect::Disposer;
use crate::error::{CordisError, Result};
use crate::registry::{AsyncPlugin, Plugin, PluginKey, PluginKind, Registry};

/// Diagnostics node for one live effect (Cordis `EffectMeta`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectMeta {
    /// Human-readable label, e.g. `ctx.on("event")` or `ctx.plugin()`.
    pub label: String,
    /// Metadata of nested effects registered while this effect ran.
    pub children: Vec<EffectMeta>,
}

/// Mutable effect tree node stored on the fiber; children are shared so nested
/// registrations can still append while the parent effect is running.
#[derive(Debug, Default)]
pub(crate) struct EffectNode {
    pub(crate) label: String,
    pub(crate) children: Vec<Arc<Mutex<EffectNode>>>,
    /// Parent node when this effect was registered while another effect was
    /// running; `Fiber::get_effects` only reports top-level nodes.
    pub(crate) parent: Option<std::sync::Weak<Mutex<EffectNode>>>,
}

impl EffectNode {
    pub(crate) fn new(label: &'static str) -> Arc<Mutex<EffectNode>> {
        Arc::new(Mutex::new(EffectNode {
            label: label.to_string(),
            children: Vec::new(),
            parent: None,
        }))
    }

    pub(crate) fn snapshot(&self) -> EffectMeta {
        EffectMeta {
            label: self.label.clone(),
            children: self
                .children
                .iter()
                .filter_map(|child| child.lock().ok().map(|child| child.snapshot()))
                .collect(),
        }
    }
}

/// Cleanup handle of one effect (sync or async lane).
pub(crate) enum EffectKind {
    Sync(Disposer),
    Async(AsyncDisposer),
}

/// One effect owned by a fiber: its cleanup handle and its diagnostics node.
///
/// Sync and async effects share a single ordered list, so `Fiber::dispose`
/// always drains them in reverse *registration* order regardless of which
/// effect lane registered them.
pub(crate) struct FiberEffect {
    pub(crate) kind: EffectKind,
    pub(crate) meta: Arc<Mutex<EffectNode>>,
    /// Activation generation that registered this effect (stale-rollback tag).
    pub(crate) activation: u64,
}

impl FiberEffect {
    fn is_disposed(&self) -> bool {
        match &self.kind {
            EffectKind::Sync(disposer) => disposer.is_disposed(),
            EffectKind::Async(disposer) => disposer.is_disposed(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FiberState {
    Pending,
    Loading,
    Active,
    Failed,
    Unloading,
    Disposed,
}

/// How a fiber links to a context.
///
/// Child fibers keep a strong handle to their own context (released on dispose,
/// so the context↔fiber edge does not survive unloading); the root fiber keeps
/// **weak** self-links so a dropped root really frees the whole graph
/// (CORE-031).
#[derive(Clone)]
pub(crate) enum FiberLink {
    Strong(Context),
    Weak(Weak<ContextInner>),
}

impl FiberLink {
    pub(crate) fn get(&self) -> Option<Context> {
        match self {
            Self::Strong(ctx) => Some(ctx.clone()),
            Self::Weak(weak) => Context::from_weak(weak),
        }
    }
}

pub type FiberRef = Arc<Mutex<FiberData>>;

/// One plugin activation. A fiber is created for each `Context::plugin` call,
/// even when the same plugin is registered more than once (like Cordis).
pub struct FiberData {
    pub id: u64,
    pub state: FiberState,
    /// Plugin implementation hosted by this fiber (sync or async).
    pub(crate) plugin: PluginKind,
    pub inject: &'static [&'static str],
    /// Resolved (validated/normalized) config currently in effect.
    pub config: Value,
    /// Config exactly as supplied by the caller / update hook. Re-resolved on
    /// every activation so derived values are not compounded (Cordis `_config`).
    pub(crate) raw_config: Value,
    pub error: Option<String>,
    /// Parent context link (`Weak` only for the root fiber; CORE-031).
    pub(crate) parent: Option<FiberLink>,
    /// The fiber's own context link (released on dispose).
    pub(crate) ctx: Option<FiberLink>,
    pub(crate) effects: Vec<FiberEffect>,
    /// Async cleanups deferred by a failed synchronous activation: they are
    /// awaited by `dispose`/`dispose_async` because a sync rollback cannot
    /// await them inline (deferred rollback drain, CORE-038; cancellation
    /// semantics, CORE-046).
    pub(crate) deferred_async: Vec<AsyncDisposer>,
    /// Registry identity of the plugin runtime this fiber belongs to.
    pub(crate) runtime_key: Option<PluginKey>,
    /// Stack of effect nodes whose bodies are currently running (nested effect
    /// registration attaches to the top node).
    pub(crate) effect_stack: Vec<Arc<Mutex<EffectNode>>>,
    /// Versions of the injected services this fiber activated against.
    pub(crate) dep_versions: Mutex<Vec<(&'static str, u64)>>,
    /// Registry that owns this fiber (used for quiescence notifications even
    /// after the fiber context link is cleared on dispose).
    pub(crate) registry: Option<Arc<Registry>>,
    /// Child fibers loaded from this fiber's context (weak handles).
    pub(crate) children: Mutex<Vec<Weak<Mutex<FiberData>>>>,
    /// A dispose was requested while another lifecycle operation owned the
    /// fiber; the owner finalizes it when it finishes.
    pub(crate) dispose_requested: AtomicBool,
    /// Cooperative cancellation flag observed by `Context::checkpoint`.
    pub(crate) cancel_requested: AtomicBool,
    /// Live listeners registered on this fiber's behalf (quota accounting).
    #[cfg_attr(not(feature = "supervision"), allow(dead_code))]
    pub(crate) listener_count: usize,
    /// Last observed activity (heartbeat / lifecycle operation), for watchdogs.
    pub(crate) last_activity: Mutex<std::time::Instant>,
    /// Generation counter of activation attempts; a stale attempt must not
    /// transition or roll back a newer one.
    pub(crate) activation_epoch: AtomicU64,
    /// Token of the thread currently running a lifecycle operation (0 = idle).
    pub(crate) lifecycle_owner: AtomicU64,
    /// Reentrancy depth of the owning thread.
    pub(crate) lifecycle_depth: AtomicUsize,
    /// In-flight lifecycle operations (`await_settled` waits for zero).
    pub(crate) pending_ops: AtomicUsize,
    /// Wakers of `await_settled` callers, tagged with the waiting future id
    /// so a dropped future can unregister its waker (CORE-044).
    pub(crate) settle_waiters: Mutex<Vec<(u64, Waker)>>,
}

impl FiberData {
    /// Resolve the parent context (None once a weak parent is gone).
    pub(crate) fn parent_ref(&self) -> Option<Context> {
        self.parent.as_ref().and_then(FiberLink::get)
    }

    /// Resolve the fiber context (None after dispose).
    pub(crate) fn ctx_ref(&self) -> Option<Context> {
        self.ctx.as_ref().and_then(FiberLink::get)
    }
}

impl Drop for FiberData {
    fn drop(&mut self) {
        // FIX-10: a fiber dropped without an explicit dispose must not leave
        // event listeners owned by it behind. Resolve the context first (the
        // root fiber's weak self-link is already dead here); the owner sweep is
        // contained so a panicking listener disposer cannot unwind out of
        // `Drop`. `self` is `&mut FiberData`, so no lock is taken on it.
        if let Some(ctx) = self.ctx.as_ref().and_then(FiberLink::get) {
            let owner = self.id;
            let _ = catch_unwind(AssertUnwindSafe(|| {
                ctx.inner.events.remove_owner(owner);
            }));
        }
        if self.effects.is_empty() && self.deferred_async.is_empty() {
            return;
        }
        // A fiber dropped without an explicit dispose must not leak its
        // registrations. Cleanups are arbitrary embedder code: never unwind
        // from Drop and never block while the thread is unwinding.
        let effects = std::mem::take(&mut self.effects);
        let deferred = std::mem::take(&mut self.deferred_async);
        let panicking = std::thread::panicking();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            if panicking {
                for effect in effects.into_iter().rev() {
                    if let EffectKind::Sync(disposer) = effect.kind {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            disposer.dispose()
                        }));
                    }
                }
                tracing::error!(
                    target: "zeokeel_cordis::fiber",
                    "fiber dropped during unwinding; async cleanups were not run"
                );
            } else {
                let _ = drain_effects_blocking(effects, deferred);
            }
        }));
    }
}

/// Public fiber handle.
#[derive(Clone)]
pub struct Fiber {
    pub(crate) data: FiberRef,
}

impl Fiber {
    /// Display name of the fiber: the plugin name (`root` for the root fiber).
    ///
    /// Cordis falls back to the nearest named ancestor; Rust plugins always
    /// carry a name, so this is the plugin's own `name()`.
    pub fn name(&self) -> String {
        // `Plugin::name()` is embedder code: clone the handle under the lock
        // and call it outside so a panicking name cannot poison the fiber.
        let plugin = self.data.lock().ok().map(|data| data.plugin.clone());
        match plugin {
            Some(plugin) => plugin.name().to_string(),
            None => "root".to_string(),
        }
    }

    pub fn id(&self) -> u64 {
        self.data.lock().map(|data| data.id).unwrap_or(0)
    }

    /// Fiber id, or `None` once disposed (Cordis `fiber.uid`).
    pub fn uid(&self) -> Option<u64> {
        match self.state() {
            FiberState::Disposed => None,
            _ => Some(self.id()),
        }
    }

    /// The context the plugin was loaded from (Cordis `fiber.parent`).
    pub fn parent(&self) -> Option<Context> {
        self.data.lock().ok().and_then(|data| data.parent_ref())
    }

    /// Metadata of the currently live effects, including nested labels
    /// (Cordis `fiber.getEffects`).
    pub fn get_effects(&self) -> Vec<EffectMeta> {
        let Ok(data) = self.data.lock() else {
            return Vec::new();
        };
        data.effects
            .iter()
            .filter(|effect| !effect.is_disposed())
            .filter_map(|effect| {
                let meta = effect.meta.lock().ok()?;
                if meta.parent.is_some() {
                    return None;
                }
                Some(meta.snapshot())
            })
            .collect()
    }

    /// Wait until in-flight lifecycle work settled, then rethrow startup errors
    /// (Cordis `await fiber`).
    pub async fn await_settled(&self) -> Result<()> {
        self.settled().await
    }

    /// The future form of [`Fiber::await_settled`], for combinators.
    pub fn settled(&self) -> Settled<'_> {
        Settled {
            data: &self.data,
            id: SETTLED_SEQ.fetch_add(1, Ordering::Relaxed) as u64,
        }
    }

    pub fn state(&self) -> FiberState {
        self.data
            .lock()
            .map(|data| data.state)
            .unwrap_or(FiberState::Disposed)
    }

    pub fn is_active(&self) -> bool {
        self.state() == FiberState::Active
    }

    /// Error unless the fiber has not been disposed yet (Cordis
    /// `Fiber.assertActive`).
    pub fn assert_active(&self) -> Result<()> {
        if self.state() == FiberState::Disposed {
            Err(CordisError::InactiveEffect)
        } else {
            Ok(())
        }
    }

    /// Update this fiber's activity timestamp (embedder heartbeat).
    ///
    /// Long-running plugins should call `ctx.heartbeat()` periodically so an
    /// embedder watchdog can distinguish "busy" from "stuck".
    #[cfg(feature = "supervision")]
    pub fn heartbeat(&self) {
        if let Ok(fiber) = self.data.lock() {
            if let Ok(mut at) = fiber.last_activity.lock() {
                *at = std::time::Instant::now();
            }
        }
    }

    /// When this fiber last reported activity (lifecycle step or heartbeat).
    #[cfg(feature = "supervision")]
    pub fn last_activity(&self) -> std::time::Instant {
        self.data
            .lock()
            .ok()
            .and_then(|fiber| fiber.last_activity.lock().ok().map(|at| *at))
            .unwrap_or_else(std::time::Instant::now)
    }

    /// How long this fiber has been quiet.
    #[cfg(feature = "supervision")]
    pub fn idle_for(&self) -> std::time::Duration {
        self.last_activity().elapsed()
    }

    /// Request cooperative cancellation: plugins observe it through
    /// `Context::checkpoint` and can return early (e.g. `CordisError::Cancelled`).
    pub fn cancel(&self) {
        if let Ok(fiber) = self.data.lock() {
            fiber.cancel_requested.store(true, Ordering::SeqCst);
        }
    }

    /// Whether cooperative cancellation was requested for this fiber.
    pub fn is_cancelled(&self) -> bool {
        self.data
            .lock()
            .map(|fiber| fiber.cancel_requested.load(Ordering::SeqCst))
            .unwrap_or(true)
    }

    /// The context this fiber's plugin runs in (Cordis `fiber.ctx`).
    pub fn context(&self) -> Option<Context> {
        self.data.lock().ok().and_then(|data| data.ctx_ref())
    }

    /// The current (validated) plugin config.
    pub fn config(&self) -> Value {
        self.data
            .lock()
            .map(|data| data.config.clone())
            .unwrap_or(Value::Null)
    }

    /// Store the caller-supplied config that the next activation resolves.
    fn store_raw_config(&self, config: Value) {
        if let Ok(mut data) = self.data.lock() {
            data.raw_config = config;
        }
    }

    /// Config exactly as supplied by the caller (Cordis `fiber._config`).
    pub fn raw_config(&self) -> Value {
        self.data
            .lock()
            .map(|data| data.raw_config.clone())
            .unwrap_or(Value::Null)
    }

    /// Swap the plugin implementation in place (used by `Registry::replace`).
    ///
    /// Keeps fiber identity, raw config and effects; the caller reloads the
    /// fiber afterwards.
    pub(crate) fn swap_plugin(&self, plugin: PluginKind) -> Result<()> {
        // B-3: `inject()` is embedder code; evaluating it under the FiberData
        // lock would poison the fiber if it panics (state() then reports
        // Disposed, effects become unreachable and provided services can never
        // be withdrawn). It is a pure declaration accessor, so compute it first.
        let inject = plugin.inject();
        let mut data = self.data.lock().map_err(|_| CordisError::Disposed)?;
        data.inject = inject;
        data.plugin = plugin;
        Ok(())
    }

    /// Replace this fiber's plugin with a new sync implementation, re-key its
    /// runtime and reload it (Cordis `registry.replace` for one fiber).
    ///
    /// The whole swap+rebind+reload is one lifecycle operation (FIX-8): a
    /// concurrent dispose/restart/update receives [`CordisError::LifecycleBusy`]
    /// instead of racing the plugin swap, and `when_idle`/`await_settled`
    /// observe the operation. The runtime move and the reload are rolled back
    /// when the reload is rejected, so a failed replace does not leave a
    /// partially re-bound fiber (the root fiber's temporary runtime is removed
    /// as well).
    pub fn replace_plugin(&self, plugin: Arc<dyn Plugin>) -> Result<()> {
        let _guard = begin_operation(&self.data);
        let _lifecycle = begin_lifecycle(&self.data)?;
        let _finalize = FinalizeGuard { data: &self.data };
        let kind = PluginKind::Sync(plugin);
        let (old_kind, old_key, registry) = self.begin_replace(&kind)?;
        if let Some(registry) = registry.as_ref() {
            if let Err(error) = registry.rebind_single_fiber(self, old_key, kind.key()) {
                self.restore_replace(old_kind, old_key, Some(registry));
                return Err(error);
            }
        }
        // D-2: the reload drains the old implementation's cleanups (embedder
        // code); a panic must not escape this `Result` API and skip the
        // rollback below.
        match catch_unwind(AssertUnwindSafe(|| self.restart())) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                self.restore_replace(old_kind, old_key, registry.as_ref());
                self.reload_after_failed_replace();
                Err(error)
            }
            Err(payload) => {
                let error = CordisError::PluginFailed(format!(
                    "replace_plugin: reload panicked: {}",
                    panic_message(payload.as_ref())
                ));
                self.restore_replace(old_kind, old_key, registry.as_ref());
                self.reload_after_failed_replace();
                Err(error)
            }
        }
    }

    /// Replace this fiber's plugin with a new async implementation and reload it.
    pub async fn replace_plugin_async(&self, plugin: Arc<dyn AsyncPlugin>) -> Result<()> {
        let _guard = begin_operation(&self.data);
        let _lifecycle = begin_lifecycle(&self.data)?;
        let _finalize = FinalizeGuard { data: &self.data };
        let kind = PluginKind::Async(plugin);
        let (old_kind, old_key, registry) = self.begin_replace(&kind)?;
        if let Some(registry) = registry.as_ref() {
            if let Err(error) = registry.rebind_single_fiber(self, old_key, kind.key()) {
                self.restore_replace(old_kind, old_key, Some(registry));
                return Err(error);
            }
        }
        let outcome =
            futures_util::FutureExt::catch_unwind(AssertUnwindSafe(self.restart_async())).await;
        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                self.restore_replace(old_kind, old_key, registry.as_ref());
                self.reload_after_failed_replace_async().await;
                Err(error)
            }
            Err(payload) => {
                let error = CordisError::PluginFailed(format!(
                    "replace_plugin_async: reload panicked: {}",
                    panic_message(payload.as_ref())
                ));
                self.restore_replace(old_kind, old_key, registry.as_ref());
                self.reload_after_failed_replace_async().await;
                Err(error)
            }
        }
    }

    /// Re-run the restored (old) implementation after a failed in-place replace
    /// if the failed reload already deactivated the fiber (D-2).
    ///
    /// Contained: the recovery must not panic either; a fiber left `Pending` is
    /// still safe (it owns no effects), and the next lifecycle operation can
    /// reload it.
    fn reload_after_failed_replace(&self) {
        if self.state() == FiberState::Active {
            return;
        }
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _ = self.restart();
        }));
    }

    /// Awaiting variant of [`Fiber::reload_after_failed_replace`].
    async fn reload_after_failed_replace_async(&self) {
        if self.state() == FiberState::Active {
            return;
        }
        let _ = futures_util::FutureExt::catch_unwind(AssertUnwindSafe(self.restart_async())).await;
    }

    fn begin_replace(
        &self,
        kind: &PluginKind,
    ) -> Result<(PluginKind, Option<PluginKey>, Option<Arc<Registry>>)> {
        // B-3: compute the embedder-provided injection list *before* taking the
        // fiber lock (a panicking `inject()` must not poison it).
        let inject = kind.inject();
        let mut data = self.data.lock().map_err(|_| CordisError::Disposed)?;
        // A disposed fiber is final: replacing its plugin must not resurrect it.
        if data.state == FiberState::Disposed {
            return Err(CordisError::Disposed);
        }
        let old_kind = data.plugin.clone();
        let old_key = data.runtime_key;
        let registry = data.ctx_ref().map(|ctx| ctx.inner.registry.clone());
        data.inject = inject;
        data.plugin = kind.clone();
        Ok((old_kind, old_key, registry))
    }

    fn restore_replace(
        &self,
        old_kind: PluginKind,
        old_key: Option<PluginKey>,
        registry: Option<&Arc<Registry>>,
    ) {
        let _ = self.swap_plugin(old_kind);
        let Some(registry) = registry else {
            return;
        };
        let current_key = self.data.lock().ok().and_then(|data| data.runtime_key);
        match old_key {
            Some(old_key) => {
                let _ = registry.rebind_single_fiber(self, current_key, old_key);
            }
            None => {
                // FIX-8 / D-1: the root fiber starts with no runtime key. A
                // failed `replace_plugin` must not leave the temporary runtime
                // (and the root fiber hanging in it) behind in the registry;
                // the detach is key-independent, so a concurrent rebind cannot
                // hide the membership under another key.
                if current_key.is_some() {
                    registry.unregister_runtime_fiber(self.id());
                    if let Ok(mut data) = self.data.lock() {
                        data.runtime_key = None;
                    }
                }
            }
        }
    }

    pub fn error(&self) -> Option<String> {
        self.data.lock().ok().and_then(|data| data.error.clone())
    }

    /// Dispose the fiber: unload effects in reverse order, remove services and
    /// listeners, then mark it `Disposed`. Idempotent.
    ///
    /// Async effects are awaited on this thread (see [`crate::block_on`]) so the
    /// call is still deterministic; use [`Fiber::dispose_async`] from async code
    /// so reactor-backed cleanups can make progress.
    pub fn dispose(&self) -> Result<()> {
        let _guard = begin_operation(&self.data);
        let _lifecycle = begin_lifecycle(&self.data)?;
        unload_fiber(&self.data, FiberState::Disposed)
    }

    /// Await-dispose the fiber: sync and async effects run in one reverse
    /// registration order, and every async cleanup is awaited before the fiber
    /// is marked `Disposed`. Idempotent.
    pub async fn dispose_async(&self) -> Result<()> {
        let _guard = begin_operation(&self.data);
        let _lifecycle = begin_lifecycle(&self.data)?;
        unload_fiber_async(&self.data, FiberState::Disposed).await
    }

    /// Unload then activate again with the current config (hot reload).
    ///
    /// Async effects are drained with the blocking fallback; use
    /// [`Fiber::restart_async`] in async code. Async plugins must use
    /// [`Fiber::restart_async`] (they are rejected with
    /// [`CordisError::AsyncPluginNeedsAsync`] here).
    ///
    /// The `Active -> Unloading` transition is announced through
    /// `internal/status` (FIX-6) and every return path (including `?` and a
    /// cleanup panic) finalizes a dispose that was requested while this
    /// operation owned the fiber (FIX-3).
    pub fn restart(&self) -> Result<()> {
        let _guard = begin_operation(&self.data);
        let _lifecycle = begin_lifecycle(&self.data)?;
        let _finalize = FinalizeGuard { data: &self.data };
        let Some((ctx, plugin, config)) = self.begin_restart(false)? else {
            return Ok(());
        };
        unload_fiber(&self.data, FiberState::Pending)?;
        if dependencies_ready(&ctx, plugin.inject()) {
            let _ = activate_fiber(&self.data, &ctx, &plugin, &config);
        }
        Ok(())
    }

    /// Awaiting variant of [`Fiber::restart`].
    pub async fn restart_async(&self) -> Result<()> {
        let _guard = begin_operation(&self.data);
        let _lifecycle = begin_lifecycle(&self.data)?;
        let _finalize = FinalizeGuard { data: &self.data };
        let Some((ctx, plugin, config)) = self.begin_restart(true)? else {
            return Ok(());
        };
        unload_fiber_async(&self.data, FiberState::Pending).await?;
        if dependencies_ready(&ctx, plugin.inject()) {
            let _ = activate_by_kind_async(&self.data, &ctx, &plugin, &config).await;
        }
        Ok(())
    }

    /// Validate the fiber is restartable, then announce `Unloading` through
    /// [`transition`] and re-read the activation inputs.
    ///
    /// `allow_async` is `false` for the synchronous [`Fiber::restart`] entry
    /// point (async plugins must use [`Fiber::restart_async`]) and `true` for
    /// the awaiting one.
    ///
    /// The lifecycle slot is already held, so only a reentrant status hook can
    /// change the fiber between the check and the reload; such a hook disposing
    /// the fiber leaves no context, in which case the caller skips the reload.
    fn begin_restart(&self, allow_async: bool) -> Result<Option<(Context, PluginKind, Value)>> {
        {
            let data = self.data.lock().map_err(|_| CordisError::Disposed)?;
            // A disposed fiber is final: restarting it must not resurrect it.
            if data.state == FiberState::Disposed {
                return Err(CordisError::Disposed);
            }
            // Reentrant restart/update from a cleanup must not start a new
            // activation while the old effect list is being drained.
            if data.state == FiberState::Unloading {
                return Err(CordisError::InactiveEffect);
            }
            if !allow_async && data.plugin.is_async() {
                return Err(CordisError::AsyncPluginNeedsAsync(
                    data.plugin.name().to_string(),
                ));
            }
        }
        transition(&self.data, FiberState::Unloading);
        let data = self.data.lock().map_err(|_| CordisError::Disposed)?;
        Ok(data
            .ctx_ref()
            .map(|ctx| (ctx, data.plugin.clone(), data.raw_config.clone())))
    }

    /// Replace config and restart (Cordis `fiber.update`).
    ///
    /// Order of operations (matching Cordis `_resolveConfig` + `internal/update`):
    /// 1. run the `internal/config` waterfall on the raw config;
    /// 2. validate it against [`Plugin::config_schema`] (Cordis
    ///    `ValidationError`) — on failure the fiber keeps its old config and is
    ///    **not** restarted;
    /// 3. run the `internal/update` waterfall; only its terminal commits the
    ///    raw config and asks for the restart. A hook that never calls `next`
    ///    vetoes the whole update: `Ok(())`, no restart, raw config unchanged.
    ///
    /// `no_save` is only a **hint for persistence hooks** (Cordis
    /// `update(config, noSave)`), never a kernel-level "do not keep the
    /// config": the reference sets `this._config = config` unconditionally and
    /// lets the terminal assign + restart, so a successful (non-vetoed) update
    /// always keeps the new raw config here too (TS parity, FIX-14).
    ///
    /// FIX-3: `FinalizeGuard` covers every return path of this function,
    /// including early errors and a cleanup panic, so a dispose requested while
    /// this operation owned the fiber is never lost (no permanent Active
    /// orphan). FIX-14 moved the raw-config commit into the waterfall terminal,
    /// so a vetoed update cannot leak its rejected config.
    pub fn update(&self, config: Value) -> Result<()> {
        self.update_with_no_save(config, false)
    }

    /// [`Fiber::update`] with the Cordis `update(config, noSave)` hint exposed.
    ///
    /// `no_save` is forwarded verbatim to the `internal/update` waterfall
    /// ([`crate::UpdateNext::next`] receives it), so persistence/telemetry hooks
    /// can tell a "save this change back" update from a transient one. It is
    /// **only a hint**: like the reference implementation, the kernel still
    /// commits the new raw config and restarts a successful (non-vetoed)
    /// update — see [`Fiber::update`] for the full order of operations.
    pub fn update_with_no_save(&self, config: Value, no_save: bool) -> Result<()> {
        let _guard = begin_operation(&self.data);
        let _lifecycle = begin_lifecycle(&self.data)?;
        let _finalize = FinalizeGuard { data: &self.data };
        if self.resolve_update(config, false, no_save)? {
            self.restart()?;
        }
        Ok(())
    }

    /// Awaiting variant of [`Fiber::update`].
    pub async fn update_async(&self, config: Value) -> Result<()> {
        self.update_async_with_no_save(config, false).await
    }

    /// Awaiting variant of [`Fiber::update_with_no_save`].
    pub async fn update_async_with_no_save(&self, config: Value, no_save: bool) -> Result<()> {
        let _guard = begin_operation(&self.data);
        let _lifecycle = begin_lifecycle(&self.data)?;
        let _finalize = FinalizeGuard { data: &self.data };
        if self.resolve_update(config, true, no_save)? {
            self.restart_async().await?;
        }
        Ok(())
    }

    /// Shared config pipeline of [`Fiber::update`] / [`Fiber::update_async`].
    ///
    /// Returns whether the update must restart the fiber (i.e. the
    /// `internal/update` terminal ran and did not reject a replacement). Raw
    /// config handling (FIX-14):
    /// - pass-through: the caller's raw config is stored by the terminal;
    /// - replacement (`next(other, ..)`): `other` is validated, then stored;
    /// - the `no_save` hint supplied by the caller is forwarded to the
    ///   `internal/update` hooks but does not suppress the kernel-side
    ///   raw-config commit (Cordis parity: `_config` is assigned
    ///   unconditionally and `noSave` only tells persistence hooks to skip
    ///   writing it back);
    /// - veto / validation failure: nothing is stored, so `raw_config()` keeps
    ///   its previous value. No rollback of an already-committed raw config is
    ///   needed here: the terminal only runs when the chain was not vetoed, and
    ///   a rejection happens inside the terminal before any commit.
    fn resolve_update(&self, config: Value, allow_async: bool, no_save: bool) -> Result<bool> {
        match self.state() {
            FiberState::Disposed => return Err(CordisError::Disposed),
            FiberState::Unloading => return Err(CordisError::InactiveEffect),
            _ => {}
        }
        let (plugin, ctx) = {
            let data = self.data.lock().map_err(|_| CordisError::Disposed)?;
            if !allow_async && data.plugin.is_async() {
                return Err(CordisError::AsyncPluginNeedsAsync(
                    data.plugin.name().to_string(),
                ));
            }
            (data.plugin.clone(), data.ctx_ref())
        };
        let Some(ctx) = ctx else {
            return Err(CordisError::Disposed);
        };
        let raw = config.clone();
        // 1. `internal/config` waterfall: derive the effective config.
        let resolved = ctx.events().internal().waterfall_config(self, raw.clone());
        // 2. Schema validation: a rejected update keeps the old raw config and
        //    does not restart.
        let config = validate_config(&plugin, resolved).map_err(CordisError::Validation)?;
        let this = self.clone();
        let passed = config.clone();
        let schema_plugin = plugin.clone();
        let raw_for_hook = raw;
        let rejected: std::sync::Arc<std::sync::Mutex<Option<crate::config::ValidationError>>> =
            std::sync::Arc::new(std::sync::Mutex::new(None));
        let rejected_for_hook = rejected.clone();
        // 3. `internal/update` waterfall: the terminal runs only when the chain
        //    is not vetoed; it commits the raw config (Cordis `_config`, written
        //    unconditionally; the `no_save` hint is for persistence hooks only)
        //    and tells the caller to restart.
        let proceeded = ctx.events().internal().waterfall_update(
            self,
            config,
            no_save,
            &move |replaced, _no_save| {
                if replaced != passed {
                    // A hook that returns a different config is replacing the
                    // caller config: that replacement becomes the new raw config
                    // (and is resolved once on restart). It must itself be
                    // schema-valid before it can be stored; otherwise reject the
                    // whole update instead of failing later on activation.
                    if let Err(error) = validate_config(&schema_plugin, replaced.clone()) {
                        if let Ok(mut slot) = rejected_for_hook.lock() {
                            *slot = Some(error);
                        }
                        return false;
                    }
                    this.store_raw_config(replaced);
                } else {
                    // A pass-through must store the caller's *raw* config, not
                    // the resolved one, or derived values would compound
                    // (CORE-040).
                    this.store_raw_config(raw_for_hook.clone());
                }
                true
            },
        );
        if let Some(error) = rejected.lock().ok().and_then(|mut slot| slot.take()) {
            // The terminal validates a replacement before storing it, so a
            // rejection never left a committed raw config behind; the fiber
            // simply keeps the config it already had.
            return Err(CordisError::Validation(error));
        }
        Ok(proceeded)
    }
}

pub(crate) fn dependencies_ready(ctx: &Context, inject: &[&'static str]) -> bool {
    // FIX-12: injection gates use the strict query (store + available +
    // provider fiber Active), so a dependent never activates against a provider
    // that is still Loading or already unloaded.
    inject
        .iter()
        .all(|name| ctx.lookup_for_injection(name).is_some())
}

/// Record the service-entry versions the fiber is activating against.
pub(crate) fn record_dep_versions(data: &FiberRef, ctx: &Context, inject: &[&'static str]) {
    let versions: Vec<(&'static str, u64)> = inject
        .iter()
        .filter_map(|name| ctx.lookup(name).map(|entry| (*name, entry.version)))
        .collect();
    if let Ok(mut data) = data.lock() {
        data.dep_versions = Mutex::new(versions);
    }
}

/// Whether every injected service is still available *and* unchanged.
///
/// A version change means the provider (or `ctx.set`) changed even though the
/// name is still available: the dependent must reactivate (Cordis epoch).
pub(crate) fn dependencies_satisfied(
    ctx: &Context,
    inject: &[&'static str],
    recorded: &[(&'static str, u64)],
) -> bool {
    inject
        .iter()
        .all(|name| match ctx.lookup_for_injection(name) {
            Some(entry) => recorded
                .iter()
                .any(|(recorded_name, version)| recorded_name == name && *version == entry.version),
            None => false,
        })
}

/// Set the fiber state and emit `internal/status` with the previous state.
///
/// No-op when the state does not change (Cordis only emits on transitions).
pub(crate) fn transition(data: &FiberRef, next: FiberState) {
    let (old, ctx) = match data.lock() {
        Ok(mut fiber) => {
            if fiber.state == next {
                return;
            }
            // A disposed fiber is final: a reentrant dispose/restart during
            // activation must win over the late transition, otherwise the
            // fiber would end `Active` with no context left (CORE-040).
            if fiber.state == FiberState::Disposed {
                return;
            }
            let old = fiber.state;
            fiber.state = next;
            (old, fiber.ctx_ref())
        }
        Err(_) => return,
    };
    if let Some(ctx) = ctx {
        let fiber = Fiber { data: data.clone() };
        ctx.events().internal().emit_status(&fiber, old);
    }
    // Notify through the stored registry: a disposed fiber has no context link
    // left, but quiescence must still observe the transition.
    let registry = data.lock().ok().and_then(|fiber| fiber.registry.clone());
    if let Some(registry) = registry {
        registry.notify_idle();
    }
}

/// Validate `config` with the plugin's schema (`Plugin::config_schema` /
/// `AsyncPlugin::config_schema`).
///
/// Returns the normalized config on success. A plugin without a schema keeps
/// the raw value, matching Cordis `resolveConfig` (which returns the input
/// unchanged when `runtime.Config` is absent).
pub(crate) fn validate_config(
    plugin: &PluginKind,
    config: Value,
) -> std::result::Result<Value, crate::config::ValidationError> {
    match plugin.config_schema() {
        None => Ok(config),
        // A panicking embedder schema must not unwind out of activation: it
        // becomes a validation failure (CORE-042).
        Some(schema) => crate::config::contained_validate(schema.as_ref(), config)
            .map_err(crate::config::ValidationError::new),
    }
}

/// Write the `Failed` error on a fiber (diagnostics).
fn set_error(data: &FiberRef, error: String) {
    if let Ok(mut fiber) = data.lock() {
        fiber.error = Some(error);
    }
}

/// Whether this state means an in-flight activation must not complete.
fn state_rejects_activation(state: FiberState) -> bool {
    matches!(state, FiberState::Disposed | FiberState::Unloading)
}

/// Whether a reentrant dispose/restart invalidated an in-flight activation.
fn activation_invalidated(data: &FiberRef) -> bool {
    match data.lock() {
        Ok(fiber) => state_rejects_activation(fiber.state),
        Err(_) => true,
    }
}

fn take_activation_effects(data: &FiberRef) -> (Vec<FiberEffect>, Vec<AsyncDisposer>) {
    match data.lock() {
        Ok(mut fiber) => (
            std::mem::take(&mut fiber.effects),
            std::mem::take(&mut fiber.deferred_async),
        ),
        Err(_) => (Vec::new(), Vec::new()),
    }
}

/// Discard the registrations of an activation that lost the race against a
/// concurrent dispose/restart, without touching the (already final) state.
fn discard_activation_effects(data: &FiberRef, ctx: &Context) {
    let (effects, deferred) = take_activation_effects(data);
    let _ = drain_effects_blocking(effects, deferred);
    // FIX-4: the activation is already being discarded, so a panicking child
    // cleanup is contained and reported instead of aborting the rollback.
    if let Some(payload) = sweep_children_sync(data) {
        log_cleanup_panic("discarding a stale activation", payload.as_ref());
    }
    ctx.inner.events.remove_owner(ctx.fiber_id());
}

async fn discard_activation_effects_async(data: &FiberRef, ctx: &Context) {
    let (effects, deferred) = take_activation_effects(data);
    let _ = drain_effects_async(effects, deferred).await;
    if let Some(payload) = sweep_children_async(data).await {
        log_cleanup_panic("discarding a stale async activation", payload.as_ref());
    }
    ctx.inner.events.remove_owner(ctx.fiber_id());
}

/// Roll back an async activation that was cancelled before it finished.
///
/// Runnable from `Drop`, so it only performs non-blocking work: sync cleanups
/// are disposed inline, async cleanups are deferred to the fiber's next
/// `dispose` (same contract as a failed synchronous activation).
struct AsyncActivationGuard {
    data: FiberRef,
    ctx: Context,
    /// Activation epoch of the attempt this guard belongs to (s1).
    epoch: u64,
}

impl Drop for AsyncActivationGuard {
    fn drop(&mut self) {
        // s1 (FIX-3/FIX-5 interaction): only this attempt may roll the fiber
        // back. A *stale* attempt that wakes up after a newer activation started
        // is still `Loading` from the outside, but the epoch belongs to the new
        // attempt; rolling back here would drain the new attempt's effects, drop
        // its listeners, write a bogus error and force `Failed`. The stale
        // attempt's own registrations were already discarded precisely by
        // `discard_stale_activation(_async)`, so the guard has nothing to do.
        let cancelled = match self.data.lock() {
            Ok(fiber) => {
                fiber.state == FiberState::Loading
                    && fiber.activation_epoch.load(Ordering::SeqCst) == self.epoch
            }
            Err(_) => false,
        };
        if !cancelled {
            return;
        }
        // A-1 hardening: roll back only *this* attempt's registrations. Draining
        // every effect here could steal the registrations of an activation that
        // starts in the window between the check above and the rollback.
        rollback_epoch_effects(&self.data, self.epoch);
        // Re-check under the lock that decided "cancelled": a new attempt may
        // have taken over while we were rolling back (epoch bumped). In that
        // case leave the new attempt alone: only our own effects were cleared.
        let still_current = match self.data.lock() {
            Ok(fiber) => {
                fiber.state == FiberState::Loading
                    && fiber.activation_epoch.load(Ordering::SeqCst) == self.epoch
            }
            Err(_) => false,
        };
        if !still_current {
            return;
        }
        let panicking = std::thread::panicking();
        // A-10: the epoch-precise rollback defers *async* effects, so the
        // ownership effects of children created by the cancelled attempt are not
        // run inline. Clean the attempt's owner listeners now and reclaim its
        // children like the regular failure path does — contained, because Drop
        // must never unwind.
        let ctx = self.ctx.clone();
        if panicking {
            // Unwinding: never block on a child's cleanup. Flag them for the
            // next dispose/dispose_all and clear the list.
            let _ = catch_unwind(AssertUnwindSafe(|| {
                ctx.inner.events.remove_owner(ctx.fiber_id());
                let children = take_children(&self.data);
                for child in &children {
                    request_child_dispose(child);
                }
                store_children(&self.data, Vec::new());
            }));
            set_error(
                &self.data,
                "activation cancelled while unwinding".to_string(),
            );
            transition(&self.data, FiberState::Failed);
            return;
        }
        let _ = catch_unwind(AssertUnwindSafe(|| {
            ctx.inner.events.remove_owner(ctx.fiber_id());
            if let Some(payload) = sweep_children_sync(&self.data) {
                log_cleanup_panic(
                    "sweeping children of a cancelled async activation",
                    payload.as_ref(),
                );
            }
        }));
        set_error(
            &self.data,
            "activation cancelled before completion".to_string(),
        );
        transition(&self.data, FiberState::Failed);
        // A-9 nit: do not run a full `refresh_dependents()` from `Drop` (it
        // synchronously unloads other fibers and could block an executor
        // thread). Waking the quiescence waiters is enough; the next refresh /
        // lifecycle operation re-evaluates dependents.
        if let Some(registry) = self
            .data
            .lock()
            .ok()
            .and_then(|fiber| fiber.registry.clone())
        {
            registry.notify_idle();
        }
    }
}

/// Roll back only the effects registered by one activation attempt.
///
/// Sync cleanups run inline (panic-contained); async cleanups are deferred to
/// the fiber's next `dispose` (deferred-cleanup rollback model, CORE-038 /
/// CORE-046). Children created by the attempt are
/// owned by its `ctx.plugin` ownership effect, so they are covered by the same
/// cleanup instead of a blanket child sweep (which would hit a newer attempt's
/// children).
fn rollback_epoch_effects(data: &FiberRef, epoch: u64) {
    let stale = take_stale_effects(data, epoch);
    let mut deferred = Vec::new();
    let mut first_panic: Option<crate::exec::PanicPayload> = None;
    for effect in stale.into_iter().rev() {
        match effect.kind {
            EffectKind::Sync(disposer) => {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| disposer.dispose())) {
                    first_panic.get_or_insert(payload);
                }
            }
            EffectKind::Async(disposer) => deferred.push(disposer),
        }
    }
    if !deferred.is_empty() {
        if let Ok(mut fiber) = data.lock() {
            fiber.deferred_async.extend(deferred);
        }
    }
    if let Some(payload) = first_panic {
        log_cleanup_panic(
            "rolling back a cancelled async activation",
            payload.as_ref(),
        );
    }
}

/// Snapshot the parent's child list.
fn take_children(data: &FiberRef) -> Vec<Weak<Mutex<FiberData>>> {
    match data.lock() {
        Ok(fiber) => fiber
            .children
            .lock()
            .map(|mut children| std::mem::take(&mut *children))
            .unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

fn store_children(data: &FiberRef, children: Vec<Weak<Mutex<FiberData>>>) {
    if let Ok(fiber) = data.lock() {
        if let Ok(mut slot) = fiber.children.lock() {
            *slot = children;
        }
    }
}

fn child_state(weak: &Weak<Mutex<FiberData>>) -> Option<FiberState> {
    weak.upgrade()
        .and_then(|data| data.lock().ok().map(|fiber| fiber.state))
}

fn request_child_dispose(weak: &Weak<Mutex<FiberData>>) {
    if let Some(data) = weak.upgrade() {
        if let Ok(fiber) = data.lock() {
            fiber.dispose_requested.store(true, Ordering::SeqCst);
        }
    }
}

/// Maximum supported plugin-loading depth (in-process). A recursive
/// A -> B -> A chain hits this limit and fails with `InvalidPlugin` instead of
/// overflowing the stack.
pub(crate) const MAX_PLUGIN_DEPTH: usize = 256;

struct ChildSweep<'a> {
    data: &'a FiberRef,
    pending: Vec<Weak<Mutex<FiberData>>>,
    armed: bool,
}

impl Drop for ChildSweep<'_> {
    fn drop(&mut self) {
        // A cancelled sweep puts the not-yet-disposed children back so a retry
        // can finish the cascade (the in-flight child stays in `pending`).
        if self.armed {
            store_children(self.data, std::mem::take(&mut self.pending));
        }
    }
}

/// Dispose the parent's children. Children whose lifecycle slot is busy are
/// flagged for disposal when their current operation ends.
///
/// FIX-4: each child's panic is *contained* and the sweep keeps going, so one
/// broken child cleanup cannot leave the remaining children (and the parent)
/// half-unloaded. The first payload is returned for the caller to re-raise once
/// the parent has reached its target state.
fn sweep_children_sync(data: &FiberRef) -> Option<crate::exec::PanicPayload> {
    let mut sweep = ChildSweep {
        data,
        pending: take_children(data),
        armed: true,
    };
    let mut first_panic: Option<crate::exec::PanicPayload> = None;
    while let Some(weak) = sweep.pending.last() {
        match child_state(weak) {
            Some(FiberState::Disposed) | None => {}
            Some(_) => {
                if let Some(child) = weak.upgrade() {
                    let child = Fiber { data: child };
                    match catch_unwind(AssertUnwindSafe(|| child.dispose())) {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => request_child_dispose(weak),
                        Err(payload) => {
                            // The child panicked while unloading: it may not have
                            // reached `Disposed`, so flag it for the owner to
                            // finalize, then continue with the other children.
                            request_child_dispose(weak);
                            first_panic.get_or_insert(payload);
                        }
                    }
                }
            }
        }
        sweep.pending.pop();
    }
    sweep.armed = false;
    store_children(data, Vec::new());
    first_panic
}

async fn sweep_children_async(data: &FiberRef) -> Option<crate::exec::PanicPayload> {
    let mut sweep = ChildSweep {
        data,
        pending: take_children(data),
        armed: true,
    };
    let mut first_panic: Option<crate::exec::PanicPayload> = None;
    while let Some(weak) = sweep.pending.last() {
        match child_state(weak) {
            Some(FiberState::Disposed) | None => {}
            Some(_) => {
                if let Some(child) = weak.upgrade() {
                    let child = Fiber { data: child };
                    // Boxed: the async sweep <-> dispose_async <-> sweep chain
                    // is mutually recursive.
                    let outcome = futures_util::FutureExt::catch_unwind(AssertUnwindSafe(
                        Box::pin(child.dispose_async()),
                    ))
                    .await;
                    match outcome {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => request_child_dispose(weak),
                        Err(payload) => {
                            request_child_dispose(weak);
                            first_panic.get_or_insert(payload);
                        }
                    }
                }
            }
        }
        sweep.pending.pop();
    }
    sweep.armed = false;
    store_children(data, Vec::new());
    first_panic
}

/// Report a contained cleanup panic (the operation already reached a safe
/// state, so the payload is logged instead of unwinding into kernel code).
fn log_cleanup_panic(operation: &str, payload: &(dyn std::any::Any + Send)) {
    tracing::error!(
        target: "zeokeel_cordis::fiber",
        "cleanup panicked while {operation}: {}",
        panic_message(payload)
    );
}

/// If a dispose was requested while this operation owned the fiber, run it now
/// (reentrant lifecycle claim on the same thread is allowed).
pub(crate) fn finalize_requested_dispose(data: &FiberRef) {
    let requested = data
        .lock()
        .map(|fiber| {
            fiber.dispose_requested.load(Ordering::SeqCst) && fiber.state != FiberState::Disposed
        })
        .unwrap_or(false);
    if requested {
        if let Ok(fiber) = data.lock() {
            fiber.dispose_requested.store(false, Ordering::SeqCst);
        }
        let _ = Fiber { data: data.clone() }.dispose();
    }
}

/// RAII finalizer for the lifecycle operations (FIX-3).
///
/// A fiber may be flagged `dispose_requested` while a lifecycle operation owns
/// it (a parent sweeping its children only sees `LifecycleBusy`). Every return
/// path of that operation — normal, early `Err`, `?`, or a cleanup panic — must
/// still honor the flag, otherwise the fiber would stay Active forever after its
/// parent is gone (a permanent orphan invisible to `is_idle`).
struct FinalizeGuard<'a> {
    data: &'a FiberRef,
}

impl Drop for FinalizeGuard<'_> {
    fn drop(&mut self) {
        // A guard must never unwind: a cleanup panic from the deferred dispose is
        // reported instead of being re-raised from `Drop`.
        if catch_unwind(AssertUnwindSafe(|| finalize_requested_dispose(self.data))).is_err() {
            tracing::error!(
                target: "zeokeel_cordis::fiber",
                "cleanup panicked while finalizing a requested dispose"
            );
        }
    }
}

/// Whether this activation attempt has been superseded by a newer one.
fn activation_stale(data: &FiberRef, epoch: u64) -> bool {
    match data.lock() {
        Ok(fiber) => fiber.activation_epoch.load(Ordering::SeqCst) != epoch,
        Err(_) => true,
    }
}

/// Start a new activation generation and return its id.
fn begin_activation_epoch(data: &FiberRef) -> Result<u64> {
    let fiber = data.lock().map_err(|_| CordisError::Disposed)?;
    Ok(fiber.activation_epoch.fetch_add(1, Ordering::SeqCst) + 1)
}

// ---------------------------------------------------------------------------
// Activation frames (FIX-5): publish the *current attempt* epoch to the
// effects it registers.
// ---------------------------------------------------------------------------

thread_local! {
    /// Activation frames of the current thread: `(fiber_id, epoch)` pushed
    /// right before an activation body runs and popped right after (including
    /// on panic, via RAII). Nested activations (a plugin loading a child while
    /// its own `apply` runs) stack; a lookup resolves the innermost matching
    /// frame.
    static ACTIVATION_FRAMES: RefCell<Vec<(u64, u64)>> = const { RefCell::new(Vec::new()) };
}

/// RAII activation frame: makes `current_activation_epoch(fiber_id)` return
/// `epoch` for the duration of the activation body.
struct EpochFrame {
    fiber_id: u64,
    epoch: u64,
}

impl EpochFrame {
    fn enter(fiber_id: u64, epoch: u64) -> Self {
        ACTIVATION_FRAMES.with(|frames| frames.borrow_mut().push((fiber_id, epoch)));
        Self { fiber_id, epoch }
    }
}

impl Drop for EpochFrame {
    fn drop(&mut self) {
        // The crate is `forbid(unsafe_code)`, but a panic in an embedder hook
        // can still drop frames out of order: remove this exact frame instead
        // of blindly popping, so other attempts keep their tag.
        ACTIVATION_FRAMES.with(|frames| {
            let mut frames = frames.borrow_mut();
            if let Some(index) = frames
                .iter()
                .rposition(|(id, epoch)| *id == self.fiber_id && *epoch == self.epoch)
            {
                frames.remove(index);
            }
        });
    }
}

/// Epoch of the activation attempt currently running `fiber_id` on this
/// thread, if any.
///
/// Effect registration uses this value (falling back to the fiber's latest
/// epoch when the effect is registered outside an activation) so a stale
/// attempt that loses the race against a newer one is tagged with *its own*
/// epoch and can be discarded precisely.
pub(crate) fn current_activation_epoch(fiber_id: u64) -> Option<u64> {
    ACTIVATION_FRAMES.with(|frames| {
        frames
            .borrow()
            .iter()
            .rev()
            .find(|(id, _)| *id == fiber_id)
            .map(|(_, epoch)| *epoch)
    })
}

/// Future wrapper that installs the activation frame around every `poll`.
///
/// A plain stack frame would be wrong for a future that is polled from another
/// executor thread; a frame created and dropped inside `poll` always matches
/// the thread that is actually running the activation body.
struct EpochFrameFuture<F> {
    fiber_id: u64,
    epoch: u64,
    inner: Pin<Box<F>>,
}

impl<F: Future> EpochFrameFuture<F> {
    fn new(fiber_id: u64, epoch: u64, inner: F) -> Self {
        Self {
            fiber_id,
            epoch,
            inner: Box::pin(inner),
        }
    }
}

impl<F: Future> Future for EpochFrameFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // `Pin<Box<F>>` is always `Unpin`, so the wrapper is `Unpin` too.
        let this = self.get_mut();
        let _frame = EpochFrame::enter(this.fiber_id, this.epoch);
        this.inner.as_mut().poll(cx)
    }
}

fn take_stale_effects(data: &FiberRef, epoch: u64) -> Vec<FiberEffect> {
    match data.lock() {
        Ok(mut fiber) => {
            let (stale, keep): (Vec<_>, Vec<_>) = fiber
                .effects
                .drain(..)
                .partition(|effect| effect.activation == epoch);
            fiber.effects = keep;
            stale
        }
        Err(_) => Vec::new(),
    }
}

/// Discard only the registrations made by a stale activation attempt (never a
/// newer activation's effects).
fn discard_stale_activation(data: &FiberRef, epoch: u64) {
    let stale = take_stale_effects(data, epoch);
    let _ = drain_effects_blocking(stale, Vec::new());
}

async fn discard_stale_activation_async(data: &FiberRef, epoch: u64) {
    let stale = take_stale_effects(data, epoch);
    let _ = drain_effects_async(stale, Vec::new()).await;
}

/// Roll back the effects registered by a failed **synchronous** activation.
///
/// Sync cleanups run immediately (each panic isolated); async cleanups cannot
/// be awaited from a sync frame, so they are deferred to the fiber's next
/// `dispose`/`dispose_async` (deferred rollback drain, CORE-038; cancellation
/// semantics, CORE-046). Services and listeners are removed before the fiber
/// transitions to `Failed`, so a failed plugin leaves no visible registrations
/// behind.
fn fail_fiber_sync(data: &FiberRef, ctx: &Context, error: String) {
    fail_fiber_sync_inner(data, ctx, error, false);
}

/// Roll back a failed activation.
///
/// `defer_async` is used by the async-activation cancel guard (which runs in
/// `Drop` and must not block): async cleanups are then deferred to the fiber's
/// next `dispose`. Normal activation failures drain everything inline so the
/// reverse registration order holds across the sync and async lanes (CORE-007).
fn fail_fiber_sync_inner(data: &FiberRef, ctx: &Context, error: String, defer_async: bool) {
    set_error(data, error);
    let effects = match data.lock() {
        Ok(mut fiber) => std::mem::take(&mut fiber.effects),
        Err(_) => Vec::new(),
    };
    let mut deferred = Vec::new();
    let mut first_panic = None;
    for effect in effects.into_iter().rev() {
        match effect.kind {
            EffectKind::Sync(disposer) => {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| disposer.dispose())) {
                    first_panic.get_or_insert(payload);
                }
            }
            EffectKind::Async(disposer) => {
                if defer_async {
                    deferred.push(disposer);
                } else if let Err(payload) =
                    catch_unwind(AssertUnwindSafe(|| disposer.dispose_blocking()))
                {
                    first_panic.get_or_insert(payload);
                }
            }
        }
    }
    if !deferred.is_empty() {
        if let Ok(mut fiber) = data.lock() {
            fiber.deferred_async.extend(deferred);
        }
    }
    ctx.inner.events.remove_owner(ctx.fiber_id());
    // FIX-4: a panicking child cleanup must not escape the failed-activation
    // rollback (the fiber is already `Failed`); report it instead.
    if let Some(payload) = sweep_children_sync(data) {
        log_cleanup_panic("rolling back a failed activation", payload.as_ref());
    }
    // A reentrant dispose during activation is final: keep it, only roll back.
    if !activation_invalidated(data) {
        transition(data, FiberState::Failed);
        ctx.refresh_dependents();
    }
    if let Some(payload) = first_panic {
        // A cleanup panic must not escape the failed activation: log it and
        // keep the kernel alive (the activation itself already failed).
        let message = panic_message(payload.as_ref());
        tracing::error!(
            target: "zeokeel_cordis::fiber",
            plugin = ctx.plugin_name().as_str(),
            "cleanup panicked during failed activation rollback: {message}"
        );
    }
}

/// Async counterpart of [`fail_fiber_sync`]: awaits async cleanups too.
async fn fail_fiber_async(data: &FiberRef, ctx: &Context, error: String) {
    set_error(data, error);
    let effects = match data.lock() {
        Ok(mut fiber) => std::mem::take(&mut fiber.effects),
        Err(_) => Vec::new(),
    };
    let mut first_panic: Option<crate::exec::PanicPayload> = None;
    for effect in effects.into_iter().rev() {
        match effect.kind {
            EffectKind::Sync(disposer) => {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| disposer.dispose())) {
                    first_panic.get_or_insert(payload);
                }
            }
            EffectKind::Async(disposer) => {
                let outcome =
                    futures_util::FutureExt::catch_unwind(AssertUnwindSafe(disposer.dispose()))
                        .await;
                if let Err(payload) = outcome {
                    first_panic.get_or_insert(payload);
                }
            }
        }
    }
    ctx.inner.events.remove_owner(ctx.fiber_id());
    if let Some(payload) = sweep_children_async(data).await {
        log_cleanup_panic("rolling back a failed async activation", payload.as_ref());
    }
    // A reentrant dispose during activation is final: keep it, only roll back.
    if !activation_invalidated(data) {
        transition(data, FiberState::Failed);
        ctx.refresh_dependents();
    }
    if let Some(payload) = first_panic {
        let message = panic_message(payload.as_ref());
        tracing::error!(
            target: "zeokeel_cordis::fiber",
            plugin = ctx.plugin_name().as_str(),
            "cleanup panicked during failed async activation rollback: {message}"
        );
    }
}

pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "panicked".to_string()
    }
}

pub(crate) fn activate_fiber(
    data: &FiberRef,
    ctx: &Context,
    plugin: &PluginKind,
    config: &Value,
) -> Result<()> {
    let fiber_id = {
        let mut fiber = data.lock().map_err(|_| CordisError::Disposed)?;
        if state_rejects_activation(fiber.state) {
            return Err(CordisError::Disposed);
        }
        fiber.error = None;
        fiber.id
    };
    // Activation is a lifecycle operation: `await_settled` must wait for it
    // and a reentrant dispose must be observable afterwards (CORE-040).
    let _guard = begin_operation(data);
    let epoch = begin_activation_epoch(data)?;
    let Some(sync_plugin) = plugin.as_sync().cloned() else {
        return Err(CordisError::AsyncPluginNeedsAsync(
            plugin.name().to_string(),
        ));
    };
    transition(data, FiberState::Loading);
    // Cordis `_resolveConfig`: dispatch the `internal/config` waterfall before
    // schema validation, then let the schema normalize the result.
    let fiber_handle = Fiber { data: data.clone() };
    let resolved = ctx
        .events()
        .internal()
        .waterfall_config(&fiber_handle, config.clone());
    let config = match validate_config(plugin, resolved) {
        Ok(config) => config,
        Err(error) => {
            if activation_stale(data, epoch) {
                return Ok(());
            }
            set_error(data, error.message());
            transition(data, FiberState::Failed);
            ctx.refresh_dependents();
            return Ok(());
        }
    };
    if let Ok(mut fiber) = data.lock() {
        fiber.config = config.clone();
    }
    // CORE-029: apply panics are isolated; a failing apply (error or panic)
    // rolls back every registration made so far and fails only this fiber.
    // FIX-5: the frame publishes *this attempt's* epoch to effect tagging, so a
    // stale attempt that loses a restart race can discard exactly its effects.
    let outcome = {
        let _epoch_frame = EpochFrame::enter(fiber_id, epoch);
        catch_unwind(AssertUnwindSafe(|| {
            sync_plugin.apply(ctx.clone(), config.clone())
        }))
    };
    match outcome {
        Ok(Ok(())) => {
            if activation_stale(data, epoch) {
                // A newer activation superseded this attempt: drop only the
                // registrations this attempt made.
                discard_stale_activation(data, epoch);
                return Ok(());
            }
            if activation_invalidated(data) {
                // The fiber was disposed/restarted while `apply` ran: its
                // registrations must not survive the lost race.
                discard_activation_effects(data, ctx);
                return Ok(());
            }
            record_dep_versions(data, ctx, plugin.inject());
            transition(data, FiberState::Active);
            ctx.refresh_dependents();
            Ok(())
        }
        Ok(Err(error)) => {
            if activation_stale(data, epoch) {
                discard_stale_activation(data, epoch);
                return Ok(());
            }
            fail_fiber_sync(data, ctx, error.to_string());
            Ok(())
        }
        Err(payload) => {
            if activation_stale(data, epoch) {
                discard_stale_activation(data, epoch);
                return Ok(());
            }
            fail_fiber_sync(
                data,
                ctx,
                format!("panic: {}", panic_message(payload.as_ref())),
            );
            Ok(())
        }
    }
}

/// Async activation path used by [`crate::AsyncPlugin`].
pub(crate) async fn activate_fiber_async(
    data: &FiberRef,
    ctx: &Context,
    plugin: &PluginKind,
    config: &Value,
) -> Result<()> {
    let fiber_id = {
        let mut fiber = data.lock().map_err(|_| CordisError::Disposed)?;
        if state_rejects_activation(fiber.state) {
            return Err(CordisError::Disposed);
        }
        fiber.error = None;
        fiber.id
    };
    // Activation is a lifecycle operation: `await_settled` must wait for it
    // and a reentrant dispose must be observable afterwards (CORE-040).
    let _guard = begin_operation(data);
    let epoch = begin_activation_epoch(data)?;
    transition(data, FiberState::Loading);
    // If the host drops this future mid-await (timeout/select/cancel), the
    // guard rolls the activation back so the fiber cannot stay `Loading` with
    // live registrations (CORE-043).
    let _cancel_guard = AsyncActivationGuard {
        data: data.clone(),
        ctx: ctx.clone(),
        epoch,
    };
    let fiber_handle = Fiber { data: data.clone() };
    let resolved = ctx
        .events()
        .internal()
        .waterfall_config(&fiber_handle, config.clone());
    let config = match validate_config(plugin, resolved) {
        Ok(config) => config,
        Err(error) => {
            if activation_stale(data, epoch) {
                return Ok(());
            }
            set_error(data, error.message());
            transition(data, FiberState::Failed);
            ctx.refresh_dependents();
            return Ok(());
        }
    };
    if let Ok(mut fiber) = data.lock() {
        fiber.config = config.clone();
    }
    let outcome = match plugin {
        PluginKind::Sync(sync_plugin) => {
            let plugin = sync_plugin.clone();
            let ctx = ctx.clone();
            let config = config.clone();
            let _epoch_frame = EpochFrame::enter(fiber_id, epoch);
            catch_unwind(AssertUnwindSafe(|| plugin.apply(ctx, config)))
        }
        PluginKind::Async(async_plugin) => {
            let plugin = async_plugin.clone();
            let call_ctx = ctx.clone();
            let call_config = config.clone();
            // FIX-5: the frame is installed around every poll of the activation
            // future, so effect tagging is correct even across executor moves.
            let future = EpochFrameFuture::new(fiber_id, epoch, async move {
                plugin.apply_async(call_ctx, call_config).await
            });
            match futures_util::FutureExt::catch_unwind(AssertUnwindSafe(future)).await {
                Ok(result) => Ok(result),
                Err(payload) => {
                    if activation_stale(data, epoch) {
                        discard_stale_activation_async(data, epoch).await;
                        return Ok(());
                    }
                    fail_fiber_async(
                        data,
                        ctx,
                        format!("panic: {}", panic_message(payload.as_ref())),
                    )
                    .await;
                    return Ok(());
                }
            }
        }
    };
    match outcome {
        Ok(Ok(())) => {
            if activation_stale(data, epoch) {
                discard_stale_activation_async(data, epoch).await;
                return Ok(());
            }
            if activation_invalidated(data) {
                discard_activation_effects_async(data, ctx).await;
                return Ok(());
            }
            record_dep_versions(data, ctx, plugin.inject());
            transition(data, FiberState::Active);
            ctx.refresh_dependents();
            Ok(())
        }
        Ok(Err(error)) => {
            if activation_stale(data, epoch) {
                discard_stale_activation_async(data, epoch).await;
                return Ok(());
            }
            fail_fiber_async(data, ctx, error.to_string()).await;
            Ok(())
        }
        Err(payload) => {
            if activation_stale(data, epoch) {
                discard_stale_activation_async(data, epoch).await;
                return Ok(());
            }
            fail_fiber_async(
                data,
                ctx,
                format!("panic: {}", panic_message(payload.as_ref())),
            )
            .await;
            Ok(())
        }
    }
}

/// Helper: activate a fiber through its own plugin kind (sync or async).
pub(crate) async fn activate_by_kind_async(
    data: &FiberRef,
    ctx: &Context,
    plugin: &PluginKind,
    config: &Value,
) -> Result<()> {
    if plugin.is_async() {
        activate_fiber_async(data, ctx, plugin, config).await
    } else {
        activate_fiber(data, ctx, plugin, config)
    }
}

/// Begin unloading: mark the fiber `Unloading` and take its effect list.
///
/// `None` means the fiber was already disposed (idempotent dispose), so the
/// caller can simply return.
/// Effects taken off a fiber for one unload (effects, deferred async cleanups,
/// owning context).
type DrainBatch = (Vec<FiberEffect>, Vec<AsyncDisposer>, Option<Context>);

fn take_effects(data: &FiberRef) -> Result<Option<DrainBatch>> {
    let (effects, deferred, ctx) = {
        let mut fiber = data.lock().map_err(|_| CordisError::Disposed)?;
        if fiber.state == FiberState::Disposed {
            // A cancelled `dispose_async` finishes the teardown synchronously and
            // parks the async cleanups it could not await in `deferred_async`
            // (A-9). A retry `dispose`/`dispose_async` on the already-Disposed
            // fiber drains them, so no cleanups are silently dropped.
            if fiber.effects.is_empty() && fiber.deferred_async.is_empty() {
                return Ok(None);
            }
            let effects = std::mem::take(&mut fiber.effects);
            let deferred = std::mem::take(&mut fiber.deferred_async);
            return Ok(Some((effects, deferred, fiber.ctx_ref())));
        }
        (
            std::mem::take(&mut fiber.effects),
            std::mem::take(&mut fiber.deferred_async),
            fiber.ctx_ref(),
        )
    };
    transition(data, FiberState::Unloading);
    // Withdraw before draining: dependents deactivate while the binding is
    // still present, then the provider's own disposers remove it (Cordis
    // provider-withdrawal order).
    if let Some(ctx) = ctx.as_ref() {
        let owner = data.lock().map(|fiber| fiber.id).unwrap_or(0);
        ctx.withdraw_owner_services(owner);
        // Retire dependents *before* draining this fiber, iterating to a fixed
        // point. `refresh_dependents` alone can be swallowed by an outer
        // refresh's re-entrancy guard, which would leave a dependent's binding
        // removed before its cleanup ran (nested-chain order).
        for _ in 0..64 {
            if !ctx.inner.registry.retire_unready_contained() {
                break;
            }
        }
    }
    Ok(Some((effects, deferred, ctx)))
}

/// Finish unloading: drop listeners/services and store the target state.
fn finish_unload(data: &FiberRef, ctx: Option<Context>, target: FiberState) -> Result<()> {
    if let Some(ctx) = &ctx {
        // Services/listeners registered by this fiber are owned by its effect
        // list (already drained above), so only event listeners need the
        // owner-based sweep here.
        ctx.inner.events.remove_owner(ctx.fiber_id());
    }
    transition(data, target);
    if target == FiberState::Disposed {
        // D-1: detach by fiber id from *every* runtime (a concurrent rebind may
        // have moved the fiber to another runtime, or may push it after this
        // cleanup ran; the rebind paths self-heal that late push). The registry
        // handle is used instead of the context link so the detach also works
        // when the context was already released.
        let (id, registry) = {
            let fiber = data.lock().map_err(|_| CordisError::Disposed)?;
            (fiber.id, fiber.registry.clone())
        };
        if let Some(registry) = registry {
            registry.unregister_runtime_fiber(id);
        }
        let mut fiber = data.lock().map_err(|_| CordisError::Disposed)?;
        // Break both edges so a disposed fiber (and its context graph) can be
        // freed instead of being pinned by `parent`/`ctx` (CORE-040).
        fiber.ctx = None;
        fiber.parent = None;
    }
    Ok(())
}

/// Drain a fiber's effects in reverse registration order without an async
/// context. Async cleanups block on this thread (through the embedder-provided
/// executor, see [`crate::exec::set_blocking_executor`]); each effect's panic
/// is isolated so one bad cleanup cannot skip the rest of the list.
fn drain_effects_blocking(
    effects: Vec<FiberEffect>,
    deferred: Vec<AsyncDisposer>,
) -> Option<crate::exec::PanicPayload> {
    let mut first_panic: Option<crate::exec::PanicPayload> = None;
    for effect in effects.into_iter().rev() {
        let outcome = catch_unwind(AssertUnwindSafe(|| match effect.kind {
            EffectKind::Sync(disposer) => disposer.dispose(),
            EffectKind::Async(disposer) => disposer.dispose_blocking(),
        }));
        if let Err(payload) = outcome {
            first_panic.get_or_insert(payload);
        }
    }
    for disposer in deferred {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| disposer.dispose_blocking())) {
            first_panic.get_or_insert(payload);
        }
    }
    first_panic
}

/// Drain a fiber's effects in reverse registration order, awaiting every async
/// cleanup before moving on to the next (older) effect; panics are isolated per
/// effect and reported, never propagated out of `dispose_async`.
async fn drain_effects_async(
    effects: Vec<FiberEffect>,
    deferred: Vec<AsyncDisposer>,
) -> Option<crate::exec::PanicPayload> {
    let mut first_panic: Option<crate::exec::PanicPayload> = None;
    for effect in effects.into_iter().rev() {
        let outcome = match effect.kind {
            EffectKind::Sync(disposer) => {
                catch_unwind(AssertUnwindSafe(|| disposer.dispose())).map(|_| ())
            }
            EffectKind::Async(disposer) => {
                futures_util::FutureExt::catch_unwind(AssertUnwindSafe(disposer.dispose()))
                    .await
                    .map(|_| ())
            }
        };
        if let Err(payload) = outcome {
            first_panic.get_or_insert(payload);
        }
    }
    for disposer in deferred {
        let outcome = futures_util::FutureExt::catch_unwind(AssertUnwindSafe(disposer.dispose()))
            .await
            .map(|_| ());
        if let Err(payload) = outcome {
            first_panic.get_or_insert(payload);
        }
    }
    first_panic
}

pub(crate) fn unload_fiber(data: &FiberRef, target: FiberState) -> Result<()> {
    let Some((effects, deferred, ctx)) = take_effects(data)? else {
        return Ok(());
    };
    let drain_panic = drain_effects_blocking(effects, deferred);
    // FIX-4: the sweep contains each child's panic and keeps going; the first
    // payload (drain before sweep) is re-raised only after this fiber reached
    // its target state, so the parent can never stay stuck in `Unloading`.
    let sweep_panic = sweep_children_sync(data);
    finish_unload(data, ctx, target)?;
    if let Some(payload) = drain_panic.or(sweep_panic) {
        std::panic::resume_unwind(payload);
    }
    Ok(())
}

pub(crate) async fn unload_fiber_async(data: &FiberRef, target: FiberState) -> Result<()> {
    let Some((effects, deferred, ctx)) = take_effects(data)? else {
        return Ok(());
    };
    // Cancel-safe drain: if the host drops this future mid-await, the guard
    // completes the teardown synchronously (A-9) instead of leaving the fiber
    // half-unloaded in `Unloading` (or in a fake `Active` state).
    let mut pending = CancelSafeUnload {
        data,
        effects,
        deferred,
        armed: true,
        target,
        ctx,
    };
    let drain_panic = pending.drain().await;
    pending.armed = false;
    let ctx = pending.ctx.take();
    drop(pending);
    let sweep_panic = sweep_children_async(data).await;
    finish_unload(data, ctx, target)?;
    if let Some(payload) = drain_panic.or(sweep_panic) {
        std::panic::resume_unwind(payload);
    }
    Ok(())
}

/// Unload `data` to `target`, containing a panicking cleanup (FIX-2).
///
/// The refresh/retire paths are kernel-internal: a dependent's cleanup panic
/// must never abort the provider's own unload (that would leave the provider
/// stuck in `Unloading` and `when_idle` blocked forever). The panic is logged
/// and returned so the caller can re-raise it at a point where the fiber state
/// is already final.
pub(crate) fn unload_contained(
    data: &FiberRef,
    target: FiberState,
) -> Option<crate::exec::PanicPayload> {
    match catch_unwind(AssertUnwindSafe(|| unload_fiber(data, target))) {
        Ok(Ok(())) => None,
        Ok(Err(error)) => {
            tracing::error!(
                target: "zeokeel_cordis::fiber",
                "contained unload failed: {error}"
            );
            None
        }
        Err(payload) => {
            tracing::error!(
                target: "zeokeel_cordis::fiber",
                "cleanup panicked during contained unload: {}",
                panic_message(payload.as_ref())
            );
            Some(payload)
        }
    }
}

/// Awaiting counterpart of [`unload_contained`] for the async refresh path.
pub(crate) async fn unload_contained_async(
    data: &FiberRef,
    target: FiberState,
) -> Option<crate::exec::PanicPayload> {
    match futures_util::FutureExt::catch_unwind(AssertUnwindSafe(unload_fiber_async(data, target)))
        .await
    {
        Ok(Ok(())) => None,
        Ok(Err(error)) => {
            tracing::error!(
                target: "zeokeel_cordis::fiber",
                "contained async unload failed: {error}"
            );
            None
        }
        Err(payload) => {
            tracing::error!(
                target: "zeokeel_cordis::fiber",
                "cleanup panicked during contained async unload: {}",
                panic_message(payload.as_ref())
            );
            Some(payload)
        }
    }
}

/// Completes the teardown when the unload future is dropped mid-await (A-2/A-9).
///
/// Restoring the previous state would leave a *fake Active* fiber (its services
/// already withdrawn, some cleanups already run); keeping `Unloading` would
/// strand it forever. So a cancelled unload finishes what it can, synchronously:
/// remaining sync cleanups run inline, remaining async cleanups are moved to
/// `deferred_async` (the deferred-cleanup model of CORE-038/CORE-046, finished
/// by the next dispose), and
/// `finish_unload` lands the fiber on its target state (`Pending` for a refresh
/// retire, `Disposed` for a cancelled `dispose_async`).
struct CancelSafeUnload<'a> {
    data: &'a FiberRef,
    effects: Vec<FiberEffect>,
    deferred: Vec<AsyncDisposer>,
    armed: bool,
    target: FiberState,
    ctx: Option<Context>,
}

impl CancelSafeUnload<'_> {
    async fn drain(&mut self) -> Option<crate::exec::PanicPayload> {
        let mut first_panic: Option<crate::exec::PanicPayload> = None;
        // Note: the in-flight effect stays in the vector until it completes, so
        // a cancelled future keeps it (and everything older) intact for the
        // retry, in the same reverse registration order.
        while let Some(effect) = self.effects.last_mut() {
            let outcome = match &mut effect.kind {
                EffectKind::Sync(disposer) => {
                    catch_unwind(AssertUnwindSafe(|| disposer.dispose())).map(|_| ())
                }
                EffectKind::Async(disposer) => {
                    futures_util::FutureExt::catch_unwind(AssertUnwindSafe(disposer.dispose()))
                        .await
                        .map(|_| ())
                }
            };
            self.effects.pop();
            if let Err(payload) = outcome {
                first_panic.get_or_insert(payload);
            }
        }
        // Deferred cleanups are stored newest-first, so drain from the front.
        while let Some(disposer) = self.deferred.first_mut() {
            let outcome =
                futures_util::FutureExt::catch_unwind(AssertUnwindSafe(disposer.dispose()))
                    .await
                    .map(|_| ());
            self.deferred.remove(0);
            if let Err(payload) = outcome {
                first_panic.get_or_insert(payload);
            }
        }
        first_panic
    }
}

impl Drop for CancelSafeUnload<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Drop must never unwind: every step below is on error-safe paths and
        // the user cleanups are contained. A panic of our own completion code is
        // logged (never silently discarded).
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.complete_teardown())) {
            let message = panic_message(payload.as_ref());
            tracing::error!(
                target: "zeokeel_cordis::fiber",
                "completing a cancelled unload panicked: {message}"
            );
        }
    }
}

impl CancelSafeUnload<'_> {
    /// Finish a cancelled teardown without awaiting anything (A-9).
    fn complete_teardown(&mut self) {
        let effects = std::mem::take(&mut self.effects);
        let mut newly_deferred: Vec<AsyncDisposer> = Vec::new();
        let mut first_panic = None;
        // Reverse registration order, like the regular drain.
        for effect in effects.into_iter().rev() {
            match effect.kind {
                EffectKind::Sync(disposer) => {
                    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| disposer.dispose())) {
                        first_panic.get_or_insert(payload);
                    }
                }
                EffectKind::Async(disposer) => newly_deferred.push(disposer),
            }
        }
        // `deferred` is stored newest-first; the async effects of this batch were
        // just collected newest-first too, so they go in front of the older tail.
        let mut deferred = std::mem::take(&mut self.deferred);
        newly_deferred.append(&mut deferred);
        match self.data.lock() {
            Ok(mut fiber) => {
                if fiber.deferred_async.is_empty() {
                    fiber.deferred_async = newly_deferred;
                } else {
                    // A concurrent failure path may have appended; keep it after
                    // the older tail.
                    fiber.deferred_async.extend(newly_deferred);
                }
            }
            Err(_) => {
                // Cannot store the deferred cleanups: their async resources are
                // leaked, so make it loud instead of dropping them silently.
                tracing::error!(
                    target: "zeokeel_cordis::fiber",
                    "cancelled unload could not persist {} deferred async cleanup(s): fiber lock poisoned",
                    newly_deferred.len()
                );
            }
        }
        if let Err(error) = finish_unload(self.data, self.ctx.take(), self.target) {
            tracing::error!(
                target: "zeokeel_cordis::fiber",
                "cancelled unload could not finish its state transition: {error}"
            );
        }
        if let Some(payload) = first_panic {
            log_cleanup_panic("completing a cancelled unload", payload.as_ref());
        }
        tracing::error!(
            target: "zeokeel_cordis::fiber",
            "fiber unload cancelled mid-teardown; sync cleanups ran, unstarted async cleanups were deferred to the next dispose"
        );
    }
}

// ---------------------------------------------------------------------------
// Settlement tracking (`await fiber`)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Lifecycle serialization (`dispose`/`restart`/`update` are exclusive)
// ---------------------------------------------------------------------------

static NEXT_THREAD_TOKEN: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static THREAD_TOKEN: u64 = NEXT_THREAD_TOKEN.fetch_add(1, Ordering::Relaxed);
}

fn current_lifecycle_token() -> u64 {
    THREAD_TOKEN.with(|token| *token)
}

/// Released when the lifecycle operation (including its reentrant nested calls)
/// finishes.
pub(crate) struct LifecycleGuard<'a> {
    data: &'a FiberRef,
}

impl Drop for LifecycleGuard<'_> {
    fn drop(&mut self) {
        if let Ok(fiber) = self.data.lock() {
            if fiber.lifecycle_depth.load(Ordering::SeqCst) > 1 {
                fiber.lifecycle_depth.fetch_sub(1, Ordering::SeqCst);
            } else {
                fiber.lifecycle_depth.store(0, Ordering::SeqCst);
                fiber.lifecycle_owner.store(0, Ordering::SeqCst);
            }
        }
    }
}

/// Claim the fiber's lifecycle slot.
///
/// Concurrent calls from **other** threads fail with `LifecycleBusy` instead of
/// racing; reentrant calls on the owning thread (a cleanup disposing its own
/// fiber, `update` calling `restart`) are allowed.
fn begin_lifecycle(data: &FiberRef) -> Result<LifecycleGuard<'_>> {
    let token = current_lifecycle_token();
    let fiber = data.lock().map_err(|_| CordisError::Disposed)?;
    let owner = fiber.lifecycle_owner.load(Ordering::SeqCst);
    if owner == 0 {
        fiber.lifecycle_owner.store(token, Ordering::SeqCst);
        fiber.lifecycle_depth.store(1, Ordering::SeqCst);
        drop(fiber);
        Ok(LifecycleGuard { data })
    } else if owner == token {
        fiber.lifecycle_depth.fetch_add(1, Ordering::SeqCst);
        drop(fiber);
        Ok(LifecycleGuard { data })
    } else {
        Err(CordisError::LifecycleBusy)
    }
}

/// RAII guard around one lifecycle operation; wakes `await_settled` waiters
/// when the last in-flight operation finishes.
pub(crate) struct OperationGuard<'a> {
    data: &'a FiberRef,
}

impl Drop for OperationGuard<'_> {
    fn drop(&mut self) {
        end_operation(self.data);
    }
}

/// Mark one lifecycle operation as in flight (`await_settled` waits for zero).
///
/// Also used by the context plugin entry points to cover the
/// "fiber registered -> activation started" window (FIX-11).
pub(crate) fn begin_operation(data: &FiberRef) -> OperationGuard<'_> {
    if let Ok(fiber) = data.lock() {
        fiber.pending_ops.fetch_add(1, Ordering::SeqCst);
        if let Ok(mut at) = fiber.last_activity.lock() {
            *at = std::time::Instant::now();
        }
    }
    OperationGuard { data }
}

fn end_operation(data: &FiberRef) {
    let waiters = match data.lock() {
        Ok(fiber) => {
            if let Ok(mut at) = fiber.last_activity.lock() {
                *at = std::time::Instant::now();
            }
            if fiber.pending_ops.fetch_sub(1, Ordering::SeqCst) == 1 {
                fiber
                    .settle_waiters
                    .lock()
                    .map(|mut waiters| {
                        waiters
                            .drain(..)
                            .map(|(_, waker)| waker)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            } else {
                Vec::new()
            }
        }
        Err(_) => Vec::new(),
    };
    for waker in waiters {
        waker.wake();
    }
    let registry = data.lock().ok().and_then(|fiber| fiber.registry.clone());
    if let Some(registry) = registry {
        registry.notify_idle();
    }
}

/// Monotonic id used to unregister `Settled` wakers.
static SETTLED_SEQ: AtomicUsize = AtomicUsize::new(1);

/// Future returned by [`Fiber::settled`] / [`Fiber::await_settled`].
pub struct Settled<'a> {
    data: &'a FiberRef,
    id: u64,
}

impl Drop for Settled<'_> {
    fn drop(&mut self) {
        // Unregister the waker so cancelled waits do not accumulate in the
        // fiber (pre-v0.3 every poll leaked one waker clone).
        if let Ok(fiber) = self.data.lock() {
            if let Ok(mut waiters) = fiber.settle_waiters.lock() {
                waiters.retain(|(id, _)| *id != self.id);
            }
        }
    }
}

impl Future for Settled<'_> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<()>> {
        let data = self.data;
        let pending = match data.lock() {
            Ok(fiber) => fiber.pending_ops.load(Ordering::SeqCst),
            Err(_) => return Poll::Ready(Err(CordisError::Disposed)),
        };
        if pending > 0 {
            if let Ok(fiber) = data.lock() {
                if let Ok(mut waiters) = fiber.settle_waiters.lock() {
                    waiters.retain(|(id, _)| *id != self.id);
                    waiters.push((self.id, cx.waker().clone()));
                }
            }
            // Re-check after registering the waker: an operation may have
            // finished between the first load and the registration.
            if let Ok(fiber) = data.lock() {
                if fiber.pending_ops.load(Ordering::SeqCst) > 0 {
                    return Poll::Pending;
                }
            }
        }
        let error = data.lock().ok().and_then(|fiber| fiber.error.clone());
        match error {
            Some(error) => Poll::Ready(Err(CordisError::PluginFailed(error))),
            None => Poll::Ready(Ok(())),
        }
    }
}

// ---------------------------------------------------------------------------
// Regression tests for the fixes that need internal state (FIX-4, FIX-10)
// ---------------------------------------------------------------------------
//
// The public integration tests live in `tests/regression_core.rs`; the two
// cases below cannot be constructed through the public API (they need a fiber
// whose children/effects are only reachable inside the crate), so they are
// unit tests next to the code they exercise.

#[cfg(test)]
mod regression_tests {
    use super::*;
    use crate::events::{EventOptions, EventResult};

    struct NoopPlugin;

    impl Plugin for NoopPlugin {
        fn name(&self) -> &'static str {
            "regression-noop"
        }

        fn apply(&self, _ctx: Context, _config: Value) -> Result<()> {
            Ok(())
        }
    }

    /// A standalone fiber with no context/registry links (unit-test only).
    fn bare_fiber(id: u64, state: FiberState) -> FiberRef {
        Arc::new(Mutex::new(FiberData {
            id,
            state,
            plugin: PluginKind::Sync(Arc::new(NoopPlugin)),
            inject: &[],
            config: Value::Null,
            raw_config: Value::Null,
            error: None,
            parent: None,
            ctx: None,
            effects: Vec::new(),
            deferred_async: Vec::new(),
            runtime_key: None,
            effect_stack: Vec::new(),
            dep_versions: Mutex::new(Vec::new()),
            registry: None,
            children: Mutex::new(Vec::new()),
            dispose_requested: AtomicBool::new(false),
            cancel_requested: AtomicBool::new(false),
            listener_count: 0,
            last_activity: Mutex::new(std::time::Instant::now()),
            activation_epoch: AtomicU64::new(0),
            lifecycle_owner: AtomicU64::new(0),
            lifecycle_depth: AtomicUsize::new(0),
            pending_ops: AtomicUsize::new(0),
            settle_waiters: Mutex::new(Vec::new()),
        }))
    }

    fn push_effect(data: &FiberRef, label: &'static str, body: crate::effect::Cleanup) {
        if let Ok(mut fiber) = data.lock() {
            fiber.effects.push(FiberEffect {
                kind: EffectKind::Sync(Disposer::new(vec![body])),
                meta: EffectNode::new(label),
                activation: 0,
            });
        }
    }

    fn attach_child(parent: &FiberRef, child: &FiberRef) {
        if let Ok(parent_data) = parent.lock() {
            if let Ok(mut children) = parent_data.children.lock() {
                children.push(Arc::downgrade(child));
            }
        }
    }

    /// FIX-4: a panicking child cleanup must be contained, the sweep must keep
    /// going and the first payload must be returned instead of unwinding.
    #[test]
    fn sweep_contains_a_panicking_child_and_disposes_the_rest() {
        let parent = bare_fiber(1, FiberState::Active);
        let panicking = bare_fiber(2, FiberState::Active);
        push_effect(
            &panicking,
            "panicking-child",
            crate::cleanup(|| panic!("sweep boom")),
        );
        let healthy = bare_fiber(3, FiberState::Active);
        push_effect(&healthy, "healthy-child", crate::cleanup(|| {}));

        // The sweep is LIFO: push the panicking child last so it is disposed
        // first and the healthy sibling proves the sweep continued.
        attach_child(&parent, &healthy);
        attach_child(&parent, &panicking);

        let payload = sweep_children_sync(&parent);
        assert!(
            payload.is_some(),
            "FIX-4: the panicking child's payload must be reported"
        );
        assert_eq!(
            Fiber { data: panicking }.state(),
            FiberState::Disposed,
            "the panicking child must still reach Disposed"
        );
        assert_eq!(
            Fiber { data: healthy }.state(),
            FiberState::Disposed,
            "FIX-4: the sweep must continue after a panicking child"
        );
        assert!(
            parent.lock().unwrap().children.lock().unwrap().is_empty(),
            "the swept children must be cleared"
        );
    }

    /// FIX-4 (async lane): same invariant for `sweep_children_async`.
    #[test]
    fn async_sweep_contains_a_panicking_child_and_disposes_the_rest() {
        let parent = bare_fiber(1, FiberState::Active);
        let panicking = bare_fiber(2, FiberState::Active);
        push_effect(
            &panicking,
            "panicking-child",
            crate::cleanup(|| panic!("async sweep boom")),
        );
        let healthy = bare_fiber(3, FiberState::Active);
        push_effect(&healthy, "healthy-child", crate::cleanup(|| {}));
        attach_child(&parent, &healthy);
        attach_child(&parent, &panicking);

        let payload = crate::block_on(sweep_children_async(&parent));
        assert!(
            payload.is_some(),
            "FIX-4: the async sweep must contain the panic and report it"
        );
        assert_eq!(Fiber { data: panicking }.state(), FiberState::Disposed);
        assert_eq!(Fiber { data: healthy }.state(), FiberState::Disposed);
    }

    /// FIX-10: dropping a fiber without an explicit dispose releases the event
    /// listeners it owns (the previous `Drop` only drained effects).
    #[test]
    fn drop_without_dispose_removes_the_event_listener_owner() {
        let root = Context::root();
        let events = root.events();
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        let _listener = events.add_listener(
            "regression.fix10",
            42,
            EventOptions::default(),
            std::collections::HashMap::new(),
            Arc::new(move |_args: &[Value]| {
                counter.fetch_add(1, Ordering::SeqCst);
                EventResult::Continue
            }),
        );
        root.emit("regression.fix10", &[]);
        assert_eq!(hits.load(Ordering::SeqCst), 1, "listener is registered");

        {
            let data = bare_fiber(42, FiberState::Active);
            if let Ok(mut fiber) = data.lock() {
                // A strong context link, exactly like a live child fiber, but no
                // effects: only `Drop`'s owner sweep can clean the listener.
                fiber.ctx = Some(FiberLink::Strong(root.clone()));
            }
            drop(data);
        }

        root.emit("regression.fix10", &[]);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "FIX-10: a dropped fiber must remove the event listeners it owns"
        );
    }
}
