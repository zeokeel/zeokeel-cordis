//! Plugin fibers: lifecycle state machine, effect ownership and unloading.

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
    /// await them inline (CORE-032).
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
        let mut data = self.data.lock().map_err(|_| CordisError::Disposed)?;
        data.inject = plugin.inject();
        data.plugin = plugin;
        Ok(())
    }

    /// Replace this fiber's plugin with a new sync implementation, re-key its
    /// runtime and reload it (Cordis `registry.replace` for one fiber).
    ///
    /// The runtime move and the reload are rolled back when the reload is
    /// rejected (e.g. `LifecycleBusy`), so a failed replace does not leave a
    /// partially re-bound fiber.
    pub fn replace_plugin(&self, plugin: Arc<dyn Plugin>) -> Result<()> {
        let kind = PluginKind::Sync(plugin);
        let (old_kind, old_key, registry) = self.begin_replace(&kind)?;
        if let Some(registry) = registry.as_ref() {
            if let Err(error) = registry.rebind_single_fiber(self, old_key, kind.key()) {
                self.restore_replace(old_kind, old_key, Some(registry));
                return Err(error);
            }
        }
        if let Err(error) = self.restart() {
            self.restore_replace(old_kind, old_key, registry.as_ref());
            return Err(error);
        }
        Ok(())
    }

    /// Replace this fiber's plugin with a new async implementation and reload it.
    pub async fn replace_plugin_async(&self, plugin: Arc<dyn AsyncPlugin>) -> Result<()> {
        let kind = PluginKind::Async(plugin);
        let (old_kind, old_key, registry) = self.begin_replace(&kind)?;
        if let Some(registry) = registry.as_ref() {
            if let Err(error) = registry.rebind_single_fiber(self, old_key, kind.key()) {
                self.restore_replace(old_kind, old_key, Some(registry));
                return Err(error);
            }
        }
        if let Err(error) = self.restart_async().await {
            self.restore_replace(old_kind, old_key, registry.as_ref());
            return Err(error);
        }
        Ok(())
    }

    fn begin_replace(
        &self,
        kind: &PluginKind,
    ) -> Result<(PluginKind, Option<PluginKey>, Option<Arc<Registry>>)> {
        let mut data = self.data.lock().map_err(|_| CordisError::Disposed)?;
        let old_kind = data.plugin.clone();
        let old_key = data.runtime_key;
        let registry = data.ctx_ref().map(|ctx| ctx.inner.registry.clone());
        data.inject = kind.inject();
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
        if let (Some(registry), Some(old_key)) = (registry, old_key) {
            let current_key = self.data.lock().ok().and_then(|data| data.runtime_key);
            let _ = registry.rebind_single_fiber(self, current_key, old_key);
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
    pub fn restart(&self) -> Result<()> {
        let _guard = begin_operation(&self.data);
        let _lifecycle = begin_lifecycle(&self.data)?;
        let (ctx, plugin, config) = {
            let mut data = self.data.lock().map_err(|_| CordisError::Disposed)?;
            // A disposed fiber is final: restarting it must not resurrect it.
            if data.state == FiberState::Disposed {
                return Err(CordisError::Disposed);
            }
            // Reentrant restart/update from a cleanup must not start a new
            // activation while the old effect list is being drained.
            if data.state == FiberState::Unloading {
                return Err(CordisError::InactiveEffect);
            }
            if data.plugin.is_async() {
                return Err(CordisError::AsyncPluginNeedsAsync(
                    data.plugin.name().to_string(),
                ));
            }
            data.state = FiberState::Unloading;
            (data.ctx_ref(), data.plugin.clone(), data.raw_config.clone())
        };
        unload_fiber(&self.data, FiberState::Pending)?;
        if let Some(ctx) = ctx {
            if dependencies_ready(&ctx, plugin.inject()) {
                let _ = activate_fiber(&self.data, &ctx, &plugin, &config);
            }
        }
        finalize_requested_dispose(&self.data);
        Ok(())
    }

    /// Awaiting variant of [`Fiber::restart`].
    pub async fn restart_async(&self) -> Result<()> {
        let _guard = begin_operation(&self.data);
        let _lifecycle = begin_lifecycle(&self.data)?;
        let (ctx, plugin, config) = {
            let mut data = self.data.lock().map_err(|_| CordisError::Disposed)?;
            if data.state == FiberState::Disposed {
                return Err(CordisError::Disposed);
            }
            if data.state == FiberState::Unloading {
                return Err(CordisError::InactiveEffect);
            }
            data.state = FiberState::Unloading;
            (data.ctx_ref(), data.plugin.clone(), data.raw_config.clone())
        };
        unload_fiber_async(&self.data, FiberState::Pending).await?;
        if let Some(ctx) = ctx {
            if dependencies_ready(&ctx, plugin.inject()) {
                let _ = activate_by_kind_async(&self.data, &ctx, &plugin, &config).await;
            }
        }
        finalize_requested_dispose(&self.data);
        Ok(())
    }

    /// Replace config and restart (Cordis `fiber.update`).
    ///
    /// Order of operations (matching Cordis `_resolveConfig` + `internal/update`):
    /// 1. run the `internal/config` waterfall on the raw config;
    /// 2. validate it against [`Plugin::config_schema`] (Cordis
    ///    `ValidationError`) — on failure the fiber keeps its old config and is
    ///    **not** restarted;
    /// 3. run the `internal/update` waterfall; the terminal stores the config
    ///    and restarts. A hook that never calls `next` vetoes the whole update.
    pub fn update(&self, config: Value) -> Result<()> {
        let _guard = begin_operation(&self.data);
        let _lifecycle = begin_lifecycle(&self.data)?;
        match self.state() {
            FiberState::Disposed => return Err(CordisError::Disposed),
            FiberState::Unloading => return Err(CordisError::InactiveEffect),
            _ => {}
        }
        let (plugin, ctx) = {
            let data = self.data.lock().map_err(|_| CordisError::Disposed)?;
            if data.plugin.is_async() {
                return Err(CordisError::AsyncPluginNeedsAsync(
                    data.plugin.name().to_string(),
                ));
            }
            (data.plugin.clone(), data.ctx_ref())
        };
        let Some(ctx) = ctx else {
            return Err(CordisError::Disposed);
        };
        let previous_raw = self.raw_config();
        let raw = config.clone();
        let resolved = ctx.events().internal().waterfall_config(self, raw.clone());
        let config = validate_config(&plugin, resolved).map_err(CordisError::Validation)?;
        // Commit the raw caller config only after validation succeeded: a
        // rejected update must not replace the retained raw config.
        self.store_raw_config(raw);
        let this = self.clone();
        let passed = config.clone();
        let schema_plugin = plugin.clone();
        let rejected: std::sync::Arc<std::sync::Mutex<Option<crate::config::ValidationError>>> =
            std::sync::Arc::new(std::sync::Mutex::new(None));
        let rejected_for_hook = rejected.clone();
        let proceeded = ctx.events().internal().waterfall_update(
            self,
            config,
            false,
            &move |replaced, _no_save| {
                // A hook that returns a different config is replacing the
                // caller config: that replacement becomes the new raw config
                // (and is resolved once on restart). A pass-through must not be
                // stored, or derived values would compound (CORE-040).
                if replaced != passed {
                    // A replacement must itself be schema-valid before it can
                    // become the next raw config; otherwise reject the whole
                    // update instead of failing later on activation.
                    match validate_config(&schema_plugin, replaced.clone()) {
                        Ok(_) => this.store_raw_config(replaced),
                        Err(error) => {
                            if let Ok(mut slot) = rejected_for_hook.lock() {
                                *slot = Some(error);
                            }
                            return false;
                        }
                    }
                }
                true
            },
        );
        if let Some(error) = rejected.lock().ok().and_then(|mut slot| slot.take()) {
            self.store_raw_config(previous_raw);
            return Err(CordisError::Validation(error));
        }
        if proceeded {
            self.restart()?;
        }
        finalize_requested_dispose(&self.data);
        Ok(())
    }

    /// Awaiting variant of [`Fiber::update`].
    pub async fn update_async(&self, config: Value) -> Result<()> {
        let _guard = begin_operation(&self.data);
        let _lifecycle = begin_lifecycle(&self.data)?;
        match self.state() {
            FiberState::Disposed => return Err(CordisError::Disposed),
            FiberState::Unloading => return Err(CordisError::InactiveEffect),
            _ => {}
        }
        let (plugin, ctx) = {
            let data = self.data.lock().map_err(|_| CordisError::Disposed)?;
            (data.plugin.clone(), data.ctx_ref())
        };
        let Some(ctx) = ctx else {
            return Err(CordisError::Disposed);
        };
        let previous_raw = self.raw_config();
        let raw = config.clone();
        let resolved = ctx.events().internal().waterfall_config(self, raw.clone());
        let config = validate_config(&plugin, resolved).map_err(CordisError::Validation)?;
        self.store_raw_config(raw);
        let this = self.clone();
        let passed = config.clone();
        let schema_plugin = plugin.clone();
        let rejected: std::sync::Arc<std::sync::Mutex<Option<crate::config::ValidationError>>> =
            std::sync::Arc::new(std::sync::Mutex::new(None));
        let rejected_for_hook = rejected.clone();
        let proceeded = ctx.events().internal().waterfall_update(
            self,
            config,
            false,
            &move |replaced, _no_save| {
                if replaced != passed {
                    match validate_config(&schema_plugin, replaced.clone()) {
                        Ok(_) => this.store_raw_config(replaced),
                        Err(error) => {
                            if let Ok(mut slot) = rejected_for_hook.lock() {
                                *slot = Some(error);
                            }
                            return false;
                        }
                    }
                }
                true
            },
        );
        if let Some(error) = rejected.lock().ok().and_then(|mut slot| slot.take()) {
            self.store_raw_config(previous_raw);
            return Err(CordisError::Validation(error));
        }
        if proceeded {
            self.restart_async().await?;
        }
        finalize_requested_dispose(&self.data);
        Ok(())
    }
}

pub(crate) fn dependencies_ready(ctx: &Context, inject: &[&'static str]) -> bool {
    inject.iter().all(|name| ctx.lookup(name).is_some())
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
    inject.iter().all(|name| match ctx.lookup(name) {
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
    sweep_children_and_finalize(data);
    ctx.inner.events.remove_owner(ctx.fiber_id());
}

async fn discard_activation_effects_async(data: &FiberRef, ctx: &Context) {
    let (effects, deferred) = take_activation_effects(data);
    let _ = drain_effects_async(effects, deferred).await;
    sweep_children_async(data).await;
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
}

impl Drop for AsyncActivationGuard {
    fn drop(&mut self) {
        let cancelled = match self.data.lock() {
            Ok(fiber) => fiber.state == FiberState::Loading,
            Err(_) => false,
        };
        if !cancelled {
            return;
        }
        if std::thread::panicking() {
            set_error(
                &self.data,
                "activation cancelled while unwinding".to_string(),
            );
            transition(&self.data, FiberState::Failed);
            return;
        }
        fail_fiber_sync_inner(
            &self.data,
            &self.ctx,
            "activation cancelled before completion".to_string(),
            true,
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
fn sweep_children_sync(data: &FiberRef) {
    let mut sweep = ChildSweep {
        data,
        pending: take_children(data),
        armed: true,
    };
    while let Some(weak) = sweep.pending.last() {
        match child_state(weak) {
            Some(FiberState::Disposed) | None => {}
            Some(_) => {
                if let Some(child) = weak.upgrade() {
                    let child = Fiber { data: child };
                    match child.dispose() {
                        Ok(()) => {}
                        Err(_) => request_child_dispose(weak),
                    }
                }
            }
        }
        sweep.pending.pop();
    }
    sweep.armed = false;
    store_children(data, Vec::new());
}

async fn sweep_children_async(data: &FiberRef) {
    let mut sweep = ChildSweep {
        data,
        pending: take_children(data),
        armed: true,
    };
    while let Some(weak) = sweep.pending.last() {
        match child_state(weak) {
            Some(FiberState::Disposed) | None => {}
            Some(_) => {
                if let Some(child) = weak.upgrade() {
                    let child = Fiber { data: child };
                    // Boxed: the async sweep <-> dispose_async <-> sweep chain
                    // is mutually recursive.
                    match Box::pin(child.dispose_async()).await {
                        Ok(()) => {}
                        Err(_) => request_child_dispose(weak),
                    }
                }
            }
        }
        sweep.pending.pop();
    }
    sweep.armed = false;
    store_children(data, Vec::new());
}

/// Sweep children then finalize (sync context).
fn sweep_children_and_finalize(data: &FiberRef) {
    sweep_children_sync(data);
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
/// `dispose`/`dispose_async` (CORE-032). Services and listeners are removed
/// before the fiber transitions to `Failed`, so a failed plugin leaves no
/// visible registrations behind.
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
    sweep_children_and_finalize(data);
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
    sweep_children_async(data).await;
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
    {
        let mut fiber = data.lock().map_err(|_| CordisError::Disposed)?;
        if state_rejects_activation(fiber.state) {
            return Err(CordisError::Disposed);
        }
        fiber.error = None;
    }
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
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        sync_plugin.apply(ctx.clone(), config.clone())
    }));
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
    {
        let mut fiber = data.lock().map_err(|_| CordisError::Disposed)?;
        if state_rejects_activation(fiber.state) {
            return Err(CordisError::Disposed);
        }
        fiber.error = None;
    }
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
            catch_unwind(AssertUnwindSafe(|| plugin.apply(ctx, config)))
        }
        PluginKind::Async(async_plugin) => {
            let plugin = async_plugin.clone();
            let call_ctx = ctx.clone();
            let call_config = config.clone();
            let future = async move { plugin.apply_async(call_ctx, call_config).await };
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
            return Ok(None);
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
            if !ctx.inner.registry.retire_unready() {
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
        let (key, id) = {
            let fiber = data.lock().map_err(|_| CordisError::Disposed)?;
            (fiber.runtime_key, fiber.id)
        };
        if let (Some(key), Some(ctx)) = (key, ctx.as_ref()) {
            ctx.inner.registry.unregister_runtime_fiber(key, id);
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
    let panic = drain_effects_blocking(effects, deferred);
    sweep_children_and_finalize(data);
    finish_unload(data, ctx, target)?;
    if let Some(payload) = panic {
        std::panic::resume_unwind(payload);
    }
    Ok(())
}

pub(crate) async fn unload_fiber_async(data: &FiberRef, target: FiberState) -> Result<()> {
    let Some((effects, deferred, ctx)) = take_effects(data)? else {
        return Ok(());
    };
    // Cancel-safe drain: if the host drops this future mid-await, the effects
    // that have not started yet are returned to the fiber so the next
    // `dispose_async` retries them (reverse order preserved).
    let mut pending = CancelSafeUnload {
        data,
        effects,
        deferred,
        armed: true,
    };
    let panic = pending.drain().await;
    pending.armed = false;
    drop(pending);
    sweep_children_async(data).await;
    finish_unload(data, ctx, target)?;
    if let Some(payload) = panic {
        std::panic::resume_unwind(payload);
    }
    Ok(())
}

/// Restores un-drained effects to the fiber when the unload future is dropped.
struct CancelSafeUnload<'a> {
    data: &'a FiberRef,
    effects: Vec<FiberEffect>,
    deferred: Vec<AsyncDisposer>,
    armed: bool,
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
        if let Ok(mut fiber) = self.data.lock() {
            // `effects` holds the not-yet-drained prefix in registration order;
            // putting it back keeps the next drain's reverse order correct.
            fiber.effects.append(&mut self.effects);
            fiber.deferred_async.append(&mut self.deferred);
        }
        tracing::error!(
            target: "zeokeel_cordis::fiber",
            "fiber unload cancelled mid-teardown; unstarted effects were restored"
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

fn begin_operation(data: &FiberRef) -> OperationGuard<'_> {
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
