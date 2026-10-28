//! Contexts: scoped service containers and plugin entry points.

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};

use serde_json::Value;

use crate::async_effects::{AsyncCleanup, AsyncDisposer, AsyncEffect};
use crate::config::ConfigSchema;
#[cfg(feature = "supervision")]
use crate::effect::cleanup;
use crate::effect::{Disposer, Effect};
use crate::error::{CordisError, Result};
use crate::events::{AsyncListenerFn, ContextScope, EventOptions, EventResult, Events, ListenerFn};
use crate::exec::{boxed, BoxFuture};
use crate::fiber::{
    activate_fiber, activate_fiber_async, dependencies_ready, EffectKind, EffectNode, Fiber,
    FiberData, FiberEffect, FiberRef, FiberState,
};
use crate::internal::{
    ConfigHook, DispatchHook, GetHook, ListenerHook, ListenerRequest, PluginHook, ServiceHook,
    SetHook, StatusHook, UpdateHook,
};
#[cfg(feature = "supervision")]
use crate::l2::{FiberQuota, StalledFiber};
use crate::logger::{LogExporter, LogLevel, Logger, LoggerService, LOGGER};
use crate::reflect::{Accessor, ContextMeta};
use crate::registry::{AsyncPlugin, Plugin, PluginKey, PluginKind, Registry, RootPlugin};
use crate::service::{AvailabilityCheck, ServiceEntry, ServiceGate, ServiceKey, ServiceValue};
use crate::waterfall::{
    AsyncWaterfallListenerFn, AsyncWaterfallNext, AsyncWaterfallTerminal, WaterfallListenerFn,
    WaterfallNext, WaterfallTerminal,
};

pub(crate) struct ContextInner {
    pub(crate) parent: Option<Context>,
    pub(crate) fiber: Fiber,

    pub(crate) isolate: HashMap<&'static str, u64>,
    pub(crate) intercepts: HashMap<&'static str, Value>,
    pub(crate) events: Arc<Events>,
    pub(crate) registry: Arc<Registry>,
    pub(crate) logger: Arc<LoggerService>,
    /// Services provided anywhere in this context tree, keyed by
    /// `(isolation scope id, name)` (Cordis stores impls on the root reflect
    /// store; scope `0` is the default, unisolated scope shared by every fiber).
    pub(crate) service_store: Arc<Mutex<HashMap<(u64, String), ServiceEntry>>>,
    /// Computed context properties declared on this context (Cordis `accessor`).
    pub(crate) accessors: Mutex<HashMap<&'static str, Accessor>>,
    /// Diagnostic/extension metadata attached with [`Context::extend_meta`].
    pub(crate) meta: ContextMeta,
}

/// RAII guard for the fiber's nested-effect stack: the node is popped even
/// when the effect body panics, so later registrations cannot attach to a stale
/// parent and become invisible (CORE-041).
struct EffectStackScope<'a> {
    fiber: &'a FiberRef,
}

impl Drop for EffectStackScope<'_> {
    fn drop(&mut self) {
        if let Ok(mut fiber) = self.fiber.lock() {
            fiber.effect_stack.pop();
        }
    }
}

/// A scoped dependency container and plugin context.
///
/// Rust cannot proxy property reads, so services are read with
/// `ctx.get(&KEY)`; dependency declarations still use names and gate
/// activation exactly like Cordis `inject`.
#[derive(Clone)]
pub struct Context {
    pub(crate) inner: Arc<ContextInner>,
}

impl Context {
    /// Create the root context with the root fiber (id 0).
    pub fn root() -> Self {
        let events = Arc::new(Events::new());
        let registry = Arc::new(Registry::new());
        let logger = LoggerService::new(LogLevel::Info);
        let data: FiberRef = Arc::new(Mutex::new(FiberData {
            id: 0,
            state: FiberState::Active,
            plugin: PluginKind::Sync(Arc::new(RootPlugin)),
            inject: &[],
            config: Value::Null,
            raw_config: Value::Null,
            error: None,
            parent: None,
            ctx: None,
            registry: Some(registry.clone()),
            effects: Vec::new(),
            deferred_async: Vec::new(),
            runtime_key: None,
            effect_stack: Vec::new(),
            dep_versions: Mutex::new(Vec::new()),
            children: Mutex::new(Vec::new()),
            dispose_requested: std::sync::atomic::AtomicBool::new(false),
            cancel_requested: std::sync::atomic::AtomicBool::new(false),
            listener_count: 0,
            last_activity: Mutex::new(std::time::Instant::now()),
            activation_epoch: std::sync::atomic::AtomicU64::new(0),
            lifecycle_owner: std::sync::atomic::AtomicU64::new(0),
            lifecycle_depth: std::sync::atomic::AtomicUsize::new(0),
            pending_ops: std::sync::atomic::AtomicUsize::new(0),
            settle_waiters: Mutex::new(Vec::new()),
        }));
        let fiber = Fiber { data };
        let inner = Arc::new(ContextInner {
            parent: None,
            fiber,
            isolate: HashMap::new(),
            intercepts: HashMap::new(),
            events,
            registry,
            logger,
            service_store: Arc::new(Mutex::new(HashMap::new())),
            accessors: Mutex::new(HashMap::new()),
            meta: ContextMeta::new(),
        });
        let ctx = Self { inner };
        if let Ok(mut data) = ctx.inner.fiber.data.lock() {
            // Weak self-links: a strong `Context` here would form the
            // Context↔FiberData reference cycle that leaks every root/fiber
            // (CORE-031).
            data.parent = Some(crate::fiber::FiberLink::Weak(Arc::downgrade(&ctx.inner)));
            data.ctx = Some(crate::fiber::FiberLink::Weak(Arc::downgrade(&ctx.inner)));
        }
        // The root fiber is not part of `Registry::fibers`; register it so its
        // lifecycle operations count for quiescence.
        ctx.inner.registry.set_root_fiber(&ctx.inner.fiber);
        // Built-in logger service (Cordis installs `LoggerService` in the root
        // context constructor).
        let logger: ServiceValue = ctx.inner.logger.clone();
        let _ = ctx.insert_service(LOGGER.name(), logger, None, None);
        ctx
    }

    /// Upgrade a weak context reference created by fiber bookkeeping.
    pub(crate) fn from_weak(inner: &Weak<ContextInner>) -> Option<Context> {
        inner.upgrade().map(|inner| Context { inner })
    }

    fn child(parent: &Context, fiber: Fiber) -> Self {
        Self {
            inner: Arc::new(ContextInner {
                parent: Some(parent.clone()),
                fiber,
                isolate: parent.inner.isolate.clone(),
                intercepts: HashMap::new(),
                events: parent.inner.events.clone(),
                registry: parent.inner.registry.clone(),
                logger: parent.inner.logger.clone(),
                service_store: parent.inner.service_store.clone(),
                accessors: Mutex::new(HashMap::new()),
                meta: ContextMeta::new(),
            }),
        }
    }

    pub fn fiber(&self) -> Fiber {
        self.inner.fiber.clone()
    }

    pub fn registry(&self) -> Arc<Registry> {
        self.inner.registry.clone()
    }

    pub fn events(&self) -> Arc<Events> {
        self.inner.events.clone()
    }

    /// Create a child context that inherits services and event bus.
    pub fn extend(&self) -> Self {
        let fiber = self.inner.fiber.clone();
        Self::child(self, fiber)
    }

    /// Create a child whose service `name` is scoped independently.
    ///
    /// A provider registered below this context shadows the parent for that
    /// name; every call creates a fresh scope (Cordis `isolate(name)` without a
    /// label). Use [`Context::isolate_labeled`] to join scopes.
    pub fn isolate(&self, name: &'static str) -> Self {
        let mut isolated = self.inner.isolate.clone();
        isolated.insert(name, self.inner.registry.next_scope_id());
        self.child_with_isolate(isolated)
    }

    /// Create a child whose service `name` joins the scope of `label`.
    ///
    /// Two contexts created with the same `(name, label)` share one service
    /// scope, so a service provided below one is visible below the other
    /// (Cordis `isolate(name, label)`).
    pub fn isolate_labeled(&self, name: &'static str, label: &'static str) -> Self {
        let mut isolated = self.inner.isolate.clone();
        isolated.insert(name, self.inner.registry.labeled_scope_id(label));
        self.child_with_isolate(isolated)
    }

    fn child_with_isolate(&self, isolated: HashMap<&'static str, u64>) -> Self {
        let fiber = self.inner.fiber.clone();
        Self {
            inner: Arc::new(ContextInner {
                parent: Some(self.clone()),
                fiber,
                isolate: isolated,
                intercepts: HashMap::new(),
                events: self.inner.events.clone(),
                registry: self.inner.registry.clone(),
                logger: self.inner.logger.clone(),
                service_store: self.inner.service_store.clone(),
                accessors: Mutex::new(HashMap::new()),
                meta: ContextMeta::new(),
            }),
        }
    }

    pub(crate) fn child_with_meta(&self, meta: ContextMeta) -> Context {
        let fiber = self.inner.fiber.clone();
        Self {
            inner: Arc::new(ContextInner {
                parent: Some(self.clone()),
                fiber,
                isolate: self.inner.isolate.clone(),
                intercepts: HashMap::new(),
                events: self.inner.events.clone(),
                registry: self.inner.registry.clone(),
                logger: self.inner.logger.clone(),
                service_store: self.inner.service_store.clone(),
                accessors: Mutex::new(HashMap::new()),
                meta,
            }),
        }
    }

    /// The isolation scope id for `name`, if any ancestor isolates it.
    pub(crate) fn service_scope(&self, name: &str) -> Option<u64> {
        let mut cursor = Some(self.clone());
        while let Some(ctx) = cursor {
            if let Some(scope) = ctx.inner.isolate.get(name) {
                return Some(*scope);
            }
            cursor = ctx.inner.parent.clone();
        }
        None
    }

    /// Add intercept config for `name` (used by `resolve_config`).
    ///
    /// Intercepts are layered (Cordis keeps them on a prototype chain): the
    /// returned context owns this entry, and [`Context::resolve_config`]
    /// collects ancestor entries root → leaf, then applies base/head around
    /// them. The parent context is not mutated.
    pub fn intercept(&self, name: &'static str, config: Value) -> Self {
        let mut intercepts = HashMap::new();
        intercepts.insert(name, config);
        let fiber = self.inner.fiber.clone();
        Self {
            inner: Arc::new(ContextInner {
                parent: Some(self.clone()),
                fiber,
                isolate: self.inner.isolate.clone(),
                intercepts,
                events: self.inner.events.clone(),
                registry: self.inner.registry.clone(),
                logger: self.inner.logger.clone(),
                service_store: self.inner.service_store.clone(),
                accessors: Mutex::new(HashMap::new()),
                meta: ContextMeta::new(),
            }),
        }
    }

    /// Resolve the current fiber id; the root fiber is 0.
    pub fn fiber_id(&self) -> u64 {
        self.inner.fiber.id()
    }

    /// Register a typed service. The returned disposer removes it; service
    /// removal re-evaluates fibers that injected this name.
    pub fn provide<T: Any + Send + Sync>(
        &self,
        key: ServiceKey<T>,
        value: Arc<T>,
    ) -> Result<Disposer> {
        self.provide_entry(key, value, None)
    }

    /// Register a typed service together with an availability predicate
    /// (Cordis `Service.check`).
    ///
    /// Dependents only activate while the predicate returns `true`. Re-evaluate
    /// them after the backing state changes with [`Context::refresh`], or use
    /// [`Context::provide_gated`], which refreshes automatically.
    pub fn provide_checked<T, F>(
        &self,
        key: ServiceKey<T>,
        value: Arc<T>,
        check: F,
    ) -> Result<Disposer>
    where
        T: Any + Send + Sync,
        F: Fn() -> bool + Send + Sync + 'static,
    {
        self.provide_entry(key, value, Some(Arc::new(check)))
    }

    /// Register a typed service gated by a [`ServiceGate`]. Flipping the gate
    /// re-evaluates this context's dependents automatically.
    pub fn provide_gated<T: Any + Send + Sync>(
        &self,
        key: ServiceKey<T>,
        value: Arc<T>,
        gate: &ServiceGate,
    ) -> Result<Disposer> {
        gate.watch(self);
        self.provide_entry(key, value, Some(gate.check()))
    }

    fn provide_entry<T: Any + Send + Sync>(
        &self,
        key: ServiceKey<T>,
        value: Arc<T>,
        check: Option<AvailabilityCheck>,
    ) -> Result<Disposer> {
        let service: ServiceValue = value;
        self.insert_service(key.name(), service, check, None)
    }

    /// Register an untyped service value (used by [`crate::service::provide_service`]
    /// and by the built-in logger service).
    pub(crate) fn provide_service_value(
        &self,
        name: &'static str,
        service: ServiceValue,
        check: Option<AvailabilityCheck>,
        schema: Option<Arc<dyn ConfigSchema>>,
    ) -> Result<Disposer> {
        self.insert_service(name, service, check, schema)
    }

    fn insert_service(
        &self,
        name: &'static str,
        service: ServiceValue,
        check: Option<AvailabilityCheck>,
        schema: Option<Arc<dyn ConfigSchema>>,
    ) -> Result<Disposer> {
        let entry = ServiceEntry {
            value: service.clone(),
            check,
            schema,
            owner: self.fiber_id(),
            version: crate::service::next_version(),
            withdrawn: false,
        };
        // Cordis registers the impl on the root reflect store, keyed by the
        // isolation label for this name; scope `0` is the default scope every
        // unisolated context resolves to, so a service provided by one fiber is
        // visible to sibling fibers and to the root (official semantics).
        let scope = self.service_scope(name).unwrap_or(0);
        let key = (scope, name.to_string());
        {
            let mut store = self
                .inner
                .service_store
                .lock()
                .map_err(|_| CordisError::Disposed)?;
            if store.contains_key(&key) {
                return Err(CordisError::DuplicateService(name.to_string()));
            }
            store.insert(key.clone(), entry);
        }
        self.inner
            .events
            .internal()
            .emit_service(self, name, Some(&service));
        // Weak capture: the disposer lives in the providing fiber's effect
        // list, so a strong `Context` here would put the context back into its
        // own fiber graph (Context↔FiberData cycle; CORE-031).
        // Remove the entry through the shared store/event handles so the
        // rollback still works when the providing context was already dropped
        // (upgrading the Weak alone would silently leave a stale service).
        let store = Arc::clone(&self.inner.service_store);
        let events = Arc::clone(&self.inner.events);
        let weak = Arc::downgrade(&self.inner);
        let disposer = Disposer::new(vec![Box::new(move || {
            if let Ok(mut store) = store.lock() {
                store.remove(&key);
            }
            if let Some(inner) = weak.upgrade() {
                let ctx = Context { inner };
                events.internal().emit_service(&ctx, name, None);
                ctx.refresh_dependents();
            }
        })]);
        self.register_effect_labeled(disposer.clone(), "ctx.provide")?;
        self.refresh_dependents();
        Ok(disposer)
    }

    /// Typed lookup with child→root inheritance (respecting `isolate` and the
    /// provider's [`AvailabilityCheck`]).
    ///
    /// Dispatches the typed `internal/get` waterfall before reporting the
    /// service, so extensions can observe/override service reads (Cordis
    /// `internal/get`). Hooks that call `ctx.get()` again do not recurse.
    /// Read an accessor with panic containment: a broken accessor must not
    /// take down service lookup (panic-containment policy; see docs/CORDIS-RUST.md).
    fn accessor_value(&self, accessor: &Accessor) -> Option<ServiceValue> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| accessor.read(self))) {
            Ok(value) => Some(value),
            Err(payload) => {
                tracing::error!(
                    target: "zeokeel_cordis::context",
                    "accessor read panicked: {}",
                    crate::fiber::panic_message(payload.as_ref())
                );
                None
            }
        }
    }

    pub fn get<T: Any + Send + Sync>(&self, key: ServiceKey<T>) -> Option<Arc<T>> {
        let name = key.name();
        if let Some(accessor) = self.find_accessor(name) {
            return self
                .accessor_value(&accessor)
                .and_then(|value| value.downcast::<T>().ok());
        }
        let internal = self.inner.events.internal().clone();
        let value =
            internal.waterfall_get(self, name, &|| self.lookup(name).map(|entry| entry.value));
        value.and_then(|value| value.downcast::<T>().ok())
    }

    pub fn require<T: Any + Send + Sync>(&self, key: ServiceKey<T>) -> Result<Arc<T>> {
        self.get(key)
            .ok_or_else(|| CordisError::UnknownService(key.name().to_string()))
    }

    /// Register a **trait-object** (or otherwise unsized) service.
    ///
    /// This is the `?Sized` counterpart of [`Context::provide`]: the value is
    /// stored behind a type-erased wrapper and read back with
    /// [`Context::get_any`] / [`Context::require_any`]. Credentials/services
    /// exposed as `Arc<dyn Trait>` should use this pair.
    pub fn provide_any<T: ?Sized + Send + Sync + 'static>(
        &self,
        key: ServiceKey<T>,
        value: Arc<T>,
    ) -> Result<Disposer> {
        self.insert_service(key.name(), crate::service::wrap_service(value), None, None)
    }

    /// Look up a **trait-object** service registered by [`Context::provide_any`].
    pub fn get_any<T: ?Sized + Send + Sync + 'static>(&self, key: ServiceKey<T>) -> Option<Arc<T>> {
        let name = key.name();
        if let Some(accessor) = self.find_accessor(name) {
            return self
                .accessor_value(&accessor)
                .and_then(|value| crate::service::unwrap_service::<T>(&value));
        }
        let internal = self.inner.events.internal().clone();
        let value =
            internal.waterfall_get(self, name, &|| self.lookup(name).map(|entry| entry.value));
        value.and_then(|value| crate::service::unwrap_service::<T>(&value))
    }

    /// [`Context::get_any`] with an error when the service is missing/inactive.
    pub fn require_any<T: ?Sized + Send + Sync + 'static>(
        &self,
        key: ServiceKey<T>,
    ) -> Result<Arc<T>> {
        self.get_any(key)
            .ok_or_else(|| CordisError::UnknownService(key.name().to_string()))
    }

    /// Overwrite a **trait-object** service registered by the current fiber.
    ///
    /// Mirrors [`Context::set`]'s ownership rule: only the providing fiber may
    /// write the service.
    pub fn set_any<T: ?Sized + Send + Sync + 'static>(
        &self,
        key: ServiceKey<T>,
        value: Arc<T>,
    ) -> Result<()> {
        let name = key.name();
        if let Some(accessor) = self.find_accessor(name) {
            let wrapped = crate::service::wrap_service(value);
            return match accessor.set {
                Some(set) => {
                    let allowed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        set(self, wrapped)
                    }))
                    .unwrap_or(false);
                    if allowed {
                        Ok(())
                    } else {
                        Err(CordisError::ServiceWriteForbidden(name.to_string()))
                    }
                }
                None => Err(CordisError::AccessorReadOnly(name.to_string())),
            };
        }
        let wrapped = crate::service::wrap_service(value);
        self.set_revertible(name, wrapped).map(|_| ())
    }

    /// Whether `name` currently resolves to an available implementation from
    /// this context (diagnostics for gated services).
    pub fn service_available(&self, name: &str) -> bool {
        self.lookup(name).is_some()
    }

    pub(crate) fn lookup(&self, name: &str) -> Option<ServiceEntry> {
        // Resolve within the scope this context maps `name` to; an unavailable
        // impl (Service.check false) shadows the scope instead of falling back
        // to the default scope (same rule as Cordis).
        let scope = self.service_scope(name).unwrap_or(0);
        // Take the entry out of the store *before* running the availability
        // check: a check may itself read services or log (official Cordis
        // allows re-entrant checks), and holding the store mutex across user
        // code would self-deadlock.
        let entry = {
            let store = self.inner.service_store.lock().ok()?;
            store.get(&(scope, name.to_string())).cloned()
        };
        entry.filter(|entry| entry.is_available())
    }

    /// Cooperative cancellation checkpoint.
    ///
    /// Returns `Err(CordisError::Cancelled)` once the owning fiber was
    /// cancelled (`Fiber::cancel`), letting long-running plugin work unwind
    /// through the normal failure/rollback path. Cheap enough for hot loops.
    pub fn checkpoint(&self) -> Result<()> {
        if self
            .inner
            .fiber
            .data
            .lock()
            .map(|fiber| {
                fiber
                    .cancel_requested
                    .load(std::sync::atomic::Ordering::SeqCst)
            })
            .unwrap_or(true)
        {
            Err(CordisError::Cancelled)
        } else {
            Ok(())
        }
    }

    /// Dispose every fiber in the tree and the root fiber (shutdown path).
    ///
    /// Equivalent to disposing each fiber bottom-up plus the root fiber; after
    /// it returns `Registry::is_idle()` is true and nothing is registered.
    /// Hosts should call this before dropping a root context: dispose+drop is
    /// the only path that reclaims the whole graph (undisposed fibers keep it).
    pub fn dispose_all(&self) -> Result<()> {
        self.inner.registry.dispose_all()
    }

    /// Resolve once the whole context tree is quiescent (no in-flight lifecycle
    /// work). Useful for tests, shutdown and supervisor loops.
    pub async fn when_idle(&self) {
        self.inner.registry.when_idle().await;
    }

    /// Re-evaluate every dependent fiber.
    ///
    /// Call this after an `AvailabilityCheck`'s backing state changed, or after
    /// an out-of-band service mutation that bypassed the effect lifecycle.
    pub fn refresh(&self) {
        self.refresh_dependents();
    }

    /// Async counterpart of [`Context::refresh`]: also awaits async plugin
    /// activation/unload, which a synchronous refresh cannot do.
    pub async fn refresh_async(&self) {
        self.inner.registry.refresh_all_async().await;
    }

    /// Attach an already-created disposer to the current fiber as a labeled
    /// effect (used by add-on crates such as `cordis-timer`); unloading or
    /// restarting the fiber disposes it.
    pub fn attach_effect(&self, disposer: Disposer, label: &'static str) -> Result<()> {
        self.register_effect_labeled(disposer, label)
    }

    /// Reserve one listener slot on this fiber (quota enforcement).
    #[cfg(feature = "supervision")]
    fn reserve_listener(&self) -> Result<()> {
        let max = self.inner.registry.quota_limits().max_listeners;
        let mut fiber = self
            .inner
            .fiber
            .data
            .lock()
            .map_err(|_| CordisError::Disposed)?;
        if let Some(max) = max {
            if fiber.listener_count >= max {
                return Err(CordisError::QuotaExceeded("listeners".to_string()));
            }
        }
        fiber.listener_count += 1;
        Ok(())
    }

    /// Own a newly registered listener: enforce the listener quota, wrap the
    /// disposer so the count is released on cleanup, and attach it to the fiber.
    #[cfg(feature = "supervision")]
    fn register_listener_effect(
        &self,
        disposer: Disposer,
        label: &'static str,
    ) -> Result<Disposer> {
        if let Err(error) = self.reserve_listener() {
            disposer.dispose();
            return Err(error);
        }
        let fiber_ref = self.inner.fiber.clone();
        let inner = disposer.clone();
        let counted = Disposer::new(vec![cleanup(move || {
            if let Ok(mut fiber) = fiber_ref.data.lock() {
                fiber.listener_count = fiber.listener_count.saturating_sub(1);
            }
            inner.dispose();
        })]);
        match self.register_effect_labeled(counted, label) {
            Ok(()) => Ok(disposer),
            Err(error) => {
                if let Ok(mut fiber) = self.inner.fiber.data.lock() {
                    fiber.listener_count = fiber.listener_count.saturating_sub(1);
                }
                disposer.dispose();
                Err(error)
            }
        }
    }

    /// Without the `supervision` feature listeners are attached directly (no
    /// quota bookkeeping in the core kernel).
    #[cfg(not(feature = "supervision"))]
    fn register_listener_effect(
        &self,
        disposer: Disposer,
        label: &'static str,
    ) -> Result<Disposer> {
        self.register_effect_labeled(disposer.clone(), label)?;
        Ok(disposer)
    }

    /// Update the current fiber's watchdog activity timestamp.
    #[cfg(feature = "supervision")]
    pub fn heartbeat(&self) {
        self.inner.fiber.heartbeat();
    }

    /// Set the per-fiber resource quota applied to the whole registry tree.
    #[cfg(feature = "supervision")]
    pub fn set_quota(&self, quota: FiberQuota) {
        self.inner.registry.set_quota(quota);
    }

    /// Current per-fiber resource quota.
    #[cfg(feature = "supervision")]
    pub fn quota(&self) -> FiberQuota {
        self.inner.registry.quota()
    }

    /// Fibers with work in flight that have been quiet for at least `timeout`
    /// (cooperative watchdog; see [`crate::IsolationCapabilities`]).
    #[cfg(feature = "supervision")]
    pub fn stalled_fibers(&self, timeout: std::time::Duration) -> Vec<StalledFiber> {
        self.inner.registry.stalled_fibers(timeout)
    }

    pub(crate) fn register_effect_labeled(
        &self,
        disposer: Disposer,
        label: &'static str,
    ) -> Result<()> {
        let node = EffectNode::new(label);
        self.attach_effect_node(&node)?;
        self.push_effect(FiberEffect {
            kind: EffectKind::Sync(disposer),
            meta: node,
            activation: 0,
        })
    }

    /// Attach a new effect node to the currently running effect (if any), so
    /// `Fiber::get_effects` reports the nested tree Cordis exposes.
    fn attach_effect_node(&self, node: &Arc<Mutex<EffectNode>>) -> Result<()> {
        let fiber = self
            .inner
            .fiber
            .data
            .lock()
            .map_err(|_| CordisError::Disposed)?;
        if let Some(parent) = fiber.effect_stack.last() {
            if let Ok(mut parent) = parent.lock() {
                parent.children.push(Arc::clone(node));
            }
            if let Ok(mut node) = node.lock() {
                node.parent = Some(Arc::downgrade(parent));
            }
        }
        Ok(())
    }

    fn push_effect(&self, effect: FiberEffect) -> Result<()> {
        #[cfg(feature = "supervision")]
        let max_effects = self.inner.registry.quota_limits().max_effects;
        #[cfg(not(feature = "supervision"))]
        let _max_effects: Option<usize> = None;
        let mut fiber = self
            .inner
            .fiber
            .data
            .lock()
            .map_err(|_| CordisError::Disposed)?;
        if fiber.state == FiberState::Disposed || fiber.state == FiberState::Unloading {
            return Err(CordisError::InactiveEffect);
        }
        #[cfg(feature = "supervision")]
        if let Some(max) = max_effects {
            if fiber.effects.len() >= max {
                return Err(CordisError::QuotaExceeded("effects".to_string()));
            }
        }
        let mut effect = effect;
        effect.activation = fiber
            .activation_epoch
            .load(std::sync::atomic::Ordering::SeqCst);
        fiber.effects.push(effect);
        Ok(())
    }

    /// Run `setup` while its effect node is the current parent for nested
    /// registrations, then own the resulting disposer with that node.
    fn run_labeled_effect<F>(&self, label: &'static str, setup: F) -> Result<Disposer>
    where
        F: FnOnce() -> Effect,
    {
        let node = EffectNode::new(label);
        self.attach_effect_node(&node)?;
        {
            let mut fiber = self
                .inner
                .fiber
                .data
                .lock()
                .map_err(|_| CordisError::Disposed)?;
            if fiber.state == FiberState::Disposed || fiber.state == FiberState::Unloading {
                return Err(CordisError::InactiveEffect);
            }
            fiber.effect_stack.push(Arc::clone(&node));
        }
        let scope = EffectStackScope {
            fiber: &self.inner.fiber.data,
        };
        let disposer = Disposer::new(setup());
        drop(scope);
        self.push_effect(FiberEffect {
            kind: EffectKind::Sync(disposer.clone()),
            meta: node,
            activation: 0,
        })?;
        Ok(disposer)
    }

    fn run_labeled_effect_async<F>(&self, label: &'static str, setup: F) -> Result<AsyncDisposer>
    where
        F: FnOnce() -> AsyncEffect,
    {
        let node = EffectNode::new(label);
        self.attach_effect_node(&node)?;
        {
            let mut fiber = self
                .inner
                .fiber
                .data
                .lock()
                .map_err(|_| CordisError::Disposed)?;
            if fiber.state == FiberState::Disposed || fiber.state == FiberState::Unloading {
                return Err(CordisError::InactiveEffect);
            }
            fiber.effect_stack.push(Arc::clone(&node));
        }
        let scope = EffectStackScope {
            fiber: &self.inner.fiber.data,
        };
        let disposer = AsyncDisposer::new(setup());
        drop(scope);
        self.push_effect(FiberEffect {
            kind: EffectKind::Async(disposer.clone()),
            meta: node,
            activation: 0,
        })?;
        Ok(disposer)
    }

    /// Register cleanup-aware effect. Callbacks run in reverse registration
    /// order when the fiber unloads, or when the returned disposer is called.
    pub fn effect<F>(&self, setup: F) -> Result<Disposer>
    where
        F: FnOnce() -> Effect,
    {
        self.effect_labeled("anonymous", setup)
    }

    /// [`Context::effect`] with an explicit diagnostics label (Cordis
    /// `ctx.effect(body, label)`), recorded in `Fiber::get_effects`.
    pub fn effect_labeled<F>(&self, label: &'static str, setup: F) -> Result<Disposer>
    where
        F: FnOnce() -> Effect,
    {
        self.run_labeled_effect(label, setup)
    }

    /// Register an async effect.
    ///
    /// The body runs immediately (like [`Context::effect`]); the returned async
    /// cleanups are awaited in reverse registration order by
    /// [`Fiber::dispose_async`] (or directly by [`AsyncDisposer::dispose`]).
    /// Awaiting them rather than spawning them is what makes teardown
    /// deterministic: when disposal returns, every resource is released.
    ///
    /// ```no_run
    /// # use zeokeel_cordis::{cleanup_async, Context};
    /// # async fn demo(ctx: &Context) -> zeokeel_cordis::Result<()> {
    /// let effect = ctx.effect_async(|| {
    ///     vec![cleanup_async(|| async {
    ///         // release the connection; the future resolves when it is gone
    ///     })]
    /// })?;
    /// // ... later, from async code (or `Fiber::dispose_async` when the
    /// // owning plugin unloads). Disposal returns only after the cleanup ran.
    /// effect.dispose().await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn effect_async<F>(&self, setup: F) -> Result<AsyncDisposer>
    where
        F: FnOnce() -> AsyncEffect,
    {
        self.effect_async_labeled("anonymous", setup)
    }

    /// [`Context::effect_async`] with an explicit diagnostics label.
    pub fn effect_async_labeled<F>(&self, label: &'static str, setup: F) -> Result<AsyncDisposer>
    where
        F: FnOnce() -> AsyncEffect,
    {
        self.run_labeled_effect_async(label, setup)
    }

    /// Load a plugin. The fiber starts Pending until every `inject` service is
    /// available; it is returned immediately like Cordis `ctx.plugin()`.
    pub fn plugin(&self, plugin: Arc<dyn Plugin>, config: Value) -> Result<Fiber> {
        let key = plugin.key();
        self.plugin_with_key(plugin, config, key)
    }

    /// Load a plugin whose registry identity is its concrete `TypeId`
    /// (`PluginKey::Type`).
    ///
    /// Two separately-constructed instances of the same plugin type share one
    /// [`crate::registry::PluginRuntime`] while keeping independent fibers, the
    /// same way Cordis dedupes by the plugin callback function.
    pub fn plugin_typed<P: Plugin>(&self, plugin: Arc<P>, config: Value) -> Result<Fiber> {
        let key = PluginKey::Type(std::any::TypeId::of::<P>());
        let plugin: Arc<dyn Plugin> = plugin;
        self.plugin_with_key(plugin, config, key)
    }

    /// Shared fiber creation for sync and async plugins.
    fn create_fiber_handle(
        &self,
        plugin: PluginKind,
        config: Value,
        key: PluginKey,
    ) -> Result<Fiber> {
        if self.inner.fiber.state() == FiberState::Disposed {
            return Err(CordisError::Disposed);
        }
        // Loading-depth guard: recursive loading (A -> B -> A, or a plugin that
        // keeps loading itself) is contained instead of overflowing the stack.
        let mut depth = 0usize;
        let mut cursor = Some(self.clone());
        while let Some(ancestor) = cursor {
            depth += 1;
            cursor = ancestor.inner.parent.clone();
        }
        if depth >= crate::fiber::MAX_PLUGIN_DEPTH {
            return Err(CordisError::InvalidPlugin(format!(
                "plugin loading depth limit ({}) exceeded at {}",
                crate::fiber::MAX_PLUGIN_DEPTH,
                plugin.name()
            )));
        }
        #[cfg(feature = "supervision")]
        if let Some(max) = self.inner.registry.quota_limits().max_child_fibers {
            let live_children = self
                .inner
                .fiber
                .data
                .lock()
                .ok()
                .and_then(|parent| parent.children.lock().ok().map(|children| children.len()))
                .unwrap_or(0);
            if live_children >= max {
                return Err(CordisError::QuotaExceeded("child_fibers".to_string()));
            }
        }
        let id = self.inner.registry.next_id();
        let name = plugin.name();
        let data: FiberRef = Arc::new(Mutex::new(FiberData {
            id,
            state: FiberState::Pending,
            plugin: plugin.clone(),
            inject: plugin.inject(),
            config: config.clone(),
            raw_config: config,
            error: None,
            parent: Some(crate::fiber::FiberLink::Strong(self.clone())),
            ctx: None,
            registry: Some(self.inner.registry.clone()),
            effects: Vec::new(),
            deferred_async: Vec::new(),
            runtime_key: Some(key),
            effect_stack: Vec::new(),
            dep_versions: Mutex::new(Vec::new()),
            children: Mutex::new(Vec::new()),
            dispose_requested: std::sync::atomic::AtomicBool::new(false),
            cancel_requested: std::sync::atomic::AtomicBool::new(false),
            listener_count: 0,
            last_activity: Mutex::new(std::time::Instant::now()),
            activation_epoch: std::sync::atomic::AtomicU64::new(0),
            lifecycle_owner: std::sync::atomic::AtomicU64::new(0),
            lifecycle_depth: std::sync::atomic::AtomicUsize::new(0),
            pending_ops: std::sync::atomic::AtomicUsize::new(0),
            settle_waiters: Mutex::new(Vec::new()),
        }));
        let fiber = Fiber { data: data.clone() };
        let child = Context::child(self, fiber.clone());
        if let Ok(mut fiber_data) = data.lock() {
            fiber_data.ctx = Some(crate::fiber::FiberLink::Strong(child.clone()));
        }
        self.inner.registry.register_fiber(fiber.clone());
        self.inner
            .registry
            .register_runtime_fiber(key, name, &fiber)?;
        // Parent ownership: remember the child on the parent fiber (weak, so no
        // parent↔child Arc cycle). The parent sweeps its children when it
        // unloads and when an activation rolls back (CORE-047).
        {
            let parent_state = self.inner.fiber.data.lock().ok().map(|parent| parent.state);
            if parent_state != Some(FiberState::Active)
                && parent_state != Some(FiberState::Loading)
                && parent_state != Some(FiberState::Pending)
            {
                let _ = fiber.dispose();
                return Err(CordisError::Disposed);
            }
            if let Ok(parent) = self.inner.fiber.data.lock() {
                if let Ok(mut children) = parent.children.lock() {
                    children.push(Arc::downgrade(&data));
                }
            }
        }
        // Ownership effect: gives the cascade a deterministic place in the
        // parent's effect list, so a cancelled disposal restores it and the
        // retry disposes the children (the child registry below is the
        // recovery path for busy children).
        let weak_child = Arc::downgrade(&data);
        let ownership = self.effect_async_labeled("ctx.plugin", move || {
            let weak_child = weak_child.clone();
            vec![crate::async_effects::cleanup_async(move || {
                let weak_child = weak_child.clone();
                async move {
                    if let Some(data) = weak_child.upgrade() {
                        let child = Fiber { data };
                        if child.dispose_async().await.is_err() {
                            if let Ok(fiber) = child.data.lock() {
                                fiber
                                    .dispose_requested
                                    .store(true, std::sync::atomic::Ordering::SeqCst);
                            }
                        }
                    }
                }
            })]
        });
        if let Err(error) = ownership {
            let _ = fiber.dispose();
            return Err(error);
        }
        self.inner.events.internal().emit_plugin(&fiber);
        Ok(fiber)
    }

    /// Resolve the fiber's context + kind + config when its inject deps are ready.
    fn ready_activation(fiber: &Fiber) -> Option<(Context, PluginKind, Value)> {
        let (ctx, plugin, config, inject) = {
            let data = fiber.data.lock().ok()?;
            (
                data.ctx_ref()?,
                data.plugin.clone(),
                data.raw_config.clone(),
                data.inject,
            )
        };
        if !dependencies_ready(&ctx, inject) {
            return None;
        }
        Some((ctx, plugin, config))
    }

    fn plugin_with_key(
        &self,
        plugin: Arc<dyn Plugin>,
        config: Value,
        key: PluginKey,
    ) -> Result<Fiber> {
        let fiber = self.create_fiber_handle(PluginKind::Sync(plugin), config, key)?;
        if let Some((ctx, kind, config)) = Self::ready_activation(&fiber) {
            let _ = activate_fiber(&fiber.data, &ctx, &kind, &config);
        }
        Ok(fiber)
    }

    /// Load an **async plugin**. Activation is awaited here when its inject
    /// dependencies are already available; otherwise the fiber stays `Pending`
    /// until the host awaits [`Context::refresh_async`] (or calls
    /// [`Fiber::restart_async`]).
    pub async fn plugin_async(&self, plugin: Arc<dyn AsyncPlugin>, config: Value) -> Result<Fiber> {
        let key = plugin.key();
        let fiber = self.create_fiber_handle(PluginKind::Async(plugin), config, key)?;
        if let Some((ctx, kind, config)) = Self::ready_activation(&fiber) {
            let _ = activate_fiber_async(&fiber.data, &ctx, &kind, &config).await;
        }
        Ok(fiber)
    }

    /// Async plugin whose registry identity is its concrete `TypeId`.
    pub async fn plugin_async_typed<P: AsyncPlugin>(
        &self,
        plugin: Arc<P>,
        config: Value,
    ) -> Result<Fiber> {
        let key = PluginKey::Type(std::any::TypeId::of::<P>());
        let plugin: Arc<dyn AsyncPlugin> = plugin;
        let fiber = self.create_fiber_handle(PluginKind::Async(plugin), config, key)?;
        if let Some((ctx, kind, config)) = Self::ready_activation(&fiber) {
            let _ = activate_fiber_async(&fiber.data, &ctx, &kind, &config).await;
        }
        Ok(fiber)
    }

    /// Cordis `ctx.inject(deps, callback)` convenience: load a closure plugin.
    pub fn inject<F>(&self, deps: &'static [&'static str], callback: F) -> Result<Fiber>
    where
        F: Fn(Context) -> Result<()> + Send + Sync + 'static,
    {
        struct Injected<F>(F, &'static [&'static str]);
        impl<F> Plugin for Injected<F>
        where
            F: Fn(Context) -> Result<()> + Send + Sync + 'static,
        {
            fn name(&self) -> &'static str {
                "injected"
            }

            fn inject(&self) -> &'static [&'static str] {
                self.1
            }

            fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
                (self.0)(ctx)
            }
        }
        self.plugin(Arc::new(Injected(callback, deps)), Value::Null)
    }

    /// Register an event listener owned by the current fiber.
    pub fn on<F>(&self, name: &'static str, listener: F) -> Result<Disposer>
    where
        F: Fn(&[Value]) -> EventResult + Send + Sync + 'static,
    {
        self.on_with(name, false, false, listener)
    }

    pub fn on_with<F>(
        &self,
        name: &'static str,
        prepend: bool,
        global: bool,
        listener: F,
    ) -> Result<Disposer>
    where
        F: Fn(&[Value]) -> EventResult + Send + Sync + 'static,
    {
        self.on_with_options(
            name,
            EventOptions {
                prepend,
                global,
                once: false,
            },
            listener,
        )
    }

    /// Register a sync event listener with explicit [`EventOptions`] (Cordis
    /// `ctx.on(name, listener, { prepend, global })`).
    pub fn on_with_options<F>(
        &self,
        name: &'static str,
        options: EventOptions,
        listener: F,
    ) -> Result<Disposer>
    where
        F: Fn(&[Value]) -> EventResult + Send + Sync + 'static,
    {
        let owner = self.fiber_id();
        let (callback, options) = self.resolve_listener(name, options, owner, Arc::new(listener));
        let disposer = Events::add_listener(
            &self.inner.events,
            name,
            owner,
            options,
            self.inner.isolate.clone(),
            callback,
        );
        self.register_listener_effect(disposer, "ctx.on")
    }

    /// Register a self-removing sync listener (Cordis `ctx.once`).
    pub fn once<F>(&self, name: &'static str, listener: F) -> Result<Disposer>
    where
        F: Fn(&[Value]) -> EventResult + Send + Sync + 'static,
    {
        self.on_with_options(name, EventOptions::default().once(), listener)
    }

    /// Run the typed `internal/listener` bail chain: the first hook returning a
    /// replacement wins (Cordis `internal/listener` bail semantics).
    fn resolve_listener(
        &self,
        name: &str,
        options: EventOptions,
        owner: u64,
        callback: ListenerFn,
    ) -> (ListenerFn, EventOptions) {
        let request = ListenerRequest {
            name,
            prepend: options.prepend,
            global: options.global,
            once: options.once,
            owner,
        };
        match self.inner.events.internal().intercept_listener(&request) {
            Some(over) => (
                over.callback,
                EventOptions {
                    prepend: over.prepend,
                    global: over.global,
                    once: over.once,
                },
            ),
            None => (callback, options),
        }
    }

    /// Register an async event listener owned by the current fiber.
    ///
    /// Async listeners are dispatched by [`Context::parallel_async`] and
    /// [`Context::serial_async`]; a sync `emit`/`bail` does not poll them. The
    /// returned future must be `'static`, so copy any dispatch argument you need
    /// into the future before returning it.
    ///
    /// The returned disposer is synchronous because unregistering is
    /// synchronous.
    pub fn on_async<F, Fut>(&self, name: &'static str, listener: F) -> Result<Disposer>
    where
        F: Fn(&[Value]) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = EventResult> + Send + 'static,
    {
        self.on_async_with(name, false, listener)
    }

    /// [`Context::on_async`] with placement control (mirrors
    /// [`Context::on_with`], without the currently unused `global` flag).
    pub fn on_async_with<F, Fut>(
        &self,
        name: &'static str,
        prepend: bool,
        listener: F,
    ) -> Result<Disposer>
    where
        F: Fn(&[Value]) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = EventResult> + Send + 'static,
    {
        let owner = self.fiber_id();
        let callback: AsyncListenerFn =
            Arc::new(move |args: &[Value]| -> BoxFuture<'static, EventResult> {
                boxed(listener(args))
            });
        let disposer = Events::add_async_listener(
            &self.inner.events,
            name,
            owner,
            EventOptions {
                prepend,
                global: false,
                once: false,
            },
            self.inner.isolate.clone(),
            callback,
        );
        self.register_listener_effect(disposer, "ctx.on_async")
    }

    // -----------------------------------------------------------------------
    // Async effect bodies (Cordis `ctx.effect(async () => ...)`)
    // -----------------------------------------------------------------------

    /// Register an effect whose body is a future (Cordis `ctx.effect(async () => ...)`).
    ///
    /// The future is driven to completion immediately (see [`crate::block_on`]) so
    /// registration stays synchronous; the cleanups it returns are awaited by
    /// [`crate::Fiber::dispose_async`] / `restart_async` in reverse order. A body
    /// error is returned to the caller, which fails the plugin when it happens
    /// during activation.
    ///
    /// ```no_run
    /// # use zeokeel_cordis::{cleanup_async, Context};
    /// # fn demo(ctx: &Context) -> zeokeel_cordis::Result<()> {
    /// let effect = ctx.effect_future(
    ///     || async { Ok(vec![cleanup_async(|| async {})]) },
    ///     "ctx.effect_future(connect)",
    /// )?;
    /// # let _ = effect;
    /// # Ok(())
    /// # }
    /// ```
    pub fn effect_future<F, Fut>(&self, body: F, label: &'static str) -> Result<AsyncDisposer>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<AsyncEffect>> + Send + 'static,
    {
        let node = self.begin_async_effect(label)?;
        let cleanups = crate::block_on(body());
        self.end_async_effect();
        match cleanups {
            Ok(cleanups) => self.finish_async_effect(node, cleanups),
            Err(error) => Err(error),
        }
    }

    /// Awaiting variant of [`Context::effect_future`]: the body is awaited by the
    /// caller instead of being driven through [`crate::block_on`].
    pub async fn effect_future_async<F, Fut>(
        &self,
        body: F,
        label: &'static str,
    ) -> Result<AsyncDisposer>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<AsyncEffect>>,
    {
        let node = self.begin_async_effect(label)?;
        let cleanups = body().await;
        self.end_async_effect();
        match cleanups {
            Ok(cleanups) => self.finish_async_effect(node, cleanups),
            Err(error) => Err(error),
        }
    }

    /// Register one async cleanup per stream item (Cordis
    /// `ctx.effect(async function* () { … yield cleanup })`).
    ///
    /// The stream is drained during registration; yielded cleanups run in reverse
    /// order when the effect is disposed or the owning fiber unloads.
    pub fn effect_stream<S>(&self, stream: S, label: &'static str) -> Result<AsyncDisposer>
    where
        S: futures_util::Stream<Item = AsyncCleanup> + Send + 'static,
    {
        let node = self.begin_async_effect(label)?;
        let cleanups = crate::block_on(async move {
            let mut stream = Box::pin(stream);
            let mut cleanups = Vec::new();
            while let Some(cleanup) = futures_util::StreamExt::next(&mut stream).await {
                cleanups.push(cleanup);
            }
            cleanups
        });
        self.end_async_effect();
        self.finish_async_effect(node, cleanups)
    }

    fn begin_async_effect(&self, label: &'static str) -> Result<Arc<Mutex<EffectNode>>> {
        let node = EffectNode::new(label);
        self.attach_effect_node(&node)?;
        let mut fiber = self
            .inner
            .fiber
            .data
            .lock()
            .map_err(|_| CordisError::Disposed)?;
        if fiber.state == FiberState::Disposed || fiber.state == FiberState::Unloading {
            return Err(CordisError::InactiveEffect);
        }
        fiber.effect_stack.push(Arc::clone(&node));
        Ok(node)
    }

    fn end_async_effect(&self) {
        if let Ok(mut fiber) = self.inner.fiber.data.lock() {
            fiber.effect_stack.pop();
        }
    }

    fn finish_async_effect(
        &self,
        node: Arc<Mutex<EffectNode>>,
        cleanups: AsyncEffect,
    ) -> Result<AsyncDisposer> {
        let disposer = AsyncDisposer::new(cleanups);
        self.push_effect(FiberEffect {
            kind: EffectKind::Async(disposer.clone()),
            meta: node,
            activation: 0,
        })?;
        Ok(disposer)
    }

    /// Register a sync waterfall middleware owned by the current fiber.
    ///
    /// The middleware receives the explicit continuation; calling
    /// `next.next()` continues the chain, returning without it vetoes.
    pub fn on_waterfall<F>(&self, name: &'static str, listener: F) -> Result<Disposer>
    where
        F: Fn(&[Value], &WaterfallNext) -> Value + Send + Sync + 'static,
    {
        let owner = self.fiber_id();
        let callback: WaterfallListenerFn = Arc::new(listener);
        let disposer = Events::add_waterfall_listener(
            &self.inner.events,
            name,
            owner,
            EventOptions::default(),
            self.inner.isolate.clone(),
            callback,
        );
        self.register_listener_effect(disposer, "ctx.on_waterfall")
    }

    /// Register an async waterfall middleware owned by the current fiber.
    ///
    /// The middleware receives an owned continuation; `next.next().await`
    /// continues the chain, returning without awaiting it vetoes.
    pub fn on_waterfall_async<F>(&self, name: &'static str, listener: F) -> Result<Disposer>
    where
        F: Fn(&[Value], AsyncWaterfallNext) -> BoxFuture<'static, Value> + Send + Sync + 'static,
    {
        let owner = self.fiber_id();
        let callback: AsyncWaterfallListenerFn = Arc::new(listener);
        let disposer = Events::add_async_waterfall_listener(
            &self.inner.events,
            name,
            owner,
            EventOptions::default(),
            self.inner.isolate.clone(),
            callback,
        );
        self.register_listener_effect(disposer, "ctx.on_waterfall_async")
    }

    pub fn emit(&self, name: &str, args: &[Value]) {
        self.inner
            .events
            .emit(name, args, Some(&self.inner.isolate));
    }

    pub fn bail(&self, name: &str, args: &[Value]) -> Option<Value> {
        self.inner
            .events
            .bail(name, args, Some(&self.inner.isolate))
    }

    /// Await all listeners concurrently, sync and async (Cordis `parallel`).
    pub async fn parallel_async(&self, name: &str, args: &[Value]) {
        let events = Arc::clone(&self.inner.events);
        let scope: ContextScope = self.inner.isolate.clone();
        events.parallel_async(name, args, Some(&scope)).await;
    }

    /// Await listeners in registration order until one bails (Cordis `serial`).
    pub async fn serial_async(&self, name: &str, args: &[Value]) -> Option<Value> {
        let events = Arc::clone(&self.inner.events);
        let scope: ContextScope = self.inner.isolate.clone();
        events.serial_async(name, args, Some(&scope)).await
    }

    /// Compose the sync waterfall chain for `name` (see [`Events::waterfall`]).
    pub fn waterfall(
        &self,
        name: &str,
        args: &[Value],
        terminal: Option<WaterfallTerminal>,
    ) -> Value {
        self.inner
            .events
            .waterfall(name, args, terminal, Some(&self.inner.isolate))
    }

    /// Compose and await the async waterfall chain for `name`.
    pub async fn waterfall_async(
        &self,
        name: &str,
        args: &[Value],
        terminal: Option<AsyncWaterfallTerminal>,
    ) -> Value {
        let events = Arc::clone(&self.inner.events);
        let scope: ContextScope = self.inner.isolate.clone();
        events
            .waterfall_async(name, args, terminal, Some(&scope))
            .await
    }

    pub(crate) fn refresh_dependents(&self) {
        self.inner.registry.refresh_all();
    }

    /// Merge intercept configs from root to leaf, with base/head overrides.
    ///
    /// Cordis `Service.resolveConfig`: when a schema is registered for `name`
    /// (through [`crate::service::provide_service`]) its
    /// [`ConfigSchema::merge`] is used, otherwise configs are shallow-merged as
    /// plain objects.
    pub fn resolve_config(&self, name: &str, base: Option<Value>, head: Option<Value>) -> Value {
        let mut intercepts = Vec::new();
        let mut cursor = Some(self.clone());
        while let Some(ctx) = cursor {
            if let Some(config) = ctx.inner.intercepts.get(name) {
                intercepts.push(config.clone());
            }
            cursor = ctx.inner.parent.clone();
        }
        intercepts.reverse();
        if let Some(head) = head {
            intercepts.push(head);
        }
        if let Some(schema) = self.find_schema(name) {
            // A panicking embedder schema must not unwind out of config
            // resolution (CORE-042); it degrades to the base layer.
            return crate::config::contained_merge(
                schema.as_ref(),
                base.unwrap_or(Value::Null),
                &intercepts,
            );
        }
        let mut result = Value::Object(Default::default());
        if let Some(base) = base {
            merge_into(&mut result, base);
        }
        for config in intercepts {
            merge_into(&mut result, config);
        }
        result
    }

    /// The nearest config schema registered for `name` (child scopes shadow
    /// ancestors), if any.
    pub fn find_schema(&self, name: &str) -> Option<Arc<dyn ConfigSchema>> {
        let scope = self.service_scope(name).unwrap_or(0);
        self.inner
            .service_store
            .lock()
            .ok()?
            .get(&(scope, name.to_string()))
            .and_then(|entry| entry.schema.clone())
    }
}

// ---------------------------------------------------------------------------
// Logging API (Cordis `ctx.logger` / `ctx.logger(name)`)
// ---------------------------------------------------------------------------

impl Context {
    /// The display name of the plugin fiber owning this context (`root` for the
    /// root fiber).
    pub fn plugin_name(&self) -> String {
        self.inner.fiber.name()
    }

    /// Cordis `ctx.logger()`: a facade named after the current fiber/plugin.
    ///
    /// `ctx.intercept("logger", ...)` can override the name and verbosity
    /// through `{ "name": "...", "level": <0..3> }`, matching the official
    /// `LoggerService.Intercept` config.
    pub fn logger(&self) -> Logger {
        let config = self.resolve_config("logger", None, None);
        let name = config
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| self.plugin_name());
        self.inner.logger.logger_with(name, config_level(&config))
    }

    /// Cordis `ctx.logger(name)`: a named logger facade.
    pub fn logger_named(&self, name: &str) -> Logger {
        let config = self.resolve_config("logger", None, None);
        self.inner
            .logger
            .logger_with(name.to_string(), config_level(&config))
    }

    /// The shared logging service (`ctx.get(LOGGER)` also resolves it).
    pub fn logger_service(&self) -> Arc<LoggerService> {
        self.inner.logger.clone()
    }

    /// Register a log exporter owned by the current fiber; unloading the fiber
    /// unregisters it.
    pub fn log_exporter(&self, exporter: Arc<dyn LogExporter>) -> Result<Disposer> {
        let disposer = self.inner.logger.exporter(exporter);
        self.register_effect_labeled(disposer.clone(), "ctx.log_exporter")?;
        Ok(disposer)
    }
}

fn config_level(config: &Value) -> Option<LogLevel> {
    let level = config.get("level")?;
    if let Some(number) = level.as_u64() {
        return Some(LogLevel::from_u8(number.min(u8::MAX as u64) as u8));
    }
    level.as_str().and_then(LogLevel::parse)
}

fn merge_into(target: &mut Value, source: Value) {
    if let (Value::Object(target), Value::Object(source)) = (target, source) {
        for (key, value) in source {
            target.insert(key, value);
        }
    }
}

// ---------------------------------------------------------------------------
// Untyped access + `internal/set` (Cordis `ctx.get(name)` / `ctx.set`)
// ---------------------------------------------------------------------------

impl Context {
    /// Untyped service lookup (Cordis `ctx.get(name)`); dispatches the
    /// `internal/get` waterfall.
    pub fn get_named(&self, name: &str) -> Option<ServiceValue> {
        if let Some(accessor) = self.find_accessor(name) {
            return self.accessor_value(&accessor);
        }
        let internal = self.inner.events.internal().clone();
        internal.waterfall_get(self, name, &|| self.lookup(name).map(|entry| entry.value))
    }

    /// Overwrite a provided service value (Cordis `ctx.set(name, value)`).
    ///
    /// Only the fiber that provided the service may set it; setting an
    /// unprovided or foreign-owned service is an error. Dispatches the typed
    /// `internal/set` waterfall around the write; a hook that skips `next`
    /// vetoes the write and the call returns `Ok(())` without changing state
    /// (Cordis waterfall semantics).
    pub fn set(&self, name: &'static str, value: ServiceValue) -> Result<()> {
        if let Some(accessor) = self.find_accessor(name) {
            return match accessor.set {
                Some(set) => {
                    let allowed =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| set(self, value)))
                            .unwrap_or(false);
                    if allowed {
                        Ok(())
                    } else {
                        Err(CordisError::ServiceWriteForbidden(name.to_string()))
                    }
                }
                None => Err(CordisError::AccessorReadOnly(name.to_string())),
            };
        }
        self.set_revertible(name, value).map(|_| ())
    }

    /// Overwrite a service value as a revertible, classified mutation.
    ///
    /// The previous value is captured and restored by an effect owned by the
    /// providing fiber (reverse order on unload), and the entry version bump
    /// re-evaluates dependents (Cordis `ctx.set` + epoch semantics).
    pub fn set_revertible(&self, name: &'static str, value: ServiceValue) -> Result<Disposer> {
        self.check_set_target(name)?;
        // A set during unload cannot publish its inverse: reject before the
        // write so the store is not mutated without an inverse.
        let state = self.inner.fiber.state();
        if matches!(state, FiberState::Unloading | FiberState::Disposed) {
            return Err(CordisError::InactiveEffect);
        }
        let scope = self.service_scope(name).unwrap_or(0);
        let previous = self
            .inner
            .service_store
            .lock()
            .map_err(|_| CordisError::Disposed)?
            .get(&(scope, name.to_string()))
            .cloned();
        let written = self
            .inner
            .events
            .internal()
            .waterfall_set(self, name, &value, &|| {
                self.write_service(name, &value).is_ok()
            });
        if !written {
            // A hook vetoed the write: nothing was mutated, no inverse needed.
            return Ok(Disposer::new(Vec::new()));
        }
        let ctx = self.clone();
        let owner_fiber = self.inner.fiber.data.clone();
        let disposer = Disposer::new(vec![Box::new(move || {
            let key = (scope, name.to_string());
            if let Ok(mut store) = ctx.inner.service_store.lock() {
                match previous.clone() {
                    Some(mut entry) => {
                        // While the providing fiber is unloading, the restored
                        // entry must stay withdrawn so dependents cannot
                        // reactivate against a binding that is being released.
                        let unloading = owner_fiber
                            .lock()
                            .map(|fiber| {
                                matches!(fiber.state, FiberState::Unloading | FiberState::Disposed)
                            })
                            .unwrap_or(true);
                        if unloading {
                            entry.withdrawn = true;
                        }
                        store.insert(key, entry);
                    }
                    None => {
                        store.remove(&key);
                    }
                }
            }
            ctx.refresh_dependents();
        })]);
        self.register_effect_labeled(disposer.clone(), "ctx.set")?;
        self.refresh_dependents();
        Ok(disposer)
    }

    /// Mark every service provided by `owner` as withdrawn so dependents
    /// deactivate before the binding disappears (provider-withdrawal order).
    pub(crate) fn withdraw_owner_services(&self, owner: u64) {
        if let Ok(mut store) = self.inner.service_store.lock() {
            for entry in store.values_mut() {
                if entry.owner == owner {
                    entry.withdrawn = true;
                }
            }
        }
    }

    fn check_set_target(&self, name: &str) -> Result<()> {
        let scope = self.service_scope(name).unwrap_or(0);
        let store = self
            .inner
            .service_store
            .lock()
            .map_err(|_| CordisError::Disposed)?;
        match store.get(&(scope, name.to_string())) {
            None => Err(CordisError::UnknownService(name.to_string())),
            Some(entry) if entry.owner != self.fiber_id() => {
                Err(CordisError::ServiceWriteForbidden(name.to_string()))
            }
            Some(_) => Ok(()),
        }
    }

    fn write_service(&self, name: &str, value: &ServiceValue) -> Result<()> {
        let scope = self.service_scope(name).unwrap_or(0);
        let mut store = self
            .inner
            .service_store
            .lock()
            .map_err(|_| CordisError::Disposed)?;
        match store.get_mut(&(scope, name.to_string())) {
            None => Err(CordisError::UnknownService(name.to_string())),
            Some(entry) => {
                entry.value = value.clone();
                entry.version = crate::service::next_version();
                Ok(())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal-event registration (fiber-owned)
// ---------------------------------------------------------------------------

impl Context {
    /// Register an `internal/plugin` hook owned by the current fiber.
    pub fn on_internal_plugin(&self, hook: PluginHook) -> Result<Disposer> {
        let disposer = self.inner.events.internal().on_plugin(hook);
        self.register_effect_labeled(disposer.clone(), "ctx.on_internal_plugin")?;
        Ok(disposer)
    }

    /// Register an `internal/status` hook owned by the current fiber.
    pub fn on_internal_status(&self, hook: StatusHook) -> Result<Disposer> {
        let disposer = self.inner.events.internal().on_status(hook);
        self.register_effect_labeled(disposer.clone(), "ctx.on_internal_status")?;
        Ok(disposer)
    }

    /// Register an `internal/service` hook owned by the current fiber.
    pub fn on_internal_service(&self, hook: ServiceHook) -> Result<Disposer> {
        let disposer = self.inner.events.internal().on_service(hook);
        self.register_effect_labeled(disposer.clone(), "ctx.on_internal_service")?;
        Ok(disposer)
    }

    /// Register an `internal/config` waterfall hook owned by the current fiber.
    pub fn on_internal_config(&self, hook: ConfigHook) -> Result<Disposer> {
        let disposer = self.inner.events.internal().on_config(hook);
        self.register_effect_labeled(disposer.clone(), "ctx.on_internal_config")?;
        Ok(disposer)
    }

    /// Register an `internal/update` waterfall hook owned by the current fiber.
    pub fn on_internal_update(&self, hook: UpdateHook) -> Result<Disposer> {
        let disposer = self.inner.events.internal().on_update(hook);
        self.register_effect_labeled(disposer.clone(), "ctx.on_internal_update")?;
        Ok(disposer)
    }

    /// Register an `internal/get` waterfall hook owned by the current fiber.
    pub fn on_internal_get(&self, hook: GetHook) -> Result<Disposer> {
        let disposer = self.inner.events.internal().on_get(hook);
        self.register_effect_labeled(disposer.clone(), "ctx.on_internal_get")?;
        Ok(disposer)
    }

    /// Register an `internal/set` waterfall hook owned by the current fiber.
    pub fn on_internal_set(&self, hook: SetHook) -> Result<Disposer> {
        let disposer = self.inner.events.internal().on_set(hook);
        self.register_effect_labeled(disposer.clone(), "ctx.on_internal_set")?;
        Ok(disposer)
    }

    /// Register an `internal/listener` bail hook owned by the current fiber.
    pub fn on_internal_listener(&self, hook: ListenerHook) -> Result<Disposer> {
        let disposer = self.inner.events.internal().on_listener(hook);
        self.register_effect_labeled(disposer.clone(), "ctx.on_internal_listener")?;
        Ok(disposer)
    }

    /// Register an `internal/dispatch` observer owned by the current fiber.
    pub fn on_internal_dispatch(&self, hook: DispatchHook) -> Result<Disposer> {
        let disposer = self.inner.events.internal().on_dispatch(hook);
        self.register_effect_labeled(disposer.clone(), "ctx.on_internal_dispatch")?;
        Ok(disposer)
    }
}

#[cfg(test)]
mod link_cycle_tests {
    use super::*;

    /// CORE-031: dropping the root context must free its fiber graph. Before
    /// v0.2.0 the root fiber stored strong `Context` handles in its own
    /// `FiberData`, so every `Context::root()` leaked its whole graph.
    #[test]
    fn root_context_graph_has_no_strong_cycle() {
        let root = Context::root();
        let weak = Arc::downgrade(&root.inner);
        let fiber = root.fiber();
        drop(fiber);
        drop(root);
        assert!(
            weak.upgrade().is_none(),
            "root context/fiber graph leaked (strong reference cycle)"
        );
    }
}
