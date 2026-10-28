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

/// Supervision listener-quota token: releases the reserved listener slot
/// exactly once, no matter which path releases it first (user disposer, `once`
/// auto-removal in the event bus, or fiber unload).
#[cfg(feature = "supervision")]
struct ListenerSlot {
    fiber: FiberRef,
    released: std::sync::atomic::AtomicBool,
}

#[cfg(feature = "supervision")]
impl ListenerSlot {
    fn release(&self) {
        if self
            .released
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        if let Ok(mut fiber) = self.fiber.lock() {
            fiber.listener_count = fiber.listener_count.saturating_sub(1);
        }
    }
}

/// Releases the listener slot when a `once` listener invocation ends (including
/// on a contained panic): the event bus removes the listener after that
/// invocation, so the user-facing disposer may never be disposed.
#[cfg(feature = "supervision")]
struct OnceListenerRelease(Arc<ListenerSlot>);

#[cfg(feature = "supervision")]
impl Drop for OnceListenerRelease {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// Zero-sized placeholder so listener registration reads the same with and
/// without the `supervision` feature (no quota bookkeeping in the core kernel).
#[cfg(not(feature = "supervision"))]
struct ListenerSlot;

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
            // The stored value's concrete type: `T` for typed provides and
            // `ArcWrap<T>` for `*_any` provides. `ctx.set` compares against it
            // so a service can never be silently replaced by another type
            // (DESIGN I5 / FIX-13).
            type_id: crate::service::service_type_id(&service),
            // Weak link to the providing fiber: injection gates require it to
            // be `Active` (FIX-12). Weak keeps the registry lock-free and can
            // never resurrect a fiber; entries without a registered provider
            // (root-only / detached stores) count as available.
            provider: Some(Arc::downgrade(&self.inner.fiber.data)),
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

    /// Read an accessor with panic containment: a broken accessor must not
    /// take down service lookup (panic-containment policy; see
    /// docs/CORDIS-RUST.md). [`Accessor::read`] logs the panic and reports
    /// `None`, so a panicking getter is indistinguishable from an absent
    /// property (never a placeholder value that only fails typed downcasts).
    fn accessor_value(&self, accessor: &Accessor) -> Option<ServiceValue> {
        accessor.read(self)
    }

    /// Typed lookup with child→root inheritance (respecting `isolate` and the
    /// provider's [`AvailabilityCheck`]).
    ///
    /// Dispatches the typed `internal/get` waterfall before reporting the
    /// service, so extensions can observe/override service reads (Cordis
    /// `internal/get`). Hooks that call `ctx.get()` again do not recurse.
    ///
    /// This is the *non-strict* read path (Cordis property-read semantics): a
    /// service provided by a fiber that is still loading is visible here, while
    /// [`Context::service_available`] and `inject` gates use the strict
    /// `Context::lookup_for_injection` query. Computed accessors
    /// ([`Context::accessor`]) are resolved before services and are only
    /// reachable through this read path, never through injection.
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
    /// Mirrors [`Context::set`]'s ownership and type rules: only the providing
    /// fiber may write the service, and the replacement must be the same
    /// concrete `Arc<T>` type ([`CordisError::ServiceTypeMismatch`]
    /// otherwise — the `*_any` lane stores `ArcWrap<T>` internally, so
    /// `set_any` and `provide_any` with the same `T` always agree).
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
                        // C5: the accessor's setter declined the write — this
                        // is a computed-property decision, not an ownership
                        // violation, so it gets its own error variant.
                        Err(CordisError::AccessorWriteRejected(name.to_string()))
                    }
                }
                None => Err(CordisError::AccessorReadOnly(name.to_string())),
            };
        }
        let wrapped = crate::service::wrap_service(value);
        self.set_revertible(name, wrapped).map(|_| ())
    }

    /// Whether `name` currently resolves to an implementation **usable for
    /// injection** from this context (diagnostics for gated services).
    ///
    /// Uses the strict injection query (`Context::lookup_for_injection`): the
    /// provider fiber must be `Active`. Computed accessors and `internal/get`
    /// virtual services are *not* services and never satisfy injection, so
    /// `ctx.get_named(name).is_some()` can be `true` while this returns
    /// `false` — that is the documented Cordis split (property read vs
    /// dependency resolution).
    pub fn service_available(&self, name: &str) -> bool {
        self.lookup_for_injection(name).is_some()
    }

    /// The stored service entry for `name` in this context's scope, ignoring
    /// availability (the raw registry cell; callers apply their own predicate).
    fn lookup_entry(&self, name: &str) -> Option<ServiceEntry> {
        // Resolve within the scope this context maps `name` to; an unavailable
        // impl (Service.check false) shadows the scope instead of falling back
        // to the default scope (same rule as Cordis).
        let store = self.inner.service_store.lock().ok()?;
        store
            .get(&(self.service_scope(name).unwrap_or(0), name.to_string()))
            .cloned()
    }

    /// Non-strict lookup: what [`Context::get`] / [`Context::get_named`] report.
    ///
    /// Honors `Service.check` and provider withdrawal but deliberately ignores
    /// the provider fiber's lifecycle state (Cordis property-read semantics: a
    /// dependent may observe a service while the providing plugin is still
    /// loading; only *injection* is gated on [`FiberState::Active`]).
    pub(crate) fn lookup(&self, name: &str) -> Option<ServiceEntry> {
        // Take the entry out of the store *before* running the availability
        // check: a check may itself read services or log (official Cordis
        // allows re-entrant checks), and holding the store mutex across user
        // code would self-deadlock.
        self.lookup_entry(name).filter(|entry| entry.is_available())
    }

    /// Strict lookup used by dependency/injection gates (FIX-12).
    ///
    /// Adds the provider-fiber gate on top of [`Context::lookup`]: a service
    /// whose providing fiber is not `Active` (still loading, failed, unloading,
    /// disposed) must not satisfy an `inject` dependency, so dependents are not
    /// activated against a provider that may never commit — matching the TS
    /// reference, where a dependent stays `Pending` while its provider loads.
    pub(crate) fn lookup_for_injection(&self, name: &str) -> Option<ServiceEntry> {
        self.lookup_entry(name)
            .filter(|entry| entry.is_available_for_injection())
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
    ///
    /// The wait is **recovery-aware** (0.4.3 A-9/F1/F5): fibers parked in
    /// `Unloading` by a cancelled async cascade are recovered while waiting, and
    /// parked frames that appear *during* the wait (another thread cancels) are
    /// picked up as well, so a cancellation can never block quiescence forever.
    /// The recovery section is bounded and RAII-latched, so cancelling this
    /// future leaves the next `when_idle()` able to recover.
    ///
    /// A caller that is itself inside a registration window (e.g. an
    /// `internal/plugin` create hook) does not wait for its **own**
    /// registration: the registry excludes the calling thread's in-flight
    /// registrations from the quiescence check, so `block_on(ctx.when_idle())`
    /// inside a create hook returns instead of deadlocking. External waiters
    /// still wait for that window, and [`crate::Registry::is_idle`] stays strict
    /// (`false` while a registration is pending).
    pub async fn when_idle(&self) {
        crate::fiber::when_idle_with_recovery(&self.inner.registry).await;
    }

    /// Re-evaluate every dependent fiber.
    ///
    /// Call this after an `AvailabilityCheck`'s backing state changed, or after
    /// an out-of-band service mutation that bypassed the effect lifecycle.
    ///
    /// The fixpoint is **bounded**: at most 64 rounds, each round advancing
    /// through a snapshot of the registry's fibers in registration order. A
    /// forward dependency chain (`A → B → C`) therefore stabilizes in one round,
    /// while a reverse chain (`C → B → A`) settles roughly one level per round —
    /// very deep dependency chains need multiple `refresh` calls to fully
    /// settle. Re-entrant calls made while a refresh is already running are
    /// coalesced through a dirty flag and drained by the outermost loop.
    pub fn refresh(&self) {
        self.refresh_dependents();
    }

    /// Async counterpart of [`Context::refresh`]: also awaits async plugin
    /// activation/unload, which a synchronous refresh cannot do.
    ///
    /// It uses the same **bounded fixed point** (at most 64 rounds over a
    /// registry snapshot in registration order: a forward dependency chain
    /// stabilizes in one round, a reverse chain roughly one level per round, so
    /// extremely deep chains need repeated refreshes). Re-entrant refreshes are
    /// coalesced through the dirty flag instead of nesting.
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
    ///
    /// The returned token releases the slot exactly once (audit R3B P2-1b): the
    /// old bookkeeping only decremented while the *fiber* unloaded, so an
    /// embedder that disposed the listener disposer kept the slot reserved
    /// forever.
    #[cfg(feature = "supervision")]
    fn reserve_listener_slot(&self) -> Result<Arc<ListenerSlot>> {
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
        drop(fiber);
        Ok(Arc::new(ListenerSlot {
            fiber: self.inner.fiber.data.clone(),
            released: std::sync::atomic::AtomicBool::new(false),
        }))
    }

    /// Wrap a sync listener so the event bus' own `once` auto-removal also frees
    /// its quota slot (in that path the user-facing disposer is never disposed).
    #[cfg(feature = "supervision")]
    fn wrap_once_listener(
        &self,
        callback: ListenerFn,
        once: bool,
        slot: &Arc<ListenerSlot>,
    ) -> ListenerFn {
        if !once {
            return callback;
        }
        let slot = Arc::clone(slot);
        Arc::new(move |args: &[Value]| {
            // Dropped when the invocation ends, including on a contained panic.
            let _release = OnceListenerRelease(Arc::clone(&slot));
            callback(args)
        })
    }

    /// Own a newly registered listener: enforce the listener quota (already
    /// reserved by [`Context::reserve_listener_slot`]), hand the embedder a
    /// disposer that immediately frees the slot **and** unregisters the
    /// listener, and attach that same single-shot disposer to the fiber so the
    /// unload path cannot double-decrement the count.
    #[cfg(feature = "supervision")]
    fn adopt_listener(
        &self,
        disposer: Disposer,
        slot: Arc<ListenerSlot>,
        label: &'static str,
    ) -> Result<Disposer> {
        let unregister = disposer;
        let counted = Disposer::new(vec![cleanup(move || {
            slot.release();
            unregister.dispose();
        })]);
        match self.register_effect_labeled(counted.clone(), label) {
            Ok(()) => Ok(counted),
            Err(error) => {
                // Undo the registration and the reservation in one shot.
                counted.dispose();
                Err(error)
            }
        }
    }

    /// Without the `supervision` feature no quota bookkeeping is performed: the
    /// token is a zero-sized placeholder and the event-bus disposer is returned
    /// as-is.
    #[cfg(not(feature = "supervision"))]
    fn reserve_listener_slot(&self) -> Result<Arc<ListenerSlot>> {
        Ok(Arc::new(ListenerSlot))
    }

    #[cfg(not(feature = "supervision"))]
    fn wrap_once_listener(
        &self,
        callback: ListenerFn,
        _once: bool,
        _slot: &Arc<ListenerSlot>,
    ) -> ListenerFn {
        callback
    }

    #[cfg(not(feature = "supervision"))]
    fn adopt_listener(
        &self,
        disposer: Disposer,
        _slot: Arc<ListenerSlot>,
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
        // Audit R3B P2-1a: a `Disposer`/`AsyncDisposer` the embedder already
        // disposed no longer owns resources, but its `FiberEffect` entry used to
        // stay in the list until the fiber unloaded, so repeated
        // register/release cycles eventually tripped `QuotaExceeded("effects")`.
        // Drop dead entries here: the quota then counts *live* effects only (and
        // the list cannot grow without bound on a long-lived fiber).
        fiber.effects.retain(Self::effect_is_live);
        #[cfg(feature = "supervision")]
        if let Some(max) = max_effects {
            if fiber.effects.len() >= max {
                return Err(CordisError::QuotaExceeded("effects".to_string()));
            }
        }
        let mut effect = effect;
        // Tag the effect with the **attempt** that registered it (FIX-5): under
        // concurrent restarts the fiber's `activation_epoch` may already belong
        // to a newer attempt, and tagging with it would make the stale
        // attempt's effects undiscardable. Outside an activation frame (effects
        // registered by the embedder after activation) fall back to the latest
        // epoch, which preserves the previous behavior.
        effect.activation = crate::fiber::current_activation_epoch(fiber.id).unwrap_or_else(|| {
            fiber
                .activation_epoch
                .load(std::sync::atomic::Ordering::SeqCst)
        });
        fiber.effects.push(effect);
        Ok(())
    }

    /// Whether an effect still owns resources.
    ///
    /// A disposed cleanup handle is inert (dispose is idempotent), so its entry
    /// no longer represents a live effect for quota accounting.
    fn effect_is_live(effect: &FiberEffect) -> bool {
        match &effect.kind {
            EffectKind::Sync(disposer) => !disposer.is_disposed(),
            EffectKind::Async(disposer) => !disposer.is_disposed(),
        }
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
        // C2: `Plugin::key()` is embedder code; a panicking key degrades to the
        // shared panicked-plugin runtime instead of poisoning kernel locks.
        let key = crate::registry::plugin_key_contained(plugin.as_ref());
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
        //
        // Count the **fiber** parent chain (Cordis `fiber.parent`), not the
        // context chain (FIX-19): `isolate`/`intercept`/`extend_meta` derive
        // child contexts without loading a plugin, so a deep scope chain must
        // not consume the loading budget of one shallow plugin load. Only a
        // fiber created by `ctx.plugin` adds a level.
        //
        // The root fiber's parent link is a *weak self-reference* (CORE-031),
        // so a naive walk never terminates: stop when the parent repeats the
        // current fiber id. The `MAX_PLUGIN_DEPTH` bound is a second, defensive
        // guarantee against a corrupted link.
        let mut depth = 0usize;
        let mut cursor = Some(self.inner.fiber.clone());
        while let Some(fiber) = cursor {
            depth += 1;
            if depth > crate::fiber::MAX_PLUGIN_DEPTH {
                break;
            }
            let fiber_id = fiber.id();
            cursor = fiber
                .parent()
                .map(|parent| parent.fiber())
                .filter(|parent| parent.id() != fiber_id);
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
            // Audit R3B P2-1c: `children` stores *weak* handles and a child that
            // disposed itself is never removed by it, so stale entries used to
            // consume `max_child_fibers` forever ("unload the old child plugin,
            // then load a new one" failed). Count only live children
            // (`Weak::upgrade()` and state != `Disposed`) and prune dead ones
            // while we hold the list.
            let live_children = self
                .inner
                .fiber
                .data
                .lock()
                .ok()
                .and_then(|parent| {
                    parent.children.lock().ok().map(|mut children| {
                        children.retain(|child| {
                            child.upgrade().is_some_and(|data| {
                                !matches!(
                                    data.lock().map(|fiber| fiber.state),
                                    Ok(FiberState::Disposed)
                                )
                            })
                        });
                        children.len()
                    })
                })
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
        // recovery path for busy children). R3-A4: registered through the
        // fibre module so the effect carries the child id; a parent can then
        // drop it when the child is disposed on its own (bounded bookkeeping
        // for dynamic-loading hosts).
        if let Err(error) = crate::fiber::register_child_ownership(self, &fiber) {
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
        // FIX-11b: the registry registration counter must span the whole
        // window — *before* `create_fiber_handle`, whose `internal/plugin` hook
        // runs before the fiber is registered/activated. `Registry::is_idle` /
        // `Context::when_idle` stay blocked until this guard drops.
        let _registration = self.inner.registry.begin_registration();
        let fiber = self.create_fiber_handle(PluginKind::Sync(plugin), config, key)?;
        // FIX-11: additionally mark the fiber's own registration → activation
        // window as an in-flight lifecycle operation, so `Fiber::is_idle` /
        // `await_settled` observe it. Dropped when this function returns.
        // (The guard borrows a clone of the `FiberRef`, so `fiber` stays
        // movable for the return value.)
        let data = fiber.data.clone();
        let _guard = crate::fiber::begin_operation(&data);
        if let Some((ctx, kind, config)) = Self::ready_activation(&fiber) {
            let _ = activate_fiber(&data, &ctx, &kind, &config);
        }
        Ok(fiber)
    }

    /// Load an **async plugin**. Activation is awaited here when its inject
    /// dependencies are already available; otherwise the fiber stays `Pending`
    /// until the host awaits [`Context::refresh_async`] (or calls
    /// [`Fiber::restart_async`]).
    pub async fn plugin_async(&self, plugin: Arc<dyn AsyncPlugin>, config: Value) -> Result<Fiber> {
        // C2: contained `AsyncPlugin::key()` (see `Context::plugin`).
        let key = crate::registry::async_plugin_key_contained(plugin.as_ref());
        // FIX-11b: cover the `internal/plugin` create hook too (see
        // `plugin_with_key`). `RegistrationGuard` is `Send`, so it may be held
        // across the activation await.
        let _registration = self.inner.registry.begin_registration();
        let fiber = self.create_fiber_handle(PluginKind::Async(plugin), config, key)?;
        // FIX-11: cover the whole registration → activation window (see
        // `plugin_with_key`).
        // (The guard borrows a clone of the `FiberRef`, so `fiber` stays
        // movable for the return value.)
        let data = fiber.data.clone();
        let _guard = crate::fiber::begin_operation(&data);
        if let Some((ctx, kind, config)) = Self::ready_activation(&fiber) {
            let _ = activate_fiber_async(&data, &ctx, &kind, &config).await;
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
        // FIX-11b: cover the `internal/plugin` create hook too (see
        // `plugin_with_key`).
        let _registration = self.inner.registry.begin_registration();
        let fiber = self.create_fiber_handle(PluginKind::Async(plugin), config, key)?;
        // FIX-11: cover the whole registration → activation window (see
        // `plugin_with_key`).
        // (The guard borrows a clone of the `FiberRef`, so `fiber` stays
        // movable for the return value.)
        let data = fiber.data.clone();
        let _guard = crate::fiber::begin_operation(&data);
        if let Some((ctx, kind, config)) = Self::ready_activation(&fiber) {
            let _ = activate_fiber_async(&data, &ctx, &kind, &config).await;
        }
        Ok(fiber)
    }

    /// Cordis `ctx.inject(deps, callback)` convenience: load a closure plugin.
    ///
    /// Each `inject` call gets its own registry identity keyed by the closure
    /// type (`PluginKey::Type(TypeId::of::<Injected<F>>())`, FIX-20): three
    /// `inject`s are three runtimes, and `Registry::replace(Named("injected"))`
    /// no longer matches them (the diagnostic name stays `"injected"`).
    /// Closures created at the same source location share one key, exactly like
    /// two instances of the same plugin type.
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
        let key = PluginKey::Type(std::any::TypeId::of::<Injected<F>>());
        self.plugin_with_key(Arc::new(Injected(callback, deps)), Value::Null, key)
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
        // Quota slot first: the `resolve_listener` hook may have forced `once`,
        // which decides whether the callback also has to release the slot by
        // itself (the event bus removes `once` listeners without touching our
        // disposer).
        let slot = self.reserve_listener_slot()?;
        let callback = self.wrap_once_listener(callback, options.once, &slot);
        let disposer = Events::add_listener(
            &self.inner.events,
            name,
            owner,
            options,
            self.inner.isolate.clone(),
            callback,
        );
        self.adopt_listener(disposer, slot, "ctx.on")
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
        let slot = self.reserve_listener_slot()?;
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
        self.adopt_listener(disposer, slot, "ctx.on_async")
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
        let slot = self.reserve_listener_slot()?;
        let disposer = Events::add_waterfall_listener(
            &self.inner.events,
            name,
            owner,
            EventOptions::default(),
            self.inner.isolate.clone(),
            callback,
        );
        self.adopt_listener(disposer, slot, "ctx.on_waterfall")
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
        let slot = self.reserve_listener_slot()?;
        let disposer = Events::add_async_waterfall_listener(
            &self.inner.events,
            name,
            owner,
            EventOptions::default(),
            self.inner.isolate.clone(),
            callback,
        );
        self.adopt_listener(disposer, slot, "ctx.on_waterfall_async")
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
        // Audit R3B P3-3: `LogLevel::from_u8` maps every code above `Debug` to
        // `Debug`, so a typo like `{"level": 99}` used to *widen* logging to the
        // most verbose level (fail-open for sensitive records). Clamp unknown
        // numeric levels to the safe end (`Error`) instead.
        return Some(if number <= 3 {
            LogLevel::from_u8(number as u8)
        } else {
            LogLevel::Error
        });
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
    ///
    /// Non-strict, like [`Context::get`]: accessors and `internal/get` virtual
    /// services are visible here but never satisfy an `inject` dependency.
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
    /// unprovided or foreign-owned service is an error. The replacement must
    /// have the same concrete type as the provided value
    /// ([`CordisError::ServiceTypeMismatch`] otherwise — DESIGN I5). Dispatches
    /// the typed `internal/set` waterfall around the write; a hook that skips
    /// `next` vetoes the write and the call returns `Ok(())` without changing
    /// state (Cordis waterfall semantics).
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
                        // C5: the accessor's setter declined the write — this
                        // is a computed-property decision, not an ownership
                        // violation, so it gets its own error variant.
                        Err(CordisError::AccessorWriteRejected(name.to_string()))
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
    ///
    /// The replacement is type-checked **before** dispatch
    /// ([`CordisError::ServiceTypeMismatch`] on mismatch), so a hook never
    /// observes an ill-typed write and the stored value can never change type.
    pub fn set_revertible(&self, name: &'static str, value: ServiceValue) -> Result<Disposer> {
        self.check_set_target(name)?;
        self.check_set_type(name, &value)?;
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

    /// Reject a `ctx.set`/`ctx.set_any` replacement whose concrete type differs
    /// from the one registered at provide time (FIX-13 / DESIGN I5).
    ///
    /// The stored type is the provided `T` for typed provides and
    /// `ArcWrap<T>` for the `*_any` family, which is exactly what
    /// [`crate::service::service_type_id`] reports for either lane, so the two
    /// APIs can share one check without extra metadata.
    fn check_set_type(&self, name: &str, value: &ServiceValue) -> Result<()> {
        let scope = self.service_scope(name).unwrap_or(0);
        let store = self
            .inner
            .service_store
            .lock()
            .map_err(|_| CordisError::Disposed)?;
        match store.get(&(scope, name.to_string())) {
            None => Err(CordisError::UnknownService(name.to_string())),
            Some(entry) => {
                let incoming = crate::service::service_type_id(value);
                if incoming == entry.type_id {
                    Ok(())
                } else {
                    Err(Self::service_type_mismatch(name, entry.type_id, incoming))
                }
            }
        }
    }

    /// The error returned when a `ctx.set` replacement has the wrong type.
    fn service_type_mismatch(
        name: &str,
        expected: std::any::TypeId,
        incoming: std::any::TypeId,
    ) -> CordisError {
        CordisError::ServiceTypeMismatch(format!(
            "`{name}`: provided {expected:?}, replacement {incoming:?}"
        ))
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
                // Defense in depth: `set_revertible` already type-checked before
                // dispatching `internal/set`, but the terminal must not be able
                // to mutate the entry into a different concrete type.
                let incoming = crate::service::service_type_id(value);
                if incoming != entry.type_id {
                    return Err(Self::service_type_mismatch(name, entry.type_id, incoming));
                }
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
