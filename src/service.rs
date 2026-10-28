//! Service keys, availability gates and the entries stored by a context.

use std::any::{Any, TypeId};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use serde_json::Value;

use crate::config::ConfigSchema;
use crate::context::{Context, ContextInner};
use crate::error::Result;
use crate::fiber::{FiberData, FiberState};

pub type ServiceValue = Arc<dyn Any + Send + Sync>;

/// Monotonic service-entry version.
///
/// Dependents record the version of every injected service they activated
/// against; a changed version (provider swap, shadowing provider change,
/// `ctx.set`) re-evaluates them even when the service stays *available*
/// (Cordis epoch semantics).
static SERVICE_VERSION: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_version() -> u64 {
    SERVICE_VERSION.fetch_add(1, Ordering::SeqCst)
}

/// Sized wrapper that lets a `dyn Trait` (`Arc<T: ?Sized>`) be stored in the
/// type-erased service store and recovered by `TypeId`.
///
/// `Arc<dyn Trait>` cannot unsize-coerce to `Arc<dyn Any>`, so the typed
/// `*_any` APIs wrap the `Arc` in this concrete holder instead.
pub(crate) struct ArcWrap<T: ?Sized + Send + Sync + 'static>(pub(crate) Arc<T>);

/// Wrap an `Arc<T>` (possibly unsized) into a [`ServiceValue`].
pub(crate) fn wrap_service<T: ?Sized + Send + Sync + 'static>(value: Arc<T>) -> ServiceValue {
    Arc::new(ArcWrap(value))
}

/// Recover an `Arc<T>` (possibly unsized) from a [`ServiceValue`] produced by
/// [`wrap_service`].
pub(crate) fn unwrap_service<T: ?Sized + Send + Sync + 'static>(
    value: &ServiceValue,
) -> Option<Arc<T>> {
    (**value)
        .downcast_ref::<ArcWrap<T>>()
        .map(|wrapped| Arc::clone(&wrapped.0))
}

/// The concrete `TypeId` stored inside a [`ServiceValue`].
///
/// This is `TypeId::of::<T>()` for a typed provide and
/// `TypeId::of::<ArcWrap<T>>()` for a `*_any` provide: exactly the identity
/// `ctx.set` compares against (FIX-13). The explicit `&dyn Any` coercion makes
/// the dynamic `Any::type_id` dispatch unambiguous (calling `.type_id()` on the
/// `Arc` itself would report the `Arc`'s own type instead of the stored one).
pub(crate) fn service_type_id(value: &ServiceValue) -> TypeId {
    (&**value as &dyn Any).type_id()
}

/// Availability predicate for a provided service (Cordis `Service.check`).
///
/// Dependents (fibers whose `inject` names this service) only see the service
/// while the predicate returns `true`; lookups through [`Context::get`] /
/// [`Context::require`] honor it as well. A panicking predicate is treated as
/// `false`, so a broken check can never break dispatch.
///
/// The predicate is invoked **outside** the service store lock, so it may
/// re-enter the context (read other services, log, …) like the official
/// Cordis checker does; holding the store mutex across user code would
/// self-deadlock.
pub type AvailabilityCheck = Arc<dyn Fn() -> bool + Send + Sync>;

#[derive(Clone)]
pub(crate) struct ServiceEntry {
    pub(crate) value: ServiceValue,
    pub(crate) check: Option<AvailabilityCheck>,
    pub(crate) schema: Option<Arc<dyn ConfigSchema>>,
    /// Fiber id that provided the service (Cordis `ctx.set` ownership rule).
    pub(crate) owner: u64,
    /// Concrete `TypeId` of [`ServiceEntry::value`]: `T` for typed provides and
    /// `ArcWrap<T>` for the `*_any` provides. `ctx.set` rejects values of a
    /// different type (DESIGN I5), so a service can never be silently replaced
    /// by an unrelated value.
    pub(crate) type_id: TypeId,
    /// Fingerprint of the providing fiber, used by the injection gate.
    ///
    /// The gate requires the provider to be [`FiberState::Active`] so dependents
    /// never activate against a provider that is still loading or already
    /// failed/unloading (FIX-12). `Weak` keeps the registry lock-free and never
    /// resurrects a fiber; `None` and already-dropped weaks count as available
    /// so hand-built entries (root-only stores, migration paths) keep working.
    pub(crate) provider: Option<Weak<Mutex<FiberData>>>,
    /// Entry version; dependents reactivate when this changes.
    pub(crate) version: u64,
    /// Set while the providing fiber is unloading: dependents deactivate
    /// before the binding is actually removed (provider-withdrawal ordering).
    pub(crate) withdrawn: bool,
}

impl ServiceEntry {
    pub(crate) fn is_available(&self) -> bool {
        if self.withdrawn {
            return false;
        }
        match &self.check {
            None => true,
            Some(check) => catch_unwind(AssertUnwindSafe(|| check())).unwrap_or(false),
        }
    }

    /// Strict availability predicate for dependency/injection gates (FIX-12).
    ///
    /// On top of [`ServiceEntry::is_available`] the providing fiber must be
    /// `Active`; the TS reference keeps a dependent `Pending` while its
    /// provider loads, so a provider that later fails never lets the dependent
    /// `apply` run at all. Reads through [`Context::get`] stay non-strict (see
    /// [`ServiceEntry::is_available`]).
    pub(crate) fn is_available_for_injection(&self) -> bool {
        self.is_available() && self.provider_is_active()
    }

    fn provider_is_active(&self) -> bool {
        let Some(provider) = &self.provider else {
            return true;
        };
        match provider.upgrade() {
            // The providing fiber was dropped without withdrawing the entry
            // (leaked/detached store): stay backward compatible and usable.
            None => true,
            Some(data) => match data.lock() {
                Ok(fiber) => fiber.state == FiberState::Active,
                // A poisoned lock is *not* evidence that the provider went
                // inactive: a contained panic while some kernel path held the
                // lock (e.g. a panicking `Plugin::inject()` during
                // `replace_plugin`) would otherwise turn one contained panic
                // into a permanent, silent service outage (S-3 / audit B-3).
                // Fail open here, like a dropped provider (`None` => true).
                Err(_) => true,
            },
        }
    }
}

/// A typed handle to a service name. The name is what plugins inject; the
/// `TypeId` makes `ctx.get(K)` type-safe.
///
/// `T` may be unsized (`ServiceKey<dyn Trait>`); trait-object services are
/// registered and read through the `*_any` methods of [`Context`], which store
/// the value behind a type-erased wrapper (Cordis `ServiceKey<?Sized>`).
pub struct ServiceKey<T: ?Sized + Send + Sync + 'static> {
    name: &'static str,
    _marker: std::marker::PhantomData<fn(&T)>,
}

impl<T: ?Sized + Send + Sync + 'static> ServiceKey<T> {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            _marker: std::marker::PhantomData,
        }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }
}

impl<T: ?Sized + Send + Sync + 'static> Clone for ServiceKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: ?Sized + Send + Sync + 'static> Copy for ServiceKey<T> {}

/// A shared availability toggle, the Rust mapping of Cordis `Service.check`.
///
/// `provide_gated` registers the gate as the service's [`AvailabilityCheck`]
/// and remembers the providing context; flipping the gate re-evaluates that
/// context's dependent fibers, so plugins unload while the gate is closed and
/// activate again when it reopens.
///
/// ```
/// # use std::sync::Arc;
/// # use zeokeel_cordis::{Context, ServiceGate, ServiceKey};
/// static READY: ServiceKey<String> = ServiceKey::new("ready");
/// let root = Context::root();
/// let gate = ServiceGate::new(false);
/// root.provide_gated(READY, Arc::new("value".to_string()), &gate).unwrap();
/// assert!(root.get(READY).is_none());
/// gate.set(true);
/// assert_eq!(root.get(READY).unwrap().as_str(), "value");
/// ```
#[derive(Clone, Default)]
pub struct ServiceGate {
    available: Arc<AtomicBool>,
    watchers: Arc<Mutex<Vec<Weak<ContextInner>>>>,
}

impl ServiceGate {
    /// Create a gate with an initial availability.
    pub fn new(available: bool) -> Self {
        let gate = Self::default();
        gate.available.store(available, Ordering::SeqCst);
        gate
    }

    pub fn is_available(&self) -> bool {
        self.available.load(Ordering::SeqCst)
    }

    /// Flip availability. Returns `true` when the value changed, after
    /// re-evaluating every context that provided a service through this gate.
    pub fn set(&self, available: bool) -> bool {
        if self.available.swap(available, Ordering::SeqCst) == available {
            return false;
        }
        self.notify();
        true
    }

    /// The predicate to hand to [`Context::provide_checked`].
    pub fn check(&self) -> AvailabilityCheck {
        let gate = self.clone();
        Arc::new(move || gate.is_available())
    }

    pub(crate) fn watch(&self, ctx: &Context) {
        if let Ok(mut watchers) = self.watchers.lock() {
            watchers.retain(|watcher| watcher.strong_count() > 0);
            watchers.push(Arc::downgrade(&ctx.inner));
        }
    }

    fn notify(&self) {
        let contexts: Vec<Arc<ContextInner>> = match self.watchers.lock() {
            Ok(mut watchers) => {
                watchers.retain(|watcher| watcher.strong_count() > 0);
                watchers.iter().filter_map(Weak::upgrade).collect()
            }
            Err(_) => return,
        };
        for inner in contexts {
            Context { inner }.refresh();
        }
    }
}

// ---------------------------------------------------------------------------
// The Service trait (Cordis `class X extends Service`)
// ---------------------------------------------------------------------------

/// A service object with lifecycle, availability and config semantics.
///
/// Official Cordis services are classes that call `super(ctx, name)` and are
/// registered by their constructor. Rust has no inheritance, so a service is a
/// plain value implementing this trait and is registered explicitly:
///
/// ```no_run
/// use std::sync::Arc;
///
/// use zeokeel_cordis::{Context, Disposer, Service};
///
/// struct Greeter;
///
/// impl Service for Greeter {
///     fn name(&self) -> &'static str {
///         "greeter"
///     }
/// }
///
/// fn start(ctx: &Context) -> zeokeel_cordis::Result<Disposer> {
///     zeokeel_cordis::service::provide_service(ctx, Arc::new(Greeter))
/// }
/// ```
///
/// The same instance is also available through [`Context::get`] with the
/// matching [`ServiceKey`]; the schema (if any) is consulted by
/// [`Context::resolve_config`] so intercept layers use
/// [`ConfigSchema::merge`] instead of a shallow object merge.
pub trait Service: Send + Sync + 'static {
    /// The service name (registration key).
    fn name(&self) -> &'static str;

    /// Cordis `Service.check`: availability predicate for dependents.
    fn check(&self) -> Option<AvailabilityCheck> {
        None
    }

    /// Optional config schema (Cordis `Service.Config`).
    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>> {
        None
    }

    /// Cordis `Service.resolveConfig`: base + intercepts + head.
    ///
    /// Delegates to [`Context::resolve_config`], which uses this service's
    /// schema when it declares one.
    fn resolve_config(&self, ctx: &Context, base: Option<Value>, head: Option<Value>) -> Value {
        ctx.resolve_config(self.name(), base, head)
    }
}

/// Register a [`Service`] instance in `ctx`; the returned disposer removes it
/// and the registration is owned by the current fiber (so it unloads with the
/// plugin that provided it).
///
/// Mirrors Cordis' `new X(ctx, name)` constructor side effect. Returns
/// [`crate::CordisError::DuplicateService`] when the name is already provided
/// in the same isolation scope.
pub fn provide_service<S: Service>(
    ctx: &Context,
    service: Arc<S>,
) -> Result<crate::effect::Disposer> {
    let name = service.name();
    let check = service.check();
    let schema = service.config_schema();
    let value: ServiceValue = service;
    ctx.provide_service_value(name, value, check, schema)
}

impl Context {
    /// Convenience wrapper around [`provide_service`].
    pub fn provide_service<S: Service>(&self, service: Arc<S>) -> Result<crate::effect::Disposer> {
        provide_service(self, service)
    }
}

#[cfg(test)]
mod provider_gate_tests {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::Arc;

    use super::*;
    use crate::context::Context;

    /// S-3 (audit B-3): a *poisoned* provider lock is not evidence that the
    /// provider went inactive — it must not make the service permanently
    /// non-injectable. C2 now contains the embedder metadata panics that used to
    /// poison the lock, so the state is constructed directly here to keep the
    /// defensive `Err(_) => true` fallback covered.
    #[test]
    fn poisoned_provider_lock_counts_as_active() {
        let root = Context::root();
        let data = root.fiber().data.clone();
        // Poison the provider fiber's lock: panic while holding it.
        let poisoned = catch_unwind(AssertUnwindSafe(|| {
            let _guard = data.lock().expect("lock the provider fiber");
            panic!("poison the provider lock");
        }));
        assert!(poisoned.is_err(), "the lock must be poisoned");

        let entry = ServiceEntry {
            value: Arc::new("value".to_string()) as ServiceValue,
            check: None,
            schema: None,
            owner: 0,
            type_id: std::any::TypeId::of::<String>(),
            provider: Some(Arc::downgrade(&data)),
            version: 1,
            withdrawn: false,
        };
        assert!(
            entry.provider_is_active(),
            "a poisoned provider lock must count as active (S-3)"
        );
        assert!(entry.is_available_for_injection());

        // Keep the process-wide (test-process) state clean for other tests.
        data.clear_poison();
        assert!(entry.is_available_for_injection());
    }
}
