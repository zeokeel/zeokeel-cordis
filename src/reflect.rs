//! Reflection helpers: computed context properties and context metadata.
//!
//! Cordis implements these through the context Proxy (`ctx.accessor`,
//! `ctx.mixin`, `ctx.extend(meta)`, `ctx.reflect.*`). Rust has no dynamic
//! property proxy, so the port keeps the *semantics* with explicit APIs:
//!
//! * [`Context::accessor`] registers a computed property that [`Context::get_named`]
//!   resolves before services (Cordis `ctx.accessor`);
//! * [`Context::extend_meta`] creates a child context carrying metadata that does
//!   not participate in service resolution (Cordis `ctx.extend(meta)`);
//! * `ctx.mixin` is a **documented deliberate difference**: Rust exposes service
//!   methods directly (`ctx.events()`/`ctx.logger()`/…), so there is no dynamic
//!   forwarding table to mix in. See `docs/CORDIS-RUST.md`.
//!
//! `ctx.reflect.get/set/provide` map to [`Context::get_named`], [`Context::set`]
//! and [`crate::service::provide_service`].

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::context::Context;
use crate::effect::{cleanup, Disposer};
use crate::error::{CordisError, Result};
use crate::service::ServiceValue;

/// Getter of a computed context property.
pub type AccessorGet = Arc<dyn Fn(&Context) -> ServiceValue + Send + Sync>;
/// Setter of a computed context property; returning `false` rejects the write.
pub type AccessorSet = Arc<dyn Fn(&Context, ServiceValue) -> bool + Send + Sync>;

/// One declared computed context property.
#[derive(Clone)]
pub struct Accessor {
    /// Compute the property value for a context.
    pub get: AccessorGet,
    /// Optional setter (Cordis `Property.Accessor.set`).
    pub set: Option<AccessorSet>,
}

impl Accessor {
    /// A read-only accessor.
    pub fn new(get: AccessorGet) -> Self {
        Self { get, set: None }
    }

    /// A read/write accessor.
    pub fn with_setter(get: AccessorGet, set: AccessorSet) -> Self {
        Self {
            get,
            set: Some(set),
        }
    }

    /// Read the computed property, containing a panicking getter.
    ///
    /// CORE-041: a panicking accessor getter must not unwind out of
    /// `Context::get` / `get_named`; the read degrades to `None` ("value not
    /// available"), matching the absent-property contract instead of returning
    /// a placeholder that only fails typed downcasts.
    pub(crate) fn read(&self, ctx: &Context) -> Option<ServiceValue> {
        crate::internal::contained("zeokeel_cordis::reflect", "accessor get()", None, || {
            Some((self.get)(ctx))
        })
    }
}

/// Diagnostic/extension metadata attached to a context child
/// (Cordis `ctx.extend(meta)`). Entries never participate in service lookup.
#[derive(Clone, Default)]
pub struct ContextMeta {
    entries: HashMap<&'static str, Arc<dyn Any + Send + Sync>>,
}

impl ContextMeta {
    /// An empty metadata bag.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a typed entry.
    pub fn insert<T: Any + Send + Sync>(&mut self, key: &'static str, value: Arc<T>) {
        self.entries.insert(key, value);
    }

    /// Insert an already type-erased entry.
    pub fn insert_any(&mut self, key: &'static str, value: Arc<dyn Any + Send + Sync>) {
        self.entries.insert(key, value);
    }

    /// Look up a typed entry.
    pub fn get<T: Any + Send + Sync>(&self, key: &'static str) -> Option<Arc<T>> {
        self.entries
            .get(key)
            .and_then(|value| Arc::clone(value).downcast::<T>().ok())
    }

    /// Whether a key is present.
    pub fn contains(&self, key: &'static str) -> bool {
        self.entries.contains_key(key)
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no entry was attached.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl std::fmt::Debug for ContextMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextMeta")
            .field("keys", &self.entries.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Context {
    /// Define a read-only computed context property (Cordis `ctx.accessor`).
    ///
    /// The accessor is owned by the current fiber (removed on unload/restart)
    /// and resolved by [`Context::get_named`] before service lookups.
    pub fn accessor<Get>(&self, name: &'static str, get: Get) -> Result<Disposer>
    where
        Get: Fn(&Context) -> ServiceValue + Send + Sync + 'static,
    {
        self.register_accessor(name, Accessor::new(Arc::new(get)))
    }

    /// Define a computed context property with a setter (Cordis
    /// `ctx.accessor(name, { get, set })`).
    pub fn accessor_with_setter<Get, Set>(
        &self,
        name: &'static str,
        get: Get,
        set: Set,
    ) -> Result<Disposer>
    where
        Get: Fn(&Context) -> ServiceValue + Send + Sync + 'static,
        Set: Fn(&Context, ServiceValue) -> bool + Send + Sync + 'static,
    {
        self.register_accessor(name, Accessor::with_setter(Arc::new(get), Arc::new(set)))
    }

    fn register_accessor(&self, name: &'static str, accessor: Accessor) -> Result<Disposer> {
        {
            let mut accessors = self
                .inner
                .accessors
                .lock()
                .map_err(|_| CordisError::Disposed)?;
            if accessors.contains_key(name) {
                return Err(CordisError::InvalidPlugin(format!(
                    "property `{name}` is already declared as an accessor"
                )));
            }
            accessors.insert(name, accessor);
        }
        // Capture the context weakly: a strong `self.clone()` here recreates
        // the context<->fiber cycle (CORE-033), because the owning fiber's
        // effect list holds this disposer. When the context is already gone its
        // accessor map was dropped with it, so there is nothing left to remove.
        let weak = Arc::downgrade(&self.inner);
        let disposer = Disposer::new(vec![cleanup(move || {
            if let Some(inner) = weak.upgrade() {
                if let Ok(mut accessors) = inner.accessors.lock() {
                    accessors.remove(name);
                }
            }
        })]);
        self.register_effect_labeled(disposer.clone(), "ctx.accessor")?;
        Ok(disposer)
    }

    /// The nearest accessor for `name` (child scopes shadow ancestors).
    pub(crate) fn find_accessor(&self, name: &str) -> Option<Accessor> {
        let mut cursor = Some(self.clone());
        while let Some(ctx) = cursor {
            if let Ok(accessors) = ctx.inner.accessors.lock() {
                if let Some(accessor) = accessors.get(name) {
                    return Some(accessor.clone());
                }
            }
            cursor = ctx.inner.parent.clone();
        }
        None
    }

    /// Create a child context carrying `meta` (Cordis `ctx.extend(meta)`).
    ///
    /// The parent is not mutated; children of the returned context inherit the
    /// metadata through the context chain.
    pub fn extend_meta(&self, meta: ContextMeta) -> Context {
        self.child_with_meta(meta)
    }

    /// Typed metadata lookup on this context and its ancestors.
    pub fn meta<T: Any + Send + Sync>(&self, key: &'static str) -> Option<Arc<T>> {
        self.meta_any(key)
            .and_then(|value| value.downcast::<T>().ok())
    }

    /// Type-erased metadata lookup on this context and its ancestors.
    pub fn meta_any(&self, key: &'static str) -> Option<Arc<dyn Any + Send + Sync>> {
        let mut cursor = Some(self.clone());
        while let Some(ctx) = cursor {
            if let Some(value) = ctx.inner.meta.entries.get(key) {
                return Some(Arc::clone(value));
            }
            cursor = ctx.inner.parent.clone();
        }
        None
    }
}
