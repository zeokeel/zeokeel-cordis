//! Internal events: the typed `internal/*` hook surface.
//!
//! Official Cordis core emits `internal/plugin`, `internal/status`,
//! `internal/config`, `internal/service`, `internal/update`, `internal/get`,
//! `internal/set`, `internal/listener` and `internal/dispatch` on the same event
//! bus plugins use. In Rust a single listener closure type cannot carry both
//! `Value` payloads and typed references (`Fiber`, `Context`, `ServiceValue`),
//! so internal events are exposed as **typed hooks** instead of
//! `serde_json::Value` payloads. This keeps the semantics while avoiding a
//! JSON round-trip for handles that are not serializable:
//!
//! | internal event | Rust hook | payload |
//! |---|---|---|
//! | `internal/plugin` | [`InternalBus::on_plugin`] | `&Fiber` |
//! | `internal/status` | [`InternalBus::on_status`] | `&Fiber`, old [`FiberState`] |
//! | `internal/config` | [`InternalBus::on_config`] | waterfall: `&Fiber`, config, [`ConfigNext`] |
//! | `internal/service` | [`InternalBus::on_service`] | `&Context`, name, `Option<&ServiceValue>` |
//! | `internal/update` | [`InternalBus::on_update`] | waterfall: `&Fiber`, config, `no_save`, [`UpdateNext`] |
//! | `internal/get` | [`InternalBus::on_get`] | waterfall: `&Context`, name, [`GetNext`] |
//! | `internal/set` | [`InternalBus::on_set`] | waterfall: `&Context`, name, value, [`SetNext`] |
//! | `internal/listener` | [`InternalBus::on_listener`] | bail: [`ListenerRequest`] → replacement |
//! | `internal/dispatch` | [`InternalBus::on_dispatch`] | [`DispatchInfo`] |
//!
//! Hooks are registered with the `Context::on_internal_*` methods and are
//! removed with the owning fiber, exactly like event listeners.
//!
//! ## Ownership: context methods vs the raw bus
//!
//! There are two ways to register a hook, and they differ in ownership:
//!
//! | entry point | owned by | removed on fiber unload | intended for |
//! |---|---|---|---|
//! | `Context::on_internal_*` (e.g. [`crate::Context::on_internal_status`]) | the calling fiber | yes (automatic) | normal plugins |
//! | [`crate::Events::internal`] / [`InternalBus`] | the caller | **no** (dispose manually) | embedder tooling that must outlive fibers |
//!
//! Both write into the same ordered hook list of the shared bus, so registration
//! order interleaves as expected and `internal/*` names are never dispatched
//! back into the bus. [`InternalBus`]'s registration methods are an
//! **advanced/unstable** surface: prefer the `Context::on_internal_*` wrappers
//! unless the hook must survive the fiber that installed it.
//!
//! [`crate::Context::on_lifecycle`] is a general convenience built on that raw
//! bus: a non-fiber-bound [`LifecycleListener`] subscriber for fiber state
//! transitions.
//!
//! ## Semantics worth knowing before writing a hook
//!
//! - **`internal/get`: `None` means "service absent".** A get hook that returns
//!   `None` (or a `next.next()` that yields `None`) reports the service as not
//!   visible; it does **not** mean "skip this hook". A hook that wants the
//!   remaining hooks to run must call [`GetNext::next`] and pass its value on.
//! - **`internal/get` uses a global re-entrancy latch.** While any get hook is
//!   running, further `ctx.get`/`get_named` calls (even for *other* names) go
//!   straight to the store and **bypass every get hook**; the latch is not a
//!   per-name recursion guard.
//! - **A panicking `internal/update` hook cannot veto.** Hook panics are
//!   contained (`tracing::error!`) and the chain continues with the incoming
//!   config, so the terminal still commits the raw config and restarts — a
//!   policy hook that must reject an update has to skip `next` explicitly.
//! - The other waterfall hooks (`config`/`get`/`set`) follow the same
//!   panic-containment rule: a panic degrades to "continue with the incoming
//!   value" (see the private `contained` helper), never to an error.
//!
//! Dispatch mode names match Cordis `DispatchMode`.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::context::Context;
use crate::effect::{cleanup, Disposer};
use crate::events::ListenerFn;
use crate::fiber::{Fiber, FiberState};
use crate::service::ServiceValue;

/// Run one embedder-supplied callback with panic containment.
///
/// Hook panics are reported through `tracing` and never unwind out of the
/// kernel: the caller continues with its fallback value (or with the remaining
/// hooks). This is the single containment point for every `internal/*` hook.
pub(crate) fn contained<T>(
    target: &'static str,
    callback: &'static str,
    fallback: T,
    body: impl FnOnce() -> T,
) -> T {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(value) => value,
        Err(payload) => {
            tracing::error!(
                target: "zeokeel_cordis::containment",
                component = target,
                callback = callback,
                panic = %panic_message(payload.as_ref()),
                "embedder callback panicked; contained"
            );
            fallback
        }
    }
}

/// Render a panic payload as text for diagnostics.
pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "panicked".to_string()
    }
}

/// Mode of a public event dispatch (`internal/dispatch` payload).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchMode {
    Emit,
    Parallel,
    Serial,
    Bail,
    Waterfall,
}

impl DispatchMode {
    /// Lower-case name used in diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Emit => "emit",
            Self::Parallel => "parallel",
            Self::Serial => "serial",
            Self::Bail => "bail",
            Self::Waterfall => "waterfall",
        }
    }
}

/// `internal/dispatch` payload: a non-internal event is about to be delivered.
#[derive(Debug, Clone)]
pub struct DispatchInfo {
    /// Dispatch strategy (`emit` / `parallel` / `serial` / `bail` / `waterfall`).
    pub mode: DispatchMode,
    /// Event name.
    pub name: String,
    /// Dispatch arguments.
    pub args: Vec<Value>,
}

/// `internal/listener` request: a listener is about to be registered.
#[derive(Debug, Clone)]
pub struct ListenerRequest<'a> {
    /// Event name.
    pub name: &'a str,
    /// Registration order (`prepend`).
    pub prepend: bool,
    /// Bypass context filtering (`global`).
    pub global: bool,
    /// Self-removing listener (`once`).
    pub once: bool,
    /// Owning fiber id.
    pub owner: u64,
}

/// `internal/listener` replacement: a hook may substitute the registration.
#[derive(Clone)]
pub struct ListenerOverride {
    /// Replacement listener.
    pub callback: ListenerFn,
    /// Replacement registration order.
    pub prepend: bool,
    /// Replacement filter bypass.
    pub global: bool,
    /// Replacement self-removal flag.
    pub once: bool,
}

/// `internal/plugin` hook: a fiber was created.
pub type PluginHook = Arc<dyn Fn(&Fiber) + Send + Sync>;
/// `internal/status` hook: a fiber changed state (`old` is the previous state).
pub type StatusHook = Arc<dyn Fn(&Fiber, FiberState) + Send + Sync>;
/// `internal/service` hook: a service was provided (`Some`) or removed (`None`).
pub type ServiceHook = Arc<dyn Fn(&Context, &str, Option<&ServiceValue>) + Send + Sync>;
/// `internal/config` waterfall hook.
pub type ConfigHook = Arc<dyn for<'a> Fn(&Fiber, Value, &ConfigNext<'a>) -> Value + Send + Sync>;
/// `internal/update` waterfall hook (never calling `next` vetoes the restart).
pub type UpdateHook = Arc<dyn for<'a> Fn(&Fiber, Value, bool, &UpdateNext<'a>) + Send + Sync>;
/// `internal/get` waterfall hook.
pub type GetHook =
    Arc<dyn for<'a> Fn(&Context, &str, &GetNext<'a>) -> Option<ServiceValue> + Send + Sync>;
/// `internal/set` waterfall hook.
pub type SetHook =
    Arc<dyn for<'a> Fn(&Context, &str, &ServiceValue, &SetNext<'a>) -> bool + Send + Sync>;
/// One fiber lifecycle transition delivered to a [`LifecycleListener`]
/// (see [`crate::Context::on_lifecycle`]).
///
/// Transitions are reported for the whole context tree the observer was
/// installed on (the internal hook bus is shared by the root context), including
/// the terminal `* -> Disposed` transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleEvent {
    /// Id of the fiber whose state changed (`0` is the root fiber).
    pub fiber_id: u64,
    /// Display name of the fiber's plugin (`root` for the root fiber).
    pub name: String,
    /// State before the transition.
    pub from: FiberState,
    /// State after the transition (snapshot taken under the state lock at
    /// transition time, XT-03; never re-read from the fiber when the hook runs).
    pub to: FiberState,
    /// Monotonic per-fiber transition sequence (XT-03). Delivery across
    /// threads may interleave; sort by `seq` per `fiber_id` to recover the
    /// exact transition order.
    pub seq: u64,
}

/// A lifecycle subscriber (see [`crate::Context::on_lifecycle`]).
///
/// The listener is **not** owned by a fiber: it keeps receiving events after the
/// fiber (or context) that installed it unloads, until its [`Disposer`] is
/// disposed. The disposer is a single-owner handle: dropping the last handle is
/// equivalent to disposing it ([`Disposer`]'s `Drop` safety net), so keep it
/// alive to keep the subscription. Panics are contained and logged, other
/// subscribers still run.
pub type LifecycleListener = Arc<dyn Fn(&LifecycleEvent) + Send + Sync>;

/// `internal/listener` bail hook: returning `Some` replaces the registration.
pub type ListenerHook =
    Arc<dyn for<'a> Fn(&ListenerRequest<'a>) -> Option<ListenerOverride> + Send + Sync>;
/// `internal/dispatch` observer hook.
pub type DispatchHook = Arc<dyn Fn(&DispatchInfo) + Send + Sync>;

/// Continuation of the `internal/config` waterfall.
pub struct ConfigNext<'a> {
    hooks: &'a [ConfigHook],
    fiber: &'a Fiber,
}

impl ConfigNext<'_> {
    /// Continue the chain with `config`; the terminal returns the final config.
    pub fn next(&self, config: Value) -> Value {
        match self.hooks.split_first() {
            None => config,
            Some((hook, rest)) => {
                let next = ConfigNext {
                    hooks: rest,
                    fiber: self.fiber,
                };
                // A panicking hook falls through to the rest of the chain with
                // the incoming config, so the caller still gets a value.
                let fallback = config.clone();
                match contained("zeokeel_cordis::internal", "internal/config", None, || {
                    Some(hook(self.fiber, config, &next))
                }) {
                    Some(value) => value,
                    None => next.next(fallback),
                }
            }
        }
    }
}

/// Continuation of the `internal/update` waterfall.
///
/// The terminal (the actual config store + restart) runs at most once; a hook
/// that never calls [`UpdateNext::next`] vetoes the update.
///
/// **Panic = allow:** a panicking hook is contained (logged, not propagated) and
/// the chain continues with the incoming config, so the terminal still commits
/// the new raw config and restarts. A policy hook that must reject dangerous
/// configs therefore has to veto explicitly (`return` without `next`) instead of
/// relying on a panic.
pub struct UpdateNext<'a> {
    hooks: &'a [UpdateHook],
    fiber: &'a Fiber,
    terminal: &'a dyn Fn(Value, bool) -> bool,
    ran: &'a AtomicBool,
}

impl UpdateNext<'_> {
    /// Continue the chain; returns whether the terminal has run.
    pub fn next(&self, config: Value, no_save: bool) -> bool {
        match self.hooks.split_first() {
            None => {
                if self.ran.swap(true, Ordering::SeqCst) {
                    return true;
                }
                (self.terminal)(config, no_save);
                true
            }
            Some((hook, rest)) => {
                let next = UpdateNext {
                    hooks: rest,
                    fiber: self.fiber,
                    terminal: self.terminal,
                    ran: self.ran,
                };
                let fallback = config.clone();
                let outcome =
                    contained("zeokeel_cordis::internal", "internal/update", None, || {
                        hook(self.fiber, config, no_save, &next);
                        Some(())
                    });
                if outcome.is_none() {
                    // Panicked: fall through; the terminal runs at most once,
                    // so an already-run terminal is not repeated.
                    return next.next(fallback, no_save);
                }
                self.ran.load(Ordering::SeqCst)
            }
        }
    }
}

/// Continuation of the `internal/get` waterfall.
///
/// `None` is the "service absent/invisible" answer, not "skip this hook": only
/// calling [`GetNext::next`] runs the remaining hooks and, eventually, the store
/// terminal.
///
/// The whole waterfall runs behind a **global** re-entrancy latch
/// (`InternalBus::get_depth`): a get hook that calls `ctx.get(other)` bypasses
/// the get hooks of `other` and reads the store directly. This prevents
/// unbounded recursion at the price of not seeing hooks while one is running.
pub struct GetNext<'a> {
    hooks: &'a [GetHook],
    ctx: &'a Context,
    name: &'a str,
    terminal: &'a dyn Fn() -> Option<ServiceValue>,
}

impl GetNext<'_> {
    /// Resolve the service (running the remaining hooks).
    pub fn next(&self) -> Option<ServiceValue> {
        match self.hooks.split_first() {
            None => (self.terminal)(),
            Some((hook, rest)) => {
                let next = GetNext {
                    hooks: rest,
                    ctx: self.ctx,
                    name: self.name,
                    terminal: self.terminal,
                };
                match contained("zeokeel_cordis::internal", "internal/get", None, || {
                    Some(hook(self.ctx, self.name, &next))
                }) {
                    Some(value) => value,
                    None => next.next(),
                }
            }
        }
    }
}

/// Continuation of the `internal/set` waterfall.
pub struct SetNext<'a> {
    hooks: &'a [SetHook],
    ctx: &'a Context,
    name: &'a str,
    value: &'a ServiceValue,
    terminal: &'a dyn Fn() -> bool,
}

impl SetNext<'_> {
    /// Perform the write (running the remaining hooks).
    pub fn next(&self) -> bool {
        match self.hooks.split_first() {
            None => (self.terminal)(),
            Some((hook, rest)) => {
                let next = SetNext {
                    hooks: rest,
                    ctx: self.ctx,
                    name: self.name,
                    value: self.value,
                    terminal: self.terminal,
                };
                match contained("zeokeel_cordis::internal", "internal/set", None, || {
                    Some(hook(self.ctx, self.name, self.value, &next))
                }) {
                    Some(written) => written,
                    None => next.next(),
                }
            }
        }
    }
}

/// A lock-free-ish ordered hook list with id-based removal.
struct Hooks<T: ?Sized> {
    next: AtomicU64,
    hooks: Mutex<Vec<(u64, Arc<T>)>>,
}

impl<T: ?Sized> Default for Hooks<T> {
    fn default() -> Self {
        Self {
            next: AtomicU64::new(0),
            hooks: Mutex::new(Vec::new()),
        }
    }
}

impl<T: ?Sized + Send + Sync + 'static> Hooks<T> {
    fn register(self: &Arc<Self>, hook: Arc<T>) -> Disposer {
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        if let Ok(mut hooks) = self.hooks.lock() {
            hooks.push((id, hook));
        }
        let holes = Arc::clone(self);
        Disposer::new(vec![cleanup(move || {
            if let Ok(mut hooks) = holes.hooks.lock() {
                hooks.retain(|(entry, _)| *entry != id);
            }
        })])
    }

    fn snapshot(&self) -> Vec<Arc<T>> {
        self.hooks
            .lock()
            .map(|hooks| hooks.iter().map(|(_, hook)| Arc::clone(hook)).collect())
            .unwrap_or_default()
    }

    fn is_empty(&self) -> bool {
        self.hooks
            .lock()
            .map(|hooks| hooks.is_empty())
            .unwrap_or(true)
    }
}

// Type aliases for the trait objects stored by the bus (kept separate so the
// struct fields stay readable and clippy::type_complexity is satisfied).
type PluginHookDyn = dyn Fn(&Fiber) + Send + Sync;
type StatusHookDyn = dyn Fn(&Fiber, FiberState) + Send + Sync;
pub(crate) type LifecycleHookDyn = dyn Fn(&Fiber, FiberState, FiberState, u64) + Send + Sync;
type ServiceHookDyn = dyn Fn(&Context, &str, Option<&ServiceValue>) + Send + Sync;
type ConfigHookDyn = dyn for<'a> Fn(&Fiber, Value, &ConfigNext<'a>) -> Value + Send + Sync;
type UpdateHookDyn = dyn for<'a> Fn(&Fiber, Value, bool, &UpdateNext<'a>) + Send + Sync;
type GetHookDyn =
    dyn for<'a> Fn(&Context, &str, &GetNext<'a>) -> Option<ServiceValue> + Send + Sync;
type SetHookDyn = dyn for<'a> Fn(&Context, &str, &ServiceValue, &SetNext<'a>) -> bool + Send + Sync;
type ListenerHookDyn =
    dyn for<'a> Fn(&ListenerRequest<'a>) -> Option<ListenerOverride> + Send + Sync;
type DispatchHookDyn = dyn Fn(&DispatchInfo) + Send + Sync;

/// Registered internal-event hooks, shared by a context tree.
#[derive(Default)]
pub struct InternalBus {
    plugin: Arc<Hooks<PluginHookDyn>>,
    status: Arc<Hooks<StatusHookDyn>>,
    lifecycle: Arc<Hooks<LifecycleHookDyn>>,
    service: Arc<Hooks<ServiceHookDyn>>,
    config: Arc<Hooks<ConfigHookDyn>>,
    update: Arc<Hooks<UpdateHookDyn>>,
    get: Arc<Hooks<GetHookDyn>>,
    set: Arc<Hooks<SetHookDyn>>,
    listener: Arc<Hooks<ListenerHookDyn>>,
    dispatch: Arc<Hooks<DispatchHookDyn>>,
    get_depth: AtomicUsize,
    dispatch_depth: AtomicUsize,
}

impl InternalBus {
    /// Create an empty bus.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an `internal/plugin` hook.
    pub fn on_plugin(self: &Arc<Self>, hook: PluginHook) -> Disposer {
        self.plugin.register(hook)
    }

    /// Register an `internal/status` hook.
    pub fn on_status(self: &Arc<Self>, hook: StatusHook) -> Disposer {
        self.status.register(hook)
    }

    /// Register an `internal/service` hook.
    pub fn on_service(self: &Arc<Self>, hook: ServiceHook) -> Disposer {
        self.service.register(hook)
    }

    /// Register an `internal/config` waterfall hook.
    pub fn on_config(self: &Arc<Self>, hook: ConfigHook) -> Disposer {
        self.config.register(hook)
    }

    /// Register an `internal/update` waterfall hook.
    pub fn on_update(self: &Arc<Self>, hook: UpdateHook) -> Disposer {
        self.update.register(hook)
    }

    /// Register an `internal/get` waterfall hook.
    pub fn on_get(self: &Arc<Self>, hook: GetHook) -> Disposer {
        self.get.register(hook)
    }

    /// Register an `internal/set` waterfall hook.
    pub fn on_set(self: &Arc<Self>, hook: SetHook) -> Disposer {
        self.set.register(hook)
    }

    /// Register an `internal/listener` bail hook.
    pub fn on_listener(self: &Arc<Self>, hook: ListenerHook) -> Disposer {
        self.listener.register(hook)
    }

    /// Register an `internal/dispatch` observer.
    pub fn on_dispatch(self: &Arc<Self>, hook: DispatchHook) -> Disposer {
        self.dispatch.register(hook)
    }

    /// Whether any hook of any kind is registered (fast-path guard).
    pub fn is_empty(&self) -> bool {
        self.plugin.is_empty()
            && self.status.is_empty()
            && self.lifecycle.is_empty()
            && self.service.is_empty()
            && self.config.is_empty()
            && self.update.is_empty()
            && self.get.is_empty()
            && self.set.is_empty()
            && self.listener.is_empty()
            && self.dispatch.is_empty()
    }

    pub(crate) fn emit_plugin(&self, fiber: &Fiber) {
        for hook in self.plugin.snapshot() {
            contained("zeokeel_cordis::internal", "internal/plugin", (), || {
                hook(fiber)
            });
        }
    }

    pub(crate) fn emit_status(&self, fiber: &Fiber, old: FiberState) {
        for hook in self.status.snapshot() {
            contained("zeokeel_cordis::internal", "internal/status", (), || {
                hook(fiber, old)
            });
        }
    }

    /// Register a lifecycle-broadcast hook (used by `Context::on_lifecycle`).
    ///
    /// Unlike [`InternalBus::on_status`], the hook receives the transition
    /// snapshot `(from, to, seq)` captured under the state lock.
    pub(crate) fn on_lifecycle_hook(self: &Arc<Self>, hook: Arc<LifecycleHookDyn>) -> Disposer {
        self.lifecycle.register(hook)
    }

    /// Emit one lifecycle broadcast with the pre-captured transition snapshot.
    pub(crate) fn emit_lifecycle(&self, fiber: &Fiber, from: FiberState, to: FiberState, seq: u64) {
        for hook in self.lifecycle.snapshot() {
            contained(
                "zeokeel_cordis::internal",
                "lifecycle broadcast",
                (),
                || hook(fiber, from, to, seq),
            );
        }
    }

    pub(crate) fn emit_service(&self, ctx: &Context, name: &str, value: Option<&ServiceValue>) {
        for hook in self.service.snapshot() {
            contained("zeokeel_cordis::internal", "internal/service", (), || {
                hook(ctx, name, value)
            });
        }
    }

    /// Report a public dispatch (never for `internal/` names, which would
    /// recurse into the bus itself).
    pub(crate) fn emit_dispatch(&self, mode: DispatchMode, name: &str, args: &[Value]) {
        if name.starts_with("internal/") || self.dispatch.is_empty() {
            return;
        }
        if self.dispatch_depth.fetch_add(1, Ordering::SeqCst) > 0 {
            self.dispatch_depth.fetch_sub(1, Ordering::SeqCst);
            return;
        }
        let info = DispatchInfo {
            mode,
            name: name.to_string(),
            args: args.to_vec(),
        };
        for hook in self.dispatch.snapshot() {
            contained("zeokeel_cordis::internal", "internal/dispatch", (), || {
                hook(&info)
            });
        }
        self.dispatch_depth.fetch_sub(1, Ordering::SeqCst);
    }

    /// `internal/config` waterfall: transform the raw plugin config.
    pub(crate) fn waterfall_config(&self, fiber: &Fiber, config: Value) -> Value {
        if self.config.is_empty() {
            return config;
        }
        let hooks = self.config.snapshot();
        ConfigNext {
            hooks: &hooks,
            fiber,
        }
        .next(config)
    }

    /// `internal/update` waterfall: returns whether the terminal (config store +
    /// restart) ran; hooks that skip `next` veto the restart.
    pub(crate) fn waterfall_update(
        &self,
        fiber: &Fiber,
        config: Value,
        no_save: bool,
        terminal: &dyn Fn(Value, bool) -> bool,
    ) -> bool {
        if self.update.is_empty() {
            return terminal(config, no_save);
        }
        let hooks = self.update.snapshot();
        let ran = AtomicBool::new(false);
        let next = UpdateNext {
            hooks: &hooks,
            fiber,
            terminal,
            ran: &ran,
        };
        next.next(config, no_save)
    }

    /// `internal/get` waterfall: resolve/override a service read.
    pub(crate) fn waterfall_get(
        &self,
        ctx: &Context,
        name: &str,
        terminal: &dyn Fn() -> Option<ServiceValue>,
    ) -> Option<ServiceValue> {
        if self.get.is_empty() {
            return terminal();
        }
        if self.get_depth.fetch_add(1, Ordering::SeqCst) > 0 {
            self.get_depth.fetch_sub(1, Ordering::SeqCst);
            return terminal();
        }
        let hooks = self.get.snapshot();
        let value = GetNext {
            hooks: &hooks,
            ctx,
            name,
            terminal,
        }
        .next();
        self.get_depth.fetch_sub(1, Ordering::SeqCst);
        value
    }

    /// `internal/set` waterfall: write a provided service value.
    pub(crate) fn waterfall_set(
        &self,
        ctx: &Context,
        name: &str,
        value: &ServiceValue,
        terminal: &dyn Fn() -> bool,
    ) -> bool {
        if self.set.is_empty() {
            return terminal();
        }
        let hooks = self.set.snapshot();
        SetNext {
            hooks: &hooks,
            ctx,
            name,
            value,
            terminal,
        }
        .next()
    }

    /// `internal/listener` bail hook: first replacement wins.
    pub(crate) fn intercept_listener(
        &self,
        request: &ListenerRequest<'_>,
    ) -> Option<ListenerOverride> {
        self.listener.snapshot().into_iter().find_map(|hook| {
            contained(
                "zeokeel_cordis::internal",
                "internal/listener",
                None,
                || hook(request),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_list_removal_and_is_empty() {
        let bus = Arc::new(InternalBus::new());
        assert!(bus.is_empty());
        let disposer = bus.on_plugin(Arc::new(|_fiber: &Fiber| {}));
        assert!(!bus.is_empty());
        disposer.dispose();
        assert!(bus.is_empty());
        // double dispose is a no-op
        disposer.dispose();
    }

    #[test]
    fn dispatch_mode_names() {
        assert_eq!(DispatchMode::Emit.as_str(), "emit");
        assert_eq!(DispatchMode::Waterfall.as_str(), "waterfall");
    }
}
