//! Event bus: sync/async lanes, bail and waterfall dispatch.

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::effect::Disposer;
use crate::exec::BoxFuture;
use crate::internal::{DispatchMode, InternalBus};
use crate::waterfall::{
    null_async_waterfall_terminal, null_waterfall_terminal, AsyncWaterfallListenerFn,
    AsyncWaterfallNext, AsyncWaterfallTerminal, WaterfallListenerFn, WaterfallNext,
    WaterfallTerminal,
};

/// Result returned by an event listener.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum EventResult {
    Continue,
    Bail(Value),
}

impl EventResult {
    /// Cordis `isBailed`: anything but `null`, `false` and `undefined` bails.
    ///
    /// `Value::Null` is the Rust representation of `undefined`/`null`, so both
    /// are treated as "keep going".
    fn into_bail(self) -> Option<Value> {
        match self {
            Self::Bail(value) if !value.is_null() && value != Value::Bool(false) => Some(value),
            _ => None,
        }
    }
}

/// Options accepted by [`Events`] registrations (Cordis `EventOptions`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EventOptions {
    /// Insert before existing listeners for the same event.
    pub prepend: bool,
    /// Receive the event regardless of context-scope filtering.
    pub global: bool,
    /// Dispose the listener after its first invocation.
    pub once: bool,
}

impl EventOptions {
    /// Default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Prepend the listener.
    pub fn prepend(mut self) -> Self {
        self.prepend = true;
        self
    }

    /// Bypass context-scope filtering.
    pub fn global(mut self) -> Self {
        self.global = true;
        self
    }

    /// Self-remove after the first invocation.
    pub fn once(mut self) -> Self {
        self.once = true;
        self
    }
}

/// Context scope map (`service name → isolation label id`).
pub(crate) type ContextScope = std::collections::HashMap<&'static str, u64>;

pub(crate) type ListenerFn = Arc<dyn Fn(&[Value]) -> EventResult + Send + Sync>;
pub(crate) type AsyncListenerFn =
    Arc<dyn Fn(&[Value]) -> BoxFuture<'static, EventResult> + Send + Sync>;

/// Invoke one synchronous listener with panic isolation (CORE-030).
///
/// A panicking listener must not abort dispatch or take down the publisher:
/// the panic is reported through `tracing` and dispatch continues as if the
/// listener returned [`EventResult::Continue`].
fn call_sync(name: &str, callback: &ListenerFn, args: &[Value]) -> EventResult {
    match catch_unwind(AssertUnwindSafe(|| callback(args))) {
        Ok(result) => result,
        Err(_) => {
            tracing::error!(
                target: "zeokeel_cordis::events",
                event = name,
                "event listener panicked; dispatch continues"
            );
            EventResult::Continue
        }
    }
}

/// Report a panic captured from an async listener (future construction or poll).
fn report_async_panic(name: &str, stage: &'static str, payload: crate::exec::PanicPayload) {
    tracing::error!(
        target: "zeokeel_cordis::events",
        event = name,
        stage = stage,
        panic = %crate::internal::panic_message(payload.as_ref()),
        "async event listener panicked; dispatch continues"
    );
}

#[derive(Clone)]
enum ListenerBody {
    Sync(ListenerFn),
    Async(AsyncListenerFn),
}

#[derive(Clone)]
struct Listener {
    id: u64,
    owner: u64,
    body: ListenerBody,
    scope: ContextScope,
    global: bool,
    once: bool,
    called: Arc<AtomicBool>,
}

#[derive(Clone)]
struct WaterfallEntry {
    id: u64,
    owner: u64,
    callback: WaterfallListenerFn,
    scope: ContextScope,
    global: bool,
}

#[derive(Clone)]
struct AsyncWaterfallEntry {
    id: u64,
    owner: u64,
    callback: AsyncWaterfallListenerFn,
    scope: ContextScope,
    global: bool,
}

/// Event bus installed on the root context. Listeners are owned by the fiber
/// that registered them and are removed when that fiber unloads.
///
/// There are two listener lanes, because a Rust closure has one fixed return
/// type:
///
/// - the **sync lane** (`on` / `on_with`) is dispatched by [`Events::emit`],
///   [`Events::bail`], [`Events::parallel`] and the (sync) [`Events::waterfall`];
/// - the **async lane** (`on_async` / `on_async_with`) is dispatched by
///   [`Events::parallel_async`] and [`Events::serial_async`].
///
/// Both lanes keep one registration order per event name: `serial_async`
/// interleaves sync and async listeners exactly as they were registered. Async
/// listeners are never polled by sync dispatch (a future cannot be completed
/// without an await point), so use the async dispatch functions for events that
/// carry async listeners.
#[derive(Default)]
pub struct Events {
    listeners: Mutex<HashMap<String, Vec<Listener>>>,
    waterfall_listeners: Mutex<HashMap<String, Vec<WaterfallEntry>>>,
    async_waterfall_listeners: Mutex<HashMap<String, Vec<AsyncWaterfallEntry>>>,
    next_id: AtomicU64,
    internal: Arc<InternalBus>,
}

impl Events {
    pub fn new() -> Self {
        Self::default()
    }

    /// The typed `internal/*` hook bus attached to this event bus.
    pub fn internal(&self) -> &Arc<InternalBus> {
        &self.internal
    }

    /// Context-scope filter (Cordis `Context.filter`): `global` listeners always
    /// pass; when a dispatch scope is present, the owner's isolation map must
    /// match it. Direct [`Events`] calls without a scope skip filtering.
    fn passes(global: bool, scope: &ContextScope, dispatch: Option<&ContextScope>) -> bool {
        global
            || match dispatch {
                None => true,
                Some(dispatch) => dispatch == scope,
            }
    }

    fn listener_snapshot(&self, name: &str) -> Vec<Listener> {
        self.listeners
            .lock()
            .ok()
            .and_then(|map| map.get(name).cloned())
            .unwrap_or_default()
    }

    fn waterfall_snapshot(
        &self,
        name: &str,
        scope: Option<&ContextScope>,
    ) -> Vec<WaterfallListenerFn> {
        self.waterfall_listeners
            .lock()
            .ok()
            .and_then(|map| map.get(name).cloned())
            .unwrap_or_default()
            .into_iter()
            .filter(|entry| Self::passes(entry.global, &entry.scope, scope))
            .map(|entry| entry.callback)
            .collect()
    }

    fn async_waterfall_snapshot(
        &self,
        name: &str,
        scope: Option<&ContextScope>,
    ) -> Vec<AsyncWaterfallListenerFn> {
        self.async_waterfall_listeners
            .lock()
            .ok()
            .and_then(|map| map.get(name).cloned())
            .unwrap_or_default()
            .into_iter()
            .filter(|entry| Self::passes(entry.global, &entry.scope, scope))
            .map(|entry| entry.callback)
            .collect()
    }

    fn remove_listener(&self, name: &str, id: u64) {
        if let Ok(mut map) = self.listeners.lock() {
            if let Some(list) = map.get_mut(name) {
                list.retain(|listener| listener.id != id);
            }
        }
    }

    fn remove_waterfall_listener(&self, name: &str, id: u64) {
        if let Ok(mut map) = self.waterfall_listeners.lock() {
            if let Some(list) = map.get_mut(name) {
                list.retain(|entry| entry.id != id);
            }
        }
    }

    fn remove_async_waterfall_listener(&self, name: &str, id: u64) {
        if let Ok(mut map) = self.async_waterfall_listeners.lock() {
            if let Some(list) = map.get_mut(name) {
                list.retain(|entry| entry.id != id);
            }
        }
    }

    pub(crate) fn remove_owner(&self, owner: u64) {
        if let Ok(mut map) = self.listeners.lock() {
            for list in map.values_mut() {
                list.retain(|listener| listener.owner != owner);
            }
        }
        if let Ok(mut map) = self.waterfall_listeners.lock() {
            for list in map.values_mut() {
                list.retain(|entry| entry.owner != owner);
            }
        }
        if let Ok(mut map) = self.async_waterfall_listeners.lock() {
            for list in map.values_mut() {
                list.retain(|entry| entry.owner != owner);
            }
        }
    }

    /// Dispatch synchronously, ignoring listener return values.
    ///
    /// Only the sync lane runs; async listeners are dispatched by
    /// [`Events::parallel_async`] / [`Events::serial_async`].
    pub fn emit(&self, name: &str, args: &[Value], scope: Option<&ContextScope>) {
        self.internal.emit_dispatch(DispatchMode::Emit, name, args);
        for listener in self.listener_snapshot(name) {
            if !Self::passes(listener.global, &listener.scope, scope) {
                continue;
            }
            if let ListenerBody::Sync(callback) = &listener.body {
                if listener.once && listener.called.swap(true, Ordering::SeqCst) {
                    continue;
                }
                let _ = call_sync(name, callback, args);
                if listener.once {
                    self.remove_listener(name, listener.id);
                }
            }
        }
    }

    /// Dispatch synchronously until a listener bails (Cordis `bail`).
    ///
    /// `Bail(Value::Null)` / `Bail(Value::Bool(false))` do not stop the chain
    /// (`isBailed` semantics). The async-lane counterpart is
    /// [`Events::serial_async`].
    pub fn bail(&self, name: &str, args: &[Value], scope: Option<&ContextScope>) -> Option<Value> {
        self.internal.emit_dispatch(DispatchMode::Bail, name, args);
        for listener in self.listener_snapshot(name) {
            if !Self::passes(listener.global, &listener.scope, scope) {
                continue;
            }
            if let ListenerBody::Sync(callback) = &listener.body {
                if listener.once && listener.called.swap(true, Ordering::SeqCst) {
                    continue;
                }
                let result = call_sync(name, callback, args);
                if listener.once {
                    self.remove_listener(name, listener.id);
                }
                if let Some(value) = result.into_bail() {
                    return Some(value);
                }
            }
        }
        None
    }

    /// Run every sync-lane listener, ignoring their results (Cordis `parallel`
    /// for listeners without an await point).
    pub fn parallel(&self, name: &str, args: &[Value], scope: Option<&ContextScope>) {
        self.internal
            .emit_dispatch(DispatchMode::Parallel, name, args);
        for listener in self.listener_snapshot(name) {
            if !Self::passes(listener.global, &listener.scope, scope) {
                continue;
            }
            if let ListenerBody::Sync(callback) = &listener.body {
                if listener.once && listener.called.swap(true, Ordering::SeqCst) {
                    continue;
                }
                let _ = call_sync(name, callback, args);
                if listener.once {
                    self.remove_listener(name, listener.id);
                }
            }
        }
    }

    /// Run sync-lane listeners in registration order until one bails.
    pub fn serial(
        &self,
        name: &str,
        args: &[Value],
        scope: Option<&ContextScope>,
    ) -> Option<Value> {
        self.bail(name, args, scope)
    }

    /// Await every listener concurrently; a `Bail` result does not cancel the
    /// other listeners (Cordis `parallel`).
    pub async fn parallel_async(&self, name: &str, args: &[Value], scope: Option<&ContextScope>) {
        self.internal
            .emit_dispatch(DispatchMode::Parallel, name, args);
        let mut pending = Vec::new();
        for listener in self.listener_snapshot(name) {
            if !Self::passes(listener.global, &listener.scope, scope) {
                continue;
            }
            match &listener.body {
                ListenerBody::Sync(callback) => {
                    if listener.once && listener.called.swap(true, Ordering::SeqCst) {
                        continue;
                    }
                    let _ = call_sync(name, callback, args);
                    if listener.once {
                        self.remove_listener(name, listener.id);
                    }
                }
                ListenerBody::Async(callback) => {
                    if listener.once && listener.called.swap(true, Ordering::SeqCst) {
                        continue;
                    }
                    match catch_unwind(AssertUnwindSafe(|| callback(args))) {
                        Ok(future) => {
                            pending.push(crate::exec::boxed(crate::exec::catch_panic(future)))
                        }
                        Err(payload) => report_async_panic(name, "future construction", payload),
                    }
                    if listener.once {
                        self.remove_listener(name, listener.id);
                    }
                }
            }
        }
        for result in crate::exec::join_all(pending).await {
            if let Err(payload) = result {
                report_async_panic(name, "poll", payload);
            }
        }
    }

    /// Await listeners in registration order until one bails (Cordis `serial`).
    pub async fn serial_async(
        &self,
        name: &str,
        args: &[Value],
        scope: Option<&ContextScope>,
    ) -> Option<Value> {
        self.internal
            .emit_dispatch(DispatchMode::Serial, name, args);
        for listener in self.listener_snapshot(name) {
            if !Self::passes(listener.global, &listener.scope, scope) {
                continue;
            }
            let result = match &listener.body {
                ListenerBody::Sync(callback) => {
                    if listener.once && listener.called.swap(true, Ordering::SeqCst) {
                        continue;
                    }
                    let result = call_sync(name, callback, args);
                    if listener.once {
                        self.remove_listener(name, listener.id);
                    }
                    result
                }
                ListenerBody::Async(callback) => {
                    if listener.once && listener.called.swap(true, Ordering::SeqCst) {
                        continue;
                    }
                    let result = match catch_unwind(AssertUnwindSafe(|| callback(args))) {
                        Ok(future) => match crate::exec::catch_panic(future).await {
                            Ok(result) => result,
                            Err(payload) => {
                                report_async_panic(name, "poll", payload);
                                EventResult::Continue
                            }
                        },
                        Err(payload) => {
                            report_async_panic(name, "future construction", payload);
                            EventResult::Continue
                        }
                    };
                    if listener.once {
                        self.remove_listener(name, listener.id);
                    }
                    result
                }
            };
            if let Some(value) = result.into_bail() {
                return Some(value);
            }
        }
        None
    }

    /// Compose the sync waterfall chain for `name`.
    pub fn waterfall(
        &self,
        name: &str,
        args: &[Value],
        terminal: Option<WaterfallTerminal>,
        scope: Option<&ContextScope>,
    ) -> Value {
        self.internal
            .emit_dispatch(DispatchMode::Waterfall, name, args);
        let listeners = self.waterfall_snapshot(name, scope);
        let terminal = terminal.unwrap_or_else(null_waterfall_terminal);
        WaterfallNext::new(listeners, args, terminal).next()
    }

    /// Compose and await the async waterfall chain for `name`.
    pub async fn waterfall_async(
        &self,
        name: &str,
        args: &[Value],
        terminal: Option<AsyncWaterfallTerminal>,
        scope: Option<&ContextScope>,
    ) -> Value {
        self.internal
            .emit_dispatch(DispatchMode::Waterfall, name, args);
        let listeners = self.async_waterfall_snapshot(name, scope);
        let terminal = terminal.unwrap_or_else(null_async_waterfall_terminal);
        // One panicking middleware must not unwind out of the dispatch; the
        // chain cannot be resumed, so the dispatch degrades to the async
        // waterfall's default value.
        let chain = AsyncWaterfallNext::new(listeners, args, terminal).next();
        match crate::exec::catch_panic(chain).await {
            Ok(value) => value,
            Err(payload) => {
                tracing::error!(
                    target: "zeokeel_cordis::events",
                    event = name,
                    panic = %crate::internal::panic_message(payload.as_ref()),
                    "async waterfall listener panicked; using the default value"
                );
                Value::Null
            }
        }
    }

    /// Number of live listeners across both lanes and both waterfall chains.
    pub fn listener_count(&self) -> usize {
        self.listeners
            .lock()
            .map(|map| map.values().map(Vec::len).sum())
            .unwrap_or(0)
            + self.waterfall_listener_count()
    }

    /// Number of live async-lane event listeners (diagnostics).
    pub fn async_listener_count(&self) -> usize {
        self.listeners
            .lock()
            .map(|map| {
                map.values()
                    .flatten()
                    .filter(|listener| matches!(listener.body, ListenerBody::Async(_)))
                    .count()
            })
            .unwrap_or(0)
    }

    /// Number of live waterfall middlewares, sync and async (diagnostics).
    pub fn waterfall_listener_count(&self) -> usize {
        let sync = self
            .waterfall_listeners
            .lock()
            .map(|map| map.values().map(Vec::len).sum())
            .unwrap_or(0);
        let asynchronous = self
            .async_waterfall_listeners
            .lock()
            .map(|map| map.values().map(Vec::len).sum())
            .unwrap_or(0);
        sync + asynchronous
    }

    pub(crate) fn add_listener(
        self: &Arc<Self>,
        name: &str,
        owner: u64,
        options: EventOptions,
        scope: ContextScope,
        callback: ListenerFn,
    ) -> Disposer {
        self.add_listener_body(name, owner, options, scope, ListenerBody::Sync(callback))
    }

    pub(crate) fn add_async_listener(
        self: &Arc<Self>,
        name: &str,
        owner: u64,
        options: EventOptions,
        scope: ContextScope,
        callback: AsyncListenerFn,
    ) -> Disposer {
        self.add_listener_body(name, owner, options, scope, ListenerBody::Async(callback))
    }

    fn add_listener_body(
        self: &Arc<Self>,
        name: &str,
        owner: u64,
        options: EventOptions,
        scope: ContextScope,
        body: ListenerBody,
    ) -> Disposer {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let listener = Listener {
            id,
            owner,
            body,
            scope,
            global: options.global,
            once: options.once,
            called: Arc::new(AtomicBool::new(false)),
        };
        if let Ok(mut map) = self.listeners.lock() {
            let list = map.entry(name.to_string()).or_default();
            if options.prepend {
                list.insert(0, listener);
            } else {
                list.push(listener);
            }
        }
        let bus = Arc::clone(self);
        let name = name.to_string();
        Disposer::new(vec![Box::new(move || bus.remove_listener(&name, id))])
    }

    pub(crate) fn add_waterfall_listener(
        self: &Arc<Self>,
        name: &str,
        owner: u64,
        options: EventOptions,
        scope: ContextScope,
        callback: WaterfallListenerFn,
    ) -> Disposer {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let entry = WaterfallEntry {
            id,
            owner,
            callback,
            scope,
            global: options.global,
        };
        if let Ok(mut map) = self.waterfall_listeners.lock() {
            let list = map.entry(name.to_string()).or_default();
            if options.prepend {
                list.insert(0, entry);
            } else {
                list.push(entry);
            }
        }
        let bus = Arc::clone(self);
        let name = name.to_string();
        Disposer::new(vec![Box::new(move || {
            bus.remove_waterfall_listener(&name, id)
        })])
    }

    pub(crate) fn add_async_waterfall_listener(
        self: &Arc<Self>,
        name: &str,
        owner: u64,
        options: EventOptions,
        scope: ContextScope,
        callback: AsyncWaterfallListenerFn,
    ) -> Disposer {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let entry = AsyncWaterfallEntry {
            id,
            owner,
            callback,
            scope,
            global: options.global,
        };
        if let Ok(mut map) = self.async_waterfall_listeners.lock() {
            let list = map.entry(name.to_string()).or_default();
            if options.prepend {
                list.insert(0, entry);
            } else {
                list.push(entry);
            }
        }
        let bus = Arc::clone(self);
        let name = name.to_string();
        Disposer::new(vec![Box::new(move || {
            bus.remove_async_waterfall_listener(&name, id)
        })])
    }
}
