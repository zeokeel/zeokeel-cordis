//! A Rust port of the **Cordis** plugin core.
//!
//! The official implementation is TypeScript (`@deepseek-ai/cordis` 4.0.5).
//! Rust has no property proxy, so the public API is typed instead of
//! `ctx.serviceName`; the semantics are kept:
//!
//! - one root [`Context`] with immutable scoped children (`extend`/`isolate`/`intercept`);
//! - [`Plugin`] fibers with `pending → loading → active → failed/unloading/disposed`;
//! - `inject` service dependencies gate activation and re-evaluate on service changes,
//!   including the optional [`AvailabilityCheck`] predicate (Cordis `Service.check`);
//! - every plugin owns one ordered effect list; disposers run in reverse order
//!   exactly once, and sync ([`Disposer`]) / async ([`AsyncDisposer`]) effects
//!   share that list so unloading keeps one deterministic order;
//! - events support emit / parallel / serial / bail and are owned by the
//!   registering fiber; the sync lane (`emit`/`bail`/`parallel`/`serial`) and the
//!   async lane (`parallel_async`/`serial_async`/`on_async`) are dispatched by
//!   the matching API, never silently downgraded;
//! - `waterfall` composes middleware around an explicit `next` continuation
//!   ([`WaterfallNext`], [`AsyncWaterfallNext`]): a listener that never calls
//!   `next` vetoes the rest of the chain, including the terminal behavior;
//! - plugin restart/update powers hot reload of plugins.
//!
//! # Module layout
//!
//! The module split mirrors the official TypeScript sources so both
//! implementations can be compared side by side:
//!
//! | module | official source | contents |
//! |---|---|---|
//! | [`context`] | `context.ts` | `Context`, `extend`/`isolate`/`intercept`, plugin entry points |
//! | [`service`] | `service.ts` | service keys, availability gates, provided-service entries |
//! | [`fiber`] | `fiber.ts` | fiber state machine, effect ownership, dispose/restart/update |
//! | [`events`] | `events.ts` | event bus (emit/bail/parallel/serial/waterfall) |
//! | [`registry`] | `registry.ts` | `Plugin` trait and the fiber registry |
//! | [`effect`] | `fiber.ts` (effects) | cleanup callbacks and the single-shot [`Disposer`] |
//! | [`async_effects`] | `fiber.ts` (async effects) | [`AsyncDisposer`] / [`AsyncEffect`] |
//! | [`exec`] | — | minimal future executor used by the sync/async bridges |
//! | [`waterfall`] | `events.ts` (waterfall) | waterfall chain types and terminals |
//!
//! Differences from the TS implementation are documented per item; in
//! particular JavaScript's callable Proxy, `Promise`-returning listeners and
//! async-iterable effects are represented by explicit Rust types
//! (`Disposer`, `AsyncDisposer`, boxed futures) instead of a dynamic proxy.
#![forbid(unsafe_code)]

// Compile-time contract: panic isolation is implemented with `catch_unwind`, so
// the crate cannot run under `panic = "abort"` (a plugin panic would abort the
// process before any containment code runs). Fail the build instead of silently
// shipping a kernel whose core promise is false.
#[cfg(panic = "abort")]
compile_error!(
    "cordis requires `panic = \"unwind\"`: panic isolation is impossible with      `panic = \"abort\"` (see docs/KERNEL-ISOLATION.md). Build with the default      unwind strategy, or run untrusted plugins behind process/WASM isolation."
);

pub mod async_effects;
pub mod config;
pub mod context;
pub mod effect;
pub mod error;
pub mod events;
pub mod exec;
pub mod fiber;
pub mod internal;
pub mod l2;
pub mod logger;
pub mod reflect;
pub mod registry;
pub mod service;
pub mod waterfall;

pub use async_effects::{cleanup_async, AsyncCleanup, AsyncDisposer, AsyncEffect};
pub use config::{ConfigSchema, JsonConfigSchema, JsonType, ValidationError};
pub use context::Context;
pub use effect::{cleanup, Cleanup, Disposer, Effect};
pub use error::{CordisError, Result};
pub use events::{EventOptions, EventResult, Events};
pub use exec::{block_on, boxed, set_blocking_executor, yield_now, BlockingExecutor, BoxFuture};
pub use fiber::{EffectMeta, Fiber, FiberState, Settled};
pub use internal::{
    ConfigHook, ConfigNext, DispatchHook, DispatchInfo, DispatchMode, GetHook, GetNext,
    InternalBus, ListenerHook, ListenerOverride, ListenerRequest, PluginHook, ServiceHook, SetHook,
    SetNext, StatusHook, UpdateHook, UpdateNext,
};
pub use l2::{FiberQuota, IsolationCapabilities, StalledFiber};
pub use logger::{LogExporter, LogLevel, LogRecord, Logger, LoggerService, TracingExporter};
pub use reflect::{Accessor, AccessorGet, AccessorSet, ContextMeta};
pub use registry::{AsyncPlugin, Plugin, PluginKey, PluginKind, PluginRuntime, Registry};
pub use service::{
    provide_service, AvailabilityCheck, Service, ServiceGate, ServiceKey, ServiceValue,
};
pub use waterfall::{
    async_waterfall_listener, async_waterfall_terminal, null_async_waterfall_terminal,
    null_waterfall_terminal, waterfall_listener, waterfall_terminal, AsyncWaterfallListenerFn,
    AsyncWaterfallNext, AsyncWaterfallTerminal, WaterfallListenerFn, WaterfallNext,
    WaterfallTerminal,
};
