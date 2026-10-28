//! Waterfall dispatch: middleware chains with an explicit `next` continuation.
//!
//! Cordis `waterfall` passes a `next` callback as the last listener argument;
//! every listener decides whether to continue the chain (call `next`), rewrite
//! the value on the way out (middleware), or veto the rest of the chain
//! (return without calling `next`). Rust has no callable-proxy, so the
//! continuation is a value: [`WaterfallNext`] for sync listeners and
//! [`AsyncWaterfallNext`] for async listeners.
//!
//! ```no_run
//! # use serde_json::{json, Value};
//! # use zeokeel_cordis::{Context, waterfall_terminal};
//! # fn demo(ctx: &Context) -> zeokeel_cordis::Result<()> {
//! ctx.on_waterfall("request", |_args, next| {
//!     let value = next.next(); // explicit continuation
//!     json!({ "wrapped": value })
//! })?;
//! let value = ctx.waterfall("request", &[], Some(waterfall_terminal(|_args| json!("base"))));
//! # let _ = value;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use serde_json::Value;

use crate::exec::{boxed, BoxFuture};

/// A synchronous waterfall middleware.
///
/// It receives the dispatch arguments and the explicit continuation; calling
/// `next.next()` runs the remaining chain, returning without calling it
/// vetoes the rest of the chain (including the terminal behavior).
pub type WaterfallListenerFn = Arc<dyn Fn(&[Value], &WaterfallNext) -> Value + Send + Sync>;

/// The built-in behavior that runs once the middleware chain is exhausted.
pub type WaterfallTerminal = Arc<dyn Fn(&[Value]) -> Value + Send + Sync>;

/// An asynchronous waterfall middleware; `next.next()` returns a future that
/// must be awaited to run the rest of the chain.
pub type AsyncWaterfallListenerFn =
    Arc<dyn Fn(&[Value], AsyncWaterfallNext) -> BoxFuture<'static, Value> + Send + Sync>;

/// The built-in behavior of an asynchronous waterfall chain.
pub type AsyncWaterfallTerminal = Arc<dyn Fn(&[Value]) -> BoxFuture<'static, Value> + Send + Sync>;

/// The explicit continuation handed to every synchronous middleware.
#[derive(Clone)]
pub struct WaterfallNext {
    remaining: Arc<[WaterfallListenerFn]>,
    args: Arc<[Value]>,
    terminal: WaterfallTerminal,
}

impl WaterfallNext {
    pub(crate) fn new(
        listeners: Vec<WaterfallListenerFn>,
        args: &[Value],
        terminal: WaterfallTerminal,
    ) -> Self {
        Self {
            remaining: Arc::from(listeners),
            args: Arc::from(args.to_vec()),
            terminal,
        }
    }

    /// The dispatch arguments (identical for every middleware).
    pub fn args(&self) -> &[Value] {
        &self.args
    }

    /// How many middlewares are left after the current one.
    pub fn remaining(&self) -> usize {
        self.remaining.len()
    }

    /// Run the rest of the chain, or the terminal behavior when nothing is
    /// left. Calling it more than once re-runs the remaining chain, which is
    /// how retry/rewrite middlewares are expressed in Cordis.
    pub fn next(&self) -> Value {
        match self.remaining.split_first() {
            Some((listener, rest)) => {
                let next = Self {
                    remaining: Arc::from(rest),
                    args: self.args.clone(),
                    terminal: self.terminal.clone(),
                };
                // A panicking middleware aborts the chain (same as the TS
                // reference). Continuing would turn a failed policy middleware
                // into an allow — fail-open is not acceptable for a kernel.
                match catch_unwind(AssertUnwindSafe(|| listener(&self.args, &next))) {
                    Ok(value) => value,
                    Err(payload) => std::panic::resume_unwind(payload),
                }
            }
            None => (self.terminal)(&self.args),
        }
    }

    /// Skip the remaining middlewares and run the built-in behavior.
    pub fn terminal(&self) -> Value {
        (self.terminal)(&self.args)
    }
}

/// The explicit continuation handed to every asynchronous middleware.
///
/// It is owned (cloned per call) so the futures returned by listeners are
/// `'static` and can be sent to any executor.
#[derive(Clone)]
pub struct AsyncWaterfallNext {
    remaining: Arc<[AsyncWaterfallListenerFn]>,
    args: Arc<[Value]>,
    terminal: AsyncWaterfallTerminal,
}

impl AsyncWaterfallNext {
    pub(crate) fn new(
        listeners: Vec<AsyncWaterfallListenerFn>,
        args: &[Value],
        terminal: AsyncWaterfallTerminal,
    ) -> Self {
        Self {
            remaining: Arc::from(listeners),
            args: Arc::from(args.to_vec()),
            terminal,
        }
    }

    /// The dispatch arguments (identical for every middleware).
    pub fn args(&self) -> &[Value] {
        &self.args
    }

    /// How many middlewares are left after the current one.
    pub fn remaining(&self) -> usize {
        self.remaining.len()
    }

    /// Run and await the rest of the chain, or the terminal behavior when
    /// nothing is left. Calling it more than once re-runs the remaining chain.
    pub fn next(&self) -> BoxFuture<'static, Value> {
        let this = self.clone();
        boxed(async move {
            match this.remaining.split_first() {
                Some((listener, rest)) => {
                    let next = AsyncWaterfallNext {
                        remaining: Arc::from(rest),
                        args: this.args.clone(),
                        terminal: this.terminal.clone(),
                    };
                    listener(&this.args, next).await
                }
                None => (this.terminal)(&this.args).await,
            }
        })
    }

    /// Skip the remaining middlewares and await the built-in behavior.
    pub fn terminal(&self) -> BoxFuture<'static, Value> {
        let this = self.clone();
        boxed(async move { (this.terminal)(&this.args).await })
    }
}

/// Wrap a closure as a [`WaterfallListenerFn`].
pub fn waterfall_listener<F>(listener: F) -> WaterfallListenerFn
where
    F: Fn(&[Value], &WaterfallNext) -> Value + Send + Sync + 'static,
{
    Arc::new(listener)
}

/// Wrap a closure as a [`WaterfallTerminal`].
pub fn waterfall_terminal<F>(terminal: F) -> WaterfallTerminal
where
    F: Fn(&[Value]) -> Value + Send + Sync + 'static,
{
    Arc::new(terminal)
}

/// Wrap a closure as an [`AsyncWaterfallListenerFn`].
pub fn async_waterfall_listener<F>(listener: F) -> AsyncWaterfallListenerFn
where
    F: Fn(&[Value], AsyncWaterfallNext) -> BoxFuture<'static, Value> + Send + Sync + 'static,
{
    Arc::new(listener)
}

/// Wrap an async closure as an [`AsyncWaterfallTerminal`].
pub fn async_waterfall_terminal<F, Fut>(terminal: F) -> AsyncWaterfallTerminal
where
    F: Fn(&[Value]) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Value> + Send + 'static,
{
    Arc::new(move |args| boxed(terminal(args)))
}

/// The default terminal: returns `Value::Null` when a chain has no built-in
/// behavior of its own.
pub fn null_waterfall_terminal() -> WaterfallTerminal {
    Arc::new(|_args| Value::Null)
}

/// The default async terminal: resolves to `Value::Null`.
pub fn null_async_waterfall_terminal() -> AsyncWaterfallTerminal {
    Arc::new(|_args| boxed(async { Value::Null }))
}
