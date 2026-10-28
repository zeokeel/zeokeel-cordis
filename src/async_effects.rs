//! Async effects and deterministic async disposal.
//!
//! Cordis allows an effect body to return a promise and an effect cleanup to be
//! async; unloading the fiber awaits those disposers. The Rust port keeps the
//! same ownership model (a fiber owns one ordered effect list) and represents
//! an async cleanup as a boxed future factory:
//!
//! ```no_run
//! # use cordis::{Context, cleanup_async};
//! # fn demo(ctx: &Context) -> cordis::Result<()> {
//! ctx.effect_async(|| {
//!     vec![cleanup_async(|| async {
//!         // release the resource; the future resolves when it is gone
//!     })]
//! })?;
//! # Ok(())
//! # }
//! ```
//!
//! Disposal is *deterministic*: cleanups run one at a time in reverse
//! registration order, and the caller observes completion (via
//! [`AsyncDisposer::dispose`], `Fiber::dispose_async`, or the blocking mirror
//! [`AsyncDisposer::dispose_blocking`]). Nothing is spawned or fire-and-forget.

use std::future::Future;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};

use crate::exec::{boxed, catch_panic, BoxFuture, PanicPayload};

/// An async cleanup callback: the returned future resolves once the resource
/// has been released.
pub type AsyncCleanup = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;

/// The result of an async effect body: zero or more async cleanup callbacks.
pub type AsyncEffect = Vec<AsyncCleanup>;

/// Wrap an async closure as an [`AsyncCleanup`].
pub fn cleanup_async<F, Fut>(callback: F) -> AsyncCleanup
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Box::new(move || boxed(callback()))
}

/// Handle to one async effect.
///
/// Calling [`dispose`](AsyncDisposer::dispose) awaits the effect's cleanup
/// callbacks in reverse order exactly once; repeated calls are no-ops. The
/// fiber that owns the effect also holds a clone of this handle, so dropping
/// it does not cancel the effect (only an unreferenced handle, e.g. a detached
/// effect, is drained on drop — same contract as the sync [`Disposer`]).
///
/// [`Disposer`]: crate::Disposer
#[derive(Clone)]
pub struct AsyncDisposer {
    inner: Arc<Mutex<Option<Vec<AsyncCleanup>>>>,
    state: Arc<DisposeState>,
}

/// Join state shared by every handle of one async effect.
///
/// A second `dispose()` (or a concurrent disposer) waits until the first one
/// finished instead of returning early while cleanups are still in flight
/// (Cordis joins in-flight teardown).
#[derive(Default)]
struct DisposeState {
    done: AtomicBool,
    cancelled: AtomicBool,
    waiters: Mutex<Vec<Waker>>,
}

impl DisposeState {
    fn mark_done(&self) {
        self.done.store(true, Ordering::SeqCst);
        let waiters = self
            .waiters
            .lock()
            .map(|mut waiters| std::mem::take(&mut *waiters))
            .unwrap_or_default();
        for waker in waiters {
            waker.wake();
        }
    }

    fn is_done(&self) -> bool {
        self.done.load(Ordering::SeqCst)
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    fn reset_cancelled(&self) {
        self.cancelled.store(false, Ordering::SeqCst);
    }

    /// A cancelled teardown must not run cleanups from `Drop`: mark the state
    /// cancelled and release joiners (the restored cleanups belong to a retry).
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.mark_done();
    }
}

/// Cancel-safe drain state: unstarted cleanups are restored to the disposer
/// when the disposing future is dropped mid-await.
struct CancelSafeDrain<'a> {
    disposer: &'a AsyncDisposer,
    remaining: Vec<AsyncCleanup>,
    panic: Option<PanicPayload>,
    armed: bool,
}

impl Drop for CancelSafeDrain<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if !self.remaining.is_empty() {
            if let Ok(mut slot) = self.disposer.inner.lock() {
                if slot.is_none() {
                    *slot = Some(std::mem::take(&mut self.remaining));
                }
            }
            tracing::error!(
                target: "cordis::fiber",
                "async disposer cancelled mid-teardown; unstarted cleanups were restored"
            );
        }
        // Cancellation is a pure restore: never run cleanups from Drop (it may
        // happen on an async worker / outside a runtime). Wake joiners.
        self.disposer.state.cancel();
    }
}

impl AsyncDisposer {
    pub fn new(cleanups: Vec<AsyncCleanup>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(cleanups))),
            state: Arc::new(DisposeState::default()),
        }
    }

    /// Await every cleanup in reverse registration order.
    ///
    /// The cleanups are awaited sequentially, never concurrently, so the
    /// teardown order is deterministic. A panicking cleanup does not skip the
    /// remaining ones: the panic is captured, the rest of the list still runs,
    /// and the first captured panic is resumed once the list is drained.
    pub async fn dispose(&self) {
        {
            self.state.reset_cancelled();
            let Some(cleanups) = self.take() else {
                // Either already disposed or another caller owns the in-flight
                // teardown: join it instead of reporting completion early.
                self.wait_done().await;
                return;
            };
            // Cancel-safe drain: if this future is dropped mid-await, the
            // cleanups that have not started yet are put back so a later
            // dispose retries them (reverse order preserved). The cleanup
            // future that was in flight cannot be recovered (Rust has no
            // cancel-safe await), which is logged by the guard.
            let mut drain = CancelSafeDrain {
                disposer: self,
                remaining: cleanups,
                panic: None,
                armed: true,
            };
            while let Some(cleanup) = drain.remaining.pop() {
                let outcome = match catch_unwind(AssertUnwindSafe(cleanup)) {
                    Ok(future) => catch_panic(future).await,
                    Err(payload) => Err(payload),
                };
                if let Err(payload) = outcome {
                    drain.panic.get_or_insert(payload);
                }
            }
            drain.armed = false;
            self.state.mark_done();
            if let Some(payload) = drain.panic.take() {
                resume_unwind(payload);
            }
        }
    }

    async fn wait_done(&self) {
        std::future::poll_fn(|cx| {
            if self.state.is_done() {
                return Poll::Ready(());
            }
            if let Ok(mut waiters) = self.state.waiters.lock() {
                waiters.push(cx.waker().clone());
            }
            if self.state.is_done() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }

    /// Block the calling thread until every cleanup has finished.
    ///
    /// This is the sync-disposal mirror used by `Fiber::dispose()` and by the
    /// [`Drop`] safety net. It honors the embedder-provided blocking executor
    /// ([`crate::exec::set_blocking_executor`]); the default is the
    /// thread-park executor of [`crate::block_on`], which cannot drive
    /// reactor-backed futures. Async hosts should install a suitable executor
    /// or prefer [`AsyncDisposer::dispose`] / [`crate::Fiber::dispose_async`].
    pub fn dispose_blocking(&self) {
        if self.is_disposed() {
            return;
        }
        let this = self.clone();
        crate::exec::block_on_unit(crate::exec::boxed(async move { this.dispose().await }));
    }

    pub fn is_disposed(&self) -> bool {
        self.inner.lock().map(|slot| slot.is_none()).unwrap_or(true)
    }

    fn take(&self) -> Option<Vec<AsyncCleanup>> {
        self.inner.lock().ok().and_then(|mut slot| slot.take())
    }
}

impl Drop for AsyncDisposer {
    fn drop(&mut self) {
        // A detached effect (its fiber no longer holds a reference) must not
        // leak its resources. Effects owned by a fiber are disposed by the
        // fiber, in the fiber's reverse registration order.
        if self.state.is_cancelled() {
            // A cancelled teardown restored the unstarted cleanups; running
            // them from Drop would block an async worker or run outside the
            // runtime. A later explicit dispose retries them.
            tracing::error!(
                target: "cordis::fiber",
                "cancelled async effect dropped; restored cleanups were discarded"
            );
            return;
        }
        if Arc::strong_count(&self.inner) != 1 || self.is_disposed() {
            return;
        }
        // Drop must never block or panic while the thread is already
        // unwinding: park the cleanups (they need an async context) and log.
        if std::thread::panicking() {
            let _ = self.inner.lock().map(|mut slot| slot.take());
            tracing::error!(
                target: "cordis::fiber",
                "async effect dropped during unwinding; its cleanups were not run"
            );
            return;
        }
        // A cleanup panic must not escape Drop; `dispose_blocking` re-raises.
        let _ = catch_unwind(AssertUnwindSafe(|| self.dispose_blocking()));
    }
}
