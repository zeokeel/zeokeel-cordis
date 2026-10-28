//! Async effects and deterministic async disposal.
//!
//! Cordis allows an effect body to return a promise and an effect cleanup to be
//! async; unloading the fiber awaits those disposers. The Rust port keeps the
//! same ownership model (a fiber owns one ordered effect list) and represents
//! an async cleanup as a boxed future factory:
//!
//! ```no_run
//! # use zeokeel_cordis::{Context, cleanup_async};
//! # fn demo(ctx: &Context) -> zeokeel_cordis::Result<()> {
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

use std::cell::RefCell;
use std::future::Future;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

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

thread_local! {
    /// Async effects currently being drained on this thread (identified by the
    /// address of their [`DisposeState`]).
    ///
    /// A cleanup that reaches its own disposer (`dispose_blocking` *or* an
    /// `await`ed `dispose`) must not wait for the teardown it is itself part
    /// of. The drain frame is entered for the duration of every poll of a
    /// cleanup future, so nested calls see it without relying on thread
    /// identity: several async tasks share a worker thread, so "same thread"
    /// is *not* a reliable self-join signal (that would break joiners that
    /// happen to be scheduled on the drainer's thread).
    static DRAIN_FRAMES: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

/// RAII drain frame of one async effect on this thread.
struct DrainFrame {
    key: usize,
}

impl DrainFrame {
    fn enter(key: usize) -> Self {
        DRAIN_FRAMES.with(|frames| frames.borrow_mut().push(key));
        Self { key }
    }
}

impl Drop for DrainFrame {
    fn drop(&mut self) {
        DRAIN_FRAMES.with(|frames| {
            let mut frames = frames.borrow_mut();
            if let Some(index) = frames.iter().rposition(|key| *key == self.key) {
                frames.remove(index);
            }
        });
    }
}

/// Whether this thread is currently polling a cleanup of the effect `key`.
fn draining_on_current_thread(key: usize) -> bool {
    DRAIN_FRAMES.with(|frames| frames.borrow().contains(&key))
}

/// Future wrapper that enters the drain frame around every poll of a cleanup
/// future, so a nested `dispose`/`dispose_blocking` of the same effect during
/// that poll detects a self-join (S-2 / R2-1).
struct DrainScopeFuture<T> {
    key: usize,
    inner: crate::exec::BoxFuture<'static, T>,
}

impl<T> Future for DrainScopeFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let key = self.key;
        let _frame = DrainFrame::enter(key);
        // `BoxFuture` is `Unpin`, so the wrapper is too.
        self.get_mut().inner.as_mut().poll(cx)
    }
}

/// Join state shared by every handle of one async effect.
///
/// A second `dispose()` (or a concurrent disposer) waits until the first one
/// finished instead of returning early while cleanups are still in flight
/// (Cordis joins in-flight teardown).
///
/// The `claimed` bit is the teardown ownership token: exactly one caller drains
/// the ordered cleanup list, everybody else joins through [`AsyncDisposer::wait_done`].
/// It also makes a *cancelled* teardown retryable (S-1): cancellation restores
/// the unstarted cleanups and marks the state settled/cancelled, and the next
/// `dispose`/`dispose_blocking` claims the disposer again to run them.
#[derive(Default)]
struct DisposeState {
    done: AtomicBool,
    cancelled: AtomicBool,
    /// Set while one caller owns the ordered teardown.
    claimed: AtomicBool,
    waiters: Mutex<Vec<Waker>>,
}

impl DisposeState {
    /// True only when the ordered teardown completed and no retry is running.
    fn is_settled(&self) -> bool {
        self.done.load(Ordering::SeqCst) && !self.claimed.load(Ordering::SeqCst)
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Take ownership of the teardown. Exactly one caller wins; the winner runs
    /// the drain, the losers join it. A claim starts a *fresh* attempt, so
    /// `done` and the cancelled flag are cleared: a cancelled teardown left the
    /// restored cleanups in the slot for exactly this retry (S-1).
    fn claim(&self) -> bool {
        if self
            .claimed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return false;
        }
        self.done.store(false, Ordering::SeqCst);
        self.cancelled.store(false, Ordering::SeqCst);
        true
    }

    /// Release the claim and wake every joiner.
    fn release(&self) {
        self.claimed.store(false, Ordering::SeqCst);
        self.wake_waiters();
    }

    /// Complete the claimed teardown: settled, nothing left to retry.
    fn finish_drain(&self) {
        self.done.store(true, Ordering::SeqCst);
        self.cancelled.store(false, Ordering::SeqCst);
        self.release();
    }

    /// A cancelled teardown must not run cleanups from `Drop`: mark the state
    /// cancelled, release the claim and wake joiners (the restored cleanups
    /// belong to a retry, S-1).
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.done.store(true, Ordering::SeqCst);
        self.release();
    }

    /// Stable identity of this effect (the `DisposeState` allocation address),
    /// used as the [`DRAIN_FRAMES`] key.
    fn key(&self) -> usize {
        self as *const Self as usize
    }

    /// Whether the **current poll stack** is inside a cleanup of this disposer
    /// (S-2/R2-1: waiting for a teardown we are part of can never complete).
    ///
    /// This is a drain-frame check rather than a thread-id check: several async
    /// tasks may share one worker thread, and such a *joiner* must still wait.
    fn drained_by_current_thread(&self) -> bool {
        draining_on_current_thread(self.key())
    }

    fn wake_waiters(&self) {
        let waiters = self
            .waiters
            .lock()
            .map(|mut waiters| std::mem::take(&mut *waiters))
            .unwrap_or_default();
        for waker in waiters {
            waker.wake();
        }
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
                target: "zeokeel_cordis::fiber",
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
    ///
    /// If the returned future is dropped mid-teardown, the cleanups that had not
    /// started are restored and this method (or [`AsyncDisposer::dispose_blocking`])
    /// retries them on the next call; a concurrent caller joins the in-flight
    /// teardown instead of reporting completion early. A cleanup that `await`s
    /// `dispose()` on its **own** disposer returns immediately instead of
    /// waiting for the teardown it is part of (both this async path and
    /// [`AsyncDisposer::dispose_blocking`] short-circuit self-joins, which can
    /// never complete).
    pub async fn dispose(&self) {
        loop {
            if !self.state.claim() {
                // R2-1: a cleanup may `await` its own disposer's `dispose()`.
                // The claim failed because *this thread* owns the drain already:
                // waiting for ourselves can never complete, so return — the
                // outer drain finishes the teardown (0.4.1 had no deadlock
                // guard for the async path either; keep both mirrors symmetric
                // with `dispose_blocking`'s self-join shortcut).
                if self.state.drained_by_current_thread() {
                    return;
                }
                // Another caller owns the in-flight teardown: join it instead
                // of reporting completion early. If that attempt was cancelled,
                // its restored cleanups are ours to retry (S-1).
                self.wait_done().await;
                if !self.state.is_cancelled() {
                    return;
                }
                continue;
            }
            // Claimed: this call runs the ordered teardown.
            let key = self.state.key();
            let Some(cleanups) = self.take() else {
                // Nothing left to run (created empty / already drained).
                self.state.finish_drain();
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
                    Ok(future) => {
                        catch_panic(crate::exec::boxed(DrainScopeFuture { key, inner: future }))
                            .await
                    }
                    Err(payload) => Err(payload),
                };
                if let Err(payload) = outcome {
                    drain.panic.get_or_insert(payload);
                }
            }
            drain.armed = false;
            self.state.finish_drain();
            if let Some(payload) = drain.panic.take() {
                resume_unwind(payload);
            }
            return;
        }
    }

    async fn wait_done(&self) {
        std::future::poll_fn(|cx| {
            if self.state.is_settled() {
                return Poll::Ready(());
            }
            if let Ok(mut waiters) = self.state.waiters.lock() {
                waiters.push(cx.waker().clone());
            }
            if self.state.is_settled() {
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
    /// [`Drop`] safety net. It *joins* an in-flight teardown owned by another
    /// caller — returning early while cleanups are still running would break
    /// the "blocks until every cleanup finished" contract — and it is a no-op
    /// only once the ordered teardown actually completed.
    ///
    /// Two reentrancy rules complete the contract: a teardown that was
    /// *cancelled* (its future dropped mid-drain) is retried here, because its
    /// unstarted cleanups were restored and still own resources; and a cleanup
    /// that calls `dispose_blocking` on its **own** disposer returns immediately
    /// instead of waiting for the teardown it is part of.
    ///
    /// It honors the embedder-provided blocking executor
    /// ([`crate::exec::set_blocking_executor`]); the default is the
    /// thread-park executor of [`crate::block_on`], which cannot drive
    /// reactor-backed futures. Async hosts should install a suitable executor
    /// or prefer [`AsyncDisposer::dispose`] / [`crate::Fiber::dispose_async`].
    pub fn dispose_blocking(&self) {
        // S-2: a cleanup may call `dispose_blocking()` on its own disposer.
        // Waiting for a teardown this thread is itself part of cannot complete
        // (and 0.4.1 returned immediately here), so it is a no-op.
        if self.state.drained_by_current_thread() {
            return;
        }
        // A *completed* teardown is a no-op; a *cancelled* one is not complete:
        // its unstarted cleanups were restored and must be retried (S-1).
        // Joining/in-flight teardowns are handled by `dispose()` itself.
        if self.state.is_settled() && !self.state.is_cancelled() {
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
                target: "zeokeel_cordis::fiber",
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
                target: "zeokeel_cordis::fiber",
                "async effect dropped during unwinding; its cleanups were not run"
            );
            return;
        }
        // A cleanup panic must not escape Drop; `dispose_blocking` re-raises.
        let _ = catch_unwind(AssertUnwindSafe(|| self.dispose_blocking()));
    }
}
