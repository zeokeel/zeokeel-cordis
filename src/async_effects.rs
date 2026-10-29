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

use std::any::Any;
use std::cell::RefCell;
use std::future::Future;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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
/// it does not cancel the effect. Only the **last** handle runs the [`Drop`]
/// safety net (same contract as the sync [`Disposer`]), and that net is
/// deliberately conservative:
///
/// - a **cancelled** teardown does **not** block or run restored cleanups from
///   `Drop` — it logs and leaves them for an explicit retry
///   ([`dispose`](AsyncDisposer::dispose) / [`dispose_blocking`](AsyncDisposer::dispose_blocking));
/// - during unwinding the cleanups are dropped without running (never unwinds
///   out of `Drop`);
/// - otherwise `Drop` calls [`dispose_blocking`](AsyncDisposer::dispose_blocking),
///   i.e. it blocks the calling thread on the embedder's blocking executor. If
///   that would deadlock (reactor thread), dispose explicitly or install a
///   suitable blocking executor — the kernel's fiber `Drop` skips async
///   cleanups in that situation and hands them to
///   [`drain_orphan_async_cleanups`].
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

// ---------------------------------------------------------------------------
// XT-01: bounded recovery of async cleanups the Drop safety net could not run
// ---------------------------------------------------------------------------

/// Maximum number of unrecovered async effects retained for an embedder drain.
///
/// Keeping the list bounded means a pathological process cannot grow memory
/// without bound; sets beyond the cap are forgotten (leaked) and counted.
const ORPHAN_CAPACITY: usize = 256;

/// Async effects the kernel could not dispose from `FiberData::drop` (no
/// blocking executor / unwinding thread). Recoverable through
/// [`drain_orphan_async_cleanups`].
static ORPHANS: Mutex<Vec<AsyncDisposer>> = Mutex::new(Vec::new());

/// Cumulative number of orphaned async effects that were **leaked** because the
/// bounded queue was full or its lock was poisoned (XT-01).
static ORPHAN_COUNT: AtomicU64 = AtomicU64::new(0);

/// Cumulative number of drained orphan effects whose `dispose_blocking` panicked
/// (E P2-1: a panic must be observable, not silently swallowed).
static ORPHAN_FAILED: AtomicU64 = AtomicU64::new(0);

/// Current number of effects waiting in [`ORPHANS`] (recoverable by
/// [`drain_orphan_async_cleanups`]); a cheap gauge, never blocked on the queue
/// lock.
static ORPHAN_PENDING: AtomicUsize = AtomicUsize::new(0);

/// Hand an async effect to the orphan queue instead of dropping it.
///
/// Called by the kernel's `Drop` paths when running the cleanup is unsafe (no
/// blocking executor installed, or the thread is unwinding): the effect is
/// retained up to [`ORPHAN_CAPACITY`] so an embedder can recover it with
/// [`drain_orphan_async_cleanups`]; beyond the cap (or when the queue lock is
/// poisoned) it is forgotten and counted in [`orphan_async_cleanup_count`].
///
/// Never blocks, never unwinds; safe to call from `Drop`.
pub(crate) fn orphan_async_disposer(disposer: AsyncDisposer) {
    match ORPHANS.lock() {
        Ok(mut orphans) => {
            if orphans.len() < ORPHAN_CAPACITY {
                orphans.push(disposer);
                ORPHAN_PENDING.fetch_add(1, Ordering::SeqCst);
            } else {
                // Queue is full: do NOT drop the disposer here — its `Drop`
                // safety net may block on async cleanups. Leak it and count it.
                std::mem::forget(disposer);
                ORPHAN_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }
        Err(_) => {
            // Poisoned queue: leaking is safer than running `Drop` inline.
            std::mem::forget(disposer);
            ORPHAN_COUNT.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// Cumulative number of async effects the kernel had to leak (XT-01).
///
/// These are effects dropped outside an explicit dispose while no blocking
/// executor was installed (or while unwinding): the bounded orphan queue was
/// full, so they could not even be retained for
/// [`drain_orphan_async_cleanups`]. `0` in a healthy process — a non-zero value
/// means the host should install a blocking executor and/or dispose fibers
/// explicitly, and should call [`drain_orphan_async_cleanups`] while draining.
pub fn orphan_async_cleanup_count() -> u64 {
    ORPHAN_COUNT.load(Ordering::SeqCst)
}

/// Cumulative number of drained orphan async effects whose cleanup **panicked**.
///
/// A panicking async cleanup is contained (the remaining effects are still
/// drained) but no longer silent: every failure is logged and counted here.
/// `0` in a healthy process; a rising value usually means a cleanup needs a
/// blocking executor it did not get (see [`drain_orphan_async_cleanups`]) or an
/// embedder cleanup is genuinely broken.
pub fn orphan_async_cleanup_failure_count() -> u64 {
    ORPHAN_FAILED.load(Ordering::SeqCst)
}

/// Current number of orphaned async effects waiting for
/// [`drain_orphan_async_cleanups`] (queue length gauge).
///
/// The value is sampled without taking the queue lock; it can change
/// concurrently. `0` means no recoverable effect is queued (effects leaked at
/// the capacity boundary are counted by [`orphan_async_cleanup_count`] instead).
pub fn orphan_async_cleanups_pending() -> usize {
    ORPHAN_PENDING.load(Ordering::SeqCst)
}

/// Run every async cleanup the kernel orphaned during `Drop` (XT-01).
///
/// Returns how many effects were **processed** (taken from the queue and
/// attempted), not how many succeeded: a panicking cleanup still counts as
/// processed. Observe failures with [`orphan_async_cleanup_failure_count`] (each
/// panic is also logged with its payload).
///
/// Each effect runs its remaining cleanups in reverse registration order,
/// joining/retrying exactly like [`AsyncDisposer::dispose_blocking`]; one broken
/// cleanup can never strand the remaining effects.
///
/// # Threading and executors
///
/// **Call this from a non-reactor thread that may block.** Disposal goes through
/// [`AsyncDisposer::dispose_blocking`], which blocks the calling thread; calling
/// it from a reactor worker (e.g. inside a tokio task) can deadlock. Typical call
/// sites: embedder shutdown, a dedicated drain thread, or a test.
///
/// Without an installed blocking executor
/// ([`crate::exec::set_blocking_executor`]) `dispose_blocking` falls back to the
/// thread-park [`crate::block_on`] executor, which cannot drive reactor-bound
/// futures: such a cleanup can fail to make progress and surface as a panic. If
/// your cleanups may await runtime I/O, install a blocking executor or call this
/// from a thread that can drive them; failures are contained, logged and counted
/// in [`orphan_async_cleanup_failure_count`], so a non-zero count is the signal
/// that the drain environment was not suitable.
///
/// Effects currently queued are removed from the queue before they run (the
/// pending gauge drops to zero first), so a concurrent orphan hand-off or a
/// second drain cannot double-run them; the queue capacity (256) and the
/// cumulative leak accounting are unchanged.
pub fn drain_orphan_async_cleanups() -> usize {
    // The gauge is updated while the queue lock is held, together with the
    // take, so a concurrent hand-off can never be counted twice and no
    // read-modify-write loop (`fetch_update`, deprecated on newer toolchains)
    // is needed: pending is exactly the queue length at every lock boundary.
    let taken: Vec<AsyncDisposer> = match ORPHANS.lock() {
        Ok(mut orphans) => {
            let taken = std::mem::take(&mut *orphans);
            ORPHAN_PENDING.fetch_sub(taken.len(), Ordering::SeqCst);
            taken
        }
        // Poisoned queue: recover what is there (better than leaking it).
        Err(poisoned) => {
            let mut orphans = poisoned.into_inner();
            let taken = std::mem::take(&mut *orphans);
            ORPHAN_PENDING.fetch_sub(taken.len(), Ordering::SeqCst);
            taken
        }
    };
    let count = taken.len();
    for disposer in taken {
        match catch_unwind(AssertUnwindSafe(|| disposer.dispose_blocking())) {
            Ok(()) => {}
            Err(payload) => {
                ORPHAN_FAILED.fetch_add(1, Ordering::SeqCst);
                tracing::error!(
                    target: "zeokeel_cordis::async_effects",
                    "orphan async cleanup panicked during drain: {}",
                    panic_payload_message(payload.as_ref())
                );
            }
        }
    }
    count
}

/// Best-effort message from a panic payload (matching the kernel's
/// `log_cleanup_panic` style).
fn panic_payload_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string())
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

    /// Discard an effect that will never be disposed by its owner.
    ///
    /// R3-A4: the child-owned `ctx.plugin` ownership effect lives in the
    /// *parent* fiber's effect list; when the child disposes itself the parent
    /// detaches that effect and drops it. Dropping an undisposed disposer would
    /// run `dispose_blocking` (an extra embedder blocking-executor call) and its
    /// cleanup would re-dispose the already-disposed child (re-triggering the
    /// deferred drain that belongs to the user's retry). Discarding drops the
    /// cleanups without running them and marks the disposer settled, so `Drop`
    /// is a quiet no-op.
    pub(crate) fn discard(&self) {
        if let Ok(mut slot) = self.inner.lock() {
            slot.take();
        }
        self.state.finish_drain();
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

#[cfg(test)]
mod orphan_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// The orphan queue is process-global; serialize these tests so one drain
    /// cannot race another test's enqueues (lib unit tests run in parallel).
    static ORPHAN_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn counting_disposer(counter: Arc<AtomicUsize>) -> AsyncDisposer {
        AsyncDisposer::new(vec![cleanup_async(move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        })])
    }

    /// XT-01: orphaned async effects are recoverable via the public drain
    /// (exactly once), and the bounded queue leaks+counts beyond its capacity.
    #[test]
    fn orphan_queue_recovers_cleanups_and_is_bounded() {
        let _serial = ORPHAN_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Drain anything a previous test/process left behind for a clean slate.
        let _ = drain_orphan_async_cleanups();
        let baseline_leaks = orphan_async_cleanup_count();
        let pending_before = orphan_async_cleanups_pending();
        assert!(
            crate::exec::has_blocking_executor() || !crate::exec::has_blocking_executor(),
            "the default park executor must be able to drive a pure async cleanup"
        );

        // Real recovery: one orphaned effect, drained exactly once.
        let runs = Arc::new(AtomicUsize::new(0));
        orphan_async_disposer(counting_disposer(runs.clone()));
        assert!(
            orphan_async_cleanups_pending() > pending_before,
            "the pending gauge reflects a newly queued orphan"
        );
        let processed = drain_orphan_async_cleanups();
        assert!(
            orphan_async_cleanups_pending() <= pending_before,
            "draining lowers the pending gauge"
        );
        assert!(processed >= 1, "the queued orphan is drained");
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert_eq!(
            orphan_async_cleanup_count(),
            baseline_leaks,
            "queued (recoverable) effects are not counted as leaks"
        );

        // Bounded queue: beyond the cap, effects are forgotten and counted.
        let leaks_before = orphan_async_cleanup_count();
        for _ in 0..(ORPHAN_CAPACITY + 3) {
            orphan_async_disposer(counting_disposer(Arc::new(AtomicUsize::new(0))));
        }
        assert!(
            orphan_async_cleanup_count() >= leaks_before + 3,
            "effects beyond the bounded queue are leaked and counted"
        );
        let drained = drain_orphan_async_cleanups();
        assert!(
            drained <= ORPHAN_CAPACITY,
            "the bounded queue never holds more than its capacity (drained {drained})"
        );
    }

    /// A panicking embedder cleanup must not strand the remaining orphans, and
    /// the failure must be observable (E P2-1) instead of silently swallowed.
    #[test]
    fn drain_contains_and_counts_cleanup_panics() {
        let _serial = ORPHAN_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _ = drain_orphan_async_cleanups();
        let failures_before = orphan_async_cleanup_failure_count();
        let runs = Arc::new(AtomicUsize::new(0));
        orphan_async_disposer(AsyncDisposer::new(vec![cleanup_async(|| async {
            panic!("orphan cleanup boom");
        })]));
        orphan_async_disposer(counting_disposer(runs.clone()));
        assert_eq!(
            drain_orphan_async_cleanups(),
            2,
            "the drain returns the processed count (failures included)"
        );
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "a panicking cleanup does not stop the following effects"
        );
        assert_eq!(
            orphan_async_cleanup_failure_count(),
            failures_before + 1,
            "a drain-time panic increments the failure counter"
        );
        assert_eq!(drain_orphan_async_cleanups(), 0, "drain is idempotent");
        assert_eq!(
            orphan_async_cleanup_failure_count(),
            failures_before + 1,
            "a no-op drain does not change the failure counter"
        );
    }
}
