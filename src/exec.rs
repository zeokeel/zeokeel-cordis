//! Minimal, dependency-free async plumbing for the kernel's synchronous edges.
//!
//! `cordis` deliberately does not depend on an async runtime: every public
//! async API takes plain [`std::future::Future`]s, so Tokio (or any other
//! runtime's) futures are accepted as long as the caller drives them inside
//! its own runtime. The helpers below only exist where the kernel itself has
//! to complete a future without an async context:
//!
//! - [`block_on`] drives one future to completion on the current thread; it
//!   backs `Fiber::dispose()` (the sync disposal path) and an
//!   `AsyncDisposer` dropped without `dispose_async()`;
//! - `join_all` polls several futures concurrently; it backs
//!   `Events::parallel_async`;
//! - `catch_panic` isolates a panicking cleanup so one failed resource
//!   release cannot skip the rest of the effect list.

use std::any::Any;
use std::future::{poll_fn, Future};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

/// A boxed, `Send` future with an explicit lifetime.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The payload carried by a caught panic.
pub(crate) type PanicPayload = Box<dyn Any + Send + 'static>;

/// Box an async block (or any future) into a [`BoxFuture`].
///
/// Handy for waterfall listeners, whose callback type must return a boxed
/// future:
///
/// ```
/// let listener = zeokeel_cordis::async_waterfall_listener(|_args, next| {
///     zeokeel_cordis::boxed(async move { next.next().await })
/// });
/// ```
pub fn boxed<'a, F>(future: F) -> BoxFuture<'a, F::Output>
where
    F: Future + Send + 'a,
{
    Box::pin(future)
}

/// Drive `future` to completion on the calling thread.
///
/// This is a deliberately tiny executor (a `Wake` implementation plus
/// park/unpark), not a general-purpose runtime: it makes progress only when
/// the future wakes its waker. Futures that need an external reactor (Tokio
/// timers, sockets, ...) only make progress here when a runtime is already
/// driving them; prefer `Fiber::dispose_async()` in async code and keep
/// [`block_on`] for the synchronous disposal edges.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let parker = Arc::new(Parker {
        thread: thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = Waker::from(parker.clone());
    let mut context = Context::from_waker(&waker);
    // `pin!` keeps the future on this stack frame; no unsafe code is needed.
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => parker.park(),
        }
    }
}

struct Parker {
    thread: Thread,
    notified: AtomicBool,
}

impl Parker {
    fn park(&self) {
        // `swap(false)` also consumes a wake that raced with the `Pending`
        // return, so no notification is ever lost.
        while !self.notified.swap(false, Ordering::AcqRel) {
            thread::park();
        }
    }
}

impl Wake for Parker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notified.store(true, Ordering::Release);
        self.thread.unpark();
    }
}

/// A future that yields once: the first poll returns `Pending` and wakes
/// itself, the second poll returns `Ready(())`.
///
/// Used by conformance tests to observe whether two listeners are driven
/// concurrently or strictly one after the other.
pub fn yield_now() -> impl Future<Output = ()> {
    struct YieldNow(bool);

    impl Future for YieldNow {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    YieldNow(false)
}

/// Poll every future to completion concurrently, keeping input order in the
/// result vector.
///
/// Each round polls every unfinished future once, in input order, so the
/// interleaving is deterministic for conformance tests while still allowing
/// genuine concurrency (one listener's pending future does not block another).
pub(crate) async fn join_all<T>(futures: Vec<BoxFuture<'static, T>>) -> Vec<T> {
    let mut futures: Vec<Option<BoxFuture<'static, T>>> = futures.into_iter().map(Some).collect();
    let mut results: Vec<Option<T>> = futures.iter().map(|_| None).collect();
    poll_fn(|cx| {
        let mut pending = false;
        for (slot, result) in futures.iter_mut().zip(results.iter_mut()) {
            if result.is_some() {
                continue;
            }
            let polled = slot.as_mut().map(|future| future.as_mut().poll(cx));
            match polled {
                Some(Poll::Ready(value)) => {
                    *result = Some(value);
                    slot.take();
                }
                Some(Poll::Pending) => pending = true,
                None => {}
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
    results
        .into_iter()
        .map(|result| result.expect("join_all resolves every slot before returning"))
        .collect()
}

/// Await `future`, catching a panic raised while polling it.
///
/// The wrapped future is boxed so the wrapper stays `Unpin` and can be polled
/// without unsafe code (the crate is `#![forbid(unsafe_code)]`).
pub(crate) async fn catch_panic<T>(future: BoxFuture<'static, T>) -> Result<T, PanicPayload> {
    struct CatchUnwind<T> {
        inner: BoxFuture<'static, T>,
    }

    impl<T> Future for CatchUnwind<T> {
        type Output = Result<T, PanicPayload>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            // `Pin<Box<dyn Future>>` is always `Unpin`, so the wrapper is too.
            let this = self.get_mut();
            match catch_unwind(AssertUnwindSafe(|| this.inner.as_mut().poll(cx))) {
                Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
                Ok(Poll::Pending) => Poll::Pending,
                Err(payload) => Poll::Ready(Err(payload)),
            }
        }
    }

    CatchUnwind { inner: future }.await
}

// ---------------------------------------------------------------------------
// Embedder-provided blocking executor
// ---------------------------------------------------------------------------

/// Executor used by the kernel's **synchronous** disposal edges
/// (`AsyncDisposer::dispose_blocking`, `Fiber::dispose`, `Fiber::restart`, …).
///
/// The kernel itself is runtime-agnostic and its default executor is the
/// thread-park [`block_on`], which can only drive futures that wake themselves
/// (it cannot advance reactor-backed I/O on its own). Async embedders such as a
/// Tokio server should install an executor that fits their runtime, and may use
/// it to fail loudly when a synchronous disposal happens on an async worker
/// thread (Cordis `blocking executor` extension; not part of the JS reference).
pub trait BlockingExecutor: Send + Sync {
    /// Run `future` to completion on the calling thread.
    fn block_on(&self, future: BoxFuture<'static, ()>);
}

static BLOCKING_EXECUTOR: OnceLock<Arc<dyn BlockingExecutor>> = OnceLock::new();

/// Install the executor used by synchronous disposal paths.
///
/// Returns `false` when an executor was already installed (the first one wins,
/// so library crates cannot silently replace their host's choice).
pub fn set_blocking_executor(executor: Arc<dyn BlockingExecutor>) -> bool {
    BLOCKING_EXECUTOR.set(executor).is_ok()
}

/// Run a unit future on the embedder-provided executor, or on the default
/// thread-park [`block_on`] when none was installed.
pub(crate) fn block_on_unit(future: BoxFuture<'static, ()>) {
    match BLOCKING_EXECUTOR.get() {
        Some(executor) => executor.block_on(future),
        None => block_on(future),
    }
}
