//! Effect handles: cleanup callbacks and the single-shot disposer.

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

/// A cleanup callback. Sync by design: async cleanups use
/// [`AsyncCleanup`] / [`Context::effect_async`].
pub type Cleanup = Box<dyn FnOnce() + Send>;

/// The result of an effect body: zero or more cleanup callbacks.
pub type Effect = Vec<Cleanup>;

/// Wrap a closure as a [`Cleanup`].
pub fn cleanup<F>(callback: F) -> Cleanup
where
    F: FnOnce() + Send + 'static,
{
    Box::new(callback)
}

/// Handle to one effect. Calling it runs the effect's cleanup callbacks in
/// reverse order; repeated calls are no-ops.
#[derive(Clone)]
pub struct Disposer {
    inner: Arc<Mutex<Option<Vec<Cleanup>>>>,
}

impl Disposer {
    pub fn new(cleanups: Vec<Cleanup>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(cleanups))),
        }
    }

    pub fn dispose(&self) {
        let callbacks = self.inner.lock().ok().and_then(|mut slot| slot.take());
        let Some(mut callbacks) = callbacks else {
            return;
        };
        // CORE-011: a panicking cleanup must not skip the remaining cleanups.
        // Run every callback, remember the first panic, then re-raise it after
        // the list is drained (same policy as the async lane).
        let mut first_panic: Option<Box<dyn std::any::Any + Send>> = None;
        while let Some(callback) = callbacks.pop() {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(callback)) {
                first_panic.get_or_insert(payload);
            }
        }
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
    }

    pub fn is_disposed(&self) -> bool {
        self.inner.lock().map(|slot| slot.is_none()).unwrap_or(true)
    }
}

impl Drop for Disposer {
    fn drop(&mut self) {
        // Effects are normally disposed by their owner. Drop is a safety net
        // for effects that were detached (e.g. a failed activation).
        if Arc::strong_count(&self.inner) == 1 {
            // Drop must never unwind: a cleanup panic is swallowed and logged
            // here (explicit `dispose()` keeps the re-raise contract).
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.dispose())) {
                let message = payload
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "panic".to_string());
                tracing::error!(
                    target: "cordis::fiber",
                    "disposer cleanup panicked during drop: {message}"
                );
            }
        }
    }
}
