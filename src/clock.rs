//! Injectable clock for the kernel's time-consuming facilities.
//!
//! The kernel itself never reads the wall clock for policy: it only *records*
//! time (log timestamps, watchdog activity) and compares monotonic values. Those
//! are the points where an embedder may want deterministic time (tests, replay,
//! simulated supervisors), so they go through a [`Clock`] instead of calling
//! [`std::time`] directly.
//!
//! # Contract
//!
//! - The clock is **per context tree**: [`Context::set_clock`] replaces it for the
//!   tree rooted at the context it is called on (the root context), child
//!   contexts share the same handle.
//! - It is meant to be installed **once, right after [`Context::root`]**, before
//!   any plugin loads. Swapping clocks at runtime is allowed but discouraged:
//!   `now_monotonic_ms` must come from one monotonic source, otherwise idle
//!   durations jump (the kernel does not hide that).
//! - Only *new* observations use a newly installed clock; already recorded
//!   timestamps are never rewritten.
//!
//! [`Context::root`]: crate::Context::root
//! [`Context::set_clock`]: crate::Context::set_clock

use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Time source used by the kernel's logger timestamps and L2 activity tracking.
///
/// Implementations must be cheap and non-blocking; the kernel calls them on hot
/// paths (one record per log line) and from contained contexts.
pub trait Clock: Send + Sync {
    /// Wall-clock time in milliseconds since the Unix epoch.
    ///
    /// Used for [`crate::LogRecord::ts_unix_ms`]. A clock may freeze or shift
    /// this freely; nothing in the kernel reasons about wall-clock deltas.
    fn now_unix_ms(&self) -> u64;

    /// Monotonic time in milliseconds (arbitrary origin, never decreasing).
    ///
    /// Used for idle/duration measurements, so it must not jump backwards and
    /// should keep advancing while the process is alive.
    fn now_monotonic_ms(&self) -> u64;
}

/// The default [`Clock`]: wall clock for Unix time, process-local monotonic
/// counter for durations.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SystemClock;

/// Process-local monotonic origin: consistent across every `SystemClock`
/// instance, so swapping a `SystemClock` for another does not reset durations.
fn monotonic_origin() -> Instant {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    *ORIGIN.get_or_init(Instant::now)
}

impl Clock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as u64)
            .unwrap_or(0)
    }

    fn now_monotonic_ms(&self) -> u64 {
        monotonic_origin().elapsed().as_millis() as u64
    }
}
