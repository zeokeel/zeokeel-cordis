//! L2 runtime defenses: capability introspection, per-fiber quotas and
//! watchdog/heartbeat support.
//!
//! The kernel cannot preempt or kill plugin code (Rust has no safe mechanism for
//! that in-process). What it *can* do is make failure visible and bounded:
//!
//! - [`IsolationCapabilities`] reports what this build can and cannot contain;
//! - [`FiberQuota`] bounds the resources one fiber may register (effects,
//!   listeners, child fibers) so a runaway plugin cannot exhaust the process;
//! - heartbeat/watchdog helpers ([`Fiber::heartbeat`], [`crate::Registry::stalled_fibers`])
//!   let an embedder supervisor observe long-running or stuck operations without
//!   the kernel ever force-killing a thread.
//!
//! See `docs/KERNEL-ISOLATION.md` for the full L1-L4 model.

use std::time::Duration;

/// What this build of the kernel can contain, and what it cannot.
///
/// This is intentionally honest: `panic_containment` is only true when the crate
/// was compiled with `panic = "unwind"` (the build otherwise fails to compile,
/// see the guard in `lib.rs`). Process/WASM isolation are embedder layers and are
/// always `false` here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsolationCapabilities {
    /// `"unwind"` or `"abort"` (the kernel only compiles under `"unwind"`).
    pub panic_strategy: &'static str,
    /// Whether unwinding panics from plugin code are contained.
    pub panic_containment: bool,
    /// Whether the kernel crate forbids `unsafe` (dependencies are not covered).
    pub unsafe_forbidden: bool,
    /// Process isolation is an embedder (L3) layer, never provided by the kernel.
    pub process_isolation: bool,
    /// WASM isolation is an embedder (L4) layer, never provided by the kernel.
    pub wasm_isolation: bool,
}

impl IsolationCapabilities {
    /// Detect the capabilities of the current build at runtime.
    pub const fn detect() -> Self {
        Self {
            panic_strategy: if cfg!(panic = "unwind") {
                "unwind"
            } else {
                "abort"
            },
            panic_containment: cfg!(panic = "unwind"),
            // `#![forbid(unsafe_code)]` is a crate-level attribute: if this
            // module compiles, the kernel itself has no `unsafe`.
            unsafe_forbidden: true,
            process_isolation: false,
            wasm_isolation: false,
        }
    }

    /// Human-readable summary for logs/startup banners.
    pub fn summary(&self) -> String {
        format!(
            "isolation: panic={} containment={} unsafe_forbidden={} process={} wasm={}",
            self.panic_strategy,
            self.panic_containment,
            self.unsafe_forbidden,
            self.process_isolation,
            self.wasm_isolation
        )
    }
}

/// Per-fiber resource limits. `None` means unlimited (the default).
///
/// Quotas are checked when a resource is registered, so a plugin that exceeds
/// them gets a [`crate::CordisError::QuotaExceeded`] error and its activation
/// rolls back through the normal failure path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FiberQuota {
    /// Maximum live effects owned by one fiber (includes listeners, child-fiber
    /// ownership effects and user effects).
    pub max_effects: Option<usize>,
    /// Maximum live event listeners registered on one fiber's behalf.
    pub max_listeners: Option<usize>,
    /// Maximum live child fibers loaded with `ctx.plugin`.
    pub max_child_fibers: Option<usize>,
}

impl FiberQuota {
    /// No limits.
    pub const fn unlimited() -> Self {
        Self {
            max_effects: None,
            max_listeners: None,
            max_child_fibers: None,
        }
    }

    /// Limit the number of live effects.
    pub const fn max_effects(mut self, limit: usize) -> Self {
        self.max_effects = Some(limit);
        self
    }

    /// Limit the number of live listeners.
    pub const fn max_listeners(mut self, limit: usize) -> Self {
        self.max_listeners = Some(limit);
        self
    }

    /// Limit the number of live child fibers.
    pub const fn max_child_fibers(mut self, limit: usize) -> Self {
        self.max_child_fibers = Some(limit);
        self
    }
}

/// One watchdog observation: a fiber that has an in-flight operation but has not
/// reported activity for at least the configured timeout.
#[derive(Clone)]
pub struct StalledFiber {
    /// The stalled fiber (cheap handle; clone-safe).
    pub fiber: crate::Fiber,
    /// How long it has been quiet.
    pub idle_for: Duration,
}

impl std::fmt::Debug for StalledFiber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StalledFiber")
            .field("id", &self.fiber.id())
            .field("name", &self.fiber.name())
            .field("idle_for", &self.idle_for)
            .finish()
    }
}

impl StalledFiber {
    /// Fiber id, for correlation with logs/metrics.
    pub fn id(&self) -> u64 {
        self.fiber.id()
    }

    /// Plugin name of the stalled fiber.
    pub fn name(&self) -> String {
        self.fiber.name()
    }
}
