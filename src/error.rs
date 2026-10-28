//! Errors returned by the Cordis core.

use std::fmt;

use crate::config::ValidationError;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CordisError {
    /// Equivalent to `CordisError.Code.INACTIVE_EFFECT` in Cordis.
    InactiveEffect,
    DuplicateService(String),
    UnknownService(String),
    InvalidPlugin(String),
    PluginFailed(String),
    Disposed,
    /// Plugin config failed schema validation (Cordis `ValidationError`).
    Validation(ValidationError),
    /// `ctx.set()` was called for a service provided by another fiber.
    ServiceWriteForbidden(String),
    /// `ctx.set()` was called for a read-only computed accessor.
    AccessorReadOnly(String),
    /// A synchronous lifecycle API was used on an async plugin (async plugins
    /// must be activated/restarted through the `*_async` entry points).
    AsyncPluginNeedsAsync(String),
    /// A cooperative checkpoint observed that the fiber was cancelled.
    Cancelled,
    /// A per-fiber resource quota was exceeded (see `FiberQuota`).
    QuotaExceeded(String),
    /// Another thread is already running a lifecycle operation on this fiber
    /// (`dispose`/`restart`/`update` are mutually exclusive; kernel-grade
    /// stability prefers a safe error over a data race).
    LifecycleBusy,
    /// `ctx.set()`/`ctx.set_any()` tried to replace an existing service with a
    /// value of a different type. The message contains the service name (and,
    /// for diagnostics, the two type names when they are known).
    ServiceTypeMismatch(String),
    /// A `Registry::replace` rollback could not restore the previous state
    /// completely. The message contains the original reload error *and* the
    /// error raised by the rollback itself.
    Rollback(String),
    /// A plugin returned a **structured** error from its lifecycle callback
    /// (`Plugin::apply` / `AsyncPlugin::apply_async`). The kernel propagates
    /// this variant unchanged (it is never flattened into
    /// [`CordisError::PluginFailed`]), so embedders can match on `kind` and
    /// react programmatically.
    ///
    /// Construct it with [`CordisError::plugin`]:
    ///
    /// ```
    /// # use zeokeel_cordis::CordisError;
    /// let error = CordisError::plugin("my-plugin", "config", "missing api key");
    /// assert_eq!(error.to_string(), "plugin error: my-plugin (config): missing api key");
    /// ```
    Plugin {
        /// Plugin display name (`Plugin::name`).
        plugin: String,
        /// Machine-readable error kind chosen by the plugin (e.g. `"config"`,
        /// `"network"`, `"auth"`).
        kind: String,
        /// Human-readable message.
        message: String,
    },
    /// A computed accessor (`Context::accessor`) **rejected** a write: its
    /// setter returned `false`. Distinct from [`CordisError::ServiceWriteForbidden`]
    /// (another fiber owns the service) and [`CordisError::AccessorReadOnly`]
    /// (the accessor has no setter at all): the owner inspected the new value
    /// and refused it.
    AccessorWriteRejected(String),
}

impl fmt::Display for CordisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InactiveEffect => write!(f, "cannot create effect on inactive context"),
            Self::DuplicateService(name) => write!(f, "service already provided: {name}"),
            Self::UnknownService(name) => write!(f, "service not available: {name}"),
            Self::InvalidPlugin(message) => write!(f, "invalid plugin: {message}"),
            Self::PluginFailed(message) => write!(f, "plugin failed: {message}"),
            Self::Disposed => write!(f, "fiber already disposed"),
            Self::Validation(error) => write!(f, "{error}"),
            Self::ServiceWriteForbidden(name) => {
                write!(f, "service provided by another fiber: {name} (ctx.set)")
            }
            Self::AccessorReadOnly(name) => {
                write!(f, "computed accessor is read-only: {name} (ctx.set)")
            }
            Self::Cancelled => write!(f, "operation cancelled"),
            Self::QuotaExceeded(kind) => write!(f, "resource quota exceeded: {kind}"),
            Self::LifecycleBusy => {
                write!(f, "another lifecycle operation is already in progress")
            }
            Self::ServiceTypeMismatch(name) => {
                write!(f, "service type mismatch: {name}")
            }
            Self::Rollback(message) => {
                write!(f, "replace rollback failed: {message}")
            }
            Self::AccessorWriteRejected(name) => {
                write!(f, "computed accessor rejected the write: {name}")
            }
            Self::Plugin {
                plugin,
                kind,
                message,
            } => write!(f, "plugin error: {plugin} ({kind}): {message}"),
            Self::AsyncPluginNeedsAsync(name) => {
                write!(
                    f,
                    "plugin `{name}` implements AsyncPlugin: use plugin_async/restart_async/update_async"
                )
            }
        }
    }
}

impl CordisError {
    /// Build a structured plugin error (see [`CordisError::Plugin`]).
    ///
    /// Plugins should return this from [`crate::Plugin::apply`] /
    /// [`crate::AsyncPlugin::apply_async`] when they can classify their
    /// failure; the kernel propagates it unchanged through activation,
    /// `Fiber::await_settled`, restart/update and `Registry::replace`.
    pub fn plugin(
        plugin: impl Into<String>,
        kind: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::Plugin {
            plugin: plugin.into(),
            kind: kind.into(),
            message: message.into(),
        }
    }
}

impl std::error::Error for CordisError {}

pub type Result<T> = std::result::Result<T, CordisError>;
