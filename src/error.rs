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
    /// Another thread is already running a lifecycle operation on this fiber
    /// (`dispose`/`restart`/`update` are mutually exclusive; kernel-grade
    /// stability prefers a safe error over a data race).
    LifecycleBusy,
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
            Self::LifecycleBusy => {
                write!(f, "another lifecycle operation is already in progress")
            }
            Self::AsyncPluginNeedsAsync(name) => {
                write!(
                    f,
                    "plugin `{name}` implements AsyncPlugin: use plugin_async/restart_async/update_async"
                )
            }
        }
    }
}

impl std::error::Error for CordisError {}

pub type Result<T> = std::result::Result<T, CordisError>;
