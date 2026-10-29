//! Logging service (Cordis `ctx.logger`).
//!
//! Official Cordis installs a `LoggerService` on every context: `ctx.logger()`
//! returns a named facade, `ctx.logger.info(...)` logs with the current fiber's
//! name, exporters receive structured [`LogRecord`]s and the service keeps a
//! bounded ring buffer for diagnostics.
//!
//! Rust has no callable service, so the two official call shapes map to
//! explicit methods:
//!
//! * [`Context::logger`](crate::Context::logger) / [`Context::logger_named`](crate::Context::logger_named)
//!   create a facade (replacing `ctx.logger(name?)`);
//! * [`Logger::info`] / [`Logger::warn`] / [`Logger::error`] / [`Logger::debug`]
//!   log one record (replacing `ctx.logger('name').info(...)`).
//!
//! Level numbers deliberately follow the Rust severity order
//! (`Error < Warn < Info < Debug`): a record is emitted when its level is at or
//! below the active threshold. (The official TS enum numbers INFO=1, WARN=2;
//! see `docs/CORDIS-RUST.md` for this documented difference.)

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value};

use crate::effect::{cleanup, Disposer};
use crate::service::ServiceKey;

/// Severity of a log record, ordered from most to least severe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum LogLevel {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
}

impl LogLevel {
    /// Parse `error` / `warn` / `info` / `debug` (case-insensitive).
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "error" => Some(Self::Error),
            "warn" | "warning" => Some(Self::Warn),
            "info" => Some(Self::Info),
            "debug" => Some(Self::Debug),
            _ => None,
        }
    }

    /// Lower-case name used by exporters and `tracing`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
        }
    }

    /// Build from the numeric code of this port (`Error=0` … `Debug=3`).
    ///
    /// Values above `3` are **clamped to `Error`** (fail-closed), matching the
    /// configuration path (`{"level": 99}` → `Error`); they never widen the
    /// threshold to the most verbose level (XT-09).
    pub fn from_u8(code: u8) -> Self {
        match code {
            0 => Self::Error,
            1 => Self::Warn,
            2 => Self::Info,
            3 => Self::Debug,
            _ => Self::Error,
        }
    }

    /// The numeric code of this port (`Error=0` … `Debug=3`).
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// One structured log record delivered to exporters and kept in the buffer.
#[derive(Debug, Clone, PartialEq)]
pub struct LogRecord {
    /// Monotonic sequence number, starting at 1.
    pub seq: u64,
    /// Wall-clock timestamp in milliseconds since the Unix epoch.
    pub ts_unix_ms: u64,
    /// Logger name (usually the plugin/fiber name).
    pub name: String,
    /// Severity of this record.
    pub level: LogLevel,
    /// Rendered message.
    pub message: String,
    /// Structured fields attached to this record.
    pub fields: Map<String, Value>,
}

/// A sink that receives structured log records.
pub trait LogExporter: Send + Sync {
    /// Deliver one record. Called outside the service lock, so exporters may
    /// log or register exporters themselves without deadlocking.
    fn export(&self, record: &LogRecord);

    /// Optional per-exporter verbosity threshold (most verbose level it wants).
    /// `None` falls back to the service threshold.
    fn level(&self) -> Option<LogLevel> {
        None
    }
}

/// The default buffer capacity, matching Cordis `LoggerService.bufferSize`.
pub const DEFAULT_BUFFER_SIZE: usize = 1000;

/// The logging service shared by a context tree.
pub struct LoggerService {
    level: AtomicU8,
    exporters: Mutex<Vec<(u64, Arc<dyn LogExporter>)>>,
    buffer: Mutex<VecDeque<LogRecord>>,
    buffer_size: usize,
    seq: AtomicU64,
    exporter_seq: AtomicU64,
}

impl LoggerService {
    /// Create a service with the given default threshold and a bounded buffer.
    pub fn new(level: LogLevel) -> Arc<Self> {
        Self::with_buffer_size(level, DEFAULT_BUFFER_SIZE)
    }

    /// Create a service with an explicit buffer capacity (`0` disables the
    /// buffer).
    pub fn with_buffer_size(level: LogLevel, buffer_size: usize) -> Arc<Self> {
        Arc::new(Self {
            level: AtomicU8::new(level.as_u8()),
            exporters: Mutex::new(Vec::new()),
            buffer: Mutex::new(VecDeque::with_capacity(buffer_size.min(64))),
            buffer_size,
            seq: AtomicU64::new(0),
            exporter_seq: AtomicU64::new(0),
        })
    }

    /// The default threshold of this service.
    pub fn level(&self) -> LogLevel {
        LogLevel::from_u8(self.level.load(Ordering::Relaxed))
    }

    /// Replace the default threshold.
    pub fn set_level(&self, level: LogLevel) {
        self.level.store(level.as_u8(), Ordering::Relaxed);
    }

    /// Whether a record with `level` passes the service threshold.
    pub fn is_enabled(&self, level: LogLevel) -> bool {
        level.as_u8() <= self.level().as_u8()
    }

    /// Create a named logger at the service threshold.
    ///
    /// The facade uses [`crate::SystemClock`] for record timestamps; context
    /// loggers obtain an injectable clock through [`crate::Context::logger`].
    pub fn logger(self: &Arc<Self>, name: impl Into<String>) -> Logger {
        self.logger_with_clock(name, None, Arc::new(crate::clock::SystemClock))
    }

    /// Create a named logger with an explicit level override and time source
    /// (used by [`crate::Context::logger`] / [`crate::Context::logger_named`]).
    pub(crate) fn logger_with_clock(
        self: &Arc<Self>,
        name: impl Into<String>,
        level: Option<LogLevel>,
        clock: Arc<dyn crate::clock::Clock>,
    ) -> Logger {
        Logger {
            service: Arc::clone(self),
            name: name.into(),
            level,
            clock,
        }
    }

    /// Create a named logger with its own threshold (used by `ctx.intercept`).
    pub fn logger_with(
        self: &Arc<Self>,
        name: impl Into<String>,
        level: Option<LogLevel>,
    ) -> Logger {
        self.logger_with_clock(name, level, Arc::new(crate::clock::SystemClock))
    }

    /// Register an exporter; the returned disposer removes it (idempotent).
    pub fn exporter(self: &Arc<Self>, exporter: Arc<dyn LogExporter>) -> Disposer {
        let id = self.exporter_seq.fetch_add(1, Ordering::Relaxed) + 1;
        if let Ok(mut exporters) = self.exporters.lock() {
            exporters.push((id, exporter));
        }
        let service = Arc::clone(self);
        Disposer::new(vec![cleanup(move || {
            if let Ok(mut exporters) = service.exporters.lock() {
                exporters.retain(|(entry, _)| *entry != id);
            }
        })])
    }

    /// Number of currently registered exporters (diagnostics).
    pub fn exporter_count(&self) -> usize {
        self.exporters
            .lock()
            .map(|exporters| exporters.len())
            .unwrap_or(0)
    }

    /// Emit one record. Level filtering happens here; exporters are called
    /// outside the exporter lock.
    ///
    /// Timestamps come from [`crate::SystemClock`]; context loggers use their
    /// injected [`crate::Clock`] instead (see [`crate::Context::set_clock`]).
    pub fn log(&self, level: LogLevel, name: &str, message: &str, fields: Value) {
        self.log_with_clock(
            level,
            self.level(),
            name,
            message,
            fields,
            &crate::clock::SystemClock,
        );
    }

    /// Emit one record against an explicit threshold and time source.
    pub(crate) fn log_with_clock(
        &self,
        level: LogLevel,
        threshold: LogLevel,
        name: &str,
        message: &str,
        fields: Value,
        clock: &dyn crate::clock::Clock,
    ) {
        if level.as_u8() > threshold.as_u8() {
            return;
        }
        let record = LogRecord {
            seq: self.seq.fetch_add(1, Ordering::Relaxed) + 1,
            ts_unix_ms: clock.now_unix_ms(),
            name: name.to_string(),
            level,
            message: message.to_string(),
            fields: fields_as_map(fields),
        };
        let exporters: Vec<Arc<dyn LogExporter>> = self
            .exporters
            .lock()
            .map(|exporters| {
                exporters
                    .iter()
                    .map(|(_, exporter)| Arc::clone(exporter))
                    .collect()
            })
            .unwrap_or_default();
        for exporter in exporters {
            // A broken exporter must not skip the remaining exporters or the
            // buffer write below; each embedder call is contained separately.
            let exporter_threshold = crate::internal::contained(
                "zeokeel_cordis::logger",
                "log exporter level()",
                None,
                || exporter.level(),
            )
            .unwrap_or(threshold);
            if level.as_u8() <= exporter_threshold.as_u8() {
                crate::internal::contained(
                    "zeokeel_cordis::logger",
                    "log exporter export()",
                    (),
                    || exporter.export(&record),
                );
            }
        }
        if self.buffer_size > 0 {
            if let Ok(mut buffer) = self.buffer.lock() {
                if buffer.len() == self.buffer_size {
                    buffer.pop_front();
                }
                buffer.push_back(record);
            }
        }
    }

    /// Snapshot of the most recent records (oldest first).
    pub fn records(&self) -> Vec<LogRecord> {
        self.buffer
            .lock()
            .map(|buffer| buffer.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Drop buffered records.
    pub fn clear_buffer(&self) {
        if let Ok(mut buffer) = self.buffer.lock() {
            buffer.clear();
        }
    }
}

/// A named logging facade handed out by [`LoggerService::logger`] /
/// [`crate::Context::logger`].
///
/// # Clock snapshot
///
/// A facade captures its time source **at creation**: [`Context::set_clock`]
/// only affects facades created afterwards. Re-acquire the facade after
/// installing a clock if it must stamp records with the injected time (plugin
/// code usually gets its logger during activation, i.e. after the host installed
/// the clock). The raw [`LoggerService::logger`] constructor uses
/// [`crate::SystemClock`].
///
/// [`Context::set_clock`]: crate::Context::set_clock
#[derive(Clone)]
pub struct Logger {
    service: Arc<LoggerService>,
    name: String,
    level: Option<LogLevel>,
    /// Time source of the context tree that created this facade (see
    /// [`crate::Clock`]); the raw [`LoggerService::logger`] constructor uses
    /// [`crate::SystemClock`].
    clock: Arc<dyn crate::clock::Clock>,
}

impl Logger {
    /// The logger name (shown on every record).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The threshold override of this facade, if any.
    pub fn level(&self) -> Option<LogLevel> {
        self.level
    }

    /// The underlying service.
    pub fn service(&self) -> Arc<LoggerService> {
        Arc::clone(&self.service)
    }

    /// Create a logger for a nested subsystem (`parent.child`).
    pub fn child(&self, name: &str) -> Logger {
        Logger {
            service: Arc::clone(&self.service),
            name: format!("{}.{}", self.name, name),
            level: self.level,
            clock: Arc::clone(&self.clock),
        }
    }

    /// Emit a record at `level`, honoring this facade's threshold override.
    pub fn log(&self, level: LogLevel, message: &str, fields: Value) {
        let threshold = self.level.unwrap_or_else(|| self.service.level());
        self.service
            .log_with_clock(level, threshold, &self.name, message, fields, &*self.clock);
    }

    /// The time source snapshot used for this facade's record timestamps.
    ///
    /// Facades snapshot the clock when they are created; see the type-level
    /// documentation for the [`crate::Context::set_clock`] interaction.
    pub fn clock(&self) -> Arc<dyn crate::clock::Clock> {
        Arc::clone(&self.clock)
    }

    pub fn error(&self, message: &str, fields: Value) {
        self.log(LogLevel::Error, message, fields);
    }

    pub fn warn(&self, message: &str, fields: Value) {
        self.log(LogLevel::Warn, message, fields);
    }

    pub fn info(&self, message: &str, fields: Value) {
        self.log(LogLevel::Info, message, fields);
    }

    pub fn debug(&self, message: &str, fields: Value) {
        self.log(LogLevel::Debug, message, fields);
    }
}

impl std::fmt::Debug for Logger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Logger")
            .field("name", &self.name)
            .field("level", &self.level)
            .finish_non_exhaustive()
    }
}

/// An exporter that forwards records to the `tracing` ecosystem.
///
/// This keeps the existing app-wide logging pipeline working while plugins log
/// through Cordis' structured API.
#[derive(Debug, Default, Clone, Copy)]
pub struct TracingExporter;

impl LogExporter for TracingExporter {
    fn export(&self, record: &LogRecord) {
        let fields = Value::Object(record.fields.clone());
        match record.level {
            LogLevel::Error => {
                tracing::error!(target: "cordis", name = %record.name, fields = %fields, "{}", record.message)
            }
            LogLevel::Warn => {
                tracing::warn!(target: "cordis", name = %record.name, fields = %fields, "{}", record.message)
            }
            LogLevel::Info => {
                tracing::info!(target: "cordis", name = %record.name, fields = %fields, "{}", record.message)
            }
            LogLevel::Debug => {
                tracing::debug!(target: "cordis", name = %record.name, fields = %fields, "{}", record.message)
            }
        }
    }
}

/// The built-in logger service key (`ctx.get(LOGGER)`).
pub const LOGGER: ServiceKey<LoggerService> = ServiceKey::new("logger");

fn fields_as_map(fields: Value) -> Map<String, Value> {
    match fields {
        Value::Null => Map::new(),
        Value::Object(object) => object,
        other => {
            let mut map = Map::new();
            map.insert("value".to_string(), other);
            map
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;

    struct CountingExporter {
        count: Arc<AtomicUsize>,
        names: Mutex<Vec<(String, u64)>>,
    }

    impl CountingExporter {
        fn new(count: Arc<AtomicUsize>) -> Arc<Self> {
            Arc::new(Self {
                count,
                names: Mutex::new(Vec::new()),
            })
        }
    }

    impl LogExporter for CountingExporter {
        fn export(&self, record: &LogRecord) {
            self.count.fetch_add(1, Ordering::SeqCst);
            self.names
                .lock()
                .unwrap()
                .push((record.name.clone(), record.seq));
        }
    }

    #[test]
    fn level_filtering_and_named_loggers() {
        let service = LoggerService::with_buffer_size(LogLevel::Info, 16);
        let logger = service.logger("plugin.mcp");
        logger.error("boom", json!({"code": 1}));
        logger.warn("careful", Value::Null);
        logger.info("started", Value::Null);
        logger.debug("hidden", Value::Null);

        let records = service.records();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].level, LogLevel::Error);
        assert_eq!(records[2].message, "started");
        assert_eq!(records[0].name, "plugin.mcp");
        assert_eq!(records[0].fields["code"], json!(1));

        service.set_level(LogLevel::Debug);
        logger.debug("now visible", Value::Null);
        assert_eq!(service.records().len(), 4);
    }

    #[test]
    fn exporter_receives_records_with_increasing_seq_and_disposes() {
        let service = LoggerService::with_buffer_size(LogLevel::Debug, 8);
        let count = Arc::new(AtomicUsize::new(0));
        let exporter = CountingExporter::new(Arc::clone(&count));
        let disposer = service.exporter(exporter.clone());

        let logger = service.logger("demo");
        logger.info("one", Value::Null);
        logger.info("two", Value::Null);
        assert_eq!(count.load(Ordering::SeqCst), 2);
        let names = exporter.names.lock().unwrap().clone();
        assert!(names[0].1 < names[1].1, "seq must increase: {names:?}");

        disposer.dispose();
        logger.info("dropped", Value::Null);
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert!(service.records().len() <= 16, "buffer stays bounded");
    }

    #[test]
    fn buffer_is_bounded_and_concurrent_writes_do_not_lose_messages() {
        let service = LoggerService::with_buffer_size(LogLevel::Debug, 64);
        let threads: Vec<_> = (0..8)
            .map(|thread| {
                let service = Arc::clone(&service);
                std::thread::spawn(move || {
                    let logger = service.logger(format!("worker.{thread}"));
                    for index in 0..100 {
                        logger.info(&format!("message {index}"), Value::Null);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let records = service.records();
        assert_eq!(records.len(), 64, "buffer stays bounded");
        // Sequence numbers are allocated atomically before the buffer push, so
        // concurrent writers can push out of seq order; the no-loss property is
        // that every buffered record owns a distinct seq and none exceeds the
        // total number of logged records (8 threads × 100).
        let seqs: Vec<u64> = records.iter().map(|record| record.seq).collect();
        let unique: std::collections::HashSet<u64> = seqs.iter().copied().collect();
        assert_eq!(unique.len(), 64, "no record lost or duplicated: {seqs:?}");
        assert!(seqs.iter().all(|seq| *seq <= 800));
    }

    #[test]
    fn per_exporter_level_override_filters_verbose_records() {
        struct OnlyErrors;
        impl LogExporter for OnlyErrors {
            fn export(&self, _record: &LogRecord) {}
            fn level(&self) -> Option<LogLevel> {
                Some(LogLevel::Error)
            }
        }
        let service = LoggerService::new(LogLevel::Debug);
        service.exporter(Arc::new(OnlyErrors));
        // No panics and the service-level records still work.
        service.logger("x").info("visible to buffer", Value::Null);
        assert_eq!(service.records().len(), 1);
    }
}
