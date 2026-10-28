//! Phase A conformance: the logger service (levels, exporters, buffering and
//! the `tracing` adapter).

use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use zeokeel_cordis::logger::LOGGER;
use zeokeel_cordis::{
    Context, LogExporter, LogLevel, LogRecord, LoggerService, Plugin, Result, TracingExporter,
};

#[derive(Default)]
struct CountingExporter {
    count: AtomicUsize,
    records: Mutex<Vec<(String, LogLevel, String)>>,
    seqs: Mutex<Vec<u64>>,
}

impl CountingExporter {
    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    fn names(&self) -> Vec<String> {
        self.records
            .lock()
            .unwrap()
            .iter()
            .map(|(name, _, _)| name.clone())
            .collect()
    }

    fn messages(&self) -> Vec<String> {
        self.records
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, message)| message.clone())
            .collect()
    }

    fn seqs(&self) -> Vec<u64> {
        self.seqs.lock().unwrap().clone()
    }
}

impl LogExporter for CountingExporter {
    fn export(&self, record: &LogRecord) {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.records.lock().unwrap().push((
            record.name.clone(),
            record.level,
            record.message.clone(),
        ));
        self.seqs.lock().unwrap().push(record.seq);
    }
}

#[test]
fn logger_filters_and_exports() {
    let root = Context::root();
    let exporter = Arc::new(CountingExporter::default());
    let _disposer = root.log_exporter(exporter.clone()).expect("exporter");

    let logger = root.logger_named("demo");
    logger.error("failed", json!({"code": 1}));
    logger.warn("careful", Value::Null);
    logger.info("started", Value::Null);
    logger.debug("hidden by the default Info threshold", Value::Null);

    assert_eq!(exporter.count(), 3, "debug is filtered at level Info");
    assert_eq!(exporter.names(), vec!["demo", "demo", "demo"]);
    assert_eq!(
        exporter.messages(),
        vec![
            "failed".to_string(),
            "careful".to_string(),
            "started".to_string()
        ]
    );
    let seqs = exporter.seqs();
    assert!(
        seqs.windows(2).all(|pair| pair[0] < pair[1]),
        "sequence numbers increase: {seqs:?}"
    );

    let records = root.logger_service().records();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].level, LogLevel::Error);
    assert_eq!(records[0].fields["code"], json!(1));
}

#[test]
fn logger_exporter_unloads_with_fiber() {
    struct ExportPlugin {
        logger_slot: Arc<Mutex<Option<zeokeel_cordis::Logger>>>,
        exporter: Arc<CountingExporter>,
    }

    impl Plugin for ExportPlugin {
        fn name(&self) -> &'static str {
            "export-plugin"
        }

        fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
            ctx.log_exporter(self.exporter.clone())?;
            *self.logger_slot.lock().unwrap() = Some(ctx.logger());
            Ok(())
        }
    }

    let root = Context::root();
    let exporter = Arc::new(CountingExporter::default());
    let slot = Arc::new(Mutex::new(None));
    let fiber = root
        .plugin(
            Arc::new(ExportPlugin {
                logger_slot: Arc::clone(&slot),
                exporter: Arc::clone(&exporter),
            }),
            Value::Null,
        )
        .expect("plugin fiber");

    let logger = slot.lock().unwrap().clone().expect("logger");
    assert_eq!(logger.name(), "export-plugin");
    logger.info("before dispose", Value::Null);
    assert_eq!(exporter.count(), 1);

    fiber.dispose().expect("dispose");
    logger.info("after dispose", Value::Null);
    assert_eq!(
        exporter.count(),
        1,
        "exporter is unregistered when its fiber unloads"
    );
}

#[test]
fn logger_intercept_overrides_name_and_level() {
    let root = Context::root();
    let ctx = root.intercept("logger", json!({"name": "custom.name", "level": 3}));
    let exporter = Arc::new(CountingExporter::default());
    let _disposer = ctx.log_exporter(exporter.clone()).expect("exporter");

    let logger = ctx.logger();
    assert_eq!(logger.name(), "custom.name");
    logger.debug("debug is visible with level 3", Value::Null);
    assert_eq!(exporter.count(), 1);

    // Without an intercept the facade is named after the current fiber/plugin.
    assert_eq!(root.logger().name(), "root");
}

#[test]
fn logger_service_is_injectable() {
    let root = Context::root();
    let service = root
        .get(LOGGER)
        .expect("logger service is provided at root");
    assert!(Arc::ptr_eq(&service, &root.logger_service()));

    service.logger("direct").info("hello", json!({"k": "v"}));
    let records = root.logger_service().records();
    assert!(records
        .iter()
        .any(|record| record.name == "direct" && record.message == "hello"));
    assert_eq!(records.last().unwrap().fields["k"], json!("v"));
}

#[test]
fn tracing_exporter_forwards_to_tracing() {
    #[derive(Clone)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBuffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct BufferWriter(Arc<Mutex<Vec<u8>>>);

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufferWriter {
        type Writer = SharedBuffer;

        fn make_writer(&'a self) -> Self::Writer {
            SharedBuffer(Arc::clone(&self.0))
        }
    }

    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(BufferWriter(Arc::clone(&buffer)))
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let service = LoggerService::new(LogLevel::Info);
    let _exporter = service.exporter(Arc::new(TracingExporter));
    service
        .logger("tracing.demo")
        .error("forwarded to tracing", Value::Null);

    let text = String::from_utf8(buffer.lock().unwrap().clone()).expect("utf-8 log output");
    assert!(text.contains("forwarded to tracing"), "output: {text}");
    assert!(text.contains("tracing.demo"), "output: {text}");
}
