//! Phase D conformance: event options (`once`, `prepend`, `global`), context
//! scope filtering and bail value semantics.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use zeokeel_cordis::{Context, EventOptions, EventResult};

type Listener = Arc<dyn Fn(&[Value]) -> EventResult + Send + Sync>;

fn counter() -> (Arc<AtomicUsize>, Listener) {
    let hits = Arc::new(AtomicUsize::new(0));
    let recording = Arc::clone(&hits);
    let callback = Arc::new(move |_args: &[Value]| {
        recording.fetch_add(1, Ordering::SeqCst);
        EventResult::Continue
    });
    (hits, callback)
}

#[test]
fn event_once_fires_exactly_once() {
    let root = Context::root();
    let (hits, callback) = counter();
    let disposer = root.once("demo", move |args| callback(args)).expect("once");
    root.emit("demo", &[]);
    root.emit("demo", &[]);
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "once listener removed itself"
    );

    // The returned disposer stays idempotent after auto-removal.
    disposer.dispose();
    root.emit("demo", &[]);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[test]
fn event_options_prepend_and_global() {
    let root = Context::root();
    let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));

    let recording = Arc::clone(&order);
    root.on("demo", move |_args| {
        recording.lock().unwrap().push("append");
        EventResult::Continue
    })
    .expect("append listener");
    let recording = Arc::clone(&order);
    root.on_with_options("demo", EventOptions::default().prepend(), move |_args| {
        recording.lock().unwrap().push("prepend");
        EventResult::Continue
    })
    .expect("prepended listener");

    root.emit("demo", &[]);
    assert_eq!(*order.lock().unwrap(), vec!["prepend", "append"]);

    // `global` bypasses the context-scope filter.
    let isolated = root.isolate("demo");
    let global_hits = Arc::new(AtomicUsize::new(0));
    let hits = Arc::clone(&global_hits);
    root.on_with_options("demo", EventOptions::default().global(), move |_args| {
        hits.fetch_add(1, Ordering::SeqCst);
        EventResult::Continue
    })
    .expect("global listener");
    isolated.emit("demo", &[]);
    assert_eq!(
        global_hits.load(Ordering::SeqCst),
        1,
        "global listeners receive events from another scope"
    );
}

#[test]
fn event_context_filter_matches_scopes() {
    let root = Context::root();
    let root_hits = Arc::new(AtomicUsize::new(0));
    let isolated_hits = Arc::new(AtomicUsize::new(0));

    let hits = Arc::clone(&root_hits);
    root.on("demo", move |_args| {
        hits.fetch_add(1, Ordering::SeqCst);
        EventResult::Continue
    })
    .expect("root listener");

    let isolated = root.isolate("demo");
    let hits = Arc::clone(&isolated_hits);
    isolated
        .on("demo", move |_args| {
            hits.fetch_add(1, Ordering::SeqCst);
            EventResult::Continue
        })
        .expect("isolated listener");

    root.emit("demo", &[]);
    assert_eq!(root_hits.load(Ordering::SeqCst), 1);
    assert_eq!(
        isolated_hits.load(Ordering::SeqCst),
        0,
        "listener from another scope must not receive the event"
    );

    isolated.emit("demo", &[]);
    assert_eq!(
        root_hits.load(Ordering::SeqCst),
        1,
        "root listener does not receive events dispatched from the isolated scope"
    );
    assert_eq!(isolated_hits.load(Ordering::SeqCst), 1);
}

#[test]
fn event_waterfall_respects_context_filter() {
    let root = Context::root();
    let root_seen = Arc::new(AtomicUsize::new(0));
    let isolated_seen = Arc::new(AtomicUsize::new(0));

    let seen = Arc::clone(&root_seen);
    root.on_waterfall("demo", move |_args, next| {
        seen.fetch_add(1, Ordering::SeqCst);
        next.next()
    })
    .expect("root middleware");

    let isolated = root.isolate("demo");
    let seen = Arc::clone(&isolated_seen);
    isolated
        .on_waterfall("demo", move |_args, next| {
            seen.fetch_add(1, Ordering::SeqCst);
            next.next()
        })
        .expect("isolated middleware");

    let terminal = Some(zeokeel_cordis::waterfall_terminal(|args: &[Value]| {
        args.first().cloned().unwrap_or(Value::Null)
    }));
    assert_eq!(root.waterfall("demo", &[json!(1)], terminal), json!(1));
    assert_eq!(root_seen.load(Ordering::SeqCst), 1);
    assert_eq!(isolated_seen.load(Ordering::SeqCst), 0);

    let terminal = Some(zeokeel_cordis::waterfall_terminal(|args: &[Value]| {
        args.first().cloned().unwrap_or(Value::Null)
    }));
    assert_eq!(isolated.waterfall("demo", &[json!(2)], terminal), json!(2));
    assert_eq!(isolated_seen.load(Ordering::SeqCst), 1);
    assert_eq!(root_seen.load(Ordering::SeqCst), 1);
}

#[test]
fn bail_ignores_null_and_false_values() {
    let root = Context::root();
    let seen = Arc::new(Mutex::new(Vec::<&'static str>::new()));

    let recording = Arc::clone(&seen);
    root.on("demo", move |_args| {
        recording.lock().unwrap().push("null");
        EventResult::Bail(Value::Null)
    })
    .expect("null bail");
    let recording = Arc::clone(&seen);
    root.on("demo", move |_args| {
        recording.lock().unwrap().push("false");
        EventResult::Bail(Value::Bool(false))
    })
    .expect("false bail");
    let recording = Arc::clone(&seen);
    root.on("demo", move |_args| {
        recording.lock().unwrap().push("stop");
        EventResult::Bail(json!("stop"))
    })
    .expect("real bail");

    assert_eq!(root.bail("demo", &[]), Some(json!("stop")));
    assert_eq!(*seen.lock().unwrap(), vec!["null", "false", "stop"]);

    // `Bail(Value::Null)`/`Bail(false)` never stop a `serial_async` chain either.
    assert_eq!(
        zeokeel_cordis::block_on(root.serial_async("demo", &[])),
        Some(json!("stop"))
    );
}
