//! Phase B conformance: typed `internal/*` events (lifecycle, config/update
//! waterfalls, listener interception, dispatch observation and service/get/set
//! hooks).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use cordis::{
    ConfigNext, Context, DispatchInfo, EventResult, Fiber, FiberState, ListenerOverride,
    ListenerRequest, Plugin, Result, ServiceKey, UpdateNext,
};

static GREETING: ServiceKey<String> = ServiceKey::new("greeting");

struct Recorder {
    runs: Arc<AtomicUsize>,
    configs: Arc<Mutex<Vec<Value>>>,
}

impl Plugin for Recorder {
    fn name(&self) -> &'static str {
        "recorder"
    }

    fn apply(&self, _ctx: Context, config: Value) -> Result<()> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        self.configs.lock().unwrap().push(config);
        Ok(())
    }
}

#[test]
fn internal_lifecycle_events() {
    let root = Context::root();
    let trace = Arc::new(Mutex::new(Vec::<String>::new()));

    let recording = Arc::clone(&trace);
    root.on_internal_plugin(Arc::new(move |fiber: &Fiber| {
        recording
            .lock()
            .unwrap()
            .push(format!("plugin:{}", fiber.name()));
    }))
    .expect("plugin hook");

    let recording = Arc::clone(&trace);
    root.on_internal_status(Arc::new(move |_fiber: &Fiber, old: FiberState| {
        recording.lock().unwrap().push(format!("status:{old:?}"));
    }))
    .expect("status hook");

    let recording = Arc::clone(&trace);
    root.on_internal_config(Arc::new(
        move |fiber: &Fiber, config: Value, next: &ConfigNext<'_>| {
            recording
                .lock()
                .unwrap()
                .push(format!("config:{}:{:?}", fiber.name(), fiber.state()));
            next.next(config)
        },
    ))
    .expect("config hook");

    let runs = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(
            Arc::new(Recorder {
                runs: Arc::clone(&runs),
                configs: Arc::new(Mutex::new(Vec::new())),
            }),
            json!({"a": 1}),
        )
        .expect("fiber");
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    {
        let trace = trace.lock().unwrap();
        assert_eq!(
            trace.iter().take(4).map(String::as_str).collect::<Vec<_>>(),
            vec![
                "plugin:recorder",
                "status:Pending",
                "config:recorder:Loading",
                "status:Loading",
            ],
            "lifecycle order: plugin → status(Loading) → config → status(Active); trace={trace:?}"
        );
    }

    fiber.dispose().expect("dispose");
    let trace = trace.lock().unwrap();
    let tail: Vec<&str> = trace.iter().rev().take(2).map(String::as_str).collect();
    assert_eq!(
        tail,
        vec!["status:Unloading", "status:Active"],
        "dispose order: Unloading → Disposed; trace={trace:?}"
    );
}

#[test]
fn internal_config_waterfall_transforms_config() {
    let root = Context::root();
    root.on_internal_config(Arc::new(
        |_fiber: &Fiber, config: Value, next: &ConfigNext<'_>| {
            let mut value = next.next(config);
            value["injected_by_hook"] = json!(true);
            value
        },
    ))
    .expect("config hook");

    let configs = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(
            Arc::new(Recorder {
                runs: Arc::new(AtomicUsize::new(0)),
                configs: Arc::clone(&configs),
            }),
            json!({"a": 1}),
        )
        .expect("fiber");
    assert_eq!(fiber.state(), FiberState::Active);
    let seen = configs.lock().unwrap()[0].clone();
    assert_eq!(seen["a"], json!(1));
    assert_eq!(seen["injected_by_hook"], json!(true));
    assert_eq!(fiber.config()["injected_by_hook"], json!(true));
}

#[test]
fn internal_update_can_veto() {
    let root = Context::root();
    let runs = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(
            Arc::new(Recorder {
                runs: Arc::clone(&runs),
                configs: Arc::new(Mutex::new(Vec::new())),
            }),
            json!({"v": 1}),
        )
        .expect("fiber");
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    let seen = Arc::new(Mutex::new(0usize));
    let recording = Arc::clone(&seen);
    let veto = root
        .on_internal_update(Arc::new(
            move |fiber: &Fiber, _config: Value, _no_save: bool, _next: &UpdateNext<'_>| {
                let _ = fiber;
                *recording.lock().unwrap() += 1;
            },
        ))
        .expect("update hook");

    fiber.update(json!({"v": 2})).expect("update returns ok");
    assert_eq!(*seen.lock().unwrap(), 1);
    assert_eq!(runs.load(Ordering::SeqCst), 1, "veto skips the restart");
    assert_eq!(fiber.config()["v"], json!(1), "vetoed update keeps config");

    veto.dispose();
    fiber
        .update(json!({"v": 3}))
        .expect("update after veto removed");
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    assert_eq!(fiber.config()["v"], json!(3));
}

#[test]
fn internal_update_hook_may_rewrite_config() {
    let root = Context::root();
    let configs = Arc::new(Mutex::new(Vec::new()));
    let fiber = root
        .plugin(
            Arc::new(Recorder {
                runs: Arc::new(AtomicUsize::new(0)),
                configs: Arc::clone(&configs),
            }),
            json!({"v": 1}),
        )
        .expect("fiber");

    root.on_internal_update(Arc::new(
        |_fiber: &Fiber, config: Value, no_save: bool, next: &UpdateNext<'_>| {
            let mut config = config;
            config["rewritten"] = json!(true);
            next.next(config, no_save);
        },
    ))
    .expect("update hook");

    fiber.update(json!({"v": 2})).expect("update");
    let seen = configs.lock().unwrap();
    let last = seen.last().expect("second activation");
    assert_eq!(last["v"], json!(2));
    assert_eq!(last["rewritten"], json!(true));
}

#[test]
fn internal_listener_bail_replaces_listener() {
    let root = Context::root();
    let original = Arc::new(AtomicUsize::new(0));
    let replacement = Arc::new(AtomicUsize::new(0));

    let hits = Arc::clone(&replacement);
    root.on_internal_listener(Arc::new(move |request: &ListenerRequest<'_>| {
        if request.name != "demo" {
            return None;
        }
        let hits = Arc::clone(&hits);
        Some(ListenerOverride {
            callback: Arc::new(move |_args: &[Value]| {
                hits.fetch_add(1, Ordering::SeqCst);
                EventResult::Continue
            }),
            prepend: request.prepend,
            global: true,
            once: false,
        })
    }))
    .expect("listener hook");

    let original_clone = Arc::clone(&original);
    root.on("demo", move |_args| {
        original_clone.fetch_add(1, Ordering::SeqCst);
        EventResult::Continue
    })
    .expect("listener");

    root.emit("demo", &[]);
    assert_eq!(
        replacement.load(Ordering::SeqCst),
        1,
        "the replacement listener runs"
    );
    assert_eq!(
        original.load(Ordering::SeqCst),
        0,
        "the original listener was replaced before registration"
    );
}

#[test]
fn internal_dispatch_does_not_recurse() {
    let root = Context::root();
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let recording = Arc::clone(&events);
    let ctx = root.clone();
    root.on_internal_dispatch(Arc::new(move |info: &DispatchInfo| {
        recording
            .lock()
            .unwrap()
            .push(format!("{}:{}", info.mode.as_str(), info.name));
        // Re-entrant dispatch must not notify `internal/dispatch` again.
        if info.name == "outer" {
            ctx.emit("inner", &[]);
        }
    }))
    .expect("dispatch hook");

    root.emit("outer", &[json!(1)]);
    let events = events.lock().unwrap();
    assert_eq!(
        events.as_slice(),
        &["emit:outer".to_string()],
        "nested dispatch and internal names are not reported: {events:?}"
    );

    // Internal event names never reach the observer.
    root.on("internal/whatever", |_args| EventResult::Continue)
        .expect("internal listener");
    root.emit("internal/whatever", &[]);
    assert_eq!(events.len(), 1);
}

#[test]
fn internal_service_events_observe_provide_and_remove() {
    let root = Context::root();
    let trace = Arc::new(Mutex::new(Vec::<String>::new()));
    let recording = Arc::clone(&trace);
    root.on_internal_service(Arc::new(
        move |_ctx: &Context, name: &str, value: Option<&_>| {
            let state = if value.is_some() {
                "provided"
            } else {
                "removed"
            };
            recording.lock().unwrap().push(format!("{name}:{state}"));
        },
    ))
    .expect("service hook");

    let disposer = root
        .provide(GREETING, Arc::new("hi".to_string()))
        .expect("provide");
    disposer.dispose();
    assert_eq!(
        trace.lock().unwrap().as_slice(),
        &[
            "greeting:provided".to_string(),
            "greeting:removed".to_string()
        ]
    );
}

#[test]
fn internal_get_and_set_waterfalls() {
    let root = Context::root();
    root.provide(GREETING, Arc::new("one".to_string()))
        .expect("provide");

    // get: a hook may resolve the read itself.
    let override_hook = root
        .on_internal_get(Arc::new(
            |_ctx: &Context, name: &str, next: &cordis::GetNext<'_>| {
                if name == "injected" {
                    Some(Arc::new("from-hook".to_string()) as cordis::ServiceValue)
                } else {
                    next.next()
                }
            },
        ))
        .expect("get hook");
    assert_eq!(
        root.get_named("injected")
            .and_then(|value| value.downcast::<String>().ok())
            .map(|value| value.as_str().to_string()),
        Some("from-hook".to_string())
    );
    assert!(
        root.get(GREETING).is_some(),
        "falls through to the terminal"
    );
    override_hook.dispose();

    // set: hooks that forward let the write through.
    let writes = Arc::new(AtomicUsize::new(0));
    let recording = Arc::clone(&writes);
    let forward = root
        .on_internal_set(Arc::new(
            move |_ctx: &Context, _name: &str, _value: &_, next: &cordis::SetNext<'_>| {
                recording.fetch_add(1, Ordering::SeqCst);
                next.next()
            },
        ))
        .expect("set hook");
    root.set("greeting", Arc::new("two".to_string()))
        .expect("set forwards");
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert_eq!(root.get(GREETING).unwrap().as_str(), "two");
    forward.dispose();

    // set: a vetoing hook blocks the write.
    root.on_internal_set(Arc::new(
        |_ctx: &Context, _name: &str, _value: &_, _next: &_| false,
    ))
    .expect("veto hook");
    root.set("greeting", Arc::new("three".to_string()))
        .expect("vetoed set returns ok");
    assert_eq!(
        root.get(GREETING).unwrap().as_str(),
        "two",
        "vetoed set does not write"
    );
}
