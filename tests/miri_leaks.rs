//! Miri leak-check scenario (run under `cargo miri test --test miri_leaks`).
//!
//! The kernel must not leak memory when contexts/plugins are created, disposed
//! and dropped. Unlike the unit-test suite (which leaks test scaffolding and is
//! therefore run with `-Zmiri-ignore-leaks`), this scenario is leak-clean so
//! Miri's leak checker stays enabled in CI.
//!
//! Run with:
//! `MIRIFLAGS="-Zmiri-disable-isolation" cargo +nightly miri test --test miri_leaks`

use std::sync::Arc;

use serde_json::Value;

use zeokeel_cordis::{cleanup, Context, Plugin, Result, ServiceKey};

static COUNTER: ServiceKey<u64> = ServiceKey::new("miri-counter");

struct Provider {
    id: u64,
}

impl Plugin for Provider {
    fn name(&self) -> &'static str {
        "miri-provider"
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.provide(COUNTER, Arc::new(self.id))?;
        ctx.effect(|| vec![cleanup(|| {})])?;
        Ok(())
    }
}

struct Consumer;

impl Plugin for Consumer {
    fn name(&self) -> &'static str {
        "miri-consumer"
    }

    fn inject(&self) -> &'static [&'static str] {
        &["counter"]
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let _value = ctx.require(COUNTER)?;
        ctx.effect(|| vec![cleanup(|| {})])?;
        ctx.on("miri/ping", |_payload| {
            zeokeel_cordis::EventResult::Continue
        })?;
        Ok(())
    }
}

#[test]
fn contexts_and_plugins_do_not_leak() {
    let root = Context::root();
    for id in 0..4u64 {
        let provider = root
            .plugin(Arc::new(Provider { id }), Value::Null)
            .expect("provider");
        let consumer = root
            .plugin(Arc::new(Consumer), Value::Null)
            .expect("consumer");
        // Exercise disposal through both paths: individual fibers and a full
        // sweep; every allocation must be released before the test ends.
        if id % 2 == 0 {
            provider.dispose().expect("dispose provider");
            consumer.dispose().expect("dispose consumer");
        }
    }
    root.dispose_all().expect("dispose all");
    drop(root);
}
