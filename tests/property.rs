//! Deterministic randomized state-machine test (no external dependencies).
//!
//! The kernel's lifecycle is a small state machine; hand-written cases cover
//! specific bugs, but random operation sequences catch interactions between
//! them (load/dispose/refresh/update/replace/dispose_all, providers and
//! consumers, ownership children). Each scenario runs a fixed LCG seed so a
//! failure is reproducible; the thread watchdog turns a deadlock into a test
//! failure instead of a hung CI job.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use zeokeel_cordis::{
    block_on, Context, EventResult, Fiber, FiberState, Plugin, Result, ServiceKey,
};

/// Small deterministic RNG (SplitMix64); avoids a dev-dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15))
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

const NAMES: [&str; 8] = ["p0", "p1", "p2", "p3", "c0", "c1", "c2", "c3"];
static SERVICES: [ServiceKey<u64>; 4] = [
    ServiceKey::new("svc0"),
    ServiceKey::new("svc1"),
    ServiceKey::new("svc2"),
    ServiceKey::new("svc3"),
];
const INJECTS: [&[&str]; 4] = [&["svc0"], &["svc1"], &["svc2"], &["svc3"]];

/// Provides `svc{slot}`.
struct Provider {
    slot: usize,
    value: u64,
}

impl Plugin for Provider {
    fn name(&self) -> &'static str {
        NAMES[self.slot]
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        ctx.provide(SERVICES[self.slot], Arc::new(self.value))?;
        Ok(())
    }
}

/// Injects `svc{slot}` and registers an effect + listener + ownership child.
struct Consumer {
    slot: usize,
    applies: Arc<AtomicUsize>,
}

impl Plugin for Consumer {
    fn name(&self) -> &'static str {
        NAMES[4 + self.slot]
    }

    fn inject(&self) -> &'static [&'static str] {
        INJECTS[self.slot]
    }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let _value = ctx.require(SERVICES[self.slot])?;
        self.applies.fetch_add(1, Ordering::SeqCst);
        ctx.effect(|| vec![zeokeel_cordis::cleanup(|| {})])?;
        ctx.on("random/ping", |_payload| EventResult::Continue)?;
        Ok(())
    }
}

/// Run one randomized scenario; panics on a violated invariant.
fn scenario(seed: u64, steps: usize) {
    let root = Context::root();
    let mut rng = Rng::new(seed);
    let mut handles: Vec<Fiber> = Vec::new();
    let applies = Arc::new(AtomicUsize::new(0));
    let mut serial = 0u64;

    for step in 0..steps {
        match rng.below(8) {
            0 | 1 => {
                // Load a provider.
                let slot = rng.below(4);
                if let Ok(fiber) = root.plugin(
                    Arc::new(Provider {
                        slot,
                        value: serial,
                    }),
                    Value::Null,
                ) {
                    handles.push(fiber);
                }
                serial += 1;
            }
            2 | 3 => {
                // Load a consumer (Pending until its provider exists).
                let slot = rng.below(4);
                if let Ok(fiber) = root.plugin(
                    Arc::new(Consumer {
                        slot,
                        applies: applies.clone(),
                    }),
                    Value::Null,
                ) {
                    handles.push(fiber);
                }
            }
            4 => {
                // Dispose a random handle.
                if !handles.is_empty() {
                    let index = rng.below(handles.len());
                    let fiber = handles.remove(index);
                    let _ = fiber.dispose();
                }
            }
            5 => {
                // Refresh: re-evaluate dependents.
                root.refresh();
            }
            6 => {
                // Update a random handle's config.
                if !handles.is_empty() {
                    let index = rng.below(handles.len());
                    let _ = handles[index].update(json!({ "step": step }));
                }
            }
            _ => {
                // Replace a random handle with a fresh plugin of a random slot.
                if !handles.is_empty() {
                    let index = rng.below(handles.len());
                    let slot = rng.below(4);
                    let _ = handles[index].replace_plugin(Arc::new(Provider {
                        slot,
                        value: serial,
                    }));
                    serial += 1;
                }
            }
        }

        // Invariants that must hold after every operation:
        // - the root context stays usable and quiescence is reached;
        // - no disposed fiber is reported as live.
        block_on(root.when_idle());
        assert!(
            root.registry().is_idle(),
            "seed {seed} step {step}: kernel did not reach quiescence"
        );
        let live = root.registry().fibers().len();
        assert_eq!(
            live,
            handles.len(),
            "seed {seed} step {step}: registry has {live} fibers for {} live handles",
            handles.len()
        );
        for fiber in &handles {
            assert!(
                fiber.state() != FiberState::Disposed,
                "seed {seed} step {step}: tracked fiber was disposed behind our back"
            );
        }
    }

    // Full shutdown must leave nothing behind.
    let _ = root.dispose_all();
    block_on(root.when_idle());
    assert!(
        root.registry().is_idle(),
        "seed {seed}: root not idle after dispose_all"
    );
    assert_eq!(
        root.registry().fibers().len(),
        0,
        "seed {seed}: fibers leaked"
    );
    assert!(
        root.registry().keys().is_empty(),
        "seed {seed}: runtime keys leaked: {:?}",
        root.registry().keys()
    );
}

fn run_with_watchdog(seed: u64, steps: usize) {
    let (done_tx, done_rx) = mpsc::channel();
    let scenario_thread = std::thread::Builder::new()
        .name(format!("property-{seed}"))
        .stack_size(4 * 1024 * 1024)
        .spawn(move || {
            scenario(seed, steps);
            let _ = done_tx.send(());
        })
        .expect("spawn scenario");
    match done_rx.recv_timeout(Duration::from_secs(60)) {
        Ok(()) => scenario_thread.join().expect("scenario panicked"),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("seed {seed}: randomized scenario timed out (deadlock?)")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            scenario_thread.join().expect("scenario panicked")
        }
    }
}

#[test]
fn randomized_lifecycle_interactions_preserve_invariants() {
    for seed in [1u64, 2, 3, 7] {
        run_with_watchdog(seed, 120);
    }
}
