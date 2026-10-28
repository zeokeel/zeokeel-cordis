//! Child-fiber ownership (CORE-047): a fiber owns the fibers it loads; unloading
//! the parent disposes the subtree, and a failed parent activation rolls the
//! children back.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use cordis::{cleanup, Context, FiberState, Plugin};
use serde_json::Value;

struct Leaf {
    cleanups: Arc<AtomicUsize>,
}

impl Plugin for Leaf {
    fn name(&self) -> &'static str {
        "leaf"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        let cleanups = self.cleanups.clone();
        ctx.effect(move || {
            vec![cleanup(move || {
                cleanups.fetch_add(1, Ordering::SeqCst);
            })]
        })?;
        Ok(())
    }
}

struct Parent {
    children: usize,
    cleanups: Arc<AtomicUsize>,
    fail: bool,
}

impl Plugin for Parent {
    fn name(&self) -> &'static str {
        "parent"
    }

    fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
        for _ in 0..self.children {
            ctx.plugin(
                Arc::new(Leaf {
                    cleanups: self.cleanups.clone(),
                }),
                Value::Null,
            )?;
        }
        if self.fail {
            return Err(cordis::CordisError::InvalidPlugin("boom".to_string()));
        }
        Ok(())
    }
}

fn leaf_count(root: &Context) -> usize {
    root.registry()
        .fibers()
        .iter()
        .filter(|fiber| fiber.state() == FiberState::Active)
        .count()
}

#[test]
fn parent_dispose_disposes_children() {
    let root = Context::root();
    let cleanups = Arc::new(AtomicUsize::new(0));
    let parent = root
        .plugin(
            Arc::new(Parent {
                children: 3,
                cleanups: cleanups.clone(),
                fail: false,
            }),
            Value::Null,
        )
        .expect("parent");
    assert_eq!(leaf_count(&root), 4, "parent + three active leaves");

    parent.dispose().expect("dispose parent");
    assert_eq!(parent.state(), FiberState::Disposed);
    assert_eq!(
        leaf_count(&root),
        0,
        "children must be disposed with the parent"
    );
    assert_eq!(root.registry().size(), 0);
    assert_eq!(
        cleanups.load(Ordering::SeqCst),
        3,
        "each leaf cleanup ran once"
    );
}

#[test]
fn failed_parent_activation_rolls_back_children() {
    let root = Context::root();
    let cleanups = Arc::new(AtomicUsize::new(0));
    let parent = root
        .plugin(
            Arc::new(Parent {
                children: 2,
                cleanups: cleanups.clone(),
                fail: true,
            }),
            Value::Null,
        )
        .expect("plugin creation");
    assert_eq!(parent.state(), FiberState::Failed);
    assert_eq!(
        leaf_count(&root),
        0,
        "no orphan children after a failed activation"
    );
    // The failed parent fiber itself stays registered in `Failed` state.
    assert_eq!(root.registry().size(), 1);
    assert_eq!(cleanups.load(Ordering::SeqCst), 2);
}

#[test]
fn nested_cascade_disposes_the_whole_subtree() {
    let root = Context::root();
    let cleanups = Arc::new(AtomicUsize::new(0));
    // grandparent loads a parent which loads two leaves.
    struct GrandParent {
        cleanups: Arc<AtomicUsize>,
    }
    impl Plugin for GrandParent {
        fn name(&self) -> &'static str {
            "grandparent"
        }
        fn apply(&self, ctx: Context, _config: Value) -> cordis::Result<()> {
            ctx.plugin(
                Arc::new(Parent {
                    children: 2,
                    cleanups: self.cleanups.clone(),
                    fail: false,
                }),
                Value::Null,
            )?;
            Ok(())
        }
    }
    let grandparent = root
        .plugin(
            Arc::new(GrandParent {
                cleanups: cleanups.clone(),
            }),
            Value::Null,
        )
        .expect("grandparent");
    assert_eq!(leaf_count(&root), 4);
    grandparent.dispose().expect("dispose subtree root");
    assert_eq!(leaf_count(&root), 0);
    assert_eq!(root.registry().size(), 0);
    assert_eq!(cleanups.load(Ordering::SeqCst), 2);
}

#[test]
fn root_fiber_dispose_cascades_the_tree() {
    let root = Context::root();
    let cleanups = Arc::new(AtomicUsize::new(0));
    let _parent = root
        .plugin(
            Arc::new(Parent {
                children: 2,
                cleanups: cleanups.clone(),
                fail: false,
            }),
            Value::Null,
        )
        .expect("parent");
    assert_eq!(leaf_count(&root), 3);
    root.fiber().dispose().expect("dispose root fiber");
    assert_eq!(leaf_count(&root), 0);
    assert_eq!(root.registry().size(), 0);
    assert_eq!(cleanups.load(Ordering::SeqCst), 2);
}

#[test]
fn dispose_all_reclaims_the_whole_tree() {
    let root = Context::root();
    let cleanups = Arc::new(AtomicUsize::new(0));
    let _parent = root
        .plugin(
            Arc::new(Parent {
                children: 3,
                cleanups: cleanups.clone(),
                fail: false,
            }),
            Value::Null,
        )
        .expect("parent");
    assert_eq!(leaf_count(&root), 4);
    root.dispose_all().expect("dispose_all");
    assert_eq!(root.registry().size(), 0);
    assert_eq!(root.registry().fiber_count(), 0);
    assert!(root.registry().is_idle());
    assert_eq!(cleanups.load(Ordering::SeqCst), 3);
    // A fresh root is unaffected.
    let fresh = Context::root();
    assert_eq!(leaf_count(&fresh), 0);
}
