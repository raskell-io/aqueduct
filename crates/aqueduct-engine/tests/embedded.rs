//! The embedded engine on a real file log.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use aqueduct_core::{Payload, Status, StepOutcome};
use aqueduct_engine::embedded::{Engine, EngineError};
use aqueduct_procedure::Procedure;

const GREET: &str = r#"
def run(user):
    name = step("lookup_name", user)
    a, b = parallel([["greet_en", name], ["greet_de", name]])
    return {"en": a, "de": b}
"#;

fn temp_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "aqueduct-embedded-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn json(value: &str) -> Payload {
    Payload::new(value.as_bytes().to_vec())
}

fn text(payload: &Payload) -> String {
    serde_json::from_slice::<String>(payload.as_bytes()).unwrap()
}

fn open_engine(dir: &PathBuf, calls: Arc<AtomicUsize>, slow: Duration) -> Engine {
    let greet = Arc::new(Procedure::compile("greet.star", GREET).unwrap());
    Engine::builder(dir)
        .workflow("greet", greet)
        .step("lookup_name", move |user| {
            calls.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(slow);
            StepOutcome::Completed(json(&format!("\"{}\"", text(user).to_uppercase())))
        })
        .step("greet_en", |name| {
            StepOutcome::Completed(json(&format!("\"hello {}\"", text(name))))
        })
        .step("greet_de", |name| {
            StepOutcome::Completed(json(&format!("\"hallo {}\"", text(name))))
        })
        .open()
        .unwrap()
}

#[test]
fn runs_complete_and_starts_are_idempotent() {
    let dir = temp_dir();
    let calls = Arc::new(AtomicUsize::new(0));
    let engine = open_engine(&dir, Arc::clone(&calls), Duration::ZERO);

    for user in ["ada", "grace", "alan"] {
        engine
            .start(
                &format!("greet:{user}"),
                "greet",
                json(&format!("\"{user}\"")),
            )
            .unwrap();
    }
    let result = engine.wait("greet:ada", Duration::from_secs(10)).unwrap();
    assert_eq!(result.status, Status::Completed);
    assert_eq!(
        result.output,
        Some(json(r#"{"en":"hello ADA","de":"hallo ADA"}"#))
    );
    for user in ["grace", "alan"] {
        assert_eq!(
            engine
                .wait(&format!("greet:{user}"), Duration::from_secs(10))
                .unwrap()
                .status,
            Status::Completed
        );
    }

    // Starting an existing key does not run it again.
    engine
        .start("greet:ada", "greet", json("\"someone else\""))
        .unwrap();
    assert_eq!(
        engine
            .wait("greet:ada", Duration::from_secs(10))
            .unwrap()
            .output,
        result.output
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    assert_eq!(
        engine.start("x", "missing", json("1")),
        Err(EngineError::UnknownWorkflow("missing".into()))
    );
    assert_eq!(
        engine.wait("nobody", Duration::from_secs(1)),
        Err(EngineError::UnknownRun("nobody".into()))
    );
    engine.shutdown().unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_run_interrupted_by_a_restart_finishes_after_reopening() {
    let dir = temp_dir();
    let calls = Arc::new(AtomicUsize::new(0));
    {
        // The first step takes a while; the engine closes before it can be recorded.
        let engine = open_engine(&dir, Arc::clone(&calls), Duration::from_millis(300));
        engine.start("greet:ada", "greet", json("\"ada\"")).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        drop(engine);
    }
    let engine = open_engine(&dir, Arc::clone(&calls), Duration::ZERO);
    let result = engine.wait("greet:ada", Duration::from_secs(10)).unwrap();
    assert_eq!(
        result.output,
        Some(json(r#"{"en":"hello ADA","de":"hallo ADA"}"#))
    );
    // The interrupted step ran again after the restart: at least once, recorded once.
    assert!(calls.load(Ordering::SeqCst) >= 1);
    engine.shutdown().unwrap();

    // Reopening once more finds the finished run in the log.
    let engine = open_engine(&dir, Arc::clone(&calls), Duration::ZERO);
    assert_eq!(
        engine
            .wait("greet:ada", Duration::from_secs(1))
            .unwrap()
            .status,
        Status::Completed
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn finished_runs_leave_memory_and_stay_queryable() {
    let dir = temp_dir();
    let calls = Arc::new(AtomicUsize::new(0));
    let greet = Arc::new(Procedure::compile("greet.star", GREET).unwrap());
    let open = |calls: Arc<AtomicUsize>| {
        Engine::builder(&dir)
            .workflow(
                "greet",
                Arc::clone(&greet) as Arc<dyn aqueduct_core::Decider + Send + Sync>,
            )
            .step("lookup_name", move |user: &Payload| {
                calls.fetch_add(1, Ordering::SeqCst);
                StepOutcome::Completed(json(&format!("\"{}\"", text(user).to_uppercase())))
            })
            .step("greet_en", |name: &Payload| {
                StepOutcome::Completed(json(&format!("\"hello {}\"", text(name))))
            })
            .step("greet_de", |name: &Payload| {
                StepOutcome::Completed(json(&format!("\"hallo {}\"", text(name))))
            })
            .checkpoint_every(1)
            .open()
            .unwrap()
    };
    let engine = open(Arc::clone(&calls));
    for n in 0..20 {
        engine
            .start(&format!("u-{n}"), "greet", json(&format!("\"u{n}\"")))
            .unwrap();
    }
    for n in 0..20 {
        assert_eq!(
            engine
                .wait(&format!("u-{n}"), Duration::from_secs(10))
                .unwrap()
                .status,
            Status::Completed
        );
    }
    // With a checkpoint after every record, finished runs have left memory; they are still answered,
    // starting them again is still a no-op, and signals to them are still acknowledged.
    std::thread::sleep(Duration::from_millis(50));
    let stats = engine.stats().unwrap();
    assert!(
        stats.runs_in_memory < 20,
        "finished runs left memory: {stats:?}"
    );
    assert_eq!(
        engine.wait("u-3", Duration::from_secs(1)).unwrap().output,
        Some(json(r#"{"en":"hello U3","de":"hallo U3"}"#))
    );
    engine
        .start("u-3", "greet", json("\"someone else\""))
        .unwrap();
    engine.signal("u-3", "anything", "s-1", json("1")).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 20);
    engine.shutdown().unwrap();

    let engine = open(Arc::clone(&calls));
    assert_eq!(
        engine.wait("u-19", Duration::from_secs(1)).unwrap().status,
        Status::Completed
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        20,
        "nothing ran again after reopening"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}
