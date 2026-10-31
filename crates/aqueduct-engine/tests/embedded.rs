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
