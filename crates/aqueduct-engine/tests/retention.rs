//! Retention (ADR 0066): an ended run is forgotten, and stays forgotten through reopening and
//! checkpoints; a run still going is not.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use aqueduct_core::{Entry, Payload};
use aqueduct_engine::embedded::{Engine, EngineError};
use aqueduct_procedure::Procedure;

fn open(dir: &std::path::Path, checkpoint_every: u64) -> Engine {
    Engine::builder(dir)
        .workflow(
            "double",
            Arc::new(Procedure::compile("d.star", "def run(x):\n    return x * 2\n").unwrap()),
        )
        .workflow(
            "waits",
            Arc::new(
                Procedure::compile(
                    "w.star",
                    "def run(x):\n    return wait(signal(\"go\"), timeout = 600)\n",
                )
                .unwrap(),
            ),
        )
        .checkpoint_every(checkpoint_every)
        .open()
        .unwrap()
}

fn gone(engine: &Engine, run: &str) {
    assert!(
        matches!(engine.describe(run), Err(EngineError::UnknownRun(_))),
        "{run} is still here"
    );
    assert_eq!(engine.history(run).unwrap(), None);
    assert!(engine.runs().unwrap().iter().all(|r| r.run_id != run));
}

#[test]
fn ended_runs_are_forgotten_and_stay_forgotten() {
    // Once with a checkpoint after nearly every record, once with none before reopening.
    for checkpoint_every in [1, 1_000_000] {
        let dir = std::env::temp_dir().join(format!(
            "aqueduct-retention-{checkpoint_every}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = open(&dir, checkpoint_every);
        for key in ["a", "b"] {
            engine.start(key, "double", Payload::new(*b"2")).unwrap();
            engine.wait(key, Duration::from_secs(10)).unwrap();
        }
        engine.start("w", "waits", Payload::new(*b"1")).unwrap();
        // A run still going is not forgotten.
        assert!(matches!(
            engine.forget("w", "test"),
            Err(EngineError::Unsupported(_))
        ));
        let history = engine.history("a").unwrap().unwrap();
        assert!(engine.forget("a", "retention: test").unwrap());
        gone(&engine, "a");
        // Forgetting again, or a run there never was, does nothing.
        assert!(!engine.forget("a", "again").unwrap());
        assert!(!engine.forget("nobody", "again").unwrap());
        // The others are untouched.
        assert!(engine.describe("b").is_ok());
        assert!(engine.describe("w").is_ok());
        drop(engine);

        let engine = open(&dir, checkpoint_every);
        gone(&engine, "a");
        assert!(engine.describe("b").is_ok());
        // A key forgotten can be started again: a new run, not the old one.
        engine.start("a", "double", Payload::new(*b"5")).unwrap();
        let result = engine.wait("a", Duration::from_secs(10)).unwrap();
        assert_eq!(result.output.unwrap().as_bytes(), b"10");
        assert_ne!(engine.history("a").unwrap().unwrap(), history);
        assert!(
            !engine
                .history("a")
                .unwrap()
                .unwrap()
                .iter()
                .any(|e| matches!(e, Entry::Forgotten { .. }))
        );
        // Forgotten and started again within one stretch of the log, then reopened: the new run is not
        // chained to the forgotten one's records.
        assert!(engine.forget("b", "retention: test").unwrap());
        engine.start("b", "double", Payload::new(*b"7")).unwrap();
        let output = |engine: &Engine, key: &str| {
            engine
                .wait(key, Duration::from_secs(10))
                .unwrap()
                .output
                .unwrap()
        };
        assert_eq!(output(&engine, "b").as_bytes(), b"14");
        drop(engine);
        let engine = open(&dir, checkpoint_every);
        assert_eq!(output(&engine, "a").as_bytes(), b"10");
        assert_eq!(output(&engine, "b").as_bytes(), b"14");
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
