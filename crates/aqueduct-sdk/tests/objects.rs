//! Virtual objects in Rust (ADR 0043), through the embedded engine: concurrent callers, a handler that
//! fails after writing, and a restart.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use aqueduct_core::Payload;
use aqueduct_engine::embedded::{Attempt, Engine, EngineError};
use aqueduct_sdk::{ObjectContext, WorkflowError, object_handler};

const SECOND: Duration = Duration::from_secs(20);

async fn record(ctx: ObjectContext, score: f64) -> Result<f64, WorkflowError> {
    let mut history: Vec<f64> = ctx.get("history")?.unwrap_or_default();
    history.push(score);
    ctx.set("history", &history)?;
    // Reads see the handler's own writes; other callers wait, across this step.
    let _: () = ctx.step("persist", &history.len()).await?;
    if score < 0.0 {
        return Err(WorkflowError::failed("a score cannot be negative"));
    }
    Ok(history.iter().sum::<f64>() / history.len() as f64)
}

async fn reset(ctx: ObjectContext, _: ()) -> Result<(), WorkflowError> {
    ctx.clear("history");
    Ok(())
}

fn engine(dir: &std::path::Path) -> Engine {
    Engine::builder(dir)
        .object_handler("scores", "record", Arc::new(object_handler(record)))
        .object_handler("scores", "reset", Arc::new(object_handler(reset)))
        .step("persist", |_: &Payload| {
            std::thread::sleep(Duration::from_millis(2));
            Attempt::Completed(Payload::new(*b"null"))
        })
        .open()
        .unwrap()
}

fn json(text: &str) -> Payload {
    Payload::new(text.as_bytes().to_vec())
}

#[test]
fn an_object_keeps_its_state_through_callers_failures_and_restarts() {
    let dir = std::env::temp_dir().join(format!("aqueduct-sdk-objects-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    {
        let engine = Arc::new(engine(&dir));
        // Concurrent callers: none of their appends is lost.
        let callers: Vec<_> = (0..16)
            .map(|i| {
                let engine = Arc::clone(&engine);
                std::thread::spawn(move || {
                    engine
                        .call_object(
                            "scores",
                            "git",
                            "record",
                            &format!("c{i}"),
                            &json("2.0"),
                            SECOND,
                        )
                        .unwrap()
                })
            })
            .collect();
        for caller in callers {
            assert_eq!(caller.join().unwrap().as_bytes(), b"2.0");
        }
        let state: serde_json::Value =
            serde_json::from_slice(engine.object_state("scores", "git").unwrap().as_bytes())
                .unwrap();
        assert_eq!(state["history"].as_array().unwrap().len(), 16);

        // A failed handler commits nothing, though it had written.
        let failed = engine.call_object("scores", "git", "record", "bad", &json("-1.0"), SECOND);
        assert_eq!(
            failed,
            Err(EngineError::HandlerFailed(
                "a score cannot be negative".into()
            ))
        );
        let state: serde_json::Value =
            serde_json::from_slice(engine.object_state("scores", "git").unwrap().as_bytes())
                .unwrap();
        assert_eq!(state["history"].as_array().unwrap().len(), 16);

        // Errors callers can act on, and keys the objects reserve.
        assert!(matches!(
            engine.call_object("scores", "git", "nope", "x", &json("1"), SECOND),
            Err(EngineError::UnknownWorkflow(_))
        ));
        assert!(matches!(
            engine.call_object("scores", "git", "record", "x", &json("{not"), SECOND),
            Err(EngineError::InvalidPayload(_))
        ));
        assert!(matches!(
            engine.start("$object/scores/git", "$object/scores", json("{}")),
            Err(EngineError::InvalidKey(_))
        ));
        assert!(matches!(
            engine.signal("$object/scores/git", "invoke", "", json("{}")),
            Err(EngineError::InvalidKey(_))
        ));
        // Without an id, every call is a new one.
        engine
            .call_object("scores", "new", "record", "", &json("1.0"), SECOND)
            .unwrap();
        engine
            .call_object("scores", "new", "record", "", &json("3.0"), SECOND)
            .unwrap();
        let state: serde_json::Value =
            serde_json::from_slice(engine.object_state("scores", "new").unwrap().as_bytes())
                .unwrap();
        assert_eq!(state["history"], serde_json::json!([1.0, 3.0]));
    }
    // After a restart the state is there; a retried call is not run again, and gets its answer.
    let engine = engine(&dir);
    let again = engine
        .call_object("scores", "git", "record", "c3", &json("2.0"), SECOND)
        .unwrap();
    assert_eq!(again.as_bytes(), b"2.0");
    let next = engine
        .call_object("scores", "git", "record", "more", &json("19.0"), SECOND)
        .unwrap();
    assert_eq!(next.as_bytes(), b"3.0"); // (16 * 2 + 19) / 17
    engine
        .call_object("scores", "git", "reset", "r", &json("null"), SECOND)
        .unwrap();
    assert_eq!(
        engine.object_state("scores", "git").unwrap().as_bytes(),
        b"{}"
    );
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}
