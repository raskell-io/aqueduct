//! Several runs under one key: continue-as-new and reuse after completion (ADR 0022).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aqueduct_core::{Payload, Status, StepOutcome};
use aqueduct_engine::ReusePolicy;
use aqueduct_engine::embedded::{Engine, EngineError};
use aqueduct_sdk::{WorkflowContext, WorkflowError, workflow};

fn json(text: &str) -> Payload {
    Payload::new(text.as_bytes().to_vec())
}

fn temp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("aqueduct-gen-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Counts to 3, one run per number, sleeping in the middle run.
async fn count(ctx: WorkflowContext, n: u32) -> Result<u32, WorkflowError> {
    let _: u32 = ctx.step("tick", &n).await?;
    if n == 1 {
        ctx.sleep(Duration::from_millis(300)).await?;
    }
    if n < 3 {
        return Err(ctx.continue_as_new(&(n + 1)));
    }
    Ok(n)
}

fn counting(dir: &std::path::Path) -> Engine {
    Engine::builder(dir)
        .workflow("count", Arc::new(workflow(count)))
        .step("tick", |n: &Payload| StepOutcome::Completed(n.clone()))
        .open()
        .unwrap()
}

#[test]
fn a_run_continues_as_new_across_a_restart_and_wait_follows_the_chain() {
    let dir = temp("chain");
    {
        let engine = counting(&dir);
        engine.start("c", "count", json("0")).unwrap();
        // Stop during the sleep of the second run.
        std::thread::sleep(Duration::from_millis(100));
    }
    let engine = counting(&dir);
    let done = engine.wait("c", Duration::from_secs(10)).unwrap();
    assert_eq!(
        (done.status, done.output),
        (Status::Completed, Some(json("3")))
    );
    // A retried start of the chain (its first input) is still idempotent; another input conflicts.
    engine.start("c", "count", json("0")).unwrap();
    assert_eq!(
        engine.start("c", "count", json("7")),
        Err(EngineError::KeyConflict("c".into()))
    );
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn reuse_policies_after_a_run_has_ended() {
    async fn double(ctx: WorkflowContext, n: u32) -> Result<u32, WorkflowError> {
        ctx.step("tick", &(n * 2)).await
    }
    let dir = temp("reuse");
    let engine = Engine::builder(&dir)
        .workflow("double", Arc::new(workflow(double)))
        .step("tick", |n: &Payload| StepOutcome::Completed(n.clone()))
        // Checkpoint after every record, so finished runs leave memory and are found in the checkpoint.
        .checkpoint_every(1)
        .open()
        .unwrap();
    engine.start("k", "double", json("1")).unwrap();
    assert_eq!(
        engine.wait("k", Duration::from_secs(10)).unwrap().output,
        Some(json("2"))
    );
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        engine.stats().unwrap().runs_in_memory,
        0,
        "the finished run left memory"
    );
    assert_eq!(
        engine.start_with("k", "double", json("1"), ReusePolicy::Reject),
        Err(EngineError::AlreadyStarted("k".into()))
    );
    // Allowed after completion: a new run (generation 1) with a new input.
    engine
        .start_with("k", "double", json("5"), ReusePolicy::AllowAfterCompletion)
        .unwrap();
    assert_eq!(
        engine.wait("k", Duration::from_secs(10)).unwrap().output,
        Some(json("10"))
    );
    // The key now means the new run: its retried start returns it, the old input conflicts.
    engine.start("k", "double", json("5")).unwrap();
    assert_eq!(
        engine.start("k", "double", json("1")),
        Err(EngineError::KeyConflict("k".into()))
    );
    assert!(matches!(
        engine.start("bad\0key", "double", json("1")),
        Err(EngineError::InvalidKey(_))
    ));
    // After a restart, the key still means its latest run.
    drop(engine);
    let engine = Engine::builder(&dir)
        .workflow("double", Arc::new(workflow(double)))
        .open()
        .unwrap();
    assert_eq!(
        engine.wait("k", Duration::from_secs(10)).unwrap().output,
        Some(json("10"))
    );
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn an_awakeable_of_an_earlier_run_never_reaches_a_later_one() {
    async fn twice(ctx: WorkflowContext, round: u32) -> Result<String, WorkflowError> {
        let (id, answer) = ctx.awakeable::<String>();
        let _: () = ctx.step("publish", &id).await?;
        if round == 0 {
            // The first run hands out an id and moves on without waiting for it.
            return Err(ctx.continue_as_new(&1));
        }
        answer.await
    }
    let published: Arc<Mutex<Vec<String>>> = Arc::default();
    let sink = Arc::clone(&published);
    let dir = temp("awakeable");
    let engine = Engine::builder(&dir)
        .workflow("twice", Arc::new(workflow(twice)))
        .step("publish", move |id: &Payload| {
            sink.lock()
                .unwrap()
                .push(serde_json::from_slice(id.as_bytes()).unwrap());
            StepOutcome::Completed(json("null"))
        })
        .open()
        .unwrap();
    engine.start("a", "twice", json("0")).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while published.lock().unwrap().len() < 2 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    let ids = published.lock().unwrap().clone();
    assert_eq!(ids, ["a#0", "a#1.0"], "ids name their generation");
    // The first run has ended: its awakeable's completion is dropped, not delivered to the second.
    engine
        .complete_awakeable(&ids[0], json("\"stale\""))
        .unwrap();
    engine
        .complete_awakeable(&ids[1], json("\"fresh\""))
        .unwrap();
    let done = engine.wait("a", Duration::from_secs(10)).unwrap();
    assert_eq!(done.output, Some(json("\"fresh\"")));
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}
