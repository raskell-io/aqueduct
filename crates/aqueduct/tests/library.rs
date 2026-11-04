//! The crate as a library: Rust workflows need no procedures, and the engine works from async code.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use aqueduct::embedded::{Attempt, Engine, StepCall};
use aqueduct::{Payload, Status, WorkflowContext, WorkflowError, workflow};

async fn order(ctx: WorkflowContext, amount: u32) -> Result<String, WorkflowError> {
    let receipt: String = ctx.step("charge", &amount).await?;
    ctx.publish("receipt", &receipt)?;
    let shipped: Option<bool> = ctx
        .wait_signal("ship", Some(Duration::from_secs(600)))
        .await?;
    Ok(format!("{receipt}, shipped {shipped:?}"))
}

fn builder(dir: &std::path::Path) -> aqueduct::embedded::Builder {
    Engine::builder(dir)
        .workflow("order", Arc::new(workflow(order)))
        .step_with("charge", |call: &StepCall<'_>| {
            let key = call.idempotency_key();
            Attempt::Completed(Payload::new(
                format!("\"charged under {key}\"").into_bytes(),
            ))
        })
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("aqueduct-library-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[test]
fn a_rust_workflow_runs_on_the_blocking_api() {
    let dir = temp_dir("blocking");
    let engine = builder(&dir).open().unwrap();
    engine.start("o-1", "order", Payload::new(*b"5")).unwrap();
    engine
        .signal("o-1", "ship", "", Payload::new(*b"true"))
        .unwrap();
    let result = engine.wait("o-1", Duration::from_secs(10)).unwrap();
    assert_eq!(result.status, Status::Completed, "{:?}", result.error);
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// On a single-threaded runtime: a blocking call on the async thread would stop the signal below from
/// ever being sent, and the wait would time out.
#[cfg(feature = "tokio")]
#[tokio::test(flavor = "current_thread")]
async fn the_engine_works_from_async_code() {
    use aqueduct::embedded::{AsyncEngine, EngineError};

    let dir = temp_dir("async");
    let engine = AsyncEngine::new(builder(&dir).open().unwrap());
    engine
        .start("o-2", "order", Payload::new(*b"7"))
        .await
        .unwrap();
    let waiting = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.wait("o-2", Duration::from_secs(10)).await })
    };
    tokio::task::yield_now().await;
    engine
        .signal("o-2", "ship", "s-1", Payload::new(*b"true"))
        .await
        .unwrap();
    let result = waiting.await.unwrap().unwrap();
    assert_eq!(result.status, Status::Completed, "{:?}", result.error);
    let output: String = serde_json::from_slice(result.output.unwrap().as_bytes()).unwrap();
    assert_eq!(output, "charged under o-2/0/0, shipped Some(true)");
    assert!(matches!(
        engine.query("nope", "receipt").await,
        Err(EngineError::UnknownRun(_))
    ));
    let (run, state) = engine.describe("o-2").await.unwrap();
    assert_eq!((run.as_str(), state.status()), ("o-2", Status::Completed));
    assert_eq!(engine.runs().await.unwrap().len(), 1);
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}
