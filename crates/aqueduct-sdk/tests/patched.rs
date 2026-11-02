//! `ctx.patched` (ADR 0035): a code workflow changed under a running run, across a restart.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use aqueduct_core::{Payload, Status};
use aqueduct_engine::embedded::{Attempt, Engine};
use aqueduct_sdk::{WorkflowContext, WorkflowError, workflow};

async fn v1(ctx: WorkflowContext, order: u32) -> Result<Vec<String>, WorkflowError> {
    let mut done = vec![ctx.step::<String, _>("charge", &order).await?];
    done.push(ctx.step("ship", &order).await?);
    ctx.wait_signal::<bool>("delivered", None).await?;
    Ok(done)
}

async fn v2(ctx: WorkflowContext, order: u32) -> Result<Vec<String>, WorkflowError> {
    let mut done = vec![ctx.step::<String, _>("charge", &order).await?];
    if ctx.patched("review") {
        done.push(ctx.step("review", &order).await?);
    }
    done.push(ctx.step("ship", &order).await?);
    ctx.wait_signal::<bool>("delivered", None).await?;
    Ok(done)
}

#[test]
fn a_running_run_keeps_its_path_when_the_code_changes() {
    let dir = std::env::temp_dir().join(format!("aqueduct-sdk-patched-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let named = |name: &'static str| {
        move |_: &Payload| Attempt::Completed(Payload::new(format!("\"{name}\"").into_bytes()))
    };
    {
        let (shipped, wait_shipped) = mpsc::channel();
        let engine = Engine::builder(&dir)
            .workflow("order", Arc::new(workflow(v1)))
            .step("charge", named("charge"))
            .step("ship", move |_: &Payload| {
                let _ = shipped.send(());
                Attempt::Completed(Payload::new(*b"\"ship\""))
            })
            .open()
            .unwrap();
        engine.start("old", "order", Payload::new(*b"1")).unwrap();
        // Once `ship` runs, it is in the journal: the run is past the review's place.
        wait_shipped.recv_timeout(Duration::from_secs(10)).unwrap();
    }
    let engine = Engine::builder(&dir)
        .workflow("order", Arc::new(workflow(v2)))
        .step("charge", named("charge"))
        .step("review", named("review"))
        .step("ship", named("ship"))
        .open()
        .unwrap();
    engine.start("new", "order", Payload::new(*b"2")).unwrap();
    for key in ["old", "new"] {
        engine
            .signal(key, "delivered", "", Payload::new(*b"true"))
            .unwrap();
    }
    let wait = |key: &str| engine.wait(key, Duration::from_secs(10)).unwrap();
    let old = wait("old");
    assert_eq!(old.status, Status::Completed, "{:?}", old.error);
    assert_eq!(old.output.unwrap().as_bytes(), br#"["charge","ship"]"#);
    assert_eq!(
        wait("new").output.unwrap().as_bytes(),
        br#"["charge","review","ship"]"#
    );
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}
