//! Code workflows and procedures calling each other as child runs (ADR 0034).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use aqueduct_core::{Payload, Status};
use aqueduct_engine::embedded::{Attempt, Engine};
use aqueduct_procedure::Procedure;
use aqueduct_sdk::{WorkflowContext, WorkflowError, workflow};

/// A code workflow that hands the decision to a procedure, then acts on it.
async fn claim(ctx: WorkflowContext, amount: u64) -> Result<String, WorkflowError> {
    let decision: String = ctx.child("decide", &amount).await?;
    let paid: String = ctx.step("pay", &decision).await?;
    Ok(paid)
}

/// A code workflow a procedure calls.
async fn score(ctx: WorkflowContext, amount: u64) -> Result<u64, WorkflowError> {
    ctx.step("score", &amount).await
}

/// A code workflow whose child fails.
async fn strict(ctx: WorkflowContext, amount: u64) -> Result<String, WorkflowError> {
    match ctx.child::<String, _>("decide", &(amount * 1000)).await {
        Err(WorkflowError::ChildFailed { workflow, error }) => Ok(format!("{workflow}: {error}")),
        other => other,
    }
}

const DECIDE: &str = r#"
def run(amount):
    score = call("score", amount)
    if score > 5000:
        fail("too risky")
    return "approve" if score < 50 else "review"
"#;

#[test]
fn code_workflows_and_procedures_call_each_other() {
    let dir = std::env::temp_dir().join(format!("aqueduct-sdk-children-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Engine::builder(&dir)
        .workflow("claim", Arc::new(workflow(claim)))
        .workflow("strict", Arc::new(workflow(strict)))
        .workflow("score", Arc::new(workflow(score)))
        .workflow(
            "decide",
            Arc::new(Procedure::compile("decide.star", DECIDE).unwrap()),
        )
        .step("score", |amount: &Payload| {
            let amount: u64 = serde_json::from_slice(amount.as_bytes()).unwrap();
            Attempt::Completed(Payload::new((amount / 10).to_string().into_bytes()))
        })
        .step("pay", |decision: &Payload| {
            let decision: String = serde_json::from_slice(decision.as_bytes()).unwrap();
            let paid = serde_json::to_vec(&format!("paid after {decision}")).unwrap();
            Attempt::Completed(Payload::new(paid))
        })
        .open()
        .unwrap();
    engine
        .start("claim-1", "claim", Payload::new(*b"120"))
        .unwrap();
    engine
        .start("strict-1", "strict", Payload::new(*b"120"))
        .unwrap();
    let wait = |key: &str| engine.wait(key, Duration::from_secs(10)).unwrap();
    // claim (code) -> decide (procedure) -> score (code)
    let claim = wait("claim-1");
    assert_eq!(claim.status, Status::Completed, "{:?}", claim.error);
    assert_eq!(claim.output.unwrap().as_bytes(), br#""paid after approve""#);
    assert_eq!(wait("claim-1#0#0").output.unwrap().as_bytes(), b"12");
    let strict = wait("strict-1");
    assert_eq!(strict.output.unwrap().as_bytes(), br#""decide: too risky""#);
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}
