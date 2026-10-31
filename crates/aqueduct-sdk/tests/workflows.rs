//! Rust workflows on the embedded engine, and divergence detection.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use aqueduct_core::{
    Command, Decider, Entry, Event, Payload, RunState, Status, StepOutcome, start,
};
use aqueduct_engine::embedded::Engine;
use aqueduct_sdk::{WorkflowContext, WorkflowError, join, join_all, workflow};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Order {
    id: String,
    items: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Shipped {
    id: String,
    prices: Vec<u32>,
    approved: bool,
}

async fn ship(ctx: WorkflowContext, order: Order) -> Result<Shipped, WorkflowError> {
    // Price every item concurrently.
    let prices = join_all(
        order
            .items
            .iter()
            .map(|item| ctx.step::<u32, _>("price", item))
            .collect(),
    )
    .await;
    let prices = prices.into_iter().collect::<Result<Vec<_>, _>>()?;
    let total: u32 = prices.iter().sum();
    let approved = if total > 100 {
        ctx.wait_signal::<bool>("approve", Some(Duration::from_secs(1)))
            .await?
            .unwrap_or(false)
    } else {
        true
    };
    if !approved {
        return Err(WorkflowError::failed("not approved"));
    }
    ctx.sleep(Duration::from_millis(300)).await;
    let (label, _) = join(
        ctx.step::<String, _>("label", &order.id),
        ctx.step::<(), _>("notify", &order.id),
    )
    .await;
    label?;
    Ok(Shipped {
        id: order.id,
        prices,
        approved,
    })
}

fn json(value: &str) -> Payload {
    Payload::new(value.as_bytes().to_vec())
}

fn engine(dir: &std::path::Path) -> Engine {
    Engine::builder(dir)
        .workflow("ship", Arc::new(workflow(ship)))
        .step("price", |item: &Payload| {
            let item: String = serde_json::from_slice(item.as_bytes()).unwrap();
            StepOutcome::Completed(json(&(item.len() as u32 * 10).to_string()))
        })
        .step("label", |id: &Payload| {
            StepOutcome::Completed(Payload::new(id.as_bytes().to_vec()))
        })
        .step("notify", |_: &Payload| StepOutcome::Completed(json("null")))
        .open()
        .unwrap()
}

#[test]
fn a_rust_workflow_runs_with_steps_signals_and_sleeps() {
    let dir = std::env::temp_dir().join(format!("aqueduct-sdk-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let started = Instant::now();
    {
        let engine = engine(&dir);
        engine
            .start(
                "small",
                "ship",
                json(r#"{"id": "small", "items": ["pen"]}"#),
            )
            .unwrap();
        engine
            .start(
                "big",
                "ship",
                json(r#"{"id": "big", "items": ["sofa", "armchair", "bookshelf"]}"#),
            )
            .unwrap();
        engine
            .start(
                "refused",
                "ship",
                json(r#"{"id": "refused", "items": ["piano", "harpsichord"]}"#),
            )
            .unwrap();
        engine
            .signal("big", "approve", "a-1", json("true"))
            .unwrap();
        // Stop while the runs sleep or wait; they finish after reopening.
        std::thread::sleep(Duration::from_millis(100));
    }
    let engine = engine(&dir);
    let small = engine.wait("small", Duration::from_secs(10)).unwrap();
    assert_eq!(
        small.output,
        Some(json(r#"{"id":"small","prices":[30],"approved":true}"#))
    );
    let big = engine.wait("big", Duration::from_secs(10)).unwrap();
    assert_eq!(
        big.output,
        Some(json(r#"{"id":"big","prices":[40,80,90],"approved":true}"#))
    );
    let refused = engine.wait("refused", Duration::from_secs(10)).unwrap();
    assert_eq!(
        (refused.status, refused.error.as_deref()),
        (Status::Failed, Some("not approved"))
    );
    assert!(
        started.elapsed() >= Duration::from_millis(300),
        "the sleep was not skipped"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn changing_a_workflow_under_a_running_run_parks_it() {
    async fn v1(ctx: WorkflowContext, x: u32) -> Result<u32, WorkflowError> {
        ctx.step("a", &x).await
    }
    async fn v2(ctx: WorkflowContext, x: u32) -> Result<u32, WorkflowError> {
        ctx.step("b", &x).await
    }
    let mut state = RunState::from_journal(&start("w", json("1")).entries).unwrap();
    for entry in state
        .handle(Event::Decision(workflow(v1).decide(&state.view())))
        .entries
    {
        state.apply(&entry).unwrap();
    }
    let transition = state.handle(Event::Decision(workflow(v2).decide(&state.view())));
    assert!(matches!(transition.entries[..], [Entry::Parked { .. }]));
}

#[test]
fn concurrent_calls_are_issued_together_and_bad_input_parks() {
    async fn both(ctx: WorkflowContext, x: u32) -> Result<u32, WorkflowError> {
        let (a, b) = join(ctx.step::<u32, _>("a", &x), ctx.step::<u32, _>("b", &x)).await;
        Ok(a? + b?)
    }
    let state = RunState::from_journal(&start("w", json("1")).entries).unwrap();
    let commands = workflow(both).decide(&state.view());
    assert_eq!(
        commands.len(),
        2,
        "both steps are issued at once: {commands:?}"
    );

    let state = RunState::from_journal(&start("w", json("\"not a number\"")).entries).unwrap();
    assert!(matches!(
        workflow(both).decide(&state.view())[..],
        [Command::Park { .. }]
    ));
}
