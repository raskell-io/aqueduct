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

#[test]
fn recorded_values_are_taken_once_and_replayed() {
    use aqueduct_core::Env;
    use std::cell::Cell;

    thread_local! { static READS: Cell<u32> = const { Cell::new(0) }; }
    async fn observe(ctx: WorkflowContext, _: ()) -> Result<String, WorkflowError> {
        let at = ctx.now();
        let id = ctx.uuid();
        let roll = ctx.random() % 6;
        let region: String = ctx.record("region", || {
            READS.with(|reads| reads.set(reads.get() + 1));
            "eu-west".to_owned()
        })?;
        let ms = at
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let echoed: String = ctx.step("echo", &id.to_string()).await?;
        Ok(format!("{ms} {id} {roll} {region} {echoed}"))
    }
    let decider = workflow(observe);
    let mut state = RunState::from_journal(&start("w", json("null")).entries).unwrap();
    let apply = |state: &mut RunState, event| {
        let transition = state.handle(event);
        for entry in &transition.entries {
            state.apply(entry).unwrap();
        }
        transition
    };

    let first = Env {
        now_ms: 1_700_000_000_000,
        seed: 42,
    };
    let commands = decider.decide(&state.view_at(first));
    assert_eq!(commands.len(), 5, "four values and the step: {commands:?}");
    let transition = apply(&mut state, Event::Decision(commands));
    assert_eq!(
        transition
            .entries
            .iter()
            .filter(|e| matches!(e, Entry::ValueRecorded { .. }))
            .count(),
        4
    );
    let Some(Entry::StepScheduled { input, .. }) = transition.entries.last() else {
        panic!("the step is scheduled after the values: {transition:?}");
    };
    let id: String = serde_json::from_slice(input.as_bytes()).unwrap();
    assert_eq!(uuid_version(&id), 4);
    let _ = apply(
        &mut state,
        Event::StepResult {
            cmd: aqueduct_core::CommandIndex(4),
            outcome: StepOutcome::Completed(input.clone()),
        },
    );

    // A later activation with another clock and seed reads the journal, and `record` does not run again.
    let later = Env {
        now_ms: 1_800_000_000_000,
        seed: 7,
    };
    let reads_before = READS.with(Cell::get);
    let commands = decider.decide(&state.view_at(later));
    let transition = apply(&mut state, Event::Decision(commands));
    assert_eq!(
        READS.with(Cell::get),
        reads_before,
        "replay does not recompute"
    );
    assert_eq!(state.status(), Status::Completed, "{transition:?}");
    let output = String::from_utf8(state.output().unwrap().as_bytes().to_vec()).unwrap();
    assert!(
        output.starts_with(&format!("\"1700000000000 {id} ")) && output.contains(" eu-west "),
        "{output}"
    );
    assert!(output.ends_with(&format!(" {id}\"")), "{output}");
}

#[test]
fn every_run_gets_its_own_uuid() {
    async fn id(ctx: WorkflowContext, _: ()) -> Result<String, WorkflowError> {
        Ok(ctx.uuid().to_string())
    }
    let dir = std::env::temp_dir().join(format!("aqueduct-sdk-uuid-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Engine::builder(&dir)
        .workflow("id", Arc::new(workflow(id)))
        .open()
        .unwrap();
    let mut ids = std::collections::BTreeSet::new();
    for n in 0..20 {
        let key = format!("r{n}");
        engine.start(&key, "id", json("null")).unwrap();
        let done = engine.wait(&key, Duration::from_secs(10)).unwrap();
        assert_eq!(done.status, Status::Completed);
        ids.insert(done.output.unwrap().as_bytes().to_vec());
    }
    assert_eq!(ids.len(), 20);
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}

fn uuid_version(id: &str) -> u32 {
    id.chars().nth(14).and_then(|c| c.to_digit(16)).unwrap()
}
