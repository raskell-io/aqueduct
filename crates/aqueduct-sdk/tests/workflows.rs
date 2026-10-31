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
    let commands = decider.decide(&state.view_at("r", first));
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
    let commands = decider.decide(&state.view_at("r", later));
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

mod select_and_awakeables {
    use super::*;
    use aqueduct_core::CommandIndex;
    use aqueduct_sdk::{Either, select, select_all};
    use std::sync::Mutex;

    async fn race(ctx: WorkflowContext, _: ()) -> Result<String, WorkflowError> {
        Ok(
            match select(
                ctx.step::<String, _>("a", &()),
                ctx.step::<String, _>("b", &()),
            )
            .await
            {
                Either::Left(a) => format!("a:{}", a?),
                Either::Right(b) => format!("b:{}", b?),
            },
        )
    }

    fn apply(state: &mut RunState, event: Event) {
        let transition = state.handle(event);
        for entry in &transition.entries {
            state.apply(entry).unwrap();
        }
    }

    fn activate(state: &mut RunState, decider: &impl Decider) {
        let commands = decider.decide(&state.view());
        apply(state, Event::Decision(commands));
    }

    fn result(cmd: u32, value: &str) -> Event {
        Event::StepResult {
            cmd: CommandIndex(cmd),
            outcome: StepOutcome::Completed(json(&format!("\"{value}\""))),
        }
    }

    /// Both steps finish before the workflow is activated again, in the given order.
    fn race_with(order: [u32; 2]) -> String {
        let decider = workflow(race);
        let mut state = RunState::from_journal(&start("w", json("null")).entries).unwrap();
        let commands = decider.decide(&state.view());
        assert_eq!(commands.len(), 2, "both branches are issued: {commands:?}");
        apply(&mut state, Event::Decision(commands));
        for cmd in order {
            apply(&mut state, result(cmd, &format!("r{cmd}")));
        }
        activate(&mut state, &decider);
        assert_eq!(state.status(), Status::Completed);
        String::from_utf8(state.output().unwrap().as_bytes().to_vec()).unwrap()
    }

    #[test]
    fn the_winner_is_the_first_outcome_in_the_journal_not_the_first_branch() {
        assert_eq!(race_with([0, 1]), "\"a:r0\"");
        assert_eq!(race_with([1, 0]), "\"b:r1\"");
    }

    #[test]
    fn a_replay_that_finds_both_finished_takes_the_same_branch() {
        // `b` finishes, the workflow chooses it and goes on; then `a` finishes. Replaying from the full
        // journal must choose `b` again, or the run would diverge.
        async fn then_more(ctx: WorkflowContext, _: ()) -> Result<String, WorkflowError> {
            let (position, first) = select_all(vec![
                ctx.step::<String, _>("a", &()),
                ctx.step::<String, _>("b", &()),
            ])
            .await;
            let next: String = ctx.step("after", &position).await?;
            Ok(format!("{}{next}", first?))
        }
        let decider = workflow(then_more);
        let mut state = RunState::from_journal(&start("w", json("null")).entries).unwrap();
        activate(&mut state, &decider);
        apply(&mut state, result(1, "b"));
        activate(&mut state, &decider);
        apply(&mut state, result(0, "a"));
        activate(&mut state, &decider);
        assert_eq!(state.status(), Status::Running, "{:?}", state.error());
        let Some(step) = state.steps().get(2) else {
            panic!("the next step is issued");
        };
        assert_eq!(
            (step.name.as_str(), step.input.as_bytes()),
            ("after", &b"1"[..])
        );
        apply(&mut state, result(2, "!"));
        activate(&mut state, &decider);
        assert_eq!(state.output(), Some(&json("\"b!\"")));
    }

    #[test]
    fn a_sleep_bounds_a_step() {
        async fn bounded(ctx: WorkflowContext, _: ()) -> Result<String, WorkflowError> {
            match select(
                ctx.step::<String, _>("slow", &()),
                ctx.sleep(Duration::from_secs(1)),
            )
            .await
            {
                Either::Left(done) => done,
                Either::Right(()) => Ok("gave up".into()),
            }
        }
        let decider = workflow(bounded);
        let mut state = RunState::from_journal(&start("w", json("null")).entries).unwrap();
        activate(&mut state, &decider);
        apply(
            &mut state,
            Event::TimerFired {
                cmd: CommandIndex(1),
            },
        );
        apply(&mut state, result(0, "late"));
        activate(&mut state, &decider);
        assert_eq!(state.output(), Some(&json("\"gave up\"")));
    }

    #[test]
    fn an_awakeable_is_completed_from_outside_once() {
        async fn approval(ctx: WorkflowContext, user: String) -> Result<String, WorkflowError> {
            let (id, decision) = ctx.awakeable::<String>();
            let _: () = ctx.step("send_link", &(user, id)).await?;
            match select(decision, ctx.sleep(Duration::from_secs(30))).await {
                Either::Left(decision) => decision,
                Either::Right(()) => Ok("expired".into()),
            }
        }
        let links: Arc<Mutex<Vec<String>>> = Arc::default();
        let dir = std::env::temp_dir().join(format!("aqueduct-sdk-awk-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let open = |links: Arc<Mutex<Vec<String>>>| {
            Engine::builder(&dir)
                .workflow("approval", Arc::new(workflow(approval)))
                .step("send_link", move |input: &Payload| {
                    let (_, id): (String, String) =
                        serde_json::from_slice(input.as_bytes()).unwrap();
                    links.lock().unwrap().push(id);
                    StepOutcome::Completed(json("null"))
                })
                .open()
                .unwrap()
        };
        let engine = open(Arc::clone(&links));
        engine
            .start("approve:ada", "approval", json("\"ada\""))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let id = loop {
            if let Some(id) = links.lock().unwrap().first().cloned() {
                break id;
            }
            assert!(Instant::now() < deadline, "the link was never sent");
            std::thread::sleep(Duration::from_millis(5));
        };
        assert_eq!(id, "approve:ada#0");
        // Restart before completing: the id still works.
        drop(engine);
        let engine = open(Arc::clone(&links));
        engine
            .complete_awakeable(&id, json("\"approved\""))
            .unwrap();
        engine
            .complete_awakeable(&id, json("\"rejected\""))
            .unwrap();
        let done = engine.wait("approve:ada", Duration::from_secs(10)).unwrap();
        assert_eq!(done.output, Some(json("\"approved\"")));
        assert!(engine.complete_awakeable("not-an-id", json("1")).is_err());
        drop(engine);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
