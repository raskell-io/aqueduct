//! Remote workflow workers under faults (ADR 0039), simulated deterministically: a partition whose
//! workflow is decided remotely, its activation queue under a simulated clock, and workers that decide
//! from the history they are sent, verified against its hash chain. They lose decisions, answer late
//! after their lease ran out, answer twice, answer an old activation after a newer one, and crash; the
//! server crashes and recovers from its log, losing the queue. Every run must complete with the result
//! a local decider gives, and never park (a stale decision applied would park it).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use aqueduct_core::{Decider, Entry, Payload, RunState, Status, StepOutcome};
use aqueduct_engine::activations::{Activation, ActivationQueue};
use aqueduct_engine::{Deciders, Outbound, Partition};
use aqueduct_procedure::Procedure;

mod common;
use common::MemLog;

const PIPELINE: &str = r#"
def run(n):
    a = step("double", n)
    b, c = parallel([["square", a], ["double", a]])
    approval = wait(signal("go"), timeout = 3600)
    return step("sum", [b, c, 1 if approval else 0])
"#;

fn execute(step: &str, input: &Payload) -> Payload {
    let value: serde_json::Value = serde_json::from_slice(input.as_bytes()).unwrap();
    let out = match step {
        "double" => value.as_i64().unwrap() * 2,
        "square" => value.as_i64().unwrap().pow(2),
        "sum" => value
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap())
            .sum(),
        other => panic!("unknown step {other}"),
    };
    Payload::new(out.to_string().into_bytes())
}

fn expected(n: i64) -> i64 {
    let a = n * 2;
    a * a + a * 2 + 1
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const LEASE_MS: u64 = 1_000;

fn open(log: MemLog) -> Partition<MemLog> {
    let mut deciders = Deciders::new();
    deciders.insert_remote("pipeline");
    Partition::recover(log, deciders).unwrap()
}

/// Flushes the partition: activations go to the queue (with their history, chained to the run id), and
/// steps run at once.
fn flush(partition: &mut Partition<MemLog>, queue: &ActivationQueue, now: u64) {
    loop {
        let messages = partition.flush().unwrap();
        let mut steps = Vec::new();
        for message in messages {
            match message {
                Outbound::Activation {
                    key,
                    workflow,
                    at,
                    env,
                } => {
                    let Some(run) = partition.run(&key) else {
                        continue;
                    };
                    let mut chain = aqueduct_codec::ChainWriter::new(&key);
                    let history = run.journal()[..at]
                        .iter()
                        .map(|e| chain.append(e))
                        .collect();
                    queue.offer(&key, &workflow, at, history, env, now);
                }
                Outbound::Dispatch {
                    key,
                    cmd,
                    name,
                    input,
                } => steps.push((key, cmd, execute(&name, &input))),
                _ => {}
            }
        }
        if steps.is_empty() {
            return;
        }
        for (key, cmd, result) in steps {
            partition
                .step_result(&key, cmd, StepOutcome::Completed(result))
                .unwrap();
        }
    }
}

/// What a workflow worker does with an activation: verify the history, replay it, decide.
fn decide(decider: &Procedure, activation: &Activation) -> Vec<aqueduct_core::Command> {
    let verified = aqueduct_codec::verify_chain(
        &activation.run,
        activation.history.iter().map(Vec::as_slice),
    )
    .unwrap();
    let state = RunState::from_journal(&verified.entries).unwrap();
    decider.decide(&state.view_at(&activation.run, activation.env))
}

fn run_seed(seed: u64) -> Result<(), String> {
    let decider = Procedure::compile("pipeline.star", PIPELINE).unwrap();
    let mut rng = Rng(seed);
    let runs: Vec<i64> = (0..1 + rng.below(4) as i64).collect();
    let mut partition = open(MemLog::default());
    let mut queue = ActivationQueue::new(Duration::from_millis(LEASE_MS));
    let mut now = 1_000;
    partition.set_time(now);
    for n in &runs {
        partition
            .start(
                &format!("p-{n}"),
                "pipeline",
                Payload::new(n.to_string().into_bytes()),
            )
            .unwrap();
    }
    flush(&mut partition, &queue, now);
    let workflows = vec!["pipeline".to_owned()];
    let workers = ["w-1", "w-2"];
    // What workers hold: (worker, activation, when the decision arrives).
    let mut held: Vec<(usize, Activation, u64)> = Vec::new();
    let mut signalled = false;

    for _ in 0..3_000 {
        let done = runs.iter().all(|n| {
            partition
                .run(&format!("p-{n}"))
                .is_some_and(|run| run.state().status().is_terminal())
        });
        if done {
            break;
        }
        now += 1 + rng.below(300);
        partition.set_time(now);
        partition.fire_due_timers().unwrap();
        match rng.below(1_000) {
            0..=4 => {
                // The server crashes and recovers; the queue is lost with it.
                let log = partition.into_log();
                partition = open(log);
                partition.set_time(now);
                queue = ActivationQueue::new(Duration::from_millis(LEASE_MS));
            }
            5..=24 => {
                let worker = rng.below(2) as usize;
                held.retain(|(w, _, _)| *w != worker);
                queue.release(workers[worker]);
            }
            25..=60 if !signalled => {
                // The signal every run waits for, once.
                for n in &runs {
                    partition
                        .signal(&format!("p-{n}"), "go", "go-1", Payload::new(*b"true"))
                        .unwrap();
                }
                signalled = true;
            }
            _ => {
                let worker = rng.below(2) as usize;
                for activation in queue.take(workers[worker], &workflows, 2, now) {
                    let delay = if rng.below(10) == 0 {
                        LEASE_MS + rng.below(LEASE_MS) // slow: after the lease
                    } else {
                        rng.below(LEASE_MS / 2)
                    };
                    held.push((worker, activation, now + delay));
                }
            }
        }
        // Decisions that are due arrive in a random order (old ones may come after newer ones).
        let (mut due, waiting): (Vec<_>, Vec<_>) =
            held.drain(..).partition(|(_, _, at)| *at <= now);
        held = waiting;
        for i in (1..due.len()).rev() {
            due.swap(i, rng.below(i as u64 + 1) as usize);
        }
        for (_, activation, _) in due {
            if rng.below(5) == 0 {
                continue; // lost
            }
            let commands = decide(&decider, &activation);
            let times = if rng.below(10) == 0 { 2 } else { 1 };
            for _ in 0..times {
                if let Some((run, at)) = queue.complete(&activation.id) {
                    partition.decision(&run, at, commands.clone()).unwrap();
                }
            }
        }
        flush(&mut partition, &queue, now);
    }

    for n in &runs {
        let key = format!("p-{n}");
        let run = partition
            .run(&key)
            .ok_or(format!("seed {seed}: {key} is gone"))?;
        let status = run.state().status();
        if status != Status::Completed {
            return Err(format!(
                "seed {seed}: {key} is {status:?} ({:?}), {} activations pending",
                run.state().error(),
                queue.len()
            ));
        }
        if run.state().output().unwrap().as_bytes() != expected(*n).to_string().as_bytes() {
            return Err(format!(
                "seed {seed}: {key} gave {:?}",
                run.state().output()
            ));
        }
        if run
            .journal()
            .iter()
            .any(|e| matches!(e, Entry::Parked { .. }))
        {
            return Err(format!("seed {seed}: {key} was parked"));
        }
    }
    Ok(())
}

#[test]
fn remote_decisions_under_faults_complete_every_run() {
    let failures: Vec<String> = (0..1_000).filter_map(|seed| run_seed(seed).err()).collect();
    assert!(
        failures.is_empty(),
        "{} seeds failed: {:#?}",
        failures.len(),
        &failures[..failures.len().min(5)]
    );
}

/// A decision computed for an older history, applied after newer ones, would park the run (it issues
/// fewer commands than the journal has). The partition drops it, and activates the run again. (The queue
/// also forgets activations once a newer one is decided, so the simulation above rarely gets this far;
/// this checks the partition on its own.)
#[test]
fn a_stale_decision_is_dropped_not_applied() {
    let decider = Procedure::compile("pipeline.star", PIPELINE).unwrap();
    let mut partition = open(MemLog::default());
    let queue = ActivationQueue::new(Duration::from_millis(LEASE_MS));
    let workflows = ["pipeline".to_owned()];
    partition
        .start("p", "pipeline", Payload::new(*b"3"))
        .unwrap();
    flush(&mut partition, &queue, 0);
    // The first decision (one step), then the next (after that step completed: three more).
    let first = queue.take("w", &workflows, 1, 0).remove(0);
    let first_commands = decide(&decider, &first);
    let (_, first_at) = queue.complete(&first.id).unwrap();
    partition
        .decision("p", first_at, first_commands.clone())
        .unwrap();
    flush(&mut partition, &queue, 0);
    let second = queue.take("w", &workflows, 1, 0).remove(0);
    assert!(second.at > first_at);
    let second_commands = decide(&decider, &second);
    assert!(second_commands.len() > first_commands.len());
    let (_, second_at) = queue.complete(&second.id).unwrap();
    partition.decision("p", second_at, second_commands).unwrap();
    flush(&mut partition, &queue, 0);
    // The first decision arrives again, late: applied, it would park the run.
    let before = partition.run("p").unwrap().journal().len();
    partition.decision("p", first_at, first_commands).unwrap();
    let run = partition.run("p").unwrap();
    assert_eq!(
        run.state().status(),
        Status::Running,
        "{:?}",
        run.state().error()
    );
    assert_eq!(run.journal().len(), before);
    // And the run is activated again for where it is.
    flush(&mut partition, &queue, 0);
    assert_eq!(queue.len(), 1);
}
