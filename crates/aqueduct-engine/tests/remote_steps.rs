//! Remote step workers under network faults (ADR 0038), simulated deterministically: a partition and
//! its step queue under a simulated clock, with workers that answer, drop results, answer late after
//! their lease ran out, answer twice, crash, or ask for a retry; and a server that crashes and recovers
//! from its log, losing the queue. Every run must end with the result it would have without faults, and
//! no step may have two recorded outcomes.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use aqueduct_core::{CommandIndex, Entry, Payload, Status, StepOutcome};
use aqueduct_engine::queue::{Invocation, StepQueue};
use aqueduct_engine::{Deciders, Outbound, Partition, RetryPolicy};
use aqueduct_procedure::Procedure;

mod common;
use common::MemLog;

const PIPELINE: &str = r#"
def run(n):
    a = step("double", n)
    b, c = parallel([["square", a], ["double", a]])
    return step("sum", [b, c])
"#;

/// What each step computes, so the expected output is known.
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
    a * a + a * 2
}

/// SplitMix64: a small, seeded random number generator.
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
    deciders.insert(
        "pipeline",
        Arc::new(Procedure::compile("pipeline.star", PIPELINE).unwrap()),
    );
    let mut partition = Partition::recover(log, deciders).unwrap();
    partition.set_default_retry_policy(RetryPolicy {
        max_attempts: 1_000,
        initial_backoff_ms: 10,
        multiplier: 2,
        max_backoff_ms: 200,
    });
    partition
}

/// What a worker holds: an invocation it took, and when (if ever) its answer arrives.
struct Held {
    invocation: Invocation,
    answer_at: u64,
    retry: bool,
    twice: bool,
}

/// Flushes the partition and offers its dispatches to the queue.
fn flush(partition: &mut Partition<MemLog>, queue: &StepQueue, now: u64) {
    let messages = partition.flush().unwrap();
    for message in messages {
        if let Outbound::Dispatch {
            key,
            cmd,
            name,
            input,
        } = message
        {
            let attempt = partition
                .run(&key)
                .and_then(|run| run.state().steps().get(cmd.0 as usize))
                .map_or(1, |step| step.failed_attempts + 1);
            queue.offer(&key, cmd, &name, input, attempt, now);
        }
    }
}

fn run_seed(seed: u64) -> Result<(), String> {
    let mut rng = Rng(seed);
    let runs: Vec<i64> = (0..1 + rng.below(4) as i64).collect();
    let mut partition = open(MemLog::default());
    let mut queue = StepQueue::new(Duration::from_millis(LEASE_MS));
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
    let steps: Vec<String> = ["double", "square", "sum"].map(str::to_owned).to_vec();
    let workers = ["w-1", "w-2", "w-3"];
    let mut held: Vec<(usize, Held)> = Vec::new();
    let mut executions = 0;

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
        match rng.below(2_000) {
            // The server crashes: what was not durable is lost, and so is the queue (it lives in
            // memory). Workers keep what they hold; their answers go to a queue that never issued them.
            0 => {
                let log = partition.into_log();
                partition = open(log);
                partition.set_time(now);
                queue = StepQueue::new(Duration::from_millis(LEASE_MS));
            }
            // The engine re-sends its dispatches, as it does periodically.
            1..=100 => partition.redispatch(),
            // A worker crashes: what it holds is lost, and its session ends.
            101..=120 => {
                let worker = rng.below(3) as usize;
                held.retain(|(w, _)| *w != worker);
                queue.release(workers[worker]);
            }
            // A worker takes work.
            _ => {
                let worker = rng.below(3) as usize;
                let max = 1 + rng.below(3) as usize;
                for invocation in queue.take(workers[worker], &steps, max, now) {
                    let delay = match rng.below(10) {
                        // Slow: answers after its lease ran out.
                        0 => LEASE_MS + rng.below(LEASE_MS),
                        _ => rng.below(LEASE_MS / 2),
                    };
                    held.push((
                        worker,
                        Held {
                            invocation,
                            answer_at: now + delay,
                            retry: rng.below(8) == 0,
                            twice: rng.below(10) == 0,
                        },
                    ));
                }
            }
        }
        // Answers that are due, some lost on the way.
        let (due, waiting): (Vec<_>, Vec<_>) =
            held.drain(..).partition(|(_, h)| h.answer_at <= now);
        held = waiting;
        for (_, answer) in due {
            executions += 1;
            if rng.below(4) == 0 {
                continue; // the result is lost
            }
            for _ in 0..if answer.twice { 2 } else { 1 } {
                let Some((run, cmd)) = queue.complete(&answer.invocation.id) else {
                    continue;
                };
                if answer.retry {
                    partition
                        .step_attempt_failed(&run, cmd, "busy, try again".into())
                        .unwrap();
                } else {
                    let result = execute(&answer.invocation.step, &answer.invocation.input);
                    partition
                        .step_result(&run, cmd, StepOutcome::Completed(result))
                        .unwrap();
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
        if run.state().status() != Status::Completed {
            return Err(format!(
                "seed {seed}: {key} did not complete ({:?}, {executions} executions, queue {:?})",
                run.state().status(),
                queue.waiting()
            ));
        }
        let output = run.state().output().unwrap();
        if output.as_bytes() != expected(*n).to_string().as_bytes() {
            return Err(format!("seed {seed}: {key} gave {output:?}"));
        }
        // At most one outcome per command (the core refuses a second; this checks it held).
        let mut outcomes = std::collections::BTreeMap::<CommandIndex, usize>::new();
        for entry in run.journal() {
            if let Entry::StepCompleted { cmd, .. } | Entry::StepFailed { cmd, .. } = entry {
                *outcomes.entry(*cmd).or_default() += 1;
            }
        }
        if outcomes.values().any(|count| *count > 1) {
            return Err(format!("seed {seed}: {key} recorded a step twice"));
        }
    }
    Ok(())
}

#[test]
fn steps_on_faulty_remote_workers_complete_exactly_once() {
    let failures: Vec<String> = (0..1_000).filter_map(|seed| run_seed(seed).err()).collect();
    assert!(
        failures.is_empty(),
        "{} seeds failed: {:#?}",
        failures.len(),
        &failures[..failures.len().min(5)]
    );
}
