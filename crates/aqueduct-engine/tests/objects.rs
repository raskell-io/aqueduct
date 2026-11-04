//! Virtual objects (ADR 0043): an object's state is the sequential application of its committed
//! handlers, across compaction and crashes at every point.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use aqueduct_core::{Payload, Status, StepOutcome};
use aqueduct_engine::objects::{ObjectLoop, call_key, loop_workflow, object_run_key};
use aqueduct_engine::{Deciders, Outbound, Partition};
use aqueduct_procedure::Procedure;
use serde_json::{Value, json};

mod common;
use common::MemLog;

/// Adds its argument, after a step; a negative one fails after having written.
const ADD: &str = r#"
def run(call):
    n = call["state"].get("n", 0) + call["args"]
    step("audit", n)
    if call["args"] < 0:
        fail("negative")
    return {"output": n, "writes": {"n": n}}
"#;

fn open(log: MemLog) -> Partition<MemLog> {
    let mut deciders = Deciders::new();
    deciders.insert(
        loop_workflow("counter"),
        Arc::new(ObjectLoop::new("counter")),
    );
    deciders.insert(
        "counter.add",
        Arc::new(Procedure::compile("add.star", ADD).unwrap()),
    );
    Partition::recover(log, deciders).unwrap()
}

/// Flushes, and answers the steps dispatched; returns the runs and inputs of those steps. Fails if a
/// key has two handlers in flight at once.
fn work(partition: &mut Partition<MemLog>) -> Vec<i64> {
    let mut audited = Vec::new();
    loop {
        let mut steps = Vec::new();
        for message in partition.flush().unwrap() {
            if let Outbound::Dispatch {
                key, cmd, input, ..
            } = message
            {
                steps.push((key, cmd, input));
            }
        }
        if steps.is_empty() {
            return audited;
        }
        let keys: BTreeSet<&str> = steps
            .iter()
            .map(|(key, _, _)| key.split('/').nth(2).unwrap())
            .collect();
        assert_eq!(
            keys.len(),
            steps.len(),
            "two handlers of one key at once: {steps:?}"
        );
        for (key, cmd, input) in steps {
            audited.push(serde_json::from_slice(input.as_bytes()).unwrap());
            partition
                .step_result(&key, cmd, StepOutcome::Completed(Payload::new(*b"null")))
                .unwrap();
        }
    }
}

fn state(partition: &mut Partition<MemLog>, key: &str) -> Value {
    let answer = partition
        .query(&object_run_key("counter", key), "state")
        .unwrap()
        .unwrap();
    serde_json::from_slice(answer.as_bytes()).unwrap()
}

fn output(partition: &mut Partition<MemLog>, key: &str, id: &str) -> Value {
    let run = partition
        .lookup_run(&call_key("counter", key, id))
        .unwrap()
        .unwrap();
    assert_eq!(run.status(), Status::Completed, "{id}: {:?}", run.error());
    serde_json::from_slice::<Value>(run.output().unwrap().as_bytes()).unwrap()["output"].clone()
}

#[test]
fn calls_to_a_key_run_one_at_a_time_and_failures_commit_nothing() {
    let mut partition = open(MemLog::default());
    // All in the inbox before any runs.
    for (id, args) in [("a", 1), ("b", 10), ("c", -5), ("d", 100)] {
        partition
            .call_object("counter", "k", "add", id, &json!(args))
            .unwrap();
    }
    partition
        .call_object("counter", "other", "add", "a", &json!(7))
        .unwrap();
    let mut audited = work(&mut partition);
    audited.sort_unstable();
    // Each saw what the one before it committed; the failed call saw 11 and committed nothing.
    assert_eq!(audited, [1, 6, 7, 11, 111]);
    assert_eq!(state(&mut partition, "k"), json!({"n": 111}));
    assert_eq!(state(&mut partition, "other"), json!({"n": 7}));
    assert_eq!(output(&mut partition, "k", "b"), json!(11));
    let failed = partition
        .lookup_run(&call_key("counter", "k", "c"))
        .unwrap()
        .unwrap();
    assert_eq!(
        (failed.status(), failed.error()),
        (Status::Failed, Some("negative"))
    );

    // A repeated id runs nothing, whether its call has ended or not.
    partition
        .call_object("counter", "k", "add", "a", &json!(1))
        .unwrap();
    assert!(work(&mut partition).is_empty());
    assert_eq!(state(&mut partition, "k"), json!({"n": 111}));
    assert!(
        partition
            .call_object("counter", "k", "nope", "z", &json!(1))
            .is_err()
    );
}

/// Calls in batches, leaving the inbox full at the points where the loop could compact (which must
/// then wait, and replay what it did), and one at a time, where it does compact.
#[test]
fn the_journal_is_compacted_without_losing_calls_or_state() {
    for batch in [1, 7, 200] {
        let mut partition = open(MemLog::default());
        let mut sum = 0;
        for i in 0..200_i64 {
            partition
                .call_object("counter", "k", "add", &format!("c{i}"), &json!(1))
                .unwrap();
            sum += 1;
            if (i + 1) % batch == 0 {
                work(&mut partition);
                assert_eq!(
                    state(&mut partition, "k"),
                    json!({"n": sum}),
                    "batch {batch}"
                );
            }
        }
        work(&mut partition);
        assert_eq!(
            state(&mut partition, "k"),
            json!({"n": 200}),
            "batch {batch}"
        );
        let run = object_run_key("counter", "k");
        let id = partition.latest(&run).unwrap().unwrap();
        let loop_run = partition.run(&id).unwrap();
        assert_eq!(loop_run.state().status(), Status::Running, "batch {batch}");
        assert!(loop_run.journal().len() < 1_000, "batch {batch}");
        if batch == 1 {
            assert_ne!(id, run, "one call at a time compacts");
        }
    }
}

/// From every prefix of the log, the same calls sent again under their ids run exactly once each, in
/// order: the outputs and the state are those of the run without a crash.
#[test]
fn calls_survive_a_crash_at_every_point() {
    // Past one compaction.
    let calls = 70;
    let id = |i: i64| format!("c{i}");
    let mut partition = open(MemLog::default());
    let mut expected = Vec::new();
    let mut sum = 0;
    for i in 0..calls {
        partition
            .call_object("counter", "k", "add", &id(i), &json!(i))
            .unwrap();
        work(&mut partition);
        sum += i;
        expected.push(json!(sum));
    }
    let log = partition.into_log();
    // Every prefix near the start and the end, and around the compaction; a stride elsewhere.
    let total = log.records.len();
    for cut in (0..=total).filter(|c| *c < 40 || *c + 40 > total || c % 7 == 0) {
        let mut partition = open(MemLog {
            records: log.records[..cut].to_vec(),
            pending: Vec::new(),
        });
        work(&mut partition);
        for i in 0..calls {
            partition
                .call_object("counter", "k", "add", &id(i), &json!(i))
                .unwrap();
            work(&mut partition);
        }
        assert_eq!(
            state(&mut partition, "k"),
            json!({"n": sum}),
            "cut at {cut}"
        );
        for i in 0..calls {
            assert_eq!(
                output(&mut partition, "k", &id(i)),
                expected[i as usize],
                "cut at {cut}, call {i}"
            );
        }
    }
}
