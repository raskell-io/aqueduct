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

// Outboxes (ADR 0044).

/// Moves its argument to the key `dest`: its writes and its message commit together.
const TRANSFER: &str = r#"
def run(call):
    n = call["state"].get("n", 0) - call["args"]
    if n < 0:
        fail("insufficient")
    return {
        "output": n,
        "writes": {"n": n},
        "sends": [{"object": "counter", "key": "dest", "handler": "add", "args": call["args"]}],
    }
"#;

/// A workflow that sends and ends in one decision. The recorded value before its message asks for an
/// activation: delivery must still come first, or the next decision logs the run's end before it.
const NOTIFY: &str = r#"
def run(amount):
    patched("first")
    send("counter", "notified", "add", amount)
    return "sent"
"#;

fn open_with_sends(log: MemLog) -> Partition<MemLog> {
    let mut deciders = Deciders::new();
    deciders.insert(
        loop_workflow("counter"),
        Arc::new(ObjectLoop::new("counter")),
    );
    for (name, source) in [
        ("counter.add", ADD),
        ("counter.transfer", TRANSFER),
        ("notify", NOTIFY),
    ] {
        deciders.insert(
            name,
            Arc::new(Procedure::compile("p.star", source).unwrap()),
        );
    }
    Partition::recover(log, deciders).unwrap()
}

#[test]
fn a_handlers_messages_commit_with_it() {
    let mut partition = open_with_sends(MemLog::default());
    partition
        .call_object("counter", "a", "add", "fund", &json!(10))
        .unwrap();
    work(&mut partition);
    partition
        .call_object("counter", "a", "transfer", "t1", &json!(4))
        .unwrap();
    work(&mut partition);
    // More than it has: fails, writes nothing and sends nothing.
    partition
        .call_object("counter", "a", "transfer", "t2", &json!(100))
        .unwrap();
    work(&mut partition);
    assert_eq!(state(&mut partition, "a"), json!({"n": 6}));
    assert_eq!(state(&mut partition, "dest"), json!({"n": 4}));
}

/// Without holding a run's end back until its messages are delivered, some prefixes keep the sender's
/// end and lose its message, with nothing left to deliver it again.
#[test]
fn a_message_survives_a_crash_at_every_point_exactly_once() {
    let mut partition = open_with_sends(MemLog::default());
    partition
        .start("n-1", "notify", Payload::new(*b"3"))
        .unwrap();
    partition
        .call_object("counter", "a", "add", "fund", &json!(10))
        .unwrap();
    partition
        .call_object("counter", "a", "transfer", "t1", &json!(4))
        .unwrap();
    work(&mut partition);
    assert_eq!(state(&mut partition, "notified"), json!({"n": 3}));
    assert_eq!(state(&mut partition, "dest"), json!({"n": 4}));
    let log = partition.into_log();

    for cut in 0..=log.records.len() {
        let mut partition = open_with_sends(MemLog {
            records: log.records[..cut].to_vec(),
            pending: Vec::new(),
        });
        let sender_ended = partition
            .run("n-1")
            .is_some_and(|run| run.state().status().is_terminal());
        work(&mut partition);
        if sender_ended {
            // An ended run is not recovered: its message must already be in what survived.
            assert_eq!(
                state(&mut partition, "notified"),
                json!({"n": 3}),
                "cut at {cut}"
            );
        }
        // The same requests again: everything happens exactly once.
        partition
            .start("n-1", "notify", Payload::new(*b"3"))
            .unwrap();
        partition
            .call_object("counter", "a", "add", "fund", &json!(10))
            .unwrap();
        partition
            .call_object("counter", "a", "transfer", "t1", &json!(4))
            .unwrap();
        work(&mut partition);
        assert_eq!(
            state(&mut partition, "notified"),
            json!({"n": 3}),
            "cut at {cut}"
        );
        assert_eq!(
            state(&mut partition, "dest"),
            json!({"n": 4}),
            "cut at {cut}"
        );
        assert_eq!(state(&mut partition, "a"), json!({"n": 6}), "cut at {cut}");
    }
}

/// A message to an object not deployed is delivered once it is: after a restart, while the sender runs.
#[test]
fn a_message_waits_for_its_target_to_be_deployed() {
    const WAITS: &str = r#"
def run(amount):
    send("counter", "late", "add", amount)
    return wait(signal("go"), timeout = 3600)
"#;
    let mut without = Deciders::new();
    without.insert(
        "waits",
        Arc::new(Procedure::compile("w.star", WAITS).unwrap()),
    );
    let mut partition = Partition::recover(MemLog::default(), without).unwrap();
    partition
        .start("w-1", "waits", Payload::new(*b"5"))
        .unwrap();
    partition.flush().unwrap();
    let log = partition.into_log();

    let mut deciders = Deciders::new();
    deciders.insert(
        "waits",
        Arc::new(Procedure::compile("w.star", WAITS).unwrap()),
    );
    deciders.insert(
        loop_workflow("counter"),
        Arc::new(ObjectLoop::new("counter")),
    );
    deciders.insert(
        "counter.add",
        Arc::new(Procedure::compile("a.star", ADD).unwrap()),
    );
    let mut partition = Partition::recover(log, deciders).unwrap();
    work(&mut partition);
    assert_eq!(state(&mut partition, "late"), json!({"n": 5}));
}
