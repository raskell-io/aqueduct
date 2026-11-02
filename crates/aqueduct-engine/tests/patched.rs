//! `patched` (ADR 0035): new code for a long-running workflow keeps old runs on their path, and the
//! determinism check over their journals shows it.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use aqueduct_core::{Entry, Payload, Status, StepOutcome};
use aqueduct_engine::replay::{ReplayError, check};
use aqueduct_engine::{Deciders, Outbound, Partition};
use aqueduct_procedure::Procedure;

mod common;
use common::MemLog;

const V1: &str = r#"
def run(order):
    step("charge", order)
    step("ship", order)
    wait(signal("delivered"))
    return "done"
"#;

/// Adds a review before shipping, behind a patch.
const V2: &str = r#"
def run(order):
    step("charge", order)
    if patched("review"):
        step("review", order)
    step("ship", order)
    wait(signal("delivered"))
    return "done"
"#;

/// The same change without a patch.
const V2_UNPATCHED: &str = r#"
def run(order):
    step("charge", order)
    step("review", order)
    step("ship", order)
    wait(signal("delivered"))
    return "done"
"#;

fn deciders(source: &str) -> Deciders {
    let mut deciders = Deciders::new();
    deciders.insert(
        "order",
        Arc::new(Procedure::compile("order.star", source).unwrap()),
    );
    deciders
}

/// Runs every dispatched step to completion.
fn work(partition: &mut Partition<MemLog>) {
    loop {
        let outbound = partition.flush().unwrap();
        let mut any = false;
        for message in outbound {
            if let Outbound::Dispatch { key, cmd, .. } = message {
                any = true;
                partition
                    .step_result(&key, cmd, StepOutcome::Completed(Payload::new(*b"null")))
                    .unwrap();
            }
        }
        if !any {
            return;
        }
    }
}

fn steps(journal: &[Entry]) -> Vec<String> {
    journal
        .iter()
        .filter_map(|entry| match entry {
            Entry::StepScheduled { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn old_runs_keep_the_old_code_and_new_runs_take_the_new() {
    // Two runs on the old code: one has shipped already, one is still charging.
    let mut partition = Partition::recover(MemLog::default(), deciders(V1)).unwrap();
    partition
        .start("shipped", "order", Payload::new(*b"1"))
        .unwrap();
    work(&mut partition);
    partition
        .start("charging", "order", Payload::new(*b"2"))
        .unwrap();
    partition.flush().unwrap();
    let log = partition.into_log();

    // The new code is deployed: the run that passed the review's place keeps to the old path; the one
    // that has not reached it yet takes the new one, as does a new run.
    let mut partition = Partition::recover(log.clone(), deciders(V2)).unwrap();
    partition
        .start("new", "order", Payload::new(*b"3"))
        .unwrap();
    work(&mut partition);
    for key in ["shipped", "charging", "new"] {
        partition
            .signal(key, "delivered", "", Payload::new(*b"true"))
            .unwrap();
    }
    partition.flush().unwrap();
    let journal = |partition: &Partition<MemLog>, key: &str| {
        let run = partition.run(key).unwrap();
        assert_eq!(run.state().status(), Status::Completed, "{key}");
        run.journal().to_vec()
    };
    assert_eq!(steps(&journal(&partition, "shipped")), ["charge", "ship"]);
    assert_eq!(
        steps(&journal(&partition, "charging")),
        ["charge", "review", "ship"]
    );
    assert_eq!(
        steps(&journal(&partition, "new")),
        ["charge", "review", "ship"]
    );

    // The determinism check over every journal: the patched code replays them all.
    let v2 = Procedure::compile("order.star", V2).unwrap();
    for key in ["shipped", "charging", "new"] {
        check(key, &journal(&partition, key), &v2).unwrap();
    }
    // The old code cannot replay the new runs, and the unpatched change not the old one.
    let v1 = Procedure::compile("order.star", V1).unwrap();
    assert!(matches!(
        check("new", &journal(&partition, "new"), &v1),
        Err(ReplayError::Diverged { .. })
    ));
    let unpatched = Procedure::compile("order.star", V2_UNPATCHED).unwrap();
    let error = check("shipped", &journal(&partition, "shipped"), &unpatched).unwrap_err();
    assert!(error.to_string().contains("command 1 diverged"), "{error}");

    // Deployed without the patch, the old run parks instead.
    let mut partition = Partition::recover(log, deciders(V2_UNPATCHED)).unwrap();
    partition
        .signal("shipped", "delivered", "", Payload::new(*b"true"))
        .unwrap();
    partition.flush().unwrap();
    assert_eq!(
        partition.run("shipped").unwrap().state().status(),
        Status::Parked
    );
}
