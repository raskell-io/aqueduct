//! Child runs over a log in memory (ADR 0034): across crashes, and in counterfactual replay.
//!
//! A parent and its children are separate runs whose records share a batch, so a crash can keep some
//! and tear off the rest; a torn tail always leaves a prefix of the log. Recovering from every prefix of
//! a history checks every crash point.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use aqueduct_core::{Payload, Status};
use aqueduct_engine::counterfactual::{CounterfactualOptions, Verdict, replay_counterfactual};
use aqueduct_engine::{Deciders, Partition};
use aqueduct_procedure::Procedure;
use aqueduct_storage::{LogStore, StorageError};

/// A log in memory: synced records survive, appended but unsynced ones are lost.
#[derive(Clone, Default)]
struct MemLog {
    records: Vec<Vec<u8>>,
    pending: Vec<Vec<u8>>,
}

impl LogStore for MemLog {
    fn append(&mut self, records: &[Vec<u8>]) -> Result<(), StorageError> {
        self.pending.extend_from_slice(records);
        Ok(())
    }

    fn sync(&mut self) -> Result<(), StorageError> {
        self.records.append(&mut self.pending);
        Ok(())
    }

    fn recover_from(&mut self, from: u64) -> Result<Vec<Vec<u8>>, StorageError> {
        self.pending.clear();
        Ok(self.records[usize::try_from(from).unwrap()..].to_vec())
    }

    fn next_lsn(&self) -> u64 {
        self.records.len() as u64
    }

    fn trim(&mut self, _before: u64) -> Result<(), StorageError> {
        Ok(())
    }
}

const PARENT: &str = r#"
def run(input):
    first = call("child", input)
    second = call("child", first + 1)
    return first + second
"#;

const CHILD: &str = r#"
def run(n):
    if n > 100:
        wait(signal("never"))
    return n * 10
"#;

fn deciders() -> Deciders {
    let mut deciders = Deciders::new();
    for (name, source) in [("parent", PARENT), ("child", CHILD)] {
        deciders.insert(
            name,
            Arc::new(Procedure::compile(&format!("{name}.star"), source).unwrap()),
        );
    }
    deciders
}

fn status(partition: &mut Partition<MemLog>, key: &str) -> Option<Status> {
    partition.lookup(key).unwrap().map(|state| state.status())
}

#[test]
fn children_start_and_report_back_whatever_prefix_of_the_log_survives() {
    let mut partition = Partition::recover(MemLog::default(), deciders()).unwrap();
    partition.start("p", "parent", Payload::new(*b"1")).unwrap();
    partition.flush().unwrap();
    assert_eq!(status(&mut partition, "p"), Some(Status::Completed));
    let full = partition.into_log().records;

    for cut in 1..=full.len() {
        let log = MemLog {
            records: full[..cut].to_vec(),
            pending: Vec::new(),
        };
        let mut partition = Partition::recover(log, deciders()).unwrap();
        partition.flush().unwrap();
        // However much survived, recovery finishes the work: the parent gets both children's outcomes.
        let parent = partition.lookup("p").unwrap().unwrap();
        assert_eq!(parent.status(), Status::Completed, "cut at {cut}");
        assert_eq!(parent.output().unwrap().as_bytes(), b"120", "cut at {cut}");
        assert_eq!(status(&mut partition, "p#0"), Some(Status::Completed));
        assert_eq!(status(&mut partition, "p#1"), Some(Status::Completed));
    }
}

#[test]
fn a_cancelled_parent_never_leaves_its_child_running() {
    let mut partition = Partition::recover(MemLog::default(), deciders()).unwrap();
    partition
        .start("p", "parent", Payload::new(*b"200"))
        .unwrap();
    partition.flush().unwrap();
    assert_eq!(status(&mut partition, "p#0"), Some(Status::Running));
    let before = partition.log().records.len();
    partition.cancel("p", "stop").unwrap();
    partition.flush().unwrap();
    assert_eq!(status(&mut partition, "p"), Some(Status::Failed));
    assert_eq!(status(&mut partition, "p#0"), Some(Status::Failed));
    let full = partition.into_log().records;

    for cut in before..=full.len() {
        let log = MemLog {
            records: full[..cut].to_vec(),
            pending: Vec::new(),
        };
        let mut partition = Partition::recover(log, deciders()).unwrap();
        partition.flush().unwrap();
        let parent = partition.lookup("p").unwrap().unwrap();
        let child = partition.lookup("p#0").unwrap().unwrap();
        if parent.cancel_reason().is_some() {
            // The cancellation survived: it reaches the child, and the parent ends.
            assert_eq!(parent.status(), Status::Failed, "cut at {cut}");
            assert_eq!(child.status(), Status::Failed, "cut at {cut}");
        } else {
            // It was lost before it was acknowledged: both still run.
            assert_eq!(
                (parent.status(), child.status()),
                (Status::Running, Status::Running),
                "cut at {cut}"
            );
        }
    }
}

#[test]
fn counterfactual_replay_answers_children_from_the_record() {
    let mut partition = Partition::recover(MemLog::default(), deciders()).unwrap();
    partition.start("p", "parent", Payload::new(*b"1")).unwrap();
    partition.flush().unwrap();
    let journal = partition.run("p").unwrap().journal().to_vec();
    let replay = |source: &str| {
        let procedure = Procedure::compile("parent.star", source).unwrap();
        replay_counterfactual("p", &journal, &procedure, &CounterfactualOptions::default()).unwrap()
    };
    // The same calls, combined differently: answered from the record, with a different outcome.
    let product = replay(&PARENT.replace("first + second", "first * second"));
    assert_eq!(product.verdict(), Verdict::Different);
    assert!(product.unresolved.is_empty());
    // A call the old run never made cannot be answered.
    let other = replay(&PARENT.replace("first + 1", "first + 2"));
    assert_eq!(other.verdict(), Verdict::Unresolved);
    assert!(
        other.unresolved[0]
            .what
            .contains("child run of `child` with input 12"),
        "{:?}",
        other.unresolved
    );
}
