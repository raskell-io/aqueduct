//! Priorities (ADR 0049) through crash recovery: a successor torn off by a crash is started again with
//! its predecessor's priority.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use aqueduct_codec::{ChainWriter, encode_log_record};
use aqueduct_core::{Command, Decider, Entry, Payload, RunView};
use aqueduct_engine::embedded::Engine;
use aqueduct_storage::{FileLog, FileLogOptions, LogStore};

/// Waits for a signal that never comes, so the run stays as it is.
struct Waits;

impl Decider for Waits {
    fn decide(&self, _view: &RunView<'_>) -> Vec<Command> {
        vec![Command::AwaitSignal {
            name: "go".into(),
            timeout_ms: 0,
        }]
    }
}

#[test]
fn a_successor_repaired_after_a_crash_keeps_the_priority() {
    let dir = std::env::temp_dir().join(format!("aqueduct-prio-repair-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let open = || {
        Engine::builder(&dir)
            .workflow("w", Arc::new(Waits))
            .open()
            .unwrap()
    };
    drop(open());
    // What a crash can leave: the run continued as new, and its successor's start was torn off.
    {
        let mut log = FileLog::open(dir.join("log"), FileLogOptions::default()).unwrap();
        log.recover_from(0).unwrap();
        let mut chain = ChainWriter::new("k");
        let entries = [
            Entry::Started {
                workflow: "w".into(),
                input: Payload::new(*b"1"),
                budget_tokens: None,
                version: None,
            },
            Entry::PrioritySet { priority: 4 },
            Entry::ContinuedAsNew {
                input: Payload::new(*b"2"),
            },
        ];
        let batch: Vec<Vec<u8>> = entries
            .iter()
            .map(|entry| encode_log_record("k", &chain.append(entry)))
            .collect();
        log.append(&batch).unwrap();
        log.sync().unwrap();
    }
    let engine = open();
    let (run_id, state) = engine.describe("k").unwrap();
    assert_ne!(run_id, "k", "the successor was started again");
    assert_eq!(state.priority(), 4);
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}
