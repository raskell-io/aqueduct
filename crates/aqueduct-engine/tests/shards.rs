//! Many partitions in one process (ADR 0057): runs spread over four partitions, their shared children
//! and messages to objects cross from one to another, and everything is there after reopening.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use aqueduct_core::{Payload, Status};
use aqueduct_engine::embedded::{Engine, EngineError};
use aqueduct_engine::shards::Shards;
use aqueduct_procedure::Procedure;
use serde_json::Value;

const PARENT: &str = r#"
def run(n):
    shared = call("child", n % 2, key = "shared-" + str(n % 2))
    send("counter", "c" + str(n % 3), "add", 1)
    return shared + n
"#;

const CHILD: &str = "def run(n):\n    return n * 10 + 1\n";

const ADD: &str = r#"
def run(call):
    n = call["state"].get("n", 0) + call["args"]
    return {"output": n, "writes": {"n": n}}
"#;

const PARENTS: u32 = 24;

fn open(dir: &Path) -> Shards {
    Shards::open(dir, 4, |path: PathBuf, _| {
        Ok(Engine::builder(path)
            .workflow(
                "parent",
                Arc::new(Procedure::compile("parent.star", PARENT).unwrap()),
            )
            .workflow(
                "child",
                Arc::new(Procedure::compile("child.star", CHILD).unwrap()),
            )
            .object_handler(
                "counter",
                "add",
                Arc::new(Procedure::compile("add.star", ADD).unwrap()),
            ))
    })
    .unwrap()
}

fn check(shards: &Shards) {
    for i in 0..PARENTS {
        let result = shards
            .wait(&format!("p{i}"), Duration::from_secs(20))
            .unwrap();
        assert_eq!(result.status, Status::Completed, "p{i}: {:?}", result.error);
        let expected = (i % 2) * 10 + 1 + i;
        assert_eq!(
            result.output.unwrap().as_bytes(),
            expected.to_string().as_bytes()
        );
    }
    // A shared child lives only at its home.
    for k in 0..2 {
        let child = format!("shared-{k}");
        let home = shards.table().partition_of(&child) as usize;
        for (index, engine) in shards.engines().iter().enumerate() {
            match engine.describe(&child) {
                Ok(_) => assert_eq!(index, home, "{child} on partition {index}"),
                Err(EngineError::UnknownRun(_)) => assert_ne!(index, home),
                Err(other) => panic!("{other}"),
            }
        }
    }
}

#[test]
fn runs_children_and_messages_spread_over_partitions_and_survive_reopening() {
    let dir = std::env::temp_dir().join(format!("aqueduct-shards-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let shards = open(&dir);
    let homes: std::collections::BTreeSet<u32> = (0..PARENTS)
        .map(|i| shards.table().partition_of(&format!("p{i}")))
        .collect();
    assert!(homes.len() > 1, "the parents should be spread");
    for i in 0..PARENTS {
        shards
            .start(
                &format!("p{i}"),
                "parent",
                Payload::new(i.to_string().into_bytes()),
            )
            .unwrap();
    }
    check(&shards);
    // Every message reached its counter exactly once, wherever the counter lives.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    for k in 0..3 {
        let sent = (0..PARENTS).filter(|i| i % 3 == k).count();
        loop {
            let state: Value = serde_json::from_slice(
                shards
                    .object_state("counter", &format!("c{k}"))
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
            if state["n"] == sent {
                break;
            }
            assert!(state["n"].as_u64().unwrap_or(0) < sent as u64, "{state}");
            assert!(
                std::time::Instant::now() < deadline,
                "counter c{k}: {state}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    drop(shards);
    let shards = open(&dir);
    check(&shards);
    drop(shards);
    let _ = std::fs::remove_dir_all(&dir);
}
