//! Many partitions (ADR 0052): children with chosen keys and messages to objects cross partitions over a
//! network that loses, repeats and reorders messages, while partitions crash and recover.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use aqueduct_core::{Payload, Status};
use aqueduct_engine::objects::{ObjectLoop, loop_workflow, object_run_key};
use aqueduct_engine::placement::SlotTable;
use aqueduct_engine::{Deciders, Outbound, Partition, Remote};
use aqueduct_procedure::Procedure;
use serde_json::Value;

mod common;
use common::MemLog;

/// Waits for a child shared by every parent with the same key, and sends to a counter.
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
const PARTITIONS: u32 = 4;

fn deciders() -> Deciders {
    let mut deciders = Deciders::new();
    deciders.insert(
        loop_workflow("counter"),
        Arc::new(ObjectLoop::new("counter")),
    );
    for (name, source) in [("parent", PARENT), ("child", CHILD), ("counter.add", ADD)] {
        deciders.insert(
            name,
            Arc::new(Procedure::compile(&format!("{name}.star"), source).unwrap()),
        );
    }
    deciders
}

/// A small deterministic generator.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

struct Cluster {
    table: SlotTable,
    partitions: Vec<Option<Partition<MemLog>>>,
    logs: Vec<MemLog>,
    network: Vec<(u32, Remote)>,
    rng: Rng,
}

impl Cluster {
    fn open(&self, index: u32, log: MemLog) -> Partition<MemLog> {
        let mut partition = Partition::load(log, deciders()).unwrap();
        partition.set_placement(self.table.placement(index));
        partition.resume().unwrap();
        partition
    }

    fn partition(&mut self, key: &str) -> &mut Partition<MemLog> {
        let index = self.table.partition_of(key);
        self.partitions[index as usize].as_mut().unwrap()
    }

    /// Flushes every partition and puts what they send on the network.
    fn flush(&mut self) {
        for partition in self.partitions.iter_mut().flatten() {
            for message in partition.flush().unwrap() {
                if let Outbound::Remote { to, message } = message {
                    self.network.push((self.table.partition_of(&to), message));
                }
            }
        }
    }

    /// One round of chaos: messages delivered in a random order, some lost or repeated; partitions
    /// crash and recover from what they synced; senders repeat what is unanswered.
    fn round(&mut self, lossy: bool) {
        let mut messages = std::mem::take(&mut self.network);
        for i in (1..messages.len()).rev() {
            let j = self.rng.below(i as u64 + 1) as usize;
            messages.swap(i, j);
        }
        for (to, message) in messages {
            if lossy && self.rng.below(5) == 0 {
                continue;
            }
            let repeat = lossy && self.rng.below(10) == 0;
            let Some(partition) = self.partitions[to as usize].as_mut() else {
                continue;
            };
            if repeat {
                partition.receive(message.clone()).unwrap();
            }
            partition.receive(message).unwrap();
        }
        if lossy && self.rng.below(4) == 0 {
            let index = self.rng.below(u64::from(PARTITIONS)) as usize;
            // What was not synced is lost.
            let log = self.partitions[index].take().unwrap().into_log();
            self.logs[index] = log;
            let partition = self.open(index as u32, self.logs[index].clone());
            self.partitions[index] = Some(partition);
        }
        for partition in self.partitions.iter_mut().flatten() {
            partition.redispatch();
        }
        self.flush();
    }
}

fn status(cluster: &mut Cluster, key: &str) -> Option<Status> {
    cluster
        .partition(key)
        .lookup(key)
        .unwrap()
        .map(|state| state.status())
}

#[test]
fn children_and_messages_cross_partitions_exactly_once_under_loss_and_crashes() {
    for seed in 1..=40u64 {
        let table = SlotTable::even(PARTITIONS);
        let mut cluster = Cluster {
            partitions: Vec::new(),
            logs: vec![MemLog::default(); PARTITIONS as usize],
            network: Vec::new(),
            rng: Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15)),
            table,
        };
        for index in 0..PARTITIONS {
            let partition = cluster.open(index, MemLog::default());
            cluster.partitions.push(Some(partition));
        }
        let homes: std::collections::BTreeSet<u32> = (0..PARENTS)
            .map(|i| cluster.table.partition_of(&format!("p{i}")))
            .collect();
        assert!(homes.len() > 1, "the parents should be spread");
        for i in 0..PARENTS {
            let key = format!("p{i}");
            cluster
                .partition(&key)
                .start(&key, "parent", Payload::new(i.to_string().into_bytes()))
                .unwrap();
        }
        cluster.flush();

        let mut rounds = 0;
        while (0..PARENTS)
            .any(|i| status(&mut cluster, &format!("p{i}")) != Some(Status::Completed))
        {
            rounds += 1;
            assert!(rounds < 400, "seed {seed}: not done after 400 rounds");
            cluster.round(rounds < 60);
        }

        // Then calm, until the handlers the messages started have run.
        for _ in 0..20 {
            cluster.round(false);
        }

        for i in 0..PARENTS {
            let key = format!("p{i}");
            let run = cluster.partition(&key).lookup(&key).unwrap().unwrap();
            let expected = (i % 2) * 10 + 1 + i;
            assert_eq!(
                run.output().unwrap().as_bytes(),
                expected.to_string().as_bytes(),
                "seed {seed}: {key}"
            );
        }
        // A shared child lives only at its home, where every parent found it.
        for k in 0..2 {
            let child = format!("shared-{k}");
            let home = cluster.table.partition_of(&child);
            for (index, partition) in cluster.partitions.iter_mut().enumerate() {
                let found = partition
                    .as_mut()
                    .unwrap()
                    .lookup(&child)
                    .unwrap()
                    .is_some();
                assert_eq!(
                    found,
                    index as u32 == home,
                    "seed {seed}: {child} on partition {index}"
                );
            }
        }
        // Every message reached its counter exactly once.
        for k in 0..3 {
            let key = format!("c{k}");
            let loop_key = object_run_key("counter", &key);
            let answer = cluster
                .partition(&loop_key)
                .query(&loop_key, "state")
                .unwrap()
                .unwrap();
            let state: Value = serde_json::from_slice(answer.as_bytes()).unwrap();
            let sent = (0..PARENTS).filter(|i| i % 3 == k).count();
            assert_eq!(state["n"], Value::from(sent), "seed {seed}: counter {key}");
        }
    }
}
