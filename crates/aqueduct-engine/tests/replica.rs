//! A partition replicated by Raft (ADR 0051, step 4): three replicas on a perfect in-memory network.
//!
//! The simulator (`cargo xtask sim --replicas 3`) runs replicas under faults; these tests pin the contract.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use aqueduct_core::{Payload, Status};
use aqueduct_engine::replica::{Replica, Setup, Unavailable};
use aqueduct_engine::{Deciders, Outbound};
use aqueduct_procedure::Procedure;
use aqueduct_raft::{Config, NodeId};

const TIMES_TEN: &str = r#"
def run(n):
    return n * 10
"#;

fn deciders() -> Deciders {
    let mut deciders = Deciders::new();
    deciders.insert(
        "times_ten",
        Arc::new(Procedure::compile("times_ten.star", TIMES_TEN).unwrap()),
    );
    deciders
}

fn group(ids: &[NodeId]) -> BTreeMap<NodeId, Replica> {
    let setup: Setup = Arc::new(|_, _| {});
    ids.iter()
        .map(|id| {
            let replica = Replica::new(*id, ids, Config::default(), *id, deciders(), setup.clone());
            (*id, replica)
        })
        .collect()
}

/// Delivers every message until the group is quiet, collecting what each replica released.
fn pump(replicas: &mut BTreeMap<NodeId, Replica>, released: &mut Vec<(NodeId, Outbound)>) {
    loop {
        let mut messages = Vec::new();
        for (id, replica) in replicas.iter_mut() {
            while replica.has_ready() {
                let ready = replica.ready().unwrap();
                messages.extend(ready.raft.messages);
                released.extend(ready.released.into_iter().map(|m| (*id, m)));
                replica.advance().unwrap();
            }
        }
        if messages.is_empty() {
            return;
        }
        for envelope in messages {
            if let Some(replica) = replicas.get_mut(&envelope.to) {
                replica.step(envelope.from, envelope.message);
            }
        }
    }
}

/// Ticks until a replica has built its partition, and returns it.
fn elect(replicas: &mut BTreeMap<NodeId, Replica>) -> NodeId {
    let mut released = Vec::new();
    for _ in 0..1_000 {
        for replica in replicas.values_mut() {
            replica.tick();
        }
        pump(replicas, &mut released);
        if let Some((id, _)) = replicas.iter().find(|(_, r)| r.partition().is_some()) {
            return *id;
        }
    }
    panic!("no leader built its partition");
}

#[test]
fn a_start_is_acknowledged_only_once_a_majority_holds_it_and_survives_its_leader() {
    let mut replicas = group(&[1, 2, 3]);
    let leader = elect(&mut replicas);
    for (id, replica) in &mut replicas {
        if *id != leader {
            assert!(matches!(
                replica.partition_mut(),
                Err(Unavailable::NotLeader(_))
            ));
        }
    }

    let replica = replicas.get_mut(&leader).unwrap();
    replica
        .partition_mut()
        .unwrap()
        .start("p", "times_ten", Payload::new(*b"4"))
        .unwrap();
    replica.flush().unwrap();
    // The batch is proposed, not committed: nothing is released yet.
    let ready = replica.ready().unwrap();
    assert!(ready.released.is_empty());
    assert!(!ready.raft.messages.is_empty());
    replica.advance().unwrap();
    let mut released = Vec::new();
    for envelope in ready.raft.messages {
        replicas
            .get_mut(&envelope.to)
            .unwrap()
            .step(envelope.from, envelope.message);
    }
    pump(&mut replicas, &mut released);
    assert!(
        released
            .iter()
            .any(|(id, m)| *id == leader && matches!(m, Outbound::Started { key } if key == "p")),
        "{released:?}"
    );

    // The leader goes; whoever leads next rebuilds the run from what was committed.
    replicas.remove(&leader);
    let next = elect(&mut replicas);
    assert_ne!(next, leader);
    let run = replicas[&next]
        .partition()
        .unwrap()
        .runs()
        .get("p")
        .unwrap();
    assert_eq!(run.state().status(), Status::Completed);
    assert_eq!(run.state().output().unwrap().as_bytes(), b"40");
    // Every replica left has applied the same records.
    let records: Vec<_> = replicas.values().map(|r| r.committed().to_vec()).collect();
    assert!(records.windows(2).all(|w| w[0] == w[1]));
}

#[test]
fn a_leader_cut_off_from_its_followers_acknowledges_nothing() {
    let mut replicas = group(&[1, 2, 3]);
    let leader = elect(&mut replicas);
    let replica = replicas.get_mut(&leader).unwrap();
    replica
        .partition_mut()
        .unwrap()
        .start("p", "times_ten", Payload::new(*b"4"))
        .unwrap();
    replica.flush().unwrap();
    // Its messages never arrive; it keeps ticking alone until check quorum makes it step down.
    let mut alone = BTreeMap::from([(leader, replicas.remove(&leader).unwrap())]);
    let mut released = Vec::new();
    for _ in 0..1_000 {
        alone.get_mut(&leader).unwrap().tick();
        let replica = alone.get_mut(&leader).unwrap();
        while replica.has_ready() {
            let ready = replica.ready().unwrap();
            released.extend(ready.released);
            replica.advance().unwrap();
        }
    }
    assert!(released.is_empty(), "{released:?}");
    assert!(alone[&leader].partition().is_none());
    // The others elect a leader of their own that never heard of the run.
    let next = elect(&mut replicas);
    assert!(
        replicas[&next]
            .partition()
            .unwrap()
            .runs()
            .get("p")
            .is_none()
    );
}
