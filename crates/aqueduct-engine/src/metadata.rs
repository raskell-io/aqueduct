//! The metadata store (ADR 0053): a small Raft group of its own that holds where each partition's
//! replicas should be, and the slot table. Hosts learn it by watching; each partition's leader moves its
//! group towards it, one membership change at a time.
//!
//! Like [`crate::replica::Replica`], a [`MetadataNode`] does no I/O: its driver persists and sends what
//! [`MetadataNode::ready`] asks, as for a [`RaftNode`].

use std::collections::BTreeMap;

use aqueduct_raft::{Change, Config, Entry, HardState, Message, NodeId, RaftNode, Ready, Snapshot};
use serde_json::{Value, json};

use crate::placement::SlotTable;

/// What the metadata store holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Metadata {
    /// How many entries it has applied: newer metadata has a higher epoch.
    epoch: u64,
    partitions: u32,
    /// Where each partition's replicas should be.
    replicas: BTreeMap<u32, Vec<NodeId>>,
}

/// A change to the metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// Move a partition's replica from one machine to another.
    Move {
        /// The partition.
        partition: u32,
        /// The machine it leaves.
        from: NodeId,
        /// The machine it goes to.
        to: NodeId,
    },
}

impl Command {
    /// The command as entry data.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Command::Move {
                partition,
                from,
                to,
            } => json!({"move": {"partition": partition, "from": from, "to": to}}),
        }
        .to_string()
        .into_bytes()
    }

    /// A command from entry data.
    pub fn decode(data: &[u8]) -> Result<Command, String> {
        let value: Value = serde_json::from_slice(data).map_err(|e| e.to_string())?;
        let moved = &value["move"];
        let number = |name: &str| {
            moved[name]
                .as_u64()
                .ok_or_else(|| format!("a metadata move without `{name}`"))
        };
        Ok(Command::Move {
            partition: u32::try_from(number("partition")?).map_err(|e| e.to_string())?,
            from: number("from")?,
            to: number("to")?,
        })
    }
}

impl Metadata {
    /// The metadata a cluster is created with: `partitions` partitions, each with a replica on every
    /// machine of `replicas`. Every node starts from it; commands change it.
    pub fn new(partitions: u32, replicas: &[NodeId]) -> Metadata {
        Metadata {
            epoch: 0,
            partitions: partitions.max(1),
            replicas: (0..partitions.max(1))
                .map(|p| (p, replicas.to_vec()))
                .collect(),
        }
    }

    /// How many entries it has applied.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The machines a partition's replicas should be on.
    pub fn replicas(&self, partition: u32) -> &[NodeId] {
        self.replicas.get(&partition).map_or(&[], Vec::as_slice)
    }

    /// The slot table.
    pub fn table(&self) -> SlotTable {
        SlotTable::even(self.partitions)
    }

    /// Applies a committed entry: a command, or nothing (a leader's no-op). A move that does not fit
    /// (the replica is not there, or already is where it goes) changes nothing but the epoch.
    pub fn apply(&mut self, data: &[u8]) -> Result<(), String> {
        self.epoch += 1;
        if data.is_empty() {
            return Ok(());
        }
        match Command::decode(data)? {
            Command::Move {
                partition,
                from,
                to,
            } => {
                if let Some(replicas) = self.replicas.get_mut(&partition)
                    && replicas.contains(&from)
                    && !replicas.contains(&to)
                {
                    for replica in replicas.iter_mut() {
                        if *replica == from {
                            *replica = to;
                        }
                    }
                    replicas.sort_unstable();
                }
            }
        }
        Ok(())
    }

    /// The whole metadata, for a snapshot or for hosts that watch it.
    pub fn encode(&self) -> Vec<u8> {
        let replicas: BTreeMap<String, &Vec<NodeId>> = self
            .replicas
            .iter()
            .map(|(p, r)| (p.to_string(), r))
            .collect();
        json!({"epoch": self.epoch, "partitions": self.partitions, "replicas": replicas})
            .to_string()
            .into_bytes()
    }

    /// Metadata from [`Metadata::encode`].
    pub fn decode(data: &[u8]) -> Result<Metadata, String> {
        let value: Value = serde_json::from_slice(data).map_err(|e| e.to_string())?;
        let bad = || "metadata that does not decode".to_owned();
        let mut replicas = BTreeMap::new();
        for (partition, machines) in value["replicas"].as_object().ok_or_else(bad)? {
            let machines = machines
                .as_array()
                .ok_or_else(bad)?
                .iter()
                .map(|m| m.as_u64().ok_or_else(bad))
                .collect::<Result<Vec<_>, _>>()?;
            replicas.insert(partition.parse().map_err(|_| bad())?, machines);
        }
        Ok(Metadata {
            epoch: value["epoch"].as_u64().ok_or_else(bad)?,
            partitions: u32::try_from(value["partitions"].as_u64().ok_or_else(bad)?)
                .map_err(|_| bad())?,
            replicas,
        })
    }
}

/// One replica of the metadata store.
#[derive(Debug)]
pub struct MetadataNode {
    raft: RaftNode,
    state: Metadata,
    applied: u64,
}

impl MetadataNode {
    /// A node with nothing persisted.
    pub fn new(
        id: NodeId,
        members: &[NodeId],
        config: Config,
        seed: u64,
        initial: Metadata,
    ) -> MetadataNode {
        MetadataNode {
            raft: RaftNode::new(id, members, config, seed),
            state: initial,
            applied: 0,
        }
    }

    /// A node restarted from what its driver persisted. Its state is the snapshot's, or `initial`.
    #[allow(clippy::too_many_arguments)]
    pub fn restart(
        id: NodeId,
        members: &[NodeId],
        config: Config,
        seed: u64,
        hard_state: HardState,
        snapshot: Option<Snapshot>,
        log: Vec<Entry>,
        initial: Metadata,
    ) -> Result<MetadataNode, String> {
        let state = match &snapshot {
            Some(snapshot) => Metadata::decode(&snapshot.data)?,
            None => initial,
        };
        let applied = snapshot.as_ref().map_or(0, |s| s.index);
        let raft = RaftNode::restart(
            id, members, config, seed, hard_state, snapshot, log, applied,
        );
        Ok(MetadataNode {
            raft,
            state,
            applied,
        })
    }

    /// The Raft node.
    pub fn raft(&self) -> &RaftNode {
        &self.raft
    }

    /// The metadata as far as this node has applied it.
    pub fn state(&self) -> &Metadata {
        &self.state
    }

    /// Proposes a command; only the leader can.
    pub fn propose(&mut self, command: &Command) -> bool {
        self.raft.propose(command.encode()).is_ok()
    }

    /// One tick of the driver's clock.
    pub fn tick(&mut self) {
        self.raft.tick();
    }

    /// A message from another node.
    pub fn step(&mut self, from: NodeId, message: Message) {
        self.raft.step(from, message);
    }

    /// Whether [`MetadataNode::ready`] has anything to do.
    pub fn has_ready(&self) -> bool {
        self.raft.has_ready()
    }

    /// What the driver must do now; applies what is committed.
    pub fn ready(&mut self) -> Result<Ready, String> {
        let ready = self.raft.ready();
        if let Some(snapshot) = &ready.snapshot {
            self.state = Metadata::decode(&snapshot.data)?;
            self.applied = snapshot.index;
        }
        for entry in &ready.committed {
            if entry.index <= self.applied {
                continue;
            }
            // A membership entry of the metadata group itself carries no command.
            if entry.members.is_none() {
                self.state.apply(&entry.data)?;
            } else {
                self.state.epoch += 1;
            }
            self.applied = entry.index;
        }
        Ok(ready)
    }

    /// The driver has done what the last ready asked.
    pub fn advance(&mut self) {
        self.raft.advance();
    }

    /// Replaces the log up to what is applied with a snapshot of the metadata.
    pub fn compact(&mut self) {
        self.raft.compact(self.applied, self.state.encode());
    }
}

/// The membership change that moves a group one step towards `desired`, if any: add first, so the group
/// never shrinks below what it should be.
pub fn next_change(current: &[NodeId], desired: &[NodeId]) -> Option<Change> {
    if let Some(add) = desired.iter().find(|m| !current.contains(m)) {
        return Some(Change::Add(*add));
    }
    current
        .iter()
        .find(|m| !desired.contains(m))
        .map(|remove| Change::Remove(*remove))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_survives_encoding_and_moves_apply_once() {
        let mut metadata = Metadata::new(4, &[1, 2, 3]);
        let command = Command::Move {
            partition: 2,
            from: 1,
            to: 5,
        };
        assert_eq!(Command::decode(&command.encode()), Ok(command.clone()));
        metadata.apply(&command.encode()).unwrap();
        metadata.apply(&command.encode()).unwrap();
        metadata.apply(&[]).unwrap();
        assert_eq!(metadata.replicas(2), &[2, 3, 5]);
        assert_eq!(metadata.replicas(1), &[1, 2, 3]);
        assert_eq!(metadata.epoch(), 3);
        assert_eq!(Metadata::decode(&metadata.encode()), Ok(metadata));
    }

    #[test]
    fn a_group_grows_before_it_shrinks() {
        assert_eq!(next_change(&[1, 2, 3], &[2, 3, 5]), Some(Change::Add(5)));
        assert_eq!(
            next_change(&[1, 2, 3, 5], &[2, 3, 5]),
            Some(Change::Remove(1))
        );
        assert_eq!(next_change(&[2, 3, 5], &[2, 3, 5]), None);
    }
}
