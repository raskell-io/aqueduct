//! A replicated partition (ADR 0051, step 4): a [`Partition`] whose log is a Raft group.
//!
//! Each batch the partition writes becomes one Raft entry. The leader runs the partition; everything a
//! batch releases (acknowledgements, dispatches) is held until the entry is committed, so nothing is
//! acknowledged that a majority does not have. A follower's state machine is the committed records
//! themselves. A new leader rebuilds its partition from them, as crash recovery does, once it has
//! committed its own no-op: then every entry in its log is committed, and it never acts on one a majority
//! might not have. A leader that loses its leadership drops its partition and what it holds: none of it
//! was acknowledged.
//!
//! Like the partition and [`RaftNode`], a replica does no I/O: its driver persists and sends what
//! [`Replica::ready`] asks, as for a [`RaftNode`], and delivers the [`ReplicaReady::released`] messages.

use std::collections::VecDeque;
use std::sync::Arc;

use aqueduct_raft::{Config, Entry, HardState, Message, NodeId, RaftNode, Ready, Role, Snapshot};
use aqueduct_storage::{LogStore, NoStateStore, StorageError};

use crate::partition::{Deciders, Outbound, Partition, PartitionError};

/// The partition's log on a replica: the committed records it was rebuilt from, and the records of the
/// batch it is writing, which the replica takes and proposes.
#[derive(Debug, Default)]
pub struct BatchLog {
    committed: Vec<Vec<u8>>,
    staged: Vec<Vec<u8>>,
}

impl BatchLog {
    fn take_staged(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.staged)
    }
}

impl LogStore for BatchLog {
    fn append(&mut self, records: &[Vec<u8>]) -> Result<(), StorageError> {
        self.staged.extend_from_slice(records);
        Ok(())
    }

    /// Durability is the group's: the batch is proposed, and what it releases waits for the commit.
    fn sync(&mut self) -> Result<(), StorageError> {
        Ok(())
    }

    fn recover_from(&mut self, from: u64) -> Result<Vec<Vec<u8>>, StorageError> {
        let from = usize::try_from(from).unwrap_or(usize::MAX);
        Ok(self
            .committed
            .get(from..)
            .map(<[_]>::to_vec)
            .unwrap_or_default())
    }

    fn next_lsn(&self) -> u64 {
        (self.committed.len() + self.staged.len()) as u64
    }

    fn trim(&mut self, _before: u64) -> Result<(), StorageError> {
        Ok(())
    }
}

/// A partition on a replica.
pub type ReplicaPartition = Partition<BatchLog, NoStateStore>;

/// Sets up a partition each time a replica builds one: retry policies, the seed for recorded values.
pub type Setup = Arc<dyn Fn(&mut ReplicaPartition, u64) + Send + Sync>;

/// Why a replica cannot take a request now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unavailable {
    /// It does not lead: try this node, if it knows the leader.
    NotLeader(Option<NodeId>),
    /// It leads, but has not committed its own no-op yet, so its partition is not built.
    NotReady,
}

/// What a replica's driver must do: what its Raft node asks (persist, then send), then deliver the
/// released messages.
#[derive(Clone, Debug, Default)]
pub struct ReplicaReady {
    /// The Raft node's ready: see [`aqueduct_raft`] for the order.
    pub raft: Ready,
    /// Messages the partition released whose batches are now committed.
    pub released: Vec<Outbound>,
}

/// One replica of a partition.
pub struct Replica {
    raft: RaftNode,
    deciders: Deciders,
    setup: Setup,
    /// The records of every committed entry, in order: the state machine. With a snapshot, those it
    /// holds come first.
    committed: Vec<Vec<u8>>,
    /// The Raft index the committed records reach.
    applied: u64,
    /// The partition, on a leader whose own no-op is committed.
    partition: Option<ReplicaPartition>,
    /// On a leader: the index of its no-op, after which its partition can be built.
    ready_at: Option<u64>,
    /// Messages the partition released, each with the Raft index it waits for.
    held: VecDeque<(u64, Vec<Outbound>)>,
    released: Vec<Outbound>,
    term: u64,
}

impl std::fmt::Debug for Replica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replica")
            .field("id", &self.raft.id())
            .field("role", &self.raft.role())
            .field("term", &self.raft.term())
            .field("applied", &self.applied)
            .field("built", &self.partition.is_some())
            .finish()
    }
}

/// Whether a message may only be released once this replica is known to lead: everything but a
/// dispatch, which is safe to repeat from a stale state.
fn needs_leadership(message: &Outbound) -> bool {
    !matches!(
        message,
        Outbound::Dispatch { .. }
            | Outbound::DispatchAgent { .. }
            | Outbound::Activation { .. }
            | Outbound::Finished { .. }
    )
}

/// Encodes a batch of records as one entry's data.
fn encode(records: &[Vec<u8>]) -> Vec<u8> {
    let mut data = Vec::new();
    for record in records {
        let length = u32::try_from(record.len()).unwrap_or(u32::MAX);
        data.extend_from_slice(&length.to_le_bytes());
        data.extend_from_slice(record);
    }
    data
}

/// Decodes an entry's data into its records.
fn decode(data: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut records = Vec::new();
    let mut rest = data;
    while !rest.is_empty() {
        let (length, tail) = rest
            .split_first_chunk::<4>()
            .ok_or("a batch ends inside a length")?;
        let length = u32::from_le_bytes(*length) as usize;
        let record = tail.get(..length).ok_or("a batch ends inside a record")?;
        records.push(record.to_vec());
        rest = &tail[length..];
    }
    Ok(records)
}

impl Replica {
    /// A replica with nothing persisted.
    pub fn new(
        id: NodeId,
        members: &[NodeId],
        config: Config,
        seed: u64,
        deciders: Deciders,
        setup: Setup,
    ) -> Replica {
        Replica::wrap(
            RaftNode::new(id, members, config, seed),
            deciders,
            setup,
            Vec::new(),
        )
    }

    /// A replica restarted from what its driver persisted: its Raft hard state, snapshot and log.
    #[allow(clippy::too_many_arguments)]
    pub fn restart(
        id: NodeId,
        members: &[NodeId],
        config: Config,
        seed: u64,
        hard_state: HardState,
        snapshot: Option<Snapshot>,
        log: Vec<Entry>,
        deciders: Deciders,
        setup: Setup,
    ) -> Result<Replica, String> {
        // The snapshot holds the committed records it replaced; the log after it is applied again.
        let committed = match &snapshot {
            Some(snapshot) => decode(&snapshot.data)?,
            None => Vec::new(),
        };
        let applied = snapshot.as_ref().map_or(0, |s| s.index);
        let raft = RaftNode::restart(
            id, members, config, seed, hard_state, snapshot, log, applied,
        );
        let mut replica = Replica::wrap(raft, deciders, setup, committed);
        replica.applied = applied;
        Ok(replica)
    }

    fn wrap(raft: RaftNode, deciders: Deciders, setup: Setup, committed: Vec<Vec<u8>>) -> Replica {
        Replica {
            term: raft.term(),
            raft,
            deciders,
            setup,
            committed,
            applied: 0,
            partition: None,
            ready_at: None,
            held: VecDeque::new(),
            released: Vec::new(),
        }
    }

    /// The Raft node.
    pub fn raft(&self) -> &RaftNode {
        &self.raft
    }

    /// The committed records: what any replica's partition is rebuilt from.
    pub fn committed(&self) -> &[Vec<u8>] {
        &self.committed
    }

    /// The partition, if this replica leads and has built it.
    pub fn partition(&self) -> Option<&ReplicaPartition> {
        self.partition.as_ref()
    }

    /// The partition, to take a request: only on a leader whose own no-op is committed.
    pub fn partition_mut(&mut self) -> Result<&mut ReplicaPartition, Unavailable> {
        if self.raft.role() != Role::Leader {
            return Err(Unavailable::NotLeader(self.raft.leader()));
        }
        self.partition.as_mut().ok_or(Unavailable::NotReady)
    }

    /// One tick of the driver's clock.
    pub fn tick(&mut self) {
        self.raft.tick();
        self.follow_role();
    }

    /// A message from another replica.
    pub fn step(&mut self, from: NodeId, message: Message) {
        self.raft.step(from, message);
        self.follow_role();
    }

    /// A leader can step down without anything to persist or send (check quorum, on a tick): its
    /// partition goes at once, not at the next ready.
    fn follow_role(&mut self) {
        if self.raft.role() != Role::Leader || self.raft.term() != self.term {
            self.lose_leadership();
            self.term = self.raft.term();
        }
    }

    /// Writes the partition's batch, if it has one: proposes it, and holds what it released until it is
    /// committed. What a flush releases without new records waits for everything proposed before it.
    pub fn flush(&mut self) -> Result<(), String> {
        let Some(partition) = self.partition.as_mut() else {
            return Ok(());
        };
        let mut released = partition.take_unsynced();
        released.extend(partition.flush().map_err(|e| e.to_string())?);
        let records = partition.log_mut().take_staged();
        // What a flush releases without records rests on what the partition already holds: a result
        // no longer needed, a start or a message already there. A deposed leader that does not know it
        // yet holds an old state, so these wait for an entry of their own to commit, which proves this
        // replica still leads. Dispatches need no proof: repeating one is harmless.
        let index = if records.is_empty() && !released.iter().any(needs_leadership) {
            self.raft
                .log()
                .last()
                .map_or(self.raft.snapshot_index(), |e| e.index)
        } else {
            match self.raft.propose(encode(&records)) {
                Ok(index) => index,
                Err(_) => {
                    self.lose_leadership();
                    return Ok(());
                }
            }
        };
        if !released.is_empty() {
            self.held.push_back((index, released));
        }
        self.release_committed();
        Ok(())
    }

    /// Moves what is held for committed entries to the released messages.
    fn release_committed(&mut self) {
        let commit = self.raft.commit();
        while self.held.front().is_some_and(|(index, _)| *index <= commit) {
            if let Some((_, messages)) = self.held.pop_front() {
                self.released.extend(messages);
            }
        }
    }

    fn lose_leadership(&mut self) {
        self.partition = None;
        self.ready_at = None;
        self.held.clear();
    }

    /// Whether [`Replica::ready`] has anything to do.
    pub fn has_ready(&self) -> bool {
        self.raft.has_ready() || !self.released.is_empty()
    }

    /// What the driver must do now.
    pub fn ready(&mut self) -> Result<ReplicaReady, String> {
        let raft = self.raft.ready();
        if let Some(snapshot) = &raft.snapshot {
            self.committed = decode(&snapshot.data)?;
            self.applied = snapshot.index;
        }
        for entry in &raft.committed {
            if entry.index <= self.applied {
                continue;
            }
            self.committed.extend(decode(&entry.data)?);
            self.applied = entry.index;
        }
        self.after_change()?;
        Ok(ReplicaReady {
            raft,
            released: std::mem::take(&mut self.released),
        })
    }

    /// The driver has done what the last ready asked.
    pub fn advance(&mut self) -> Result<(), String> {
        self.raft.advance();
        self.after_change()
    }

    /// Follows the Raft node: builds the partition once a new leader's no-op is committed, releases what
    /// is committed, and drops everything on losing leadership.
    fn after_change(&mut self) -> Result<(), String> {
        self.follow_role();
        if self.raft.role() != Role::Leader {
            return Ok(());
        }
        if self.partition.is_none() {
            let ready_at = *self.ready_at.get_or_insert_with(|| {
                self.raft
                    .log()
                    .last()
                    .map_or(self.raft.snapshot_index(), |e| e.index)
            });
            if self.applied < ready_at {
                return Ok(());
            }
            self.build()?;
        }
        self.release_committed();
        Ok(())
    }

    /// A partition loaded from the committed records alone, as a new leader's starts: to check a
    /// leader's partition against.
    pub fn rebuild(&self) -> Result<ReplicaPartition, String> {
        let log = BatchLog {
            committed: self.committed.clone(),
            staged: Vec::new(),
        };
        Partition::load_with_state(log, NoStateStore, self.deciders.clone())
            .map_err(|e: PartitionError| e.to_string())
    }

    /// A leader's partition, rebuilt from the committed records as crash recovery rebuilds one.
    fn build(&mut self) -> Result<(), String> {
        let mut partition = self.rebuild()?;
        (self.setup)(&mut partition, self.raft.term());
        partition.resume().map_err(|e| e.to_string())?;
        self.partition = Some(partition);
        // Recovery may have work to write: dispatches again, successors repaired.
        self.flush()
    }

    /// Whether this replica leads and has built its partition: it takes requests.
    pub fn leads(&self) -> bool {
        self.partition.is_some() && self.raft.role() == Role::Leader
    }

    /// The Raft index the committed records reach.
    pub fn applied(&self) -> u64 {
        self.applied
    }

    /// The workflows a partition built here decides with: changing them changes the next one built,
    /// and the current one's are the driver's to change too.
    pub fn deciders_mut(&mut self) -> &mut Deciders {
        &mut self.deciders
    }

    /// On the leader: proposes an empty entry and returns its index. Once it is committed, everything
    /// this leader wrote before is, and it still led when it was: answers read from its memory can go.
    pub fn barrier(&mut self) -> Option<u64> {
        if !self.leads() {
            return None;
        }
        match self.raft.propose(Vec::new()) {
            Ok(index) => Some(index),
            Err(_) => {
                self.lose_leadership();
                None
            }
        }
    }

    /// On the leader, moves the group one membership change towards `desired` (the metadata store's
    /// replicas for this partition, ADR 0053): one change at a time, adding before removing.
    pub fn reconcile(&mut self, desired: &[NodeId]) {
        if self.raft.role() != Role::Leader || desired.is_empty() {
            return;
        }
        if let Some(change) = crate::metadata::next_change(self.raft.members(), desired) {
            // Refused while another change is pending or the leader is new: tried again next time.
            let _ = self.raft.propose_change(change);
        }
    }

    /// Replaces the log up to what is applied with a snapshot of the committed records.
    pub fn compact(&mut self) {
        let data = encode(&self.committed);
        self.raft.compact(self.applied, data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_survives_encoding() {
        let records = vec![b"a".to_vec(), Vec::new(), b"three".to_vec()];
        assert_eq!(decode(&encode(&records)), Ok(records));
        assert!(decode(&[1, 0, 0]).is_err());
        assert!(decode(&[5, 0, 0, 0, 1]).is_err());
    }
}
