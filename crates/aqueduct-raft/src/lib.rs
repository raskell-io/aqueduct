//! Raft as a pure state machine (ADR 0051).
//!
//! A [`RaftNode`] takes messages ([`RaftNode::step`]), clock ticks ([`RaftNode::tick`]) and proposals
//! ([`RaftNode::propose`]), and produces a [`Ready`]: the state to persist, the messages to send, and the
//! committed entries to apply. It never touches a disk, a socket or a clock, so the simulator in [`sim`]
//! drives whole clusters deterministically.
//!
//! The driver's contract, for every `Ready`:
//!
//! 1. persist its hard state, and its entries (after dropping persisted entries from `truncate_from` on);
//! 2. then send its messages: a vote or an acknowledgement in them may rely on what was just persisted;
//! 3. apply its committed entries, in order;
//! 4. call [`RaftNode::advance`].
//!
//! The algorithm follows Ongaro's thesis (2014): leader election with pre-vote (9.6) and check quorum,
//! log replication with conflict hints, commitment of entries from the leader's own term only (3.6.2),
//! helped by a no-op entry at the start of each term.

#![forbid(unsafe_code)]

pub mod sim;
pub mod wire;

use std::collections::{BTreeMap, BTreeSet};

/// A node's id.
pub type NodeId = u64;

/// An entry in the replicated log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The term of the leader that created it.
    pub term: u64,
    /// Its position, from 1.
    pub index: u64,
    /// What it carries; empty for the no-op a leader appends when its term starts.
    pub data: Vec<u8>,
    /// For a membership change: the group from this entry on (thesis 4.1).
    pub members: Option<Vec<NodeId>>,
}

/// A state machine's state as of an index, standing in for the log up to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// The last entry it includes.
    pub index: u64,
    /// That entry's term.
    pub term: u64,
    /// The state machine's state, as the driver encoded it.
    pub data: Vec<u8>,
    /// The group as of its last entry.
    pub members: Vec<NodeId>,
}

/// What a node must persist before it acts on it: its term, its vote in that term, and how far it knows
/// the log is committed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    /// The latest term the node has seen.
    pub term: u64,
    /// The candidate it voted for in that term.
    pub vote: Option<NodeId>,
    /// The highest index it knows to be committed.
    pub commit: u64,
}

/// A message between nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// Asks for a vote, or, with `pre_vote`, whether a vote would be granted (thesis 9.6).
    RequestVote {
        /// The term the candidate stands for.
        term: u64,
        /// The candidate's last log entry, to compare logs.
        last_index: u64,
        /// Its term.
        last_term: u64,
        /// Whether this is a pre-vote: nothing changes on the receiver.
        pre_vote: bool,
    },
    /// The answer to a [`Message::RequestVote`].
    Vote {
        /// The term asked for, or the receiver's own if it is later.
        term: u64,
        /// Whether the vote is granted.
        granted: bool,
        /// Whether it answers a pre-vote.
        pre_vote: bool,
    },
    /// Entries for a follower to append after `prev_index`, or none: a heartbeat.
    Append {
        /// The leader's term.
        term: u64,
        /// The entry before the first one sent.
        prev_index: u64,
        /// Its term, for the consistency check.
        prev_term: u64,
        /// The entries.
        entries: Vec<Entry>,
        /// The leader's commit index.
        commit: u64,
    },
    /// A snapshot, for a follower that needs entries the leader no longer has.
    InstallSnapshot {
        /// The leader's term.
        term: u64,
        /// The snapshot.
        snapshot: Snapshot,
    },
    /// From a leader handing over its leadership: stand for election now, without a pre-vote (thesis
    /// 3.10).
    TimeoutNow {
        /// The leader's term.
        term: u64,
    },
    /// The answer to a [`Message::Append`] or a [`Message::InstallSnapshot`].
    AppendReply {
        /// The follower's term.
        term: u64,
        /// Whether the entries were appended (and persisted).
        success: bool,
        /// On success, the follower's last entry from this append; on failure, where the leader should
        /// look next: the follower's log agrees, at most, up to this index.
        index: u64,
    },
}

/// A message, with its sender and receiver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    /// The sender.
    pub from: NodeId,
    /// The receiver.
    pub to: NodeId,
    /// The message.
    pub message: Message,
}

/// What a node asks its driver to do: see the crate documentation for the order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Ready {
    /// The hard state to persist, if it changed.
    pub hard_state: Option<HardState>,
    /// Persisted entries from this index on must be dropped first: they conflicted with a leader's.
    pub truncate_from: Option<u64>,
    /// Entries to persist, in order.
    pub entries: Vec<Entry>,
    /// Messages to send, once the above is persisted.
    pub messages: Vec<Envelope>,
    /// Entries committed and durable on this node, to apply in order.
    pub committed: Vec<Entry>,
    /// A snapshot from the leader, to persist before anything else: the state machine is replaced by it,
    /// and persisted entries up to its index are dropped. Committed entries in the same ready follow it.
    pub snapshot: Option<Snapshot>,
}

impl Ready {
    /// Whether there is nothing to do.
    pub fn is_empty(&self) -> bool {
        self.hard_state.is_none()
            && self.truncate_from.is_none()
            && self.entries.is_empty()
            && self.messages.is_empty()
            && self.committed.is_empty()
            && self.snapshot.is_none()
    }
}

/// A node's role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Follows a leader, or waits for one.
    Follower,
    /// Asks whether it could win an election, without raising its term.
    PreCandidate,
    /// Stands for election in a new term.
    Candidate,
    /// Leads its term.
    Leader,
}

/// Timing, in ticks of the driver's clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// The shortest election timeout; each is chosen at random in `[election_ticks, 2 * election_ticks)`.
    pub election_ticks: u32,
    /// How often a leader sends heartbeats.
    pub heartbeat_ticks: u32,
    /// The most entries in one append.
    pub max_entries: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            election_ticks: 10,
            heartbeat_ticks: 2,
            max_entries: 64,
        }
    }
}

/// A change to the group's membership: one node at a time, so any majority of the old group overlaps
/// any majority of the new one (thesis 4.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change {
    /// A node joins.
    Add(NodeId),
    /// A node leaves.
    Remove(NodeId),
}

/// Why a membership change was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeError {
    /// This node is not the leader.
    NotLeader(NotLeader),
    /// Another change is not committed yet: one at a time.
    Pending,
    /// The leader has not committed an entry of its own term yet: until it has, a change could be
    /// overwritten (the correction to the single-server algorithm, Ongaro 2015).
    NotReady,
    /// It would change nothing, or leave no one.
    Pointless,
}

/// A proposal was refused: this node is not the leader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotLeader {
    /// The leader this node knows of, if any: where to propose instead.
    pub leader: Option<NodeId>,
}

/// Why a leadership transfer was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferError {
    /// This node is not the leader.
    NotLeader(NotLeader),
    /// The target is this node, or not in the group.
    NotAMember,
}

/// One member of a Raft group.
#[derive(Clone, Debug)]
pub struct RaftNode {
    id: NodeId,
    /// The group as the node was started with, before any change in its log or snapshot.
    bootstrap: Vec<NodeId>,
    /// The group now: the newest change in its log, committed or not, else its snapshot's, else the
    /// bootstrap.
    members: Vec<NodeId>,
    config: Config,
    rng: u64,
    term: u64,
    vote: Option<NodeId>,
    commit: u64,
    /// The log after the snapshot: entry `i` is at `log[i - snapshot_index - 1]`.
    log: Vec<Entry>,
    /// The last entry the snapshot includes, and its term: 0 and 0 without one.
    snapshot_index: u64,
    snapshot_term: u64,
    /// The snapshot, to send to followers that need what it replaced.
    snapshot: Option<Snapshot>,
    /// A snapshot from the leader that the driver has not been given yet.
    incoming: Option<Snapshot>,
    applied: u64,
    role: Role,
    leader: Option<NodeId>,
    elapsed: u32,
    timeout: u32,
    heartbeat_elapsed: u32,
    /// A leadership transfer under way: its target, and the ticks it has left.
    transfer: Option<(NodeId, u32)>,
    votes: BTreeMap<NodeId, bool>,
    next: BTreeMap<NodeId, u64>,
    matched: BTreeMap<NodeId, u64>,
    /// Nodes heard from since the leader last checked its quorum.
    active: BTreeSet<NodeId>,
    // What the next `Ready` carries.
    messages: Vec<Envelope>,
    stable: HardState,
    /// The first entry not yet handed to the driver to persist.
    unstable_from: u64,
    truncate_from: Option<u64>,
    /// The last entry the driver has persisted.
    persisted: u64,
}

impl RaftNode {
    /// A new member of the group `members`, with an empty log. `seed` drives its random election timeouts.
    /// A node joining a running group starts with no members: it takes its group from the leader's log.
    pub fn new(id: NodeId, members: &[NodeId], config: Config, seed: u64) -> RaftNode {
        RaftNode::restart(
            id,
            members,
            config,
            seed,
            HardState::default(),
            None,
            Vec::new(),
            0,
        )
    }

    /// A member restarted from what it persisted: its hard state, its snapshot and the log after it.
    /// `applied` is how far its state machine has durably applied the log (at least the snapshot);
    /// entries after it are applied again.
    #[allow(clippy::too_many_arguments)]
    pub fn restart(
        id: NodeId,
        members: &[NodeId],
        config: Config,
        seed: u64,
        hard_state: HardState,
        snapshot: Option<Snapshot>,
        log: Vec<Entry>,
        applied: u64,
    ) -> RaftNode {
        let (snapshot_index, snapshot_term) =
            snapshot.as_ref().map_or((0, 0), |s| (s.index, s.term));
        let last = log.last().map_or(snapshot_index, |e| e.index);
        let mut node = RaftNode {
            id,
            bootstrap: members.to_vec(),
            members: members.to_vec(),
            config,
            rng: seed ^ 0x9e37_79b9_7f4a_7c15 ^ id.wrapping_mul(0xbf58_476d_1ce4_e5b9),
            term: hard_state.term,
            vote: hard_state.vote,
            commit: hard_state.commit.min(last).max(snapshot_index),
            log,
            snapshot_index,
            snapshot_term,
            snapshot,
            incoming: None,
            applied: applied.max(snapshot_index).min(last),
            role: Role::Follower,
            leader: None,
            elapsed: 0,
            timeout: 0,
            heartbeat_elapsed: 0,
            transfer: None,
            votes: BTreeMap::new(),
            next: BTreeMap::new(),
            matched: BTreeMap::new(),
            active: BTreeSet::new(),
            messages: Vec::new(),
            stable: hard_state,
            unstable_from: last + 1,
            truncate_from: None,
            persisted: last,
        };
        node.refresh_members();
        node.reset_timeout();
        node
    }

    /// The group as it is now.
    pub fn members(&self) -> &[NodeId] {
        &self.members
    }

    /// The group as of `index`: the newest change at or before it.
    fn members_at(&self, index: u64) -> Vec<NodeId> {
        self.log
            .iter()
            .rev()
            .filter(|e| e.index <= index)
            .find_map(|e| e.members.clone())
            .or_else(|| self.snapshot.as_ref().map(|s| s.members.clone()))
            .unwrap_or_else(|| self.bootstrap.clone())
    }

    /// Takes the group from the log, after it changed. A leader starts replicating to a node that joins.
    fn refresh_members(&mut self) {
        self.members = self.members_at(u64::MAX);
        if self.role == Role::Leader {
            let next = self.last_index() + 1;
            for peer in self.others() {
                self.next.entry(peer).or_insert(next);
                self.matched.entry(peer).or_insert(0);
            }
        }
    }

    /// This node's id.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Its role.
    pub fn role(&self) -> Role {
        self.role
    }

    /// Its term.
    pub fn term(&self) -> u64 {
        self.term
    }

    /// The leader it knows of.
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// How far it knows the log is committed.
    pub fn commit(&self) -> u64 {
        self.commit
    }

    /// Its log after its snapshot.
    pub fn log(&self) -> &[Entry] {
        &self.log
    }

    /// The last entry its snapshot includes: 0 without one.
    pub fn snapshot_index(&self) -> u64 {
        self.snapshot_index
    }

    /// Its snapshot, if it has one.
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    /// The entry at `index`, unless the snapshot replaced it (or there is none).
    pub fn entry(&self, index: u64) -> Option<&Entry> {
        self.log.get(self.position(index)?)
    }

    fn position(&self, index: u64) -> Option<usize> {
        if index <= self.snapshot_index {
            return None;
        }
        usize::try_from(index - self.snapshot_index - 1).ok()
    }

    fn last_index(&self) -> u64 {
        self.log.last().map_or(self.snapshot_index, |e| e.index)
    }

    fn last_term(&self) -> u64 {
        self.log.last().map_or(self.snapshot_term, |e| e.term)
    }

    /// An entry's term: known for the snapshot's last entry, and unknown for those it replaced.
    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.entry(index).map(|e| e.term)
    }

    /// The driver has persisted a snapshot of its state machine as of `index`, which it has applied: the
    /// log up to it can go. The driver may then drop its persisted entries up to `index` too. Ignored for
    /// an index not applied, or already compacted.
    pub fn compact(&mut self, index: u64, data: Vec<u8>) {
        if index <= self.snapshot_index || index > self.applied {
            return;
        }
        let Some(term) = self.term_at(index) else {
            return;
        };
        let Some(position) = self.position(index) else {
            return;
        };
        let members = self.members_at(index);
        self.log.drain(..=position);
        self.snapshot_index = index;
        self.snapshot_term = term;
        self.snapshot = Some(Snapshot {
            index,
            term,
            data,
            members,
        });
    }

    fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }

    fn others(&self) -> Vec<NodeId> {
        self.members
            .iter()
            .copied()
            .filter(|m| *m != self.id)
            .collect()
    }

    fn random(&mut self) -> u64 {
        // SplitMix64: the node's own generator, so a seed reproduces its timeouts forever.
        self.rng = self.rng.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn reset_timeout(&mut self) {
        let base = self.config.election_ticks.max(1);
        let spread = u64::from(base);
        self.timeout = base + u32::try_from(self.random() % spread).unwrap_or(0);
        self.elapsed = 0;
    }

    fn send(&mut self, to: NodeId, message: Message) {
        self.messages.push(Envelope {
            from: self.id,
            to,
            message,
        });
    }

    /// One tick of the driver's clock.
    pub fn tick(&mut self) {
        self.elapsed = self.elapsed.saturating_add(1);
        if self.role == Role::Leader {
            // A transfer that has not happened within an election timeout is given up: this node leads
            // on, and takes proposals again.
            if let Some((target, left)) = self.transfer {
                self.transfer = (left > 1).then_some((target, left - 1));
            }
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.config.heartbeat_ticks {
                self.heartbeat_elapsed = 0;
                self.broadcast_append();
            }
            // Check quorum: a leader cut off from a majority steps down rather than lead alone.
            if self.elapsed >= self.timeout {
                self.active.insert(self.id);
                let heard = self
                    .active
                    .iter()
                    .filter(|id| self.members.contains(id))
                    .count();
                self.active.clear();
                self.elapsed = 0;
                if heard < self.quorum() {
                    self.become_follower(self.term, None);
                }
            }
        } else if self.elapsed >= self.timeout && self.members.contains(&self.id) {
            // Only a member stands: a node removed, or not yet added, waits to be told.
            self.pre_campaign();
        }
    }

    /// Proposes a membership change: its index, if this node leads and may change the group now. The new
    /// group counts from the moment the entry is in a log, committed or not.
    pub fn propose_change(&mut self, change: Change) -> Result<u64, ChangeError> {
        if self.role != Role::Leader || self.transfer.is_some() {
            return Err(ChangeError::NotLeader(NotLeader {
                leader: self.leader,
            }));
        }
        if self
            .log
            .iter()
            .any(|e| e.members.is_some() && e.index > self.commit)
        {
            return Err(ChangeError::Pending);
        }
        if self.term_at(self.commit) != Some(self.term) {
            return Err(ChangeError::NotReady);
        }
        let mut members = self.members.clone();
        match change {
            Change::Add(id) if !members.contains(&id) => members.push(id),
            Change::Remove(id) if members.contains(&id) && members.len() > 1 => {
                members.retain(|m| *m != id);
            }
            _ => return Err(ChangeError::Pointless),
        }
        members.sort_unstable();
        let index = self.last_index() + 1;
        self.log.push(Entry {
            term: self.term,
            index,
            data: Vec::new(),
            members: Some(members),
        });
        self.refresh_members();
        self.broadcast_append();
        Ok(index)
    }

    /// Proposes data for the log: its index, if this node leads. It is committed once a majority has it.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<u64, NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader {
                leader: self.leader,
            });
        }
        // Handing over: new entries would only keep the target behind.
        if let Some((target, _)) = self.transfer {
            return Err(NotLeader {
                leader: Some(target),
            });
        }
        let index = self.append_own(data);
        self.broadcast_append();
        Ok(index)
    }

    /// Hands leadership to `to`, a member (thesis 3.10): this node stops taking proposals, brings `to`'s
    /// log up to date, and tells it to stand for election at once. Given up after an election timeout,
    /// if `to` has not taken over by then.
    pub fn transfer_leadership(&mut self, to: NodeId) -> Result<(), TransferError> {
        if self.role != Role::Leader {
            return Err(TransferError::NotLeader(NotLeader {
                leader: self.leader,
            }));
        }
        if to == self.id || !self.members.contains(&to) {
            return Err(TransferError::NotAMember);
        }
        self.transfer = Some((to, self.config.election_ticks));
        self.nudge_transfer(to);
        Ok(())
    }

    /// On the leader: whether `node` has every entry this leader has, so that it answers and could take
    /// over at once.
    pub fn caught_up(&self, node: NodeId) -> bool {
        self.role == Role::Leader
            && self.matched.get(&node).copied().unwrap_or(0) >= self.last_index()
    }

    /// The target of a transfer under way, if any.
    pub fn transferring(&self) -> Option<NodeId> {
        self.transfer.map(|(target, _)| target)
    }

    /// Sends the transfer's target what it lacks, or, once it has everything, `TimeoutNow`.
    fn nudge_transfer(&mut self, target: NodeId) {
        if self.matched.get(&target).copied().unwrap_or(0) >= self.last_index() {
            let term = self.term;
            self.send(target, Message::TimeoutNow { term });
        } else {
            self.send_append(target);
        }
    }

    fn append_own(&mut self, data: Vec<u8>) -> u64 {
        let index = self.last_index() + 1;
        self.log.push(Entry {
            term: self.term,
            index,
            data,
            members: None,
        });
        index
    }

    fn pre_campaign(&mut self) {
        self.role = Role::PreCandidate;
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.id, true);
        self.reset_timeout();
        if self.won(true) {
            self.campaign();
            return;
        }
        let (term, last_index, last_term) = (self.term + 1, self.last_index(), self.last_term());
        for peer in self.others() {
            self.send(
                peer,
                Message::RequestVote {
                    term,
                    last_index,
                    last_term,
                    pre_vote: true,
                },
            );
        }
    }

    fn campaign(&mut self) {
        self.term += 1;
        self.vote = Some(self.id);
        self.role = Role::Candidate;
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.id, true);
        self.reset_timeout();
        if self.won(false) {
            self.become_leader();
            return;
        }
        let (term, last_index, last_term) = (self.term, self.last_index(), self.last_term());
        for peer in self.others() {
            self.send(
                peer,
                Message::RequestVote {
                    term,
                    last_index,
                    last_term,
                    pre_vote: false,
                },
            );
        }
    }

    fn won(&self, _pre_vote: bool) -> bool {
        // Only members' votes count, in the group as this node knows it.
        let granted = self
            .votes
            .iter()
            .filter(|(id, granted)| **granted && self.members.contains(id))
            .count();
        granted >= self.quorum()
    }

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) {
        self.transfer = None;
        if term > self.term {
            self.term = term;
            self.vote = None;
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        self.reset_timeout();
    }

    fn become_leader(&mut self) {
        self.transfer = None;
        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.heartbeat_elapsed = 0;
        self.active.clear();
        self.elapsed = 0;
        // Nothing about followers survives from an earlier term: a node outside the group then, added
        // back later, would otherwise be probed from where it once was, past the end of this log.
        self.next.clear();
        self.matched.clear();
        let next = self.last_index() + 1;
        for peer in self.others() {
            self.next.insert(peer, next);
            self.matched.insert(peer, 0);
        }
        self.matched.insert(self.id, self.persisted);
        // Entries of earlier terms commit only with one of this term (thesis 3.6.2).
        self.append_own(Vec::new());
        self.broadcast_append();
    }

    fn up_to_date(&self, last_index: u64, last_term: u64) -> bool {
        (last_term, last_index) >= (self.last_term(), self.last_index())
    }

    /// Handles a message from another node.
    pub fn step(&mut self, from: NodeId, message: Message) {
        // From anyone: a node joining hears from a leader before its log says who is in the group.
        if from == self.id {
            return;
        }
        match message {
            Message::RequestVote {
                term,
                last_index,
                last_term,
                pre_vote: true,
            } => {
                // Granted only to a candidate that is not behind, and only if this node has not heard
                // from a leader within an election timeout: a node cut off and come back cannot unseat
                // a working leader.
                let leaderless = match self.role {
                    Role::Leader => false,
                    Role::Follower => {
                        self.leader.is_none() || self.elapsed >= self.config.election_ticks
                    }
                    Role::PreCandidate | Role::Candidate => true,
                };
                let granted =
                    term > self.term && leaderless && self.up_to_date(last_index, last_term);
                // A refusal carries this node's own term: echoing the candidate's would raise it.
                let term = if granted { term } else { self.term };
                self.send(
                    from,
                    Message::Vote {
                        term,
                        granted,
                        pre_vote: true,
                    },
                );
            }
            Message::RequestVote {
                term,
                last_index,
                last_term,
                pre_vote: false,
            } => {
                if term > self.term {
                    self.become_follower(term, None);
                }
                let granted = term == self.term
                    && self.vote.is_none_or(|vote| vote == from)
                    && self.up_to_date(last_index, last_term);
                if granted {
                    self.vote = Some(from);
                    self.elapsed = 0;
                }
                self.send(
                    from,
                    Message::Vote {
                        term: self.term,
                        granted,
                        pre_vote: false,
                    },
                );
            }
            Message::Vote {
                term,
                granted,
                pre_vote,
            } => self.on_vote(from, term, granted, pre_vote),
            Message::Append {
                term,
                prev_index,
                prev_term,
                entries,
                commit,
            } => self.on_append(from, term, prev_index, prev_term, entries, commit),
            Message::InstallSnapshot { term, snapshot } => {
                self.on_install_snapshot(from, term, snapshot);
            }
            Message::AppendReply {
                term,
                success,
                index,
            } => self.on_append_reply(from, term, success, index),
            Message::TimeoutNow { term } => {
                // Only from this term's leader, to a member: a stale one could unseat a newer leader.
                if term == self.term
                    && self.role == Role::Follower
                    && self.leader == Some(from)
                    && self.members.contains(&self.id)
                {
                    self.campaign();
                }
            }
        }
    }

    fn on_vote(&mut self, from: NodeId, term: u64, granted: bool, pre_vote: bool) {
        if pre_vote {
            if self.role != Role::PreCandidate {
                return;
            }
            if granted && term == self.term + 1 {
                self.votes.insert(from, true);
                if self.won(true) {
                    self.campaign();
                }
            } else if !granted && term > self.term {
                // Someone is further on: follow, and learn the term.
                self.become_follower(term, None);
            }
            return;
        }
        if term > self.term {
            self.become_follower(term, None);
            return;
        }
        if self.role != Role::Candidate || term != self.term {
            return;
        }
        self.votes.insert(from, granted);
        if self.won(false) {
            self.become_leader();
        }
    }

    fn on_append(
        &mut self,
        from: NodeId,
        term: u64,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        commit: u64,
    ) {
        if term < self.term {
            let (term, index) = (self.term, self.last_index());
            self.send(
                from,
                Message::AppendReply {
                    term,
                    success: false,
                    index,
                },
            );
            return;
        }
        self.become_follower(term, Some(from));
        if prev_index > self.last_index() {
            let (term, index) = (self.term, self.last_index());
            self.send(
                from,
                Message::AppendReply {
                    term,
                    success: false,
                    index,
                },
            );
            return;
        }
        if let Some(found) = self.term_at(prev_index)
            && found != prev_term
        {
            // Skip the whole conflicting term: the leader looks before its first entry.
            let mut hint = prev_index - 1;
            while hint > 0 && self.term_at(hint) == Some(found) {
                hint -= 1;
            }
            let term = self.term;
            self.send(
                from,
                Message::AppendReply {
                    term,
                    success: false,
                    index: hint,
                },
            );
            return;
        }
        let last_new = (prev_index + entries.len() as u64).max(self.snapshot_index);
        for entry in entries {
            // The snapshot holds what it replaced, committed: nothing to compare or append.
            if entry.index <= self.snapshot_index {
                continue;
            }
            match self.term_at(entry.index) {
                Some(term) if term == entry.term => {}
                Some(_) => {
                    self.truncate(entry.index);
                    self.log.push(entry);
                }
                None => self.log.push(entry),
            }
        }
        self.refresh_members();
        self.commit = self.commit.max(commit.min(last_new));
        let term = self.term;
        self.send(
            from,
            Message::AppendReply {
                term,
                success: true,
                index: last_new,
            },
        );
    }

    fn truncate(&mut self, from: u64) {
        if let Some(position) = self.position(from) {
            self.log.truncate(position);
        }
        if from <= self.persisted {
            self.truncate_from = Some(self.truncate_from.map_or(from, |t| t.min(from)));
            self.persisted = from - 1;
        }
        self.unstable_from = self.unstable_from.min(from);
    }

    fn on_install_snapshot(&mut self, from: NodeId, term: u64, snapshot: Snapshot) {
        if term < self.term {
            let (term, index) = (self.term, self.last_index());
            self.send(
                from,
                Message::AppendReply {
                    term,
                    success: false,
                    index,
                },
            );
            return;
        }
        self.become_follower(term, Some(from));
        if snapshot.index <= self.commit {
            // It has all that, and more.
            let (term, index) = (self.term, self.commit);
            self.send(
                from,
                Message::AppendReply {
                    term,
                    success: true,
                    index,
                },
            );
            return;
        }
        // The log after the snapshot is kept if it agrees with it; otherwise none of it is.
        if self.term_at(snapshot.index) == Some(snapshot.term) {
            if let Some(position) = self.position(snapshot.index) {
                self.log.drain(..=position);
            }
        } else {
            if self.persisted > snapshot.index {
                self.truncate_from = Some(snapshot.index + 1);
            }
            self.log.clear();
            self.persisted = snapshot.index;
            self.unstable_from = snapshot.index + 1;
        }
        self.persisted = self.persisted.max(snapshot.index);
        self.unstable_from = self.unstable_from.max(snapshot.index + 1);
        self.snapshot_index = snapshot.index;
        self.snapshot_term = snapshot.term;
        self.commit = self.commit.max(snapshot.index);
        self.applied = self.applied.max(snapshot.index);
        self.snapshot = Some(snapshot.clone());
        let index = snapshot.index;
        self.incoming = Some(snapshot);
        self.refresh_members();
        let term = self.term;
        self.send(
            from,
            Message::AppendReply {
                term,
                success: true,
                index,
            },
        );
    }

    fn on_append_reply(&mut self, from: NodeId, term: u64, success: bool, index: u64) {
        if term > self.term {
            self.become_follower(term, None);
            return;
        }
        if self.role != Role::Leader || term < self.term {
            return;
        }
        self.active.insert(from);
        let matched = self.matched.get(&from).copied().unwrap_or(0);
        if success {
            let matched = matched.max(index);
            self.matched.insert(from, matched);
            let next = self.next.get(&from).copied().unwrap_or(1).max(matched + 1);
            self.next.insert(from, next);
            self.maybe_commit();
            if self.transfer.is_some_and(|(target, _)| target == from) {
                self.nudge_transfer(from);
            } else if next <= self.last_index() {
                self.send_append(from);
            }
        } else {
            let next = self.next.get(&from).copied().unwrap_or(1);
            let next = next
                .saturating_sub(1)
                .min(index + 1)
                .max(matched + 1)
                .max(1);
            self.next.insert(from, next);
            self.send_append(from);
        }
    }

    fn maybe_commit(&mut self) {
        let mut matched: Vec<u64> = self
            .members
            .iter()
            .map(|m| self.matched.get(m).copied().unwrap_or(0))
            .collect();
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let Some(&majority) = matched.get(self.quorum() - 1) else {
            return;
        };
        if majority > self.commit && self.term_at(majority) == Some(self.term) {
            self.commit = majority;
            // Followers learn it with the next append.
            self.broadcast_append();
        }
        // A leader that removed itself leads until the change is committed, then steps down
        // (thesis 4.2.2). Nothing it leads after that would count it.
        let change_committed = !self
            .log
            .iter()
            .any(|e| e.members.is_some() && e.index > self.commit);
        if self.role == Role::Leader && !self.members.contains(&self.id) && change_committed {
            self.become_follower(self.term, None);
        }
    }

    fn broadcast_append(&mut self) {
        for peer in self.others() {
            self.send_append(peer);
        }
    }

    fn send_append(&mut self, to: NodeId) {
        let next = self.next.get(&to).copied().unwrap_or(1).max(1);
        // What it needs is in the snapshot now.
        if next <= self.snapshot_index
            && let Some(snapshot) = self.snapshot.clone()
        {
            let term = self.term;
            self.send(to, Message::InstallSnapshot { term, snapshot });
            return;
        }
        let prev_index = next - 1;
        let prev_term = self.term_at(prev_index).unwrap_or(0);
        let start = usize::try_from(prev_index - self.snapshot_index).unwrap_or(usize::MAX);
        let end = start
            .saturating_add(self.config.max_entries)
            .min(self.log.len());
        let entries = self
            .log
            .get(start..end)
            .map(<[Entry]>::to_vec)
            .unwrap_or_default();
        let (term, commit) = (self.term, self.commit);
        self.send(
            to,
            Message::Append {
                term,
                prev_index,
                prev_term,
                entries,
                commit,
            },
        );
    }

    /// What the driver must do now (see the crate documentation).
    pub fn ready(&mut self) -> Ready {
        let hard_state = HardState {
            term: self.term,
            vote: self.vote,
            commit: self.commit,
        };
        let entries = self
            .position(self.unstable_from)
            .and_then(|position| self.log.get(position..))
            .map(<[Entry]>::to_vec)
            .unwrap_or_default();
        // Everything in this ready is persisted before anything is applied, so the whole log is durable.
        let durable = self.last_index();
        let apply_to = self.commit.min(durable);
        let committed = match (self.position(self.applied + 1), self.position(apply_to)) {
            (Some(first), Some(last)) => self
                .log
                .get(first..=last)
                .map(<[Entry]>::to_vec)
                .unwrap_or_default(),
            _ => Vec::new(),
        };
        let ready = Ready {
            hard_state: (hard_state != self.stable).then_some(hard_state),
            truncate_from: self.truncate_from.take(),
            entries,
            messages: std::mem::take(&mut self.messages),
            committed,
            snapshot: self.incoming.take(),
        };
        self.stable = hard_state;
        self.unstable_from = self.last_index() + 1;
        self.applied = self.applied.max(apply_to);
        ready
    }

    /// The driver has done what the last [`Ready`] asked: its entries are persisted.
    pub fn advance(&mut self) {
        self.persisted = self.unstable_from - 1;
        if self.role == Role::Leader {
            self.matched.insert(self.id, self.persisted);
            self.maybe_commit();
        }
    }

    /// Whether [`RaftNode::ready`] has anything to do.
    pub fn has_ready(&self) -> bool {
        !self.messages.is_empty()
            || self.incoming.is_some()
            || self.truncate_from.is_some()
            || self.unstable_from <= self.last_index()
            || self.applied < self.commit.min(self.last_index())
            || HardState {
                term: self.term,
                vote: self.vote,
                commit: self.commit,
            } != self.stable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Delivers every message until nothing is left, persisting and applying as the contract says.
    fn settle(nodes: &mut [RaftNode], applied: &mut [Vec<Entry>]) {
        loop {
            let mut sent = Vec::new();
            for (node, applied) in nodes.iter_mut().zip(applied.iter_mut()) {
                while node.has_ready() {
                    let ready = node.ready();
                    sent.extend(ready.messages);
                    applied.extend(ready.committed);
                    node.advance();
                }
            }
            if sent.is_empty() {
                return;
            }
            for envelope in sent {
                if let Some(node) = nodes.iter_mut().find(|n| n.id() == envelope.to) {
                    node.step(envelope.from, envelope.message);
                }
            }
        }
    }

    fn cluster(size: u64) -> Vec<RaftNode> {
        let members: Vec<NodeId> = (1..=size).collect();
        members
            .iter()
            .map(|id| RaftNode::new(*id, &members, Config::default(), 7))
            .collect()
    }

    fn elect(nodes: &mut [RaftNode], applied: &mut [Vec<Entry>], who: usize) {
        for _ in 0..40 {
            nodes[who].tick();
            settle(nodes, applied);
            if nodes[who].role() == Role::Leader {
                return;
            }
        }
        panic!("no leader");
    }

    #[test]
    fn a_leader_hands_over_to_a_follower_it_has_brought_up_to_date() {
        let mut nodes = cluster(3);
        let mut applied = vec![Vec::new(); 3];
        elect(&mut nodes, &mut applied, 0);
        // The target lags: it misses an entry before the transfer.
        nodes[0].propose(b"one".to_vec()).unwrap();
        let lost: Vec<Envelope> = nodes[0].ready().messages;
        nodes[0].advance();
        for envelope in lost.into_iter().filter(|e| e.to == 3) {
            nodes[1].step(envelope.from, envelope.message);
        }
        settle(&mut nodes, &mut applied);
        let term = nodes[0].term();
        nodes[0].transfer_leadership(3).unwrap();
        assert_eq!(nodes[0].transferring(), Some(3));
        // While it hands over, it takes no proposals.
        assert_eq!(
            nodes[0].propose(b"two".to_vec()),
            Err(NotLeader { leader: Some(3) })
        );
        settle(&mut nodes, &mut applied);
        assert_eq!(nodes[2].role(), Role::Leader);
        assert_eq!(nodes[2].term(), term + 1);
        assert_eq!(nodes[0].role(), Role::Follower);
        assert_eq!(nodes[0].transferring(), None);
        // Nothing committed is lost.
        assert!(nodes[2].log().iter().any(|e| e.data == b"one"));
        assert_eq!(
            nodes[0].transfer_leadership(2),
            Err(TransferError::NotLeader(NotLeader { leader: Some(3) }))
        );
        assert_eq!(
            nodes[2].transfer_leadership(9),
            Err(TransferError::NotAMember)
        );
    }

    #[test]
    fn a_transfer_that_does_not_happen_is_given_up() {
        let mut nodes = cluster(3);
        let mut applied = vec![Vec::new(); 3];
        elect(&mut nodes, &mut applied, 0);
        nodes[0].transfer_leadership(2).unwrap();
        // The target never hears of it.
        let _ = nodes[0].ready();
        nodes[0].advance();
        for _ in 0..Config::default().election_ticks {
            nodes[0].tick();
        }
        assert_eq!(nodes[0].transferring(), None);
        assert!(nodes[0].propose(b"again".to_vec()).is_ok());
    }

    #[test]
    fn a_single_node_leads_and_commits_alone() {
        let mut nodes = cluster(1);
        let mut applied = vec![Vec::new()];
        elect(&mut nodes, &mut applied, 0);
        assert_eq!(nodes[0].propose(b"a".to_vec()), Ok(2));
        settle(&mut nodes, &mut applied);
        let data: Vec<&[u8]> = applied[0].iter().map(|e| e.data.as_slice()).collect();
        assert_eq!(data, [b"".as_slice(), b"a"]);
    }

    #[test]
    fn three_nodes_elect_one_leader_and_replicate() {
        let mut nodes = cluster(3);
        let mut applied = vec![Vec::new(); 3];
        elect(&mut nodes, &mut applied, 0);
        assert_eq!(nodes[1].role(), Role::Follower);
        assert_eq!(nodes[1].leader(), Some(1));
        assert_eq!(
            nodes[1].propose(b"x".to_vec()),
            Err(NotLeader { leader: Some(1) })
        );
        nodes[0].propose(b"x".to_vec()).unwrap();
        settle(&mut nodes, &mut applied);
        for log in &applied {
            assert_eq!(log.last().map(|e| e.data.clone()), Some(b"x".to_vec()));
        }
    }

    #[test]
    fn a_follower_drops_a_conflicting_suffix() {
        let mut follower = RaftNode::restart(
            2,
            &[1, 2, 3],
            Config::default(),
            1,
            HardState {
                term: 1,
                vote: None,
                commit: 0,
            },
            None,
            vec![
                Entry {
                    term: 1,
                    index: 1,
                    data: b"a".to_vec(),
                    members: None,
                },
                Entry {
                    term: 1,
                    index: 2,
                    data: b"stale".to_vec(),
                    members: None,
                },
            ],
            0,
        );
        follower.step(
            1,
            Message::Append {
                term: 2,
                prev_index: 1,
                prev_term: 1,
                entries: vec![Entry {
                    term: 2,
                    index: 2,
                    data: b"b".to_vec(),
                    members: None,
                }],
                commit: 2,
            },
        );
        let ready = follower.ready();
        assert_eq!(ready.truncate_from, Some(2), "the persisted entry must go");
        assert_eq!(ready.entries.len(), 1);
        assert_eq!(ready.committed.len(), 2);
        assert_eq!(ready.committed[1].data, b"b");
        assert!(matches!(
            ready.messages[0].message,
            Message::AppendReply {
                success: true,
                index: 2,
                ..
            }
        ));
    }

    fn entry(term: u64, index: u64) -> Entry {
        Entry {
            term,
            index,
            data: vec![u8::try_from(index).unwrap_or(0)],
            members: None,
        }
    }

    /// Makes `node` lead the next term, with votes from `voters`.
    fn lead(node: &mut RaftNode, voters: &[NodeId]) {
        while node.role() != Role::PreCandidate {
            node.tick();
        }
        let term = node.term() + 1;
        for voter in voters {
            node.step(
                *voter,
                Message::Vote {
                    term,
                    granted: true,
                    pre_vote: true,
                },
            );
        }
        assert_eq!(node.role(), Role::Candidate);
        for voter in voters {
            node.step(
                *voter,
                Message::Vote {
                    term,
                    granted: true,
                    pre_vote: false,
                },
            );
        }
        assert_eq!(node.role(), Role::Leader);
        let _ = node.ready();
        node.advance();
    }

    #[test]
    fn an_earlier_terms_entry_is_not_committed_by_counting_replicas() {
        // The Raft paper's figure 8: a leader of term 4 holds an entry of term 2 that followers acknowledge.
        // Committing it because a majority has it would be unsafe: a node holding a term 3 entry at the same
        // index could still be elected and overwrite it.
        let mut leader = RaftNode::restart(
            1,
            &[1, 2, 3, 4, 5],
            Config::default(),
            9,
            HardState {
                term: 3,
                vote: None,
                commit: 1,
            },
            None,
            vec![entry(1, 1), entry(2, 2)],
            1,
        );
        lead(&mut leader, &[2, 3]);
        assert_eq!(leader.term(), 4);
        assert_eq!(
            leader.log().last().map(|e| (e.term, e.index)),
            Some((4, 3)),
            "the no-op"
        );
        for follower in [2, 3] {
            leader.step(
                follower,
                Message::AppendReply {
                    term: 4,
                    success: true,
                    index: 2,
                },
            );
        }
        assert_eq!(
            leader.commit(),
            1,
            "a majority holds index 2, but of an earlier term"
        );
        // Once the leader's own entry is on a majority, everything before it is committed with it.
        for follower in [2, 3] {
            leader.step(
                follower,
                Message::AppendReply {
                    term: 4,
                    success: true,
                    index: 3,
                },
            );
        }
        assert_eq!(leader.commit(), 3);
    }

    #[test]
    fn a_restarted_node_keeps_its_vote() {
        let members = [1, 2, 3];
        let mut node = RaftNode::new(2, &members, Config::default(), 3);
        node.step(
            1,
            Message::RequestVote {
                term: 5,
                last_index: 0,
                last_term: 0,
                pre_vote: false,
            },
        );
        let persisted = node.ready().hard_state.unwrap();
        assert_eq!(persisted.vote, Some(1));
        node.advance();
        // Restarted from what it persisted, it does not vote again in that term.
        let mut node = RaftNode::restart(
            2,
            &members,
            Config::default(),
            4,
            persisted,
            None,
            Vec::new(),
            0,
        );
        node.step(
            3,
            Message::RequestVote {
                term: 5,
                last_index: 0,
                last_term: 0,
                pre_vote: false,
            },
        );
        let refused = node.ready().messages;
        assert!(matches!(
            refused[0].message,
            Message::Vote {
                granted: false,
                term: 5,
                ..
            }
        ));
    }

    fn snapshot(index: u64, term: u64) -> Snapshot {
        Snapshot {
            index,
            term,
            data: b"state".to_vec(),
            members: vec![1, 2, 3],
        }
    }

    fn follower_with(log: Vec<Entry>, commit: u64) -> RaftNode {
        RaftNode::restart(
            2,
            &[1, 2, 3],
            Config::default(),
            1,
            HardState {
                term: 2,
                vote: None,
                commit,
            },
            None,
            log,
            commit,
        )
    }

    #[test]
    fn compaction_keeps_what_was_not_applied() {
        let mut node = follower_with(vec![entry(1, 1), entry(1, 2), entry(2, 3)], 2);
        node.compact(3, b"too far".to_vec());
        assert_eq!(node.snapshot_index(), 0, "index 3 is not applied");
        node.compact(2, b"state".to_vec());
        assert_eq!(node.snapshot_index(), 2);
        assert_eq!(node.log(), [entry(2, 3)]);
        assert_eq!(node.entry(2), None);
        assert_eq!(node.entry(3), Some(&entry(2, 3)));
        node.compact(1, b"older".to_vec());
        assert_eq!(node.snapshot_index(), 2, "a snapshot does not go back");
    }

    #[test]
    fn an_installed_snapshot_keeps_the_log_that_agrees_with_it() {
        let mut node = follower_with(vec![entry(1, 1), entry(1, 2), entry(2, 3)], 1);
        node.step(
            1,
            Message::InstallSnapshot {
                term: 2,
                snapshot: snapshot(2, 1),
            },
        );
        let ready = node.ready();
        assert_eq!(ready.snapshot.map(|s| s.index), Some(2));
        assert_eq!(ready.truncate_from, None, "index 3 agrees, and is kept");
        assert_eq!(node.log(), [entry(2, 3)]);
        assert!(matches!(
            ready.messages[0].message,
            Message::AppendReply {
                success: true,
                index: 2,
                ..
            }
        ));
    }

    #[test]
    fn an_installed_snapshot_discards_a_log_that_disagrees() {
        let mut node = follower_with(vec![entry(1, 1), entry(1, 2), entry(1, 3)], 1);
        node.step(
            1,
            Message::InstallSnapshot {
                term: 3,
                snapshot: snapshot(2, 2),
            },
        );
        let ready = node.ready();
        assert_eq!(
            ready.truncate_from,
            Some(3),
            "index 3 came from another history"
        );
        assert!(node.log().is_empty());
        assert_eq!(node.commit(), 2);
        // What it already has, it does not install again.
        node.advance();
        node.step(
            1,
            Message::InstallSnapshot {
                term: 3,
                snapshot: snapshot(1, 1),
            },
        );
        assert!(node.ready().snapshot.is_none());
    }

    #[test]
    fn an_append_right_after_the_snapshot_is_accepted() {
        let mut node = follower_with(vec![entry(1, 1), entry(1, 2)], 2);
        node.compact(2, b"state".to_vec());
        let _ = node.ready();
        node.advance();
        // The leader's previous entry is the snapshot's last: its term is known, and it agrees.
        node.step(
            1,
            Message::Append {
                term: 2,
                prev_index: 2,
                prev_term: 1,
                entries: vec![entry(2, 3)],
                commit: 3,
            },
        );
        let ready = node.ready();
        assert!(matches!(
            ready.messages[0].message,
            Message::AppendReply {
                success: true,
                index: 3,
                ..
            }
        ));
        assert_eq!(ready.committed, [entry(2, 3)]);
    }

    #[test]
    fn a_node_restarts_from_its_snapshot_and_the_log_after_it() {
        let node = RaftNode::restart(
            2,
            &[1, 2, 3],
            Config::default(),
            1,
            HardState {
                term: 2,
                vote: None,
                commit: 5,
            },
            Some(snapshot(4, 2)),
            vec![entry(2, 5), entry(2, 6)],
            4,
        );
        assert_eq!((node.snapshot_index(), node.commit()), (4, 5));
        assert_eq!(node.entry(5), Some(&entry(2, 5)));
        let mut node = node;
        let ready = node.ready();
        let applied: Vec<u64> = ready.committed.iter().map(|e| e.index).collect();
        assert_eq!(
            applied,
            [5],
            "only what follows the snapshot, up to the commit index"
        );
    }

    #[test]
    fn the_group_changes_one_node_at_a_time() {
        let mut nodes = cluster(3);
        // Node 4 joins with no group of its own: it learns it from the leader.
        nodes.push(RaftNode::new(4, &[], Config::default(), 11));
        let mut applied = vec![Vec::new(); 4];
        elect(&mut nodes, &mut applied, 0);
        assert_eq!(nodes[0].propose_change(Change::Add(4)), Ok(2));
        // One at a time: the next waits until this one commits.
        assert_eq!(
            nodes[0].propose_change(Change::Remove(2)),
            Err(ChangeError::Pending)
        );
        settle(&mut nodes, &mut applied);
        assert_eq!(nodes[0].members(), [1, 2, 3, 4]);
        assert_eq!(
            nodes[3].members(),
            [1, 2, 3, 4],
            "the new node knows its group"
        );
        assert_eq!(
            nodes[0].propose_change(Change::Add(4)),
            Err(ChangeError::Pointless)
        );
        assert!(nodes[0].propose_change(Change::Remove(2)).is_ok());
        settle(&mut nodes, &mut applied);
        assert_eq!(nodes[3].members(), [1, 3, 4]);
        assert_eq!(
            nodes[1].propose_change(Change::Remove(3)),
            Err(ChangeError::NotLeader(NotLeader { leader: Some(1) }))
        );
    }

    #[test]
    fn a_leader_changes_the_group_only_after_committing_in_its_term() {
        let mut leader = RaftNode::restart(
            1,
            &[1, 2, 3],
            Config::default(),
            9,
            HardState {
                term: 3,
                vote: None,
                commit: 1,
            },
            None,
            vec![entry(1, 1)],
            1,
        );
        lead(&mut leader, &[2, 3]);
        assert_eq!(
            leader.propose_change(Change::Add(4)),
            Err(ChangeError::NotReady)
        );
        for follower in [2, 3] {
            leader.step(
                follower,
                Message::AppendReply {
                    term: 4,
                    success: true,
                    index: 2,
                },
            );
        }
        assert_eq!(leader.commit(), 2, "its no-op");
        assert!(leader.propose_change(Change::Add(4)).is_ok());
    }

    #[test]
    fn a_leader_that_removes_itself_steps_down_once_that_commits() {
        let mut nodes = cluster(3);
        let mut applied = vec![Vec::new(); 3];
        elect(&mut nodes, &mut applied, 0);
        nodes[0].propose_change(Change::Remove(1)).unwrap();
        assert_eq!(
            nodes[0].role(),
            Role::Leader,
            "it leads until the change commits"
        );
        settle(&mut nodes, &mut applied);
        assert_eq!(nodes[0].role(), Role::Follower);
        // It is not in the group, so it does not stand again; the others elect a leader without it.
        for _ in 0..60 {
            nodes[0].tick();
        }
        assert_eq!(nodes[0].role(), Role::Follower);
        // Both remaining nodes' clocks run, so both notice the old leader is gone.
        for _ in 0..200 {
            nodes[1].tick();
            nodes[2].tick();
            settle(&mut nodes, &mut applied);
            if nodes[1..].iter().any(|n| n.role() == Role::Leader) {
                break;
            }
        }
        let leader = nodes[1..].iter().find(|n| n.role() == Role::Leader);
        assert_eq!(leader.map(RaftNode::members), Some([2, 3].as_slice()));
    }

    #[test]
    fn a_node_added_back_is_probed_from_this_terms_log() {
        // Node 1 led term 4, with node 4 caught up to index 6; then lost it and led term 6 with a
        // shorter log, in a group without node 4.
        let mut leader = RaftNode::restart(
            1,
            &[1, 2, 3, 4],
            Config::default(),
            9,
            HardState {
                term: 3,
                vote: None,
                commit: 0,
            },
            None,
            Vec::new(),
            0,
        );
        lead(&mut leader, &[2, 3]);
        for value in [b"v", b"w", b"x", b"y", b"z"] {
            leader.propose(value.to_vec()).unwrap();
        }
        // Node 4 is caught up to index 6, far past the log node 1 will lead with next.
        leader.step(
            4,
            Message::AppendReply {
                term: 4,
                success: true,
                index: 6,
            },
        );
        leader.step(
            2,
            Message::Append {
                term: 5,
                prev_index: 0,
                prev_term: 0,
                entries: Vec::new(),
                commit: 0,
            },
        );
        assert_eq!(leader.role(), Role::Follower);
        leader.step(
            2,
            Message::Append {
                term: 5,
                prev_index: 0,
                prev_term: 0,
                entries: vec![Entry {
                    term: 5,
                    index: 1,
                    data: Vec::new(),
                    members: Some(vec![1, 2, 3]),
                }],
                commit: 1,
            },
        );
        let _ = leader.ready();
        leader.advance();
        assert_eq!(leader.members(), [1, 2, 3]);
        lead(&mut leader, &[2, 3]);
        let _ = leader.ready();
        for follower in [2, 3] {
            leader.step(
                follower,
                Message::AppendReply {
                    term: 6,
                    success: true,
                    index: 2,
                },
            );
        }
        assert!(leader.propose_change(Change::Add(4)).is_ok());
        // Node 4 is probed from this log's end, not from where it was in term 4.
        let probes: Vec<u64> = leader
            .ready()
            .messages
            .iter()
            .filter(|m| m.to == 4)
            .filter_map(|m| match m.message {
                Message::Append { prev_index, .. } => Some(prev_index),
                _ => None,
            })
            .collect();
        assert!(
            !probes.is_empty() && probes.iter().all(|p| *p <= 3),
            "{probes:?}"
        );
    }

    #[test]
    fn votes_from_outside_the_group_do_not_count() {
        let mut node = RaftNode::new(1, &[1, 2, 3], Config::default(), 5);
        while node.role() != Role::PreCandidate {
            node.tick();
        }
        // Nodes 4 and 5 are not in its group: removed, or never added. Their votes elect nobody.
        for outsider in [4, 5] {
            node.step(
                outsider,
                Message::Vote {
                    term: 1,
                    granted: true,
                    pre_vote: true,
                },
            );
        }
        assert_eq!(node.role(), Role::PreCandidate);
        node.step(
            2,
            Message::Vote {
                term: 1,
                granted: true,
                pre_vote: true,
            },
        );
        assert_eq!(node.role(), Role::Candidate);
        for outsider in [4, 5] {
            node.step(
                outsider,
                Message::Vote {
                    term: 1,
                    granted: true,
                    pre_vote: false,
                },
            );
        }
        assert_eq!(
            node.role(),
            Role::Candidate,
            "only its own vote counts so far"
        );
        node.step(
            3,
            Message::Vote {
                term: 1,
                granted: true,
                pre_vote: false,
            },
        );
        assert_eq!(node.role(), Role::Leader);
    }

    #[test]
    fn a_vote_is_not_given_to_a_candidate_behind() {
        let mut node = RaftNode::restart(
            2,
            &[1, 2, 3],
            Config::default(),
            1,
            HardState {
                term: 3,
                vote: None,
                commit: 0,
            },
            None,
            vec![Entry {
                term: 3,
                index: 1,
                data: Vec::new(),
                members: None,
            }],
            0,
        );
        node.step(
            1,
            Message::RequestVote {
                term: 4,
                last_index: 5,
                last_term: 2,
                pre_vote: false,
            },
        );
        let ready = node.ready();
        assert!(matches!(
            ready.messages[0].message,
            Message::Vote {
                granted: false,
                term: 4,
                ..
            }
        ));
        assert_eq!(
            ready.hard_state.map(|h| h.term),
            Some(4),
            "the term is learned"
        );
    }
}
