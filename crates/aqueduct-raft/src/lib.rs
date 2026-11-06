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
    /// The answer to a [`Message::Append`].
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
}

impl Ready {
    /// Whether there is nothing to do.
    pub fn is_empty(&self) -> bool {
        self.hard_state.is_none()
            && self.truncate_from.is_none()
            && self.entries.is_empty()
            && self.messages.is_empty()
            && self.committed.is_empty()
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

/// A proposal was refused: this node is not the leader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotLeader {
    /// The leader this node knows of, if any: where to propose instead.
    pub leader: Option<NodeId>,
}

/// One member of a Raft group.
#[derive(Clone, Debug)]
pub struct RaftNode {
    id: NodeId,
    members: Vec<NodeId>,
    config: Config,
    rng: u64,
    term: u64,
    vote: Option<NodeId>,
    commit: u64,
    /// The log; entry `i` is at `log[i - 1]`.
    log: Vec<Entry>,
    applied: u64,
    role: Role,
    leader: Option<NodeId>,
    elapsed: u32,
    timeout: u32,
    heartbeat_elapsed: u32,
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
    /// A new member of the group `members` (which includes `id`), with an empty log. `seed` drives its
    /// random election timeouts.
    pub fn new(id: NodeId, members: &[NodeId], config: Config, seed: u64) -> RaftNode {
        RaftNode::restart(
            id,
            members,
            config,
            seed,
            HardState::default(),
            Vec::new(),
            0,
        )
    }

    /// A member restarted from what it persisted: its hard state and log. `applied` is how far its state
    /// machine has durably applied the log; entries after it are applied again.
    pub fn restart(
        id: NodeId,
        members: &[NodeId],
        config: Config,
        seed: u64,
        hard_state: HardState,
        log: Vec<Entry>,
        applied: u64,
    ) -> RaftNode {
        let last = log.last().map_or(0, |e| e.index);
        let mut node = RaftNode {
            id,
            members: members.to_vec(),
            config,
            rng: seed ^ 0x9e37_79b9_7f4a_7c15 ^ id.wrapping_mul(0xbf58_476d_1ce4_e5b9),
            term: hard_state.term,
            vote: hard_state.vote,
            commit: hard_state.commit.min(last),
            log,
            applied: applied.min(last),
            role: Role::Follower,
            leader: None,
            elapsed: 0,
            timeout: 0,
            heartbeat_elapsed: 0,
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
        node.reset_timeout();
        node
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

    /// Its log.
    pub fn log(&self) -> &[Entry] {
        &self.log
    }

    fn last_index(&self) -> u64 {
        self.log.last().map_or(0, |e| e.index)
    }

    fn last_term(&self) -> u64 {
        self.log.last().map_or(0, |e| e.term)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.log
            .get(usize::try_from(index - 1).ok()?)
            .map(|e| e.term)
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
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.config.heartbeat_ticks {
                self.heartbeat_elapsed = 0;
                self.broadcast_append();
            }
            // Check quorum: a leader cut off from a majority steps down rather than lead alone.
            if self.elapsed >= self.timeout {
                self.active.insert(self.id);
                let heard = self.active.len();
                self.active.clear();
                self.elapsed = 0;
                if heard < self.quorum() {
                    self.become_follower(self.term, None);
                }
            }
        } else if self.elapsed >= self.timeout {
            self.pre_campaign();
        }
    }

    /// Proposes data for the log: its index, if this node leads. It is committed once a majority has it.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<u64, NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader {
                leader: self.leader,
            });
        }
        let index = self.append_own(data);
        self.broadcast_append();
        Ok(index)
    }

    fn append_own(&mut self, data: Vec<u8>) -> u64 {
        let index = self.last_index() + 1;
        self.log.push(Entry {
            term: self.term,
            index,
            data,
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
        self.votes.values().filter(|granted| **granted).count() >= self.quorum()
    }

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) {
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
        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.heartbeat_elapsed = 0;
        self.active.clear();
        self.elapsed = 0;
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
        if from == self.id || !self.members.contains(&from) {
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
            Message::AppendReply {
                term,
                success,
                index,
            } => self.on_append_reply(from, term, success, index),
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
        let last_new = prev_index + entries.len() as u64;
        for entry in entries {
            match self.term_at(entry.index) {
                Some(term) if term == entry.term => {}
                Some(_) => {
                    self.truncate(entry.index);
                    self.log.push(entry);
                }
                None => self.log.push(entry),
            }
        }
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
        self.log.truncate(usize::try_from(from - 1).unwrap_or(0));
        if from <= self.persisted {
            self.truncate_from = Some(self.truncate_from.map_or(from, |t| t.min(from)));
            self.persisted = from - 1;
        }
        self.unstable_from = self.unstable_from.min(from);
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
            if next <= self.last_index() {
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
    }

    fn broadcast_append(&mut self) {
        for peer in self.others() {
            self.send_append(peer);
        }
    }

    fn send_append(&mut self, to: NodeId) {
        let next = self.next.get(&to).copied().unwrap_or(1).max(1);
        let prev_index = next - 1;
        let prev_term = self.term_at(prev_index).unwrap_or(0);
        let start = usize::try_from(prev_index).unwrap_or(usize::MAX);
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
            .log
            .get(usize::try_from(self.unstable_from - 1).unwrap_or(usize::MAX)..)
            .map(<[Entry]>::to_vec)
            .unwrap_or_default();
        // Everything in this ready is persisted before anything is applied, so the whole log is durable.
        let durable = self.last_index();
        let apply_to = self.commit.min(durable);
        let committed = self
            .log
            .get(
                usize::try_from(self.applied).unwrap_or(usize::MAX)
                    ..usize::try_from(apply_to).unwrap_or(0),
            )
            .map(<[Entry]>::to_vec)
            .unwrap_or_default();
        let ready = Ready {
            hard_state: (hard_state != self.stable).then_some(hard_state),
            truncate_from: self.truncate_from.take(),
            entries,
            messages: std::mem::take(&mut self.messages),
            committed,
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
            vec![
                Entry {
                    term: 1,
                    index: 1,
                    data: b"a".to_vec(),
                },
                Entry {
                    term: 1,
                    index: 2,
                    data: b"stale".to_vec(),
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
        let mut node =
            RaftNode::restart(2, &members, Config::default(), 4, persisted, Vec::new(), 0);
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
            vec![Entry {
                term: 3,
                index: 1,
                data: Vec::new(),
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
