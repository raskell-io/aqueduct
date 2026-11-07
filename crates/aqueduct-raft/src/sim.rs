//! A deterministic simulator for Raft groups (ADR 0051, ADR 0006).
//!
//! One seed drives one world: a group of nodes, each with a disk that keeps what its `Ready`s asked to
//! persist, a clock that ticks at its own rate (skew) and sometimes stops (a pause), and a network that
//! delays, reorders, drops and duplicates messages, and splits the group into sides. Nodes crash, losing
//! everything not persisted, and restart. A client proposes values to whichever node it thinks leads,
//! and retries. Then faults heal, and the group must converge.
//!
//! Checked after every event:
//!
//! - **election safety:** at most one leader per term;
//! - **log matching:** two logs with an entry of the same index and term agree up to it;
//! - **leader completeness:** a new leader's log holds every entry applied anywhere;
//! - **state machine safety:** no two nodes apply different entries at one index;
//! - **durability:** a node's disk holds what it applied: in its snapshot, or its log;
//! - **membership:** an operator adds and removes nodes one at a time during faults, the leader too;
//!   removed nodes keep running. The checks above hold throughout, and at the end for the final group;
//! - **snapshots:** every node's state machine agrees at each index, whether it applied entries or
//!   restored a snapshot (nodes compact their logs often, and followers catch up from snapshots).
//!
//! And at the end: every proposal the client saw applied is applied, at the same index, on every node;
//! every node has applied the same log; and the client's proposals all got in (liveness, once healed).

use std::collections::{BTreeMap, BTreeSet};

use crate::{Change, Config, Entry, Envelope, HardState, NodeId, RaftNode, Role, Snapshot};

/// Parameters of a simulated world. Probabilities are in parts per million, times in milliseconds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimConfig {
    /// Nodes in the world.
    pub nodes: u64,
    /// How many of them form the group at first; the others wait to be added.
    pub initial_members: u64,
    /// Whether an operator changes the group while faults last.
    pub changes: bool,
    /// Values the client proposes.
    pub proposals: usize,
    /// How long faults are injected.
    pub fault_window: u64,
    /// By when everything must be done.
    pub max_time: u64,
    /// Chance that a message is dropped.
    pub drop_ppm: u32,
    /// Chance that a message is duplicated.
    pub duplicate_ppm: u32,
    /// Longest message delay; random delays reorder messages.
    pub max_delay: u64,
    /// Chance, per event, that a node crashes.
    pub crash_ppm: u32,
    /// Chance, per event, that the network splits.
    pub partition_ppm: u32,
    /// Chance, per event, that a node's clock stops for a while.
    pub pause_ppm: u32,
    /// The ticks of each node's clock are this many milliseconds apart, plus up to `skew` more.
    pub tick_ms: u64,
    /// How much slower than `tick_ms` a node's clock may run.
    pub skew: u64,
}

impl Default for SimConfig {
    fn default() -> SimConfig {
        SimConfig {
            nodes: 5,
            initial_members: 3,
            changes: true,
            proposals: 60,
            fault_window: 4_000,
            max_time: 60_000,
            drop_ppm: 50_000,
            duplicate_ppm: 20_000,
            max_delay: 12,
            crash_ppm: 1_500,
            partition_ppm: 600,
            pause_ppm: 600,
            tick_ms: 10,
            skew: 5,
        }
    }
}

/// What a world did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SimReport {
    /// Events processed.
    pub events: u64,
    /// Crashes.
    pub crashes: u64,
    /// Network splits.
    pub partitions: u64,
    /// Clock pauses.
    pub pauses: u64,
    /// Terms in which some node led.
    pub leaders: u64,
    /// Entries applied on every node at the end.
    pub applied: u64,
    /// Messages dropped and duplicated.
    pub dropped: u64,
    /// Messages duplicated.
    pub duplicated: u64,
    /// When the world was done.
    pub end_time: u64,
    /// Logs compacted into snapshots.
    pub compactions: u64,
    /// Snapshots a follower installed from its leader.
    pub installs: u64,
    /// Membership changes committed.
    pub changes: u64,
    /// Leadership transfers the operator started.
    pub transfers: u64,
}

/// A world that broke an invariant, with the seed that reproduces it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimFailure {
    /// The seed.
    pub seed: u64,
    /// When.
    pub time: u64,
    /// What broke.
    pub message: String,
}

/// SplitMix64: the simulator's own generator, so a seed reproduces a world forever (ADR 0006).
#[derive(Clone, Debug)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }

    fn chance(&mut self, ppm: u32) -> bool {
        self.below(1_000_000) < u64::from(ppm)
    }
}

/// What a node's disk holds.
#[derive(Clone, Debug, Default)]
struct Disk {
    hard_state: HardState,
    snapshot: Option<Snapshot>,
    log: Vec<Entry>,
}

/// A node's state machine: how far it has applied the log, and a digest of everything applied.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct State {
    index: u64,
    digest: u64,
}

impl State {
    fn apply(self, entry: &Entry) -> State {
        let mut digest = self.digest ^ 0xcbf2_9ce4_8422_2325;
        let members: Vec<u8> = entry
            .members
            .iter()
            .flatten()
            .flat_map(|m| m.to_le_bytes())
            .collect();
        for byte in entry
            .term
            .to_le_bytes()
            .iter()
            .chain(&entry.index.to_le_bytes())
            .chain(&entry.data)
            .chain(&members)
        {
            digest ^= u64::from(*byte);
            digest = digest.wrapping_mul(0x0100_0000_01b3);
        }
        State {
            index: entry.index,
            digest,
        }
    }

    fn encode(self) -> Vec<u8> {
        [self.index.to_le_bytes(), self.digest.to_le_bytes()].concat()
    }

    fn decode(data: &[u8]) -> Option<State> {
        let index = u64::from_le_bytes(data.get(..8)?.try_into().ok()?);
        let digest = u64::from_le_bytes(data.get(8..16)?.try_into().ok()?);
        Some(State { index, digest })
    }
}

#[derive(Clone, Debug)]
enum Event {
    Tick {
        node: NodeId,
        incarnation: u64,
    },
    Deliver(Envelope),
    Restart {
        node: NodeId,
    },
    Propose,
    Heal,
    Change,
    /// The operator hands leadership to another member.
    Transfer,
}

struct World<'c> {
    config: &'c SimConfig,
    seed: u64,
    rng: Rng,
    now: u64,
    seq: u64,
    queue: BTreeMap<(u64, u64), Event>,
    /// Every node in the world, member or not.
    members: Vec<NodeId>,
    /// The group the world starts with.
    initial: Vec<NodeId>,
    nodes: BTreeMap<NodeId, RaftNode>,
    incarnations: BTreeMap<NodeId, u64>,
    disks: BTreeMap<NodeId, Disk>,
    tick_ms: BTreeMap<NodeId, u64>,
    paused_until: BTreeMap<NodeId, u64>,
    /// While the network is split: the side of each node.
    sides: Option<BTreeMap<NodeId, bool>>,
    /// Each node's state machine, as it is since it last started.
    states: BTreeMap<NodeId, State>,
    /// The state machine's digest after each index, wherever it was reached: they must all agree.
    digest_at: BTreeMap<u64, u64>,
    /// The entry applied at each index, anywhere: they must all agree.
    applied_at: BTreeMap<u64, Entry>,
    /// The term whose leader first committed each index, with the entry it committed.
    committed_in: BTreeMap<u64, (u64, Entry)>,
    leaders: BTreeMap<u64, NodeId>,
    /// The client's proposals not yet seen applied, and those seen.
    pending: BTreeSet<u64>,
    acked: BTreeMap<u64, u64>,
    next_proposal: u64,
    /// The node the client tries next.
    target: NodeId,
    faults: bool,
    report: SimReport,
}

/// Runs one world.
pub fn run(seed: u64, config: &SimConfig) -> Result<SimReport, SimFailure> {
    let mut rng = Rng(seed);
    let members: Vec<NodeId> = (1..=config.nodes).collect();
    let initial: Vec<NodeId> = (1..=config.initial_members.clamp(1, config.nodes)).collect();
    let raft = Config::default();
    let mut nodes = BTreeMap::new();
    let mut tick_ms = BTreeMap::new();
    for id in &members {
        nodes.insert(
            *id,
            RaftNode::new(*id, &bootstrap(*id, &initial), raft, rng.next()),
        );
        tick_ms.insert(*id, config.tick_ms + rng.below(config.skew + 1));
    }
    let mut world = World {
        config,
        seed,
        rng,
        now: 0,
        seq: 0,
        queue: BTreeMap::new(),
        members: members.clone(),
        initial,
        incarnations: members.iter().map(|id| (*id, 0)).collect(),
        disks: members.iter().map(|id| (*id, Disk::default())).collect(),
        states: members.iter().map(|id| (*id, State::default())).collect(),
        digest_at: BTreeMap::new(),
        nodes,
        tick_ms,
        paused_until: BTreeMap::new(),
        sides: None,
        applied_at: BTreeMap::new(),
        committed_in: BTreeMap::new(),
        leaders: BTreeMap::new(),
        pending: BTreeSet::new(),
        acked: BTreeMap::new(),
        next_proposal: 0,
        target: 1,
        faults: true,
        report: SimReport::default(),
    };
    world.run().map_err(|message| SimFailure {
        seed,
        time: world.now,
        message,
    })
}

impl World<'_> {
    fn schedule(&mut self, delay: u64, event: Event) {
        self.queue.insert((self.now + delay, self.seq), event);
        self.seq += 1;
    }

    fn run(&mut self) -> Result<SimReport, String> {
        for id in self.members.clone() {
            let delay = self.rng.below(self.tick_ms[&id]);
            self.schedule(
                delay,
                Event::Tick {
                    node: id,
                    incarnation: 0,
                },
            );
        }
        self.schedule(1, Event::Propose);
        if self.config.changes {
            self.schedule(100, Event::Change);
        }
        self.schedule(150, Event::Transfer);
        loop {
            let Some(((time, _), event)) = self.queue.pop_first() else {
                return Err("the event queue ran dry".into());
            };
            self.now = time;
            self.report.events += 1;
            // A world takes a few thousand events: far more is a storm that never settles.
            if self.report.events > 2_000_000 {
                return Err(format!(
                    "liveness: {} events by time {}, and not done: messages without progress\n{}",
                    self.report.events,
                    self.now,
                    self.summary()
                ));
            }
            if self.faults && self.now >= self.config.fault_window {
                self.faults = false;
                self.sides = None;
                self.paused_until.clear();
            }
            if self.now > self.config.max_time {
                return Err(format!(
                    "liveness: not done by {}: {} proposals pending, leaders by term {:?}, seed {}\n{}",
                    self.config.max_time,
                    self.pending.len(),
                    self.leaders.iter().rev().take(3).collect::<Vec<_>>(),
                    self.seed,
                    self.summary()
                ));
            }
            self.handle(event)?;
            if self.faults {
                self.inject();
            }
            self.check()?;
            if !self.faults && self.done() {
                self.final_checks()?;
                self.report.end_time = self.now;
                self.report.leaders = self.leaders.len() as u64;
                return Ok(std::mem::take(&mut self.report));
            }
        }
    }

    fn handle(&mut self, event: Event) -> Result<(), String> {
        match event {
            Event::Tick { node, incarnation } => {
                if self.incarnations.get(&node) != Some(&incarnation) {
                    return Ok(());
                }
                let paused = self
                    .paused_until
                    .get(&node)
                    .is_some_and(|until| *until > self.now);
                if !paused && let Some(raft) = self.nodes.get_mut(&node) {
                    raft.tick();
                    self.drive(node)?;
                }
                let delay = self.tick_ms[&node];
                self.schedule(delay, Event::Tick { node, incarnation });
            }
            Event::Deliver(envelope) => {
                let to = envelope.to;
                if let Some(until) = self.paused_until.get(&to).copied()
                    && until > self.now
                {
                    // A paused node reads its socket when it resumes.
                    let wait = until - self.now;
                    self.schedule(wait, Event::Deliver(envelope));
                    return Ok(());
                }
                if let Some(raft) = self.nodes.get_mut(&to) {
                    raft.step(envelope.from, envelope.message);
                    self.drive(to)?;
                }
            }
            Event::Restart { node } => {
                let disk = self.disks[&node].clone();
                let seed = self.rng.next();
                // Its state machine is what its snapshot holds; the log after it is applied again.
                let state = match &disk.snapshot {
                    Some(snapshot) => State::decode(&snapshot.data)
                        .ok_or_else(|| format!("node {node}'s snapshot does not decode"))?,
                    None => State::default(),
                };
                let raft = RaftNode::restart(
                    node,
                    &bootstrap(node, &self.initial),
                    Config::default(),
                    seed,
                    disk.hard_state,
                    disk.snapshot,
                    disk.log,
                    state.index,
                );
                self.nodes.insert(node, raft);
                let incarnation = self.incarnations[&node] + 1;
                self.incarnations.insert(node, incarnation);
                self.states.insert(node, state);
                self.schedule(1, Event::Tick { node, incarnation });
            }
            Event::Propose => self.propose(),
            Event::Heal => self.sides = None,
            Event::Change => self.change(),
            Event::Transfer => self.transfer(),
        }
        Ok(())
    }

    /// The operator, while faults last: asks whichever member leads to hand leadership to another
    /// member, which it may or may not manage before an election timeout.
    fn transfer(&mut self) {
        if !self.faults {
            return;
        }
        let leader = self
            .nodes
            .values()
            .find(|n| n.role() == Role::Leader && n.members().contains(&n.id()))
            .map(RaftNode::id);
        if let Some(leader) = leader {
            let others: Vec<NodeId> = self.nodes[&leader]
                .members()
                .iter()
                .copied()
                .filter(|m| *m != leader)
                .collect();
            if !others.is_empty() {
                let to = others[self.rng.below(others.len() as u64) as usize];
                if let Some(raft) = self.nodes.get_mut(&leader)
                    && raft.transfer_leadership(to).is_ok()
                {
                    self.report.transfers += 1;
                }
                let _ = self.drive(leader);
            }
        }
        let delay = 150 + self.rng.below(500);
        self.schedule(delay, Event::Transfer);
    }

    /// The operator: while faults last, adds or removes a node through whichever member leads, keeping
    /// the group between three and five (or what the world allows). It is told nothing: a change that
    /// does not commit is simply not there.
    fn change(&mut self) {
        if !self.faults {
            return;
        }
        let leader = self
            .nodes
            .values()
            .find(|n| n.role() == Role::Leader && n.members().contains(&n.id()))
            .map(RaftNode::id);
        if let Some(leader) = leader {
            let group = self.nodes[&leader].members().to_vec();
            let outside: Vec<NodeId> = self
                .members
                .iter()
                .copied()
                .filter(|m| !group.contains(m))
                .collect();
            let (low, high) = (3.min(self.members.len()), 5.min(self.members.len()));
            let add = match group.len() {
                n if n <= low => true,
                n if n >= high => false,
                _ => self.rng.below(2) == 0,
            };
            let change = if add && !outside.is_empty() {
                Change::Add(outside[self.rng.below(outside.len() as u64) as usize])
            } else {
                Change::Remove(group[self.rng.below(group.len() as u64) as usize])
            };
            if let Some(raft) = self.nodes.get_mut(&leader) {
                let _ = raft.propose_change(change);
            }
            let _ = self.drive(leader);
        }
        let delay = 100 + self.rng.below(400);
        self.schedule(delay, Event::Change);
    }

    /// The client: proposes the next value, or proposes a pending one again, to the node it thinks leads.
    fn propose(&mut self) {
        let value = if self.next_proposal < self.config.proposals as u64 {
            self.next_proposal += 1;
            self.pending.insert(self.next_proposal);
            Some(self.next_proposal)
        } else {
            let pending: Vec<u64> = self.pending.iter().copied().collect();
            pending
                .get(self.rng.below(pending.len() as u64) as usize)
                .copied()
        };
        let Some(value) = value else {
            return;
        };
        let target = self.target;
        if let Some(raft) = self.nodes.get_mut(&target) {
            match raft.propose(value.to_be_bytes().to_vec()) {
                Ok(_) => {}
                Err(not_leader) => {
                    self.target = not_leader.leader.unwrap_or_else(|| {
                        self.members[self.rng.below(self.members.len() as u64) as usize]
                    });
                }
            }
            let _ = self.drive(target);
        } else {
            self.target = self.members[self.rng.below(self.members.len() as u64) as usize];
        }
        if !self.pending.is_empty() || self.next_proposal < self.config.proposals as u64 {
            let delay = 5 + self.rng.below(40);
            self.schedule(delay, Event::Propose);
        }
    }

    /// Does what the node's `Ready`s ask: persists, then sends, then applies.
    fn drive(&mut self, node: NodeId) -> Result<(), String> {
        for _ in 0..10_000 {
            let Some(raft) = self.nodes.get_mut(&node) else {
                return Ok(());
            };
            if !raft.has_ready() {
                return Ok(());
            }
            let ready = raft.ready();
            let (role, term) = (raft.role(), raft.term());
            let disk = self.disks.entry(node).or_default();
            // A leader's snapshot first: it replaces the state machine and the log it covers.
            if let Some(snapshot) = &ready.snapshot {
                disk.log.retain(|e| e.index > snapshot.index);
                disk.snapshot = Some(snapshot.clone());
            }
            if let Some(hard_state) = ready.hard_state {
                disk.hard_state = hard_state;
            }
            if let Some(from) = ready.truncate_from {
                disk.log.retain(|e| e.index < from);
            }
            // An append-only log: it drops entries only when told to.
            for entry in &ready.entries {
                disk.log.push(entry.clone());
            }
            let base = disk.snapshot.as_ref().map_or(0, |s| s.index);
            if let Some((position, entry)) = disk
                .log
                .iter()
                .enumerate()
                .find(|(position, entry)| entry.index != base + *position as u64 + 1)
            {
                return Err(format!(
                    "node {node}'s disk holds index {} at position {}: a conflicting suffix was kept",
                    entry.index,
                    position + 1
                ));
            }
            if role == Role::Leader {
                self.became_leader(node, term)?;
            }
            // Any node may learn a commit first: a leader removed by a change commits it and steps
            // down at once. A follower's term is at least the committing leader's, which only makes
            // the completeness check looser, never wrong.
            self.record_commits(node, term)?;
            if let Some(snapshot) = &ready.snapshot {
                self.restore(node, snapshot)?;
            }
            for envelope in ready.messages {
                self.send(envelope);
            }
            for entry in ready.committed {
                self.apply(node, entry)?;
            }
            if let Some(raft) = self.nodes.get_mut(&node) {
                raft.advance();
            }
            self.maybe_compact(node);
        }
        Err(format!(
            "node {node} kept asking for more: 10,000 readies for one event"
        ))
    }

    /// A node's state machine is replaced by a snapshot from its leader.
    fn restore(&mut self, node: NodeId, snapshot: &Snapshot) -> Result<(), String> {
        let state = State::decode(&snapshot.data)
            .ok_or_else(|| format!("node {node} was sent a snapshot that does not decode"))?;
        if state.index != snapshot.index {
            return Err(format!(
                "node {node} was sent a snapshot of index {} that holds index {}",
                snapshot.index, state.index
            ));
        }
        if let Some(expected) = self.digest_at.get(&state.index)
            && *expected != state.digest
        {
            return Err(format!(
                "snapshot safety: node {node} installed a snapshot of index {} that differs from the state applied there",
                state.index
            ));
        }
        self.digest_at.insert(state.index, state.digest);
        self.states.insert(node, state);
        self.report.installs += 1;
        Ok(())
    }

    /// Sometimes, a node compacts what it has applied into a snapshot: often enough that followers
    /// fall behind it and need one.
    fn maybe_compact(&mut self, node: NodeId) {
        let Some(raft) = self.nodes.get_mut(&node) else {
            return;
        };
        let state = self.states[&node];
        if state.index < raft.snapshot_index() + 4 || !self.rng.chance(300_000) {
            return;
        }
        // The driver persists its snapshot before the node drops the log it replaces.
        raft.compact(state.index, state.encode());
        let Some(snapshot) = raft.snapshot().cloned() else {
            return;
        };
        if snapshot.index != state.index {
            return;
        }
        let disk = self.disks.entry(node).or_default();
        disk.log.retain(|e| e.index > snapshot.index);
        disk.snapshot = Some(snapshot);
        self.report.compactions += 1;
    }

    fn became_leader(&mut self, node: NodeId, term: u64) -> Result<(), String> {
        match self.leaders.get(&term) {
            Some(leader) if *leader != node => {
                return Err(format!(
                    "election safety: nodes {leader} and {node} both lead term {term}"
                ));
            }
            Some(_) => return Ok(()),
            None => {}
        }
        self.leaders.insert(term, node);
        // Leader completeness: it holds every entry committed in an earlier term (thesis 3.6.3). A
        // leader of an earlier term can still come to power after a later one has committed: delayed
        // votes do that, and it need not hold those.
        let raft = &self.nodes[&node];
        for (index, (committed_term, entry)) in &self.committed_in {
            if *committed_term >= term {
                continue;
            }
            // What its snapshot replaced, it holds as state: the snapshot checks say it is the same.
            if *index <= raft.snapshot_index() {
                continue;
            }
            if raft.entry(*index) != Some(entry) {
                return Err(format!(
                    "leader completeness: node {node}, leader of term {term}, lacks {entry:?}, committed at {index} in term {committed_term}"
                ));
            }
        }
        Ok(())
    }

    /// Notes, for each index known committed, the term of the first node to know it.
    fn record_commits(&mut self, node: NodeId, term: u64) -> Result<(), String> {
        let Some(raft) = self.nodes.get(&node) else {
            return Ok(());
        };
        let commit = raft.commit();
        let first = self
            .committed_in
            .keys()
            .next_back()
            .map_or(1, |last| last + 1);
        for index in first..=commit {
            let Some(entry) = raft.entry(index) else {
                // In its snapshot: the snapshot checks cover it.
                if index <= raft.snapshot_index() {
                    continue;
                }
                return Err(format!(
                    "node {node} committed index {index} it does not hold"
                ));
            };
            self.committed_in.insert(index, (term, entry.clone()));
        }
        Ok(())
    }

    fn apply(&mut self, node: NodeId, entry: Entry) -> Result<(), String> {
        let state = self.states.get(&node).copied().unwrap_or_default();
        if entry.index != state.index + 1 {
            return Err(format!(
                "node {node} applied index {} after {}",
                entry.index, state.index
            ));
        }
        if let Some(earlier) = self.applied_at.get(&entry.index)
            && *earlier != entry
        {
            return Err(format!(
                "state machine safety: index {} applied as {earlier:?} and as {entry:?}",
                entry.index
            ));
        }
        self.applied_at.insert(entry.index, entry.clone());
        if entry.data.len() == 8 {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&entry.data);
            let value = u64::from_be_bytes(bytes);
            if self.pending.remove(&value) {
                self.acked.insert(value, entry.index);
            }
        }
        let state = state.apply(&entry);
        if let Some(expected) = self.digest_at.get(&state.index)
            && *expected != state.digest
        {
            return Err(format!(
                "state machine safety: node {node}'s state after index {} differs from another's",
                state.index
            ));
        }
        self.digest_at.insert(state.index, state.digest);
        self.states.insert(node, state);
        Ok(())
    }

    fn send(&mut self, envelope: Envelope) {
        if let Some(sides) = &self.sides
            && sides.get(&envelope.from) != sides.get(&envelope.to)
        {
            self.report.dropped += 1;
            return;
        }
        if self.faults && self.rng.chance(self.config.drop_ppm) {
            self.report.dropped += 1;
            return;
        }
        if self.faults && self.rng.chance(self.config.duplicate_ppm) {
            self.report.duplicated += 1;
            let delay = 1 + self.rng.below(self.config.max_delay);
            self.schedule(delay, Event::Deliver(envelope.clone()));
        }
        let delay = 1 + self.rng.below(self.config.max_delay);
        self.schedule(delay, Event::Deliver(envelope));
    }

    fn inject(&mut self) {
        if self.rng.chance(self.config.crash_ppm) {
            let node = self.members[self.rng.below(self.members.len() as u64) as usize];
            if self.nodes.remove(&node).is_some() {
                // Gone: everything not persisted. Its ticks stop; it comes back later.
                self.report.crashes += 1;
                let incarnation = self.incarnations[&node] + 1;
                self.incarnations.insert(node, incarnation);
                let delay = 20 + self.rng.below(400);
                self.schedule(delay, Event::Restart { node });
            }
        }
        if self.sides.is_none() && self.rng.chance(self.config.partition_ppm) {
            self.report.partitions += 1;
            let sides = self
                .members
                .iter()
                .map(|id| (*id, self.rng.below(2) == 0))
                .collect();
            self.sides = Some(sides);
            let delay = 50 + self.rng.below(800);
            self.schedule(delay, Event::Heal);
        }
        if self.rng.chance(self.config.pause_ppm) {
            let node = self.members[self.rng.below(self.members.len() as u64) as usize];
            self.report.pauses += 1;
            let until = self.now + 30 + self.rng.below(300);
            self.paused_until.insert(node, until);
        }
    }

    /// The invariants that hold after every event.
    fn check(&self) -> Result<(), String> {
        // Log matching: two logs that share an entry agree up to it, where both still hold it.
        let logs: Vec<(NodeId, &[Entry])> = self
            .nodes
            .iter()
            .map(|(id, raft)| (*id, raft.log()))
            .collect();
        for (i, (a, left)) in logs.iter().enumerate() {
            let by_index: BTreeMap<u64, &Entry> = left.iter().map(|e| (e.index, e)).collect();
            for (b, right) in &logs[i + 1..] {
                let shared: Vec<(&Entry, &Entry)> = right
                    .iter()
                    .filter_map(|r| by_index.get(&r.index).map(|l| (*l, r)))
                    .collect();
                if let Some(last) = shared.iter().rposition(|(l, r)| l.term == r.term)
                    && shared[..=last].iter().any(|(l, r)| l != r)
                {
                    return Err(format!(
                        "log matching: nodes {a} and {b} share index {} and term {} but differ before it",
                        shared[last].0.index, shared[last].0.term
                    ));
                }
            }
        }
        // Durability: what a node applied is on its disk, in its snapshot or its log.
        for (node, state) in &self.states {
            if state.index == 0 || !self.nodes.contains_key(node) {
                continue;
            }
            let disk = &self.disks[node];
            let in_snapshot = disk
                .snapshot
                .as_ref()
                .is_some_and(|s| s.index >= state.index);
            let in_log = disk.log.iter().any(|e| e.index == state.index);
            if !in_snapshot && !in_log {
                return Err(format!(
                    "durability: node {node} applied index {} that its disk does not hold",
                    state.index
                ));
            }
        }
        Ok(())
    }

    /// Every node's state, for a failure's message.
    fn summary(&self) -> String {
        let mut out = String::new();
        for id in &self.members {
            match self.nodes.get(id) {
                None => out.push_str(&format!("  node {id}: down\n")),
                Some(raft) => out.push_str(&format!(
                    "  node {id}: {:?} term {} leader {:?} group {:?} commit {} snapshot {} last {} applied {}\n",
                    raft.role(),
                    raft.term(),
                    raft.leader(),
                    raft.members(),
                    raft.commit(),
                    raft.snapshot_index(),
                    raft.log().last().map_or(raft.snapshot_index(), |e| e.index),
                    self.states.get(id).map_or(0, |s| s.index),
                )),
            }
        }
        out
    }

    /// The group's leader, if a member leads.
    fn leader(&self) -> Option<&RaftNode> {
        self.nodes
            .values()
            .find(|n| n.role() == Role::Leader && n.members().contains(&n.id()))
    }

    fn done(&self) -> bool {
        if !self.pending.is_empty() || self.next_proposal < self.config.proposals as u64 {
            return false;
        }
        // Every member is up and has applied everything committed, and the leader's no-op of its term.
        // Nodes outside the group are not waited for.
        let Some(leader) = self.leader() else {
            return false;
        };
        let commit = leader.commit();
        commit
            == leader
                .log()
                .last()
                .map_or(leader.snapshot_index(), |e| e.index)
            && leader.members().iter().all(|m| {
                self.nodes.contains_key(m)
                    && self
                        .states
                        .get(m)
                        .is_some_and(|state| state.index == commit)
            })
    }

    fn final_checks(&mut self) -> Result<(), String> {
        let group = self
            .leader()
            .map(|l| l.members().to_vec())
            .unwrap_or_default();
        let reference = group
            .first()
            .and_then(|m| self.states.get(m))
            .copied()
            .unwrap_or_default();
        for (node, state) in self.states.iter().filter(|(n, _)| group.contains(n)) {
            if *state != reference {
                return Err(format!("node {node}'s state machine differs at the end"));
            }
        }
        for (value, index) in &self.acked {
            let entry = self.applied_at.get(index);
            if *index > reference.index
                || entry.map(|e| e.data.as_slice()) != Some(value.to_be_bytes().as_slice())
            {
                return Err(format!(
                    "proposal {value}, seen applied at {index}, was lost"
                ));
            }
        }
        self.report.applied = reference.index;
        self.report.changes = self
            .applied_at
            .values()
            .filter(|e| e.members.is_some())
            .count() as u64;
        Ok(())
    }
}

/// What a node is started with: the initial group if it is in it, otherwise nothing, until a leader's
/// log adds it.
fn bootstrap(node: NodeId, initial: &[NodeId]) -> Vec<NodeId> {
    if initial.contains(&node) {
        initial.to_vec()
    } else {
        Vec::new()
    }
}
