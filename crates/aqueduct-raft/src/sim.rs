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
//! - **durability:** a node's persisted log never loses an entry it applied.
//!
//! And at the end: every proposal the client saw applied is applied, at the same index, on every node;
//! every node has applied the same log; and the client's proposals all got in (liveness, once healed).

use std::collections::{BTreeMap, BTreeSet};

use crate::{Config, Entry, Envelope, HardState, NodeId, RaftNode, Role};

/// Parameters of a simulated world. Probabilities are in parts per million, times in milliseconds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimConfig {
    /// Nodes in the group.
    pub nodes: u64,
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
            proposals: 40,
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
    log: Vec<Entry>,
}

#[derive(Clone, Debug)]
enum Event {
    Tick { node: NodeId, incarnation: u64 },
    Deliver(Envelope),
    Restart { node: NodeId },
    Propose,
    Heal,
}

struct World<'c> {
    config: &'c SimConfig,
    seed: u64,
    rng: Rng,
    now: u64,
    seq: u64,
    queue: BTreeMap<(u64, u64), Event>,
    members: Vec<NodeId>,
    nodes: BTreeMap<NodeId, RaftNode>,
    incarnations: BTreeMap<NodeId, u64>,
    disks: BTreeMap<NodeId, Disk>,
    tick_ms: BTreeMap<NodeId, u64>,
    paused_until: BTreeMap<NodeId, u64>,
    /// While the network is split: the side of each node.
    sides: Option<BTreeMap<NodeId, bool>>,
    /// What each node has applied since it last started.
    applied: BTreeMap<NodeId, Vec<Entry>>,
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
    let raft = Config::default();
    let mut nodes = BTreeMap::new();
    let mut tick_ms = BTreeMap::new();
    for id in &members {
        nodes.insert(*id, RaftNode::new(*id, &members, raft, rng.next()));
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
        incarnations: members.iter().map(|id| (*id, 0)).collect(),
        disks: members.iter().map(|id| (*id, Disk::default())).collect(),
        applied: members.iter().map(|id| (*id, Vec::new())).collect(),
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
        loop {
            let Some(((time, _), event)) = self.queue.pop_first() else {
                return Err("the event queue ran dry".into());
            };
            self.now = time;
            self.report.events += 1;
            if self.faults && self.now >= self.config.fault_window {
                self.faults = false;
                self.sides = None;
                self.paused_until.clear();
            }
            if self.now > self.config.max_time {
                return Err(format!(
                    "liveness: not done by {}: {} proposals pending, leaders by term {:?}, seed {}",
                    self.config.max_time,
                    self.pending.len(),
                    self.leaders.iter().rev().take(3).collect::<Vec<_>>(),
                    self.seed
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
                let raft = RaftNode::restart(
                    node,
                    &self.members,
                    Config::default(),
                    seed,
                    disk.hard_state,
                    disk.log,
                    0,
                );
                self.nodes.insert(node, raft);
                let incarnation = self.incarnations[&node] + 1;
                self.incarnations.insert(node, incarnation);
                self.applied.insert(node, Vec::new());
                self.schedule(1, Event::Tick { node, incarnation });
            }
            Event::Propose => self.propose(),
            Event::Heal => self.sides = None,
        }
        Ok(())
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
        loop {
            let Some(raft) = self.nodes.get_mut(&node) else {
                return Ok(());
            };
            if !raft.has_ready() {
                return Ok(());
            }
            let ready = raft.ready();
            let (role, term) = (raft.role(), raft.term());
            let disk = self.disks.entry(node).or_default();
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
            if let Some((position, entry)) = disk
                .log
                .iter()
                .enumerate()
                .find(|(position, entry)| entry.index != *position as u64 + 1)
            {
                return Err(format!(
                    "node {node}'s disk holds index {} at position {}: a conflicting suffix was kept",
                    entry.index,
                    position + 1
                ));
            }
            if role == Role::Leader {
                self.became_leader(node, term)?;
                self.record_commits(node, term)?;
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
        }
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
        let log = self.nodes[&node].log();
        for (index, (committed_term, entry)) in &self.committed_in {
            if *committed_term >= term {
                continue;
            }
            let held = log.get(usize::try_from(*index - 1).unwrap_or(usize::MAX));
            if held != Some(entry) {
                return Err(format!(
                    "leader completeness: node {node}, leader of term {term}, lacks {entry:?}, committed at {index} in term {committed_term}"
                ));
            }
        }
        Ok(())
    }

    /// Notes, for each index a leader has committed, the first term in which one did.
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
            let Some(entry) = raft
                .log()
                .get(usize::try_from(index - 1).unwrap_or(usize::MAX))
            else {
                return Err(format!(
                    "node {node} committed index {index} it does not hold"
                ));
            };
            self.committed_in.insert(index, (term, entry.clone()));
        }
        Ok(())
    }

    fn apply(&mut self, node: NodeId, entry: Entry) -> Result<(), String> {
        let applied = self.applied.entry(node).or_default();
        let expected = applied.len() as u64 + 1;
        if entry.index != expected {
            return Err(format!(
                "node {node} applied index {} after {}",
                entry.index,
                expected - 1
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
        applied.push(entry);
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
        // Log matching: two logs that share an entry agree up to it.
        let logs: Vec<(NodeId, &[Entry])> = self
            .nodes
            .iter()
            .map(|(id, raft)| (*id, raft.log()))
            .collect();
        for (i, (a, left)) in logs.iter().enumerate() {
            for (b, right) in &logs[i + 1..] {
                let shared = left.len().min(right.len());
                if let Some(last) = (0..shared).rev().find(|k| left[*k].term == right[*k].term)
                    && left[..=last] != right[..=last]
                {
                    return Err(format!(
                        "log matching: nodes {a} and {b} share index {} and term {} but differ before it",
                        last + 1,
                        left[last].term
                    ));
                }
            }
        }
        // Durability: what a node applied is on its disk.
        for (node, applied) in &self.applied {
            let disk = &self.disks[node].log;
            if let Some(last) = applied.last()
                && disk.get(usize::try_from(last.index - 1).unwrap_or(usize::MAX)) != Some(last)
            {
                return Err(format!(
                    "durability: node {node} applied index {} that its disk does not hold",
                    last.index
                ));
            }
        }
        Ok(())
    }

    fn done(&self) -> bool {
        if !self.pending.is_empty() || self.next_proposal < self.config.proposals as u64 {
            return false;
        }
        if self.nodes.len() != self.members.len() {
            return false;
        }
        // Every node has applied everything committed, and the leader's no-op of its term.
        let Some(leader) = self.nodes.values().find(|n| n.role() == Role::Leader) else {
            return false;
        };
        let commit = leader.commit();
        commit == leader.log().len() as u64
            && self
                .applied
                .values()
                .all(|applied| applied.len() as u64 == commit)
    }

    fn final_checks(&mut self) -> Result<(), String> {
        let reference = self.applied.values().next().cloned().unwrap_or_default();
        for (node, applied) in &self.applied {
            if *applied != reference {
                return Err(format!("node {node} applied a different log at the end"));
            }
        }
        for (value, index) in &self.acked {
            let entry = reference.get(usize::try_from(*index - 1).unwrap_or(usize::MAX));
            if entry.map(|e| e.data.as_slice()) != Some(value.to_be_bytes().as_slice()) {
                return Err(format!(
                    "proposal {value}, seen applied at {index}, was lost"
                ));
            }
        }
        self.report.applied = reference.len() as u64;
        Ok(())
    }
}
