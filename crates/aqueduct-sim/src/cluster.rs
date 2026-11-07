//! A replicated partition under faults (ADR 0051, step 4): replicas of one partition, each a
//! [`Replica`] (a Raft node and, on the leader, the partition), with a client starting runs and a worker
//! running their steps, both talking to whichever replica they reach and retrying.
//!
//! Faults: replicas crash (losing everything their driver did not persist) and restart; the network
//! between them, and to the client and the worker, delays, reorders, drops and duplicates messages,
//! and splits; clocks pause and run at different rates. Then faults heal, and the cluster must finish
//! every run.
//!
//! Checked after every event:
//!
//! - every replica's committed records are a prefix of one sequence: replicas never disagree;
//! - the leader's partition is the fold of its journals.
//!
//! And at the end:
//!
//! - every acknowledged start is a run, and every acknowledged step result is in its journal, once;
//! - every run ends as the sequential reference says;
//! - the leader's partition equals a partition rebuilt from the committed records: nothing it acted on
//!   is missing from what a majority holds;
//! - every replica has applied the same records.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use aqueduct_core::{CommandIndex, Entry as Journal, Payload, Status, StepOutcome};
use aqueduct_engine::replica::{Replica, ReplicaPartition, Setup};
use aqueduct_engine::{Deciders, Outbound, PartitionRun, StartOptions};
use aqueduct_raft::{Config as RaftConfig, Entry, Envelope, HardState, NodeId, Snapshot};

use crate::rng::Rng;
use crate::workflow::{self, Workload};
use crate::{Config, Failure, Report};

/// What a replica's disk holds: what its Raft node asked to persist.
#[derive(Clone, Debug, Default)]
struct Disk {
    hard_state: HardState,
    snapshot: Option<Snapshot>,
    log: Vec<Entry>,
}

#[derive(Clone, Debug)]
enum Event {
    Tick {
        replica: NodeId,
        incarnation: u64,
    },
    Raft(Envelope),
    Restart {
        replica: NodeId,
    },
    Heal,
    /// The client retries what is not acknowledged.
    ClientRetry,
    /// The worker sends again the results not acknowledged.
    WorkerResend,
    Start {
        replica: NodeId,
        key: String,
        input: Payload,
    },
    StartAck {
        key: String,
    },
    Dispatch {
        key: String,
        cmd: CommandIndex,
        name: String,
        input: Payload,
    },
    Result {
        replica: NodeId,
        key: String,
        cmd: CommandIndex,
        outcome: StepOutcome,
    },
    ResultAck {
        key: String,
        cmd: CommandIndex,
    },
}

struct World<'c> {
    config: &'c Config,
    seed: u64,
    rng: Rng,
    now: u64,
    seq: u64,
    queue: BTreeMap<(u64, u64), Event>,
    workload: Workload,
    deciders: Deciders,
    setup: Setup,
    ids: Vec<NodeId>,
    replicas: BTreeMap<NodeId, Replica>,
    disks: BTreeMap<NodeId, Disk>,
    incarnations: BTreeMap<NodeId, u64>,
    tick_ms: BTreeMap<NodeId, u64>,
    ticks: BTreeMap<NodeId, u64>,
    paused_until: BTreeMap<NodeId, u64>,
    sides: Option<BTreeMap<NodeId, bool>>,
    /// The longest committed record sequence any replica has applied.
    committed: Vec<Vec<u8>>,
    wanted: Vec<(String, Payload)>,
    acked_starts: BTreeSet<String>,
    /// Results the worker has, not yet acknowledged.
    results: BTreeMap<(String, CommandIndex), StepOutcome>,
    acked_results: BTreeSet<(String, CommandIndex)>,
    executions: BTreeMap<(String, CommandIndex), u64>,
    builds: u64,
    faults: bool,
    report: Report,
}

/// The node that stands for "the client" and "the worker" on the network: on no side of a split.
const OUTSIDE: NodeId = 0;

/// Runs one replicated world.
pub(crate) fn run(seed: u64, config: &Config) -> Result<Report, Failure> {
    let fail = |time, message| Failure {
        seed,
        workload: config.workload,
        time,
        message: format!(
            "{message}\n(a replicated world: add --replicas {})",
            config.replicas
        ),
    };
    let workload = Workload::new(config.workload).map_err(|m| fail(0, m))?;
    if workload.signals || workload.escalations || workload.updates || !workload.others.is_empty() {
        return Err(fail(
            0,
            "the replicated world runs workloads without signals, escalations, updates, children or objects yet"
                .into(),
        ));
    }
    let mut rng = Rng::new(seed);
    let mut deciders = Deciders::new();
    deciders.insert_version(
        crate::node::WORKFLOW,
        crate::node::VERSION,
        workload.decider.clone(),
    );
    deciders
        .activate(crate::node::WORKFLOW, crate::node::VERSION)
        .map_err(|e| fail(0, e.to_string()))?;
    let setup: Setup = Arc::new(|partition: &mut ReplicaPartition, term: u64| {
        partition.set_default_retry_policy(crate::node::SIM_RETRY);
        partition.set_retry_policy("classify", crate::node::SIM_AGENT_RETRY);
        partition.set_seed(crate::node::SIM_SEED ^ term);
    });
    let ids: Vec<NodeId> = (1..=config.replicas.max(1) as u64).collect();
    let mut replicas = BTreeMap::new();
    let mut tick_ms = BTreeMap::new();
    for id in &ids {
        let replica = Replica::new(
            *id,
            &ids,
            RaftConfig::default(),
            rng.next_u64(),
            deciders.clone(),
            setup.clone(),
        );
        replicas.insert(*id, replica);
        tick_ms.insert(*id, 10 + rng.below(6));
    }
    let wanted = (0..config.runs)
        .map(|i| (format!("run-{i}"), workload.input(&mut rng)))
        .collect();
    let mut world = World {
        config,
        seed,
        rng,
        now: 0,
        seq: 0,
        queue: BTreeMap::new(),
        workload,
        deciders,
        setup,
        disks: ids.iter().map(|id| (*id, Disk::default())).collect(),
        incarnations: ids.iter().map(|id| (*id, 0)).collect(),
        ticks: ids.iter().map(|id| (*id, 0)).collect(),
        ids,
        replicas,
        tick_ms,
        paused_until: BTreeMap::new(),
        sides: None,
        committed: Vec::new(),
        wanted,
        acked_starts: BTreeSet::new(),
        results: BTreeMap::new(),
        acked_results: BTreeSet::new(),
        executions: BTreeMap::new(),
        builds: 0,
        faults: true,
        report: Report {
            seed,
            ..Report::default()
        },
    };
    world.run().map_err(|message| fail(world.now, message))
}

impl World<'_> {
    fn schedule(&mut self, delay: u64, event: Event) {
        self.queue.insert((self.now + delay, self.seq), event);
        self.seq += 1;
    }

    fn pick(&mut self) -> NodeId {
        self.ids[self.rng.below(self.ids.len() as u64) as usize]
    }

    fn run(&mut self) -> Result<Report, String> {
        for id in self.ids.clone() {
            let delay = self.rng.below(self.tick_ms[&id]);
            self.schedule(
                delay,
                Event::Tick {
                    replica: id,
                    incarnation: 0,
                },
            );
        }
        self.schedule(1, Event::ClientRetry);
        self.schedule(1, Event::WorkerResend);
        loop {
            let Some(((time, _), event)) = self.queue.pop_first() else {
                return Err("the event queue ran dry".into());
            };
            self.now = time;
            self.report.events += 1;
            if self.report.events > 3_000_000 {
                return Err(format!(
                    "liveness: {} events by time {}, and not done\n{}",
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
                    "liveness: not done by {}: {} starts and {} results unacknowledged, seed {}\n{}",
                    self.config.max_time,
                    self.wanted.len() - self.acked_starts.len(),
                    self.results.len(),
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
                self.report.failovers = self.builds;
                self.report.step_executions = self.executions.values().sum();
                self.report.duplicate_executions =
                    self.report.step_executions - self.executions.len() as u64;
                return Ok(std::mem::take(&mut self.report));
            }
        }
    }

    fn summary(&self) -> String {
        let mut out = String::new();
        for id in &self.ids {
            match self.replicas.get(id) {
                None => out.push_str(&format!("  replica {id}: down\n")),
                Some(replica) => out.push_str(&format!(
                    "  replica {id}: {:?} term {} commit {} records {} built {}\n",
                    replica.raft().role(),
                    replica.raft().term(),
                    replica.raft().commit(),
                    replica.committed().len(),
                    replica.partition().is_some()
                )),
            }
        }
        out
    }

    fn handle(&mut self, event: Event) -> Result<(), String> {
        match event {
            Event::Tick {
                replica,
                incarnation,
            } => {
                if self.incarnations.get(&replica) != Some(&incarnation) {
                    return Ok(());
                }
                let paused = self
                    .paused_until
                    .get(&replica)
                    .is_some_and(|u| *u > self.now);
                if !paused {
                    self.housekeeping(replica)?;
                }
                let delay = self.tick_ms[&replica];
                self.schedule(
                    delay,
                    Event::Tick {
                        replica,
                        incarnation,
                    },
                );
            }
            Event::Raft(envelope) => {
                let to = envelope.to;
                if let Some(until) = self.paused_until.get(&to).copied()
                    && until > self.now
                {
                    let wait = until - self.now;
                    self.schedule(wait, Event::Raft(envelope));
                    return Ok(());
                }
                if let Some(replica) = self.replicas.get_mut(&to) {
                    replica.step(envelope.from, envelope.message);
                    self.drive(to)?;
                }
            }
            Event::Restart { replica } => self.restart(replica)?,
            Event::Heal => self.sides = None,
            Event::ClientRetry => {
                let missing: Vec<(String, Payload)> = self
                    .wanted
                    .iter()
                    .filter(|(key, _)| !self.acked_starts.contains(key))
                    .cloned()
                    .collect();
                let more = !missing.is_empty();
                for (key, input) in missing {
                    let replica = self.pick();
                    self.send(
                        OUTSIDE,
                        replica,
                        Event::Start {
                            replica,
                            key,
                            input,
                        },
                    );
                }
                if more {
                    let delay = 20 + self.rng.below(40);
                    self.schedule(delay, Event::ClientRetry);
                }
            }
            Event::WorkerResend => {
                let unacked: Vec<((String, CommandIndex), StepOutcome)> = self
                    .results
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                for ((key, cmd), outcome) in unacked {
                    let replica = self.pick();
                    self.send(
                        OUTSIDE,
                        replica,
                        Event::Result {
                            replica,
                            key,
                            cmd,
                            outcome,
                        },
                    );
                }
                let delay = 30 + self.rng.below(40);
                self.schedule(delay, Event::WorkerResend);
            }
            Event::Start {
                replica,
                key,
                input,
            } => {
                let options = StartOptions {
                    budget_tokens: workflow::run_budget(&input),
                    ..StartOptions::default()
                };
                if let Some(node) = self.replicas.get_mut(&replica)
                    && let Ok(partition) = node.partition_mut()
                {
                    partition
                        .start_with(&key, crate::node::WORKFLOW, input, options)
                        .map_err(|e| e.to_string())?;
                    node.flush()?;
                    self.drive(replica)?;
                }
            }
            Event::StartAck { key } => {
                self.acked_starts.insert(key);
            }
            Event::Dispatch {
                key,
                cmd,
                name,
                input,
            } => {
                // The worker runs each dispatch it gets: at least once.
                *self.executions.entry((key.clone(), cmd)).or_default() += 1;
                let outcome = (self.workload.execute)(&name, &input);
                if !self.acked_results.contains(&(key.clone(), cmd)) {
                    self.results.insert((key.clone(), cmd), outcome.clone());
                }
                let replica = self.pick();
                self.send(
                    OUTSIDE,
                    replica,
                    Event::Result {
                        replica,
                        key,
                        cmd,
                        outcome,
                    },
                );
            }
            Event::Result {
                replica,
                key,
                cmd,
                outcome,
            } => {
                if let Some(node) = self.replicas.get_mut(&replica)
                    && let Ok(partition) = node.partition_mut()
                {
                    partition
                        .step_result(&key, cmd, outcome)
                        .map_err(|e| e.to_string())?;
                    node.flush()?;
                    self.drive(replica)?;
                }
            }
            Event::ResultAck { key, cmd } => {
                self.results.remove(&(key.clone(), cmd));
                self.acked_results.insert((key, cmd));
            }
        }
        Ok(())
    }

    /// A replica's clock ticked: its Raft node ticks, and a leader's partition fires timers and
    /// re-dispatches what may have been lost.
    fn housekeeping(&mut self, id: NodeId) -> Result<(), String> {
        let now = self.now;
        let Some(replica) = self.replicas.get_mut(&id) else {
            return Ok(());
        };
        replica.tick();
        let tick = {
            let count = self.ticks.entry(id).or_default();
            *count += 1;
            *count
        };
        if let Ok(partition) = replica.partition_mut() {
            partition.set_time(now);
            partition.fire_due_timers().map_err(|e| e.to_string())?;
            if tick % 8 == 0 {
                partition.redispatch();
            }
            replica.flush()?;
        }
        self.drive(id)
    }

    /// Does what the replica's readies ask: persists, sends, then delivers what it released.
    fn drive(&mut self, id: NodeId) -> Result<(), String> {
        for _ in 0..10_000 {
            let Some(replica) = self.replicas.get_mut(&id) else {
                return Ok(());
            };
            if !replica.has_ready() {
                return Ok(());
            }
            let built_before = replica.partition().is_some();
            let ready = replica.ready()?;
            let disk = self.disks.entry(id).or_default();
            if let Some(snapshot) = &ready.raft.snapshot {
                disk.log.retain(|e| e.index > snapshot.index);
                disk.snapshot = Some(snapshot.clone());
            }
            if let Some(hard_state) = ready.raft.hard_state {
                disk.hard_state = hard_state;
            }
            if let Some(from) = ready.raft.truncate_from {
                disk.log.retain(|e| e.index < from);
            }
            disk.log.extend(ready.raft.entries.iter().cloned());
            for envelope in ready.raft.messages {
                let (from, to) = (envelope.from, envelope.to);
                self.send(from, to, Event::Raft(envelope));
            }
            for message in ready.released {
                self.release(message);
            }
            let replica = self.replicas.get_mut(&id).ok_or("a replica vanished")?;
            replica.advance()?;
            if !built_before && replica.partition().is_some() {
                self.builds += 1;
            }
            let records = replica.committed().to_vec();
            self.agree(id, &records)?;
            if self.rng.chance(50_000)
                && let Some(replica) = self.replicas.get_mut(&id)
            {
                // The driver persists the snapshot before the node drops the log it replaces.
                replica.compact();
                if let Some(snapshot) = replica.raft().snapshot().cloned() {
                    let disk = self.disks.entry(id).or_default();
                    disk.log.retain(|e| e.index > snapshot.index);
                    disk.snapshot = Some(snapshot);
                }
            }
        }
        Err(format!(
            "replica {id} kept asking for more: 10,000 readies for one event"
        ))
    }

    /// A message the leader released, now that what it depends on is committed.
    fn release(&mut self, message: Outbound) {
        match message {
            Outbound::Started { key } => self.send(OUTSIDE, OUTSIDE, Event::StartAck { key }),
            Outbound::Dispatch {
                key,
                cmd,
                name,
                input,
            } => {
                self.send(
                    OUTSIDE,
                    OUTSIDE,
                    Event::Dispatch {
                        key,
                        cmd,
                        name,
                        input,
                    },
                );
            }
            Outbound::ResultRecorded { key, cmd } | Outbound::AttemptRecorded { key, cmd } => {
                self.send(OUTSIDE, OUTSIDE, Event::ResultAck { key, cmd });
            }
            _ => {}
        }
    }

    /// Replicas never disagree on what is committed: each one's records extend, or are extended by,
    /// the longest any has applied.
    fn agree(&mut self, id: NodeId, records: &[Vec<u8>]) -> Result<(), String> {
        let shared = records.len().min(self.committed.len());
        if records[..shared] != self.committed[..shared] {
            let at = (0..shared)
                .find(|i| records[*i] != self.committed[*i])
                .unwrap_or(0);
            return Err(format!(
                "replica {id} committed a different record at position {at} than another replica"
            ));
        }
        if records.len() > self.committed.len() {
            self.committed = records.to_vec();
        }
        Ok(())
    }

    fn send(&mut self, from: NodeId, to: NodeId, event: Event) {
        if let Some(sides) = &self.sides
            && from != OUTSIDE
            && to != OUTSIDE
            && sides.get(&from) != sides.get(&to)
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
            self.schedule(delay, event.clone());
        }
        let delay = 1 + self.rng.below(self.config.max_delay);
        self.schedule(delay, event);
    }

    fn restart(&mut self, id: NodeId) -> Result<(), String> {
        let disk = self.disks[&id].clone();
        let seed = self.rng.next_u64();
        let replica = Replica::restart(
            id,
            &self.ids,
            RaftConfig::default(),
            seed,
            disk.hard_state,
            disk.snapshot,
            disk.log,
            self.deciders.clone(),
            self.setup.clone(),
        )?;
        self.replicas.insert(id, replica);
        let incarnation = self.incarnations[&id] + 1;
        self.incarnations.insert(id, incarnation);
        self.schedule(
            1,
            Event::Tick {
                replica: id,
                incarnation,
            },
        );
        Ok(())
    }

    fn inject(&mut self) {
        if self.rng.chance(self.config.crash_ppm) {
            let id = self.pick();
            if self.replicas.remove(&id).is_some() {
                self.report.crashes += 1;
                let incarnation = self.incarnations[&id] + 1;
                self.incarnations.insert(id, incarnation);
                let delay = 20 + self.rng.below(400);
                self.schedule(delay, Event::Restart { replica: id });
            }
        }
        if self.sides.is_none() && self.rng.chance(self.config.crash_ppm / 3) {
            let sides = self
                .ids
                .iter()
                .map(|id| (*id, self.rng.below(2) == 0))
                .collect();
            self.sides = Some(sides);
            let delay = 50 + self.rng.below(600);
            self.schedule(delay, Event::Heal);
        }
        if self.rng.chance(self.config.crash_ppm / 3) {
            let id = self.pick();
            let until = self.now + 30 + self.rng.below(200);
            self.paused_until.insert(id, until);
        }
    }

    fn leader(&self) -> Option<&Replica> {
        self.replicas.values().find(|r| r.partition().is_some())
    }

    fn check(&self) -> Result<(), String> {
        for replica in self.replicas.values() {
            let Some(partition) = replica.partition() else {
                continue;
            };
            partition.check_fold()?;
            // A deposed leader cut off from the group may not know yet, and holds what it held.
            // Nor does one whose successor has crashed since: terms persisted count too.
            let newest = self
                .replicas
                .values()
                .map(|r| r.raft().term())
                .chain(self.disks.values().map(|d| d.hard_state.term))
                .max()
                .unwrap_or(0);
            if replica.raft().term() < newest {
                continue;
            }
            // Whoever leads holds everything acknowledged: a start, or a result the worker stopped
            // resending (unless its run ended without it).
            for key in &self.acked_starts {
                if Self::latest(partition, key).is_none() {
                    return Err(format!(
                        "the leader in term {} lost the acknowledged start of {key}",
                        replica.raft().term()
                    ));
                }
            }
            for (key, cmd) in &self.acked_results {
                let index = usize::try_from(cmd.0).map_err(|e| e.to_string())?;
                let kept = partition.runs().get(key).is_some_and(|run| {
                    run.state().status().is_terminal()
                        || run
                            .state()
                            .steps()
                            .get(index)
                            .is_some_and(|s| s.outcome.is_some())
                });
                if !kept {
                    return Err(format!(
                        "the leader in term {} lost the acknowledged result of {key:?} command {cmd}",
                        replica.raft().term()
                    ));
                }
            }
        }
        Ok(())
    }

    /// The latest run of a key, in a partition: its last generation.
    fn latest<'p>(partition: &'p ReplicaPartition, key: &str) -> Option<&'p PartitionRun> {
        let prefix = format!("{key}\0");
        partition
            .runs()
            .range::<str, _>((std::ops::Bound::Included(key), std::ops::Bound::Unbounded))
            .take_while(|(id, _)| id.as_str() == key || id.starts_with(&prefix))
            .max_by_key(|(id, _)| aqueduct_core::split_run_id(id).1)
            .map(|(_, run)| run)
    }

    fn done(&self) -> bool {
        if self.acked_starts.len() < self.wanted.len() || !self.results.is_empty() {
            return false;
        }
        if self.replicas.len() < self.ids.len() {
            return false;
        }
        let Some(leader) = self.leader() else {
            return false;
        };
        let Some(partition) = leader.partition() else {
            return false;
        };
        if partition.needs_flush() {
            return false;
        }
        let commit = leader.raft().commit();
        let last = leader
            .raft()
            .log()
            .last()
            .map_or(leader.raft().snapshot_index(), |e| e.index);
        commit == last
            && self
                .replicas
                .values()
                .all(|r| r.committed().len() == leader.committed().len())
            && self.wanted.iter().all(|(key, _)| {
                Self::latest(partition, key).is_some_and(|run| run.state().status().is_terminal())
            })
    }

    fn final_checks(&mut self) -> Result<(), String> {
        let leader = self.leader().ok_or("no leader at the end")?;
        let partition = leader.partition().ok_or("no partition at the end")?;
        // The leader acted on nothing a majority does not hold.
        let rebuilt = leader.rebuild()?;
        if rebuilt.runs().len() != partition.runs().len() {
            return Err(format!(
                "the leader holds {} runs, the committed records {}",
                partition.runs().len(),
                rebuilt.runs().len()
            ));
        }
        for (key, run) in partition.runs() {
            let Some(durable) = rebuilt.runs().get(key) else {
                return Err(format!("run {key} is not in the committed records"));
            };
            if durable.journal() != run.journal() {
                return Err(format!(
                    "run {key}: the leader's journal differs from the committed one"
                ));
            }
        }
        for key in &self.acked_starts {
            if Self::latest(partition, key).is_none() {
                return Err(format!(
                    "start of {key} was acknowledged, but there is no run"
                ));
            }
        }
        // An acknowledged result is recorded, once, unless its run ended without it (a race's loser).
        // A dispatch names the run id: the key with its generation.
        for (key, cmd) in &self.acked_results {
            let run = partition
                .runs()
                .get(key)
                .ok_or_else(|| format!("lost run {key:?} with acknowledged results"))?;
            let index = usize::try_from(cmd.0).map_err(|e| e.to_string())?;
            let recorded = run
                .state()
                .steps()
                .get(index)
                .is_some_and(|s| s.outcome.is_some());
            if !recorded && !run.state().status().is_terminal() {
                return Err(format!("lost acknowledged result of {key:?} command {cmd}"));
            }
        }
        for (key, run) in partition.runs() {
            let mut seen = BTreeSet::new();
            for entry in run.journal() {
                if let Journal::StepCompleted { cmd, .. } | Journal::StepFailed { cmd, .. } = entry
                    && !seen.insert(*cmd)
                {
                    return Err(format!("run {key:?} recorded command {cmd} twice"));
                }
            }
        }
        let (mut completed, mut failed) = (0, 0);
        for (key, input) in &self.wanted {
            let run = Self::latest(partition, key).ok_or_else(|| format!("run {key} missing"))?;
            let state = run.state();
            if state.status() == Status::Parked {
                return Err(format!(
                    "run {key} was parked: {}",
                    state.error().unwrap_or_default()
                ));
            }
            let actual = (
                state.status(),
                state.output().cloned(),
                state.error().map(str::to_owned),
            );
            let expected = workflow::reference(&self.workload, input)?;
            if actual != expected {
                return Err(format!(
                    "run {key} ended as {actual:?}, the reference says {expected:?}"
                ));
            }
            match state.status() {
                Status::Completed => completed += 1,
                _ => failed += 1,
            }
        }
        self.report.completed = completed;
        self.report.failed = failed;
        Ok(())
    }
}
