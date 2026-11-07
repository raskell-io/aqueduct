//! A cluster under faults (ADR 0051, 0052): partitions, each a Raft group of replicas
//! ([`Replica`]: a Raft node and, on the leader, the partition), on a few machines. Machine `m` holds
//! replica `m` of every partition. A client starts runs and a worker runs their steps, both talking to
//! whichever machine they reach and retrying; partitions send each other children and messages to
//! objects (ADR 0052).
//!
//! Faults: machines crash (their replicas lose everything their driver did not persist) and restart; the
//! network delays, reorders, drops and duplicates messages, and splits the machines; machines pause, and
//! their clocks run at different rates. Then faults heal, and the cluster must finish every run.
//!
//! Checked after every event:
//!
//! - in each partition, every replica's committed records are a prefix of one sequence: replicas never
//!   disagree;
//! - every leader's partition is the fold of its journals;
//! - the leader of a partition's newest term holds every acknowledged start and result of its keys.
//!
//! And at the end:
//!
//! - every acknowledged step result is recorded once (or its run ended without it);
//! - every run ends as the sequential reference says, on the partition of its home;
//! - every message reached its object exactly once;
//! - each leader's partition equals one rebuilt from its group's committed records: nothing it acted on is
//!   missing from what a majority holds;
//! - every replica of a partition has applied the same records.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use aqueduct_core::{CommandIndex, Entry as Journal, Payload, Status, StepOutcome};
use aqueduct_engine::placement::SlotTable;
use aqueduct_engine::replica::{Replica, ReplicaPartition, Setup};
use aqueduct_engine::{Deciders, Outbound, PartitionRun, Remote, StartOptions};
use aqueduct_raft::{Config as RaftConfig, Entry, Envelope, HardState, NodeId, Snapshot};

use crate::rng::Rng;
use crate::workflow::{self, Workload};
use crate::{Config, Failure, Report};

/// A partition's index.
type Group = u32;

/// A replica: (partition, machine).
type Actor = (Group, NodeId);

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
        machine: NodeId,
        incarnation: u64,
    },
    Raft {
        group: Group,
        envelope: Envelope,
    },
    Restart {
        machine: NodeId,
    },
    Heal,
    /// The client retries what is not acknowledged.
    ClientRetry,
    /// The worker sends again the results not acknowledged.
    WorkerResend,
    Start {
        machine: NodeId,
        key: String,
        input: Payload,
    },
    StartAck {
        key: String,
    },
    Dispatch {
        group: Group,
        key: String,
        cmd: CommandIndex,
        name: String,
        input: Payload,
    },
    Result {
        machine: NodeId,
        group: Group,
        key: String,
        cmd: CommandIndex,
        outcome: StepOutcome,
    },
    ResultAck {
        key: String,
        cmd: CommandIndex,
    },
    /// A message from one partition to another.
    Remote {
        machine: NodeId,
        group: Group,
        message: Remote,
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
    table: SlotTable,
    groups: Vec<Group>,
    setups: Vec<Setup>,
    machines: Vec<NodeId>,
    replicas: BTreeMap<Actor, Replica>,
    disks: BTreeMap<Actor, Disk>,
    incarnations: BTreeMap<NodeId, u64>,
    tick_ms: BTreeMap<NodeId, u64>,
    ticks: BTreeMap<NodeId, u64>,
    paused_until: BTreeMap<NodeId, u64>,
    sides: Option<BTreeMap<NodeId, bool>>,
    /// Per partition: the longest committed record sequence any of its replicas has applied.
    committed: BTreeMap<Group, Vec<Vec<u8>>>,
    wanted: Vec<(String, Payload)>,
    /// Messages each object key must receive: (object, key) and how many.
    sends: BTreeMap<(String, String), usize>,
    acked_starts: BTreeSet<String>,
    /// Results the worker has, not yet acknowledged, with the partition that dispatched them.
    results: BTreeMap<(String, CommandIndex), (Group, StepOutcome)>,
    acked_results: BTreeSet<(String, CommandIndex)>,
    executions: BTreeMap<(String, CommandIndex), u64>,
    builds: u64,
    faults: bool,
    report: Report,
}

/// The node that stands for "the client" and "the worker" on the network: on no side of a split.
const OUTSIDE: NodeId = 0;

/// Runs one cluster world.
pub(crate) fn run(seed: u64, config: &Config) -> Result<Report, Failure> {
    let fail = |time, message| Failure {
        seed,
        workload: config.workload,
        time,
        message: format!(
            "{message}\n(a cluster world: add --replicas {} --partitions {})",
            config.replicas,
            config.partitions.max(1)
        ),
    };
    let workload = Workload::new(config.workload).map_err(|m| fail(0, m))?;
    if workload.signals || workload.escalations || workload.updates {
        return Err(fail(
            0,
            "the cluster world runs workloads without signals, escalations or updates yet".into(),
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
    for (name, decider) in &workload.others {
        deciders.insert_version(name.clone(), crate::node::VERSION, Arc::clone(decider));
        deciders
            .activate(name, crate::node::VERSION)
            .map_err(|e| fail(0, e.to_string()))?;
    }
    for object in &workload.objects {
        deciders.insert(
            aqueduct_engine::objects::loop_workflow(object),
            Arc::new(aqueduct_engine::objects::ObjectLoop::new(object)),
        );
    }
    let partitions = u32::try_from(config.partitions.max(1)).map_err(|e| fail(0, e.to_string()))?;
    let table = SlotTable::even(partitions);
    let groups: Vec<Group> = (0..partitions).collect();
    let setups: Vec<Setup> = groups
        .iter()
        .map(|group| {
            let placement = table.placement(*group);
            let setup: Setup = Arc::new(move |partition: &mut ReplicaPartition, term: u64| {
                partition.set_default_retry_policy(crate::node::SIM_RETRY);
                partition.set_retry_policy("classify", crate::node::SIM_AGENT_RETRY);
                partition.set_seed(crate::node::SIM_SEED ^ term);
                partition.set_placement(placement.clone());
            });
            setup
        })
        .collect();
    let machines: Vec<NodeId> = (1..=config.replicas.max(1) as u64).collect();
    let mut replicas = BTreeMap::new();
    let mut tick_ms = BTreeMap::new();
    for machine in &machines {
        for group in &groups {
            let replica = Replica::new(
                *machine,
                &machines,
                RaftConfig::default(),
                rng.next_u64(),
                deciders.clone(),
                setups[*group as usize].clone(),
            );
            replicas.insert((*group, *machine), replica);
        }
        tick_ms.insert(*machine, 10 + rng.below(6));
    }
    let wanted: Vec<(String, Payload)> = (0..config.runs)
        .map(|i| (format!("run-{i}"), workload.input(&mut rng)))
        .collect();
    let mut sends = BTreeMap::new();
    for (_, input) in &wanted {
        let (_, sent) = workflow::reference_with_sends(&workload, input).map_err(|m| fail(0, m))?;
        for target in sent {
            *sends.entry(target).or_insert(0) += 1;
        }
    }
    let actors: Vec<Actor> = replicas.keys().copied().collect();
    let mut world = World {
        config,
        seed,
        rng,
        now: 0,
        seq: 0,
        queue: BTreeMap::new(),
        workload,
        deciders,
        table,
        setups,
        disks: actors.iter().map(|a| (*a, Disk::default())).collect(),
        committed: groups.iter().map(|g| (*g, Vec::new())).collect(),
        groups,
        incarnations: machines.iter().map(|m| (*m, 0)).collect(),
        ticks: machines.iter().map(|m| (*m, 0)).collect(),
        machines,
        replicas,
        tick_ms,
        paused_until: BTreeMap::new(),
        sides: None,
        wanted,
        sends,
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

/// The partition a run lives on: an object's calls live with its loop (the simulator's object keys have
/// no `/`).
fn group_of(table: &SlotTable, key: &str) -> Group {
    if let Some(rest) = key.strip_prefix("$call/") {
        let mut parts = rest.splitn(3, '/');
        if let (Some(object), Some(object_key)) = (parts.next(), parts.next()) {
            return table.partition_of(&aqueduct_engine::objects::object_run_key(
                object, object_key,
            ));
        }
    }
    table.partition_of(key)
}

impl World<'_> {
    fn schedule(&mut self, delay: u64, event: Event) {
        self.queue.insert((self.now + delay, self.seq), event);
        self.seq += 1;
    }

    fn pick(&mut self) -> NodeId {
        self.machines[self.rng.below(self.machines.len() as u64) as usize]
    }

    fn run(&mut self) -> Result<Report, String> {
        for machine in self.machines.clone() {
            let delay = self.rng.below(self.tick_ms[&machine]);
            self.schedule(
                delay,
                Event::Tick {
                    machine,
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
        for group in &self.groups {
            for machine in &self.machines {
                match self.replicas.get(&(*group, *machine)) {
                    None => out.push_str(&format!("  partition {group} replica {machine}: down\n")),
                    Some(replica) => out.push_str(&format!(
                        "  partition {group} replica {machine}: {:?} term {} commit {} records {} built {}\n",
                        replica.raft().role(),
                        replica.raft().term(),
                        replica.raft().commit(),
                        replica.committed().len(),
                        replica.partition().is_some()
                    )),
                }
            }
        }
        for ((object, key), count) in &self.sends {
            out.push_str(&format!(
                "  {object} {key}: {} of {count} calls done\n",
                self.calls_done(object, key)
            ));
        }
        out
    }

    fn handle(&mut self, event: Event) -> Result<(), String> {
        match event {
            Event::Tick {
                machine,
                incarnation,
            } => {
                if self.incarnations.get(&machine) != Some(&incarnation) {
                    return Ok(());
                }
                let paused = self
                    .paused_until
                    .get(&machine)
                    .is_some_and(|u| *u > self.now);
                if !paused {
                    for group in self.groups.clone() {
                        self.housekeeping((group, machine))?;
                    }
                    *self.ticks.entry(machine).or_default() += 1;
                }
                let delay = self.tick_ms[&machine];
                self.schedule(
                    delay,
                    Event::Tick {
                        machine,
                        incarnation,
                    },
                );
            }
            Event::Raft { group, envelope } => {
                let to = envelope.to;
                if let Some(until) = self.paused_until.get(&to).copied()
                    && until > self.now
                {
                    let wait = until - self.now;
                    self.schedule(wait, Event::Raft { group, envelope });
                    return Ok(());
                }
                if let Some(replica) = self.replicas.get_mut(&(group, to)) {
                    replica.step(envelope.from, envelope.message);
                    self.drive((group, to))?;
                }
            }
            Event::Restart { machine } => self.restart(machine)?,
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
                    let machine = self.pick();
                    self.send(
                        OUTSIDE,
                        machine,
                        Event::Start {
                            machine,
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
                let unacked: Vec<((String, CommandIndex), (Group, StepOutcome))> = self
                    .results
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                for ((key, cmd), (group, outcome)) in unacked {
                    let machine = self.pick();
                    self.send(
                        OUTSIDE,
                        machine,
                        Event::Result {
                            machine,
                            group,
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
                machine,
                key,
                input,
            } => {
                let options = StartOptions {
                    budget_tokens: workflow::run_budget(&input),
                    ..StartOptions::default()
                };
                // The client routes by the slot table; the machine it reaches may not lead.
                let actor = (self.table.partition_of(&key), machine);
                if let Some(node) = self.replicas.get_mut(&actor)
                    && let Ok(partition) = node.partition_mut()
                {
                    partition
                        .start_with(&key, crate::node::WORKFLOW, input, options)
                        .map_err(|e| e.to_string())?;
                    node.flush()?;
                    self.drive(actor)?;
                }
            }
            Event::StartAck { key } => {
                self.acked_starts.insert(key);
            }
            Event::Dispatch {
                group,
                key,
                cmd,
                name,
                input,
            } => {
                // The worker runs each dispatch it gets: at least once.
                *self.executions.entry((key.clone(), cmd)).or_default() += 1;
                let outcome = (self.workload.execute)(&name, &input);
                if !self.acked_results.contains(&(key.clone(), cmd)) {
                    self.results
                        .insert((key.clone(), cmd), (group, outcome.clone()));
                }
                let machine = self.pick();
                self.send(
                    OUTSIDE,
                    machine,
                    Event::Result {
                        machine,
                        group,
                        key,
                        cmd,
                        outcome,
                    },
                );
            }
            Event::Result {
                machine,
                group,
                key,
                cmd,
                outcome,
            } => {
                if let Some(node) = self.replicas.get_mut(&(group, machine))
                    && let Ok(partition) = node.partition_mut()
                {
                    partition
                        .step_result(&key, cmd, outcome)
                        .map_err(|e| e.to_string())?;
                    node.flush()?;
                    self.drive((group, machine))?;
                }
            }
            Event::ResultAck { key, cmd } => {
                self.results.remove(&(key.clone(), cmd));
                self.acked_results.insert((key, cmd));
            }
            Event::Remote {
                machine,
                group,
                message,
            } => {
                if let Some(node) = self.replicas.get_mut(&(group, machine))
                    && let Ok(partition) = node.partition_mut()
                {
                    partition.receive(message).map_err(|e| e.to_string())?;
                    node.flush()?;
                    self.drive((group, machine))?;
                }
            }
        }
        Ok(())
    }

    /// A machine's clock ticked: each of its replicas' Raft nodes ticks, and a leader's partition fires
    /// timers and repeats what may have been lost.
    fn housekeeping(&mut self, actor: Actor) -> Result<(), String> {
        let now = self.now;
        let tick = self.ticks.get(&actor.1).copied().unwrap_or(0) + 1;
        let Some(replica) = self.replicas.get_mut(&actor) else {
            return Ok(());
        };
        replica.tick();
        if let Ok(partition) = replica.partition_mut() {
            partition.set_time(now);
            partition.fire_due_timers().map_err(|e| e.to_string())?;
            if tick % 8 == 0 {
                partition.redispatch();
            }
            replica.flush()?;
        }
        self.drive(actor)
    }

    /// Does what the replica's readies ask: persists, sends, then delivers what it released.
    fn drive(&mut self, actor: Actor) -> Result<(), String> {
        let (group, machine) = actor;
        for _ in 0..10_000 {
            let Some(replica) = self.replicas.get_mut(&actor) else {
                return Ok(());
            };
            if !replica.has_ready() {
                return Ok(());
            }
            let built_before = replica.partition().is_some();
            let ready = replica.ready()?;
            let disk = self.disks.entry(actor).or_default();
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
                self.send(from, to, Event::Raft { group, envelope });
            }
            for message in ready.released {
                self.release(machine, group, message);
            }
            let replica = self.replicas.get_mut(&actor).ok_or("a replica vanished")?;
            replica.advance()?;
            if !built_before && replica.partition().is_some() {
                self.builds += 1;
            }
            let records = replica.committed().to_vec();
            self.agree(actor, &records)?;
            if self.rng.chance(50_000)
                && let Some(replica) = self.replicas.get_mut(&actor)
            {
                // The driver persists the snapshot before the node drops the log it replaces.
                replica.compact();
                if let Some(snapshot) = replica.raft().snapshot().cloned() {
                    let disk = self.disks.entry(actor).or_default();
                    disk.log.retain(|e| e.index > snapshot.index);
                    disk.snapshot = Some(snapshot);
                }
            }
        }
        Err(format!(
            "partition {group} replica {machine} kept asking for more: 10,000 readies for one event"
        ))
    }

    /// A message a leader released, now that what it depends on is committed.
    fn release(&mut self, machine: NodeId, group: Group, message: Outbound) {
        match message {
            Outbound::Started { key } if !key.starts_with('$') => {
                self.send(OUTSIDE, OUTSIDE, Event::StartAck { key });
            }
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
                        group,
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
            Outbound::Remote { to, message } => {
                // Routed by the slot table, to whichever machine the network reaches.
                let target = self.table.partition_of(&to);
                let to = self.pick();
                self.send(
                    machine,
                    to,
                    Event::Remote {
                        machine: to,
                        group: target,
                        message,
                    },
                );
            }
            _ => {}
        }
    }

    /// A partition's replicas never disagree on what is committed: each one's records extend, or are
    /// extended by, the longest any has applied.
    fn agree(&mut self, actor: Actor, records: &[Vec<u8>]) -> Result<(), String> {
        let longest = self.committed.entry(actor.0).or_default();
        let shared = records.len().min(longest.len());
        if records[..shared] != longest[..shared] {
            let at = (0..shared)
                .find(|i| records[*i] != longest[*i])
                .unwrap_or(0);
            return Err(format!(
                "partition {} replica {} committed a different record at position {at} than another replica",
                actor.0, actor.1
            ));
        }
        if records.len() > longest.len() {
            *longest = records.to_vec();
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

    fn restart(&mut self, machine: NodeId) -> Result<(), String> {
        for group in self.groups.clone() {
            let disk = self.disks[&(group, machine)].clone();
            let seed = self.rng.next_u64();
            let replica = Replica::restart(
                machine,
                &self.machines,
                RaftConfig::default(),
                seed,
                disk.hard_state,
                disk.snapshot,
                disk.log,
                self.deciders.clone(),
                self.setups[group as usize].clone(),
            )?;
            self.replicas.insert((group, machine), replica);
        }
        let incarnation = self.incarnations[&machine] + 1;
        self.incarnations.insert(machine, incarnation);
        self.schedule(
            1,
            Event::Tick {
                machine,
                incarnation,
            },
        );
        Ok(())
    }

    fn inject(&mut self) {
        if self.rng.chance(self.config.crash_ppm) {
            let machine = self.pick();
            let mut crashed = false;
            for group in self.groups.clone() {
                crashed |= self.replicas.remove(&(group, machine)).is_some();
            }
            if crashed {
                self.report.crashes += 1;
                let incarnation = self.incarnations[&machine] + 1;
                self.incarnations.insert(machine, incarnation);
                let delay = 20 + self.rng.below(400);
                self.schedule(delay, Event::Restart { machine });
            }
        }
        if self.sides.is_none() && self.rng.chance(self.config.crash_ppm / 3) {
            let sides = self
                .machines
                .iter()
                .map(|m| (*m, self.rng.below(2) == 0))
                .collect();
            self.sides = Some(sides);
            let delay = 50 + self.rng.below(600);
            self.schedule(delay, Event::Heal);
        }
        if self.rng.chance(self.config.crash_ppm / 3) {
            let machine = self.pick();
            let until = self.now + 30 + self.rng.below(200);
            self.paused_until.insert(machine, until);
        }
    }

    /// A partition's leader with its partition built, if it has one now.
    fn leader(&self, group: Group) -> Option<&Replica> {
        self.machines
            .iter()
            .filter_map(|m| self.replicas.get(&(group, *m)))
            .find(|r| r.partition().is_some())
    }

    /// The newest term a partition's replicas know of, persisted or not.
    fn newest_term(&self, group: Group) -> u64 {
        self.machines
            .iter()
            .flat_map(|m| {
                let live = self.replicas.get(&(group, *m)).map(|r| r.raft().term());
                let saved = self.disks.get(&(group, *m)).map(|d| d.hard_state.term);
                live.into_iter().chain(saved)
            })
            .max()
            .unwrap_or(0)
    }

    fn check(&self) -> Result<(), String> {
        for group in &self.groups {
            let newest = self.newest_term(*group);
            for machine in &self.machines {
                let Some(replica) = self.replicas.get(&(*group, *machine)) else {
                    continue;
                };
                let Some(partition) = replica.partition() else {
                    continue;
                };
                partition.check_fold()?;
                // A deposed leader cut off from the group may not know yet, and holds what it held;
                // so may one whose successor has crashed since.
                if replica.raft().term() < newest {
                    continue;
                }
                self.check_leader(*group, replica.raft().term(), partition)?;
            }
        }
        Ok(())
    }

    /// The leader of a partition's newest term holds everything acknowledged for its keys: a start,
    /// or a result the worker stopped resending (unless its run ended without it).
    fn check_leader(
        &self,
        group: Group,
        term: u64,
        partition: &ReplicaPartition,
    ) -> Result<(), String> {
        for key in &self.acked_starts {
            if group_of(&self.table, key) == group && Self::latest(partition, key).is_none() {
                return Err(format!(
                    "the leader of partition {group} in term {term} lost the acknowledged start of {key}"
                ));
            }
        }
        for (key, cmd) in &self.acked_results {
            if group_of(&self.table, key) != group {
                continue;
            }
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
                    "the leader of partition {group} in term {term} lost the acknowledged result of {key:?} command {cmd}"
                ));
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

    /// A key's latest run, on the leader of its partition.
    fn run_of(&self, key: &str) -> Option<&PartitionRun> {
        let partition = self.leader(group_of(&self.table, key))?.partition()?;
        Self::latest(partition, key)
    }

    /// How many calls of an object key's handlers have completed, on its partition's leader.
    fn calls_done(&self, object: &str, key: &str) -> usize {
        let home = aqueduct_engine::objects::object_run_key(object, key);
        let Some(partition) = self
            .leader(self.table.partition_of(&home))
            .and_then(|leader| leader.partition())
        else {
            return 0;
        };
        let prefix = format!("$call/{object}/{key}/");
        partition
            .runs()
            .iter()
            .filter(|(id, run)| {
                id.starts_with(&prefix) && run.state().status() == Status::Completed
            })
            .count()
    }

    fn done(&self) -> bool {
        if self.acked_starts.len() < self.wanted.len() || !self.results.is_empty() {
            return false;
        }
        if self.replicas.len() < self.groups.len() * self.machines.len() {
            return false;
        }
        for group in &self.groups {
            let Some(leader) = self.leader(*group) else {
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
            let applied_everywhere = self.machines.iter().all(|m| {
                self.replicas
                    .get(&(*group, *m))
                    .is_some_and(|r| r.committed().len() == leader.committed().len())
            });
            if commit != last || !applied_everywhere {
                return false;
            }
        }
        self.wanted.iter().all(|(key, _)| {
            self.run_of(key)
                .is_some_and(|run| run.state().status().is_terminal())
        }) && self
            .sends
            .iter()
            .all(|((object, key), count)| self.calls_done(object, key) >= *count)
    }

    fn final_checks(&mut self) -> Result<(), String> {
        for group in self.groups.clone() {
            let leader = self
                .leader(group)
                .ok_or_else(|| format!("partition {group} has no leader at the end"))?;
            let partition = leader.partition().ok_or("no partition at the end")?;
            // The leader acted on nothing a majority does not hold.
            let rebuilt = leader.rebuild()?;
            if rebuilt.runs().len() != partition.runs().len() {
                return Err(format!(
                    "the leader of partition {group} holds {} runs, the committed records {}",
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
                // Every run is where its home is: a shared child too.
                if group_of(&self.table, key) != group {
                    return Err(format!(
                        "run {key:?} is on partition {group}, its home on {}",
                        group_of(&self.table, key)
                    ));
                }
                let mut seen = BTreeSet::new();
                for entry in run.journal() {
                    if let Journal::StepCompleted { cmd, .. } | Journal::StepFailed { cmd, .. } =
                        entry
                        && !seen.insert(*cmd)
                    {
                        return Err(format!("run {key:?} recorded command {cmd} twice"));
                    }
                }
            }
            self.check_leader(group, leader.raft().term(), partition)?;
        }
        // An acknowledged result is recorded, unless its run ended without it (a race's loser).
        for (key, cmd) in &self.acked_results {
            let run = self
                .leader(group_of(&self.table, key))
                .and_then(|leader| leader.partition())
                .and_then(|partition| partition.runs().get(key))
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
        // Every message reached its object exactly once.
        for ((object, key), count) in &self.sends {
            let done = self.calls_done(object, key);
            if done != *count {
                return Err(format!(
                    "{object} {key} ran {done} calls for {count} messages"
                ));
            }
        }
        let (mut completed, mut failed) = (0, 0);
        for (key, input) in &self.wanted {
            let run = self
                .run_of(key)
                .ok_or_else(|| format!("run {key} missing"))?;
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
        self.report.messages = self.sends.values().sum::<usize>() as u64;
        Ok(())
    }
}
