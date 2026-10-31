//! The simulated world: one node, a client, a step worker, a lossy network and a seeded event queue.

use std::collections::{BTreeMap, BTreeSet};

use aqueduct_codec::{decode_log_record, verify_chain};
use aqueduct_core::{CommandIndex, Entry, Payload, Status, StepOutcome};
use aqueduct_storage::frame;

use crate::disk::SimDisk;
use aqueduct_engine::Outbound;

use crate::node::Node;
use crate::rng::Rng;
use crate::workflow::{self, Workload};
use crate::{Config, Failure, Report};

#[derive(Clone, Debug)]
enum Ev {
    NodeStart {
        key: String,
        input: Payload,
    },
    NodeStepResult {
        key: String,
        cmd: CommandIndex,
        outcome: StepOutcome,
    },
    ClientStartAck {
        key: String,
    },
    WorkerDispatch {
        key: String,
        cmd: CommandIndex,
        name: String,
        input: Payload,
    },
    WorkerResultAck {
        key: String,
        cmd: CommandIndex,
    },
    Flush {
        incarnation: u64,
    },
    Redispatch {
        incarnation: u64,
    },
    Crash,
    Restart,
    ClientRetry,
    WorkerResend,
}

impl Ev {
    fn code(&self) -> u64 {
        match self {
            Ev::NodeStart { .. } => 1,
            Ev::NodeStepResult { .. } => 2,
            Ev::ClientStartAck { .. } => 3,
            Ev::WorkerDispatch { .. } => 4,
            Ev::WorkerResultAck { .. } => 5,
            Ev::Flush { .. } => 6,
            Ev::Redispatch { .. } => 7,
            Ev::Crash => 8,
            Ev::Restart => 9,
            Ev::ClientRetry => 10,
            Ev::WorkerResend => 11,
        }
    }

    fn is_message(&self) -> bool {
        matches!(
            self,
            Ev::NodeStart { .. }
                | Ev::NodeStepResult { .. }
                | Ev::ClientStartAck { .. }
                | Ev::WorkerDispatch { .. }
                | Ev::WorkerResultAck { .. }
        )
    }
}

struct World<'c> {
    config: &'c Config,
    seed: u64,
    rng: Rng,
    now: u64,
    seq: u64,
    queue: BTreeMap<(u64, u64), Ev>,
    faults: bool,
    node: Node,
    crash_pending: bool,
    pre_crash: BTreeMap<String, Vec<Entry>>,
    /// The client's runs: key and input.
    wanted: Vec<(String, Payload)>,
    /// Starts the client saw acknowledged. Must never be lost.
    acked_starts: BTreeSet<String>,
    /// Results the worker saw acknowledged. Must be recorded (or the run ended without them).
    acked_results: BTreeSet<(String, CommandIndex)>,
    /// Results the worker holds and resends until acknowledged.
    worker_pending: BTreeMap<(String, CommandIndex), StepOutcome>,
    executions: BTreeMap<(String, CommandIndex), u64>,
    report: Report,
    workload: Workload,
}

pub(crate) fn run(seed: u64, config: &Config) -> Result<Report, Failure> {
    let mut rng = Rng::new(seed);
    let mut disk = SimDisk::new(rng.fork(), config.sync_fail_ppm, config.torn_ppm);
    disk.skip_truncation = config.bugs.skip_tail_truncation;
    let workload = Workload::new(config.workload).map_err(|message| Failure {
        seed,
        time: 0,
        message,
    })?;
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
        faults: true,
        node: Node::new(disk, config.bugs.ack_before_sync, workload.decider.clone()).map_err(
            |message| Failure {
                seed,
                time: 0,
                message,
            },
        )?,
        workload,
        crash_pending: false,
        pre_crash: BTreeMap::new(),
        wanted,
        acked_starts: BTreeSet::new(),
        acked_results: BTreeSet::new(),
        worker_pending: BTreeMap::new(),
        executions: BTreeMap::new(),
        report: Report {
            seed,
            ..Report::default()
        },
    };
    world.run().map_err(|message| Failure {
        seed,
        time: world.now,
        message,
    })
}

impl World<'_> {
    fn run(&mut self) -> Result<Report, String> {
        self.schedule(0, Ev::ClientRetry);
        self.schedule(
            self.config.redispatch_interval,
            Ev::Redispatch { incarnation: 0 },
        );
        self.schedule(self.config.worker_resend, Ev::WorkerResend);

        loop {
            let Some(((time, _), ev)) = self.queue.pop_first() else {
                return Err("the event queue ran dry".into());
            };
            self.now = time;
            self.report.events += 1;
            self.report.trace = mix(mix(self.report.trace, time), ev.code());

            if self.faults && self.now >= self.config.fault_window {
                self.faults = false;
                if let Some(disk) = self.node.disk_mut() {
                    disk.set_faults(0, 0);
                }
            }
            if self.now > self.config.max_time {
                return Err(format!(
                    "liveness: not quiescent by time {}: {}",
                    self.config.max_time,
                    self.unfinished()
                ));
            }

            self.handle(ev)?;

            if self.faults
                && self.node.up
                && !self.crash_pending
                && self.rng.chance(self.config.crash_ppm)
            {
                self.crash_pending = true;
                let delay = self.rng.below(30);
                self.schedule(delay, Ev::Crash);
            }
            self.node.check_fold()?;

            if !self.faults && self.quiescent() {
                self.final_checks()?;
                self.report.end_time = self.now;
                self.report.syncs = self.node.disk().syncs;
                self.report.torn_tails = self.node.disk().torn_tails;
                self.report.step_executions = self.executions.values().sum();
                self.report.duplicate_executions =
                    self.report.step_executions - self.executions.len() as u64;
                return Ok(std::mem::take(&mut self.report));
            }
        }
    }

    fn handle(&mut self, ev: Ev) -> Result<(), String> {
        match ev {
            Ev::NodeStart { key, input } => {
                if self.node.up {
                    self.node.on_start(&key, input)?;
                    self.after_node_event();
                }
            }
            Ev::NodeStepResult { key, cmd, outcome } => {
                if self.node.up {
                    self.node.on_step_result(&key, cmd, outcome)?;
                    self.after_node_event();
                }
            }
            Ev::Flush { incarnation } => {
                if self.node.up && incarnation == self.node.incarnation {
                    self.node.flush_scheduled = false;
                    match self.node.flush() {
                        Ok(outbound) => self.deliver(outbound),
                        Err(_) => {
                            self.report.sync_failures += 1;
                            self.crash();
                        }
                    }
                }
            }
            Ev::Redispatch { incarnation } => {
                if self.node.up && incarnation == self.node.incarnation {
                    if !self.config.bugs.no_redispatch {
                        self.node.on_redispatch();
                    }
                    self.after_node_event();
                    self.schedule(
                        self.config.redispatch_interval,
                        Ev::Redispatch { incarnation },
                    );
                }
            }
            Ev::Crash => {
                self.crash_pending = false;
                if self.node.up {
                    self.crash();
                }
            }
            Ev::Restart => {
                if !self.node.up {
                    self.restart()?;
                }
            }
            Ev::ClientStartAck { key } => {
                self.acked_starts.insert(key);
            }
            Ev::ClientRetry => {
                let missing: Vec<(String, Payload)> = self
                    .wanted
                    .iter()
                    .filter(|(key, _)| !self.acked_starts.contains(key))
                    .cloned()
                    .collect();
                if !missing.is_empty() {
                    for (key, input) in missing {
                        self.send(Ev::NodeStart { key, input });
                    }
                    self.schedule(self.config.client_retry, Ev::ClientRetry);
                }
            }
            Ev::WorkerDispatch {
                key,
                cmd,
                name,
                input,
            } => {
                if self.faults && self.rng.chance(self.config.worker_amnesia_ppm) {
                    // The worker restarts and forgets the results it was still trying to report.
                    self.worker_pending.clear();
                }
                *self.executions.entry((key.clone(), cmd)).or_default() += 1;
                let outcome = (self.workload.execute)(&name, &input);
                self.worker_pending
                    .insert((key.clone(), cmd), outcome.clone());
                self.send(Ev::NodeStepResult { key, cmd, outcome });
            }
            Ev::WorkerResultAck { key, cmd } => {
                self.worker_pending.remove(&(key.clone(), cmd));
                self.acked_results.insert((key, cmd));
            }
            Ev::WorkerResend => {
                let pending: Vec<_> = self
                    .worker_pending
                    .iter()
                    .map(|((key, cmd), outcome)| (key.clone(), *cmd, outcome.clone()))
                    .collect();
                for (key, cmd, outcome) in pending {
                    self.send(Ev::NodeStepResult { key, cmd, outcome });
                }
                self.schedule(self.config.worker_resend, Ev::WorkerResend);
            }
        }
        Ok(())
    }

    /// Schedules a flush if needed and sends anything the (buggy) node released early.
    fn after_node_event(&mut self) {
        if self.node.needs_flush() && !self.node.flush_scheduled {
            self.node.flush_scheduled = true;
            let incarnation = self.node.incarnation;
            self.schedule(self.config.flush_interval, Ev::Flush { incarnation });
        }
        let early = self.node.take_immediate();
        self.deliver(early);
    }

    fn deliver(&mut self, outbound: Vec<Outbound>) {
        for message in outbound {
            let ev = match message {
                Outbound::Started { key } => Ev::ClientStartAck { key },
                Outbound::Dispatch {
                    key,
                    cmd,
                    name,
                    input,
                } => Ev::WorkerDispatch {
                    key,
                    cmd,
                    name,
                    input,
                },
                Outbound::ResultRecorded { key, cmd } => Ev::WorkerResultAck { key, cmd },
                Outbound::Finished { .. } => continue,
            };
            self.send(ev);
        }
    }

    fn crash(&mut self) {
        self.pre_crash = self
            .node
            .runs()
            .iter()
            .map(|(k, r)| (k.clone(), r.journal().to_vec()))
            .collect();
        self.node.crash();
        self.report.crashes += 1;
        let delay = 1 + self.rng.below(self.config.restart_delay);
        self.schedule(delay, Ev::Restart);
    }

    fn restart(&mut self) -> Result<(), String> {
        self.node.recover()?;

        // Recovered journals are prefixes of what the node had in memory before the crash.
        for (key, run) in self.node.runs() {
            let before = self.pre_crash.get(key).ok_or_else(|| {
                format!("run {key} was recovered but did not exist before the crash")
            })?;
            if !before.starts_with(run.journal()) {
                return Err(format!(
                    "run {key}: the recovered journal is not a prefix of the journal before the crash"
                ));
            }
        }
        self.check_acknowledged()?;

        self.node.resume()?;
        let incarnation = self.node.incarnation;
        self.schedule(
            self.config.redispatch_interval,
            Ev::Redispatch { incarnation },
        );
        self.after_node_event();
        Ok(())
    }

    /// Invariant: acknowledged work is never lost.
    fn check_acknowledged(&self) -> Result<(), String> {
        for key in &self.acked_starts {
            if !self.node.runs().contains_key(key) {
                return Err(format!("lost acknowledged start of {key}"));
            }
        }
        for (key, cmd) in &self.acked_results {
            let run = self
                .node
                .runs()
                .get(key)
                .ok_or_else(|| format!("lost run {key} with acknowledged results"))?;
            let index = usize::try_from(cmd.0).map_err(|e| e.to_string())?;
            let recorded = run
                .state()
                .steps()
                .get(index)
                .is_some_and(|s| s.outcome.is_some());
            if !recorded && !run.state().status().is_terminal() {
                return Err(format!("lost acknowledged result of {key} command {cmd}"));
            }
        }
        Ok(())
    }

    fn quiescent(&self) -> bool {
        self.node.up
            && !self.node.needs_flush()
            && self.acked_starts.len() == self.wanted.len()
            && self.wanted.iter().all(|(key, _)| {
                self.node
                    .runs()
                    .get(key)
                    .is_some_and(|r| r.state().status().is_terminal())
            })
    }

    fn final_checks(&mut self) -> Result<(), String> {
        self.check_acknowledged()?;

        // Every run ended as the sequential reference says it should.
        for (key, input) in &self.wanted {
            let run = self
                .node
                .runs()
                .get(key)
                .ok_or_else(|| format!("run {key} missing at the end"))?;
            let state = &run.state();
            if state.status() == Status::Parked {
                return Err(format!(
                    "run {key} was parked: {}",
                    state.error().unwrap_or_default()
                ));
            }
            let expected = workflow::reference(&self.workload, input)?;
            let actual = (
                state.status(),
                state.output().cloned(),
                state.error().map(str::to_owned),
            );
            if actual != expected {
                return Err(format!(
                    "run {key} ended as {actual:?}, the reference says {expected:?}"
                ));
            }
            match state.status() {
                Status::Completed => self.report.completed += 1,
                _ => self.report.failed += 1,
            }
        }

        // The durable log holds exactly the in-memory journals, with valid chains.
        let decoded = frame::decode_all(self.node.disk().durable());
        if decoded.valid_len != self.node.disk().durable().len() {
            return Err("the durable log has an invalid tail after quiescence".into());
        }
        let mut by_run: BTreeMap<String, Vec<Vec<u8>>> = BTreeMap::new();
        for bytes in &decoded.payloads {
            let (key, record) = decode_log_record(bytes).map_err(|e| e.to_string())?;
            by_run
                .entry(key.to_owned())
                .or_default()
                .push(record.to_vec());
        }
        if by_run.len() != self.node.runs().len() {
            return Err("the durable log and memory hold different runs".into());
        }
        for (key, records) in &by_run {
            let verified =
                verify_chain(key, records.iter().map(Vec::as_slice)).map_err(|e| e.to_string())?;
            let run = self
                .node
                .runs()
                .get(key)
                .ok_or_else(|| format!("run {key} only on disk"))?;
            if verified.entries != run.journal() {
                return Err(format!("run {key}: durable journal differs from memory"));
            }
            for byte in verified.head {
                self.report.trace = mix(self.report.trace, u64::from(byte));
            }
        }
        Ok(())
    }

    fn unfinished(&self) -> String {
        let pending: Vec<String> = self
            .wanted
            .iter()
            .filter(|(key, _)| {
                !self
                    .node
                    .runs()
                    .get(key)
                    .is_some_and(|r| r.state().status().is_terminal())
            })
            .map(|(key, _)| key.clone())
            .collect();
        format!(
            "node up: {}, acked starts: {}/{}, unfinished runs: {:?}, seed {}",
            self.node.up,
            self.acked_starts.len(),
            self.wanted.len(),
            pending,
            self.seed
        )
    }

    /// Sends a message over the network: delayed, and while faults are on, maybe dropped or duplicated.
    fn send(&mut self, ev: Ev) {
        debug_assert!(ev.is_message());
        if self.faults && self.rng.chance(self.config.drop_ppm) {
            self.report.dropped += 1;
            return;
        }
        if self.faults && self.rng.chance(self.config.duplicate_ppm) {
            self.report.duplicated += 1;
            let delay = 1 + self.rng.below(self.config.max_delay);
            self.schedule(delay, ev.clone());
        }
        let delay = 1 + self.rng.below(self.config.max_delay);
        self.schedule(delay, ev);
    }

    fn schedule(&mut self, delay: u64, ev: Ev) {
        self.queue.insert((self.now + delay, self.seq), ev);
        self.seq += 1;
    }
}

/// FNV-1a style mixing for the trace fingerprint.
fn mix(hash: u64, value: u64) -> u64 {
    let mut h = if hash == 0 {
        0xcbf2_9ce4_8422_2325
    } else {
        hash
    };
    for byte in value.to_le_bytes() {
        h ^= u64::from(byte);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}
