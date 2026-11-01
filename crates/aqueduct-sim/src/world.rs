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
        /// For an agent call, its token budget.
        agent: Option<u64>,
    },
    NodeAgentResult {
        key: String,
        cmd: CommandIndex,
        outcome: Result<aqueduct_core::AgentResponse, String>,
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
    NodeSignal {
        key: String,
    },
    ClientSignalAck {
        key: String,
    },
    NodeCancel {
        key: String,
    },
    ClientCancelAck {
        key: String,
    },
    NodeResolve {
        run: String,
        cmd: CommandIndex,
        outcome: Payload,
    },
    ClientResolveAck {
        run: String,
    },
    TimerTick {
        incarnation: u64,
    },
    NodeAttemptFailed {
        key: String,
        cmd: CommandIndex,
        error: String,
    },
    WorkerAttemptAck {
        key: String,
        cmd: CommandIndex,
    },
    CheckpointTick {
        incarnation: u64,
    },
}

/// What a worker reports for one execution of a step.
#[derive(Clone, Debug)]
enum WorkerReport {
    Outcome(StepOutcome),
    Agent(Result<aqueduct_core::AgentResponse, String>),
    Retry(String),
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
            Ev::NodeSignal { .. } => 12,
            Ev::ClientSignalAck { .. } => 13,
            Ev::TimerTick { .. } => 14,
            Ev::NodeAttemptFailed { .. } => 15,
            Ev::WorkerAttemptAck { .. } => 16,
            Ev::CheckpointTick { .. } => 17,
            Ev::NodeCancel { .. } => 18,
            Ev::ClientCancelAck { .. } => 19,
            Ev::NodeResolve { .. } => 20,
            Ev::ClientResolveAck { .. } => 21,
            Ev::NodeAgentResult { .. } => 22,
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
                | Ev::NodeSignal { .. }
                | Ev::ClientSignalAck { .. }
                | Ev::NodeCancel { .. }
                | Ev::ClientCancelAck { .. }
                | Ev::NodeResolve { .. }
                | Ev::ClientResolveAck { .. }
                | Ev::NodeAgentResult { .. }
                | Ev::NodeAttemptFailed { .. }
                | Ev::WorkerAttemptAck { .. }
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
    /// Runs whose `go` signal the client saw acknowledged.
    acked_signals: BTreeSet<String>,
    /// Runs the client cancels, and those whose cancellation it saw acknowledged (never lost).
    to_cancel: BTreeSet<String>,
    acked_cancels: BTreeSet<String>,
    /// The escalation the client last sent a resolution for, by run: (command, reason).
    resolving: BTreeMap<String, (CommandIndex, String)>,
    /// Escalations whose resolution the client saw acknowledged (never lost): (run id, command,
    /// reason). One agent call can escalate twice, for its budget and then for its answer.
    acked_resolutions: BTreeSet<(String, CommandIndex, String)>,
    /// Timer firings (run, journal position) already checked against their wake time.
    checked_timers: BTreeSet<(String, usize)>,
    /// Results the worker holds and resends until acknowledged.
    worker_pending: BTreeMap<(String, CommandIndex), WorkerReport>,
    executions: BTreeMap<(String, CommandIndex), u64>,
    /// Set when the node dispatches an agent call whose response is already recorded.
    agent_reissued: Option<String>,
    /// The name and input each command was dispatched with.
    dispatched: BTreeMap<(String, CommandIndex), (String, Payload)>,
    report: Report,
    workload: Workload,
}

fn report_event(key: String, cmd: CommandIndex, report: WorkerReport) -> Ev {
    match report {
        WorkerReport::Outcome(outcome) => Ev::NodeStepResult { key, cmd, outcome },
        WorkerReport::Agent(outcome) => Ev::NodeAgentResult { key, cmd, outcome },
        WorkerReport::Retry(error) => Ev::NodeAttemptFailed { key, cmd, error },
    }
}

/// How often the node saves a checkpoint (when nothing is waiting to be written).
const CHECKPOINT_TICK: u64 = 70;

/// How often the node checks for due timers, in simulated milliseconds.
const TIMER_TICK: u64 = 25;

pub(crate) fn run(seed: u64, config: &Config) -> Result<Report, Failure> {
    let mut rng = Rng::new(seed);
    let mut disk = SimDisk::new(rng.fork(), config.sync_fail_ppm, config.torn_ppm);
    disk.skip_truncation = config.bugs.skip_tail_truncation;
    let workload = Workload::new(config.workload).map_err(|message| Failure {
        seed,
        workload: config.workload,
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
                workload: config.workload,
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
        acked_signals: BTreeSet::new(),
        to_cancel: BTreeSet::new(),
        acked_cancels: BTreeSet::new(),
        resolving: BTreeMap::new(),
        acked_resolutions: BTreeSet::new(),
        checked_timers: BTreeSet::new(),
        worker_pending: BTreeMap::new(),
        executions: BTreeMap::new(),
        dispatched: BTreeMap::new(),
        agent_reissued: None,
        report: Report {
            seed,
            ..Report::default()
        },
    };
    world.node.forget_dirty_runs = config.bugs.checkpoint_forgets_recovered_runs;
    world.node.skip_successor_repair = config.bugs.skip_successor_repair;
    world.node.release_stale_dispatches = config.bugs.release_stale_dispatches;
    if config.bugs.release_stale_dispatches {
        world.node.inject_release_stale_dispatches();
    }
    if config.cancel_every > 0 {
        world.to_cancel = (0..config.runs)
            .filter(|i| i % config.cancel_every == config.cancel_every - 1)
            .map(|i| format!("run-{i}"))
            .collect();
    }
    world.run().map_err(|message| Failure {
        seed,
        workload: config.workload,
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
        self.schedule(TIMER_TICK, Ev::TimerTick { incarnation: 0 });
        self.schedule(CHECKPOINT_TICK, Ev::CheckpointTick { incarnation: 0 });

        loop {
            let Some(((time, _), ev)) = self.queue.pop_first() else {
                return Err("the event queue ran dry".into());
            };
            self.now = time;
            self.node.set_time(time);
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
            self.check_timers()?;
            if let Some(reissued) = self.agent_reissued.take() {
                return Err(reissued);
            }

            if !self.faults && self.quiescent() {
                self.final_checks()?;
                self.report.end_time = self.now;
                self.report.syncs = self.node.disk().syncs;
                self.report.torn_tails = self.node.disk().torn_tails;
                self.report.retries = self
                    .node
                    .runs()
                    .values()
                    .flat_map(|run| run.journal())
                    .filter(|e| matches!(e, aqueduct_core::Entry::StepRetryScheduled { .. }))
                    .count() as u64;
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
            Ev::NodeAgentResult { key, cmd, outcome } => {
                if self.node.up {
                    self.node.on_agent_result(&key, cmd, outcome)?;
                    self.after_node_event();
                }
            }
            Ev::NodeStepResult { key, cmd, outcome } => {
                if self.node.up {
                    self.node.on_step_result(&key, cmd, outcome)?;
                    self.after_node_event();
                }
            }
            Ev::NodeAttemptFailed { key, cmd, error } => {
                if self.node.up {
                    self.node.on_attempt_failed(&key, cmd, error)?;
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
            Ev::ClientSignalAck { key } => {
                self.acked_signals.insert(key);
            }
            Ev::NodeSignal { key } => {
                if self.node.up {
                    self.node.on_signal(&key)?;
                    self.after_node_event();
                }
            }
            Ev::ClientCancelAck { key } => {
                self.acked_cancels.insert(key);
            }
            Ev::ClientResolveAck { run } => {
                if let Some((cmd, reason)) = self.resolving.get(&run).cloned() {
                    self.acked_resolutions.insert((run, cmd, reason));
                }
            }
            Ev::NodeResolve { run, cmd, outcome } => {
                if self.node.up {
                    self.node.on_resolve(&run, cmd, outcome)?;
                    self.after_node_event();
                }
            }
            Ev::NodeCancel { key } => {
                if self.node.up {
                    self.node.on_cancel(&key)?;
                    self.after_node_event();
                }
            }
            Ev::CheckpointTick { incarnation } => {
                if self.node.up && incarnation == self.node.incarnation {
                    if self.node.checkpoint()? {
                        self.report.checkpoints += 1;
                    }
                    self.schedule(CHECKPOINT_TICK, Ev::CheckpointTick { incarnation });
                }
            }
            Ev::TimerTick { incarnation } => {
                if self.node.up && incarnation == self.node.incarnation {
                    self.node.fire_due_timers()?;
                    self.after_node_event();
                    self.schedule(TIMER_TICK, Ev::TimerTick { incarnation });
                }
            }
            Ev::ClientRetry => {
                let missing: Vec<(String, Payload)> = self
                    .wanted
                    .iter()
                    .filter(|(key, _)| !self.acked_starts.contains(key))
                    .cloned()
                    .collect();
                // Signal runs whose start was acknowledged, retrying until the signal is acknowledged.
                let unsignalled: Vec<String> = if self.workload.signals {
                    self.acked_starts
                        .difference(&self.acked_signals)
                        .cloned()
                        .collect()
                } else {
                    Vec::new()
                };
                // Cancel some runs once started, at a random moment, until acknowledged.
                let uncancelled: Vec<String> = self
                    .to_cancel
                    .iter()
                    .filter(|key| {
                        self.acked_starts.contains(*key) && !self.acked_cancels.contains(*key)
                    })
                    .cloned()
                    .collect();
                // The reviewer: looks at the open escalations, as a dashboard would, and resolves each,
                // retrying until the resolution is acknowledged.
                let open: Vec<(String, CommandIndex, String)> = if self.workload.escalations {
                    self.node.open_escalations()
                } else {
                    Vec::new()
                };
                let unfinished = self.wanted.iter().any(|(key, _)| {
                    !self
                        .latest(key)
                        .is_some_and(|r| r.state().status().is_terminal())
                });
                let pending = !missing.is_empty()
                    || !unsignalled.is_empty()
                    || self.acked_cancels.len() < self.to_cancel.len()
                    // A run may still escalate: keep looking.
                    || (self.workload.escalations && unfinished)
                    || (self.workload.signals && self.acked_signals.len() < self.wanted.len());
                for (key, input) in missing {
                    self.send(Ev::NodeStart { key, input });
                }
                for key in unsignalled {
                    if self.rng.chance(400_000) {
                        self.send(Ev::NodeSignal { key });
                    }
                }
                for key in uncancelled {
                    if self.rng.chance(300_000) {
                        self.send(Ev::NodeCancel { key });
                    }
                }
                for (run, cmd, reason) in open {
                    if self.rng.chance(400_000) {
                        let key = aqueduct_core::split_run_id(&run).0;
                        let Some((_, input)) = self.wanted.iter().find(|(k, _)| k == key) else {
                            return Err(format!("an escalation of unknown run {run:?}"));
                        };
                        let outcome = if reason == aqueduct_core::SCHEMA_ESCALATION {
                            // The reviewer supplies the answer the model should have given.
                            let Some(step) = self.node.runs().get(&run).and_then(|r| {
                                r.state().steps().get(usize::try_from(cmd.0).ok()?).cloned()
                            }) else {
                                continue;
                            };
                            workflow::fake_model(&step.name, &step.input).response
                        } else {
                            workflow::resolution(&reason, input)
                        };
                        self.resolving.insert(run.clone(), (cmd, reason.clone()));
                        self.send(Ev::NodeResolve { run, cmd, outcome });
                    }
                }
                if pending {
                    self.schedule(self.config.client_retry, Ev::ClientRetry);
                }
            }
            Ev::WorkerDispatch {
                key,
                cmd,
                name,
                input,
                agent,
            } => {
                if self.faults && self.rng.chance(self.config.worker_amnesia_ppm) {
                    // The worker restarts and forgets the results it was still trying to report.
                    self.worker_pending.clear();
                }
                // Invariant: a command is dispatched with one name and input only, so nothing a
                // worker saw is ever recorded differently (for example, a recorded value used before
                // it was durable, then recorded again after a crash).
                let sent = (name.clone(), input.clone());
                match self.dispatched.get(&(key.clone(), cmd)) {
                    Some(earlier) if *earlier != sent => {
                        return Err(format!(
                            "run {key} command {cmd} was dispatched as {earlier:?}, then as {sent:?}"
                        ));
                    }
                    Some(_) => {}
                    None => {
                        self.dispatched.insert((key.clone(), cmd), sent);
                    }
                }
                *self.executions.entry((key.clone(), cmd)).or_default() += 1;
                let report = if agent.is_some() {
                    self.report.agent_calls += 1;
                    let mut answer = workflow::fake_model(&name, &input);
                    // A model that sometimes answers off schema: retried, then escalated.
                    if self.faults && self.rng.chance(300_000) {
                        answer.response = Payload::new(*b"\"off schema\"");
                    }
                    WorkerReport::Agent(Ok(answer))
                } else if self.faults && self.rng.chance(self.config.transient_ppm) {
                    // A transient failure: the engine's retry policy schedules another attempt.
                    WorkerReport::Retry("transient failure".into())
                } else {
                    WorkerReport::Outcome((self.workload.execute)(&name, &input))
                };
                self.worker_pending
                    .insert((key.clone(), cmd), report.clone());
                self.send(report_event(key, cmd, report));
            }
            Ev::WorkerResultAck { key, cmd } => {
                // Acknowledges an outcome: it must never be lost.
                if let Some(WorkerReport::Outcome(_) | WorkerReport::Agent(_)) =
                    self.worker_pending.get(&(key.clone(), cmd))
                {
                    self.worker_pending.remove(&(key.clone(), cmd));
                }
                self.acked_results.insert((key, cmd));
            }
            Ev::WorkerAttemptAck { key, cmd } => {
                // Acknowledges a retryable failure (or an answer refused by its schema); the command
                // stays pending until a retry completes it.
                if let Some(WorkerReport::Retry(_) | WorkerReport::Agent(_)) =
                    self.worker_pending.get(&(key.clone(), cmd))
                {
                    self.worker_pending.remove(&(key, cmd));
                }
            }
            Ev::WorkerResend => {
                let pending: Vec<_> = self
                    .worker_pending
                    .iter()
                    .map(|((key, cmd), report)| (key.clone(), *cmd, report.clone()))
                    .collect();
                for (key, cmd, report) in pending {
                    self.send(report_event(key, cmd, report));
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
                Outbound::Signaled { key } => Ev::ClientSignalAck { key },
                Outbound::Cancelled { key } => Ev::ClientCancelAck { key },
                Outbound::Resolved { key } => Ev::ClientResolveAck { run: key },
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
                    agent: None,
                },
                Outbound::DispatchAgent {
                    key,
                    cmd,
                    name,
                    request,
                    budget_tokens,
                } => {
                    // Invariant: a recorded agent call is never issued again (phase 2 acceptance). Checked
                    // when the node sends the dispatch, not when a delayed copy arrives.
                    let recorded = self.node.runs().get(&key).is_some_and(|run| {
                        run.journal().iter().any(
                            |e| matches!(e, Entry::AgentCallCompleted { cmd: c, .. } if *c == cmd),
                        )
                    });
                    if recorded && self.agent_reissued.is_none() {
                        self.agent_reissued = Some(format!(
                            "run {key} agent call {cmd} was dispatched again after its response was recorded"
                        ));
                    }
                    Ev::WorkerDispatch {
                        key,
                        cmd,
                        name,
                        input: request,
                        agent: Some(budget_tokens),
                    }
                }
                Outbound::ResultRecorded { key, cmd } => Ev::WorkerResultAck { key, cmd },
                Outbound::AttemptRecorded { key, cmd } => Ev::WorkerAttemptAck { key, cmd },
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
        self.schedule(TIMER_TICK, Ev::TimerTick { incarnation });
        self.schedule(CHECKPOINT_TICK, Ev::CheckpointTick { incarnation });
        let incarnation = self.node.incarnation;
        self.schedule(
            self.config.redispatch_interval,
            Ev::Redispatch { incarnation },
        );
        self.after_node_event();
        Ok(())
    }

    /// A key's latest run (the last generation: a run may continue as new), as a client sees it.
    fn latest(&self, key: &str) -> Option<&aqueduct_engine::PartitionRun> {
        let prefix = format!("{key}\0");
        self.node
            .runs()
            .range::<str, _>((std::ops::Bound::Included(key), std::ops::Bound::Unbounded))
            .take_while(|(id, _)| id.as_str() == key || id.starts_with(&prefix))
            .max_by_key(|(id, _)| aqueduct_core::split_run_id(id).1)
            .map(|(_, run)| run)
    }

    /// Invariant: acknowledged work is never lost.
    fn check_acknowledged(&self) -> Result<(), String> {
        for key in &self.acked_starts {
            if !self.node.runs().contains_key(key) {
                return Err(format!("lost acknowledged start of {key}"));
            }
        }
        for key in &self.acked_cancels {
            let Some(run) = self.latest(key) else {
                return Err(format!("lost run {key} with an acknowledged cancellation"));
            };
            // A cancellation of a run that had already ended is dropped, as specified.
            if run.state().cancel_reason().is_none() && !run.state().status().is_terminal() {
                return Err(format!("lost acknowledged cancellation of {key}"));
            }
        }
        for (id, cmd, reason) in &self.acked_resolutions {
            let Some(run) = self.node.runs().get(id) else {
                return Err(format!("lost run {id:?} with an acknowledged resolution"));
            };
            // Once a resolution is acknowledged, its escalation is no longer open (resolved, approved
            // or declined; or closed first by a cancellation, which drops the resolution as specified).
            let still_open = run
                .state()
                .open_escalations()
                .iter()
                .any(|(c, r, _)| c == cmd && r == reason);
            if still_open {
                return Err(format!(
                    "lost acknowledged resolution of {id:?} command {cmd}"
                ));
            }
        }
        for key in &self.acked_signals {
            let Some(run) = self.latest(key) else {
                return Err(format!("lost run {key} with an acknowledged signal"));
            };
            let has = |f: fn(&aqueduct_core::Entry) -> bool| run.journal().iter().any(f);
            let received = has(|e| matches!(e, aqueduct_core::Entry::SignalReceived { .. }));
            let waited = has(|e| matches!(e, aqueduct_core::Entry::SignalAwaited { .. }));
            // A signal to a run that ended without waiting for one is dropped, as specified.
            let ended_first = run.state().status().is_terminal() && !waited;
            if !received && !ended_first {
                return Err(format!("lost the acknowledged signal to {key}"));
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

    /// Invariant: timers never fire early. Checked when a firing is first seen, right after the event
    /// that appended it, so the simulated clock is the time it fired.
    fn check_timers(&mut self) -> Result<(), String> {
        use aqueduct_core::Entry;
        for (key, run) in self.node.runs() {
            let journal = run.journal();
            for (position, entry) in journal.iter().enumerate() {
                let Entry::TimerFired { cmd } = entry else {
                    continue;
                };
                if !self.checked_timers.insert((key.clone(), position)) {
                    continue;
                }
                // The timer that fired is the latest one armed for this command before the firing.
                let wake_at = journal[..position].iter().rev().find_map(|e| match e {
                    Entry::TimerStarted {
                        cmd: c, wake_at, ..
                    }
                    | Entry::SignalAwaited {
                        cmd: c, wake_at, ..
                    }
                    | Entry::StepRetryScheduled {
                        cmd: c, wake_at, ..
                    } if c == cmd => Some(*wake_at),
                    _ => None,
                });
                match wake_at {
                    Some(wake_at) if wake_at <= self.now => {}
                    Some(wake_at) => {
                        return Err(format!(
                            "run {key}: timer {cmd} fired at {} before its wake time {wake_at}",
                            self.now
                        ));
                    }
                    None => return Err(format!("run {key}: timer {cmd} fired without a timer")),
                }
            }
        }
        Ok(())
    }

    fn quiescent(&self) -> bool {
        self.node.up
            && !self.node.needs_flush()
            && self.acked_starts.len() == self.wanted.len()
            && (!self.workload.signals || self.acked_signals.len() == self.wanted.len())
            && self.acked_cancels.len() == self.to_cancel.len()
            && self.wanted.iter().all(|(key, _)| {
                self.latest(key)
                    .is_some_and(|r| r.state().status().is_terminal())
            })
    }

    fn final_checks(&mut self) -> Result<(), String> {
        self.check_acknowledged()?;

        // Signals were delivered exactly once, despite retries and duplicated messages.
        for (key, run) in self.node.runs() {
            let count = |f: fn(&aqueduct_core::Entry) -> bool| {
                run.journal().iter().filter(|e| f(e)).count()
            };
            let received = count(|e| matches!(e, aqueduct_core::Entry::SignalReceived { .. }));
            // A wait that a cancellation completed takes no signal.
            let waited = run
                .state()
                .steps()
                .iter()
                .filter(|s| {
                    matches!(s.kind, aqueduct_core::CommandKind::Signal { .. }) && !s.cancelled
                })
                .count();
            if received > 1 || received < waited {
                return Err(format!(
                    "run {key} recorded {received} signals and waited for {waited}"
                ));
            }
        }

        let count = |f: fn(&Entry) -> bool| {
            self.node
                .runs()
                .values()
                .flat_map(|run| run.journal())
                .filter(|e| f(e))
                .count() as u64
        };
        self.report.budget_escalations = count(|e| matches!(e, Entry::AgentCallEscalated { .. }));
        self.report.budget_approvals = count(|e| matches!(e, Entry::BudgetApproved { .. }));
        self.report.answer_escalations = count(|e| matches!(e, Entry::AgentAnswerEscalated { .. }));
        self.report.escalations_resolved = self
            .node
            .runs()
            .values()
            .flat_map(|run| run.journal())
            .filter(|e| matches!(e, Entry::EscalationResolved { .. }))
            .count() as u64;

        // Every dispatched step and agent call is in the final journal as it was dispatched.
        for ((key, cmd), (name, input)) in &self.dispatched {
            let scheduled = self.node.runs().get(key).and_then(|run| {
                run.journal().iter().find_map(|e| match e {
                    aqueduct_core::Entry::StepScheduled {
                        cmd: c,
                        name,
                        input,
                    }
                    | aqueduct_core::Entry::AgentCallScheduled {
                        cmd: c,
                        name,
                        request: input,
                        ..
                    }
                    | aqueduct_core::Entry::AgentCallEscalated {
                        cmd: c,
                        name,
                        request: input,
                        ..
                    } if c == cmd => Some((name, input)),
                    _ => None,
                })
            });
            if scheduled != Some((name, input)) {
                return Err(format!(
                    "run {key} command {cmd} was dispatched as {name} {input:?}, the journal has {scheduled:?}"
                ));
            }
        }

        // Every run ended as the sequential reference says it should.
        for (key, input) in &self.wanted {
            let run = self
                .latest(key)
                .ok_or_else(|| format!("run {key} missing at the end"))?;
            let state = &run.state();
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
            // A run cancelled while it was running fails with the reason, after undoing its work, unless
            // the cancellation only reached what the run no longer depended on (a compensation, which is
            // issued again); then it ends as the reference says.
            if state.cancel_reason().is_some() {
                if !self.to_cancel.contains(key) {
                    return Err(format!("run {key} was cancelled, but nobody cancelled it"));
                }
                let cancelled = (
                    Status::Failed,
                    None,
                    Some(aqueduct_core::cancelled_error(workflow::CANCEL_REASON)),
                );
                if actual != cancelled && actual != expected {
                    return Err(format!(
                        "cancelled run {key} ended as {actual:?}, expected {cancelled:?} or {expected:?}"
                    ));
                }
                match state.status() {
                    _ if actual == cancelled => self.report.cancelled += 1,
                    Status::Completed => self.report.completed += 1,
                    _ => self.report.failed += 1,
                }
                continue;
            }
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
                    .latest(key)
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
