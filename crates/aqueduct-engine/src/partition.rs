//! The partition processor: the sans-I/O heart of the engine (ADR 0004, ADR 0006).
//!
//! A [`Partition`] owns a [`LogStore`] and the in-memory runs whose journals it holds. It follows three
//! rules:
//!
//! - entries are appended to a run's journal and to the log batch together;
//! - nothing leaves the partition ([`Outbound`] messages: acknowledgements, dispatches, completions)
//!   until [`Partition::flush`] has synced the batch holding every entry it depends on; after a
//!   successful flush, memory equals the durable log;
//! - a failed flush is fatal: the caller drops the partition and recovers from the log.
//!
//! The simulator drives it with a simulated disk; the embedded engine drives it with a file log.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use aqueduct_codec::{ChainWriter, decode_log_record, encode_log_record, verify_chain};
use aqueduct_core::{
    CommandIndex, CommandKind, Decider, Effect, Entry, Env, Event, Payload, RunState, Status,
    StepOutcome, Transition, child_key, run_id, split_run_id, start_version, valid_key,
};
use aqueduct_storage::{LogStore, NoStateStore, StateStore, StorageError};

/// The deciders a partition can run, by workflow name.
#[derive(Clone, Default)]
pub struct Deciders {
    by_name: BTreeMap<String, Registered>,
}

type SharedDecider = Arc<dyn Decider + Send + Sync>;

/// What is registered under one workflow name: an unversioned decider (code workflows), versions, and
/// which version new runs start on.
#[derive(Clone, Default)]
struct Registered {
    unversioned: Option<SharedDecider>,
    versions: BTreeMap<String, SharedDecider>,
    active: Option<String>,
    /// Decided by a remote workflow worker (ADR 0039): its activations leave the partition as
    /// [`Outbound::Activation`], and its decisions come back through [`Partition::decision`].
    remote: bool,
    /// For a remote workflow, the builds runs may be started on (ADR 0048).
    builds: BTreeSet<String>,
}

impl Deciders {
    /// An empty set.
    pub fn new() -> Self {
        Deciders::default()
    }

    /// Registers an unversioned decider under a workflow name, replacing any earlier one. Its runs are
    /// not pinned: they are decided by whatever is registered under the name when they resume.
    pub fn insert(&mut self, workflow: impl Into<String>, decider: SharedDecider) {
        self.by_name.entry(workflow.into()).or_default().unversioned = Some(decider);
    }

    /// Registers a workflow that a remote workflow worker decides (ADR 0039). Its runs are unversioned.
    pub fn insert_remote(&mut self, workflow: impl Into<String>) {
        self.by_name.entry(workflow.into()).or_default().remote = true;
    }

    /// Whether a workflow is decided remotely.
    pub fn is_remote(&self, workflow: &str) -> bool {
        self.by_name.get(workflow).is_some_and(|r| r.remote)
    }

    /// Registers a version of a workflow (ADR 0029). Runs started on it are pinned to it: they are only
    /// ever decided by the decider registered under this version. Registering does not activate it.
    pub fn insert_version(
        &mut self,
        workflow: impl Into<String>,
        version: impl Into<String>,
        decider: SharedDecider,
    ) {
        self.by_name
            .entry(workflow.into())
            .or_default()
            .versions
            .insert(version.into(), decider);
    }

    /// Makes a build of a remote workflow the one new runs start on (ADR 0048). A build need not be known
    /// beforehand: its code is in a worker.
    pub fn activate_build(&mut self, workflow: &str, build: &str) -> Result<(), PartitionError> {
        let registered = self
            .by_name
            .get_mut(workflow)
            .filter(|r| r.remote)
            .ok_or_else(|| PartitionError::UnknownWorkflow(workflow.to_owned()))?;
        registered.builds.insert(build.to_owned());
        registered.active = Some(build.to_owned());
        Ok(())
    }

    /// Lets runs be started on a build of a remote workflow, without activating it: a draining one.
    pub fn allow_build(&mut self, workflow: &str, build: &str) {
        if let Some(registered) = self.by_name.get_mut(workflow).filter(|r| r.remote) {
            registered.builds.insert(build.to_owned());
        }
    }

    /// Stops runs being started on a build of a remote workflow.
    pub fn forbid_build(&mut self, workflow: &str, build: &str) {
        if let Some(registered) = self.by_name.get_mut(workflow) {
            registered.builds.remove(build);
        }
    }

    /// Makes a registered version the one new runs of the workflow start on.
    pub fn activate(&mut self, workflow: &str, version: &str) -> Result<(), PartitionError> {
        let registered = self
            .by_name
            .get_mut(workflow)
            .filter(|r| r.versions.contains_key(version))
            .ok_or_else(|| PartitionError::UnknownVersion {
                workflow: workflow.to_owned(),
                version: version.to_owned(),
            })?;
        registered.active = Some(version.to_owned());
        Ok(())
    }

    /// The version new runs of the workflow start on, if one is active.
    pub fn active_version(&self, workflow: &str) -> Option<&str> {
        self.by_name.get(workflow)?.active.as_deref()
    }

    /// The registered versions of a workflow.
    pub fn versions(&self, workflow: &str) -> Vec<&str> {
        self.by_name
            .get(workflow)
            .map(|r| r.versions.keys().map(String::as_str).collect())
            .unwrap_or_default()
    }

    /// The decider new runs of a workflow start with: the active version's, otherwise the unversioned
    /// one.
    pub fn get(&self, workflow: &str) -> Option<&(dyn Decider + Send + Sync)> {
        let registered = self.by_name.get(workflow)?;
        match &registered.active {
            Some(version) => registered.versions.get(version).map(|d| d.as_ref()),
            None => registered.unversioned.as_deref(),
        }
    }

    /// The decider for a run of `workflow` pinned to `version`: that version's, and nothing else. A run
    /// without a version gets the unversioned decider, or the active version's.
    pub fn for_run(
        &self,
        workflow: &str,
        version: Option<&str>,
    ) -> Option<&(dyn Decider + Send + Sync)> {
        let registered = self.by_name.get(workflow)?;
        match version {
            Some(version) => registered.versions.get(version).map(|d| d.as_ref()),
            None => registered.unversioned.as_deref().or_else(|| {
                let active = registered.active.as_ref()?;
                registered.versions.get(active).map(|d| d.as_ref())
            }),
        }
    }

    /// The version a new run is pinned to: `requested` if given (it must be registered), otherwise the
    /// active version, otherwise none (an unversioned workflow).
    fn version_for_start(
        &self,
        workflow: &str,
        requested: Option<&str>,
    ) -> Result<Option<String>, PartitionError> {
        let registered = self
            .by_name
            .get(workflow)
            .ok_or_else(|| PartitionError::UnknownWorkflow(workflow.to_owned()))?;
        match requested {
            Some(version)
                if registered.versions.contains_key(version)
                    || registered.builds.contains(version) =>
            {
                Ok(Some(version.to_owned()))
            }
            Some(version) => Err(PartitionError::UnknownVersion {
                workflow: workflow.to_owned(),
                version: version.to_owned(),
            }),
            None if registered.active.is_some() => Ok(registered.active.clone()),
            None if registered.unversioned.is_some() || registered.remote => Ok(None),
            None => Err(PartitionError::UnknownWorkflow(workflow.to_owned())),
        }
    }
}

impl std::fmt::Debug for Deciders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        for (name, registered) in &self.by_name {
            map.entry(
                name,
                &(registered.active.as_deref(), registered.versions.keys()),
            );
        }
        map.finish()
    }
}

/// How a step is retried after a retryable error (ADR 0012): exponential backoff, capped, up to
/// `max_attempts` attempts in total. The last failure becomes the step's terminal failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Attempts in total, including the first. 1 means no retries.
    pub max_attempts: u32,
    /// Backoff after the first failed attempt, in milliseconds.
    pub initial_backoff_ms: u64,
    /// Factor applied to the backoff after each further failure.
    pub multiplier: u32,
    /// Upper bound on the backoff, in milliseconds.
    pub max_backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 3,
            initial_backoff_ms: 1_000,
            multiplier: 2,
            max_backoff_ms: 60_000,
        }
    }
}

impl RetryPolicy {
    /// No retries: the first failure is terminal.
    pub fn none() -> Self {
        RetryPolicy {
            max_attempts: 1,
            ..RetryPolicy::default()
        }
    }

    /// The backoff after `failed` failed attempts (1 for the first failure).
    pub fn backoff_ms(&self, failed: u32) -> u64 {
        let exponent = failed.saturating_sub(1).min(32);
        let factor = u64::from(self.multiplier.max(1)).saturating_pow(exponent);
        self.initial_backoff_ms
            .saturating_mul(factor)
            .min(self.max_backoff_ms)
    }
}

/// A run held by a partition: its state and its journal.
#[derive(Debug)]
pub struct PartitionRun {
    state: RunState,
    journal: Vec<Entry>,
    chain: ChainWriter,
}

impl PartitionRun {
    /// The run's state.
    pub fn state(&self) -> &RunState {
        &self.state
    }

    /// The run's journal.
    pub fn journal(&self) -> &[Entry] {
        &self.journal
    }
}

/// A message the partition releases once the entries it depends on are durable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outbound {
    /// A start was accepted (or the run already existed).
    Started {
        /// The run key.
        key: String,
    },
    /// A step should be executed.
    Dispatch {
        /// The run key.
        key: String,
        /// The step's command index.
        cmd: CommandIndex,
        /// The step's name.
        name: String,
        /// The step's input.
        input: Payload,
    },
    /// An agent call should be executed.
    DispatchAgent {
        /// The run id.
        key: String,
        /// The call's command index.
        cmd: CommandIndex,
        /// The call's name.
        name: String,
        /// The request.
        request: Payload,
        /// The most tokens the call may use.
        budget_tokens: u64,
    },
    /// A step result was recorded (or is no longer needed); the sender can stop resending it.
    ResultRecorded {
        /// The run key.
        key: String,
        /// The command.
        cmd: CommandIndex,
    },
    /// A retryable failure report was processed (a retry was scheduled, the step failed terminally,
    /// or the report was no longer needed); the sender can stop resending it.
    AttemptRecorded {
        /// The run key.
        key: String,
        /// The command.
        cmd: CommandIndex,
    },
    /// A signal was recorded (or dropped because the run had ended).
    Signaled {
        /// The run key.
        key: String,
    },
    /// A cancellation was recorded (or dropped because the run had ended or was already cancelled).
    Cancelled {
        /// The run key.
        key: String,
    },
    /// An escalation's resolution was recorded (or dropped: already resolved, or the run ended).
    Resolved {
        /// The run key.
        key: String,
    },
    /// A run of a remote workflow needs a decision (ADR 0039): its logic, given the first `at` entries
    /// of its journal and `env`, returns its commands through [`Partition::decision`].
    Activation {
        /// The run id.
        key: String,
        /// The workflow.
        workflow: String,
        /// How many journal entries the decision is for.
        at: usize,
        /// The clock and seed for values the logic records.
        env: Env,
    },
    /// A run reached a terminal status.
    Finished {
        /// The run key.
        key: String,
        /// The terminal status.
        status: Status,
    },
    /// A message for the partition where `to` lives (ADR 0052): the host routes it by the slot table and
    /// hands it to that partition's [`Partition::receive`].
    Remote {
        /// The home of the message's target ([`crate::placement::home`]).
        to: String,
        /// The message.
        message: Remote,
    },
}

/// A message between partitions (ADR 0052). Each is safe to repeat; senders repeat until answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Remote {
    /// Start a parent's child under a key that lives elsewhere, or find the run the key has. Answered by
    /// [`Remote::ChildResult`] once the child has ended.
    StartChild {
        /// The parent's run id.
        parent: String,
        /// The parent's command.
        cmd: CommandIndex,
        /// The child's run key.
        child: String,
        /// The child's workflow.
        workflow: String,
        /// The child's input.
        input: Payload,
        /// The parent's priority, which the child takes.
        priority: u32,
    },
    /// A child's outcome, for a parent that waits for it; also the answer to [`Remote::CancelChild`].
    ChildResult {
        /// The parent's run id.
        parent: String,
        /// The parent's command.
        cmd: CommandIndex,
        /// The child's outcome.
        outcome: StepOutcome,
    },
    /// A parent was cancelled while it waited: cancel the child, unless another parent waits for it.
    CancelChild {
        /// The parent's run id.
        parent: String,
        /// The parent's command.
        cmd: CommandIndex,
        /// The child's run key.
        child: String,
    },
    /// Put a message to an object in its inbox (ADR 0044). Answered by [`Remote::Delivered`].
    Deliver {
        /// The sender's run id.
        sender: String,
        /// The sender's command.
        cmd: CommandIndex,
        /// The object.
        object: String,
        /// The object key, in the sender's namespace.
        key: String,
        /// The handler.
        handler: String,
        /// The handler's arguments (JSON).
        args: Payload,
    },
    /// A message is in its object's inbox, durably.
    Delivered {
        /// The sender's run id.
        sender: String,
        /// The sender's command.
        cmd: CommandIndex,
    },
}

impl Remote {
    /// The home of the message's target: where it must go.
    pub fn to(&self) -> String {
        use crate::placement::home;
        match self {
            Remote::StartChild { child, .. } | Remote::CancelChild { child, .. } => {
                home(child).to_owned()
            }
            Remote::ChildResult { parent, .. } => home(parent).to_owned(),
            Remote::Deliver { object, key, .. } => crate::objects::object_run_key(object, key),
            Remote::Delivered { sender, .. } => home(sender).to_owned(),
        }
    }
}

/// Errors from the partition.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PartitionError {
    /// The workflow is not registered.
    #[error("unknown workflow `{0}`")]
    UnknownWorkflow(String),
    /// The workflow has no version with this id.
    #[error("workflow `{workflow}` has no version `{version}`")]
    UnknownVersion {
        /// The workflow.
        workflow: String,
        /// The version asked for.
        version: String,
    },
    /// No run with this key exists.
    #[error("no run `{0}`")]
    UnknownRun(String),
    /// Storage failed. Fatal: drop the partition and recover from the log.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// The log does not hold a valid history. The partition must not start.
    #[error("corrupt log: {0}")]
    Corrupt(String),
    /// The core rejected an entry it produced itself: a bug.
    #[error("internal error: {0}")]
    Internal(String),
    /// A run with this key exists, with another workflow or input.
    #[error("run `{0}` exists with another workflow or input")]
    KeyConflict(String),
    /// A run with this key exists, and the start's policy is [`ReusePolicy::Reject`].
    #[error("run `{0}` already exists")]
    AlreadyStarted(String),
    /// Run keys must not be empty or contain NUL.
    #[error("invalid run key {0:?}")]
    InvalidKey(String),
    /// A resolution was refused: an answer supplied for an agent call does not match its output schema.
    #[error("{0}")]
    InvalidResolution(String),
    /// The request needs something not available for this run yet.
    #[error("{0}")]
    Unsupported(String),
}

/// What an update sent to a run came to, so far (ADR 0036).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpdateStatus {
    /// Answered: the logic's result, or why the update was rejected. An update the run cannot take
    /// (it has ended, or waits for no update of this name) and one the logic rejects as it receives it
    /// are rejected without being journaled.
    Answered(Result<Payload, String>),
    /// Accepted: the update is journaled as received by the wait `cmd` of run `run`, and the logic has
    /// not answered it yet. Its answer will be [`RunState::response`] for `cmd`; if the run ends
    /// without one, it never comes.
    Accepted {
        /// The run id.
        run: String,
        /// The wait that received it.
        cmd: CommandIndex,
    },
}

/// How a run is started.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StartOptions {
    /// What to do if the key already has a run.
    pub reuse: ReusePolicy,
    /// The run's token budget for agent calls, if it has one. An agent call that would exceed it is not
    /// made; it escalates for approval (reason [`aqueduct_core::BUDGET_ESCALATION`]).
    pub budget_tokens: Option<u64>,
    /// The version to start the run on; by default the workflow's active version (ADR 0029).
    pub version: Option<String>,
    /// The run's priority (ADR 0049): higher is taken first. Starting a key that already has a running
    /// run sets that run's priority, so a retried start that was lost before it was durable converges.
    /// `None` leaves it as it is: 0 for a new run, inherited for a child or a successor.
    pub priority: Option<u32>,
}

impl From<ReusePolicy> for StartOptions {
    fn from(reuse: ReusePolicy) -> Self {
        StartOptions {
            reuse,
            ..StartOptions::default()
        }
    }
}

/// What starting a key that already has a run does (BRIEF.md section 5.3).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReusePolicy {
    /// Return the existing run if it has the same workflow and input (a retried start); otherwise fail
    /// with [`PartitionError::KeyConflict`]. Makes starts idempotent.
    #[default]
    ReturnExisting,
    /// Fail with [`PartitionError::AlreadyStarted`] whenever the key has a run. A caller that retries
    /// its own start after a timeout then cannot tell whether its first attempt succeeded.
    Reject,
    /// Start a new run (the key's next generation) if the key's latest run has ended; while it is
    /// running, behave as [`ReusePolicy::ReturnExisting`]. A retried start after the run ended starts
    /// yet another run, so retries should use `ReturnExisting`.
    AllowAfterCompletion,
}

/// A partition processor over a log store.
pub struct Partition<L: LogStore, S: StateStore = NoStateStore> {
    log: L,
    state: S,
    /// Runs changed since the last checkpoint.
    dirty: BTreeSet<String>,
    deciders: Deciders,
    runs: BTreeMap<String, PartitionRun>,
    /// How many runs are running in each namespace, not counting object loops.
    running: BTreeMap<String, usize>,
    batch: Vec<Vec<u8>>,
    /// Entries of the batch for the observer: (run key, workflow, entry).
    observed: Vec<(String, String, Option<String>, Entry)>,
    observer: Option<std::sync::Arc<dyn crate::Observer>>,
    deferred: Vec<Outbound>,
    immediate: Vec<Outbound>,
    ack_before_sync: bool,
    now_ms: u64,
    /// The state of the generator that gives each activation its seed for recorded values.
    seed: u64,
    retry_policies: BTreeMap<String, RetryPolicy>,
    default_retry: RetryPolicy,
    /// Every timer that has not fired: (wake time, run, command).
    timers: BTreeSet<(u64, String, CommandIndex)>,
    /// Each run's entries in `timers`, to update the index when a run changes.
    run_timers: BTreeMap<String, Vec<(CommandIndex, u64)>>,
    evict_finished: bool,
    skip_successor_repair: bool,
    release_stale_dispatches: bool,
    /// Child runs that parents wait for, by the child's run key: (parent run id, command).
    children: BTreeMap<String, BTreeSet<(String, CommandIndex)>>,
    /// Effects raised while effects are being performed (a child's start, a parent's result): performed
    /// in turn by the outermost call, so a chain of children does not deepen the stack.
    queued: VecDeque<(String, Vec<Effect>)>,
    performing: bool,
    /// Which homes are this partition's (ADR 0052); all, without one.
    placement: Option<crate::placement::Placement>,
    /// Remote children parents wait for, by (parent run id, command): repeated until answered.
    remote_children: BTreeMap<(String, CommandIndex), Remote>,
    /// Cancellations of remote children, repeated until answered.
    remote_cancels: BTreeMap<(String, CommandIndex), Remote>,
    /// Messages to objects elsewhere, by (sender run id, command): repeated until delivered.
    remote_sends: BTreeMap<(String, CommandIndex), Remote>,
    /// Runs whose activation waits for their messages to be delivered.
    held_activations: BTreeSet<String>,
    /// Parents elsewhere waiting for this partition's runs, by the child's run key. In memory only:
    /// parents ask again.
    remote_waiters: BTreeMap<String, BTreeSet<(String, CommandIndex)>>,
}

impl<L: LogStore, S: StateStore> std::fmt::Debug for Partition<L, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Partition")
            .field("runs", &self.runs.len())
            .field("deciders", &self.deciders)
            .finish()
    }
}

/// Whether a run counts as running while it is: an object's loop waits for calls, whoever is calling.
fn counts_as_running(key: &str) -> bool {
    !key.starts_with("$object/")
}

fn count_running(runs: &BTreeMap<String, PartitionRun>) -> BTreeMap<String, usize> {
    let mut running = BTreeMap::new();
    for (key, run) in runs {
        if counts_as_running(key) && !run.state.status().is_terminal() {
            *running
                .entry(crate::namespaces::namespace_of(key).to_owned())
                .or_default() += 1;
        }
    }
    running
}

impl<L: LogStore> Partition<L> {
    /// Recovers a partition from its whole log and performs every run's recovery effects.
    pub fn recover(log: L, deciders: Deciders) -> Result<Partition<L>, PartitionError> {
        Partition::recover_with_state(log, NoStateStore, deciders)
    }

    /// Recovers runs from the whole log without performing their recovery effects (see
    /// [`Partition::resume`]).
    pub fn load(log: L, deciders: Deciders) -> Result<Partition<L>, PartitionError> {
        Partition::load_with_state(log, NoStateStore, deciders)
    }
}

impl<L: LogStore, S: StateStore> Partition<L, S> {
    /// Recovers a partition from its last checkpoint and the log after it, then performs every run's
    /// recovery effects.
    pub fn recover_with_state(
        log: L,
        state: S,
        deciders: Deciders,
    ) -> Result<Partition<L, S>, PartitionError> {
        let mut partition = Partition::load_with_state(log, state, deciders)?;
        partition.resume()?;
        Ok(partition)
    }

    /// Recovers runs from the last checkpoint and the log written after it, without performing their
    /// recovery effects (see [`Partition::resume`]).
    pub fn load_with_state(
        mut log: L,
        mut state: S,
        deciders: Deciders,
    ) -> Result<Partition<L, S>, PartitionError> {
        let checkpoint = state.load()?;
        let records = log.recover_from(checkpoint.lsn)?;
        // Each run's records: those in the checkpoint, then those logged after it.
        let mut by_run: BTreeMap<String, Vec<Vec<u8>>> = checkpoint.runs;
        // Runs with records after the checkpoint are not in it yet: the next checkpoint must save them,
        // because it moves past the log records that hold them.
        let mut dirty = BTreeSet::new();
        for bytes in &records {
            let (key, record) = decode_log_record(bytes).map_err(|e| {
                PartitionError::Corrupt(format!("a framed log record does not decode: {e}"))
            })?;
            if !dirty.contains(key) {
                dirty.insert(key.to_owned());
            }
            by_run
                .entry(key.to_owned())
                .or_default()
                .push(record.to_vec());
        }
        let mut runs = BTreeMap::new();
        for (key, records) in by_run {
            let verified = verify_chain(&key, records.iter().map(Vec::as_slice)).map_err(|e| {
                PartitionError::Corrupt(format!("run {key}: hash chain does not verify: {e}"))
            })?;
            let state = RunState::from_journal(&verified.entries).map_err(|e| {
                PartitionError::Corrupt(format!("run {key}: journal is invalid: {e}"))
            })?;
            runs.insert(
                key,
                PartitionRun {
                    state,
                    journal: verified.entries,
                    chain: ChainWriter::resume(verified.head),
                },
            );
        }
        let running = count_running(&runs);
        Ok(Partition {
            log,
            state,
            dirty,
            deciders,
            runs,
            running,
            batch: Vec::new(),
            observed: Vec::new(),
            observer: None,
            deferred: Vec::new(),
            immediate: Vec::new(),
            ack_before_sync: false,
            now_ms: 0,
            seed: os_seed(),
            retry_policies: BTreeMap::new(),
            default_retry: RetryPolicy::default(),
            timers: BTreeSet::new(),
            run_timers: BTreeMap::new(),
            evict_finished: false,
            skip_successor_repair: false,
            release_stale_dispatches: false,
            children: BTreeMap::new(),
            queued: VecDeque::new(),
            performing: false,
            placement: None,
            remote_children: BTreeMap::new(),
            remote_cancels: BTreeMap::new(),
            remote_sends: BTreeMap::new(),
            held_activations: BTreeSet::new(),
            remote_waiters: BTreeMap::new(),
        })
        .map(|mut partition| {
            let keys: Vec<String> = partition.runs.keys().cloned().collect();
            for key in keys {
                partition.refresh_timers(&key);
                partition.index_children(&key);
            }
            partition
        })
    }

    /// Performs the recovery effects of every run: activation and re-dispatch.
    pub fn resume(&mut self) -> Result<(), PartitionError> {
        let keys: Vec<String> = self.runs.keys().cloned().collect();
        for key in &keys {
            let effects = match self.runs.get(key) {
                Some(run) => run.state.recovery_effects(),
                None => continue,
            };
            self.perform(key, effects)?;
        }
        if self.skip_successor_repair {
            return Ok(());
        }
        // A run that continued as new and its successor's start are separate records of one batch: a
        // crash can keep the first and tear off the second. The successor is fully determined by the
        // journaled `ContinuedAsNew`, so it is started again.
        for key in &keys {
            let Some(run) = self.runs.get(key) else {
                continue;
            };
            let Some(input) = run.state.continued_with().cloned() else {
                continue;
            };
            let workflow = run.state.workflow().to_owned();
            let budget = successor_budget(&run.state);
            let priority = run.state.priority();
            let (base, generation) = split_run_id(key);
            let next = run_id(base, generation + 1);
            if !self.runs.contains_key(&next) && self.lookup_run(&next)?.is_none() {
                let version = self.successor_version(&workflow);
                self.create_run(&next, &workflow, input, budget, version, priority)?;
            }
        }
        Ok(())
    }

    /// Starts a run, or acknowledges an existing one with the same key (reuse policy: return existing).
    pub fn start(
        &mut self,
        key: &str,
        workflow: &str,
        input: Payload,
    ) -> Result<(), PartitionError> {
        self.start_with(key, workflow, input, ReusePolicy::ReturnExisting)
    }

    /// Starts a run, or applies `policy` if the key already has one.
    pub fn start_with(
        &mut self,
        key: &str,
        workflow: &str,
        input: Payload,
        options: impl Into<StartOptions>,
    ) -> Result<(), PartitionError> {
        self.start_run(key, workflow, input, options.into())?;
        self.send(Outbound::Started {
            key: key.to_owned(),
        });
        Ok(())
    }

    /// Starts a run, or applies the reuse policy if the key already has one, without acknowledging it.
    fn start_run(
        &mut self,
        key: &str,
        workflow: &str,
        input: Payload,
        options: StartOptions,
    ) -> Result<(), PartitionError> {
        let StartOptions {
            reuse: policy,
            budget_tokens,
            version,
            priority,
        } = options;
        if !valid_key(key) {
            return Err(PartitionError::InvalidKey(key.to_owned()));
        }
        // A finished run may have been unloaded; it still exists.
        let next = match self.latest(key)? {
            None => Some(0),
            Some(latest) => {
                let state = self
                    .lookup_run(&latest)?
                    .ok_or_else(|| PartitionError::Internal(format!("run {latest:?} vanished")))?;
                match policy {
                    ReusePolicy::Reject => {
                        return Err(PartitionError::AlreadyStarted(key.to_owned()));
                    }
                    ReusePolicy::AllowAfterCompletion if state.status().is_terminal() => {
                        Some(split_run_id(&latest).1 + 1)
                    }
                    _ => {
                        // A retried start: the run its first attempt started heads the latest chain of
                        // runs (one continued as new is still the same chain).
                        let head = self.chain_head(&latest)?;
                        if head.workflow() != workflow || *head.input() != input {
                            return Err(PartitionError::KeyConflict(key.to_owned()));
                        }
                        if let Some(priority) = priority {
                            self.set_run_priority(&latest, priority)?;
                        }
                        None
                    }
                }
            }
        };
        if let Some(generation) = next {
            let version = self
                .deciders
                .version_for_start(workflow, version.as_deref())?;
            self.create_run(
                &run_id(key, generation),
                workflow,
                input,
                budget_tokens,
                version,
                priority.unwrap_or(0),
            )?;
        }
        Ok(())
    }

    /// Indexes the children a run waits for.
    fn index_children(&mut self, id: &str) {
        let Some(run) = self.runs.get(id) else {
            return;
        };
        if run.state.status().is_terminal() {
            return;
        }
        let waiting: Vec<(String, CommandIndex)> = run
            .state
            .steps()
            .iter()
            .zip(0u32..)
            .filter(|(step, _)| step.kind == CommandKind::Child && step.outcome.is_none())
            .filter_map(|(step, index)| {
                let cmd = CommandIndex(index);
                let child = match &step.child_key {
                    // A key logic may not choose failed when it was started.
                    Some(key) => crate::namespaces::chosen_key(id, key)?,
                    None => child_key(id, cmd),
                };
                Some((child, cmd))
            })
            .collect();
        for (child, cmd) in waiting {
            self.children
                .entry(child)
                .or_default()
                .insert((id.to_owned(), cmd));
        }
    }

    /// Starts the child run of command `cmd` of run `parent` (or finds the run its key has), and
    /// delivers its outcome if it has already ended. A child that cannot be started fails the command.
    fn start_child(
        &mut self,
        parent: &str,
        cmd: CommandIndex,
        workflow: &str,
        input: Payload,
        key: Option<String>,
    ) -> Result<(), PartitionError> {
        let child = match key {
            None => child_key(parent, cmd),
            Some(key) => match crate::namespaces::chosen_key(parent, &key) {
                Some(child) => child,
                None => {
                    let error = format!("a child run cannot have the key `{key}`");
                    return self.deliver_child(parent, cmd, StepOutcome::Failed(error));
                }
            },
        };
        // An object's calls live with its loop.
        if !child.starts_with("$call/") && !self.is_local(&child) {
            let message = Remote::StartChild {
                parent: parent.to_owned(),
                cmd,
                child,
                workflow: workflow.to_owned(),
                input,
                priority: self.runs.get(parent).map_or(0, |run| run.state.priority()),
            };
            self.remote_children
                .insert((parent.to_owned(), cmd), message.clone());
            self.send_remote(message);
            return Ok(());
        }
        self.children
            .entry(child.clone())
            .or_default()
            .insert((parent.to_owned(), cmd));
        // A child is as urgent as the run waiting for it.
        let options = StartOptions {
            priority: Some(self.runs.get(parent).map_or(0, |run| run.state.priority())),
            ..StartOptions::default()
        };
        match self.start_run(&child, workflow, input, options) {
            Ok(()) => {}
            Err(
                e @ (PartitionError::UnknownWorkflow(_)
                | PartitionError::UnknownVersion { .. }
                | PartitionError::KeyConflict(_)
                | PartitionError::AlreadyStarted(_)
                | PartitionError::InvalidKey(_)),
            ) => {
                self.forget_child(&child, parent, cmd);
                let error = format!("child run `{child}` could not be started: {e}");
                return self.deliver_child(parent, cmd, StepOutcome::Failed(error));
            }
            Err(e) => return Err(e),
        }
        if let Some(outcome) = self.child_outcome(&child)? {
            self.forget_child(&child, parent, cmd);
            self.deliver_child(parent, cmd, outcome)?;
        }
        Ok(())
    }

    /// The outcome of a child key's run, if it has ended: its output or its failure. A run continued as
    /// new has not ended (its successor carries on), and a parked one may still be repaired.
    fn child_outcome(&mut self, child: &str) -> Result<Option<StepOutcome>, PartitionError> {
        let Some(state) = self.lookup(child)? else {
            return Ok(None);
        };
        Ok(match state.status() {
            Status::Completed => Some(StepOutcome::Completed(
                state.output().cloned().unwrap_or_default(),
            )),
            Status::Failed => Some(StepOutcome::Failed(
                state.error().unwrap_or_default().to_owned(),
            )),
            Status::Running | Status::Parked | Status::Continued => None,
        })
    }

    fn forget_child(&mut self, child: &str, parent: &str, cmd: CommandIndex) {
        if let Some(waiting) = self.children.get_mut(child) {
            waiting.remove(&(parent.to_owned(), cmd));
            if waiting.is_empty() {
                self.children.remove(child);
            }
        }
    }

    /// Records a child's outcome in its parent, if the parent still waits for it.
    fn deliver_child(
        &mut self,
        parent: &str,
        cmd: CommandIndex,
        outcome: StepOutcome,
    ) -> Result<(), PartitionError> {
        let Some(run) = self.runs.get(parent) else {
            return Ok(());
        };
        let transition = run
            .state
            .handle_at(Event::ChildResult { cmd, outcome }, self.now_ms);
        let effects = self.append(parent, transition)?;
        self.perform(parent, effects)
    }

    /// A run ended: the parents waiting for its key get its outcome.
    fn child_ended(&mut self, id: &str) -> Result<(), PartitionError> {
        let key = split_run_id(id).0;
        if self.remote_waiters.contains_key(key)
            && let Some(outcome) = self.child_outcome(key)?
        {
            for (parent, cmd) in self.remote_waiters.remove(key).unwrap_or_default() {
                self.send_remote(Remote::ChildResult {
                    parent,
                    cmd,
                    outcome: outcome.clone(),
                });
            }
        }
        if !self.children.contains_key(key) {
            return Ok(());
        }
        let Some(outcome) = self.child_outcome(key)? else {
            return Ok(());
        };
        for (parent, cmd) in self.children.remove(key).unwrap_or_default() {
            self.deliver_child(&parent, cmd, outcome.clone())?;
        }
        Ok(())
    }

    /// The parent was cancelled while it waited for this child: the child is cancelled too, unless
    /// another parent still waits for it (a child shared under a chosen key).
    fn cancel_child(&mut self, parent: &str, cmd: CommandIndex) -> Result<(), PartitionError> {
        let Some(step) = self
            .runs
            .get(parent)
            .and_then(|run| run.state.steps().get(usize::try_from(cmd.0).ok()?))
        else {
            return Ok(());
        };
        let child = match &step.child_key {
            Some(key) => match crate::namespaces::chosen_key(parent, key) {
                Some(child) => child,
                None => return Ok(()),
            },
            None => child_key(parent, cmd),
        };
        self.forget_child(&child, parent, cmd);
        if !child.starts_with("$call/") && !self.is_local(&child) {
            self.remote_children.remove(&(parent.to_owned(), cmd));
            let message = Remote::CancelChild {
                parent: parent.to_owned(),
                cmd,
                child,
            };
            self.remote_cancels
                .insert((parent.to_owned(), cmd), message.clone());
            self.send_remote(message);
            return Ok(());
        }
        self.cancel_unwaited(&child, parent)
    }

    /// Cancels a child key's latest run, if no parent waits for it any more.
    fn cancel_unwaited(&mut self, child: &str, parent: &str) -> Result<(), PartitionError> {
        if self.children.contains_key(child) || self.remote_waiters.contains_key(child) {
            return Ok(());
        }
        let Some(id) = self.latest(child)? else {
            return Ok(());
        };
        let Some(run) = self.runs.get(&id) else {
            return Ok(());
        };
        let event = Event::Cancel {
            reason: format!("its parent run `{parent}` was cancelled"),
        };
        let transition = run.state.handle_at(event, self.now_ms);
        let effects = self.append(&id, transition)?;
        self.perform(&id, effects)
    }

    /// Creates a run with this run id, logs its start and activates it.
    fn create_run(
        &mut self,
        id: &str,
        workflow: &str,
        input: Payload,
        budget: Option<u64>,
        version: Option<String>,
        priority: u32,
    ) -> Result<(), PartitionError> {
        let mut first = start_version(workflow, input, budget, version);
        // In the same batch as the start: a run is never acknowledged without its priority.
        if priority != 0 {
            first.entries.push(Entry::PrioritySet { priority });
        }
        let state = RunState::from_journal(&first.entries)
            .map_err(|e| PartitionError::Internal(e.to_string()))?;
        self.runs.insert(
            id.to_owned(),
            PartitionRun {
                state,
                journal: Vec::new(),
                chain: ChainWriter::new(id),
            },
        );
        if counts_as_running(id)
            && self
                .runs
                .get(id)
                .is_some_and(|run| !run.state.status().is_terminal())
        {
            *self
                .running
                .entry(crate::namespaces::namespace_of(id).to_owned())
                .or_default() += 1;
        }
        for entry in &first.entries {
            self.log_entry(id, entry)?;
        }
        self.perform(id, first.effects)
    }

    /// The version a run continued as new starts on: the workflow's active version (how a long-lived run
    /// moves to new code), or none for an unversioned workflow.
    fn successor_version(&self, workflow: &str) -> Option<String> {
        self.deciders.active_version(workflow).map(str::to_owned)
    }

    /// The id of the latest run (highest generation) of `key`, if it has any run.
    pub fn latest(&mut self, key: &str) -> Result<Option<String>, PartitionError> {
        let prefix = format!("{key}\0");
        let mut best: Option<(u32, bool)> = None;
        for (id, run) in self
            .runs
            .range::<str, _>((std::ops::Bound::Included(key), std::ops::Bound::Unbounded))
            .take_while(|(id, _)| id.as_str() == key || id.starts_with(&prefix))
        {
            let generation = split_run_id(id).1;
            if best.is_none_or(|(b, _)| generation > b) {
                best = Some((generation, run.state.status().is_terminal()));
            }
        }
        // A running run is always the latest: a new generation starts only after it ends.
        if let Some((generation, false)) = best {
            return Ok(Some(run_id(key, generation)));
        }
        let saved = self.state.generations(key)?.into_iter().max();
        let latest = best.map(|(g, _)| g).max(saved);
        Ok(latest.map(|generation| run_id(key, generation)))
    }

    /// The first run of the chain of runs (continued as new) that ends with run `id`.
    fn chain_head(&mut self, id: &str) -> Result<RunState, PartitionError> {
        let (key, mut generation) = split_run_id(id);
        let key = key.to_owned();
        let mut head = self
            .lookup_run(id)?
            .ok_or_else(|| PartitionError::Internal(format!("run {id:?} vanished")))?;
        while generation > 0 {
            match self.lookup_run(&run_id(&key, generation - 1))? {
                Some(previous) if previous.status() == Status::Continued => {
                    head = previous;
                    generation -= 1;
                }
                _ => break,
            }
        }
        Ok(head)
    }

    /// Sets the partition's clock (milliseconds since the Unix epoch). New timers wake relative to it,
    /// and [`Partition::fire_due_timers`] fires timers whose wake time has been reached.
    pub fn set_time(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    /// Seeds the generator of the random bits offered to deciders for recorded values (by default
    /// seeded from the operating system). A simulation sets it to stay reproducible.
    pub fn set_seed(&mut self, seed: u64) {
        self.seed = seed;
    }

    /// The seed for the next activation (SplitMix64).
    fn next_seed(&mut self) -> u64 {
        self.seed = self.seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.seed;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Asks a run to stop (see [`Event::Cancel`]): every command it is waiting for fails as cancelled,
    /// and the run decides what to do (undo work, then fail). Only the first cancellation counts; later
    /// ones, and cancellations of ended runs, are acknowledged and dropped.
    pub fn cancel(&mut self, key: &str, reason: &str) -> Result<(), PartitionError> {
        let id = self
            .latest(key)?
            .ok_or_else(|| PartitionError::UnknownRun(key.to_owned()))?;
        if let Some(run) = self.runs.get(&id) {
            let event = Event::Cancel {
                reason: reason.to_owned(),
            };
            let transition = run.state.handle_at(event, self.now_ms);
            let effects = self.append(&id, transition)?;
            self.perform(&id, effects)?;
        }
        self.send(Outbound::Cancelled {
            key: key.to_owned(),
        });
        Ok(())
    }

    /// Sets the priority of a key's latest run (ADR 0049), journaled. A run that has ended is left as it
    /// is. Returns the run id, if the run is running.
    pub fn set_priority(
        &mut self,
        key: &str,
        priority: u32,
    ) -> Result<Option<String>, PartitionError> {
        let id = self
            .latest(key)?
            .ok_or_else(|| PartitionError::UnknownRun(key.to_owned()))?;
        Ok(self.set_run_priority(&id, priority)?.then_some(id))
    }

    fn set_run_priority(&mut self, id: &str, priority: u32) -> Result<bool, PartitionError> {
        let Some(run) = self.runs.get(id) else {
            return Ok(false);
        };
        if run.state.status().is_terminal() {
            return Ok(false);
        }
        let transition = run.state.set_priority(priority);
        self.append(id, transition)?;
        Ok(true)
    }

    /// A run's priority: 0 for a run this partition does not have in memory.
    pub fn priority(&self, key: &str) -> u32 {
        self.runs.get(key).map_or(0, |run| run.state.priority())
    }

    /// Resolves an escalation of the run with run id `key` (an exact run, not the key's latest); the run
    /// continues with `outcome`. Only the first resolution counts; later ones, and resolutions of ended
    /// runs, are acknowledged and dropped.
    pub fn resolve_escalation(
        &mut self,
        key: &str,
        cmd: CommandIndex,
        outcome: Payload,
    ) -> Result<(), PartitionError> {
        let Some(run) = self.runs.get(key) else {
            if self.lookup_run(key)?.is_none() {
                return Err(PartitionError::UnknownRun(key.to_owned()));
            }
            self.send(Outbound::Resolved {
                key: key.to_owned(),
            });
            return Ok(());
        };
        // An answer supplied for an agent call held for its schema must match that schema.
        if let Some(step) = usize::try_from(cmd.0)
            .ok()
            .and_then(|index| run.state.steps().get(index))
            && matches!(step.held, Some(aqueduct_core::Hold::Answer(_)))
            && let Some(schema) = crate::schema::output_schema(&step.input)
            && let Err(crate::schema::SchemaError::Mismatch(error)) =
                crate::schema::check(&schema, &outcome)
        {
            return Err(PartitionError::InvalidResolution(format!(
                "the answer does not match the output schema of `{}`: {error}",
                step.name
            )));
        }
        let transition = run
            .state
            .handle_at(Event::ResolveEscalation { cmd, outcome }, self.now_ms);
        let effects = self.append(key, transition)?;
        self.perform(key, effects)?;
        self.send(Outbound::Resolved {
            key: key.to_owned(),
        });
        Ok(())
    }

    /// Every open escalation, by run key: `(key, command, reason, payload)`. Runs with an open
    /// escalation are running, so they are all in memory.
    pub fn open_escalations(&self) -> Vec<(String, CommandIndex, String, Payload)> {
        self.runs
            .iter()
            .flat_map(|(key, run)| {
                run.state
                    .open_escalations()
                    .into_iter()
                    .map(|(cmd, reason, payload)| {
                        (key.clone(), cmd, reason.to_owned(), payload.clone())
                    })
            })
            .collect()
    }

    /// Sends a signal to a run. A signal whose non-empty `id` the run has already recorded is dropped,
    /// as are signals to a run that has ended; both are still acknowledged.
    pub fn signal(
        &mut self,
        key: &str,
        name: &str,
        id: &str,
        payload: Payload,
    ) -> Result<(), PartitionError> {
        let run = self
            .latest(key)?
            .ok_or_else(|| PartitionError::UnknownRun(key.to_owned()))?;
        self.deliver_signal(&run, name, id, payload)?;
        self.send(Outbound::Signaled {
            key: key.to_owned(),
        });
        Ok(())
    }

    /// Sends a signal to the run with this run id: an exact run, not the key's latest (for awakeables,
    /// whose ids name their run). Acknowledged with the run id.
    pub fn signal_run(
        &mut self,
        run: &str,
        name: &str,
        id: &str,
        payload: Payload,
    ) -> Result<(), PartitionError> {
        if !self.runs.contains_key(run) && self.lookup_run(run)?.is_none() {
            return Err(PartitionError::UnknownRun(run.to_owned()));
        }
        self.deliver_signal(run, name, id, payload)?;
        self.send(Outbound::Signaled {
            key: run.to_owned(),
        });
        Ok(())
    }

    /// Sends an update to a key's latest run: a request that its logic answers (ADR 0036). It is
    /// delivered to the run's oldest wait for an update of this name; a run that has ended, or waits
    /// for none, rejects it. Before it is journaled, the logic's reaction is tried on a copy of the run:
    /// if the logic rejects the update as it receives it (validation), nothing is journaled and the
    /// rejection is the answer. Otherwise the update and that reaction are journaled. An update whose
    /// non-empty `id` the run has already received is not delivered again; it gets the first one's
    /// answer.
    pub fn update(
        &mut self,
        key: &str,
        name: &str,
        id: &str,
        payload: Payload,
    ) -> Result<UpdateStatus, PartitionError> {
        let run = self
            .latest(key)?
            .ok_or_else(|| PartitionError::UnknownRun(key.to_owned()))?;
        let ended = || {
            Ok(UpdateStatus::Answered(Err(format!(
                "run `{key}` has ended"
            ))))
        };
        let Some(state) = self.runs.get(&run).map(|r| &r.state) else {
            // Not in memory: it has ended. A retried update may still have its answer there.
            let state = self
                .lookup_run(&run)?
                .ok_or_else(|| PartitionError::UnknownRun(key.to_owned()))?;
            return match (!id.is_empty())
                .then(|| state.update_with_id(id))
                .flatten()
                .and_then(|cmd| state.response(cmd))
            {
                Some(answer) => Ok(UpdateStatus::Answered(answer.clone())),
                None => ended(),
            };
        };
        if let Some(cmd) = (!id.is_empty()).then(|| state.update_with_id(id)).flatten() {
            return match state.response(cmd) {
                Some(answer) => Ok(UpdateStatus::Answered(answer.clone())),
                None if state.status().is_terminal() => ended(),
                None => Ok(UpdateStatus::Accepted { run, cmd }),
            };
        }
        if state.status().is_terminal() {
            return ended();
        }
        let Some(cmd) = state.awaiting_update(name) else {
            return Ok(UpdateStatus::Answered(Err(format!(
                "run `{key}` is not waiting for update `{name}`"
            ))));
        };
        let event = Event::Update {
            name: name.to_owned(),
            id: id.to_owned(),
            payload,
        };
        let received = state.handle_at(event, self.now_ms);
        let env = Env {
            now_ms: self.now_ms,
            seed: self.next_seed(),
        };
        let state = &self.runs[&run].state;
        // The logic's reaction, tried on a copy: a rejection here leaves no trace in the journal.
        let reaction = match self.deciders.for_run(state.workflow(), state.version()) {
            Some(decider) => {
                let mut trial = state.clone();
                for entry in &received.entries {
                    trial.apply(entry).map_err(|e| {
                        PartitionError::Internal(format!(
                            "run {run}: the core produced an invalid entry: {e}"
                        ))
                    })?;
                }
                let commands = decider.decide(&trial.view_at(&run, env));
                Some(trial.handle_at(Event::Decision(commands), self.now_ms))
            }
            None => None,
        };
        let rejected = reaction.as_ref().and_then(|reaction| {
            reaction.entries.iter().find_map(|entry| match entry {
                Entry::UpdateRejected { update, error, .. } if *update == cmd => {
                    Some(error.clone())
                }
                _ => None,
            })
        });
        if let Some(error) = rejected {
            return Ok(UpdateStatus::Answered(Err(error)));
        }
        match reaction {
            // The reaction was computed for the run with the update received: journal both, so the
            // logic is not asked twice.
            Some(reaction) => {
                let received = Transition {
                    entries: received.entries,
                    effects: Vec::new(),
                };
                self.append(&run, received)?;
                let effects = self.append(&run, reaction)?;
                self.perform(&run, effects)?;
            }
            None => {
                let effects = self.append(&run, received)?;
                self.perform(&run, effects)?;
            }
        }
        Ok(UpdateStatus::Accepted { run, cmd })
    }

    /// Puts a call to an object's handler in the object key's inbox (ADR 0043), starting the key's loop if
    /// it has none, and returns the call's run key: the call's outcome is that run's. A call whose id
    /// has a run already, or is already in the inbox, is not delivered again.
    pub fn call_object(
        &mut self,
        object: &str,
        key: &str,
        handler: &str,
        id: &str,
        args: &serde_json::Value,
    ) -> Result<String, PartitionError> {
        use crate::objects::{INVOKE, call_key, handler_workflow, loop_workflow, object_run_key};
        let workflow = handler_workflow(object, handler);
        if self.deciders.get(&workflow).is_none() && !self.deciders.is_remote(&workflow) {
            return Err(PartitionError::UnknownWorkflow(workflow));
        }
        let call = call_key(object, key, id);
        if !valid_key(&call) || !crate::objects::valid_object_key(object, key) {
            return Err(PartitionError::InvalidKey(call));
        }
        if self.latest(&call)?.is_some() {
            return Ok(call);
        }
        let run = object_run_key(object, key);
        self.start_with(
            &run,
            &loop_workflow(object),
            Payload::new(*b"{\"state\":{}}"),
            StartOptions::default(),
        )?;
        let message = serde_json::json!({ "handler": handler, "key": call, "args": args });
        self.signal(
            &run,
            INVOKE,
            &call,
            Payload::new(message.to_string().into_bytes()),
        )?;
        Ok(call)
    }

    /// A remote workflow's decision (ADR 0039): the commands its logic issued, given the first `at`
    /// entries of run `key`'s journal. A decision for another length is stale (the run has moved on, or
    /// it is a repeat): it is dropped, and the run is activated again for where it is now. Decisions for
    /// runs that have ended or are not in memory are dropped.
    pub fn decision(
        &mut self,
        key: &str,
        at: usize,
        commands: Vec<aqueduct_core::Command>,
    ) -> Result<(), PartitionError> {
        let Some(run) = self.runs.get(key) else {
            return Ok(());
        };
        if run.state.status().is_terminal() || !self.deciders.is_remote(run.state.workflow()) {
            return Ok(());
        }
        if run.state.len() != at {
            let message = Outbound::Activation {
                key: key.to_owned(),
                workflow: run.state.workflow().to_owned(),
                at: run.state.len(),
                env: Env {
                    now_ms: self.now_ms,
                    seed: self.next_seed(),
                },
            };
            self.send(message);
            return Ok(());
        }
        let transition = run.state.handle_at(Event::Decision(commands), self.now_ms);
        let effects = self.append(key, transition)?;
        self.perform(key, effects)
    }

    /// Answers query `name` about a key's latest run, from its logic (ADR 0036): `None` if the logic
    /// has no such query. Nothing is journaled. The run is read as it is in memory, which may include
    /// entries not yet durable: a caller that must not show those answers after the next flush.
    pub fn query(&mut self, key: &str, name: &str) -> Result<Option<Payload>, PartitionError> {
        let run = self
            .latest(key)?
            .ok_or_else(|| PartitionError::UnknownRun(key.to_owned()))?;
        let state = self
            .lookup_run(&run)?
            .ok_or_else(|| PartitionError::UnknownRun(key.to_owned()))?;
        if self.deciders.is_remote(state.workflow()) {
            return Err(PartitionError::Unsupported(format!(
                "queries on `{}`, whose logic runs in a remote workflow worker, are not supported yet",
                state.workflow()
            )));
        }
        let decider = self
            .deciders
            .for_run(state.workflow(), state.version())
            .ok_or_else(|| match state.version() {
                Some(version) => PartitionError::UnknownVersion {
                    workflow: state.workflow().to_owned(),
                    version: version.to_owned(),
                },
                None => PartitionError::UnknownWorkflow(state.workflow().to_owned()),
            })?;
        let env = Env {
            now_ms: self.now_ms,
            seed: 0,
        };
        Ok(decider.query(&state.view_at(&run, env), name))
    }

    /// Records a signal for a run in memory; a run that is not in memory has ended and drops it.
    fn deliver_signal(
        &mut self,
        run: &str,
        name: &str,
        id: &str,
        payload: Payload,
    ) -> Result<(), PartitionError> {
        let Some(state) = self.runs.get(run).map(|r| &r.state) else {
            return Ok(());
        };
        let event = Event::Signal {
            name: name.to_owned(),
            id: id.to_owned(),
            payload,
        };
        let transition = state.handle_at(event, self.now_ms);
        let effects = self.append(run, transition)?;
        self.perform(run, effects)
    }

    /// Fires every timer whose wake time is at or before the partition's clock. Timers never fire early.
    pub fn fire_due_timers(&mut self) -> Result<(), PartitionError> {
        let due: Vec<(String, CommandIndex)> = self
            .timers
            .iter()
            .take_while(|(wake_at, _, _)| *wake_at <= self.now_ms)
            .map(|(_, key, cmd)| (key.clone(), *cmd))
            .collect();
        for (key, cmd) in due {
            let Some(run) = self.runs.get(&key) else {
                continue;
            };
            let transition = run.state.handle_at(Event::TimerFired { cmd }, self.now_ms);
            let effects = self.append(&key, transition)?;
            self.perform(&key, effects)?;
        }
        Ok(())
    }

    /// The earliest wake time of any timer that has not fired.
    pub fn next_timer(&self) -> Option<u64> {
        self.timers.first().map(|(wake_at, _, _)| *wake_at)
    }

    /// The number of pending timers.
    pub fn pending_timers(&self) -> usize {
        self.timers.len()
    }

    /// Unloads finished runs once a checkpoint holds them, so memory holds only active runs. Lookups
    /// ([`Partition::lookup`]), repeated starts and signals fall back to the state store.
    pub fn set_evict_finished(&mut self, evict: bool) {
        self.evict_finished = evict;
        self.evict();
    }

    /// The run ids of every run: those in memory and those only in the state store (finished runs
    /// unloaded after a checkpoint), in order.
    pub fn run_ids(&mut self) -> Result<Vec<String>, PartitionError> {
        let mut ids: BTreeSet<String> = self.runs.keys().cloned().collect();
        ids.extend(self.state.run_ids()?);
        Ok(ids.into_iter().collect())
    }

    /// The state of a key's latest run, from memory or, for an unloaded finished run, from the state
    /// store.
    pub fn lookup(&mut self, key: &str) -> Result<Option<RunState>, PartitionError> {
        match self.latest(key)? {
            Some(id) => self.lookup_run(&id),
            None => Ok(None),
        }
    }

    /// The state of the run with this run id, from memory or the state store.
    pub fn lookup_run(&mut self, key: &str) -> Result<Option<RunState>, PartitionError> {
        if let Some(run) = self.runs.get(key) {
            return Ok(Some(run.state.clone()));
        }
        let Some(records) = self.state.load_run(key)? else {
            return Ok(None);
        };
        let verified = verify_chain(key, records.iter().map(Vec::as_slice)).map_err(|e| {
            PartitionError::Corrupt(format!("run {key}: hash chain does not verify: {e}"))
        })?;
        let state = RunState::from_journal(&verified.entries)
            .map_err(|e| PartitionError::Corrupt(format!("run {key}: journal is invalid: {e}")))?;
        Ok(Some(state))
    }

    fn evict(&mut self) {
        if !self.evict_finished {
            return;
        }
        let finished: Vec<String> = self
            .runs
            .iter()
            .filter(|(key, run)| run.state.status().is_terminal() && !self.dirty.contains(*key))
            .map(|(key, _)| key.clone())
            .collect();
        for key in finished {
            self.runs.remove(&key);
            self.refresh_timers(&key);
        }
    }

    /// Re-indexes one run's pending timers.
    fn refresh_timers(&mut self, key: &str) {
        for (cmd, wake_at) in self.run_timers.remove(key).unwrap_or_default() {
            self.timers.remove(&(wake_at, key.to_owned(), cmd));
        }
        let Some(run) = self.runs.get(key) else {
            return;
        };
        let pending = run.state.timers();
        for (cmd, wake_at) in &pending {
            self.timers.insert((*wake_at, key.to_owned(), *cmd));
        }
        if !pending.is_empty() {
            self.run_timers.insert(key.to_owned(), pending);
        }
    }

    /// Sets the retry policy for one step name.
    pub fn set_retry_policy(&mut self, step: &str, policy: RetryPolicy) {
        self.retry_policies.insert(step.to_owned(), policy);
    }

    /// Sets the retry policy for steps without their own.
    pub fn set_default_retry_policy(&mut self, policy: RetryPolicy) {
        self.default_retry = policy;
    }

    /// Records that an attempt of a step failed with a retryable error. While the step's policy allows
    /// another attempt, a durable retry is scheduled; otherwise the step fails terminally.
    ///
    /// Known limitation: reports carry no attempt number, so a delayed duplicate report of an earlier
    /// attempt can count as one more failure.
    pub fn step_attempt_failed(
        &mut self,
        key: &str,
        cmd: CommandIndex,
        error: String,
    ) -> Result<(), PartitionError> {
        let Some(run) = self.runs.get(key) else {
            return Ok(());
        };
        let step = usize::try_from(cmd.0)
            .ok()
            .and_then(|i| run.state.steps().get(i));
        let Some(step) = step else {
            return Ok(());
        };
        let policy = self
            .retry_policies
            .get(&step.name)
            .copied()
            .unwrap_or(self.default_retry);
        let attempt = step.failed_attempts.saturating_add(1);
        if attempt >= policy.max_attempts {
            self.step_result(key, cmd, StepOutcome::Failed(error))?;
        } else {
            let event = Event::StepAttemptFailed {
                cmd,
                error,
                retry_after_ms: policy.backoff_ms(attempt),
            };
            let transition = run.state.handle_at(event, self.now_ms);
            let effects = self.append(key, transition)?;
            self.perform(key, effects)?;
        }
        self.send(Outbound::AttemptRecorded {
            key: key.to_owned(),
            cmd,
        });
        Ok(())
    }

    /// Records an agent call's result: the model's response, or a terminal failure. Recorded at most
    /// once; acknowledged with [`Outbound::ResultRecorded`] like a step result.
    pub fn agent_result(
        &mut self,
        key: &str,
        cmd: CommandIndex,
        outcome: Result<aqueduct_core::AgentResponse, String>,
    ) -> Result<(), PartitionError> {
        let mut rejected = false;
        if let Some(run) = self.runs.get(key) {
            let event = self.check_answer(&run.state, cmd, outcome);
            // An answer refused by its schema is processed like a failed attempt, not recorded.
            rejected = matches!(
                event,
                Event::StepAttemptFailed { .. } | Event::AgentAnswerRejected { .. }
            );
            let transition = run.state.handle_at(event, self.now_ms);
            let effects = self.append(key, transition)?;
            self.perform(key, effects)?;
        }
        let key = key.to_owned();
        self.send(if rejected {
            Outbound::AttemptRecorded { key, cmd }
        } else {
            Outbound::ResultRecorded { key, cmd }
        });
        Ok(())
    }

    /// Checks an agent call's answer against its output schema: a match is recorded; a mismatch is a
    /// retryable failure until the retry policy is spent, then an escalation; a schema that does not
    /// compile fails the call (a mistake in the workflow).
    fn check_answer(
        &self,
        state: &RunState,
        cmd: CommandIndex,
        outcome: Result<aqueduct_core::AgentResponse, String>,
    ) -> Event {
        let Ok(response) = outcome else {
            return Event::AgentResult { cmd, outcome };
        };
        let Some(step) = usize::try_from(cmd.0)
            .ok()
            .and_then(|index| state.steps().get(index))
        else {
            return Event::AgentResult {
                cmd,
                outcome: Ok(response),
            };
        };
        let Some(schema) = crate::schema::output_schema(&step.input) else {
            return Event::AgentResult {
                cmd,
                outcome: Ok(response),
            };
        };
        match crate::schema::check(&schema, &response.response) {
            Ok(()) => Event::AgentResult {
                cmd,
                outcome: Ok(response),
            },
            Err(crate::schema::SchemaError::Schema(error)) => Event::AgentResult {
                cmd,
                outcome: Err(format!(
                    "the output schema of `{}` is invalid: {error}",
                    step.name
                )),
            },
            Err(crate::schema::SchemaError::Mismatch(error)) => {
                let policy = self
                    .retry_policies
                    .get(&step.name)
                    .copied()
                    .unwrap_or(self.default_retry);
                let attempt = step.failed_attempts.saturating_add(1);
                if attempt >= policy.max_attempts {
                    Event::AgentAnswerRejected {
                        cmd,
                        error,
                        answer: response.response,
                    }
                } else {
                    Event::StepAttemptFailed {
                        cmd,
                        error: format!("the answer does not match the output schema: {error}"),
                        retry_after_ms: policy.backoff_ms(attempt),
                    }
                }
            }
        }
    }

    /// Records a step result (at most once per command).
    pub fn step_result(
        &mut self,
        key: &str,
        cmd: CommandIndex,
        outcome: StepOutcome,
    ) -> Result<(), PartitionError> {
        let Some(run) = self.runs.get(key) else {
            // Not in memory: the run ended (and was unloaded) or never existed. Nothing to record.
            self.send(Outbound::ResultRecorded {
                key: key.to_owned(),
                cmd,
            });
            return Ok(());
        };
        let transition = run
            .state
            .handle_at(Event::StepResult { cmd, outcome }, self.now_ms);
        let effects = self.append(key, transition)?;
        self.perform(key, effects)?;
        self.send(Outbound::ResultRecorded {
            key: key.to_owned(),
            cmd,
        });
        Ok(())
    }

    /// Re-dispatches every step without a recorded outcome (at-least-once delivery).
    pub fn redispatch(&mut self) {
        let mut dispatches = Vec::new();
        for (key, run) in &self.runs {
            for effect in run.state.pending_steps() {
                dispatches.extend(dispatch_message(key, effect));
            }
        }
        for dispatch in dispatches {
            self.send(dispatch);
        }
        let waiting: Vec<Remote> = self
            .remote_children
            .values()
            .chain(self.remote_cancels.values())
            .chain(self.remote_sends.values())
            .cloned()
            .collect();
        for message in waiting {
            self.send_remote(message);
        }
    }

    /// Whether there is anything to write or release.
    pub fn needs_flush(&self) -> bool {
        !self.batch.is_empty() || !self.deferred.is_empty()
    }

    /// Writes and syncs the batch, then releases the messages that waited for it. On error the
    /// partition must be dropped and recovered.
    pub fn flush(&mut self) -> Result<Vec<Outbound>, StorageError> {
        debug_assert_eq!(self.running, count_running(&self.runs));
        if !self.batch.is_empty() {
            let started = std::time::Instant::now();
            let records = self.batch.len();
            let written = self.log.append(&self.batch).and_then(|()| self.log.sync());
            self.batch.clear();
            let observed = std::mem::take(&mut self.observed);
            written?;
            if let Some(observer) = &self.observer {
                observer.flush(records, started.elapsed());
                for (key, workflow, version, entry) in &observed {
                    observer.entry(key, workflow, version.as_deref(), entry, self.now_ms);
                }
            }
        }
        let mut released = std::mem::take(&mut self.deferred);
        // A dispatch waits for the flush; meanwhile a late result of an earlier attempt may have been
        // recorded. Releasing it would run the step (or call the model) again for nothing.
        if !self.release_stale_dispatches {
            released.retain(|message| !self.already_recorded(message));
        }
        Ok(released)
    }

    /// Whether `message` dispatches a command whose outcome is already recorded.
    fn already_recorded(&self, message: &Outbound) -> bool {
        let (Outbound::Dispatch { key, cmd, .. } | Outbound::DispatchAgent { key, cmd, .. }) =
            message
        else {
            return false;
        };
        self.runs
            .get(key)
            .and_then(|run| run.state.steps().get(usize::try_from(cmd.0).ok()?))
            .is_some_and(|step| step.outcome.is_some())
    }

    /// Sets the observer told about durable entries and flushes (see [`crate::Observer`]).
    pub fn set_observer(&mut self, observer: std::sync::Arc<dyn crate::Observer>) {
        self.observer = Some(observer);
    }

    /// The runs, by key.
    pub fn runs(&self) -> &BTreeMap<String, PartitionRun> {
        &self.runs
    }

    /// The build a run is pinned to, if it is a run of a remote workflow with one (ADR 0048).
    pub fn pinned_build(&self, key: &str) -> Option<&str> {
        let run = self.runs.get(key)?;
        if self.deciders.is_remote(run.state.workflow()) {
            run.state.version()
        } else {
            None
        }
    }

    /// Open runs of remote workflows, by workflow and build.
    pub fn open_builds(&self) -> BTreeMap<(String, String), usize> {
        let mut open = BTreeMap::new();
        for run in self.runs.values() {
            if run.state.status().is_terminal() || !self.deciders.is_remote(run.state.workflow()) {
                continue;
            }
            if let Some(build) = run.state.version() {
                *open
                    .entry((run.state.workflow().to_owned(), build.to_owned()))
                    .or_default() += 1;
            }
        }
        open
    }

    /// The workflows' logic, to change at run time (ADR 0048).
    pub fn deciders_mut(&mut self) -> &mut Deciders {
        &mut self.deciders
    }

    /// A run's journal, whether it is in memory or only in the last checkpoint.
    pub fn history(&mut self, key: &str) -> Result<Option<Vec<Entry>>, PartitionError> {
        if let Some(run) = self.runs.get(key) {
            return Ok(Some(run.journal.clone()));
        }
        let Some(records) = self.state.load_run(key)? else {
            return Ok(None);
        };
        let verified = verify_chain(key, records.iter().map(Vec::as_slice)).map_err(|e| {
            PartitionError::Corrupt(format!("run {key}: hash chain does not verify: {e}"))
        })?;
        Ok(Some(verified.entries))
    }

    /// Adds a run's history from an import (ADR 0047), or the part of it this partition does not have.
    /// Performs no effects: the caller has not resumed the partition yet. Returns whether anything was
    /// added.
    pub(crate) fn import_run(
        &mut self,
        key: &str,
        records: &[Vec<u8>],
    ) -> Result<bool, PartitionError> {
        const FLUSH_EVERY: usize = 4096;
        let have = self.runs.get(key).map_or(0, |run| run.journal.len());
        if have >= records.len() {
            return Ok(false);
        }
        let verified = verify_chain(key, records.iter().map(Vec::as_slice)).map_err(|e| {
            PartitionError::Corrupt(format!("run {key}: hash chain does not verify: {e}"))
        })?;
        let state = RunState::from_journal(&verified.entries)
            .map_err(|e| PartitionError::Corrupt(format!("run {key}: journal is invalid: {e}")))?;
        for record in &records[have..] {
            self.batch.push(encode_log_record(key, record));
        }
        let was_running = self
            .runs
            .get(key)
            .is_some_and(|run| !run.state.status().is_terminal());
        let is_running = !state.status().is_terminal();
        self.runs.insert(
            key.to_owned(),
            PartitionRun {
                state,
                journal: verified.entries,
                chain: ChainWriter::resume(verified.head),
            },
        );
        if counts_as_running(key) && was_running != is_running {
            let namespace = crate::namespaces::namespace_of(key).to_owned();
            if is_running {
                *self.running.entry(namespace).or_default() += 1;
            } else if let Some(count) = self.running.get_mut(&namespace) {
                *count -= 1;
                if *count == 0 {
                    self.running.remove(&namespace);
                }
            }
        }
        self.dirty.insert(key.to_owned());
        self.refresh_timers(key);
        self.index_children(key);
        if self.batch.len() >= FLUSH_EVERY {
            self.flush()
                .map_err(|e| PartitionError::Internal(e.to_string()))?;
        }
        Ok(true)
    }

    /// How many runs are running in each namespace, not counting object loops. Kept up to date as runs
    /// start and end: reading it costs nothing however many runs there are.
    pub fn running(&self) -> &BTreeMap<String, usize> {
        &self.running
    }

    /// One run.
    pub fn run(&self, key: &str) -> Option<&PartitionRun> {
        self.runs.get(key)
    }

    /// The log store, for inspection.
    pub fn log(&self) -> &L {
        &self.log
    }

    /// The log store, mutably (for tests that change its fault behaviour).
    pub fn log_mut(&mut self) -> &mut L {
        &mut self.log
    }

    /// Gives the log store back, dropping everything in memory (as a crash would).
    pub fn into_log(self) -> L {
        self.log
    }

    /// Gives the log and state stores back, dropping everything in memory (as a crash would).
    pub fn into_parts(self) -> (L, S) {
        (self.log, self.state)
    }

    /// Saves a checkpoint of every run changed since the last one, then lets the log drop the records
    /// it covers. Returns the checkpoint's LSN. Call only after [`Partition::flush`], when nothing is
    /// waiting to be written: a checkpoint must never cover a record that is not durable.
    pub fn checkpoint(&mut self) -> Result<u64, PartitionError> {
        if !self.batch.is_empty() {
            return Err(PartitionError::Internal(
                "a checkpoint was requested before a flush".into(),
            ));
        }
        let lsn = self.log.next_lsn();
        let runs: Vec<(String, Vec<Vec<u8>>)> = self
            .dirty
            .iter()
            .filter_map(|key| self.runs.get(key).map(|run| (key, run)))
            .map(|(key, run)| {
                // The canonical encoding re-creates exactly the records that were logged.
                let mut chain = ChainWriter::new(key);
                (
                    key.clone(),
                    run.journal
                        .iter()
                        .map(|entry| chain.append(entry))
                        .collect(),
                )
            })
            .collect();
        self.state.save(lsn, &runs)?;
        self.dirty.clear();
        self.log.trim(lsn)?;
        self.evict();
        Ok(lsn)
    }

    /// Invariant check: every run's state equals the fold of its journal.
    pub fn check_fold(&self) -> Result<(), String> {
        for (key, run) in &self.runs {
            let folded = RunState::from_journal(&run.journal)
                .map_err(|e| format!("run {key}: in-memory journal is invalid: {e}"))?;
            if folded != run.state {
                return Err(format!(
                    "run {key}: state differs from the fold of its journal"
                ));
            }
        }
        Ok(())
    }

    /// Testing only: release messages before their entries are durable. This is a deliberate bug
    /// that the simulator must catch.
    #[doc(hidden)]
    pub fn inject_ack_before_sync(&mut self, enabled: bool) {
        self.ack_before_sync = enabled;
    }

    /// Testing only: release dispatches of commands whose result was recorded while they waited for the
    /// flush (running the step, or calling the model, again for nothing). A deliberate bug that the
    /// simulator must catch.
    #[doc(hidden)]
    pub fn inject_release_stale_dispatches(&mut self) {
        self.release_stale_dispatches = true;
    }

    /// Testing only: on recovery, do not restart the successor of a run that continued as new when a
    /// crash tore off its start. A deliberate bug that the simulator must catch.
    #[doc(hidden)]
    pub fn inject_skip_successor_repair(&mut self) {
        self.skip_successor_repair = true;
    }

    /// Testing only: forget which runs changed since the last checkpoint, as if runs recovered from the
    /// log tail were not marked. A deliberate bug that the simulator must catch.
    #[doc(hidden)]
    pub fn inject_forget_dirty_runs(&mut self) {
        self.dirty.clear();
    }

    /// Testing only: messages released early by [`Partition::inject_ack_before_sync`].
    #[doc(hidden)]
    pub fn take_unsynced(&mut self) -> Vec<Outbound> {
        std::mem::take(&mut self.immediate)
    }

    fn send_remote(&mut self, message: Remote) {
        let to = message.to();
        self.send(Outbound::Remote { to, message });
    }

    /// Whether a key lives on this partition.
    fn is_local(&self, key: &str) -> bool {
        self.placement
            .as_ref()
            .is_none_or(|own| own(crate::placement::home(key)))
    }

    /// Whether a run has messages not yet delivered.
    fn sending(&self, run: &str) -> bool {
        self.remote_sends
            .range((run.to_owned(), CommandIndex(0))..)
            .next()
            .is_some_and(|((sender, _), _)| sender == run)
    }

    /// Tells the partition which homes are its own (ADR 0052). Children and messages for others become
    /// [`Outbound::Remote`]. Set it before [`Partition::resume`].
    pub fn set_placement(&mut self, placement: crate::placement::Placement) {
        self.placement = Some(placement);
    }

    /// A message from another partition (ADR 0052). Repeats are harmless.
    pub fn receive(&mut self, message: Remote) -> Result<(), PartitionError> {
        match message {
            Remote::StartChild {
                parent,
                cmd,
                child,
                workflow,
                input,
                priority,
            } => {
                let options = StartOptions {
                    priority: Some(priority),
                    ..StartOptions::default()
                };
                match self.start_run(&child, &workflow, input, options) {
                    Ok(()) => {}
                    Err(
                        e @ (PartitionError::UnknownWorkflow(_)
                        | PartitionError::UnknownVersion { .. }
                        | PartitionError::KeyConflict(_)
                        | PartitionError::AlreadyStarted(_)
                        | PartitionError::InvalidKey(_)),
                    ) => {
                        let error = format!("child run `{child}` could not be started: {e}");
                        self.send_remote(Remote::ChildResult {
                            parent,
                            cmd,
                            outcome: StepOutcome::Failed(error),
                        });
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                }
                match self.child_outcome(&child)? {
                    Some(outcome) => self.send_remote(Remote::ChildResult {
                        parent,
                        cmd,
                        outcome,
                    }),
                    None => {
                        self.remote_waiters
                            .entry(child)
                            .or_default()
                            .insert((parent, cmd));
                    }
                }
            }
            Remote::ChildResult {
                parent,
                cmd,
                outcome,
            } => {
                self.remote_children.remove(&(parent.clone(), cmd));
                self.remote_cancels.remove(&(parent.clone(), cmd));
                let waiting = self.runs.get(&parent).is_some_and(|run| {
                    !run.state.status().is_terminal()
                        && usize::try_from(cmd.0)
                            .ok()
                            .and_then(|index| run.state.steps().get(index))
                            .is_some_and(|step| {
                                step.kind == CommandKind::Child && step.outcome.is_none()
                            })
                });
                if waiting {
                    self.deliver_child(&parent, cmd, outcome)?;
                }
            }
            Remote::CancelChild { parent, cmd, child } => {
                if let Some(waiting) = self.remote_waiters.get_mut(&child) {
                    waiting.remove(&(parent.clone(), cmd));
                    if waiting.is_empty() {
                        self.remote_waiters.remove(&child);
                    }
                }
                self.cancel_unwaited(&child, &parent)?;
                let outcome = StepOutcome::Failed("cancelled".to_owned());
                self.send_remote(Remote::ChildResult {
                    parent,
                    cmd,
                    outcome,
                });
            }
            Remote::Deliver {
                sender,
                cmd,
                object,
                key,
                handler,
                args,
            } => {
                let id = child_key(&sender, cmd);
                let args = serde_json::from_slice(args.as_bytes()).unwrap_or_default();
                match self.call_object(&object, &key, &handler, &id, &args) {
                    // Not deployed: as within one partition, delivered again if the sender recovers
                    // while it runs.
                    Ok(_)
                    | Err(
                        PartitionError::UnknownWorkflow(_)
                        | PartitionError::InvalidKey(_)
                        | PartitionError::KeyConflict(_),
                    ) => {}
                    Err(error) => return Err(error),
                }
                self.send_remote(Remote::Delivered { sender, cmd });
            }
            Remote::Delivered { sender, cmd } => {
                self.remote_sends.remove(&(sender.clone(), cmd));
                if !self.sending(&sender) && self.held_activations.remove(&sender) {
                    self.perform(&sender, vec![Effect::Activate])?;
                }
            }
        }
        Ok(())
    }

    fn send(&mut self, message: Outbound) {
        if self.ack_before_sync {
            self.immediate.push(message);
        } else {
            self.deferred.push(message);
        }
    }

    fn log_entry(&mut self, key: &str, entry: &Entry) -> Result<(), PartitionError> {
        let run = self
            .runs
            .get_mut(key)
            .ok_or_else(|| PartitionError::Internal(format!("run {key} vanished")))?;
        let record = run.chain.append(entry);
        run.journal.push(entry.clone());
        self.batch.push(encode_log_record(key, &record));
        if self.observer.is_some() {
            self.observed.push((
                key.to_owned(),
                run.state.workflow().to_owned(),
                run.state.version().map(str::to_owned),
                entry.clone(),
            ));
        }
        if !self.dirty.contains(key) {
            self.dirty.insert(key.to_owned());
        }
        self.refresh_timers(key);
        Ok(())
    }

    fn append(&mut self, key: &str, transition: Transition) -> Result<Vec<Effect>, PartitionError> {
        for entry in &transition.entries {
            let run = self
                .runs
                .get_mut(key)
                .ok_or_else(|| PartitionError::Internal(format!("run {key} vanished")))?;
            let was_terminal = run.state.status().is_terminal();
            run.state.apply(entry).map_err(|e| {
                PartitionError::Internal(format!(
                    "run {key}: the core produced an invalid entry: {e}"
                ))
            })?;
            if !was_terminal && run.state.status().is_terminal() && counts_as_running(key) {
                let namespace = crate::namespaces::namespace_of(key);
                if let Some(count) = self.running.get_mut(namespace) {
                    *count -= 1;
                    if *count == 0 {
                        self.running.remove(namespace);
                    }
                }
            }
            self.log_entry(key, entry)?;
        }
        Ok(transition.effects)
    }

    /// Performs a run's effects. Effects raised meanwhile (by children, parents, successors) are
    /// queued and performed in turn, so the stack stays flat however deep the chain of runs.
    fn perform(&mut self, key: &str, effects: Vec<Effect>) -> Result<(), PartitionError> {
        self.queued.push_back((key.to_owned(), effects));
        if self.performing {
            return Ok(());
        }
        self.performing = true;
        let mut result = Ok(());
        while let Some((key, effects)) = self.queued.pop_front() {
            result = self.perform_now(&key, effects);
            if result.is_err() {
                self.queued.clear();
                break;
            }
        }
        self.performing = false;
        result
    }

    fn perform_now(&mut self, key: &str, effects: Vec<Effect>) -> Result<(), PartitionError> {
        let mut queue: VecDeque<Effect> = effects.into();
        while let Some(effect) = queue.pop_front() {
            match effect {
                Effect::Activate => {
                    // Its messages first: the decision may end the run.
                    if self.sending(key) {
                        self.held_activations.insert(key.to_owned());
                        continue;
                    }
                    let env = Env {
                        now_ms: self.now_ms,
                        seed: self.next_seed(),
                    };
                    let run = self
                        .runs
                        .get(key)
                        .ok_or_else(|| PartitionError::Internal(format!("run {key} vanished")))?;
                    if self.deciders.is_remote(run.state.workflow()) {
                        let message = Outbound::Activation {
                            key: key.to_owned(),
                            workflow: run.state.workflow().to_owned(),
                            at: run.state.len(),
                            env,
                        };
                        self.send(message);
                        continue;
                    }
                    // A run whose workflow or version is not registered waits until it is: a pinned run
                    // is only ever decided by its own version.
                    let Some(decider) = self
                        .deciders
                        .for_run(run.state.workflow(), run.state.version())
                    else {
                        continue;
                    };
                    let commands = decider.decide(&run.state.view_at(key, env));
                    let transition = run.state.handle_at(Event::Decision(commands), self.now_ms);
                    queue.extend(self.append(key, transition)?);
                }
                effect @ (Effect::DispatchStep { .. } | Effect::DispatchAgent { .. }) => {
                    if let Some(message) = dispatch_message(key, effect) {
                        self.send(message);
                    }
                }
                Effect::Finished(status) => {
                    self.send(Outbound::Finished {
                        key: key.to_owned(),
                        status,
                    });
                    if matches!(status, Status::Completed | Status::Failed) {
                        self.child_ended(key)?;
                    }
                }
                Effect::StartChild {
                    cmd,
                    workflow,
                    input,
                    key: child,
                } => self.start_child(key, cmd, &workflow, input, child)?,
                Effect::CancelChild { cmd } => self.cancel_child(key, cmd)?,
                Effect::Send {
                    cmd,
                    object,
                    key: target,
                    handler,
                    args,
                } => {
                    // A key logic may not name goes nowhere; the sender gets no answer either way.
                    let Some(target) = crate::namespaces::chosen_key(key, &target) else {
                        continue;
                    };
                    if !self.is_local(&crate::objects::object_run_key(&object, &target)) {
                        let message = Remote::Deliver {
                            sender: key.to_owned(),
                            cmd,
                            object,
                            key: target,
                            handler,
                            args,
                        };
                        self.remote_sends
                            .insert((key.to_owned(), cmd), message.clone());
                        self.send_remote(message);
                        continue;
                    }
                    let id = child_key(key, cmd);
                    let args = serde_json::from_slice(args.as_bytes()).unwrap_or_default();
                    match self.call_object(&object, &target, &handler, &id, &args) {
                        // A target not deployed (yet) is delivered to when it is: this is performed
                        // again after a restart while the sender runs.
                        Ok(_)
                        | Err(
                            PartitionError::UnknownWorkflow(_)
                            | PartitionError::InvalidKey(_)
                            | PartitionError::KeyConflict(_),
                        ) => {}
                        Err(error) => return Err(error),
                    }
                }
                Effect::ContinueAsNew { input } => {
                    let (workflow, budget, priority) = self
                        .runs
                        .get(key)
                        .map(|run| {
                            (
                                run.state.workflow().to_owned(),
                                successor_budget(&run.state),
                                run.state.priority(),
                            )
                        })
                        .ok_or_else(|| PartitionError::Internal(format!("run {key} vanished")))?;
                    let (base, generation) = split_run_id(key);
                    let next = run_id(base, generation + 1);
                    let version = self.successor_version(&workflow);
                    self.create_run(&next, &workflow, input, budget, version, priority)?;
                }
            }
        }
        Ok(())
    }
}

/// Random bits from the operating system, without a dependency: the standard library seeds every
/// `RandomState` from it. Good for seeds; not for secrets.
fn os_seed() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(0x6171_7565_6475_6374);
    hasher.finish()
}

/// The budget a run's successor (after continue-as-new) starts with: what is left of the run's.
fn successor_budget(state: &RunState) -> Option<u64> {
    state
        .budget()
        .map(|budget| budget.saturating_sub(state.tokens_used()))
}

/// The message that executes a dispatch effect, if it is one.
fn dispatch_message(key: &str, effect: Effect) -> Option<Outbound> {
    match effect {
        Effect::DispatchStep { cmd, name, input } => Some(Outbound::Dispatch {
            key: key.to_owned(),
            cmd,
            name,
            input,
        }),
        Effect::DispatchAgent {
            cmd,
            name,
            request,
            budget_tokens,
        } => Some(Outbound::DispatchAgent {
            key: key.to_owned(),
            cmd,
            name,
            request,
            budget_tokens,
        }),
        _ => None,
    }
}
