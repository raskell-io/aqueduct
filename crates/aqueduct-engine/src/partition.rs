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
    CommandIndex, Decider, Effect, Entry, Env, Event, Payload, RunState, Status, StepOutcome,
    Transition, run_id, split_run_id, start_with_budget, valid_key,
};
use aqueduct_storage::{LogStore, NoStateStore, StateStore, StorageError};

/// The deciders a partition can run, by workflow name.
#[derive(Clone, Default)]
pub struct Deciders {
    by_name: BTreeMap<String, Arc<dyn Decider + Send + Sync>>,
}

impl Deciders {
    /// An empty set.
    pub fn new() -> Self {
        Deciders::default()
    }

    /// Registers a decider under a workflow name, replacing any earlier one.
    pub fn insert(&mut self, workflow: impl Into<String>, decider: Arc<dyn Decider + Send + Sync>) {
        self.by_name.insert(workflow.into(), decider);
    }

    /// The decider for a workflow, if registered.
    pub fn get(&self, workflow: &str) -> Option<&(dyn Decider + Send + Sync)> {
        self.by_name.get(workflow).map(|d| d.as_ref())
    }
}

impl std::fmt::Debug for Deciders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.by_name.keys()).finish()
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
    /// A run reached a terminal status.
    Finished {
        /// The run key.
        key: String,
        /// The terminal status.
        status: Status,
    },
}

/// Errors from the partition.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PartitionError {
    /// The workflow is not registered.
    #[error("unknown workflow `{0}`")]
    UnknownWorkflow(String),
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
}

/// How a run is started.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StartOptions {
    /// What to do if the key already has a run.
    pub reuse: ReusePolicy,
    /// The run's token budget for agent calls, if it has one. An agent call that would exceed it is not
    /// made; it escalates for approval (reason [`aqueduct_core::BUDGET_ESCALATION`]).
    pub budget_tokens: Option<u64>,
}

impl From<ReusePolicy> for StartOptions {
    fn from(reuse: ReusePolicy) -> Self {
        StartOptions {
            reuse,
            budget_tokens: None,
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
    batch: Vec<Vec<u8>>,
    /// Entries of the batch for the observer: (run key, workflow, entry).
    observed: Vec<(String, String, Entry)>,
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
}

impl<L: LogStore, S: StateStore> std::fmt::Debug for Partition<L, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Partition")
            .field("runs", &self.runs.len())
            .field("deciders", &self.deciders)
            .finish()
    }
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
        Ok(Partition {
            log,
            state,
            dirty,
            deciders,
            runs,
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
        })
        .map(|mut partition| {
            let keys: Vec<String> = partition.runs.keys().cloned().collect();
            for key in keys {
                partition.refresh_timers(&key);
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
            let (base, generation) = split_run_id(key);
            let next = run_id(base, generation + 1);
            if !self.runs.contains_key(&next) && self.lookup_run(&next)?.is_none() {
                self.create_run(&next, &workflow, input, budget)?;
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
        let StartOptions {
            reuse: policy,
            budget_tokens,
        } = options.into();
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
                        None
                    }
                }
            }
        };
        if let Some(generation) = next {
            if self.deciders.get(workflow).is_none() {
                return Err(PartitionError::UnknownWorkflow(workflow.to_owned()));
            }
            self.create_run(&run_id(key, generation), workflow, input, budget_tokens)?;
        }
        self.send(Outbound::Started {
            key: key.to_owned(),
        });
        Ok(())
    }

    /// Creates a run with this run id, logs its start and activates it.
    fn create_run(
        &mut self,
        id: &str,
        workflow: &str,
        input: Payload,
        budget: Option<u64>,
    ) -> Result<(), PartitionError> {
        let first = start_with_budget(workflow, input, budget);
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
        for entry in &first.entries {
            self.log_entry(id, entry)?;
        }
        self.perform(id, first.effects)
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
        if let Some(run) = self.runs.get(key) {
            let transition = run
                .state
                .handle_at(Event::AgentResult { cmd, outcome }, self.now_ms);
            let effects = self.append(key, transition)?;
            self.perform(key, effects)?;
        }
        self.send(Outbound::ResultRecorded {
            key: key.to_owned(),
            cmd,
        });
        Ok(())
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
    }

    /// Whether there is anything to write or release.
    pub fn needs_flush(&self) -> bool {
        !self.batch.is_empty() || !self.deferred.is_empty()
    }

    /// Writes and syncs the batch, then releases the messages that waited for it. On error the
    /// partition must be dropped and recovered.
    pub fn flush(&mut self) -> Result<Vec<Outbound>, StorageError> {
        if !self.batch.is_empty() {
            let started = std::time::Instant::now();
            let records = self.batch.len();
            let written = self.log.append(&self.batch).and_then(|()| self.log.sync());
            self.batch.clear();
            let observed = std::mem::take(&mut self.observed);
            written?;
            if let Some(observer) = &self.observer {
                observer.flush(records, started.elapsed());
                for (key, workflow, entry) in &observed {
                    observer.entry(key, workflow, entry, self.now_ms);
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
            run.state.apply(entry).map_err(|e| {
                PartitionError::Internal(format!(
                    "run {key}: the core produced an invalid entry: {e}"
                ))
            })?;
            self.log_entry(key, entry)?;
        }
        Ok(transition.effects)
    }

    fn perform(&mut self, key: &str, effects: Vec<Effect>) -> Result<(), PartitionError> {
        let mut queue: VecDeque<Effect> = effects.into();
        while let Some(effect) = queue.pop_front() {
            match effect {
                Effect::Activate => {
                    let env = Env {
                        now_ms: self.now_ms,
                        seed: self.next_seed(),
                    };
                    let run = self
                        .runs
                        .get(key)
                        .ok_or_else(|| PartitionError::Internal(format!("run {key} vanished")))?;
                    // A run whose workflow is no longer registered waits until it is.
                    let Some(decider) = self.deciders.get(run.state.workflow()) else {
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
                }
                Effect::ContinueAsNew { input } => {
                    let (workflow, budget) = self
                        .runs
                        .get(key)
                        .map(|run| {
                            (
                                run.state.workflow().to_owned(),
                                successor_budget(&run.state),
                            )
                        })
                        .ok_or_else(|| PartitionError::Internal(format!("run {key} vanished")))?;
                    let (base, generation) = split_run_id(key);
                    let next = run_id(base, generation + 1);
                    self.create_run(&next, &workflow, input, budget)?;
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
