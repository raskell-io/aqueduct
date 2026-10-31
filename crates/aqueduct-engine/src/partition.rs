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

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use aqueduct_codec::{ChainWriter, decode_log_record, encode_log_record, verify_chain};
use aqueduct_core::{
    CommandIndex, Decider, Effect, Entry, Event, Payload, RunState, Status, StepOutcome,
    Transition, start,
};
use aqueduct_storage::{LogStore, StorageError};

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
    /// A step result was recorded (or is no longer needed); the sender can stop resending it.
    ResultRecorded {
        /// The run key.
        key: String,
        /// The command.
        cmd: CommandIndex,
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
    /// Storage failed. Fatal: drop the partition and recover from the log.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// The log does not hold a valid history. The partition must not start.
    #[error("corrupt log: {0}")]
    Corrupt(String),
    /// The core rejected an entry it produced itself: a bug.
    #[error("internal error: {0}")]
    Internal(String),
}

/// A partition processor over a log store.
pub struct Partition<L: LogStore> {
    log: L,
    deciders: Deciders,
    runs: BTreeMap<String, PartitionRun>,
    batch: Vec<Vec<u8>>,
    deferred: Vec<Outbound>,
    immediate: Vec<Outbound>,
    ack_before_sync: bool,
}

impl<L: LogStore> std::fmt::Debug for Partition<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Partition")
            .field("runs", &self.runs.len())
            .field("deciders", &self.deciders)
            .finish()
    }
}

impl<L: LogStore> Partition<L> {
    /// Recovers a partition from its log and performs every run's recovery effects.
    pub fn recover(log: L, deciders: Deciders) -> Result<Partition<L>, PartitionError> {
        let mut partition = Partition::load(log, deciders)?;
        partition.resume()?;
        Ok(partition)
    }

    /// Recovers runs from the log without performing their recovery effects (see [`Partition::resume`]).
    pub fn load(mut log: L, deciders: Deciders) -> Result<Partition<L>, PartitionError> {
        let records = log.recover()?;
        let mut by_run: BTreeMap<String, Vec<Vec<u8>>> = BTreeMap::new();
        for bytes in &records {
            let (key, record) = decode_log_record(bytes).map_err(|e| {
                PartitionError::Corrupt(format!("a framed log record does not decode: {e}"))
            })?;
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
            deciders,
            runs,
            batch: Vec::new(),
            deferred: Vec::new(),
            immediate: Vec::new(),
            ack_before_sync: false,
        })
    }

    /// Performs the recovery effects of every run: activation and re-dispatch.
    pub fn resume(&mut self) -> Result<(), PartitionError> {
        let keys: Vec<String> = self.runs.keys().cloned().collect();
        for key in keys {
            let effects = match self.runs.get(&key) {
                Some(run) => run.state.recovery_effects(),
                None => continue,
            };
            self.perform(&key, effects)?;
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
        if !self.runs.contains_key(key) {
            if self.deciders.get(workflow).is_none() {
                return Err(PartitionError::UnknownWorkflow(workflow.to_owned()));
            }
            let first = start(workflow, input);
            let state = RunState::from_journal(&first.entries)
                .map_err(|e| PartitionError::Internal(e.to_string()))?;
            self.runs.insert(
                key.to_owned(),
                PartitionRun {
                    state,
                    journal: Vec::new(),
                    chain: ChainWriter::new(key),
                },
            );
            for entry in &first.entries {
                self.log_entry(key, entry)?;
            }
            self.perform(key, first.effects)?;
        }
        self.send(Outbound::Started {
            key: key.to_owned(),
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
            return Ok(());
        };
        let transition = run.state.handle(Event::StepResult { cmd, outcome });
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
                if let Effect::DispatchStep { cmd, name, input } = effect {
                    dispatches.push(Outbound::Dispatch {
                        key: key.clone(),
                        cmd,
                        name,
                        input,
                    });
                }
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
            self.log.append(&self.batch)?;
            self.batch.clear();
            self.log.sync()?;
        }
        Ok(std::mem::take(&mut self.deferred))
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
                    let run = self
                        .runs
                        .get(key)
                        .ok_or_else(|| PartitionError::Internal(format!("run {key} vanished")))?;
                    // A run whose workflow is no longer registered waits until it is.
                    let Some(decider) = self.deciders.get(run.state.workflow()) else {
                        continue;
                    };
                    let commands = decider.decide(&run.state.view());
                    let transition = run.state.handle(Event::Decision(commands));
                    queue.extend(self.append(key, transition)?);
                }
                Effect::DispatchStep { cmd, name, input } => {
                    self.send(Outbound::Dispatch {
                        key: key.to_owned(),
                        cmd,
                        name,
                        input,
                    });
                }
                Effect::Finished(status) => {
                    self.send(Outbound::Finished {
                        key: key.to_owned(),
                        status,
                    });
                }
            }
        }
        Ok(())
    }
}
