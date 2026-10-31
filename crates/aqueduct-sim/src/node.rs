//! A single-partition node: the sans-I/O partition processor of phase 0, driven by the simulator.
//!
//! Rules it follows (ADR 0003 and 0004):
//! - entries are appended to the in-memory journal and the log batch together;
//! - nothing leaves the node (acknowledgements, dispatches) until the batch holding the entries it
//!   depends on is synced; after a successful sync, memory equals the durable log;
//! - a failed sync is fatal: the node crashes and recovers from the log.

use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use aqueduct_codec::{ChainWriter, decode_log_record, encode_log_record, verify_chain};
use aqueduct_core::{
    CommandIndex, Decider, Effect, Entry, Event, Payload, RunState, StepOutcome, Transition, start,
};
use aqueduct_storage::{LogStore, StorageError};

use crate::disk::SimDisk;

/// A run held in the node's memory.
pub(crate) struct NodeRun {
    pub state: RunState,
    pub journal: Vec<Entry>,
    chain: ChainWriter,
}

/// A message the node sends once the entries it depends on are durable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Outbound {
    StartAck {
        key: String,
    },
    Dispatch {
        key: String,
        cmd: CommandIndex,
        name: String,
        input: Payload,
    },
    ResultAck {
        key: String,
        cmd: CommandIndex,
    },
}

pub(crate) struct Node {
    pub up: bool,
    pub incarnation: u64,
    pub disk: SimDisk,
    pub runs: BTreeMap<String, NodeRun>,
    pub flush_scheduled: bool,
    batch: Vec<Vec<u8>>,
    deferred: Vec<Outbound>,
    /// Only used by the injected bug that acknowledges before syncing.
    immediate: Vec<Outbound>,
    ack_before_sync: bool,
    decider: Rc<dyn Decider>,
}

impl Node {
    pub fn new(disk: SimDisk, ack_before_sync: bool, decider: Rc<dyn Decider>) -> Self {
        Node {
            decider,
            up: true,
            incarnation: 0,
            disk,
            runs: BTreeMap::new(),
            flush_scheduled: false,
            batch: Vec::new(),
            deferred: Vec::new(),
            immediate: Vec::new(),
            ack_before_sync,
        }
    }

    pub fn needs_flush(&self) -> bool {
        !self.batch.is_empty() || !self.deferred.is_empty()
    }

    pub fn take_immediate(&mut self) -> Vec<Outbound> {
        std::mem::take(&mut self.immediate)
    }

    pub fn on_start(&mut self, key: &str, input: Payload) -> Result<(), String> {
        if !self.runs.contains_key(key) {
            let first = start(input);
            let state = RunState::from_journal(&first.entries)
                .map_err(|e| format!("start produced an invalid journal: {e}"))?;
            self.runs.insert(
                key.to_owned(),
                NodeRun {
                    state,
                    journal: Vec::new(),
                    chain: ChainWriter::new(key),
                },
            );
            for entry in &first.entries {
                self.log(key, entry)?;
            }
            self.perform(key, first.effects)?;
        }
        // Idempotent: a repeated start is acknowledged once the run's start is durable.
        self.send(Outbound::StartAck {
            key: key.to_owned(),
        });
        Ok(())
    }

    pub fn on_step_result(
        &mut self,
        key: &str,
        cmd: CommandIndex,
        outcome: StepOutcome,
    ) -> Result<(), String> {
        let Some(run) = self.runs.get(key) else {
            return Ok(());
        };
        let transition = run.state.handle(Event::StepResult { cmd, outcome });
        let effects = self.append(key, transition)?;
        self.perform(key, effects)?;
        self.send(Outbound::ResultAck {
            key: key.to_owned(),
            cmd,
        });
        Ok(())
    }

    /// Re-dispatches every step without a recorded outcome (at-least-once delivery to workers).
    pub fn on_redispatch(&mut self) {
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

    /// Writes and syncs the batch, then releases the messages that waited for it.
    pub fn flush(&mut self) -> Result<Vec<Outbound>, StorageError> {
        if !self.batch.is_empty() {
            self.disk.append(&self.batch)?;
            self.batch.clear();
            self.disk.sync()?;
        }
        Ok(std::mem::take(&mut self.deferred))
    }

    pub fn crash(&mut self) {
        self.up = false;
        self.runs.clear();
        self.batch.clear();
        self.deferred.clear();
        self.immediate.clear();
        self.flush_scheduled = false;
        self.disk.crash();
    }

    /// Recovers runs from the log, without performing their recovery effects yet.
    pub fn recover(&mut self) -> Result<(), String> {
        self.up = true;
        self.incarnation += 1;
        self.flush_scheduled = false;
        let records = self
            .disk
            .recover()
            .map_err(|e| format!("log recovery failed: {e}"))?;
        let mut by_run: BTreeMap<String, Vec<Vec<u8>>> = BTreeMap::new();
        for bytes in &records {
            let (key, record) = decode_log_record(bytes)
                .map_err(|e| format!("a framed log record does not decode: {e}"))?;
            by_run
                .entry(key.to_owned())
                .or_default()
                .push(record.to_vec());
        }
        for (key, records) in by_run {
            let verified = verify_chain(&key, records.iter().map(Vec::as_slice))
                .map_err(|e| format!("run {key}: hash chain does not verify: {e}"))?;
            let state = RunState::from_journal(&verified.entries)
                .map_err(|e| format!("run {key}: recovered journal is invalid: {e}"))?;
            self.runs.insert(
                key,
                NodeRun {
                    state,
                    journal: verified.entries,
                    chain: ChainWriter::resume(verified.head),
                },
            );
        }
        Ok(())
    }

    /// Performs the recovery effects of every recovered run.
    pub fn resume(&mut self) -> Result<(), String> {
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

    /// Invariant: every run's state equals the fold of its journal.
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

    fn send(&mut self, message: Outbound) {
        if self.ack_before_sync {
            self.immediate.push(message);
        } else {
            self.deferred.push(message);
        }
    }

    fn log(&mut self, key: &str, entry: &Entry) -> Result<(), String> {
        let run = self
            .runs
            .get_mut(key)
            .ok_or_else(|| format!("run {key} vanished"))?;
        let record = run.chain.append(entry);
        run.journal.push(entry.clone());
        self.batch.push(encode_log_record(key, &record));
        Ok(())
    }

    fn append(&mut self, key: &str, transition: Transition) -> Result<Vec<Effect>, String> {
        for entry in &transition.entries {
            let run = self
                .runs
                .get_mut(key)
                .ok_or_else(|| format!("run {key} vanished"))?;
            run.state
                .apply(entry)
                .map_err(|e| format!("run {key}: the core produced an invalid entry: {e}"))?;
            self.log(key, entry)?;
        }
        Ok(transition.effects)
    }

    fn perform(&mut self, key: &str, effects: Vec<Effect>) -> Result<(), String> {
        let mut queue: VecDeque<Effect> = effects.into();
        while let Some(effect) = queue.pop_front() {
            match effect {
                Effect::Activate => {
                    let run = self
                        .runs
                        .get(key)
                        .ok_or_else(|| format!("run {key} vanished"))?;
                    let commands = self.decider.decide(&run.state.view());
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
                Effect::Finished(_) => {}
            }
        }
        Ok(())
    }
}
