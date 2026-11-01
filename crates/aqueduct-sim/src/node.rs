//! The simulated node: the engine's [`Partition`] plus what only a simulation needs (being up or
//! down, incarnations, handing the disk back on a crash).

use std::collections::BTreeMap;
use std::sync::Arc;

use aqueduct_core::{CommandIndex, Decider, Payload, StepOutcome};
use aqueduct_engine::{Deciders, Outbound, Partition, PartitionRun, RetryPolicy};

/// Retries never run out in the simulation, so every run still ends as the fault-free reference.
const SIM_RETRY: RetryPolicy = RetryPolicy {
    max_attempts: u32::MAX,
    initial_backoff_ms: 20,
    multiplier: 2,
    max_backoff_ms: 200,
};
use aqueduct_storage::{Checkpoint, StateStore, StorageError};

use crate::disk::SimDisk;

/// The single workflow the simulated workload runs.
pub(crate) const WORKFLOW: &str = "pipeline";

/// A simulated state store. Saves are atomic and survive crashes, like a committed redb transaction.
#[derive(Clone, Debug, Default)]
pub(crate) struct SimState {
    checkpoint: Checkpoint,
    pub saves: u64,
}

impl StateStore for SimState {
    fn load(&mut self) -> Result<Checkpoint, StorageError> {
        Ok(self.checkpoint.clone())
    }

    fn save(&mut self, lsn: u64, runs: &[(String, Vec<Vec<u8>>)]) -> Result<(), StorageError> {
        for (key, records) in runs {
            self.checkpoint.runs.insert(key.clone(), records.clone());
        }
        self.checkpoint.lsn = lsn;
        self.saves += 1;
        Ok(())
    }
}

/// The seed of recorded values in a simulation: fixed, so a seed replays exactly. A restarted node
/// draws other values, as a real restart would.
const SIM_SEED: u64 = 0x5eed_a9ed_0c70;

static NO_RUNS: BTreeMap<String, PartitionRun> = BTreeMap::new();

pub(crate) struct Node {
    pub up: bool,
    pub incarnation: u64,
    pub flush_scheduled: bool,
    partition: Option<Partition<SimDisk, SimState>>,
    stopped: Option<(SimDisk, SimState)>,
    deciders: Deciders,
    ack_before_sync: bool,
    pub forget_dirty_runs: bool,
    pub skip_successor_repair: bool,
}

impl Node {
    pub fn new(
        disk: SimDisk,
        ack_before_sync: bool,
        decider: Arc<dyn Decider + Send + Sync>,
    ) -> Result<Self, String> {
        let mut deciders = Deciders::new();
        deciders.insert(WORKFLOW, decider);
        let mut partition = Partition::load_with_state(disk, SimState::default(), deciders.clone())
            .map_err(|e| e.to_string())?;
        partition.inject_ack_before_sync(ack_before_sync);
        partition.set_default_retry_policy(SIM_RETRY);
        partition.set_seed(SIM_SEED);
        Ok(Node {
            up: true,
            incarnation: 0,
            flush_scheduled: false,
            partition: Some(partition),
            stopped: None,
            deciders,
            ack_before_sync,
            forget_dirty_runs: false,
            skip_successor_repair: false,
        })
    }

    fn partition(&mut self) -> Result<&mut Partition<SimDisk, SimState>, String> {
        self.partition
            .as_mut()
            .ok_or_else(|| "the node is down".to_owned())
    }

    pub fn disk(&self) -> &SimDisk {
        match (&self.partition, &self.stopped) {
            (Some(partition), _) => partition.log(),
            (None, Some((disk, _))) => disk,
            (None, None) => unreachable_disk(),
        }
    }

    pub fn disk_mut(&mut self) -> Option<&mut SimDisk> {
        match (&mut self.partition, &mut self.stopped) {
            (Some(partition), _) => Some(partition.log_mut()),
            (None, stopped) => stopped.as_mut().map(|(disk, _)| disk),
        }
    }

    pub fn runs(&self) -> &BTreeMap<String, PartitionRun> {
        self.partition.as_ref().map_or(&NO_RUNS, Partition::runs)
    }

    pub fn needs_flush(&self) -> bool {
        self.partition.as_ref().is_some_and(Partition::needs_flush)
    }

    pub fn take_immediate(&mut self) -> Vec<Outbound> {
        self.partition
            .as_mut()
            .map(Partition::take_unsynced)
            .unwrap_or_default()
    }

    /// Gives the partition the simulated clock.
    pub fn set_time(&mut self, now: u64) {
        if let Some(partition) = self.partition.as_mut() {
            partition.set_time(now);
        }
    }

    pub fn on_start(&mut self, key: &str, input: Payload) -> Result<(), String> {
        self.partition()?
            .start(key, WORKFLOW, input)
            .map_err(|e| e.to_string())
    }

    pub fn on_step_result(
        &mut self,
        key: &str,
        cmd: CommandIndex,
        outcome: StepOutcome,
    ) -> Result<(), String> {
        self.partition()?
            .step_result(key, cmd, outcome)
            .map_err(|e| e.to_string())
    }

    pub fn on_signal(&mut self, key: &str) -> Result<(), String> {
        let id = format!("go-{key}");
        match self
            .partition()?
            .signal(key, "go", &id, Payload::new(crate::workflow::GO.to_vec()))
        {
            Ok(()) | Err(aqueduct_engine::PartitionError::UnknownRun(_)) => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }

    pub fn on_resolve(
        &mut self,
        run: &str,
        cmd: CommandIndex,
        outcome: Payload,
    ) -> Result<(), String> {
        match self.partition()?.resolve_escalation(run, cmd, outcome) {
            Ok(()) | Err(aqueduct_engine::PartitionError::UnknownRun(_)) => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }

    /// Open escalations, as an operator's dashboard would list them: (run id, command).
    pub fn open_escalations(&self) -> Vec<(String, CommandIndex)> {
        self.partition
            .as_ref()
            .map(|partition| {
                partition
                    .open_escalations()
                    .into_iter()
                    .map(|(run, cmd, _, _)| (run, cmd))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn on_cancel(&mut self, key: &str) -> Result<(), String> {
        match self
            .partition()?
            .cancel(key, crate::workflow::CANCEL_REASON)
        {
            Ok(()) | Err(aqueduct_engine::PartitionError::UnknownRun(_)) => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }

    pub fn fire_due_timers(&mut self) -> Result<(), String> {
        match self.partition.as_mut() {
            Some(partition) => partition.fire_due_timers().map_err(|e| e.to_string()),
            None => Ok(()),
        }
    }

    pub fn on_attempt_failed(
        &mut self,
        key: &str,
        cmd: CommandIndex,
        error: String,
    ) -> Result<(), String> {
        self.partition()?
            .step_attempt_failed(key, cmd, error)
            .map_err(|e| e.to_string())
    }

    /// Saves a checkpoint, if nothing is waiting to be written.
    pub fn checkpoint(&mut self) -> Result<bool, String> {
        match self.partition.as_mut() {
            Some(partition) if !partition.needs_flush() => {
                partition.checkpoint().map_err(|e| e.to_string())?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub fn on_redispatch(&mut self) {
        if let Some(partition) = self.partition.as_mut() {
            partition.redispatch();
        }
    }

    pub fn flush(&mut self) -> Result<Vec<Outbound>, StorageError> {
        match self.partition.as_mut() {
            Some(partition) => partition.flush(),
            None => Ok(Vec::new()),
        }
    }

    pub fn crash(&mut self) {
        if let Some(partition) = self.partition.take() {
            let (mut disk, state) = partition.into_parts();
            disk.crash();
            self.stopped = Some((disk, state));
        }
        self.up = false;
        self.flush_scheduled = false;
    }

    /// Recovers runs from the log, without performing their recovery effects yet.
    pub fn recover(&mut self) -> Result<(), String> {
        let (disk, state) = self.stopped.take().ok_or("no disk to recover from")?;
        let mut partition = Partition::load_with_state(disk, state, self.deciders.clone())
            .map_err(|e| e.to_string())?;
        if self.forget_dirty_runs {
            partition.inject_forget_dirty_runs();
        }
        if self.skip_successor_repair {
            partition.inject_skip_successor_repair();
        }
        partition.inject_ack_before_sync(self.ack_before_sync);
        partition.set_default_retry_policy(SIM_RETRY);
        partition.set_seed(SIM_SEED ^ self.incarnation);
        self.partition = Some(partition);
        self.up = true;
        self.incarnation += 1;
        self.flush_scheduled = false;
        Ok(())
    }

    /// Performs the recovery effects of every recovered run.
    pub fn resume(&mut self) -> Result<(), String> {
        self.partition()?.resume().map_err(|e| e.to_string())
    }

    pub fn check_fold(&self) -> Result<(), String> {
        match &self.partition {
            Some(partition) => partition.check_fold(),
            None => Ok(()),
        }
    }
}

#[allow(clippy::panic)]
fn unreachable_disk() -> ! {
    panic!("the simulated node always holds its disk, in the partition or while stopped")
}
