//! The simulated node: the engine's [`Partition`] plus what only a simulation needs (being up or
//! down, incarnations, handing the disk back on a crash).

use std::collections::BTreeMap;
use std::sync::Arc;

use aqueduct_core::{CommandIndex, Decider, Payload, StepOutcome};
use aqueduct_engine::{Deciders, Outbound, Partition, PartitionRun};
use aqueduct_storage::StorageError;

use crate::disk::SimDisk;

/// The single workflow the simulated workload runs.
pub(crate) const WORKFLOW: &str = "pipeline";

static NO_RUNS: BTreeMap<String, PartitionRun> = BTreeMap::new();

pub(crate) struct Node {
    pub up: bool,
    pub incarnation: u64,
    pub flush_scheduled: bool,
    partition: Option<Partition<SimDisk>>,
    stopped_disk: Option<SimDisk>,
    deciders: Deciders,
    ack_before_sync: bool,
}

impl Node {
    pub fn new(
        disk: SimDisk,
        ack_before_sync: bool,
        decider: Arc<dyn Decider + Send + Sync>,
    ) -> Result<Self, String> {
        let mut deciders = Deciders::new();
        deciders.insert(WORKFLOW, decider);
        let mut partition = Partition::load(disk, deciders.clone()).map_err(|e| e.to_string())?;
        partition.inject_ack_before_sync(ack_before_sync);
        Ok(Node {
            up: true,
            incarnation: 0,
            flush_scheduled: false,
            partition: Some(partition),
            stopped_disk: None,
            deciders,
            ack_before_sync,
        })
    }

    fn partition(&mut self) -> Result<&mut Partition<SimDisk>, String> {
        self.partition
            .as_mut()
            .ok_or_else(|| "the node is down".to_owned())
    }

    pub fn disk(&self) -> &SimDisk {
        match (&self.partition, &self.stopped_disk) {
            (Some(partition), _) => partition.log(),
            (None, Some(disk)) => disk,
            (None, None) => unreachable_disk(),
        }
    }

    pub fn disk_mut(&mut self) -> Option<&mut SimDisk> {
        match (&mut self.partition, &mut self.stopped_disk) {
            (Some(partition), _) => Some(partition.log_mut()),
            (None, disk) => disk.as_mut(),
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

    pub fn fire_due_timers(&mut self) -> Result<(), String> {
        match self.partition.as_mut() {
            Some(partition) => partition.fire_due_timers().map_err(|e| e.to_string()),
            None => Ok(()),
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
            let mut disk = partition.into_log();
            disk.crash();
            self.stopped_disk = Some(disk);
        }
        self.up = false;
        self.flush_scheduled = false;
    }

    /// Recovers runs from the log, without performing their recovery effects yet.
    pub fn recover(&mut self) -> Result<(), String> {
        let disk = self.stopped_disk.take().ok_or("no disk to recover from")?;
        let mut partition =
            Partition::load(disk, self.deciders.clone()).map_err(|e| e.to_string())?;
        partition.inject_ack_before_sync(self.ack_before_sync);
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
