//! A simulated disk holding one partition log.

use aqueduct_storage::{LogStore, StorageError, frame};

use crate::rng::Rng;

/// A log file with a durable part and an unsynced part.
///
/// On a crash, a random prefix of the unsynced bytes survives (the OS may have written some of them),
/// and with some probability one surviving unsynced byte is corrupted (a torn write). Synced bytes are
/// never lost. A sync can fail, after which the caller must crash.
#[derive(Clone, Debug)]
pub struct SimDisk {
    durable: Vec<u8>,
    pending: Vec<u8>,
    pending_records: u64,
    synced_records: u64,
    rng: Rng,
    sync_fail_ppm: u32,
    torn_ppm: u32,
    /// Successful syncs.
    pub syncs: u64,
    /// Recoveries that truncated a torn or corrupt tail.
    pub torn_tails: u64,
    /// Injected bug: do not truncate the torn tail on recovery.
    pub skip_truncation: bool,
}

impl SimDisk {
    /// Creates an empty disk with the given fault rates.
    pub fn new(rng: Rng, sync_fail_ppm: u32, torn_ppm: u32) -> Self {
        SimDisk {
            durable: Vec::new(),
            pending: Vec::new(),
            pending_records: 0,
            synced_records: 0,
            rng,
            sync_fail_ppm,
            torn_ppm,
            syncs: 0,
            torn_tails: 0,
            skip_truncation: false,
        }
    }

    /// Changes the fault rates (for example, to zero when faults heal).
    pub fn set_faults(&mut self, sync_fail_ppm: u32, torn_ppm: u32) {
        self.sync_fail_ppm = sync_fail_ppm;
        self.torn_ppm = torn_ppm;
    }

    /// Simulates power loss: keeps a random prefix of the unsynced bytes, possibly torn.
    pub fn crash(&mut self) {
        let keep = usize::try_from(self.rng.below(self.pending.len() as u64 + 1)).unwrap_or(0);
        let mut kept = self.pending[..keep].to_vec();
        if keep > 0 && self.rng.chance(self.torn_ppm) {
            let at = usize::try_from(self.rng.below(keep as u64)).unwrap_or(0);
            kept[at] ^= 0xFF;
        }
        self.durable.extend_from_slice(&kept);
        self.pending.clear();
        self.pending_records = 0;
    }

    /// The bytes that would survive a crash right now if no unsynced data survived.
    pub fn durable(&self) -> &[u8] {
        &self.durable
    }
}

impl LogStore for SimDisk {
    fn append(&mut self, records: &[Vec<u8>]) -> Result<(), StorageError> {
        self.pending_records += records.len() as u64;
        for record in records {
            self.pending.extend_from_slice(&frame::encode(record)?);
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<(), StorageError> {
        if self.rng.chance(self.sync_fail_ppm) {
            return Err(StorageError::SyncFailed("injected fsync failure".into()));
        }
        self.durable.append(&mut self.pending);
        self.synced_records += self.pending_records;
        self.pending_records = 0;
        self.syncs += 1;
        Ok(())
    }

    fn recover_from(&mut self, from: u64) -> Result<Vec<Vec<u8>>, StorageError> {
        // Only called after a crash, when nothing is pending.
        self.pending.clear();
        self.pending_records = 0;
        let decoded = frame::decode_all(&self.durable);
        if decoded.valid_len < self.durable.len() {
            self.torn_tails += 1;
            if !self.skip_truncation {
                self.durable.truncate(decoded.valid_len);
            }
        }
        self.synced_records = decoded.payloads.len() as u64;
        if from > self.synced_records {
            return Err(StorageError::Corrupt(format!(
                "records up to LSN {from} are expected, but the log ends at LSN {}",
                self.synced_records
            )));
        }
        let skip = usize::try_from(from).unwrap_or(usize::MAX);
        Ok(decoded.payloads.into_iter().skip(skip).collect())
    }

    fn next_lsn(&self) -> u64 {
        self.synced_records
    }

    fn trim(&mut self, _before: u64) -> Result<(), StorageError> {
        // The simulated disk keeps everything, so the final check can compare the whole log with memory.
        Ok(())
    }
}
