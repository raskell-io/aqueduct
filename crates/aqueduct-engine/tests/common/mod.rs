//! Helpers shared by the engine's integration tests.

#![allow(dead_code)]

use aqueduct_storage::{LogStore, StorageError};

/// A log in memory: synced records survive, appended but unsynced ones are lost.
#[derive(Clone, Default)]
pub struct MemLog {
    pub records: Vec<Vec<u8>>,
    pub pending: Vec<Vec<u8>>,
}

impl LogStore for MemLog {
    fn append(&mut self, records: &[Vec<u8>]) -> Result<(), StorageError> {
        self.pending.extend_from_slice(records);
        Ok(())
    }

    fn sync(&mut self) -> Result<(), StorageError> {
        self.records.append(&mut self.pending);
        Ok(())
    }

    fn recover_from(&mut self, from: u64) -> Result<Vec<Vec<u8>>, StorageError> {
        self.pending.clear();
        Ok(self.records[usize::try_from(from).unwrap()..].to_vec())
    }

    fn next_lsn(&self) -> u64 {
        self.records.len() as u64
    }

    fn trim(&mut self, _before: u64) -> Result<(), StorageError> {
        Ok(())
    }
}
