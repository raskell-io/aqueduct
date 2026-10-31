//! Storage for Aqueduct: the log store trait, record framing, and the file-backed log (ADR 0009).
//! See `BRIEF.md`, section 6.3.
//!
//! Records are framed with a length and a CRC-32 so that a torn write at the end of a log is detected
//! and truncated on recovery, before any record is interpreted.

#![forbid(unsafe_code)]

mod file_log;
pub mod frame;
mod state;

pub use file_log::{FileLog, FileLogOptions};
pub use state::{Checkpoint, NoStateStore, RedbStateStore, StateStore};

/// A storage failure.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StorageError {
    /// An I/O operation failed.
    #[error("i/o error: {0}")]
    Io(String),
    /// A sync failed. The state of unsynced data is unknown; the caller must treat this as fatal and
    /// recover from the log (see ADR 0003 and the simulation's fsync fault).
    #[error("sync failed: {0}")]
    SyncFailed(String),
    /// A record is too large to frame.
    #[error("record of {0} bytes exceeds the maximum frame size")]
    RecordTooLarge(usize),
    /// Stored data is damaged somewhere a crash cannot explain (not at the tail of the log).
    #[error("corrupt log: {0}")]
    Corrupt(String),
    /// Another process (or another handle in this one) has the log open.
    #[error("the log at {0} is in use (is the engine running?)")]
    Locked(String),
    /// The log was used before [`LogStore::recover`].
    #[error("the log must be recovered before it is written")]
    NotRecovered,
}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        StorageError::Io(error.to_string())
    }
}

/// An append-only log of opaque records, for one partition.
///
/// Appended records are not durable until [`LogStore::sync`] returns `Ok`. After a crash, any suffix
/// of the unsynced records may be missing or torn; [`LogStore::recover`] returns the longest valid
/// prefix and truncates the rest, so that later appends follow valid data.
pub trait LogStore {
    /// Buffers records for appending.
    fn append(&mut self, records: &[Vec<u8>]) -> Result<(), StorageError>;

    /// Makes every appended record durable.
    fn sync(&mut self) -> Result<(), StorageError>;

    /// Returns all valid records in order and truncates any torn or corrupt tail.
    fn recover(&mut self) -> Result<Vec<Vec<u8>>, StorageError> {
        self.recover_from(0)
    }

    /// Returns the valid records with LSN `from` and later, in order, truncating any torn tail. Fails if
    /// records from `from` on are missing (trimmed or never written).
    fn recover_from(&mut self, from: u64) -> Result<Vec<Vec<u8>>, StorageError>;

    /// The LSN the next synced record will get.
    fn next_lsn(&self) -> u64;

    /// Allows records before `before` to be deleted (a checkpoint covers them). Implementations may keep
    /// some of them; they must keep everything from `before` on.
    fn trim(&mut self, before: u64) -> Result<(), StorageError>;
}
