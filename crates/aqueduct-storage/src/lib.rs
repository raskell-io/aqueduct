//! Storage for Aqueduct: the log store trait, record framing, and (from phase 0's storage benchmark)
//! the embedded implementations. See `BRIEF.md`, section 6.3.
//!
//! Records are framed with a length and a CRC-32 so that a torn write at the end of a log is detected
//! and truncated on recovery, before any record is interpreted.

#![forbid(unsafe_code)]

pub mod frame;

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
    fn recover(&mut self) -> Result<Vec<Vec<u8>>, StorageError>;
}
