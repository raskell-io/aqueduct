//! The history format: versioned, canonical encoding of journal entries with a per-run hash chain.
//!
//! This crate implements `docs/history-format.md` (version 0, draft). The document is the contract;
//! this code follows it.
//!
//! ```
//! use aqueduct_codec::{verify_chain, ChainWriter};
//! use aqueduct_core::{Entry, Payload};
//!
//! let entries = [
//!     Entry::Started { input: Payload::new(*b"in") },
//!     Entry::RunCompleted { output: Payload::new(*b"out") },
//! ];
//! let mut chain = ChainWriter::new("orders:42");
//! let records: Vec<Vec<u8>> = entries.iter().map(|entry| chain.append(entry)).collect();
//!
//! let verified = verify_chain("orders:42", records.iter().map(Vec::as_slice)).unwrap();
//! assert_eq!(verified.entries, entries);
//! assert_eq!(verified.head, chain.head());
//!
//! // The chain is bound to its run key.
//! assert!(verify_chain("orders:43", records.iter().map(Vec::as_slice)).is_err());
//! ```

#![forbid(unsafe_code)]

mod wire;

use aqueduct_core::{CommandIndex, Entry, Payload};
use sha2::{Digest, Sha256};
use wire::{Reader, put_bytes, put_varint};

/// The history format version this crate writes and reads.
pub const FORMAT_VERSION: u8 = 0;

/// A SHA-256 digest.
pub type Hash = [u8; 32];

const GENESIS_DOMAIN: &[u8] = b"aqueduct-history-v0\0";

mod kind {
    pub const STARTED: u8 = 1;
    pub const STEP_SCHEDULED: u8 = 2;
    pub const STEP_COMPLETED: u8 = 3;
    pub const STEP_FAILED: u8 = 4;
    pub const RUN_COMPLETED: u8 = 5;
    pub const RUN_FAILED: u8 = 6;
    pub const PARKED: u8 = 7;
}

/// Bytes that are not a valid record or log record.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// The version byte is not one this crate understands.
    #[error("unsupported format version {0}")]
    UnsupportedVersion(u8),
    /// The kind byte is not a known entry kind.
    #[error("unknown entry kind {0}")]
    UnknownKind(u8),
    /// The bytes end in the middle of a field.
    #[error("unexpected end of record")]
    UnexpectedEnd,
    /// Bytes remain after the last field.
    #[error("trailing bytes after record")]
    TrailingBytes,
    /// A varint is not minimally encoded.
    #[error("non-canonical varint")]
    NonCanonicalVarint,
    /// A varint exceeds 64 bits, or a length exceeds the address space.
    #[error("integer overflow")]
    Overflow,
    /// A string field is not valid UTF-8.
    #[error("invalid UTF-8 in a string field")]
    InvalidUtf8,
    /// A command index does not fit in 32 bits.
    #[error("command index out of range")]
    CommandOutOfRange,
}

/// A history whose chain does not verify.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ChainError {
    /// A record does not decode.
    #[error("record {index} does not decode: {source}")]
    Decode {
        /// The record's position in the history.
        index: usize,
        /// Why it does not decode.
        source: DecodeError,
    },
    /// A record's `prev` hash does not match the previous record.
    #[error("record {index} does not link to its predecessor")]
    BrokenLink {
        /// The record's position in the history.
        index: usize,
    },
}

/// A decoded record: the entry and the hash it links to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    /// The journal entry.
    pub entry: Entry,
    /// The hash of the previous record, or the genesis hash for the first record.
    pub prev: Hash,
}

/// The genesis hash of a run's chain: `SHA-256("aqueduct-history-v0" || 0x00 || run_key)`.
pub fn genesis(run_key: &str) -> Hash {
    let mut hasher = Sha256::new();
    hasher.update(GENESIS_DOMAIN);
    hasher.update(run_key.as_bytes());
    hasher.finalize().into()
}

/// The SHA-256 hash of a record's bytes.
pub fn record_hash(record: &[u8]) -> Hash {
    Sha256::digest(record).into()
}

/// Encodes an entry as a record linking to `prev`.
pub fn encode_record(entry: &Entry, prev: &Hash) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(FORMAT_VERSION);
    let kind = match entry {
        Entry::Started { .. } => kind::STARTED,
        Entry::StepScheduled { .. } => kind::STEP_SCHEDULED,
        Entry::StepCompleted { .. } => kind::STEP_COMPLETED,
        Entry::StepFailed { .. } => kind::STEP_FAILED,
        Entry::RunCompleted { .. } => kind::RUN_COMPLETED,
        Entry::RunFailed { .. } => kind::RUN_FAILED,
        Entry::Parked { .. } => kind::PARKED,
    };
    out.push(kind);
    out.extend_from_slice(prev);
    match entry {
        Entry::Started { input } => put_bytes(&mut out, input.as_bytes()),
        Entry::StepScheduled { cmd, name, input } => {
            put_varint(&mut out, u64::from(cmd.0));
            put_bytes(&mut out, name.as_bytes());
            put_bytes(&mut out, input.as_bytes());
        }
        Entry::StepCompleted { cmd, result } => {
            put_varint(&mut out, u64::from(cmd.0));
            put_bytes(&mut out, result.as_bytes());
        }
        Entry::StepFailed { cmd, error } => {
            put_varint(&mut out, u64::from(cmd.0));
            put_bytes(&mut out, error.as_bytes());
        }
        Entry::RunCompleted { output } => put_bytes(&mut out, output.as_bytes()),
        Entry::RunFailed { error } => put_bytes(&mut out, error.as_bytes()),
        Entry::Parked { reason } => put_bytes(&mut out, reason.as_bytes()),
    }
    out
}

/// Decodes a record. Strict: rejects anything `docs/history-format.md` does not allow.
pub fn decode_record(bytes: &[u8]) -> Result<Record, DecodeError> {
    let mut reader = Reader::new(bytes);
    let version = reader.u8()?;
    if version != FORMAT_VERSION {
        return Err(DecodeError::UnsupportedVersion(version));
    }
    let kind = reader.u8()?;
    let prev = reader.hash()?;
    let entry = match kind {
        kind::STARTED => Entry::Started {
            input: reader.payload()?,
        },
        kind::STEP_SCHEDULED => Entry::StepScheduled {
            cmd: reader.cmd()?,
            name: reader.string()?,
            input: reader.payload()?,
        },
        kind::STEP_COMPLETED => Entry::StepCompleted {
            cmd: reader.cmd()?,
            result: reader.payload()?,
        },
        kind::STEP_FAILED => Entry::StepFailed {
            cmd: reader.cmd()?,
            error: reader.string()?,
        },
        kind::RUN_COMPLETED => Entry::RunCompleted {
            output: reader.payload()?,
        },
        kind::RUN_FAILED => Entry::RunFailed {
            error: reader.string()?,
        },
        kind::PARKED => Entry::Parked {
            reason: reader.string()?,
        },
        other => return Err(DecodeError::UnknownKind(other)),
    };
    reader.finish()?;
    Ok(Record { entry, prev })
}

/// Writes the records of one run, maintaining the chain head.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainWriter {
    head: Hash,
}

impl ChainWriter {
    /// Starts a new chain for a run.
    pub fn new(run_key: &str) -> Self {
        ChainWriter {
            head: genesis(run_key),
        }
    }

    /// Continues a chain from a verified head.
    pub fn resume(head: Hash) -> Self {
        ChainWriter { head }
    }

    /// Encodes the next entry and advances the head.
    pub fn append(&mut self, entry: &Entry) -> Vec<u8> {
        let record = encode_record(entry, &self.head);
        self.head = record_hash(&record);
        record
    }

    /// The hash of the last record written, or the genesis hash if none.
    pub fn head(&self) -> Hash {
        self.head
    }
}

/// A history whose chain verified.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedHistory {
    /// The entries, in order.
    pub entries: Vec<Entry>,
    /// The head hash, committing to the whole history.
    pub head: Hash,
}

/// Decodes and verifies the records of one run.
pub fn verify_chain<'a>(
    run_key: &str,
    records: impl IntoIterator<Item = &'a [u8]>,
) -> Result<VerifiedHistory, ChainError> {
    let mut head = genesis(run_key);
    let mut entries = Vec::new();
    for (index, bytes) in records.into_iter().enumerate() {
        let record = decode_record(bytes).map_err(|source| ChainError::Decode { index, source })?;
        if record.prev != head {
            return Err(ChainError::BrokenLink { index });
        }
        head = record_hash(bytes);
        entries.push(record.entry);
    }
    Ok(VerifiedHistory { entries, head })
}

/// Wraps a record with its run key for a partition log (`docs/history-format.md`, section 6).
pub fn encode_log_record(run_key: &str, record: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(record.len() + run_key.len() + 4);
    out.push(FORMAT_VERSION);
    put_bytes(&mut out, run_key.as_bytes());
    put_bytes(&mut out, record);
    out
}

/// Unwraps a partition log record into its run key and record bytes.
pub fn decode_log_record(bytes: &[u8]) -> Result<(&str, &[u8]), DecodeError> {
    let mut reader = Reader::new(bytes);
    let version = reader.u8()?;
    if version != FORMAT_VERSION {
        return Err(DecodeError::UnsupportedVersion(version));
    }
    let key = reader.str()?;
    let record = reader.bytes()?;
    reader.finish()?;
    Ok((key, record))
}

impl Reader<'_> {
    fn payload(&mut self) -> Result<Payload, DecodeError> {
        Ok(Payload::new(self.bytes()?))
    }

    fn string(&mut self) -> Result<String, DecodeError> {
        Ok(self.str()?.to_owned())
    }

    fn cmd(&mut self) -> Result<CommandIndex, DecodeError> {
        let value = self.varint()?;
        u32::try_from(value)
            .map(CommandIndex)
            .map_err(|_| DecodeError::CommandOutOfRange)
    }
}
