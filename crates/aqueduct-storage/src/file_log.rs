//! The file-backed log (ADR 0009): a directory of segment files holding framed records.
//!
//! - Every record has a log sequence number (LSN): 0, 1, 2, ... Each segment file is named after the LSN of
//!   its first record (`<lsn:020>.log`).
//! - Records are framed (`frame`) and buffered by [`LogStore::append`]; [`LogStore::sync`] writes the
//!   buffer to the current segment and calls `sync_data` (`F_FULLFSYNC` on macOS, `fdatasync` on Linux).
//! - When the current segment is full, the next sync starts a new segment and syncs the directory, so the
//!   new file's existence is durable too.
//! - [`LogStore::recover_from`] reads the segments that hold records from a given LSN on, and checks
//!   that consecutive segments leave no gap. Only the last segment may end in a torn or corrupt tail,
//!   which a crash during a write can explain; it is truncated. Damage anywhere else, and missing records,
//!   are reported as [`StorageError::Corrupt`] and never repaired silently. Bit rot in the middle of the
//!   last segment cannot be told apart from a torn tail.
//! - [`LogStore::trim`] deletes whole segments before an LSN that a checkpoint covers; the current
//!   segment is never deleted.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::{LogStore, StorageError, frame};

/// Options for a [`FileLog`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileLogOptions {
    /// Size after which the next sync starts a new segment. A single sync never splits its records
    /// across segments, so a segment can exceed this size.
    pub segment_bytes: u64,
}

impl Default for FileLogOptions {
    fn default() -> Self {
        FileLogOptions {
            segment_bytes: 64 << 20,
        }
    }
}

/// A log of framed records in a directory of segment files.
#[derive(Debug)]
pub struct FileLog {
    dir: PathBuf,
    options: FileLogOptions,
    current: Option<Segment>,
    pending: Vec<u8>,
    pending_records: u64,
    next_lsn: u64,
}

#[derive(Debug)]
struct Segment {
    file: File,
    len: u64,
}

impl FileLog {
    /// Opens (or creates) a log in `dir`. Call [`LogStore::recover_from`] before appending.
    pub fn open(dir: impl Into<PathBuf>, options: FileLogOptions) -> Result<FileLog, StorageError> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(FileLog {
            dir,
            options,
            current: None,
            pending: Vec::new(),
            pending_records: 0,
            next_lsn: 0,
        })
    }

    /// The directory holding the segments.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The first LSNs of the segment files present, in order.
    pub fn segments(&self) -> Result<Vec<u64>, StorageError> {
        let mut bases = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let name = entry?.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(base) = name
                .strip_suffix(".log")
                .and_then(|n| n.parse::<u64>().ok())
            {
                bases.push(base);
            }
        }
        bases.sort_unstable();
        Ok(bases)
    }

    fn path(&self, base: u64) -> PathBuf {
        self.dir.join(format!("{base:020}.log"))
    }

    fn open_segment(&self, base: u64, len: u64) -> Result<Segment, StorageError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path(base))?;
        Ok(Segment { file, len })
    }

    fn start_segment(&mut self, base: u64) -> Result<(), StorageError> {
        let segment = self.open_segment(base, 0)?;
        segment.file.sync_all()?;
        sync_dir(&self.dir)?;
        self.current = Some(segment);
        Ok(())
    }
}

impl LogStore for FileLog {
    fn append(&mut self, records: &[Vec<u8>]) -> Result<(), StorageError> {
        if self.current.is_none() {
            return Err(StorageError::NotRecovered);
        }
        for record in records {
            self.pending.extend_from_slice(&frame::encode(record)?);
        }
        self.pending_records += records.len() as u64;
        Ok(())
    }

    fn sync(&mut self) -> Result<(), StorageError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let full = match &self.current {
            None => return Err(StorageError::NotRecovered),
            Some(segment) => segment.len > 0 && segment.len >= self.options.segment_bytes,
        };
        if full {
            self.start_segment(self.next_lsn)?;
        }
        let segment = self.current.as_mut().ok_or(StorageError::NotRecovered)?;
        segment
            .file
            .write_all(&self.pending)
            .map_err(|e| StorageError::SyncFailed(e.to_string()))?;
        segment
            .file
            .sync_data()
            .map_err(|e| StorageError::SyncFailed(e.to_string()))?;
        segment.len += self.pending.len() as u64;
        self.next_lsn += self.pending_records;
        self.pending.clear();
        self.pending_records = 0;
        Ok(())
    }

    fn recover_from(&mut self, from: u64) -> Result<Vec<Vec<u8>>, StorageError> {
        self.pending.clear();
        self.pending_records = 0;
        self.current = None;
        let bases = self.segments()?;
        if bases.is_empty() {
            if from > 0 {
                return Err(StorageError::Corrupt(format!(
                    "the log is empty, but records up to LSN {from} are expected"
                )));
            }
            self.next_lsn = 0;
            self.start_segment(0)?;
            return Ok(Vec::new());
        }
        let Some(start) = bases.iter().rposition(|&base| base <= from) else {
            return Err(StorageError::Corrupt(format!(
                "records from LSN {from} are missing: the oldest segment starts at LSN {}",
                bases[0]
            )));
        };
        let mut records = Vec::new();
        let mut expected = bases[start];
        for (position, &base) in bases.iter().enumerate().skip(start) {
            let path = self.path(base);
            if base != expected {
                return Err(StorageError::Corrupt(format!(
                    "segment {} starts at LSN {base}, but the previous segment ends at LSN {expected}",
                    path.display()
                )));
            }
            let mut bytes = Vec::new();
            File::open(&path)?.read_to_end(&mut bytes)?;
            let decoded = frame::decode_all(&bytes);
            let last = position + 1 == bases.len();
            if decoded.valid_len < bytes.len() {
                if !last {
                    return Err(StorageError::Corrupt(format!(
                        "segment {} is damaged at byte {} of {}, and it is not the last segment",
                        path.display(),
                        decoded.valid_len,
                        bytes.len()
                    )));
                }
                // A torn tail: truncate it durably before anything is appended after it.
                let file = OpenOptions::new().write(true).open(&path)?;
                file.set_len(decoded.valid_len as u64)?;
                file.sync_all()?;
            }
            let count = decoded.payloads.len() as u64;
            let skip = usize::try_from(from.saturating_sub(base)).unwrap_or(usize::MAX);
            records.extend(decoded.payloads.into_iter().skip(skip));
            expected = base + count;
            if last {
                self.current = Some(self.open_segment(base, decoded.valid_len as u64)?);
            }
        }
        self.next_lsn = expected;
        if from > self.next_lsn {
            return Err(StorageError::Corrupt(format!(
                "records up to LSN {from} are expected, but the log ends at LSN {}",
                self.next_lsn
            )));
        }
        Ok(records)
    }

    fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    fn trim(&mut self, before: u64) -> Result<(), StorageError> {
        let bases = self.segments()?;
        let mut removed = false;
        for pair in bases.windows(2) {
            // A segment can go when the next one starts at or before `before`: all its records are older.
            if let [base, next] = pair
                && *next <= before
            {
                fs::remove_file(self.path(*base))?;
                removed = true;
            }
        }
        if removed {
            sync_dir(&self.dir)?;
        }
        Ok(())
    }
}

/// Makes a directory's entries durable (new or removed files). A no-op where directories cannot be
/// opened as files.
fn sync_dir(dir: &Path) -> Result<(), StorageError> {
    #[cfg(unix)]
    File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "aqueduct-file-log-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn record(n: u64) -> Vec<u8> {
        let mut bytes = n.to_be_bytes().to_vec();
        bytes.resize(8 + (n % 50) as usize, n as u8);
        bytes
    }

    #[test]
    fn round_trip_across_segments_and_reopens() {
        let dir = temp_dir();
        let options = FileLogOptions { segment_bytes: 300 };
        let mut log = FileLog::open(&dir, options).unwrap();
        assert!(log.recover().unwrap().is_empty());
        for batch in 0..20u64 {
            let records: Vec<Vec<u8>> = (batch * 5..batch * 5 + 5).map(record).collect();
            log.append(&records).unwrap();
            log.sync().unwrap();
        }
        assert!(log.segments().unwrap().len() > 3, "the log rotated");
        drop(log);

        let mut log = FileLog::open(&dir, options).unwrap();
        let expected: Vec<Vec<u8>> = (0..100).map(record).collect();
        assert_eq!(log.recover().unwrap(), expected);

        // Appending after recovery continues the same log.
        log.append(&[record(100)]).unwrap();
        log.sync().unwrap();
        let mut log = FileLog::open(&dir, options).unwrap();
        assert_eq!(log.recover().unwrap().len(), 101);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_torn_tail_is_truncated_and_the_log_continues() {
        let dir = temp_dir();
        let mut log = FileLog::open(&dir, FileLogOptions::default()).unwrap();
        log.recover().unwrap();
        log.append(&[record(1), record(2)]).unwrap();
        log.sync().unwrap();
        drop(log);

        // Half of a frame, as a crash during a write would leave it.
        let path = dir.join(format!("{:020}.log", 0));
        let torn = frame::encode(&record(3)).unwrap();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&torn[..torn.len() / 2]).unwrap();
        drop(file);

        let mut log = FileLog::open(&dir, FileLogOptions::default()).unwrap();
        assert_eq!(log.recover().unwrap(), vec![record(1), record(2)]);
        log.append(&[record(4)]).unwrap();
        log.sync().unwrap();
        let mut log = FileLog::open(&dir, FileLogOptions::default()).unwrap();
        assert_eq!(
            log.recover().unwrap(),
            vec![record(1), record(2), record(4)]
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn damage_in_an_older_segment_is_reported() {
        let dir = temp_dir();
        let options = FileLogOptions { segment_bytes: 50 };
        let mut log = FileLog::open(&dir, options).unwrap();
        log.recover().unwrap();
        for n in 0..10 {
            log.append(&[record(n)]).unwrap();
            log.sync().unwrap();
        }
        drop(log);
        let first = dir.join(format!("{:020}.log", 0));
        let mut bytes = fs::read(&first).unwrap();
        bytes[10] ^= 0xFF;
        fs::write(&first, bytes).unwrap();

        let mut log = FileLog::open(&dir, options).unwrap();
        assert!(matches!(log.recover(), Err(StorageError::Corrupt(_))));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn recovery_from_an_lsn_and_trimming() {
        let dir = temp_dir();
        let options = FileLogOptions { segment_bytes: 200 };
        let mut log = FileLog::open(&dir, options).unwrap();
        log.recover().unwrap();
        for n in 0..60 {
            log.append(&[record(n)]).unwrap();
            log.sync().unwrap();
        }
        assert_eq!(log.next_lsn(), 60);
        let before = log.segments().unwrap().len();
        log.trim(40).unwrap();
        let after = log.segments().unwrap();
        assert!(after.len() < before, "old segments are deleted");
        assert!(after[0] <= 40, "nothing at or after LSN 40 is deleted");
        drop(log);

        let mut log = FileLog::open(&dir, options).unwrap();
        let tail = log.recover_from(40).unwrap();
        assert_eq!(tail, (40..60).map(record).collect::<Vec<_>>());
        assert_eq!(log.next_lsn(), 60);
        // Asking for trimmed records, or for more than the log holds, is an error.
        assert!(matches!(
            FileLog::open(&dir, options).unwrap().recover_from(0),
            Err(StorageError::Corrupt(_))
        ));
        assert!(matches!(
            FileLog::open(&dir, options).unwrap().recover_from(61),
            Err(StorageError::Corrupt(_))
        ));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn writing_before_recovery_is_refused() {
        let dir = temp_dir();
        let mut log = FileLog::open(&dir, FileLogOptions::default()).unwrap();
        assert_eq!(log.append(&[record(1)]), Err(StorageError::NotRecovered));
        fs::remove_dir_all(&dir).unwrap();
    }
}
