//! Materialised state (ADR 0009): checkpoints of every run's records, so recovery replays only the log
//! written after the last checkpoint, and the log can be trimmed before it.

use std::collections::BTreeMap;
use std::path::Path;

use crate::{StorageError, frame};

/// A checkpoint: each run's journal records as of a log position.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Checkpoint {
    /// The checkpoint covers every log record before this LSN.
    pub lsn: u64,
    /// Each run's records, in journal order, by run key.
    pub runs: BTreeMap<String, Vec<Vec<u8>>>,
}

/// Durable storage for checkpoints.
pub trait StateStore {
    /// Loads the last saved checkpoint (an empty one at LSN 0 if none was saved).
    fn load(&mut self) -> Result<Checkpoint, StorageError>;

    /// Atomically and durably records the given runs' complete records and the new checkpoint LSN. Runs
    /// not mentioned keep their saved records. Called only when every log record before `lsn` is durable.
    fn save(&mut self, lsn: u64, runs: &[(String, Vec<Vec<u8>>)]) -> Result<(), StorageError>;

    /// Loads one run's saved records, if the run is in the last checkpoint.
    fn load_run(&mut self, key: &str) -> Result<Option<Vec<Vec<u8>>>, StorageError> {
        Ok(self.load()?.runs.remove(key))
    }
}

/// No state store: recovery always replays the whole log.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoStateStore;

impl StateStore for NoStateStore {
    fn load(&mut self) -> Result<Checkpoint, StorageError> {
        Ok(Checkpoint::default())
    }

    fn save(&mut self, _lsn: u64, _runs: &[(String, Vec<Vec<u8>>)]) -> Result<(), StorageError> {
        Ok(())
    }
}

const RUNS: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("runs");
const META: redb::TableDefinition<&str, u64> = redb::TableDefinition::new("meta");
const CHECKPOINT: &str = "checkpoint_lsn";

/// Checkpoints in a redb database: one row per run holding its framed records, and the checkpoint LSN,
/// written in one durable transaction.
#[derive(Debug)]
pub struct RedbStateStore {
    db: redb::Database,
}

impl RedbStateStore {
    /// Opens (or creates) the state store at `path`.
    pub fn open(path: &Path) -> Result<RedbStateStore, StorageError> {
        if !path.exists() {
            create_atomically(path)?;
        }
        let db = redb::Database::create(path).map_err(io)?;
        Ok(RedbStateStore { db })
    }
}

/// Creates a new state store so that a crash at any moment leaves either no store or a complete one.
/// Creating a redb file in place is not atomic: a crash part-way leaves a file that is not a redb
/// database, and every later open fails. So the store is built under a temporary name, synced, and
/// renamed into place; a leftover temporary file from an earlier crash is discarded.
fn create_atomically(path: &Path) -> Result<(), StorageError> {
    let creating = path.with_extension("redb.creating");
    if creating.exists() {
        std::fs::remove_file(&creating)?;
    }
    {
        let db = redb::Database::create(&creating).map_err(io)?;
        let txn = db.begin_write().map_err(io)?;
        {
            // Create the tables, so that readers never see them missing.
            txn.open_table(RUNS).map_err(io)?;
            txn.open_table(META).map_err(io)?;
        }
        txn.commit().map_err(io)?;
    }
    std::fs::File::open(&creating)?.sync_all()?;
    std::fs::rename(&creating, path)?;
    if let Some(dir) = path.parent() {
        #[cfg(unix)]
        std::fs::File::open(dir)?.sync_all()?;
        #[cfg(not(unix))]
        let _ = dir;
    }
    Ok(())
}

impl StateStore for RedbStateStore {
    fn load(&mut self) -> Result<Checkpoint, StorageError> {
        use redb::{ReadableDatabase, ReadableTable};
        let txn = self.db.begin_read().map_err(io)?;
        let meta = txn.open_table(META).map_err(io)?;
        let lsn = meta.get(CHECKPOINT).map_err(io)?.map_or(0, |v| v.value());
        let table = txn.open_table(RUNS).map_err(io)?;
        let mut runs = BTreeMap::new();
        for row in table.iter().map_err(io)? {
            let (key, value) = row.map_err(io)?;
            let decoded = frame::decode_all(value.value());
            if decoded.valid_len != value.value().len() {
                return Err(StorageError::Corrupt(format!(
                    "the checkpoint of run {} does not decode",
                    key.value()
                )));
            }
            runs.insert(key.value().to_owned(), decoded.payloads);
        }
        Ok(Checkpoint { lsn, runs })
    }

    fn save(&mut self, lsn: u64, runs: &[(String, Vec<Vec<u8>>)]) -> Result<(), StorageError> {
        let mut txn = self.db.begin_write().map_err(io)?;
        txn.set_durability(redb::Durability::Immediate)
            .map_err(io)?;
        {
            let mut table = txn.open_table(RUNS).map_err(io)?;
            for (key, records) in runs {
                let mut value = Vec::new();
                for record in records {
                    value.extend_from_slice(&frame::encode(record)?);
                }
                table.insert(key.as_str(), value.as_slice()).map_err(io)?;
            }
            let mut meta = txn.open_table(META).map_err(io)?;
            meta.insert(CHECKPOINT, lsn).map_err(io)?;
        }
        txn.commit()
            .map_err(|e| StorageError::SyncFailed(e.to_string()))?;
        Ok(())
    }

    fn load_run(&mut self, key: &str) -> Result<Option<Vec<Vec<u8>>>, StorageError> {
        use redb::ReadableDatabase;
        let txn = self.db.begin_read().map_err(io)?;
        let table = txn.open_table(RUNS).map_err(io)?;
        let Some(value) = table.get(key).map_err(io)? else {
            return Ok(None);
        };
        let decoded = frame::decode_all(value.value());
        if decoded.valid_len != value.value().len() {
            return Err(StorageError::Corrupt(format!(
                "the checkpoint of run {key} does not decode"
            )));
        }
        Ok(Some(decoded.payloads))
    }
}

fn io(error: impl std::fmt::Display) -> StorageError {
    StorageError::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoints_round_trip_and_merge() {
        let dir = std::env::temp_dir().join(format!("aqueduct-state-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.redb");
        {
            let mut store = RedbStateStore::open(&path).unwrap();
            assert_eq!(store.load().unwrap(), Checkpoint::default());
            store
                .save(
                    10,
                    &[
                        ("a".into(), vec![b"1".to_vec(), b"2".to_vec()]),
                        ("b".into(), vec![b"x".to_vec()]),
                    ],
                )
                .unwrap();
            store
                .save(
                    15,
                    &[(
                        "a".into(),
                        vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()],
                    )],
                )
                .unwrap();
        }
        let mut store = RedbStateStore::open(&path).unwrap();
        let checkpoint = store.load().unwrap();
        assert_eq!(checkpoint.lsn, 15);
        assert_eq!(checkpoint.runs["a"].len(), 3);
        assert_eq!(
            checkpoint.runs["b"],
            vec![b"x".to_vec()],
            "runs not saved again keep their records"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn an_interrupted_creation_is_discarded() {
        // A crash while the store was first being created leaves only the temporary file behind.
        let dir =
            std::env::temp_dir().join(format!("aqueduct-state-create-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.redb");
        std::fs::write(path.with_extension("redb.creating"), b"half-written").unwrap();
        let mut store = RedbStateStore::open(&path).unwrap();
        assert_eq!(store.load().unwrap(), Checkpoint::default());
        assert!(!path.with_extension("redb.creating").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
