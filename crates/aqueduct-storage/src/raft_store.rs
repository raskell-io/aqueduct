//! What a Raft node asks its driver to persist (ADR 0054), in a directory per node and group:
//!
//! - `hard_state` and `snapshot`: each replaced whole, atomically (written under a temporary name,
//!   synced, renamed, and the directory synced);
//! - `entries`: the log after the snapshot, as framed records ([`crate::frame`]) of
//!   [`aqueduct_raft::wire`] entries. A torn tail from a crash is cut off when the store opens; a
//!   conflict truncates the file; a snapshot rewrites it without the entries it covers.
//!
//! [`RaftStore::persist`] does what a [`Ready`] asks, in an order a crash cannot break: the snapshot,
//! then the entries (synced), then the hard state, whose commit index never runs ahead of the entries
//! on disk.

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use aqueduct_raft::wire;
use aqueduct_raft::{Entry, HardState, Ready, Snapshot};

use crate::{StorageError, frame};

/// What a store held when it was opened: what [`aqueduct_raft::RaftNode::restart`] takes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Recovered {
    /// The hard state.
    pub hard_state: HardState,
    /// The snapshot, if there is one.
    pub snapshot: Option<Snapshot>,
    /// The entries after it.
    pub entries: Vec<Entry>,
}

/// A Raft node's persistent state on disk.
#[derive(Debug)]
pub struct RaftStore {
    dir: PathBuf,
    entries: File,
    /// Each entry in the file: its index and where its frame starts.
    offsets: Vec<(u64, u64)>,
    /// The end of the valid frames.
    end: u64,
}

fn corrupt(what: &str, error: impl std::fmt::Display) -> StorageError {
    StorageError::Corrupt(format!("{what}: {error}"))
}

/// Replaces a file with these bytes, so that a crash leaves the old file or the new one.
fn replace(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), StorageError> {
    let path = dir.join(name);
    let writing = dir.join(format!("{name}.writing"));
    {
        let mut file = File::create(&writing)?;
        file.write_all(bytes)?;
        file.sync_all()
            .map_err(|e| StorageError::SyncFailed(e.to_string()))?;
    }
    std::fs::rename(&writing, &path)?;
    sync_dir(dir)
}

fn sync_dir(dir: &Path) -> Result<(), StorageError> {
    #[cfg(unix)]
    File::open(dir)?
        .sync_all()
        .map_err(|e| StorageError::SyncFailed(e.to_string()))?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// A whole file whose contents were replaced atomically, if it exists. Its bytes are one frame.
fn read_framed(dir: &Path, name: &str) -> Result<Option<Vec<u8>>, StorageError> {
    let bytes = match std::fs::read(dir.join(name)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let decoded = frame::decode_all(&bytes);
    match (decoded.payloads.len(), decoded.valid_len == bytes.len()) {
        (1, true) => Ok(decoded.payloads.into_iter().next()),
        _ => Err(corrupt(name, "not one whole frame")),
    }
}

impl RaftStore {
    /// Opens (or creates) the store in `dir`, cutting off a torn tail of the entries.
    pub fn open(dir: impl AsRef<Path>) -> Result<(RaftStore, Recovered), StorageError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let hard_state = match read_framed(&dir, "hard_state")? {
            Some(bytes) => wire::decode_hard_state(&bytes).map_err(|e| corrupt("hard_state", e))?,
            None => HardState::default(),
        };
        let snapshot = match read_framed(&dir, "snapshot")? {
            Some(bytes) => Some(wire::decode_snapshot(&bytes).map_err(|e| corrupt("snapshot", e))?),
            None => None,
        };
        let path = dir.join("entries");
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        let decoded = frame::decode_all(&bytes);
        let mut entries = Vec::new();
        let mut offsets = Vec::new();
        let mut offset = 0u64;
        for payload in &decoded.payloads {
            let entry = wire::decode_entry(payload).map_err(|e| corrupt("entries", e))?;
            offsets.push((entry.index, offset));
            offset += (payload.len() + 8) as u64;
            entries.push(entry);
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        let end = decoded.valid_len as u64;
        if end < bytes.len() as u64 {
            // A torn tail: entries the node never acknowledged as persisted.
            file.set_len(end)?;
            file.sync_all()
                .map_err(|e| StorageError::SyncFailed(e.to_string()))?;
        }
        // Entries the snapshot covers may remain if a crash came between the two writes.
        let covered = snapshot.as_ref().map_or(0, |s| s.index);
        entries.retain(|e| e.index > covered);
        let store = RaftStore {
            dir,
            entries: file,
            offsets,
            end,
        };
        Ok((
            store,
            Recovered {
                hard_state,
                snapshot,
                entries,
            },
        ))
    }

    /// Persists what a ready asks, before its messages are sent.
    pub fn persist(&mut self, ready: &Ready) -> Result<(), StorageError> {
        if let Some(snapshot) = &ready.snapshot {
            self.save_snapshot(snapshot)?;
        }
        if let Some(from) = ready.truncate_from {
            self.truncate(from)?;
        }
        if !ready.entries.is_empty() {
            self.append(&ready.entries)?;
        }
        if let Some(hard_state) = &ready.hard_state {
            let bytes = frame::encode(&wire::encode_hard_state(hard_state))?;
            replace(&self.dir, "hard_state", &bytes)?;
        }
        Ok(())
    }

    /// Saves a snapshot (a leader's, or the node's own after compaction) and drops the entries it
    /// covers.
    pub fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), StorageError> {
        let bytes = frame::encode(&wire::encode_snapshot(snapshot))?;
        replace(&self.dir, "snapshot", &bytes)?;
        self.rewrite_after(snapshot.index)
    }

    fn append(&mut self, entries: &[Entry]) -> Result<(), StorageError> {
        let mut bytes = Vec::new();
        let mut offset = self.end;
        for entry in entries {
            let framed = frame::encode(&wire::encode_entry(entry))?;
            self.offsets.push((entry.index, offset));
            offset += framed.len() as u64;
            bytes.extend_from_slice(&framed);
        }
        self.entries.seek(SeekFrom::Start(self.end))?;
        self.entries.write_all(&bytes)?;
        self.entries
            .sync_data()
            .map_err(|e| StorageError::SyncFailed(e.to_string()))?;
        self.end = offset;
        Ok(())
    }

    /// Drops the entries from index `from` on.
    fn truncate(&mut self, from: u64) -> Result<(), StorageError> {
        let Some(position) = self.offsets.iter().position(|(index, _)| *index >= from) else {
            return Ok(());
        };
        let cut = self.offsets[position].1;
        self.offsets.truncate(position);
        self.entries.set_len(cut)?;
        self.entries
            .sync_all()
            .map_err(|e| StorageError::SyncFailed(e.to_string()))?;
        self.end = cut;
        Ok(())
    }

    /// Rewrites the entries file without the entries up to `index`.
    fn rewrite_after(&mut self, index: u64) -> Result<(), StorageError> {
        let keep = self.offsets.iter().position(|(i, _)| *i > index);
        let Some(keep) = keep else {
            // Nothing after it: an empty file.
            self.entries.set_len(0)?;
            self.entries
                .sync_all()
                .map_err(|e| StorageError::SyncFailed(e.to_string()))?;
            self.offsets.clear();
            self.end = 0;
            return Ok(());
        };
        let start = self.offsets[keep].1;
        let mut tail = vec![0u8; usize::try_from(self.end - start).unwrap_or(0)];
        {
            use std::io::Read;
            self.entries.seek(SeekFrom::Start(start))?;
            self.entries.read_exact(&mut tail)?;
        }
        replace(&self.dir, "entries", &tail)?;
        self.entries = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.dir.join("entries"))?;
        self.offsets = self.offsets[keep..]
            .iter()
            .map(|(i, offset)| (*i, offset - start))
            .collect();
        self.end -= start;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("aqueduct-raft-store-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn entry(term: u64, index: u64) -> Entry {
        Entry {
            term,
            index,
            data: format!("e{index}").into_bytes(),
            members: None,
        }
    }

    #[test]
    fn what_was_persisted_is_what_reopens() {
        let dir = dir("reopen");
        let (mut store, recovered) = RaftStore::open(&dir).unwrap();
        assert_eq!(recovered, Recovered::default());
        let hard_state = HardState {
            term: 2,
            vote: Some(1),
            commit: 3,
        };
        store
            .persist(&Ready {
                hard_state: Some(hard_state),
                entries: (1..=5).map(|i| entry(1, i)).collect(),
                ..Ready::default()
            })
            .unwrap();
        // A conflict: entries from 4 on are replaced by a newer leader's.
        store
            .persist(&Ready {
                truncate_from: Some(4),
                entries: vec![entry(2, 4)],
                ..Ready::default()
            })
            .unwrap();
        let snapshot = Snapshot {
            index: 2,
            term: 1,
            data: b"two".to_vec(),
            members: vec![1, 2, 3],
        };
        store.save_snapshot(&snapshot).unwrap();
        store
            .persist(&Ready {
                entries: vec![entry(2, 5)],
                ..Ready::default()
            })
            .unwrap();
        drop(store);
        let (_, recovered) = RaftStore::open(&dir).unwrap();
        assert_eq!(recovered.hard_state, hard_state);
        assert_eq!(recovered.snapshot, Some(snapshot));
        assert_eq!(
            recovered.entries,
            vec![entry(1, 3), entry(2, 4), entry(2, 5)]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_torn_tail_is_cut_off() {
        let dir = dir("torn");
        let (mut store, _) = RaftStore::open(&dir).unwrap();
        store
            .persist(&Ready {
                entries: vec![entry(1, 1), entry(1, 2)],
                ..Ready::default()
            })
            .unwrap();
        drop(store);
        let path = dir.join("entries");
        let len = std::fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 3).unwrap();
        drop(file);
        let (mut store, recovered) = RaftStore::open(&dir).unwrap();
        assert_eq!(recovered.entries, vec![entry(1, 1)]);
        // Appending after the cut works.
        store
            .persist(&Ready {
                entries: vec![entry(2, 2)],
                ..Ready::default()
            })
            .unwrap();
        drop(store);
        let (_, recovered) = RaftStore::open(&dir).unwrap();
        assert_eq!(recovered.entries, vec![entry(1, 1), entry(2, 2)]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Random readies, snapshots and reopenings, against a model of what the disk must hold: the same
    /// rules the simulators' disks follow.
    #[test]
    fn the_store_matches_a_model_through_random_work_and_reopening() {
        let dir = dir("model");
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = move |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let (mut store, _) = RaftStore::open(&dir).unwrap();
        let mut model = Recovered::default();
        let mut term = 1;
        for _ in 0..400 {
            let last = model
                .entries
                .last()
                .map_or(model.snapshot.as_ref().map_or(0, |s| s.index), |e| e.index);
            let first = model.snapshot.as_ref().map_or(0, |s| s.index) + 1;
            match next(6) {
                0 if last >= first => {
                    // A snapshot of a prefix.
                    let index = first + next(last - first + 1);
                    let snapshot = Snapshot {
                        index,
                        term,
                        data: format!("s{index}").into_bytes(),
                        members: vec![1, 2, 3],
                    };
                    store.save_snapshot(&snapshot).unwrap();
                    model.entries.retain(|e| e.index > index);
                    model.snapshot = Some(snapshot);
                }
                1 => {
                    drop(store);
                    let (reopened, recovered) = RaftStore::open(&dir).unwrap();
                    assert_eq!(recovered, model);
                    store = reopened;
                }
                _ => {
                    // A conflict sometimes, then new entries, and a hard state.
                    let mut ready = Ready::default();
                    let mut from = last + 1;
                    if last >= first && next(3) == 0 {
                        from = first + next(last - first + 1);
                        ready.truncate_from = Some(from);
                        model.entries.retain(|e| e.index < from);
                        term += 1;
                    }
                    let count = next(4);
                    ready.entries = (from..from + count).map(|i| entry(term, i)).collect();
                    model.entries.extend(ready.entries.iter().cloned());
                    let hard_state = HardState {
                        term,
                        vote: Some(next(3) + 1),
                        commit: next(from + count),
                    };
                    ready.hard_state = Some(hard_state);
                    model.hard_state = hard_state;
                    store.persist(&ready).unwrap();
                }
            }
        }
        drop(store);
        assert_eq!(RaftStore::open(&dir).unwrap().1, model);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
