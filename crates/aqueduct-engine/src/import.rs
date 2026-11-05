//! Moving a data directory to a server (ADR 0047).
//!
//! The source is frozen first, so that no run ever goes on in two places. On the server, what arrives is
//! staged in `import/stage`, and on commit renamed to `import/pending`. The next time the engine opens, it
//! applies what is pending before any run's recovery effects, so no run acts on a partial import (a parent
//! would otherwise start a child whose history had not arrived). A crash while applying is repaired by
//! applying again: runs already there are skipped, and one that was torn gets the rest of its records.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use aqueduct_codec::verify_chain;
use aqueduct_core::Entry;
use aqueduct_storage::{LogStore, StateStore};
use serde_json::{Value, json};

use crate::namespaces::{DEFAULT, namespace_of};
use crate::partition::Partition;

/// In a source directory: where its runs went.
const MARKER: &str = "migrated.json";
const IMPORTS: &str = "import";
const STAGE: &str = "stage";
const PENDING: &str = "pending";
const RUNS: &str = "runs";

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Whether a source directory has been frozen, and why an engine must not open it: its runs are, or are
/// about to be, on a server.
pub fn frozen(dir: &Path) -> Option<String> {
    let text = std::fs::read_to_string(dir.join(MARKER)).ok()?;
    let marker: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
    let to = marker["to"].as_str().unwrap_or("a server");
    Some(match marker["state"].as_str() {
        Some("migrating") => format!(
            "{} is being migrated to {to}, and cannot run: finish with aqueduct migrate, or undo with \
             aqueduct migrate --from <dir> --abort",
            dir.display()
        ),
        _ => format!(
            "{} was migrated to {to}: its runs go on there, and running them here too would repeat their steps",
            dir.display()
        ),
    })
}

/// The state of a frozen source: `migrating` or `migrated`.
pub fn frozen_state(dir: &Path) -> Option<String> {
    let text = std::fs::read_to_string(dir.join(MARKER)).ok()?;
    let marker: Value = serde_json::from_str(&text).ok()?;
    marker["state"].as_str().map(str::to_owned)
}

/// Freezes a source directory, durably: an engine will not open it from here on.
pub fn freeze(dir: &Path, to: &str, state: &str) -> Result<(), String> {
    let io = |e: std::io::Error| format!("{}: {e}", dir.display());
    let marker = json!({"to": to, "state": state, "at_ms": now_ms()}).to_string();
    let temporary = dir.join("migrated.json.new");
    let mut file = File::create(&temporary).map_err(io)?;
    file.write_all(marker.as_bytes()).map_err(io)?;
    file.sync_all().map_err(io)?;
    std::fs::rename(&temporary, dir.join(MARKER)).map_err(io)?;
    sync_dir(dir).map_err(io)
}

/// Undoes [`freeze`].
pub fn thaw(dir: &Path) -> Result<(), String> {
    let io = |e: std::io::Error| format!("{}: {e}", dir.display());
    std::fs::remove_file(dir.join(MARKER)).map_err(io)?;
    sync_dir(dir).map_err(io)
}

/// How a run on the server stands to the same run's history from an import.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Relation {
    /// The server has all of it, and perhaps has gone on from it.
    AlreadyThere,
    /// The server has the start of it, and nothing else.
    Partial,
    /// They differ: the key has another run here.
    Conflict,
}

/// How `existing` stands to `incoming`.
pub fn relation(existing: &[Entry], incoming: &[Entry]) -> Relation {
    let common = existing.len().min(incoming.len());
    if existing[..common] != incoming[..common] {
        Relation::Conflict
    } else if existing.len() >= incoming.len() {
        Relation::AlreadyThere
    } else {
        Relation::Partial
    }
}

/// An upload in progress. One at a time; dropping it abandons it, and the next one clears what it left.
#[derive(Debug)]
pub struct Stage {
    imports: PathBuf,
    dir: PathBuf,
    runs: BufWriter<File>,
    /// Held while the stage is written: the lock on `import/lock`.
    _lock: File,
}

impl Stage {
    /// Starts an upload into a data directory.
    pub fn begin(data_dir: &Path) -> Result<Stage, String> {
        let imports = data_dir.join(IMPORTS);
        let io = |e: std::io::Error| format!("{}: {e}", imports.display());
        std::fs::create_dir_all(&imports).map_err(io)?;
        let lock = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(imports.join("lock"))
            .map_err(io)?;
        if lock.try_lock().is_err() {
            return Err("another import is being uploaded".to_owned());
        }
        if imports.join(PENDING).exists() {
            return Err(
                "an import is waiting for this server to restart: restart it, then try again"
                    .to_owned(),
            );
        }
        let dir = imports.join(STAGE);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).map_err(io)?;
        }
        std::fs::create_dir_all(dir.join("registry/sources")).map_err(io)?;
        let runs = BufWriter::new(File::create(dir.join(RUNS)).map_err(io)?);
        Ok(Stage {
            imports,
            dir,
            runs,
            _lock: lock,
        })
    }

    /// Adds a run's history, after verifying it. A run's hash chain starts from its key, so a run keeps
    /// its key, and only the default namespace's keys are taken: one tenant's token, or a file, cannot
    /// write into another's. Returns its entries.
    pub fn add_run(&mut self, key: &str, records: &[Vec<u8>]) -> Result<Vec<Entry>, String> {
        if namespace_of(key) != DEFAULT {
            return Err(format!("run `{key}` is not in the default namespace"));
        }
        let verified = verify_chain(key, records.iter().map(Vec::as_slice))
            .map_err(|e| format!("run `{key}`: its history does not verify: {e}"))?;
        aqueduct_core::RunState::from_journal(&verified.entries)
            .map_err(|e| format!("run `{key}`: its journal is invalid: {e}"))?;
        let io = |e: std::io::Error| e.to_string();
        let write_bytes = |out: &mut BufWriter<File>, bytes: &[u8]| {
            let length = u32::try_from(bytes.len()).map_err(|e| e.to_string())?;
            out.write_all(&length.to_le_bytes()).map_err(io)?;
            out.write_all(bytes).map_err(io)
        };
        write_bytes(&mut self.runs, key.as_bytes())?;
        let count = u32::try_from(records.len()).map_err(|e| e.to_string())?;
        self.runs.write_all(&count.to_le_bytes()).map_err(io)?;
        for record in records {
            write_bytes(&mut self.runs, record)?;
        }
        Ok(verified.entries)
    }

    /// Adds a procedure version's source. Its id is checked when the registry opens it.
    pub fn add_source(&mut self, version: &str, text: &[u8]) -> Result<(), String> {
        if version.len() != 64 || !version.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("`{version}` is not a version id"));
        }
        self.write_file(&format!("registry/sources/{version}.star"), text)
    }

    /// Adds a registry file: `events.log`, `policy.json` or `identities.json`.
    pub fn add_registry_file(&mut self, name: &str, data: &[u8]) -> Result<(), String> {
        if !["events.log", "policy.json", "identities.json"].contains(&name) {
            return Err(format!("`{name}` is not a registry file"));
        }
        self.write_file(&format!("registry/{name}"), data)
    }

    fn write_file(&self, relative: &str, data: &[u8]) -> Result<(), String> {
        let io = |e: std::io::Error| format!("{relative}: {e}");
        let mut file = File::create(self.dir.join(relative)).map_err(io)?;
        file.write_all(data).map_err(io)?;
        file.sync_all().map_err(io)
    }

    /// Makes the upload durable and pending: the engine applies it the next time it opens.
    pub fn commit(mut self) -> Result<(), String> {
        let io = |e: std::io::Error| format!("{}: {e}", self.dir.display());
        self.runs.flush().map_err(io)?;
        self.runs.get_ref().sync_all().map_err(io)?;
        sync_dir(&self.dir.join("registry/sources")).map_err(io)?;
        sync_dir(&self.dir.join("registry")).map_err(io)?;
        sync_dir(&self.dir).map_err(io)?;
        std::fs::rename(&self.dir, self.imports.join(PENDING)).map_err(io)?;
        sync_dir(&self.imports).map_err(io)
    }
}

/// The pending import's directory, if there is one. Its `registry` is laid out as a data directory's.
pub fn pending(data_dir: &Path) -> Option<PathBuf> {
    let dir = data_dir.join(IMPORTS).join(PENDING);
    dir.is_dir().then_some(dir)
}

/// Sets the pending import aside, with why; it will not be applied.
pub fn reject(data_dir: &Path, why: &str) -> Result<(), String> {
    set_aside(data_dir, "rejected", why)
}

fn set_aside(data_dir: &Path, outcome: &str, note: &str) -> Result<(), String> {
    let imports = data_dir.join(IMPORTS);
    let io = |e: std::io::Error| format!("{}: {e}", imports.display());
    let from = imports.join(PENDING);
    let _ = std::fs::write(from.join("outcome.txt"), note);
    std::fs::rename(&from, imports.join(format!("{outcome}-{}", now_ms()))).map_err(io)?;
    sync_dir(&imports).map_err(io)
}

/// What applying an import did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportReport {
    /// Runs added or completed.
    pub imported: usize,
    /// Runs the partition had already.
    pub already_there: usize,
    /// Why nothing was applied, if nothing was.
    pub rejected: Option<String>,
}

/// A run in a stage: its key and records.
type StagedRun = (String, Vec<Vec<u8>>);

/// One run read from a stage.
fn read_run(input: &mut impl Read) -> Result<Option<StagedRun>, String> {
    let io = |e: std::io::Error| format!("the staged runs: {e}");
    let mut number = [0u8; 4];
    match input.read_exact(&mut number) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(io(e)),
    }
    let mut read_bytes = |length: u32| -> Result<Vec<u8>, String> {
        let mut bytes = vec![0u8; length as usize];
        input.read_exact(&mut bytes).map_err(io)?;
        Ok(bytes)
    };
    let key = String::from_utf8(read_bytes(u32::from_le_bytes(number))?)
        .map_err(|e| format!("the staged runs: {e}"))?;
    let count = u32::from_le_bytes(
        <[u8; 4]>::try_from(read_bytes(4)?).map_err(|_| "the staged runs are torn".to_owned())?,
    );
    let mut records = Vec::new();
    for _ in 0..count {
        let length = u32::from_le_bytes(
            <[u8; 4]>::try_from(read_bytes(4)?)
                .map_err(|_| "the staged runs are torn".to_owned())?,
        );
        records.push(read_bytes(length)?);
    }
    Ok(Some((key, records)))
}

fn for_each_run(
    stage: &Path,
    mut each: impl FnMut(&str, &[Vec<u8>]) -> Result<(), String>,
) -> Result<(), String> {
    let file = File::open(stage.join(RUNS)).map_err(|e| format!("the staged runs: {e}"))?;
    let mut input = BufReader::new(file);
    while let Some((key, records)) = read_run(&mut input)? {
        each(&key, &records)?;
    }
    Ok(())
}

/// Applies the pending import to a loaded partition whose recovery effects have not run. All of it, or,
/// if a run conflicts with one the partition has, none of it: it is then set aside and reported, and the
/// engine opens without it.
pub(crate) fn apply_pending<L: LogStore, S: StateStore>(
    partition: &mut Partition<L, S>,
    data_dir: &Path,
) -> Result<Option<ImportReport>, String> {
    let Some(stage) = pending(data_dir) else {
        return Ok(None);
    };
    let (mut imported, mut already_there) = (0, 0);
    let mut conflicts = Vec::new();
    for_each_run(&stage, |key, records| {
        let verified = verify_chain(key, records.iter().map(Vec::as_slice))
            .map_err(|e| format!("run `{key}`: its history does not verify: {e}"))?;
        if let Some(run) = partition.run(key)
            && relation(run.journal(), &verified.entries) == Relation::Conflict
        {
            conflicts.push(key.to_owned());
        }
        Ok(())
    })?;
    if !conflicts.is_empty() {
        conflicts.truncate(10);
        let why = format!("these keys have other runs here: {}", conflicts.join(", "));
        reject(data_dir, &why)?;
        return Ok(Some(ImportReport {
            imported: 0,
            already_there: 0,
            rejected: Some(why),
        }));
    }
    for_each_run(&stage, |key, records| {
        if partition
            .import_run(key, records)
            .map_err(|e| e.to_string())?
        {
            imported += 1;
        } else {
            already_there += 1;
        }
        Ok(())
    })?;
    partition.flush().map_err(|e| e.to_string())?;
    set_aside(
        data_dir,
        "applied",
        &format!("{imported} imported, {already_there} already there"),
    )?;
    Ok(Some(ImportReport {
        imported,
        already_there,
        rejected: None,
    }))
}
