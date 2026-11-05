//! The audit log (ADR 0041): who did what to which run, and every refused request, as hash-chained JSON
//! Lines in `<dir>/audit.log`.
//!
//! Each line is a JSON object: `time_ms`, `who` (a token's name, or `anyone` without authentication),
//! `action`, `target`, `outcome` (`ok`, or why it was refused or failed), and `prev`, the SHA-256 of
//! the previous line (of 64 zeros for the first). Changing, removing or reordering a line breaks the
//! chain; [`verify`] checks it. Lines are synced before [`Audit::record`] returns, under an exclusive file
//! lock, so the server and the command line can both write to it. Concurrent lines share a sync (group
//! commit), so the disk's flush time does not cap the server's actions per second. A line torn by a crash
//! is dropped on opening: it was never acknowledged.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

fn sha256_hex(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The audit log of a data directory.
pub fn audit_path(dir: &Path) -> PathBuf {
    dir.join("audit.log")
}

/// An open audit log: appends lines, chained to the last one.
#[derive(Debug)]
pub struct Audit {
    group: Option<Group>,
}

/// Lines waiting to be written, and the file. One caller at a time writes all that wait, and syncs them;
/// the others wait for it, and one of those that it did not cover writes next.
#[derive(Debug)]
struct Group {
    queue: Mutex<Queue>,
    written: Condvar,
    file: Mutex<Open>,
}

#[derive(Debug, Default)]
struct Queue {
    waiting: Vec<Line>,
    /// How many lines were ever queued, and how many of those are written.
    queued: u64,
    done: u64,
    /// Whether a caller is writing.
    writing: bool,
}

#[derive(Debug)]
struct Line {
    time_ms: u64,
    who: String,
    action: String,
    target: String,
    outcome: String,
}

#[derive(Debug)]
struct Open {
    path: PathBuf,
    file: std::fs::File,
    /// The hash of the last line, and the file's length after it, as this process last saw them.
    head: String,
    len: u64,
}

impl Audit {
    /// Opens (or creates) the audit log of `dir`, after verifying what it holds.
    pub fn open(dir: &Path) -> Result<Audit, String> {
        let path = audit_path(dir);
        let io = |e: std::io::Error| format!("{}: {e}", path.display());
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(io)?;
        file.lock().map_err(io)?;
        // A line without its newline was torn by a crash, and never acknowledged.
        let text = std::fs::read(&path).map_err(io)?;
        let whole = text.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
        if whole < text.len() {
            file.set_len(whole as u64).map_err(io)?;
            file.sync_all().map_err(io)?;
        }
        let head = verify(&path)?.1;
        file.unlock().map_err(io)?;
        Ok(Audit {
            group: Some(Group {
                queue: Mutex::new(Queue::default()),
                written: Condvar::new(),
                file: Mutex::new(Open {
                    path,
                    file,
                    head,
                    len: whole as u64,
                }),
            }),
        })
    }

    /// An audit log that records nothing (for tests and embedded use).
    pub fn none() -> Audit {
        Audit { group: None }
    }

    /// Whether it records anything.
    pub fn is_enabled(&self) -> bool {
        self.group.is_some()
    }

    /// Appends a line, and returns once it is synced. A log that cannot be written is reported on
    /// standard error, not hidden.
    pub fn record(&self, who: &str, action: &str, target: &str, outcome: &str) {
        let Some(group) = &self.group else {
            return;
        };
        let line = Line {
            time_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            who: who.to_owned(),
            action: action.to_owned(),
            target: target.to_owned(),
            outcome: outcome.to_owned(),
        };
        group.record(line);
    }
}

impl Group {
    fn record(&self, line: Line) {
        let mut queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        queue.waiting.push(line);
        queue.queued += 1;
        let mine = queue.queued;
        while queue.done < mine {
            if queue.writing {
                queue = self.written.wait(queue).unwrap_or_else(|e| e.into_inner());
                continue;
            }
            queue.writing = true;
            let lines = std::mem::take(&mut queue.waiting);
            let upto = queue.queued;
            drop(queue);
            self.write(&lines);
            queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
            queue.writing = false;
            queue.done = upto;
            self.written.notify_all();
        }
    }

    fn write(&self, lines: &[Line]) {
        let mut open = self.file.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(e) = open.append(lines) {
            for line in lines {
                eprintln!(
                    "audit log: cannot record {} {} {} ({}): {e}",
                    line.who, line.action, line.target, line.outcome
                );
            }
        }
    }
}

impl Open {
    fn append(&mut self, lines: &[Line]) -> Result<(), String> {
        let io = |e: std::io::Error| e.to_string();
        self.file.lock().map_err(io)?;
        let written = self.append_locked(lines);
        let _ = self.file.unlock();
        written
    }

    fn append_locked(&mut self, lines: &[Line]) -> Result<(), String> {
        let io = |e: std::io::Error| e.to_string();
        // Another process wrote since: continue from the head it left.
        let len = self.file.metadata().map_err(io)?.len();
        if len != self.len {
            self.head = verify(&self.path)?.1;
            self.len = len;
        }
        let mut head = self.head.clone();
        let mut text = String::new();
        for line in lines {
            let line = json!({
                "time_ms": line.time_ms, "who": line.who, "action": line.action,
                "target": line.target, "outcome": line.outcome, "prev": head,
            })
            .to_string();
            head = sha256_hex(&line);
            text.push_str(&line);
            text.push('\n');
        }
        // One write: a crash can tear only the last line, which opening drops.
        self.file
            .write_all(text.as_bytes())
            .and_then(|()| self.file.sync_data())
            .map_err(io)?;
        self.head = head;
        self.len += text.len() as u64;
        Ok(())
    }
}

/// Verifies an audit log's chain; its lines and the hash of the last one.
pub fn verify(path: &Path) -> Result<(Vec<Value>, String), String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut head = GENESIS.to_owned();
    let mut lines = Vec::new();
    for (number, line) in text.lines().enumerate() {
        let value: Value = serde_json::from_str(line)
            .map_err(|e| format!("audit log line {}: not JSON ({e})", number + 1))?;
        if value["prev"].as_str() != Some(head.as_str()) {
            return Err(format!(
                "audit log line {}: the chain is broken (a line was changed, removed or reordered)",
                number + 1
            ));
        }
        head = sha256_hex(line);
        lines.push(value);
    }
    Ok((lines, head))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_lines_share_a_write_and_wait_for_it() {
        let dir = std::env::temp_dir().join(format!("aqueduct-audit-group-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let audit = std::sync::Arc::new(Audit::open(&dir).unwrap());
        let group = audit.group.as_ref().unwrap();
        let finished = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // While the file is held, the first caller cannot write, and the others queue behind it.
        let held = group.file.lock().unwrap();
        let callers: Vec<_> = (0..8)
            .map(|i| {
                let (audit, finished) = (audit.clone(), finished.clone());
                std::thread::spawn(move || {
                    audit.record("ada", "start", &format!("k{i}"), "ok");
                    finished.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                })
            })
            .collect();
        while group.queue.lock().unwrap().queued < 8 {
            std::thread::yield_now();
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert_eq!(
            finished.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "a line is acknowledged only once it is synced"
        );
        // At most one waits for the first write, so the rest go out together: seven lines in one write.
        assert_eq!(group.queue.lock().unwrap().waiting.len(), 7);
        drop(held);
        for caller in callers {
            caller.join().unwrap();
        }
        assert_eq!(group.queue.lock().unwrap().done, 8);
        let (lines, _) = verify(&audit_path(&dir)).unwrap();
        assert_eq!(lines.len(), 8);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn many_writers_keep_one_chain() {
        let dir = std::env::temp_dir().join(format!("aqueduct-audit-many-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Two logs on one file, as the server and the command line are, each with concurrent callers.
        let logs = [
            std::sync::Arc::new(Audit::open(&dir).unwrap()),
            std::sync::Arc::new(Audit::open(&dir).unwrap()),
        ];
        let callers: Vec<_> = (0..16)
            .map(|i| {
                let audit = logs[i % 2].clone();
                std::thread::spawn(move || {
                    for n in 0..25 {
                        audit.record("ada", "start", &format!("{i}-{n}"), "ok");
                    }
                })
            })
            .collect();
        for caller in callers {
            caller.join().unwrap();
        }
        let (lines, _) = verify(&audit_path(&dir)).unwrap();
        assert_eq!(lines.len(), 400);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_log_is_chained_and_tampering_shows() {
        let dir = std::env::temp_dir().join(format!("aqueduct-audit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        {
            let audit = Audit::open(&dir).unwrap();
            audit.record("ada", "start", "claim-7", "ok");
            audit.record(
                "anyone",
                "cancel",
                "claim-7",
                "refused: unknown or revoked token",
            );
        }
        // Reopened, it continues the chain.
        Audit::open(&dir)
            .unwrap()
            .record("ada", "signal", "claim-7", "ok");
        // Two writers at once keep one chain.
        let (first, second) = (Audit::open(&dir).unwrap(), Audit::open(&dir).unwrap());
        first.record("server", "start", "a", "ok");
        second.record("cli", "token create", "b", "ok");
        first.record("server", "start", "c", "ok");
        let (lines, _) = verify(&audit_path(&dir)).unwrap();
        assert_eq!(lines.len(), 6);
        // A line torn by a crash is dropped on opening.
        let mut torn = std::fs::read(audit_path(&dir)).unwrap();
        torn.extend_from_slice(br#"{"time_ms":1,"who":"#);
        std::fs::write(audit_path(&dir), &torn).unwrap();
        Audit::open(&dir)
            .unwrap()
            .record("ada", "cancel", "claim-7", "ok");
        let (lines, _) = verify(&audit_path(&dir)).unwrap();
        assert_eq!(lines.len(), 7);
        assert_eq!(lines[1]["outcome"], "refused: unknown or revoked token");
        let text = std::fs::read_to_string(audit_path(&dir)).unwrap();
        std::fs::write(audit_path(&dir), text.replace("\"ada\"", "\"eve\"")).unwrap();
        assert!(
            verify(&audit_path(&dir))
                .unwrap_err()
                .contains("chain is broken")
        );
        assert!(Audit::open(&dir).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
