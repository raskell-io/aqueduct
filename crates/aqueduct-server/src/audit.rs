//! The audit log (ADR 0041): who did what to which run, and every refused request, as hash-chained JSON
//! Lines in `<dir>/audit.log`.
//!
//! Each line is a JSON object: `time_ms`, `who` (a token's name, or `anyone` without authentication),
//! `action`, `target`, `outcome` (`ok`, or why it was refused or failed), and `prev`, the SHA-256 of
//! the previous line (of 64 zeros for the first). Changing, removing or reordering a line breaks the
//! chain; [`verify`] checks it. Lines are synced as they are written, under an exclusive file lock, so
//! the server and the command line can both write to it. A line torn by a crash is dropped on opening:
//! it was never acknowledged.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

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
    file: Mutex<Option<Open>>,
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
            file: Mutex::new(Some(Open {
                path,
                file,
                head,
                len: whole as u64,
            })),
        })
    }

    /// An audit log that records nothing (for tests and embedded use).
    pub fn none() -> Audit {
        Audit {
            file: Mutex::new(None),
        }
    }

    /// Whether it records anything.
    pub fn is_enabled(&self) -> bool {
        self.file
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }

    /// Appends a line. A log that cannot be written is reported on standard error, not hidden.
    pub fn record(&self, who: &str, action: &str, target: &str, outcome: &str) {
        let mut guard = self.file.lock().unwrap_or_else(|e| e.into_inner());
        let Some(open) = guard.as_mut() else {
            return;
        };
        if let Err(e) = open.append(who, action, target, outcome) {
            eprintln!("audit log: cannot record {who} {action} {target} ({outcome}): {e}");
        }
    }
}

impl Open {
    fn append(
        &mut self,
        who: &str,
        action: &str,
        target: &str,
        outcome: &str,
    ) -> Result<(), String> {
        let io = |e: std::io::Error| e.to_string();
        self.file.lock().map_err(io)?;
        let written = self.append_locked(who, action, target, outcome);
        let _ = self.file.unlock();
        written
    }

    fn append_locked(
        &mut self,
        who: &str,
        action: &str,
        target: &str,
        outcome: &str,
    ) -> Result<(), String> {
        let io = |e: std::io::Error| e.to_string();
        // Another process wrote since: continue from the head it left.
        let len = self.file.metadata().map_err(io)?.len();
        if len != self.len {
            self.head = verify(&self.path)?.1;
            self.len = len;
        }
        let time_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        let line = json!({
            "time_ms": time_ms, "who": who, "action": action, "target": target,
            "outcome": outcome, "prev": self.head,
        })
        .to_string();
        writeln!(self.file, "{line}")
            .and_then(|()| self.file.sync_data())
            .map_err(io)?;
        self.head = sha256_hex(&line);
        self.len += line.len() as u64 + 1;
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
