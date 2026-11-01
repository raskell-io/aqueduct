//! `cargo xtask crash`: kills a log writer with SIGKILL at random moments and checks that no
//! acknowledged record is lost (BRIEF.md section 10.2, phase 1 acceptance: 1,000 random kills).
//!
//! The writer (`cargo xtask crash-child <dir> <seed>`) recovers the log, appends batches of records
//! whose content is a pure function of their position, and prints the last position after every
//! successful sync. The harness kills it after a random delay, then checks the log:
//!
//! - every acknowledged record is present, in order, with exactly the expected content;
//! - every record after that is also an expected record (unacknowledged but durable is fine);
//! - nothing else is in the log.
//!
//! This tests process crashes on a real disk. It does not simulate power loss: the OS page cache
//! survives a killed process. Power loss is covered by the simulator's disk model and needs a VM-level
//! harness on real hardware later.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use aqueduct_sim::Rng;
use aqueduct_storage::{FileLog, FileLogOptions, LogStore};

/// Segments are kept small so that the harness also kills writers in the middle of a rotation.
const OPTIONS: FileLogOptions = FileLogOptions {
    segment_bytes: 64 << 10,
    durability: aqueduct_storage::Durability::Fsync,
};

/// The expected record at a position: its position, then 0 to 299 bytes derived from it.
pub fn expected(position: u64) -> Vec<u8> {
    let mut rng = Rng::new(position ^ 0xA9_0E_DC_7F);
    let len = rng.below(300) as usize;
    let mut bytes = position.to_be_bytes().to_vec();
    bytes.extend((0..len).map(|_| rng.below(256) as u8));
    bytes
}

pub fn run(args: &[String]) -> Result<(), String> {
    let mut kills = 100u64;
    let mut seed = 1u64;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        let mut value = || -> Result<u64, String> {
            iter.next()
                .ok_or(format!("{arg} needs a value"))?
                .parse()
                .map_err(|e| format!("{arg}: {e}"))
        };
        match arg.as_str() {
            "--kills" => kills = value()?,
            "--seed" => seed = value()?,
            other => {
                return Err(format!(
                    "unknown argument {other}; use --kills N [--seed S]"
                ));
            }
        }
    }

    let dir = std::env::temp_dir().join(format!("aqueduct-crash-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let mut rng = Rng::new(seed);
    let started = Instant::now();
    let mut acknowledged: Option<u64> = None;
    let mut torn_or_unacked = 0u64;

    for kill in 0..kills {
        let mut child = Command::new(&exe)
            .args([
                "crash-child",
                &dir.display().to_string(),
                &rng.next_u64().to_string(),
            ])
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| format!("cannot start the writer: {e}"))?;
        let stdout = child.stdout.take().ok_or("no stdout")?;
        let (sender, receiver) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });

        // Let it write for a random while, then kill it without warning.
        std::thread::sleep(Duration::from_millis(5 + rng.below(80)));
        child.kill().map_err(|e| format!("kill failed: {e}"))?;
        child.wait().map_err(|e| e.to_string())?;
        for line in receiver.try_iter() {
            let position: u64 = line
                .trim()
                .parse()
                .map_err(|e| format!("bad ack {line:?}: {e}"))?;
            if acknowledged.is_some_and(|a| position < a) {
                return Err(format!("kill {kill}: acknowledgements went backwards"));
            }
            acknowledged = Some(position);
        }

        let records =
            verify(&dir, acknowledged).map_err(|e| format!("kill {kill} (seed {seed}): {e}"))?;
        torn_or_unacked += records.saturating_sub(acknowledged.map_or(0, |a| a + 1));
    }

    println!(
        "{kills} kills passed in {:.1} s: {} records acknowledged, {} durable beyond the last acknowledgement at the end",
        started.elapsed().as_secs_f64(),
        acknowledged.map_or(0, |a| a + 1),
        torn_or_unacked
    );
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

/// Recovers the log and checks it against the acknowledgements. Returns the number of records.
fn verify(dir: &Path, acknowledged: Option<u64>) -> Result<u64, String> {
    let mut log = FileLog::open(dir, OPTIONS).map_err(|e| e.to_string())?;
    let records = log.recover().map_err(|e| format!("recovery failed: {e}"))?;
    if let Some(last) = acknowledged
        && (records.len() as u64) <= last
    {
        return Err(format!(
            "lost acknowledged records: {} recovered, {} acknowledged",
            records.len(),
            last + 1
        ));
    }
    for (position, record) in records.iter().enumerate() {
        if *record != expected(position as u64) {
            return Err(format!(
                "record {position} is not the record that was written"
            ));
        }
    }
    Ok(records.len() as u64)
}

/// The writer process.
pub fn child(args: &[String]) -> Result<(), String> {
    let [dir, seed] = args else {
        return Err("usage: crash-child <dir> <seed>".into());
    };
    let seed: u64 = seed.parse().map_err(|e| format!("seed: {e}"))?;
    let mut rng = Rng::new(seed);
    let mut log = FileLog::open(PathBuf::from(dir), OPTIONS).map_err(|e| e.to_string())?;
    let mut next = log.recover().map_err(|e| e.to_string())?.len() as u64;
    let stdout = std::io::stdout();
    loop {
        let batch = 1 + rng.below(64);
        let records: Vec<Vec<u8>> = (next..next + batch).map(expected).collect();
        // Injected bug for checking the harness itself: acknowledge before the write.
        let early = std::env::var_os("AQUEDUCT_CRASH_BUG").is_some_and(|v| v == "ack-before-write");
        if early {
            acknowledge(&stdout, next + batch - 1)?;
            std::thread::sleep(Duration::from_millis(1));
        }
        log.append(&records).map_err(|e| e.to_string())?;
        log.sync().map_err(|e| e.to_string())?;
        next += batch;
        if !early {
            acknowledge(&stdout, next - 1)?;
        }
    }
}

fn acknowledge(stdout: &std::io::Stdout, position: u64) -> Result<(), String> {
    let mut out = stdout.lock();
    writeln!(out, "{position}").map_err(|e| e.to_string())?;
    out.flush().map_err(|e| e.to_string())
}
