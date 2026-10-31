//! Measures raw fsync latency and group commit throughput on the current machine.
//!
//! On macOS, `File::sync_all` and `File::sync_data` issue `F_FULLFSYNC`, which flushes the drive's
//! write cache; on Linux they issue `fsync` and `fdatasync`. The numbers feed the performance targets
//! in BRIEF.md, section 9.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const ENTRY_BYTES: usize = 256;

pub fn run(args: &[String]) -> io::Result<()> {
    let dir = args
        .first()
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let path = dir.join(format!("aqueduct-fsync-probe-{}", std::process::id()));
    let result = probe(&path);
    let _ = std::fs::remove_file(&path);
    result
}

fn probe(path: &std::path::Path) -> io::Result<()> {
    println!(
        "fsync probe: {} on {}",
        path.display(),
        std::env::consts::OS
    );
    println!("entry size: {ENTRY_BYTES} bytes\n");

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    let entry = vec![0xA5u8; ENTRY_BYTES];

    // Warm up: extend the file so the first measurements are not dominated by allocation.
    for _ in 0..64 {
        file.write_all(&entry)?;
    }
    file.sync_all()?;

    for (label, sync) in [("sync_all", Sync::All), ("sync_data", Sync::Data)] {
        let mut samples = Vec::with_capacity(300);
        for _ in 0..300 {
            file.write_all(&entry)?;
            let start = Instant::now();
            sync.run(&file)?;
            samples.push(start.elapsed());
        }
        let stats = Stats::of(&mut samples);
        println!(
            "{label:<10} one entry per sync: p50 {:>8.3} ms  p99 {:>8.3} ms  max {:>8.3} ms",
            ms(stats.p50),
            ms(stats.p99),
            ms(stats.max)
        );
    }

    println!("\ngroup commit (sync_data after each batch):");
    println!(
        "{:>8}  {:>14}  {:>14}",
        "batch", "entries/s", "batch p99 ms"
    );
    for batch in [1usize, 8, 64, 256, 1024] {
        let batches = (4096 / batch).clamp(20, 300);
        let mut samples = Vec::with_capacity(batches);
        let buffer = entry.repeat(batch);
        let total = Instant::now();
        for _ in 0..batches {
            let start = Instant::now();
            file.write_all(&buffer)?;
            file.sync_data()?;
            samples.push(start.elapsed());
        }
        let elapsed = total.elapsed();
        let rate = (batches * batch) as f64 / elapsed.as_secs_f64();
        let stats = Stats::of(&mut samples);
        println!("{batch:>8}  {rate:>14.0}  {:>14.3}", ms(stats.p99));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Sync {
    All,
    Data,
}

impl Sync {
    fn run(self, file: &File) -> io::Result<()> {
        match self {
            Sync::All => file.sync_all(),
            Sync::Data => file.sync_data(),
        }
    }
}

struct Stats {
    p50: Duration,
    p99: Duration,
    max: Duration,
}

impl Stats {
    fn of(samples: &mut [Duration]) -> Stats {
        samples.sort_unstable();
        let at = |q: f64| samples[((samples.len() - 1) as f64 * q).round() as usize];
        Stats {
            p50: at(0.50),
            p99: at(0.99),
            max: at(1.0),
        }
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}
