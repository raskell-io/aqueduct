//! Phase 0 storage benchmark (ADR 0009): redb vs fjall vs a plain framed log file.
//!
//! Run with `cargo run --release -p aqueduct-bench-storage [-- <dir>]`. Prints Markdown tables.
//! Both engines sync with `File::sync_all`/`sync_data`, which on macOS is `F_FULLFSYNC`, like the
//! plain log and like `cargo xtask fsync`.

#![forbid(unsafe_code)]

use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use aqueduct_storage::frame;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const RECORD: usize = 256;
const STATE_VALUE: usize = 512;
const STATE_RUNS: u64 = 200_000;
const BATCHES: [usize; 4] = [1, 16, 128, 1024];
const POOL: usize = 1 << 20;

/// Incompressible bytes, so that compressing engines get no unrealistic advantage.
fn pool() -> &'static [u8] {
    static POOL_BYTES: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    POOL_BYTES.get_or_init(|| {
        let mut state = 0xC0FFEE_u64;
        (0..POOL / 8)
            .flat_map(|_| splitmix(&mut state).to_le_bytes())
            .collect()
    })
}

/// A distinct, incompressible slice of `len` bytes for record `n`.
fn noise(n: u64, len: usize) -> &'static [u8] {
    let start = usize::try_from(n.wrapping_mul(997)).unwrap_or(0) % (POOL - len);
    &pool()[start..start + len]
}

fn main() -> Result<()> {
    let root = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!("aqueduct-bench-storage-{}", std::process::id()));
    std::fs::create_dir_all(&root)?;
    let result = run(&root);
    let _ = std::fs::remove_dir_all(&root);
    result
}

fn run(root: &Path) -> Result<()> {
    println!(
        "storage benchmark on {} ({}), records {RECORD} B\n",
        root.display(),
        std::env::consts::OS
    );

    println!("## Durable journal appends (fsync on every commit)\n");
    println!("| engine | batch | records/s | commit p50 | commit p99 |");
    println!("|---|---:|---:|---:|---:|");
    for batch in BATCHES {
        let commits = commits_for(batch);
        for engine in Engine::ALL {
            let dir = root.join(format!("append-{}-{batch}", engine.slug()));
            let mut store = engine.open(&dir)?;
            let stats = append(&mut *store, batch, commits, true)?;
            println!(
                "| {} | {batch} | {:.0} | {} | {} |",
                engine.name(),
                stats.rate,
                ms(stats.p50),
                ms(stats.p99)
            );
        }
    }

    println!("\n## Journal appends without fsync (batch 128)\n");
    println!("| engine | records/s |");
    println!("|---|---:|");
    for engine in Engine::ALL {
        let dir = root.join(format!("append-os-{}", engine.slug()));
        let mut store = engine.open(&dir)?;
        let stats = append(&mut *store, 128, 400, false)?;
        println!("| {} | {:.0} |", engine.name(), stats.rate);
    }

    println!(
        "\n## Run state and recovery ({STATE_RUNS} runs, {STATE_VALUE} B state, {STATE_RUNS} log records)\n"
    );
    println!("| engine | load records/s | reopen | point reads/s | log scan records/s | disk |");
    println!("|---|---:|---:|---:|---:|---:|");
    for engine in Engine::ALL {
        let dir = root.join(format!("state-{}", engine.slug()));
        let mut store = engine.open(&dir)?;
        let started = Instant::now();
        for chunk in 0..STATE_RUNS / 1000 {
            let runs: Vec<u64> = (chunk * 1000..(chunk + 1) * 1000).collect();
            store.write_state(&runs)?;
        }
        store.sync()?;
        let load = (2 * STATE_RUNS) as f64 / started.elapsed().as_secs_f64();
        drop(store);

        let started = Instant::now();
        let mut store = engine.open(&dir)?;
        let reopen = started.elapsed();

        let reads = if engine.has_state() {
            let mut rng = 0x5eed_u64;
            let started = Instant::now();
            for _ in 0..STATE_RUNS {
                let run = splitmix(&mut rng) % STATE_RUNS;
                if !store.read_state(run)? {
                    return Err(format!("{}: missing state for run {run}", engine.name()).into());
                }
            }
            format!("{:.0}", STATE_RUNS as f64 / started.elapsed().as_secs_f64())
        } else {
            "n/a".to_owned()
        };

        let started = Instant::now();
        let scanned = store.scan_log()?;
        let scan = scanned as f64 / started.elapsed().as_secs_f64();
        if scanned != STATE_RUNS {
            return Err(format!("{}: scanned {scanned} log records", engine.name()).into());
        }
        let disk = dir_size(&dir)?;
        println!(
            "| {} | {load:.0} | {} | {reads} | {scan:.0} | {:.1} MB |",
            engine.name(),
            ms(reopen),
            disk as f64 / 1e6
        );
    }
    Ok(())
}

fn commits_for(batch: usize) -> usize {
    (20_000 / batch).clamp(60, 400)
}

struct AppendStats {
    rate: f64,
    p50: Duration,
    p99: Duration,
}

fn append(
    store: &mut dyn Store,
    batch: usize,
    commits: usize,
    durable: bool,
) -> Result<AppendStats> {
    let mut lsn = 0u64;
    let mut samples = Vec::with_capacity(commits);
    let started = Instant::now();
    for _ in 0..commits {
        let records: Vec<(u64, &[u8])> = (0..batch)
            .map(|_| {
                lsn += 1;
                (lsn, noise(lsn, RECORD))
            })
            .collect();
        let commit = Instant::now();
        store.append(&records, durable)?;
        samples.push(commit.elapsed());
    }
    let rate = (commits * batch) as f64 / started.elapsed().as_secs_f64();
    samples.sort_unstable();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q).round() as usize];
    Ok(AppendStats {
        rate,
        p50: at(0.5),
        p99: at(0.99),
    })
}

/// The operations the engine needs from local storage in phase 1.
trait Store {
    /// Appends log records (key: LSN) in one commit, durable or not.
    fn append(&mut self, records: &[(u64, &[u8])], durable: bool) -> Result<()>;
    /// Writes one log record and the state of each run in one commit (not durable).
    fn write_state(&mut self, runs: &[u64]) -> Result<()>;
    /// Makes everything durable.
    fn sync(&mut self) -> Result<()>;
    /// Reads one run's state; false if missing.
    fn read_state(&mut self, run: u64) -> Result<bool>;
    /// Reads the whole log in order; returns the number of records.
    fn scan_log(&mut self) -> Result<u64>;
}

#[derive(Clone, Copy)]
enum Engine {
    PlainLog,
    Redb,
    Fjall,
}

impl Engine {
    const ALL: [Engine; 3] = [Engine::PlainLog, Engine::Redb, Engine::Fjall];

    fn name(self) -> &'static str {
        match self {
            Engine::PlainLog => "plain log",
            Engine::Redb => "redb",
            Engine::Fjall => "fjall",
        }
    }

    fn slug(self) -> &'static str {
        match self {
            Engine::PlainLog => "plain",
            Engine::Redb => "redb",
            Engine::Fjall => "fjall",
        }
    }

    fn has_state(self) -> bool {
        !matches!(self, Engine::PlainLog)
    }

    fn open(self, dir: &Path) -> Result<Box<dyn Store>> {
        std::fs::create_dir_all(dir)?;
        Ok(match self {
            Engine::PlainLog => Box::new(PlainLog::open(dir)?),
            Engine::Redb => Box::new(RedbStore::open(dir)?),
            Engine::Fjall => Box::new(FjallStore::open(dir)?),
        })
    }
}

fn state_key(run: u64) -> String {
    format!("run-{run:012}")
}

fn state_value(run: u64) -> &'static [u8] {
    noise(run.wrapping_add(1 << 40), STATE_VALUE)
}

// --- plain framed log ---------------------------------------------------------------------------

struct PlainLog {
    path: PathBuf,
    file: File,
}

impl PlainLog {
    fn open(dir: &Path) -> Result<PlainLog> {
        let path = dir.join("log");
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(PlainLog { path, file })
    }
}

impl Store for PlainLog {
    fn append(&mut self, records: &[(u64, &[u8])], durable: bool) -> Result<()> {
        let mut buffer = Vec::with_capacity(records.len() * (RECORD + 16));
        for (lsn, payload) in records {
            let mut record = lsn.to_be_bytes().to_vec();
            record.extend_from_slice(payload);
            buffer.extend_from_slice(&frame::encode(&record)?);
        }
        self.file.write_all(&buffer)?;
        if durable {
            self.file.sync_data()?;
        }
        Ok(())
    }

    fn write_state(&mut self, runs: &[u64]) -> Result<()> {
        let records: Vec<(u64, &[u8])> =
            runs.iter().map(|&run| (run, noise(run, RECORD))).collect();
        self.append(&records, false)
    }

    fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    fn read_state(&mut self, _run: u64) -> Result<bool> {
        Ok(false)
    }

    fn scan_log(&mut self) -> Result<u64> {
        let mut bytes = Vec::new();
        File::open(&self.path)?.read_to_end(&mut bytes)?;
        Ok(frame::decode_all(&bytes).payloads.len() as u64)
    }
}

// --- redb ---------------------------------------------------------------------------------------

const REDB_LOG: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("log");
const REDB_STATE: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("state");

struct RedbStore {
    db: redb::Database,
}

impl RedbStore {
    fn open(dir: &Path) -> Result<RedbStore> {
        Ok(RedbStore {
            db: redb::Database::create(dir.join("db.redb"))?,
        })
    }
}

impl Store for RedbStore {
    fn append(&mut self, records: &[(u64, &[u8])], durable: bool) -> Result<()> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(if durable {
            redb::Durability::Immediate
        } else {
            redb::Durability::None
        })?;
        {
            let mut log = txn.open_table(REDB_LOG)?;
            for (lsn, payload) in records {
                log.insert(*lsn, *payload)?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    fn write_state(&mut self, runs: &[u64]) -> Result<()> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(redb::Durability::None)?;
        {
            let mut log = txn.open_table(REDB_LOG)?;
            let mut state = txn.open_table(REDB_STATE)?;
            for &run in runs {
                log.insert(run, noise(run, RECORD))?;
                state.insert(state_key(run).as_str(), state_value(run))?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        // An empty durable commit persists all earlier non-durable commits.
        let mut txn = self.db.begin_write()?;
        txn.set_durability(redb::Durability::Immediate)?;
        txn.commit()?;
        Ok(())
    }

    fn read_state(&mut self, run: u64) -> Result<bool> {
        use redb::ReadableDatabase;
        let txn = self.db.begin_read()?;
        let state = txn.open_table(REDB_STATE)?;
        Ok(state.get(state_key(run).as_str())?.is_some())
    }

    fn scan_log(&mut self) -> Result<u64> {
        use redb::{ReadableDatabase, ReadableTable};
        let txn = self.db.begin_read()?;
        let log = txn.open_table(REDB_LOG)?;
        let mut count = 0u64;
        for entry in log.iter()? {
            let (_, value) = entry?;
            count += u64::from(!value.value().is_empty());
        }
        Ok(count)
    }
}

// --- fjall --------------------------------------------------------------------------------------

struct FjallStore {
    db: fjall::Database,
    log: fjall::Keyspace,
    state: fjall::Keyspace,
}

impl FjallStore {
    fn open(dir: &Path) -> Result<FjallStore> {
        let db = fjall::Database::builder(dir.join("fjall")).open()?;
        let log = db.keyspace("log", fjall::KeyspaceCreateOptions::default)?;
        let state = db.keyspace("state", fjall::KeyspaceCreateOptions::default)?;
        Ok(FjallStore { db, log, state })
    }
}

impl Store for FjallStore {
    fn append(&mut self, records: &[(u64, &[u8])], durable: bool) -> Result<()> {
        let mode = if durable {
            Some(fjall::PersistMode::SyncAll)
        } else {
            None
        };
        let mut batch = self.db.batch().durability(mode);
        for (lsn, payload) in records {
            batch.insert(&self.log, lsn.to_be_bytes(), *payload);
        }
        batch.commit()?;
        Ok(())
    }

    fn write_state(&mut self, runs: &[u64]) -> Result<()> {
        let mut batch = self.db.batch().durability(None);
        for &run in runs {
            batch.insert(&self.log, run.to_be_bytes(), noise(run, RECORD));
            batch.insert(&self.state, state_key(run), state_value(run));
        }
        batch.commit()?;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.db.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }

    fn read_state(&mut self, run: u64) -> Result<bool> {
        Ok(self.state.get(state_key(run))?.is_some())
    }

    fn scan_log(&mut self) -> Result<u64> {
        let mut count = 0u64;
        for guard in self.log.iter() {
            let value = guard.value()?;
            count += u64::from(!value.is_empty());
        }
        Ok(count)
    }
}

// --- helpers ------------------------------------------------------------------------------------

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn dir_size(dir: &Path) -> Result<u64> {
    let mut total = 0;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        total += if meta.is_dir() {
            dir_size(&entry.path())?
        } else {
            meta.len()
        };
    }
    Ok(total)
}

fn ms(d: Duration) -> String {
    format!("{:.2} ms", d.as_secs_f64() * 1000.0)
}
