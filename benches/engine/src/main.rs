//! End-to-end benchmark of the embedded engine against ADR 0003's targets:
//!
//! - start to first step dispatched, p99: under 15 ms with `Durability::Fsync`, under 5 ms with `Os`;
//! - journal appends per second with group commit and `Fsync`: 50,000 or more.
//!
//! `cargo run --release -p aqueduct-bench-engine` (add `-- --quick` for a short run). The workflow is a
//! Rust workflow of three sequential steps; step bodies do nothing, so the numbers are the engine's.
//! Every run journals 8 entries: started, three steps scheduled and completed, run completed.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aqueduct_core::{Payload, Status, StepOutcome};
use aqueduct_engine::Durability;
use aqueduct_engine::embedded::Engine;
use aqueduct_sdk::{WorkflowContext, WorkflowError, workflow};

const ENTRIES_PER_RUN: u64 = 8;

async fn three_steps(ctx: WorkflowContext, key: String) -> Result<String, WorkflowError> {
    let a: String = ctx.step("first", &key).await?;
    let b: String = ctx.step("second", &a).await?;
    ctx.step("third", &b).await
}

/// When each run's first step began executing, by run key.
type FirstSteps = Arc<Mutex<BTreeMap<String, Sender<Instant>>>>;

fn engine(dir: &PathBuf, durability: Durability, first: FirstSteps) -> Engine {
    let _ = std::fs::remove_dir_all(dir);
    let echo = |input: &Payload| StepOutcome::Completed(input.clone());
    Engine::builder(dir)
        .workflow("three_steps", Arc::new(workflow(three_steps)))
        .step("first", move |input: &Payload| {
            let now = Instant::now();
            let key: String = String::from_utf8_lossy(input.as_bytes())
                .trim_matches('"')
                .to_owned();
            if let Some(tx) = first.lock().unwrap().remove(&key) {
                let _ = tx.send(now);
            }
            StepOutcome::Completed(input.clone())
        })
        .step("second", echo)
        .step("third", echo)
        .workers(8)
        .durability(durability)
        .open()
        .unwrap()
}

fn json(text: &str) -> Payload {
    Payload::new(format!("\"{text}\"").into_bytes())
}

struct Latencies(Vec<Duration>);

impl Latencies {
    fn percentile(&self, p: f64) -> Duration {
        let mut sorted = self.0.clone();
        sorted.sort();
        let index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[index]
    }

    fn row(&self, label: &str) -> String {
        let ms = |d: Duration| d.as_secs_f64() * 1_000.0;
        format!(
            "| {label} | {} | {:.2} ms | {:.2} ms | {:.2} ms | {:.2} ms |",
            self.0.len(),
            ms(self.percentile(0.5)),
            ms(self.percentile(0.9)),
            ms(self.percentile(0.99)),
            ms(self.percentile(1.0)),
        )
    }
}

/// Start to first step executing, measured by `clients` threads each starting runs one after another.
fn latency(dir: &PathBuf, durability: Durability, clients: usize, per_client: usize) -> Latencies {
    let first: FirstSteps = Arc::default();
    let engine = Arc::new(engine(dir, durability, Arc::clone(&first)));
    let threads: Vec<_> = (0..clients)
        .map(|c| {
            let engine = Arc::clone(&engine);
            let first = Arc::clone(&first);
            std::thread::spawn(move || {
                let mut samples = Vec::with_capacity(per_client);
                for n in 0..per_client {
                    let key = format!("c{c}-{n}");
                    let (tx, rx) = mpsc::channel();
                    first.lock().unwrap().insert(key.clone(), tx);
                    let started = Instant::now();
                    engine.start(&key, "three_steps", json(&key)).unwrap();
                    let dispatched = rx.recv_timeout(Duration::from_secs(30)).unwrap();
                    samples.push(dispatched - started);
                    // Let the run finish before the next one, so a client measures one run at a time.
                    let done = engine.wait(&key, Duration::from_secs(30)).unwrap();
                    assert_eq!(done.status, Status::Completed);
                }
                samples
            })
        })
        .collect();
    let samples = threads
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    drop(engine);
    Latencies(samples)
}

/// Journal entries per second with `clients` threads starting `runs` runs in total, then waiting for all.
fn throughput(dir: &PathBuf, durability: Durability, clients: usize, runs: usize) -> (f64, f64) {
    let engine = Arc::new(engine(dir, durability, Arc::default()));
    let started = Instant::now();
    let threads: Vec<_> = (0..clients)
        .map(|c| {
            let engine = Arc::clone(&engine);
            std::thread::spawn(move || {
                let keys: Vec<String> = (c..runs)
                    .step_by(clients)
                    .map(|n| format!("t{n}"))
                    .collect();
                for key in &keys {
                    engine.start(key, "three_steps", json(key)).unwrap();
                }
                for key in &keys {
                    let done = engine.wait(key, Duration::from_secs(120)).unwrap();
                    assert_eq!(done.status, Status::Completed);
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let seconds = started.elapsed().as_secs_f64();
    drop(engine);
    let runs = runs as f64;
    (runs * ENTRIES_PER_RUN as f64 / seconds, runs / seconds)
}

fn main() {
    let quick = std::env::args().any(|a| a == "--quick");
    let dir = std::env::temp_dir().join(format!("aqueduct-bench-engine-{}", std::process::id()));
    let scale = if quick { 10 } else { 1 };
    for (name, durability) in [("Fsync", Durability::Fsync), ("Os", Durability::Os)] {
        println!("\n## Durability::{name}\n");
        println!("| start to first step | runs | p50 | p90 | p99 | max |");
        println!("|---|---:|---:|---:|---:|---:|");
        let sequential = if durability == Durability::Fsync {
            1_000
        } else {
            5_000
        } / scale;
        println!(
            "{}",
            latency(&dir, durability, 1, sequential).row("1 client")
        );
        println!(
            "{}",
            latency(&dir, durability, 16, sequential / 16).row("16 clients")
        );
        println!();
        println!("| throughput | clients | runs | entries/s | runs/s |");
        println!("|---|---:|---:|---:|---:|");
        for clients in [64, 256] {
            let runs = 20_000 / scale;
            let (entries, per_second) = throughput(&dir, durability, clients, runs);
            println!("| 3-step runs | {clients} | {runs} | {entries:.0} | {per_second:.0} |");
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
