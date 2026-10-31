//! Timers and signals in the embedded engine, with real time.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use aqueduct_core::{Payload, Status, StepOutcome};
use aqueduct_engine::embedded::Engine;
use aqueduct_procedure::Procedure;

const APPROVAL: &str = r#"
def run(request):
    decision = wait(signal("approve"), timeout = request["timeout"])
    if decision == None:
        return "timed out"
    return step("apply", decision)
"#;

const REMINDER: &str = r#"
def run(request):
    sleep(request["seconds"])
    note = wait(signal("note"))
    return step("apply", note)
"#;

fn temp_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "aqueduct-timers-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn json(value: &str) -> Payload {
    Payload::new(value.as_bytes().to_vec())
}

fn open(dir: &PathBuf) -> Engine {
    Engine::builder(dir)
        .workflow(
            "approval",
            Arc::new(Procedure::compile("approval.star", APPROVAL).unwrap()),
        )
        .workflow(
            "reminder",
            Arc::new(Procedure::compile("reminder.star", REMINDER).unwrap()),
        )
        .step("apply", |input| {
            let text = String::from_utf8_lossy(input.as_bytes()).to_string();
            StepOutcome::Completed(json(&format!("\"applied {}\"", text.trim_matches('"'))))
        })
        .open()
        .unwrap()
}

#[test]
fn a_signal_approves_a_waiting_run() {
    let dir = temp_dir();
    let engine = open(&dir);
    engine
        .start("req-1", "approval", json(r#"{"timeout": 30}"#))
        .unwrap();
    engine
        .signal("req-1", "approve", "s-1", json("\"yes\""))
        .unwrap();
    let result = engine.wait("req-1", Duration::from_secs(10)).unwrap();
    assert_eq!(result.output, Some(json("\"applied yes\"")));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_wait_times_out_but_never_early() {
    let dir = temp_dir();
    let engine = open(&dir);
    let started = Instant::now();
    engine
        .start("req-2", "approval", json(r#"{"timeout": 1}"#))
        .unwrap();
    let result = engine.wait("req-2", Duration::from_secs(10)).unwrap();
    assert_eq!(result.output, Some(json("\"timed out\"")));
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "fired after {:?}",
        started.elapsed()
    );
    // A signal after the end is acknowledged and changes nothing.
    engine
        .signal("req-2", "approve", "s-2", json("\"too late\""))
        .unwrap();
    assert_eq!(
        engine.wait("req-2", Duration::from_secs(1)).unwrap().output,
        result.output
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_sleep_survives_a_restart_and_wakes_at_its_original_time() {
    let dir = temp_dir();
    let started = Instant::now();
    {
        let engine = open(&dir);
        engine
            .start("rem-1", "reminder", json(r#"{"seconds": 1}"#))
            .unwrap();
        // Sent while the run sleeps: kept until the run waits for it.
        engine
            .signal("rem-1", "note", "n-1", json("\"water the plants\""))
            .unwrap();
    }
    let engine = open(&dir);
    let result = engine.wait("rem-1", Duration::from_secs(10)).unwrap();
    assert_eq!(result.status, Status::Completed);
    assert_eq!(result.output, Some(json("\"applied water the plants\"")));
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_secs(1),
        "woke early, after {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(1_900),
        "the wake time was not kept across the restart: {elapsed:?}"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}
