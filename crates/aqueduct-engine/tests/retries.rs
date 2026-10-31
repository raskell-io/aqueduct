//! Retry policies in the embedded engine, with real time.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use aqueduct_core::{Payload, Status};
use aqueduct_engine::RetryPolicy;
use aqueduct_engine::embedded::{Attempt, Engine};
use aqueduct_procedure::Procedure;

const CALL: &str = "def run(x):\n    return step(\"call\", x)\n";

fn temp_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "aqueduct-retries-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn open(
    dir: &PathBuf,
    policy: RetryPolicy,
    step: impl Fn(&Payload) -> Attempt + Send + Sync + 'static,
) -> Engine {
    Engine::builder(dir)
        .workflow(
            "call",
            Arc::new(Procedure::compile("call.star", CALL).unwrap()),
        )
        .step("call", step)
        .retry_policy("call", policy)
        .open()
        .unwrap()
}

fn fast(max_attempts: u32) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        initial_backoff_ms: 20,
        multiplier: 2,
        max_backoff_ms: 100,
    }
}

#[test]
fn a_flaky_step_succeeds_on_a_later_attempt() {
    let dir = temp_dir();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let engine = open(&dir, fast(5), move |_| {
        if counter.fetch_add(1, Ordering::SeqCst) < 2 {
            Attempt::Retry("503".into())
        } else {
            Attempt::Completed(Payload::new(*b"\"ok\""))
        }
    });
    engine.start("r", "call", Payload::new(*b"1")).unwrap();
    let result = engine.wait("r", Duration::from_secs(10)).unwrap();
    assert_eq!(result.output, Some(Payload::new(*b"\"ok\"")));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn terminal_failures_are_not_retried_and_retries_run_out() {
    let dir = temp_dir();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let engine = open(&dir, fast(3), move |input| {
        counter.fetch_add(1, Ordering::SeqCst);
        if input.as_bytes() == b"1" {
            Attempt::Fail("invalid".into())
        } else {
            Attempt::Retry("still down".into())
        }
    });
    engine
        .start("terminal", "call", Payload::new(*b"1"))
        .unwrap();
    let result = engine.wait("terminal", Duration::from_secs(10)).unwrap();
    assert_eq!(
        (result.status, result.error.as_deref()),
        (Status::Failed, Some("invalid"))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    engine
        .start("exhausted", "call", Payload::new(*b"2"))
        .unwrap();
    let result = engine.wait("exhausted", Duration::from_secs(10)).unwrap();
    assert_eq!(
        (result.status, result.error.as_deref()),
        (Status::Failed, Some("still down"))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1 + 3);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_pending_retry_survives_a_restart() {
    let dir = temp_dir();
    let policy = RetryPolicy {
        max_attempts: 3,
        initial_backoff_ms: 1_000,
        multiplier: 2,
        max_backoff_ms: 5_000,
    };
    let started = Instant::now();
    {
        let engine = open(&dir, policy, |_| Attempt::Retry("down".into()));
        engine.start("r", "call", Payload::new(*b"1")).unwrap();
        std::thread::sleep(Duration::from_millis(200));
    }
    let engine = open(&dir, policy, |_| {
        Attempt::Completed(Payload::new(*b"\"up\""))
    });
    let result = engine.wait("r", Duration::from_secs(10)).unwrap();
    assert_eq!(result.output, Some(Payload::new(*b"\"up\"")));
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_secs(1),
        "retried before its backoff: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(1_900),
        "the backoff was not kept across the restart: {elapsed:?}"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}
