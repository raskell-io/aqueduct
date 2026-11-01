//! Observing an engine from outside: a hook for telemetry (traces, metrics, logs), with no dependency on
//! any telemetry library. `aqueduct-otel` implements it for OpenTelemetry.

use std::time::{Duration, SystemTime};

use aqueduct_core::{CommandIndex, Entry};

/// How a step execution ended, as the worker reported it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepEnd {
    /// The step returned a result.
    Completed,
    /// The step failed terminally.
    Failed,
    /// The step failed with a retryable error (the retry policy decides what happens next).
    Retry,
}

/// Receives what an engine does. Every method has an empty default, so an observer implements only what
/// it needs. Methods are called on the engine's threads: they must be quick and must not block.
pub trait Observer: Send + Sync {
    /// A journal entry of a run became durable. Called once per entry, in journal order per run, after
    /// the flush that made it durable; entries recovered from the log after a restart are not observed
    /// again. `version` is the version the run is pinned to, if any (ADR 0029). `now_ms` is the engine's
    /// clock at the flush.
    fn entry(&self, key: &str, workflow: &str, version: Option<&str>, entry: &Entry, now_ms: u64) {
        let _ = (key, workflow, version, entry, now_ms);
    }

    /// A worker executed a step (at least once execution: a step may be executed more than once).
    fn step(
        &self,
        key: &str,
        cmd: CommandIndex,
        name: &str,
        started: SystemTime,
        elapsed: Duration,
        end: StepEnd,
    ) {
        let _ = (key, cmd, name, started, elapsed, end);
    }

    /// The log made a batch durable: how many records, and how long the write and sync took.
    fn flush(&self, records: usize, elapsed: Duration) {
        let _ = (records, elapsed);
    }
}
