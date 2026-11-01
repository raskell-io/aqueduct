//! Deterministic simulation testing for Aqueduct (ADR 0006).
//!
//! One seed drives one simulated world: a partition node with a simulated disk, a client starting
//! runs, a step worker, and a network that delays, reorders, drops and duplicates messages. While
//! faults are on, the node crashes and restarts, unsynced writes are lost or torn, syncs fail and the
//! worker forgets results. Then faults heal and the world must converge.
//!
//! Invariants checked after every event, at every recovery, and at the end:
//!
//! - every run's state equals the fold of its journal;
//! - recovered journals have valid hash chains and are prefixes of what the node held before the crash;
//! - no acknowledged start or step result is lost;
//! - every run ends exactly as a sequential, fault-free reference execution says it should, and no run
//!   is parked;
//! - after quiescence, the durable log holds exactly the in-memory journals.
//!
//! Any failure reproduces with `cargo xtask sim --seed <n>`.

#![forbid(unsafe_code)]

mod disk;
mod node;
mod rng;
mod workflow;
mod world;

pub use rng::Rng;

/// The decider and step bodies a simulated world runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WorkloadKind {
    /// The pipeline as a native Rust decider.
    #[default]
    Pipeline,
    /// The same pipeline as a Starlark procedure (`aqueduct-procedure`).
    StarlarkPipeline,
    /// A Starlark procedure that sleeps and waits for a signal: exercises durable timers and
    /// exactly-once signals under faults.
    StarlarkTimers,
    /// The pipeline as a Rust `async fn` written with the SDK (`aqueduct-sdk`).
    RustPipeline,
}

/// Deliberately injected bugs, used to check that the simulator catches them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Bugs {
    /// Acknowledge starts and results before the entries they depend on are synced.
    pub ack_before_sync: bool,
    /// Recover the log without truncating a torn tail, so later appends follow garbage.
    pub skip_tail_truncation: bool,
    /// Never re-dispatch steps whose dispatch or result was lost.
    pub no_redispatch: bool,
    /// After recovery, forget which runs have records after the checkpoint, so the next checkpoint
    /// skips them while moving past their log records.
    pub checkpoint_forgets_recovered_runs: bool,
    /// On recovery, do not restart the successor of a run that continued as new when a crash tore its
    /// start off the log.
    pub skip_successor_repair: bool,
}

/// Parameters of a simulated world. Probabilities are in parts per million.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// Runs the client starts.
    pub runs: usize,
    /// Simulated time during which faults are injected.
    pub fault_window: u64,
    /// Simulated time by which the world must be quiescent.
    pub max_time: u64,
    /// Chance that a message is dropped.
    pub drop_ppm: u32,
    /// Chance that a message is duplicated.
    pub duplicate_ppm: u32,
    /// Maximum message delay; random delays also reorder messages.
    pub max_delay: u64,
    /// Chance, after each event, that the node crashes soon.
    pub crash_ppm: u32,
    /// Maximum time until a crashed node restarts.
    pub restart_delay: u64,
    /// Chance that a sync fails.
    pub sync_fail_ppm: u32,
    /// Chance that a crash corrupts a surviving unsynced byte.
    pub torn_ppm: u32,
    /// Chance, per dispatch, that the worker forgets its unacknowledged results.
    pub worker_amnesia_ppm: u32,
    /// Chance, per step execution while faults are on, of a transient (retryable) failure.
    pub transient_ppm: u32,
    /// Group commit delay.
    pub flush_interval: u64,
    /// Interval at which the node re-dispatches steps without an outcome.
    pub redispatch_interval: u64,
    /// Interval at which the client retries unacknowledged starts.
    pub client_retry: u64,
    /// Interval at which the worker resends unacknowledged results.
    pub worker_resend: u64,
    /// Every how many runs the client cancels one (0: none), retrying until acknowledged.
    pub cancel_every: usize,
    /// Injected bugs.
    pub bugs: Bugs,
    /// The workload.
    pub workload: WorkloadKind,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            runs: 6,
            fault_window: 2_000,
            max_time: 100_000,
            drop_ppm: 50_000,
            duplicate_ppm: 50_000,
            max_delay: 20,
            crash_ppm: 3_000,
            restart_delay: 50,
            sync_fail_ppm: 20_000,
            torn_ppm: 300_000,
            worker_amnesia_ppm: 20_000,
            transient_ppm: 100_000,
            flush_interval: 3,
            redispatch_interval: 150,
            client_retry: 80,
            worker_resend: 60,
            cancel_every: 3,
            bugs: Bugs::default(),
            workload: WorkloadKind::default(),
        }
    }
}

/// What happened in a successful simulation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
    /// The seed.
    pub seed: u64,
    /// Events processed.
    pub events: u64,
    /// Simulated time at quiescence.
    pub end_time: u64,
    /// Node crashes (including those caused by failed syncs).
    pub crashes: u64,
    /// Failed syncs.
    pub sync_failures: u64,
    /// Successful syncs.
    pub syncs: u64,
    /// Messages dropped.
    pub dropped: u64,
    /// Messages duplicated.
    pub duplicated: u64,
    /// Retries the engine scheduled after transient failures.
    pub retries: u64,
    /// Checkpoints saved; recovery loads the last one and replays only the log after it.
    pub checkpoints: u64,
    /// Recoveries that found and truncated a torn or corrupt tail.
    pub torn_tails: u64,
    /// Step executions, including repeats.
    pub step_executions: u64,
    /// Step executions beyond the first per command (at-least-once in action).
    pub duplicate_executions: u64,
    /// Runs that completed.
    pub completed: u64,
    /// Runs that failed (as the workload intends for some inputs).
    pub failed: u64,
    /// Runs that were cancelled while running (and so failed as cancelled).
    pub cancelled: u64,
    /// A fingerprint of the event sequence and the final chain heads. Equal for equal seeds.
    pub trace: u64,
}

/// A violated invariant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    /// The seed that reproduces it.
    pub seed: u64,
    /// The workload it ran.
    pub workload: WorkloadKind,
    /// Simulated time of the failure.
    pub time: u64,
    /// What went wrong.
    pub message: String,
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let workload = match self.workload {
            WorkloadKind::Pipeline => "native",
            WorkloadKind::StarlarkPipeline => "starlark",
            WorkloadKind::StarlarkTimers => "timers",
            WorkloadKind::RustPipeline => "rust",
        };
        write!(
            f,
            "seed {} failed at time {}: {}\nreproduce with: cargo xtask sim --seed {} --workload {workload}",
            self.seed, self.time, self.message, self.seed
        )
    }
}

impl std::error::Error for Failure {}

/// Runs one seed.
pub fn run_seed(seed: u64, config: &Config) -> Result<Report, Failure> {
    world::run(seed, config)
}
