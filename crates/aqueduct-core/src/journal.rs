//! The journal model: entries, payloads and command indexes.

use std::fmt;

/// Opaque payload bytes. Their encoding is chosen by the SDK's codec; the core never interprets them.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Payload(pub Vec<u8>);

impl Payload {
    /// Creates a payload from bytes.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Payload(bytes.into())
    }

    /// The payload's bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// The position of a command in the order a run issues its commands.
///
/// Command indexes are deterministic across replays; completions refer to their command by index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandIndex(pub u32);

impl fmt::Display for CommandIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The recorded outcome of a step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepOutcome {
    /// The step completed with a result.
    Completed(Payload),
    /// The step failed terminally (after its retry policy was exhausted).
    Failed(String),
}

/// One entry in a run's journal. The encoding is specified in `docs/history-format.md`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    /// The run was started with an input. Always the first entry.
    Started {
        /// The name of the workflow or procedure the run executes.
        workflow: String,
        /// The run's input.
        input: Payload,
    },
    /// The run issued a step command.
    StepScheduled {
        /// The command's index.
        cmd: CommandIndex,
        /// The step's name.
        name: String,
        /// The step's input.
        input: Payload,
    },
    /// A step completed. Recorded at most once per command.
    StepCompleted {
        /// The command this completes.
        cmd: CommandIndex,
        /// The step's result.
        result: Payload,
    },
    /// A step failed terminally. Recorded at most once per command.
    StepFailed {
        /// The command this completes.
        cmd: CommandIndex,
        /// A description of the failure.
        error: String,
    },
    /// An attempt of a step failed with a retryable error; the step runs again at `wake_at`.
    StepRetryScheduled {
        /// The step's command index.
        cmd: CommandIndex,
        /// The number of the attempt that failed (1 for the first).
        attempt: u32,
        /// The attempt's error.
        error: String,
        /// When the next attempt is dispatched: milliseconds since the Unix epoch.
        wake_at: u64,
    },
    /// The run started a durable timer (sleep).
    TimerStarted {
        /// The command's index.
        cmd: CommandIndex,
        /// The requested duration, in milliseconds.
        duration_ms: u64,
        /// When it fires: milliseconds since the Unix epoch, by the engine's clock at the activation.
        wake_at: u64,
    },
    /// The run started waiting for a signal, optionally with a timeout.
    SignalAwaited {
        /// The command's index.
        cmd: CommandIndex,
        /// The signal's name.
        name: String,
        /// The timeout in milliseconds; 0 means no timeout.
        timeout_ms: u64,
        /// When the timeout fires (milliseconds since the Unix epoch); 0 means never.
        wake_at: u64,
    },
    /// A timer fired: a sleep ended, or a signal wait timed out.
    TimerFired {
        /// The command whose timer fired.
        cmd: CommandIndex,
    },
    /// A signal arrived. It completes the oldest waiting command for its name, or is kept until one
    /// waits for it.
    SignalReceived {
        /// The signal's name.
        name: String,
        /// The sender's id for this signal; a second signal with the same id is not recorded. Empty
        /// means no deduplication.
        id: String,
        /// The signal's payload.
        payload: Payload,
    },
    /// The run completed with an output.
    RunCompleted {
        /// The run's output.
        output: Payload,
    },
    /// The run failed.
    RunFailed {
        /// A description of the failure.
        error: String,
    },
    /// The run was parked because its logic diverged from its journal.
    Parked {
        /// What diverged.
        reason: String,
    },
}

/// A stable idempotency key for a step command: `run_key/epoch/cmd`.
///
/// Pass it to external systems so that repeated executions of the same command can be deduplicated.
/// The epoch increments when an operator resets a run, so a command re-issued after a reset never
/// reuses an old key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// Builds the key for a command of a run in a given epoch.
    pub fn new(run_key: &str, epoch: u32, cmd: CommandIndex) -> Self {
        IdempotencyKey(format!("{run_key}/{epoch}/{}", cmd.0))
    }

    /// The key as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
