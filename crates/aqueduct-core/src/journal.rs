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

/// What a model call cost and which model answered it: recorded with the response, so replay never calls
/// the model again and drift can be traced to a model or parameter change.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AgentResponse {
    /// The model that answered, as `<provider>/<model>[@<version>]` (for example
    /// `anthropic/claude-sonnet-5-5`).
    pub model: String,
    /// The parameters it was called with (JSON: temperature, max tokens, ...).
    pub params: Payload,
    /// The response (JSON), the agent call's outcome.
    pub response: Payload,
    /// Tokens read.
    pub input_tokens: u64,
    /// Tokens written.
    pub output_tokens: u64,
}

impl AgentResponse {
    /// Tokens used in total.
    pub fn tokens(&self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }
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
        /// The run's token budget for agent calls, if it has one. Part of the start itself, so a crash
        /// can never keep the start and lose the budget.
        budget_tokens: Option<u64>,
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
    /// The run issued an agent call: a bounded model call whose response is journaled.
    AgentCallScheduled {
        /// The command's index.
        cmd: CommandIndex,
        /// The agent call's name (for example `classify_documents`).
        name: String,
        /// The request (JSON: input, prompt, output schema).
        request: Payload,
        /// The most tokens the call may use.
        budget_tokens: u64,
    },
    /// An agent call completed: the model, its parameters, the response and the tokens used. Recorded
    /// at most once per command.
    AgentCallCompleted {
        /// The command this completes.
        cmd: CommandIndex,
        /// What answered and what it cost.
        response: AgentResponse,
    },
    /// The run issued an agent call that would exceed the run's budget: it is scheduled but not made, and
    /// waits for a person or an agent to approve more (an escalation with reason `budget`). One record,
    /// so a crash can never keep the call and lose the fact that it must wait.
    AgentCallEscalated {
        /// The command's index.
        cmd: CommandIndex,
        /// The agent call's name.
        name: String,
        /// The request (JSON).
        request: Payload,
        /// The most tokens the call may use: what it needs from the run's budget.
        budget_tokens: u64,
        /// The tokens the run had left for it.
        available: u64,
    },
    /// More budget was approved for a waiting agent call; the call is made.
    BudgetApproved {
        /// The agent call's command.
        cmd: CommandIndex,
        /// The run's new budget.
        tokens: u64,
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
    /// The run observed a value from outside its logic (the time, random bits, a local computation).
    /// The command is born completed with that value; replay reads it from here.
    ValueRecorded {
        /// The command's index.
        cmd: CommandIndex,
        /// What the value is (for example `now` or `random`).
        name: String,
        /// The value.
        value: Payload,
    },
    /// The run escalated a decision to a person or an agent and waits for it to be resolved.
    Escalated {
        /// The command's index.
        cmd: CommandIndex,
        /// Why (for example `review-timeout`); what escalations are counted by.
        reason: String,
        /// What the resolver needs to decide.
        payload: Payload,
    },
    /// An escalation was resolved; `outcome` is what the run continues with.
    EscalationResolved {
        /// The escalation's command.
        cmd: CommandIndex,
        /// The resolution.
        outcome: Payload,
    },
    /// The run was asked to stop. Every command without an outcome completes as failed and cancelled;
    /// when none is pending, the next command the run issues does. At most once per run.
    CancelRequested {
        /// Why, as given by whoever cancelled.
        reason: String,
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
    /// The run ended and asked for a new run of the same workflow under the same key, with this input
    /// (continue-as-new). The engine starts it as the key's next generation.
    ContinuedAsNew {
        /// The next run's input.
        input: Payload,
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
