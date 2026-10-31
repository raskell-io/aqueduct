//! Deciders: the deterministic logic of a run.

use crate::journal::{CommandIndex, Payload, StepOutcome};
use crate::run::RunState;

/// A command issued by a run's logic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// Schedule a step.
    ScheduleStep {
        /// The step's name.
        name: String,
        /// The step's input.
        input: Payload,
    },
    /// Sleep durably.
    Sleep {
        /// How long, in milliseconds.
        duration_ms: u64,
    },
    /// Wait for the next signal with this name, or until the timeout.
    AwaitSignal {
        /// The signal's name.
        name: String,
        /// The timeout in milliseconds; 0 waits forever.
        timeout_ms: u64,
    },
    /// Record a value the logic observed from outside (the time, random bits, a local computation).
    /// The first time it is issued, `value` is journaled and is the command's outcome at once; the
    /// logic may use it in the same activation. When the journal already holds the command, the
    /// journaled value is the outcome and `value` is ignored: only the name must match.
    Record {
        /// What the value is (for example `now` or `random`).
        name: String,
        /// The value observed now.
        value: Payload,
    },
    /// Complete the run. Must be the last command.
    CompleteRun {
        /// The run's output.
        output: Payload,
    },
    /// Fail the run. Must be the last command.
    FailRun {
        /// A description of the failure.
        error: String,
    },
    /// The logic cannot continue (for example, a procedure script raised an error or exceeded its
    /// budget). Parks the run for an operator instead of failing it. Must be the last command.
    Park {
        /// What went wrong.
        reason: String,
    },
}

/// What the engine offers a decider for new [`Command::Record`] values in one activation: its clock
/// and a random seed. Values taken from it are journaled; replay never reads it again.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Env {
    /// The engine's clock: milliseconds since the Unix epoch.
    pub now_ms: u64,
    /// Random bits for this activation (from the operating system in an engine, from the seed in a
    /// simulation).
    pub seed: u64,
}

/// What a decider can observe about a run: its input and the outcomes recorded so far.
#[derive(Clone, Copy, Debug)]
pub struct RunView<'a> {
    state: &'a RunState,
    key: &'a str,
    env: Env,
}

impl<'a> RunView<'a> {
    pub(crate) fn new(state: &'a RunState, key: &'a str, env: Env) -> Self {
        RunView { state, key, env }
    }

    /// The run's key (empty in a view without one, such as a local replay).
    pub fn key(&self) -> &'a str {
        self.key
    }

    /// The clock and seed for values recorded in this activation.
    pub fn env(&self) -> Env {
        self.env
    }

    /// The run's input.
    pub fn input(&self) -> &'a Payload {
        self.state.input()
    }

    /// The recorded outcome of a command, if there is one.
    pub fn outcome(&self, cmd: CommandIndex) -> Option<&'a StepOutcome> {
        let index = usize::try_from(cmd.0).ok()?;
        self.state.steps().get(index)?.outcome.as_ref()
    }

    /// The outcome of every scheduled step command, by command index (`None` while pending).
    pub fn outcomes(&self) -> impl Iterator<Item = Option<&'a StepOutcome>> + 'a {
        self.state.steps().iter().map(|step| step.outcome.as_ref())
    }

    /// Whether a cancellation completed this command (its outcome is then a failure).
    pub fn cancelled(&self, cmd: CommandIndex) -> bool {
        usize::try_from(cmd.0)
            .ok()
            .and_then(|index| self.state.steps().get(index))
            .is_some_and(|step| step.cancelled)
    }

    /// The reason the run was cancelled, if it was.
    pub fn cancel_reason(&self) -> Option<&'a str> {
        self.state.cancel_reason()
    }

    /// Commands in the order their outcomes were recorded. Logic that depends on this order must be
    /// aware that it differs between runs with the same input.
    pub fn completion_order(&self) -> &'a [CommandIndex] {
        self.state.completion_order()
    }
}

/// The deterministic logic of a run.
///
/// A decider is a pure function from what the run has observed to the complete list of commands the
/// run has issued so far, in issue order. It must return the same commands for the same observations.
/// Code workflows become deciders by replaying their future against the journal; procedures are
/// interpreted by a decider.
pub trait Decider {
    /// Returns every command the run has issued, given what it has observed.
    fn decide(&self, view: &RunView<'_>) -> Vec<Command>;
}
