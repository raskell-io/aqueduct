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

/// What a decider can observe about a run: its input and the outcomes recorded so far.
#[derive(Clone, Copy, Debug)]
pub struct RunView<'a> {
    state: &'a RunState,
}

impl<'a> RunView<'a> {
    pub(crate) fn new(state: &'a RunState) -> Self {
        RunView { state }
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
