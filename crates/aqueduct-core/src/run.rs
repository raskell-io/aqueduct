//! The run state machine.

use crate::decider::{Command, RunView};
use crate::journal::{CommandIndex, Entry, Payload, StepOutcome};

/// The lifecycle status of a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Status {
    /// The run is in progress.
    Running,
    /// The run completed with an output.
    Completed,
    /// The run failed.
    Failed,
    /// The run's logic diverged from its journal; an operator must intervene.
    Parked,
}

impl Status {
    /// Whether the run has ended (completed, failed or parked).
    pub fn is_terminal(self) -> bool {
        !matches!(self, Status::Running)
    }
}

/// A step command as recorded in the journal, with its outcome if one has been recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduledStep {
    /// The step's name.
    pub name: String,
    /// The step's input.
    pub input: Payload,
    /// The recorded outcome, if any.
    pub outcome: Option<StepOutcome>,
}

/// An input to the state machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The run's decider returned these commands on activation.
    Decision(Vec<Command>),
    /// A step execution finished. May arrive more than once for the same command.
    StepResult {
        /// The command the result belongs to.
        cmd: CommandIndex,
        /// The step's outcome.
        outcome: StepOutcome,
    },
}

/// Something the caller must do once the entries of the same [`Transition`] are durable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Ask the run's decider for its commands and feed them back as [`Event::Decision`].
    Activate,
    /// Execute a step and feed the outcome back as [`Event::StepResult`].
    DispatchStep {
        /// The step's command index.
        cmd: CommandIndex,
        /// The step's name.
        name: String,
        /// The step's input.
        input: Payload,
    },
    /// The run reached a terminal status.
    Finished(Status),
}

/// The result of handling an event: entries to append to the journal, then effects to perform after
/// those entries are durable.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[must_use]
pub struct Transition {
    /// Entries to append, in order.
    pub entries: Vec<Entry>,
    /// Effects to perform after the entries are durable.
    pub effects: Vec<Effect>,
}

impl Transition {
    fn none() -> Self {
        Transition::default()
    }

    fn park(reason: String) -> Self {
        Transition {
            entries: vec![Entry::Parked { reason }],
            effects: vec![Effect::Finished(Status::Parked)],
        }
    }
}

/// Starts a new run of a workflow: the transition holds the `Started` entry and an activation.
pub fn start(workflow: impl Into<String>, input: Payload) -> Transition {
    Transition {
        entries: vec![Entry::Started {
            workflow: workflow.into(),
            input,
        }],
        effects: vec![Effect::Activate],
    }
}

/// A journal that violates the rules in `docs/history-format.md`, section 5.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum JournalError {
    /// The journal is empty.
    #[error("the journal is empty")]
    Empty,
    /// The first entry is not `Started`.
    #[error("the first entry is not Started")]
    NotStarted,
    /// `Started` occurs more than once.
    #[error("Started occurs more than once")]
    DuplicateStart,
    /// An entry follows a terminal entry.
    #[error("an entry follows the end of the run")]
    EntryAfterEnd,
    /// A step was scheduled out of order.
    #[error("step scheduled with command {found}, expected {expected}")]
    CommandOutOfOrder {
        /// The next command index expected.
        expected: CommandIndex,
        /// The command index found.
        found: CommandIndex,
    },
    /// A completion refers to a command that was never scheduled.
    #[error("completion for unknown command {0}")]
    UnknownCommand(CommandIndex),
    /// A command has more than one recorded outcome.
    #[error("command {0} has more than one recorded outcome")]
    DuplicateCompletion(CommandIndex),
    /// More commands than a command index can address.
    #[error("too many commands in one run")]
    TooManyCommands,
}

/// The state of one run: by definition, the fold of its journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunState {
    workflow: String,
    status: Status,
    input: Payload,
    steps: Vec<ScheduledStep>,
    completion_order: Vec<CommandIndex>,
    output: Option<Payload>,
    error: Option<String>,
    entries: usize,
}

impl RunState {
    /// Rebuilds a run's state from its journal, checking the journal rules.
    pub fn from_journal(journal: &[Entry]) -> Result<RunState, JournalError> {
        let (first, rest) = journal.split_first().ok_or(JournalError::Empty)?;
        let Entry::Started { workflow, input } = first else {
            return Err(JournalError::NotStarted);
        };
        let mut state = RunState {
            workflow: workflow.clone(),
            status: Status::Running,
            input: input.clone(),
            steps: Vec::new(),
            completion_order: Vec::new(),
            output: None,
            error: None,
            entries: 1,
        };
        for entry in rest {
            state.apply(entry)?;
        }
        Ok(state)
    }

    /// Applies one journal entry. Call this for every entry of a [`Transition`], in order.
    pub fn apply(&mut self, entry: &Entry) -> Result<(), JournalError> {
        if matches!(entry, Entry::Started { .. }) {
            return Err(JournalError::DuplicateStart);
        }
        if self.status.is_terminal() {
            return Err(JournalError::EntryAfterEnd);
        }
        match entry {
            Entry::Started { .. } => return Err(JournalError::DuplicateStart),
            Entry::StepScheduled { cmd, name, input } => {
                let expected = self.next_command()?;
                if *cmd != expected {
                    return Err(JournalError::CommandOutOfOrder {
                        expected,
                        found: *cmd,
                    });
                }
                self.steps.push(ScheduledStep {
                    name: name.clone(),
                    input: input.clone(),
                    outcome: None,
                });
            }
            Entry::StepCompleted { cmd, result } => {
                self.record(*cmd, StepOutcome::Completed(result.clone()))?;
            }
            Entry::StepFailed { cmd, error } => {
                self.record(*cmd, StepOutcome::Failed(error.clone()))?;
            }
            Entry::RunCompleted { output } => {
                self.status = Status::Completed;
                self.output = Some(output.clone());
            }
            Entry::RunFailed { error } => {
                self.status = Status::Failed;
                self.error = Some(error.clone());
            }
            Entry::Parked { reason } => {
                self.status = Status::Parked;
                self.error = Some(reason.clone());
            }
        }
        self.entries += 1;
        Ok(())
    }

    /// Handles an event. Pure: returns what to append and what to do, without changing the state.
    pub fn handle(&self, event: Event) -> Transition {
        if self.status.is_terminal() {
            return Transition::none();
        }
        match event {
            Event::StepResult { cmd, outcome } => self.handle_result(cmd, outcome),
            Event::Decision(commands) => self.handle_decision(commands),
        }
    }

    /// What must happen after a restart: activate a running run and re-dispatch its steps that have
    /// no recorded outcome.
    pub fn recovery_effects(&self) -> Vec<Effect> {
        if self.status.is_terminal() {
            return Vec::new();
        }
        let mut effects = vec![Effect::Activate];
        effects.extend(self.pending_steps());
        effects
    }

    /// Dispatch effects for every scheduled step without a recorded outcome.
    pub fn pending_steps(&self) -> Vec<Effect> {
        if self.status.is_terminal() {
            return Vec::new();
        }
        self.steps
            .iter()
            .zip(0u32..)
            .filter(|(step, _)| step.outcome.is_none())
            .map(|(step, index)| Effect::DispatchStep {
                cmd: CommandIndex(index),
                name: step.name.clone(),
                input: step.input.clone(),
            })
            .collect()
    }

    /// The view a decider sees.
    pub fn view(&self) -> RunView<'_> {
        RunView::new(self)
    }

    /// The name of the workflow or procedure the run executes.
    pub fn workflow(&self) -> &str {
        &self.workflow
    }

    /// The run's status.
    pub fn status(&self) -> Status {
        self.status
    }

    /// The run's input.
    pub fn input(&self) -> &Payload {
        &self.input
    }

    /// The scheduled steps, indexed by command index.
    pub fn steps(&self) -> &[ScheduledStep] {
        &self.steps
    }

    /// Commands in the order their outcomes were recorded.
    pub fn completion_order(&self) -> &[CommandIndex] {
        &self.completion_order
    }

    /// The output, if the run completed.
    pub fn output(&self) -> Option<&Payload> {
        self.output.as_ref()
    }

    /// The failure or parking reason, if the run failed or was parked.
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// The number of journal entries applied.
    pub fn len(&self) -> usize {
        self.entries
    }

    /// Always false: a run's journal contains at least its `Started` entry.
    pub fn is_empty(&self) -> bool {
        false
    }

    fn next_command(&self) -> Result<CommandIndex, JournalError> {
        u32::try_from(self.steps.len())
            .map(CommandIndex)
            .map_err(|_| JournalError::TooManyCommands)
    }

    fn record(&mut self, cmd: CommandIndex, outcome: StepOutcome) -> Result<(), JournalError> {
        let step = usize::try_from(cmd.0)
            .ok()
            .and_then(|index| self.steps.get_mut(index))
            .ok_or(JournalError::UnknownCommand(cmd))?;
        if step.outcome.is_some() {
            return Err(JournalError::DuplicateCompletion(cmd));
        }
        step.outcome = Some(outcome);
        self.completion_order.push(cmd);
        Ok(())
    }

    fn handle_result(&self, cmd: CommandIndex, outcome: StepOutcome) -> Transition {
        let Some(step) = usize::try_from(cmd.0).ok().and_then(|i| self.steps.get(i)) else {
            // Not a command of this run (for example, from an earlier epoch): ignore.
            return Transition::none();
        };
        if step.outcome.is_some() {
            // At most once: a repeated execution's result is dropped.
            return Transition::none();
        }
        let entry = match outcome {
            StepOutcome::Completed(result) => Entry::StepCompleted { cmd, result },
            StepOutcome::Failed(error) => Entry::StepFailed { cmd, error },
        };
        Transition {
            entries: vec![entry],
            effects: vec![Effect::Activate],
        }
    }

    fn handle_decision(&self, commands: Vec<Command>) -> Transition {
        let known = self.steps.len();
        let total = commands.len();
        let mut transition = Transition::none();
        let mut steps_seen = 0usize;

        for (position, command) in commands.into_iter().enumerate() {
            match command {
                Command::ScheduleStep { name, input } => {
                    if let Some(recorded) = self.steps.get(steps_seen) {
                        if recorded.name != name || recorded.input != input {
                            return Transition::park(format!(
                                "command {steps_seen} diverged: the journal has step `{}`, the logic issued step `{name}`{}",
                                recorded.name,
                                if recorded.name == name {
                                    " with a different input"
                                } else {
                                    ""
                                },
                            ));
                        }
                    } else {
                        let Ok(index) = u32::try_from(steps_seen) else {
                            return Transition::park("too many commands in one run".into());
                        };
                        let cmd = CommandIndex(index);
                        transition.entries.push(Entry::StepScheduled {
                            cmd,
                            name: name.clone(),
                            input: input.clone(),
                        });
                        transition
                            .effects
                            .push(Effect::DispatchStep { cmd, name, input });
                    }
                    steps_seen += 1;
                }
                Command::CompleteRun { .. } | Command::FailRun { .. } | Command::Park { .. }
                    if position + 1 != total =>
                {
                    return Transition::park(format!(
                        "the logic issued a terminal command at position {position} that is not its last command"
                    ));
                }
                Command::CompleteRun { output } => {
                    if steps_seen < known {
                        break;
                    }
                    transition.entries.push(Entry::RunCompleted { output });
                    transition.effects.push(Effect::Finished(Status::Completed));
                }
                Command::Park { reason } => {
                    return Transition::park(reason);
                }
                Command::FailRun { error } => {
                    if steps_seen < known {
                        break;
                    }
                    transition.entries.push(Entry::RunFailed { error });
                    transition.effects.push(Effect::Finished(Status::Failed));
                }
            }
        }

        if steps_seen < known {
            return Transition::park(format!(
                "the logic issued {steps_seen} step commands, but the journal has {known}"
            ));
        }
        transition
    }
}
