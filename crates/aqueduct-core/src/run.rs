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

/// The error of a signal wait whose timeout fired.
pub const TIMED_OUT: &str = "timed out";

/// What kind of command a scheduled command is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandKind {
    /// A step, executed by a worker.
    Step,
    /// A durable sleep. Its outcome is `Completed(null)` when it fires.
    Sleep {
        /// The requested duration in milliseconds.
        duration_ms: u64,
        /// When it fires, in milliseconds since the Unix epoch.
        wake_at: u64,
    },
    /// A wait for a signal. Its outcome is `Completed(payload)`, or `Failed(TIMED_OUT)`.
    Signal {
        /// The timeout in milliseconds; 0 means none.
        timeout_ms: u64,
        /// When the timeout fires; 0 means never.
        wake_at: u64,
    },
}

/// A command as recorded in the journal, with its outcome if one has been recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduledStep {
    /// The step's name, the signal's name, or `sleep`.
    pub name: String,
    /// The step's input (empty for sleeps and signal waits).
    pub input: Payload,
    /// The kind of command.
    pub kind: CommandKind,
    /// The recorded outcome, if any.
    pub outcome: Option<StepOutcome>,
}

impl ScheduledStep {
    fn describe(&self) -> String {
        match self.kind {
            CommandKind::Step => format!("step `{}`", self.name),
            CommandKind::Sleep { duration_ms, .. } => format!("a sleep of {duration_ms} ms"),
            CommandKind::Signal { timeout_ms, .. } => {
                format!(
                    "a wait for signal `{}` (timeout {timeout_ms} ms)",
                    self.name
                )
            }
        }
    }
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
    /// A timer is due (the engine fires it no earlier than its wake time).
    TimerFired {
        /// The command whose timer is due.
        cmd: CommandIndex,
    },
    /// A signal was sent to the run.
    Signal {
        /// The signal's name.
        name: String,
        /// The sender's id for this signal: a signal whose id was already recorded is dropped, which
        /// makes retried sends exactly-once. Empty means no deduplication.
        id: String,
        /// The signal's payload.
        payload: Payload,
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
    /// Two signals carry the same id.
    #[error("signal id `{0}` is recorded twice")]
    DuplicateSignal(String),
    /// A completion does not fit the kind of its command (for example, a timer for a step).
    #[error("completion of the wrong kind for command {0}")]
    InvalidCompletion(CommandIndex),
}

/// The state of one run: by definition, the fold of its journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunState {
    workflow: String,
    status: Status,
    input: Payload,
    steps: Vec<ScheduledStep>,
    completion_order: Vec<CommandIndex>,
    buffered_signals: Vec<(String, Payload)>,
    signal_ids: std::collections::BTreeSet<String>,
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
            buffered_signals: Vec::new(),
            signal_ids: std::collections::BTreeSet::new(),
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
                self.push_command(*cmd, name, input.clone(), CommandKind::Step)?;
            }
            Entry::TimerStarted {
                cmd,
                duration_ms,
                wake_at,
            } => {
                let kind = CommandKind::Sleep {
                    duration_ms: *duration_ms,
                    wake_at: *wake_at,
                };
                self.push_command(*cmd, "sleep", Payload::default(), kind)?;
            }
            Entry::SignalAwaited {
                cmd,
                name,
                timeout_ms,
                wake_at,
            } => {
                let kind = CommandKind::Signal {
                    timeout_ms: *timeout_ms,
                    wake_at: *wake_at,
                };
                self.push_command(*cmd, name, Payload::default(), kind)?;
                if let Some(position) = self.buffered_signals.iter().position(|(n, _)| n == name) {
                    let (_, payload) = self.buffered_signals.remove(position);
                    self.record(*cmd, StepOutcome::Completed(payload))?;
                }
            }
            Entry::StepCompleted { cmd, result } => {
                self.expect_kind(*cmd, |kind| kind == CommandKind::Step)?;
                self.record(*cmd, StepOutcome::Completed(result.clone()))?;
            }
            Entry::StepFailed { cmd, error } => {
                self.expect_kind(*cmd, |kind| kind == CommandKind::Step)?;
                self.record(*cmd, StepOutcome::Failed(error.clone()))?;
            }
            Entry::TimerFired { cmd } => {
                let outcome = match self.command(*cmd)?.kind {
                    CommandKind::Sleep { .. } => StepOutcome::Completed(Payload::new(*b"null")),
                    CommandKind::Signal { wake_at, .. } if wake_at > 0 => {
                        StepOutcome::Failed(TIMED_OUT.to_owned())
                    }
                    _ => return Err(JournalError::InvalidCompletion(*cmd)),
                };
                self.record(*cmd, outcome)?;
            }
            Entry::SignalReceived { name, id, payload } => {
                if !id.is_empty() && !self.signal_ids.insert(id.clone()) {
                    return Err(JournalError::DuplicateSignal(id.clone()));
                }
                match self.waiting_for(name) {
                    Some(cmd) => self.record(cmd, StepOutcome::Completed(payload.clone()))?,
                    None => self.buffered_signals.push((name.clone(), payload.clone())),
                }
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

    /// Handles an event at time zero. Pure; see [`RunState::handle_at`].
    pub fn handle(&self, event: Event) -> Transition {
        self.handle_at(event, 0)
    }

    /// Handles an event. Pure: returns what to append and what to do, without changing the state.
    /// `now_ms` (milliseconds since the Unix epoch, by the engine's clock) is used only to compute the
    /// wake time of timers started by this event, which is then journaled; replay never reads it.
    pub fn handle_at(&self, event: Event, now_ms: u64) -> Transition {
        if self.status.is_terminal() {
            return Transition::none();
        }
        match event {
            Event::StepResult { cmd, outcome } => self.handle_result(cmd, outcome),
            Event::Decision(commands) => self.handle_decision(commands, now_ms),
            Event::TimerFired { cmd } => self.handle_timer(cmd),
            Event::Signal { name, id, payload } => self.handle_signal(name, id, payload),
        }
    }

    /// Timers that have not fired: `(command, wake_at)`.
    pub fn timers(&self) -> Vec<(CommandIndex, u64)> {
        if self.status.is_terminal() {
            return Vec::new();
        }
        self.steps
            .iter()
            .zip(0u32..)
            .filter(|(step, _)| step.outcome.is_none())
            .filter_map(|(step, index)| match step.kind {
                CommandKind::Sleep { wake_at, .. } => Some((CommandIndex(index), wake_at)),
                CommandKind::Signal { wake_at, .. } if wake_at > 0 => {
                    Some((CommandIndex(index), wake_at))
                }
                _ => None,
            })
            .collect()
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
            .filter(|(step, _)| step.outcome.is_none() && step.kind == CommandKind::Step)
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

    fn push_command(
        &mut self,
        cmd: CommandIndex,
        name: &str,
        input: Payload,
        kind: CommandKind,
    ) -> Result<(), JournalError> {
        let expected = self.next_command()?;
        if cmd != expected {
            return Err(JournalError::CommandOutOfOrder {
                expected,
                found: cmd,
            });
        }
        self.steps.push(ScheduledStep {
            name: name.to_owned(),
            input,
            kind,
            outcome: None,
        });
        Ok(())
    }

    fn command(&self, cmd: CommandIndex) -> Result<&ScheduledStep, JournalError> {
        usize::try_from(cmd.0)
            .ok()
            .and_then(|index| self.steps.get(index))
            .ok_or(JournalError::UnknownCommand(cmd))
    }

    fn expect_kind(
        &self,
        cmd: CommandIndex,
        ok: impl Fn(CommandKind) -> bool,
    ) -> Result<(), JournalError> {
        if ok(self.command(cmd)?.kind) {
            Ok(())
        } else {
            Err(JournalError::InvalidCompletion(cmd))
        }
    }

    /// The oldest command waiting for a signal with this name.
    fn waiting_for(&self, name: &str) -> Option<CommandIndex> {
        self.steps
            .iter()
            .zip(0u32..)
            .find(|(step, _)| {
                matches!(step.kind, CommandKind::Signal { .. })
                    && step.name == name
                    && step.outcome.is_none()
            })
            .map(|(_, index)| CommandIndex(index))
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
        if step.outcome.is_some() || step.kind != CommandKind::Step {
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

    fn handle_timer(&self, cmd: CommandIndex) -> Transition {
        let Ok(step) = self.command(cmd) else {
            return Transition::none();
        };
        let has_timer = match step.kind {
            CommandKind::Sleep { .. } => true,
            CommandKind::Signal { wake_at, .. } => wake_at > 0,
            CommandKind::Step => false,
        };
        if step.outcome.is_some() || !has_timer {
            return Transition::none();
        }
        Transition {
            entries: vec![Entry::TimerFired { cmd }],
            effects: vec![Effect::Activate],
        }
    }

    fn handle_signal(&self, name: String, id: String, payload: Payload) -> Transition {
        if !id.is_empty() && self.signal_ids.contains(&id) {
            // Already recorded: a retried or duplicated send.
            return Transition::none();
        }
        let wakes = self.waiting_for(&name).is_some();
        Transition {
            entries: vec![Entry::SignalReceived { name, id, payload }],
            effects: if wakes {
                vec![Effect::Activate]
            } else {
                Vec::new()
            },
        }
    }

    fn handle_decision(&self, commands: Vec<Command>, now_ms: u64) -> Transition {
        let known = self.steps.len();
        let total = commands.len();
        let mut transition = Transition::none();
        let mut steps_seen = 0usize;
        // Signals already received that new waits in this decision will take, by name.
        let mut buffered: Vec<&str> = self
            .buffered_signals
            .iter()
            .map(|(n, _)| n.as_str())
            .collect();

        for (position, command) in commands.into_iter().enumerate() {
            let new_kind = match &command {
                Command::ScheduleStep { .. } => Some(CommandKind::Step),
                Command::Sleep { duration_ms } => Some(CommandKind::Sleep {
                    duration_ms: *duration_ms,
                    wake_at: now_ms.saturating_add(*duration_ms),
                }),
                Command::AwaitSignal { timeout_ms, .. } => Some(CommandKind::Signal {
                    timeout_ms: *timeout_ms,
                    wake_at: if *timeout_ms > 0 {
                        now_ms.saturating_add(*timeout_ms)
                    } else {
                        0
                    },
                }),
                _ => None,
            };
            if let Some(kind) = new_kind {
                let (name, input) = match &command {
                    Command::ScheduleStep { name, input } => (name.clone(), input.clone()),
                    Command::AwaitSignal { name, .. } => (name.clone(), Payload::default()),
                    _ => ("sleep".to_owned(), Payload::default()),
                };
                let issued = ScheduledStep {
                    name,
                    input,
                    kind,
                    outcome: None,
                };
                if let Some(recorded) = self.steps.get(steps_seen) {
                    if !same_command(recorded, &issued) {
                        return Transition::park(format!(
                            "command {steps_seen} diverged: the journal has {}, the logic issued {}{}",
                            recorded.describe(),
                            issued.describe(),
                            if recorded.name == issued.name && recorded.kind == CommandKind::Step {
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
                    match issued.kind {
                        CommandKind::Step => {
                            transition.entries.push(Entry::StepScheduled {
                                cmd,
                                name: issued.name.clone(),
                                input: issued.input.clone(),
                            });
                            transition.effects.push(Effect::DispatchStep {
                                cmd,
                                name: issued.name,
                                input: issued.input,
                            });
                        }
                        CommandKind::Sleep {
                            duration_ms,
                            wake_at,
                        } => {
                            transition.entries.push(Entry::TimerStarted {
                                cmd,
                                duration_ms,
                                wake_at,
                            });
                        }
                        CommandKind::Signal {
                            timeout_ms,
                            wake_at,
                        } => {
                            if let Some(position) = buffered.iter().position(|n| *n == issued.name)
                            {
                                // A signal is already here: the wait completes at once.
                                buffered.remove(position);
                                if !transition.effects.contains(&Effect::Activate) {
                                    transition.effects.push(Effect::Activate);
                                }
                            }
                            transition.entries.push(Entry::SignalAwaited {
                                cmd,
                                name: issued.name,
                                timeout_ms,
                                wake_at,
                            });
                        }
                    }
                }
                steps_seen += 1;
                continue;
            }
            match command {
                Command::ScheduleStep { .. }
                | Command::Sleep { .. }
                | Command::AwaitSignal { .. } => {}
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
                "the logic issued {steps_seen} commands, but the journal has {known}"
            ));
        }
        transition
    }
}

/// Whether a recorded command and a newly issued one are the same command. Wake times are not compared:
/// they depend on when the command was first issued and are taken from the journal.
fn same_command(recorded: &ScheduledStep, issued: &ScheduledStep) -> bool {
    recorded.name == issued.name
        && recorded.input == issued.input
        && match (recorded.kind, issued.kind) {
            (CommandKind::Step, CommandKind::Step) => true,
            (
                CommandKind::Sleep { duration_ms: a, .. },
                CommandKind::Sleep { duration_ms: b, .. },
            ) => a == b,
            (
                CommandKind::Signal { timeout_ms: a, .. },
                CommandKind::Signal { timeout_ms: b, .. },
            ) => a == b,
            _ => false,
        }
}
