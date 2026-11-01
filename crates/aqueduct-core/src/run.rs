//! The run state machine.

use crate::decider::{Command, Env, RunView};
use crate::journal::{AgentResponse, CommandIndex, Entry, Payload, StepOutcome};

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
    /// The run ended and a new run of the same key continues it (continue-as-new).
    Continued,
}

impl Status {
    /// Whether the run has ended (completed, failed or parked).
    pub fn is_terminal(self) -> bool {
        !matches!(self, Status::Running)
    }
}

/// The error of a signal wait whose timeout fired.
pub const TIMED_OUT: &str = "timed out";

/// The error of a command that a cancellation completed: `cancelled: <reason>`.
pub fn cancelled_error(reason: &str) -> String {
    format!("cancelled: {reason}")
}

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
    /// A value recorded from outside the logic. It is born completed.
    Recorded,
    /// An escalation, waiting for a person or an agent to resolve it. Its outcome is the resolution.
    Escalation,
    /// An agent call (a bounded model call), executed by the engine's agent executor. Its outcome is the
    /// model's response.
    Agent {
        /// The most tokens the call may use.
        budget_tokens: u64,
    },
}

impl CommandKind {
    /// Whether commands of this kind are dispatched for execution (steps and agent calls): they can be
    /// retried, re-dispatched after a restart, and their late results are dropped.
    pub fn is_dispatched(self) -> bool {
        matches!(self, CommandKind::Step | CommandKind::Agent { .. })
    }
}

/// A command as recorded in the journal, with its outcome if one has been recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduledStep {
    /// The step's name, the signal's name, the recorded value's name, or `sleep`.
    pub name: String,
    /// The step's input (empty for sleeps and signal waits).
    pub input: Payload,
    /// The kind of command.
    pub kind: CommandKind,
    /// The recorded outcome, if any.
    pub outcome: Option<StepOutcome>,
    /// Attempts of this step that failed with a retryable error.
    pub failed_attempts: u32,
    /// When the next attempt is due, while a retry is pending (milliseconds since the Unix epoch).
    pub retry_at: Option<u64>,
    /// Whether a cancellation completed this command (its outcome is then a failure).
    pub cancelled: bool,
}

impl ScheduledStep {
    /// The effect that executes this command, if it is executed by the engine.
    fn dispatch(&self, cmd: CommandIndex) -> Option<Effect> {
        match self.kind {
            CommandKind::Step => Some(Effect::DispatchStep {
                cmd,
                name: self.name.clone(),
                input: self.input.clone(),
            }),
            CommandKind::Agent { budget_tokens } => Some(Effect::DispatchAgent {
                cmd,
                name: self.name.clone(),
                request: self.input.clone(),
                budget_tokens,
            }),
            _ => None,
        }
    }

    fn describe(&self) -> String {
        match self.kind {
            CommandKind::Step => format!("step `{}`", self.name),
            CommandKind::Agent { budget_tokens } => {
                format!("agent call `{}` (budget {budget_tokens} tokens)", self.name)
            }
            CommandKind::Sleep { duration_ms, .. } => format!("a sleep of {duration_ms} ms"),
            CommandKind::Signal { timeout_ms, .. } => {
                format!(
                    "a wait for signal `{}` (timeout {timeout_ms} ms)",
                    self.name
                )
            }
            CommandKind::Recorded => format!("a recorded value `{}`", self.name),
            CommandKind::Escalation => format!("an escalation `{}`", self.name),
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
    /// An agent call finished: the model's response with its identity and usage, or a terminal failure.
    /// May arrive more than once for the same command.
    AgentResult {
        /// The command the result belongs to.
        cmd: CommandIndex,
        /// The response, or the failure.
        outcome: Result<AgentResponse, String>,
    },
    /// A step attempt failed with a retryable error, and the engine's retry policy allows another
    /// attempt after `retry_after_ms`. (When it does not, the engine reports a terminal
    /// [`Event::StepResult`] instead.)
    StepAttemptFailed {
        /// The step's command index.
        cmd: CommandIndex,
        /// The attempt's error.
        error: String,
        /// The backoff before the next attempt.
        retry_after_ms: u64,
    },
    /// A timer is due (the engine fires it no earlier than its wake time).
    TimerFired {
        /// The command whose timer is due.
        cmd: CommandIndex,
    },
    /// A person or an agent resolved an escalation. Only the first resolution counts.
    ResolveEscalation {
        /// The escalation's command.
        cmd: CommandIndex,
        /// The resolution.
        outcome: Payload,
    },
    /// Someone asked the run to stop. Only the first cancellation counts.
    Cancel {
        /// Why.
        reason: String,
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
    /// Run an agent call and feed the result back as [`Event::AgentResult`].
    DispatchAgent {
        /// The call's command index.
        cmd: CommandIndex,
        /// The call's name.
        name: String,
        /// The request (JSON).
        request: Payload,
        /// The most tokens the call may use.
        budget_tokens: u64,
    },
    /// The run reached a terminal status.
    Finished(Status),
    /// Start the key's next run with the same workflow and this input (the run continued as new).
    ContinueAsNew {
        /// The next run's input.
        input: Payload,
    },
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
    /// `CancelRequested` occurs more than once.
    #[error("the run is cancelled more than once")]
    DuplicateCancel,
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
    /// The cancellation's reason, once one is recorded.
    cancel: Option<String>,
    /// The next run's input, if the run continued as new.
    continued_with: Option<Payload>,
    /// Tokens used by the run's completed agent calls.
    tokens_used: u64,
    /// A cancellation found no pending command; the next waiting command takes it.
    pending_cancel: bool,
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
            cancel: None,
            continued_with: None,
            tokens_used: 0,
            pending_cancel: false,
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
            Entry::Escalated {
                cmd,
                reason,
                payload,
            } => {
                self.push_command(*cmd, reason, payload.clone(), CommandKind::Escalation)?;
            }
            Entry::EscalationResolved { cmd, outcome } => {
                self.expect_kind(*cmd, |kind| kind == CommandKind::Escalation)?;
                self.record(*cmd, StepOutcome::Completed(outcome.clone()))?;
            }
            Entry::CancelRequested { reason } => {
                if self.cancel.is_some() {
                    return Err(JournalError::DuplicateCancel);
                }
                let pending: Vec<CommandIndex> = self
                    .steps
                    .iter()
                    .zip(0u32..)
                    .filter(|(step, _)| step.outcome.is_none())
                    .map(|(_, index)| CommandIndex(index))
                    .collect();
                self.pending_cancel = pending.is_empty();
                self.cancel = Some(reason.clone());
                for cmd in pending {
                    self.cancel_command(cmd)?;
                }
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
                let cancelled = self.command(*cmd)?.cancelled;
                if let Some(position) = self
                    .buffered_signals
                    .iter()
                    .position(|(n, _)| n == name && !cancelled)
                {
                    let (_, payload) = self.buffered_signals.remove(position);
                    self.record(*cmd, StepOutcome::Completed(payload))?;
                }
            }
            Entry::ValueRecorded { cmd, name, value } => {
                self.push_command(*cmd, name, Payload::default(), CommandKind::Recorded)?;
                self.record(*cmd, StepOutcome::Completed(value.clone()))?;
            }
            Entry::StepCompleted { cmd, result } => {
                self.expect_kind(*cmd, |kind| kind == CommandKind::Step)?;
                self.record(*cmd, StepOutcome::Completed(result.clone()))?;
            }
            Entry::AgentCallScheduled {
                cmd,
                name,
                request,
                budget_tokens,
            } => {
                let kind = CommandKind::Agent {
                    budget_tokens: *budget_tokens,
                };
                self.push_command(*cmd, name, request.clone(), kind)?;
            }
            Entry::AgentCallCompleted { cmd, response } => {
                self.expect_kind(*cmd, |kind| matches!(kind, CommandKind::Agent { .. }))?;
                self.record(*cmd, StepOutcome::Completed(response.response.clone()))?;
                self.tokens_used = self.tokens_used.saturating_add(response.tokens());
            }
            Entry::StepFailed { cmd, error } => {
                self.expect_kind(*cmd, CommandKind::is_dispatched)?;
                self.record(*cmd, StepOutcome::Failed(error.clone()))?;
            }
            Entry::StepRetryScheduled {
                cmd,
                attempt,
                wake_at,
                ..
            } => {
                let index = self.index(*cmd)?;
                let step = &mut self.steps[index];
                let valid = step.kind.is_dispatched()
                    && step.outcome.is_none()
                    && step.retry_at.is_none()
                    && *attempt == step.failed_attempts + 1;
                if !valid {
                    return Err(JournalError::InvalidCompletion(*cmd));
                }
                step.failed_attempts = *attempt;
                step.retry_at = Some(*wake_at);
            }
            Entry::TimerFired { cmd } if self.command(*cmd)?.kind.is_dispatched() => {
                // A retry is due: the step is dispatched again; it still has no outcome.
                let index = self.index(*cmd)?;
                if self.steps[index].retry_at.take().is_none()
                    || self.steps[index].outcome.is_some()
                {
                    return Err(JournalError::InvalidCompletion(*cmd));
                }
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
            Entry::ContinuedAsNew { input } => {
                self.status = Status::Continued;
                self.continued_with = Some(input.clone());
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
            Event::AgentResult { cmd, outcome } => self.handle_agent_result(cmd, outcome),
            Event::Decision(commands) => self.handle_decision(commands, now_ms),
            Event::TimerFired { cmd } => self.handle_timer(cmd),
            Event::StepAttemptFailed {
                cmd,
                error,
                retry_after_ms,
            } => self.handle_attempt_failed(cmd, error, retry_after_ms, now_ms),
            Event::Signal { name, id, payload } => self.handle_signal(name, id, payload),
            Event::ResolveEscalation { cmd, outcome } => {
                match self.command(cmd) {
                    Ok(step) if step.kind == CommandKind::Escalation && step.outcome.is_none() => {
                        Transition {
                            entries: vec![Entry::EscalationResolved { cmd, outcome }],
                            effects: vec![Effect::Activate],
                        }
                    }
                    // Unknown, not an escalation, or already resolved (or cancelled): dropped.
                    _ => Transition::none(),
                }
            }
            Event::Cancel { reason } => {
                if self.cancel.is_some() {
                    return Transition::none();
                }
                Transition {
                    entries: vec![Entry::CancelRequested { reason }],
                    effects: vec![Effect::Activate],
                }
            }
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
                CommandKind::Step | CommandKind::Agent { .. } => {
                    step.retry_at.map(|wake_at| (CommandIndex(index), wake_at))
                }
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
            .filter(|(step, _)| {
                step.outcome.is_none() && step.kind.is_dispatched() && step.retry_at.is_none()
            })
            .filter_map(|(step, index)| step.dispatch(CommandIndex(index)))
            .collect()
    }

    /// The view a decider sees, with an empty [`Env`] (for replay and tests: values already in the
    /// journal do not need one).
    pub fn view(&self) -> RunView<'_> {
        RunView::new(self, "", Env::default())
    }

    /// The view a decider sees in an activation of the run with this key, with the engine's clock and
    /// seed for new values.
    pub fn view_at<'a>(&'a self, key: &'a str, env: Env) -> RunView<'a> {
        RunView::new(self, key, env)
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

    /// Escalations waiting for a resolution: `(command, reason, payload)`.
    pub fn open_escalations(&self) -> Vec<(CommandIndex, &str, &Payload)> {
        if self.status.is_terminal() {
            return Vec::new();
        }
        self.steps
            .iter()
            .zip(0u32..)
            .filter(|(step, _)| step.kind == CommandKind::Escalation && step.outcome.is_none())
            .map(|(step, index)| (CommandIndex(index), step.name.as_str(), &step.input))
            .collect()
    }

    /// Tokens used by the run's completed agent calls.
    pub fn tokens_used(&self) -> u64 {
        self.tokens_used
    }

    /// The next run's input, if the run continued as new.
    pub fn continued_with(&self) -> Option<&Payload> {
        self.continued_with.as_ref()
    }

    /// The cancellation's reason, if the run was cancelled.
    pub fn cancel_reason(&self) -> Option<&str> {
        self.cancel.as_deref()
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
            failed_attempts: 0,
            retry_at: None,
            cancelled: false,
        });
        // A cancellation that found nothing pending is taken by the next command that waits.
        if self.pending_cancel && kind != CommandKind::Recorded {
            self.pending_cancel = false;
            self.cancel_command(cmd)?;
        }
        Ok(())
    }

    fn cancel_command(&mut self, cmd: CommandIndex) -> Result<(), JournalError> {
        let reason = cancelled_error(self.cancel.as_deref().unwrap_or_default());
        self.record(cmd, StepOutcome::Failed(reason))?;
        let index = self.index(cmd)?;
        self.steps[index].cancelled = true;
        self.steps[index].retry_at = None;
        Ok(())
    }

    fn index(&self, cmd: CommandIndex) -> Result<usize, JournalError> {
        usize::try_from(cmd.0)
            .ok()
            .filter(|index| *index < self.steps.len())
            .ok_or(JournalError::UnknownCommand(cmd))
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

    fn handle_agent_result(
        &self,
        cmd: CommandIndex,
        outcome: Result<AgentResponse, String>,
    ) -> Transition {
        let Ok(step) = self.command(cmd) else {
            return Transition::none();
        };
        if step.outcome.is_some() || !matches!(step.kind, CommandKind::Agent { .. }) {
            // At most once: a repeated call's result is dropped.
            return Transition::none();
        }
        let budget = match step.kind {
            CommandKind::Agent { budget_tokens } => budget_tokens,
            _ => 0,
        };
        let entry = match outcome {
            // The budget is a hard limit: an answer that cost more is not recorded as an answer.
            Ok(response) if response.tokens() > budget => Entry::StepFailed {
                cmd,
                error: format!(
                    "agent call `{}` used {} tokens, over its budget of {budget}",
                    step.name,
                    response.tokens()
                ),
            },
            Ok(response) => Entry::AgentCallCompleted { cmd, response },
            Err(error) => Entry::StepFailed { cmd, error },
        };
        Transition {
            entries: vec![entry],
            effects: vec![Effect::Activate],
        }
    }

    fn handle_attempt_failed(
        &self,
        cmd: CommandIndex,
        error: String,
        retry_after_ms: u64,
        now_ms: u64,
    ) -> Transition {
        let Ok(step) = self.command(cmd) else {
            return Transition::none();
        };
        if !step.kind.is_dispatched() || step.outcome.is_some() || step.retry_at.is_some() {
            // Already completed, or already waiting to retry (a duplicate report): nothing to do.
            return Transition::none();
        }
        Transition {
            entries: vec![Entry::StepRetryScheduled {
                cmd,
                attempt: step.failed_attempts + 1,
                error,
                wake_at: now_ms.saturating_add(retry_after_ms),
            }],
            effects: Vec::new(),
        }
    }

    fn handle_timer(&self, cmd: CommandIndex) -> Transition {
        let Ok(step) = self.command(cmd) else {
            return Transition::none();
        };
        if step.kind.is_dispatched() {
            if step.outcome.is_some() || step.retry_at.is_none() {
                return Transition::none();
            }
            return Transition {
                entries: vec![Entry::TimerFired { cmd }],
                effects: step.dispatch(cmd).into_iter().collect(),
            };
        }
        let has_timer = match step.kind {
            CommandKind::Sleep { .. } => true,
            CommandKind::Signal { wake_at, .. } => wake_at > 0,
            CommandKind::Step
            | CommandKind::Recorded
            | CommandKind::Escalation
            | CommandKind::Agent { .. } => false,
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
        // A cancellation waiting for the next command: that command is recorded but never started.
        let mut cancel_next = self.pending_cancel;
        // Signals already received that new waits in this decision will take, by name.
        let mut buffered: Vec<&str> = self
            .buffered_signals
            .iter()
            .map(|(n, _)| n.as_str())
            .collect();

        for (position, command) in commands.into_iter().enumerate() {
            let new_kind = match &command {
                Command::ScheduleStep { .. } => Some(CommandKind::Step),
                Command::CallAgent { budget_tokens, .. } => Some(CommandKind::Agent {
                    budget_tokens: *budget_tokens,
                }),
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
                Command::Record { .. } => Some(CommandKind::Recorded),
                Command::Escalate { .. } => Some(CommandKind::Escalation),
                _ => None,
            };
            if let Some(kind) = new_kind {
                let (name, input) = match &command {
                    Command::ScheduleStep { name, input }
                    | Command::CallAgent {
                        name,
                        request: input,
                        ..
                    } => (name.clone(), input.clone()),
                    Command::Escalate { reason, payload } => (reason.clone(), payload.clone()),
                    Command::AwaitSignal { name, .. } | Command::Record { name, .. } => {
                        (name.clone(), Payload::default())
                    }
                    _ => ("sleep".to_owned(), Payload::default()),
                };
                let issued = ScheduledStep {
                    name,
                    input,
                    kind,
                    outcome: None,
                    failed_attempts: 0,
                    retry_at: None,
                    cancelled: false,
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
                    if cancel_next && issued.kind != CommandKind::Recorded {
                        cancel_next = false;
                        transition.entries.push(match issued.kind {
                            CommandKind::Step => Entry::StepScheduled {
                                cmd,
                                name: issued.name,
                                input: issued.input,
                            },
                            CommandKind::Sleep {
                                duration_ms,
                                wake_at,
                            } => Entry::TimerStarted {
                                cmd,
                                duration_ms,
                                wake_at,
                            },
                            CommandKind::Signal {
                                timeout_ms,
                                wake_at,
                            } => Entry::SignalAwaited {
                                cmd,
                                name: issued.name,
                                timeout_ms,
                                wake_at,
                            },
                            CommandKind::Escalation => Entry::Escalated {
                                cmd,
                                reason: issued.name,
                                payload: issued.input,
                            },
                            CommandKind::Agent { budget_tokens } => Entry::AgentCallScheduled {
                                cmd,
                                name: issued.name,
                                request: issued.input,
                                budget_tokens,
                            },
                            CommandKind::Recorded => continue,
                        });
                        if !transition.effects.contains(&Effect::Activate) {
                            transition.effects.push(Effect::Activate);
                        }
                        steps_seen += 1;
                        continue;
                    }
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
                        CommandKind::Agent { budget_tokens } => {
                            transition.entries.push(Entry::AgentCallScheduled {
                                cmd,
                                name: issued.name.clone(),
                                request: issued.input.clone(),
                                budget_tokens,
                            });
                            transition.effects.push(Effect::DispatchAgent {
                                cmd,
                                name: issued.name,
                                request: issued.input,
                                budget_tokens,
                            });
                        }
                        CommandKind::Escalation => {
                            transition.entries.push(Entry::Escalated {
                                cmd,
                                reason: issued.name,
                                payload: issued.input,
                            });
                        }
                        CommandKind::Recorded => {
                            let value = match command {
                                Command::Record { value, .. } => value,
                                _ => Payload::default(),
                            };
                            transition.entries.push(Entry::ValueRecorded {
                                cmd,
                                name: issued.name,
                                value,
                            });
                            // The logic may have waited for the value instead of using it at once.
                            if !transition.effects.contains(&Effect::Activate) {
                                transition.effects.push(Effect::Activate);
                            }
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
                | Command::AwaitSignal { .. }
                | Command::Record { .. }
                | Command::Escalate { .. }
                | Command::CallAgent { .. } => {}
                Command::CompleteRun { .. }
                | Command::FailRun { .. }
                | Command::Park { .. }
                | Command::ContinueAsNew { .. }
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
                Command::ContinueAsNew { input } => {
                    if steps_seen < known {
                        break;
                    }
                    transition.entries.push(Entry::ContinuedAsNew {
                        input: input.clone(),
                    });
                    transition.effects.push(Effect::Finished(Status::Continued));
                    transition.effects.push(Effect::ContinueAsNew { input });
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
            (CommandKind::Recorded, CommandKind::Recorded) => true,
            (CommandKind::Escalation, CommandKind::Escalation) => true,
            (CommandKind::Agent { budget_tokens: a }, CommandKind::Agent { budget_tokens: b }) => {
                a == b
            }
            _ => false,
        }
}
