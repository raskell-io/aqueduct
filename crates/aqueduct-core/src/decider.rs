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
    /// Call a model, bounded by a token budget (an agent step). The engine's agent executor runs it; the
    /// response, model identity and usage are journaled, and replay reads them instead of calling again.
    CallAgent {
        /// The agent call's name.
        name: String,
        /// The request (JSON: input, prompt, output schema).
        request: Payload,
        /// The most tokens the call may use.
        budget_tokens: u64,
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
    /// Escalate a decision to a person or an agent, and wait for its resolution (the outcome).
    Escalate {
        /// Why; escalations are counted by reason.
        reason: String,
        /// What the resolver needs to decide.
        payload: Payload,
    },
    /// Start a child run of `workflow` with `input`, and wait for its outcome: its output, or its
    /// failure. Under `key`, a child run is shared: if the key already has a run of the same workflow
    /// with the same input, its outcome is the command's. Without one, the key is derived from the
    /// parent's run id and the command ([`crate::child_key`]), so the child is the parent's alone.
    StartChild {
        /// The child's workflow.
        workflow: String,
        /// The child's input.
        input: Payload,
        /// The child's run key, if the logic chooses one.
        key: Option<String>,
    },
    /// Wait for the next update with this name, or until the timeout. Its outcome is the update's
    /// payload, or `Failed(TIMED_OUT)`. While the wait is pending, the engine accepts an update of this
    /// name; the logic answers it with [`Command::Respond`].
    AwaitUpdate {
        /// The update's name.
        name: String,
        /// The timeout in milliseconds; 0 waits forever.
        timeout_ms: u64,
    },
    /// Answer the update received by the wait `update`: with a result, or a rejection. Born completed.
    /// Each received update is answered at most once.
    Respond {
        /// The wait that received the update.
        update: CommandIndex,
        /// The result, or why the update is rejected.
        result: Result<Payload, String>,
    },
    /// Send a message: call a virtual object's handler without waiting for it (ADR 0044). Born completed.
    /// The message is durable with the command, and delivered exactly once.
    Send {
        /// The target object.
        object: String,
        /// The target object key.
        key: String,
        /// The handler to call.
        handler: String,
        /// The call's arguments (JSON).
        args: Payload,
    },
    /// Complete the run. Must be the last command.
    CompleteRun {
        /// The run's output.
        output: Payload,
    },
    /// End the run and start a new run of the same workflow under the same key with `input`
    /// (continue-as-new): a long-lived process keeps its journal short. Must be the last command.
    ContinueAsNew {
        /// The next run's input.
        input: Payload,
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

/// The prefix of the names of recorded values that mark a patch (see [`RunView::patched`]). Value names
/// starting with it are reserved.
pub const PATCH_PREFIX: &str = "$patch/";

/// The recorded value that marks patch `id`: [`Command::Record`] under this name, with the value `true`.
pub fn patch_marker(id: &str) -> Command {
    Command::Record {
        name: format!("{PATCH_PREFIX}{id}"),
        value: Payload::new(*b"true"),
    }
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

    /// Whether the run takes patch `id`'s new code at command `cmd`, the next command the logic issues
    /// (ADR 0035). A run that has not reached `cmd` yet takes it, and so does one whose journal has the
    /// patch's marker there: the logic then issues the marker ([`patch_marker`]) as command `cmd`. A run
    /// whose journal has another command there passed this point with the old code, and keeps to it:
    /// the logic issues nothing for the patch and continues with the old code at `cmd`.
    pub fn patched(&self, cmd: CommandIndex, id: &str) -> bool {
        let journaled = usize::try_from(cmd.0).is_ok_and(|index| index < self.state.steps().len());
        !journaled || self.patch_at(cmd) == Some(id)
    }

    /// The id of the patch whose marker the journal holds as command `cmd`, if it holds one there.
    pub fn patch_at(&self, cmd: CommandIndex) -> Option<&'a str> {
        let step = self.state.steps().get(usize::try_from(cmd.0).ok()?)?;
        if step.kind != crate::CommandKind::Recorded {
            return None;
        }
        step.name.strip_prefix(PATCH_PREFIX)
    }

    /// How many signals with this name are kept, waiting for a command to wait for them. It depends on
    /// when the logic runs, not only on the journal, so logic may use it only for decisions that are
    /// not replayed: at the end of the journal, as a run that continues as new.
    pub fn buffered_signals(&self, name: &str) -> usize {
        self.state.buffered_signals(name)
    }

    /// Whether a cancellation completed this command (its outcome is then a failure).
    pub fn cancelled(&self, cmd: CommandIndex) -> bool {
        usize::try_from(cmd.0)
            .ok()
            .and_then(|index| self.state.steps().get(index))
            .is_some_and(|step| step.cancelled)
    }

    /// Tokens used so far by the run's completed agent calls.
    pub fn tokens_used(&self) -> u64 {
        self.state.tokens_used()
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

    /// Answers query `name` about the run (a read-only look at its state), or `None` if the logic has
    /// no such query. Pure, like [`Decider::decide`], and nothing it does is journaled. Procedures and
    /// Rust workflows answer with the last value they published under the name, replaying the journal
    /// to where the run is (ADR 0036). The default has no queries.
    fn query(&self, view: &RunView<'_>, name: &str) -> Option<Payload> {
        let _ = (view, name);
        None
    }
}
