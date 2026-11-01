//! The pure core of Aqueduct: the journal model, commands and events, and the state machine
//! `(state, event) -> (entries, effects)` that decides what happens to a run.
//!
//! This crate has no I/O, no clock, no randomness and no async. Everything with side effects lives
//! in other crates so it can be simulated. The execution model is described in
//! `docs/adr/0004-execution-model.md`.
//!
//! The flow for a run:
//!
//! 1. [`start`] produces the first [`Entry`] and an [`Effect::Activate`].
//! 2. The caller appends entries to the journal, applying each with [`RunState::apply`].
//! 3. Once the entries are durable, the caller performs the effects. [`Effect::Activate`] means: ask the
//!    run's [`Decider`] for its commands and feed them back with [`Event::Decision`].
//!    [`Effect::DispatchStep`] means: execute the step and feed the outcome back with
//!    [`Event::StepResult`].
//! 4. After a restart, [`RunState::from_journal`] rebuilds the state and
//!    [`RunState::recovery_effects`] says what to do next.
//!
//! ```
//! use aqueduct_core::{
//!     start, Command, CommandIndex, Decider, Effect, Entry, Event, Payload, RunState, RunView,
//!     Status, StepOutcome,
//! };
//!
//! // A decider that runs one step and completes with its result.
//! struct Echo;
//! impl Decider for Echo {
//!     fn decide(&self, view: &RunView<'_>) -> Vec<Command> {
//!         let mut commands =
//!             vec![Command::ScheduleStep { name: "echo".into(), input: view.input().clone() }];
//!         if let Some(StepOutcome::Completed(result)) = view.outcome(CommandIndex(0)) {
//!             commands.push(Command::CompleteRun { output: result.clone() });
//!         }
//!         commands
//!     }
//! }
//!
//! // Appends a transition's entries and returns its effects (a real engine makes the
//! // entries durable before performing the effects).
//! fn handle(state: &mut RunState, journal: &mut Vec<Entry>, event: Event) -> Vec<Effect> {
//!     let transition = state.handle(event);
//!     for entry in &transition.entries {
//!         state.apply(entry).expect("valid entry");
//!     }
//!     journal.extend(transition.entries);
//!     transition.effects
//! }
//!
//! let first = start("test", Payload::new(*b"hello"));
//! let mut journal = first.entries;
//! let mut state = RunState::from_journal(&journal).expect("valid journal");
//!
//! let commands = Echo.decide(&state.view());
//! let effects = handle(&mut state, &mut journal, Event::Decision(commands));
//! assert!(matches!(effects[0], Effect::DispatchStep { .. }));
//!
//! let outcome = StepOutcome::Completed(Payload::new(*b"hello"));
//! handle(&mut state, &mut journal, Event::StepResult { cmd: CommandIndex(0), outcome });
//! let commands = Echo.decide(&state.view());
//! handle(&mut state, &mut journal, Event::Decision(commands));
//!
//! assert_eq!(state.status(), Status::Completed);
//! assert_eq!(state, RunState::from_journal(&journal).expect("valid journal"));
//! ```

#![forbid(unsafe_code)]

mod awakeable;
mod decider;
mod journal;
mod run;
mod run_id;

pub use awakeable::{
    AWAKEABLE_PREFIX, awakeable_id, awakeable_signal, parse_awakeable_id, parse_command_id,
};
pub use decider::{Command, Decider, Env, RunView};
pub use journal::{AgentResponse, CommandIndex, Entry, IdempotencyKey, Payload, StepOutcome};
pub use run::{
    CommandKind, Effect, Event, JournalError, RunState, ScheduledStep, Status, TIMED_OUT,
    Transition, cancelled_error, start,
};
pub use run_id::{run_id, split_run_id, valid_key};
