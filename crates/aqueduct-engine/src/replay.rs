//! The determinism check (BRIEF.md section 5.2, mode 2; section 4.5, the replay compatibility check):
//! replay a run's journal against a decider, the code that wrote it or new code meant to continue it,
//! and report where the decider would issue something other than what the journal holds.
//!
//! The journal is replayed entry by entry. Before every entry that the logic wrote (a command, or the
//! run's end), the decider is asked what it does now; if the core would park the run on its answer, that
//! is the divergence. New code that changes a long-running workflow keeps old runs on their path with
//! `patched` (ADR 0035); this check, run over a corpus of their histories, shows that it does.

use aqueduct_core::{Decider, Entry, Env, Event, RunState};

/// Where a journal and a decider part ways.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ReplayError {
    /// The journal is not a valid journal.
    #[error("journal entry {position}: {error}")]
    Invalid {
        /// The entry, from 0.
        position: usize,
        /// What is wrong with it.
        error: String,
    },
    /// The decider diverges before journal entry `position` (or, if it is the journal's length, at
    /// its end: where the run waits now).
    #[error("diverges at journal entry {position}: {reason}")]
    Diverged {
        /// The entry, from 0.
        position: usize,
        /// What diverged.
        reason: String,
    },
}

/// Whether the logic wrote this entry, in response to a decision.
pub fn decided(entry: &Entry) -> bool {
    matches!(
        entry,
        Entry::StepScheduled { .. }
            | Entry::AgentCallScheduled { .. }
            | Entry::AgentCallEscalated { .. }
            | Entry::TimerStarted { .. }
            | Entry::SignalAwaited { .. }
            | Entry::ValueRecorded { .. }
            | Entry::Escalated { .. }
            | Entry::ChildStarted { .. }
            | Entry::UpdateAwaited { .. }
            | Entry::MessageSent { .. }
            | Entry::UpdateCompleted { .. }
            | Entry::UpdateRejected { .. }
            | Entry::RunCompleted { .. }
            | Entry::RunFailed { .. }
            | Entry::ContinuedAsNew { .. }
    )
}

/// Replays the journal of run `key` against `decider`. Returns how many decisions were checked.
pub fn check(key: &str, journal: &[Entry], decider: &dyn Decider) -> Result<usize, ReplayError> {
    let invalid = |position: usize, error: String| ReplayError::Invalid { position, error };
    let first = journal.get(..1).unwrap_or_default();
    let mut state = RunState::from_journal(first).map_err(|e| invalid(0, e.to_string()))?;
    let mut checked = 0;
    let mut ask = |state: &RunState, position: usize| -> Result<(), ReplayError> {
        checked += 1;
        let commands = decider.decide(&state.view_at(key, Env::default()));
        match state.handle(Event::Decision(commands)).entries.first() {
            // A decider that parks on purpose did so in the journal too, which then holds the same
            // entry: not a divergence of this decider.
            Some(Entry::Parked { reason })
                if journal.get(position)
                    != Some(&Entry::Parked {
                        reason: reason.clone(),
                    }) =>
            {
                Err(ReplayError::Diverged {
                    position,
                    reason: reason.clone(),
                })
            }
            _ => Ok(()),
        }
    };
    for (position, entry) in journal.iter().enumerate().skip(1) {
        if decided(entry) {
            ask(&state, position)?;
        }
        state
            .apply(entry)
            .map_err(|e| invalid(position, e.to_string()))?;
    }
    if !state.status().is_terminal() {
        ask(&state, journal.len())?;
    }
    Ok(checked)
}

#[cfg(test)]
mod tests {
    use aqueduct_core::{Command, CommandIndex, Payload, RunView, StepOutcome};

    use super::*;

    /// Runs steps named by its input's characters, one after another.
    struct Steps(&'static str);

    impl Decider for Steps {
        fn decide(&self, view: &RunView<'_>) -> Vec<Command> {
            let mut commands = Vec::new();
            for (i, name) in self.0.chars().enumerate() {
                commands.push(Command::ScheduleStep {
                    name: name.to_string(),
                    input: Payload::default(),
                });
                if view.outcome(CommandIndex(i as u32)).is_none() {
                    return commands;
                }
            }
            commands.push(Command::CompleteRun {
                output: Payload::new(*b"null"),
            });
            commands
        }
    }

    fn journal() -> Vec<Entry> {
        let mut journal = aqueduct_core::start("w", Payload::default()).entries;
        let mut state = RunState::from_journal(&journal).unwrap();
        for event in [None, Some(0), Some(1)] {
            let event = match event {
                None => Event::Decision(Steps("ab").decide(&state.view())),
                Some(i) => Event::StepResult {
                    cmd: CommandIndex(i),
                    outcome: StepOutcome::Completed(Payload::new(*b"1")),
                },
            };
            let mut work = vec![event];
            while let Some(event) = work.pop() {
                let transition = state.handle(event);
                for entry in &transition.entries {
                    state.apply(entry).unwrap();
                }
                journal.extend(transition.entries);
                if transition
                    .effects
                    .contains(&aqueduct_core::Effect::Activate)
                {
                    work.push(Event::Decision(Steps("ab").decide(&state.view())));
                }
            }
        }
        journal
    }

    #[test]
    fn the_code_that_wrote_a_journal_replays_it() {
        let journal = journal();
        assert_eq!(state_of(&journal), aqueduct_core::Status::Completed);
        assert!(check("k", &journal, &Steps("ab")).unwrap() >= 3);
    }

    #[test]
    fn other_code_is_reported_where_it_diverges() {
        let journal = journal();
        let error = check("k", &journal, &Steps("ac")).unwrap_err();
        assert!(
            matches!(&error, ReplayError::Diverged { reason, .. } if reason.contains("command 1 diverged")),
            "{error}"
        );
    }

    fn state_of(journal: &[Entry]) -> aqueduct_core::Status {
        RunState::from_journal(journal).unwrap().status()
    }
}
