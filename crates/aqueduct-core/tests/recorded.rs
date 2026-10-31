//! Recorded values: what a run observed from outside its logic.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use aqueduct_core::{
    Command, CommandIndex, Effect, Entry, Event, Payload, RunState, Status, StepOutcome, start,
};

fn record(name: &str, value: &str) -> Command {
    Command::Record {
        name: name.into(),
        value: Payload::new(value.as_bytes().to_vec()),
    }
}

fn step(name: &str) -> Command {
    Command::ScheduleStep {
        name: name.into(),
        input: Payload::default(),
    }
}

fn run() -> (RunState, Vec<Entry>) {
    let journal = start("w", Payload::new(*b"{}")).entries;
    (RunState::from_journal(&journal).unwrap(), journal)
}

fn apply(state: &mut RunState, journal: &mut Vec<Entry>, entries: Vec<Entry>) {
    for entry in entries {
        state.apply(&entry).unwrap();
        journal.push(entry);
    }
    assert_eq!(*state, RunState::from_journal(journal).unwrap());
}

#[test]
fn a_new_value_is_journaled_and_is_its_command_outcome_at_once() {
    let (mut state, mut journal) = run();
    let transition = state.handle(Event::Decision(vec![record("now", "1000"), step("a")]));
    assert_eq!(
        transition.entries[0],
        Entry::ValueRecorded {
            cmd: CommandIndex(0),
            name: "now".into(),
            value: Payload::new(*b"1000"),
        }
    );
    assert!(transition.effects.contains(&Effect::Activate));
    apply(&mut state, &mut journal, transition.entries);
    assert_eq!(
        state.view().outcome(CommandIndex(0)),
        Some(&StepOutcome::Completed(Payload::new(*b"1000")))
    );
    assert!(state.timers().is_empty());
    assert_eq!(
        state.pending_steps().len(),
        1,
        "only the step is dispatched"
    );
}

#[test]
fn replay_keeps_the_journaled_value_whatever_the_logic_observes_now() {
    let (mut state, mut journal) = run();
    let first = state.handle(Event::Decision(vec![record("now", "1000")]));
    apply(&mut state, &mut journal, first.entries);
    let replay = state.handle(Event::Decision(vec![record("now", "9999")]));
    assert!(replay.entries.is_empty(), "nothing new: {replay:?}");
    assert_eq!(
        state.view().outcome(CommandIndex(0)),
        Some(&StepOutcome::Completed(Payload::new(*b"1000")))
    );
}

#[test]
fn a_value_under_another_name_or_in_place_of_a_step_parks() {
    let (mut state, mut journal) = run();
    let first = state.handle(Event::Decision(vec![record("now", "1")]));
    apply(&mut state, &mut journal, first.entries);
    for commands in [vec![record("uuid", "1")], vec![step("now")]] {
        let transition = state.handle(Event::Decision(commands));
        assert!(
            matches!(transition.entries[..], [Entry::Parked { .. }]),
            "{transition:?}"
        );
    }
}

#[test]
fn step_results_and_timers_for_a_recorded_value_are_ignored() {
    let (mut state, mut journal) = run();
    let first = state.handle(Event::Decision(vec![record("random", "7")]));
    apply(&mut state, &mut journal, first.entries);
    let result = Event::StepResult {
        cmd: CommandIndex(0),
        outcome: StepOutcome::Completed(Payload::default()),
    };
    assert!(state.handle(result).entries.is_empty());
    assert!(
        state
            .handle(Event::TimerFired {
                cmd: CommandIndex(0)
            })
            .entries
            .is_empty()
    );
    let done = state.handle(Event::Decision(vec![
        record("random", "8"),
        Command::CompleteRun {
            output: Payload::new(*b"7"),
        },
    ]));
    apply(&mut state, &mut journal, done.entries);
    assert_eq!(state.status(), Status::Completed);
}

#[test]
fn a_journal_completing_a_recorded_value_again_is_rejected() {
    let (_, mut journal) = run();
    journal.push(Entry::ValueRecorded {
        cmd: CommandIndex(0),
        name: "now".into(),
        value: Payload::new(*b"1"),
    });
    journal.push(Entry::StepCompleted {
        cmd: CommandIndex(0),
        result: Payload::default(),
    });
    assert!(RunState::from_journal(&journal).is_err());
}
