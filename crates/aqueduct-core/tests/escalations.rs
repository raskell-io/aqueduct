//! Escalations: a decision handed to a person or an agent, waited for durably.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use aqueduct_core::{
    Command, CommandIndex, Effect, Entry, Event, Payload, RunState, StepOutcome, cancelled_error,
    start,
};

fn escalate(reason: &str) -> Command {
    Command::Escalate {
        reason: reason.into(),
        payload: Payload::new(*b"{\"claim\":7}"),
    }
}

fn run() -> (RunState, Vec<Entry>) {
    let journal = start("w", Payload::default()).entries;
    (RunState::from_journal(&journal).unwrap(), journal)
}

fn handle(state: &mut RunState, journal: &mut Vec<Entry>, event: Event) -> Vec<Effect> {
    let transition = state.handle(event);
    for entry in &transition.entries {
        state.apply(entry).unwrap();
    }
    journal.extend(transition.entries);
    assert_eq!(*state, RunState::from_journal(journal).unwrap());
    transition.effects
}

fn resolve(cmd: u32, outcome: &str) -> Event {
    Event::ResolveEscalation {
        cmd: CommandIndex(cmd),
        outcome: Payload::new(outcome.as_bytes().to_vec()),
    }
}

#[test]
fn an_escalation_waits_until_it_is_resolved_once() {
    let (mut state, mut journal) = run();
    let effects = handle(
        &mut state,
        &mut journal,
        Event::Decision(vec![escalate("manual-review")]),
    );
    assert!(effects.is_empty(), "nothing is dispatched: {effects:?}");
    assert_eq!(
        journal.last(),
        Some(&Entry::Escalated {
            cmd: CommandIndex(0),
            reason: "manual-review".into(),
            payload: Payload::new(*b"{\"claim\":7}"),
        })
    );
    let open = state.open_escalations();
    assert_eq!(open.len(), 1);
    assert_eq!((open[0].0, open[0].1), (CommandIndex(0), "manual-review"));
    assert!(state.timers().is_empty() && state.pending_steps().is_empty());

    assert_eq!(
        handle(&mut state, &mut journal, resolve(0, "\"settle\"")),
        vec![Effect::Activate]
    );
    assert_eq!(
        state.view().outcome(CommandIndex(0)),
        Some(&StepOutcome::Completed(Payload::new(*b"\"settle\"")))
    );
    assert!(state.open_escalations().is_empty());
    // A second resolution, or one for a command that is not an escalation, is dropped.
    assert!(handle(&mut state, &mut journal, resolve(0, "\"decline\"")).is_empty());
    assert!(handle(&mut state, &mut journal, resolve(9, "1")).is_empty());
}

#[test]
fn a_resolution_for_a_step_is_dropped_and_a_step_result_does_not_resolve() {
    let (mut state, mut journal) = run();
    handle(
        &mut state,
        &mut journal,
        Event::Decision(vec![
            Command::ScheduleStep {
                name: "a".into(),
                input: Payload::default(),
            },
            escalate("why"),
        ]),
    );
    assert!(handle(&mut state, &mut journal, resolve(0, "1")).is_empty());
    let result = Event::StepResult {
        cmd: CommandIndex(1),
        outcome: StepOutcome::Completed(Payload::default()),
    };
    assert!(handle(&mut state, &mut journal, result).is_empty());
    assert_eq!(state.open_escalations().len(), 1);
}

#[test]
fn cancellation_closes_an_open_escalation() {
    let (mut state, mut journal) = run();
    handle(
        &mut state,
        &mut journal,
        Event::Decision(vec![escalate("why")]),
    );
    handle(
        &mut state,
        &mut journal,
        Event::Cancel {
            reason: "withdrawn".into(),
        },
    );
    assert!(state.open_escalations().is_empty());
    assert_eq!(
        state.view().outcome(CommandIndex(0)),
        Some(&StepOutcome::Failed(cancelled_error("withdrawn")))
    );
}

#[test]
fn a_changed_escalation_parks_the_run() {
    let (mut state, mut journal) = run();
    handle(
        &mut state,
        &mut journal,
        Event::Decision(vec![escalate("why")]),
    );
    let transition = state.handle(Event::Decision(vec![escalate("other reason")]));
    assert!(matches!(transition.entries[..], [Entry::Parked { .. }]));
}
