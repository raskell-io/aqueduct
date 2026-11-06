//! Priorities (ADR 0049): journaled, never seen by the logic, and only on a run that has not ended.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use aqueduct_core::{Entry, JournalError, Payload, RunState};

fn started() -> Entry {
    Entry::Started {
        workflow: "w".into(),
        input: Payload::new(*b"1"),
        budget_tokens: None,
        version: None,
    }
}

#[test]
fn the_last_priority_set_counts() {
    let state = RunState::from_journal(&[
        started(),
        Entry::PrioritySet { priority: 3 },
        Entry::PrioritySet { priority: 7 },
    ])
    .unwrap();
    assert_eq!(state.priority(), 7);
    assert_eq!(RunState::from_journal(&[started()]).unwrap().priority(), 0);
}

#[test]
fn setting_the_same_priority_journals_nothing() {
    let state = RunState::from_journal(&[started(), Entry::PrioritySet { priority: 3 }]).unwrap();
    assert!(state.set_priority(3).entries.is_empty());
    assert_eq!(
        state.set_priority(4).entries,
        [Entry::PrioritySet { priority: 4 }]
    );
    assert!(state.set_priority(4).effects.is_empty(), "it wakes nothing");
}

#[test]
fn a_run_that_ended_has_no_priority_to_set() {
    let ended = [
        started(),
        Entry::RunCompleted {
            output: Payload::new(*b"1"),
        },
    ];
    let state = RunState::from_journal(&ended).unwrap();
    assert!(state.set_priority(5).entries.is_empty());
    let mut journal = ended.to_vec();
    journal.push(Entry::PrioritySet { priority: 5 });
    assert_eq!(
        RunState::from_journal(&journal).unwrap_err(),
        JournalError::EntryAfterEnd
    );
    assert!(
        RunState::from_journal(&[Entry::PrioritySet { priority: 1 }]).is_err(),
        "not before the start"
    );
}

#[test]
fn a_priority_changes_no_command() {
    // The same commands, with and without priorities between them.
    let commands = [
        Entry::StepScheduled {
            cmd: aqueduct_core::CommandIndex(0),
            name: "s".into(),
            input: Payload::new(*b"1"),
        },
        Entry::StepCompleted {
            cmd: aqueduct_core::CommandIndex(0),
            result: Payload::new(*b"2"),
        },
    ];
    let plain = RunState::from_journal(&[&[started()][..], &commands[..]].concat()).unwrap();
    let with = RunState::from_journal(&[
        started(),
        Entry::PrioritySet { priority: 9 },
        commands[0].clone(),
        Entry::PrioritySet { priority: 1 },
        commands[1].clone(),
    ])
    .unwrap();
    assert_eq!(plain.steps(), with.steps());
    assert_eq!(plain.status(), with.status());
}
