//! Cancellation: what a run waits for fails as cancelled, and the run decides what to do next.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use aqueduct_core::{
    Command, CommandIndex, Effect, Entry, Event, JournalError, Payload, RunState, StepOutcome,
    cancelled_error, start,
};

struct Run {
    state: RunState,
    journal: Vec<Entry>,
}

impl Run {
    fn new() -> Run {
        let journal = start("w", Payload::new(*b"{}")).entries;
        Run {
            state: RunState::from_journal(&journal).unwrap(),
            journal,
        }
    }

    fn handle(&mut self, event: Event) -> Vec<Effect> {
        let transition = self.state.handle_at(event, 1_000);
        for entry in &transition.entries {
            self.state.apply(entry).unwrap();
        }
        self.journal.extend(transition.entries);
        assert_eq!(self.state, RunState::from_journal(&self.journal).unwrap());
        transition.effects
    }

    fn cancel(&mut self, reason: &str) -> Vec<Effect> {
        self.handle(Event::Cancel {
            reason: reason.into(),
        })
    }

    fn outcome(&self, cmd: u32) -> Option<&StepOutcome> {
        self.state.view().outcome(CommandIndex(cmd))
    }
}

fn step(name: &str) -> Command {
    Command::ScheduleStep {
        name: name.into(),
        input: Payload::default(),
    }
}

fn cancelled(reason: &str) -> StepOutcome {
    StepOutcome::Failed(cancelled_error(reason))
}

#[test]
fn everything_pending_fails_as_cancelled_and_late_results_are_dropped() {
    let mut run = Run::new();
    run.handle(Event::Decision(vec![
        step("a"),
        step("b"),
        Command::Sleep { duration_ms: 500 },
        Command::AwaitSignal {
            name: "go".into(),
            timeout_ms: 0,
        },
    ]));
    run.handle(Event::StepResult {
        cmd: CommandIndex(0),
        outcome: StepOutcome::Completed(Payload::new(*b"1")),
    });
    let effects = run.cancel("customer changed their mind");
    assert_eq!(effects, vec![Effect::Activate]);
    assert_eq!(
        run.outcome(0),
        Some(&StepOutcome::Completed(Payload::new(*b"1")))
    );
    for cmd in 1..4 {
        assert_eq!(
            run.outcome(cmd),
            Some(&cancelled("customer changed their mind"))
        );
        assert!(run.state.view().cancelled(CommandIndex(cmd)));
    }
    assert!(!run.state.view().cancelled(CommandIndex(0)));
    assert!(run.state.timers().is_empty());
    assert!(run.state.pending_steps().is_empty());
    // The step that was running finishes anyway; its result is not recorded.
    let late = run.handle(Event::StepResult {
        cmd: CommandIndex(1),
        outcome: StepOutcome::Completed(Payload::new(*b"2")),
    });
    assert!(late.is_empty());
    assert!(
        run.handle(Event::TimerFired {
            cmd: CommandIndex(2)
        })
        .is_empty()
    );
}

#[test]
fn only_the_first_cancellation_counts_and_the_run_goes_on() {
    let mut run = Run::new();
    run.handle(Event::Decision(vec![step("a")]));
    run.cancel("first");
    let before = run.journal.len();
    assert!(run.cancel("second").is_empty());
    assert_eq!(run.journal.len(), before);
    assert_eq!(run.state.cancel_reason(), Some("first"));
    // After the cancellation, new commands run normally: this is how a workflow undoes its work.
    let effects = run.handle(Event::Decision(vec![step("a"), step("undo_a")]));
    assert!(
        matches!(&effects[..], [Effect::DispatchStep { name, .. }] if name == "undo_a"),
        "{effects:?}"
    );
}

#[test]
fn a_retry_waiting_for_its_backoff_is_cancelled() {
    let mut run = Run::new();
    run.handle(Event::Decision(vec![step("a")]));
    run.handle(Event::StepAttemptFailed {
        cmd: CommandIndex(0),
        error: "busy".into(),
        retry_after_ms: 1_000,
    });
    assert_eq!(run.state.timers().len(), 1);
    run.cancel("stop");
    assert!(run.state.timers().is_empty());
    assert!(
        run.handle(Event::TimerFired {
            cmd: CommandIndex(0)
        })
        .is_empty()
    );
    assert_eq!(run.outcome(0), Some(&cancelled("stop")));
}

#[test]
fn a_cancellation_with_nothing_pending_is_taken_by_the_next_wait() {
    let mut run = Run::new();
    // Cancelled before the logic ever ran (for example, its workflow is not registered yet).
    run.cancel("too late");
    let effects = run.handle(Event::Decision(vec![
        Command::Record {
            name: "now".into(),
            value: Payload::new(*b"1"),
        },
        step("a"),
        step("b"),
    ]));
    // The recorded value is not a wait and keeps its value; `a` is cancelled and never dispatched; `b`
    // runs normally.
    assert_eq!(
        run.outcome(0),
        Some(&StepOutcome::Completed(Payload::new(*b"1")))
    );
    assert_eq!(run.outcome(1), Some(&cancelled("too late")));
    assert_eq!(run.outcome(2), None);
    let dispatched: Vec<&str> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::DispatchStep { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(dispatched, ["b"]);
    assert!(effects.contains(&Effect::Activate));
}

#[test]
fn a_cancelled_signal_wait_leaves_the_signal_for_the_next_wait() {
    let mut run = Run::new();
    run.cancel("stop");
    run.handle(Event::Signal {
        name: "go".into(),
        id: String::new(),
        payload: Payload::new(*b"\"x\""),
    });
    let wait = || Command::AwaitSignal {
        name: "go".into(),
        timeout_ms: 0,
    };
    run.handle(Event::Decision(vec![wait(), wait()]));
    assert_eq!(run.outcome(0), Some(&cancelled("stop")));
    assert_eq!(
        run.outcome(1),
        Some(&StepOutcome::Completed(Payload::new(*b"\"x\"")))
    );
}

#[test]
fn a_journal_cancelled_twice_is_rejected() {
    let mut journal = start("w", Payload::default()).entries;
    journal.push(Entry::CancelRequested { reason: "a".into() });
    journal.push(Entry::CancelRequested { reason: "b".into() });
    assert_eq!(
        RunState::from_journal(&journal),
        Err(JournalError::DuplicateCancel)
    );
}
