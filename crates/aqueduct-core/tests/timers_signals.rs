//! Timers and signals in the run state machine.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use aqueduct_core::{
    Command, CommandIndex, Decider, Effect, Entry, Event, Payload, RunState, RunView, Status,
    StepOutcome, TIMED_OUT, start,
};
use proptest::prelude::*;

/// Sleeps 100 ms, then waits up to 1 s for `approve`; completes with the signal or with "timeout".
struct Approval;

impl Decider for Approval {
    fn decide(&self, view: &RunView<'_>) -> Vec<Command> {
        let mut commands = vec![Command::Sleep { duration_ms: 100 }];
        if view.outcome(CommandIndex(0)).is_none() {
            return commands;
        }
        commands.push(Command::AwaitSignal {
            name: "approve".into(),
            timeout_ms: 1_000,
        });
        match view.outcome(CommandIndex(1)) {
            None => {}
            Some(StepOutcome::Completed(payload)) => commands.push(Command::CompleteRun {
                output: payload.clone(),
            }),
            Some(StepOutcome::Failed(error)) => {
                assert_eq!(error, TIMED_OUT);
                commands.push(Command::CompleteRun {
                    output: Payload::new(*b"\"timeout\""),
                });
            }
        }
        commands
    }
}

struct Run {
    state: RunState,
    journal: Vec<Entry>,
}

impl Run {
    fn new() -> Run {
        let journal = start("approval", Payload::new(*b"{}")).entries;
        Run {
            state: RunState::from_journal(&journal).unwrap(),
            journal,
        }
    }

    fn handle(&mut self, event: Event, now: u64) -> Vec<Effect> {
        let transition = self.state.handle_at(event, now);
        for entry in &transition.entries {
            self.state.apply(entry).unwrap();
        }
        self.journal.extend(transition.entries);
        assert_eq!(
            self.state,
            RunState::from_journal(&self.journal).unwrap(),
            "state is the fold of the journal"
        );
        transition.effects
    }

    fn activate(&mut self, now: u64) {
        let commands = Approval.decide(&self.state.view());
        let effects = self.handle(Event::Decision(commands), now);
        if effects.contains(&Effect::Activate) {
            self.activate(now);
        }
    }

    fn fire_due(&mut self, now: u64) {
        for (cmd, wake_at) in self.state.timers() {
            if wake_at <= now
                && self
                    .handle(Event::TimerFired { cmd }, now)
                    .contains(&Effect::Activate)
            {
                self.activate(now);
            }
        }
    }

    fn signal(&mut self, payload: &str, now: u64) {
        let event = Event::Signal {
            name: "approve".into(),
            id: String::new(),
            payload: Payload::new(payload.as_bytes().to_vec()),
        };
        if self.handle(event, now).contains(&Effect::Activate) {
            self.activate(now);
        }
    }
}

#[test]
fn wake_times_come_from_the_clock_at_first_issue_and_are_replayed() {
    let mut run = Run::new();
    run.activate(5_000);
    assert_eq!(run.state.timers(), vec![(CommandIndex(0), 5_100)]);
    // A later activation with another clock reading issues the same command and journals nothing new.
    let commands = Approval.decide(&run.state.view());
    assert!(
        run.state
            .handle_at(Event::Decision(commands), 9_999)
            .entries
            .is_empty()
    );
    // Timers never fire early: the engine fires them at or after wake_at.
    run.fire_due(5_099);
    assert_eq!(run.state.timers().len(), 1);
    run.fire_due(5_100);
    assert_eq!(run.state.timers(), vec![(CommandIndex(1), 6_100)]);
}

#[test]
fn a_signal_that_arrives_before_the_wait_is_kept_for_it() {
    let mut run = Run::new();
    run.activate(0);
    run.signal("\"yes\"", 50);
    assert_eq!(run.state.status(), Status::Running);
    run.fire_due(100);
    assert_eq!(run.state.status(), Status::Completed);
    assert_eq!(run.state.output(), Some(&Payload::new(*b"\"yes\"")));
}

#[test]
fn a_signal_completes_a_waiting_command() {
    let mut run = Run::new();
    run.activate(0);
    run.fire_due(100);
    run.signal("\"approved by ops\"", 500);
    assert_eq!(
        run.state.output(),
        Some(&Payload::new(*b"\"approved by ops\""))
    );
}

#[test]
fn a_wait_times_out_and_a_late_signal_changes_nothing() {
    let mut run = Run::new();
    run.activate(0);
    run.fire_due(100);
    run.fire_due(1_100);
    assert_eq!(run.state.output(), Some(&Payload::new(*b"\"timeout\"")));
    let before = run.journal.len();
    run.signal("\"too late\"", 2_000);
    assert_eq!(
        run.journal.len(),
        before,
        "nothing is recorded after the end"
    );
}

#[test]
fn stray_timers_and_step_results_for_waits_are_ignored() {
    let mut run = Run::new();
    run.activate(0);
    assert!(
        run.state
            .handle(Event::TimerFired {
                cmd: CommandIndex(7)
            })
            .entries
            .is_empty()
    );
    run.fire_due(100);
    assert!(
        run.state
            .handle(Event::TimerFired {
                cmd: CommandIndex(0)
            })
            .entries
            .is_empty(),
        "fired once"
    );
    let result = Event::StepResult {
        cmd: CommandIndex(1),
        outcome: StepOutcome::Completed(Payload::default()),
    };
    assert!(
        run.state.handle(result).entries.is_empty(),
        "a signal wait is not completed by a step result"
    );
}

#[test]
fn changing_a_sleep_parks_the_run() {
    struct Longer;
    impl Decider for Longer {
        fn decide(&self, _: &RunView<'_>) -> Vec<Command> {
            vec![Command::Sleep { duration_ms: 200 }]
        }
    }
    let mut run = Run::new();
    run.activate(0);
    let transition = run
        .state
        .handle(Event::Decision(Longer.decide(&run.state.view())));
    assert!(matches!(transition.entries[..], [Entry::Parked { .. }]));
}

#[derive(Clone, Debug)]
enum Op {
    Signal(u8),
    Advance(u16),
    Activate,
}

proptest! {
    /// Any interleaving of signals, clock advances and activations keeps the journal valid, ends the
    /// run, and the run's output is either the first signal that reached the wait, or the timeout.
    #[test]
    fn any_interleaving_is_consistent(ops in prop::collection::vec(prop_oneof![
        any::<u8>().prop_map(Op::Signal),
        (0u16..400).prop_map(Op::Advance),
        Just(Op::Activate),
    ], 0..40)) {
        let mut run = Run::new();
        let mut now = 0u64;
        let mut signals = Vec::new();
        run.activate(now);
        for op in ops {
            match op {
                Op::Signal(n) => {
                    let payload = format!("{n}");
                    if run.state.status() == Status::Running {
                        signals.push(payload.clone());
                    }
                    run.signal(&payload, now);
                }
                Op::Advance(ms) => {
                    now += u64::from(ms);
                    run.fire_due(now);
                }
                Op::Activate => run.activate(now),
            }
        }
        // Let time pass until every timer has fired (the sleep, then the wait's timeout).
        for _ in 0..3 {
            now += 2_000;
            run.fire_due(now);
        }
        prop_assert_eq!(run.state.status(), Status::Completed);
        let output = String::from_utf8(run.state.output().unwrap().as_bytes().to_vec()).unwrap();
        prop_assert!(output == "\"timeout\"" || signals.first() == Some(&output), "output {output}, signals {signals:?}");
    }
}

#[test]
fn a_signal_with_a_known_id_is_recorded_once() {
    let mut run = Run::new();
    run.activate(0);
    let send = |run: &mut Run, payload: &str| {
        let event = Event::Signal {
            name: "approve".into(),
            id: "s-1".into(),
            payload: Payload::new(payload.as_bytes().to_vec()),
        };
        run.handle(event, 10)
    };
    send(&mut run, "\"first\"");
    let before = run.journal.len();
    send(&mut run, "\"retry\"");
    assert_eq!(run.journal.len(), before, "the retried send is dropped");
    run.fire_due(100);
    assert_eq!(run.state.output(), Some(&Payload::new(*b"\"first\"")));
}
