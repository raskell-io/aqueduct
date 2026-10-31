//! Journaled retries of steps.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use aqueduct_core::{
    Command, CommandIndex, Decider, Effect, Entry, Event, Payload, RunState, RunView, Status,
    StepOutcome, start,
};

struct One;

impl Decider for One {
    fn decide(&self, view: &RunView<'_>) -> Vec<Command> {
        let mut commands = vec![Command::ScheduleStep {
            name: "call".into(),
            input: view.input().clone(),
        }];
        match view.outcome(CommandIndex(0)) {
            None => {}
            Some(StepOutcome::Completed(result)) => commands.push(Command::CompleteRun {
                output: result.clone(),
            }),
            Some(StepOutcome::Failed(error)) => commands.push(Command::FailRun {
                error: error.clone(),
            }),
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
        let mut run = Run {
            state: RunState::from_journal(&start("one", Payload::new(*b"1")).entries).unwrap(),
            journal: start("one", Payload::new(*b"1")).entries,
        };
        run.handle(Event::Decision(One.decide(&run.state.view())), 0);
        run
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

    fn fail_attempt(&mut self, now: u64) -> Vec<Effect> {
        self.handle(
            Event::StepAttemptFailed {
                cmd: CommandIndex(0),
                error: "503".into(),
                retry_after_ms: 100,
            },
            now,
        )
    }
}

#[test]
fn a_failed_attempt_schedules_a_durable_retry() {
    let mut run = Run::new();
    assert!(
        run.fail_attempt(1_000).is_empty(),
        "nothing is dispatched until the retry is due"
    );
    assert_eq!(run.state.timers(), vec![(CommandIndex(0), 1_100)]);
    assert!(
        run.state.pending_steps().is_empty(),
        "redispatch leaves a step waiting to retry alone"
    );
    assert!(matches!(
        run.journal.last(),
        Some(Entry::StepRetryScheduled { attempt: 1, .. })
    ));

    // A duplicate report of the same failure changes nothing.
    assert!(run.fail_attempt(1_010).is_empty());
    assert_eq!(run.state.steps()[0].failed_attempts, 1);

    // When the retry is due, the step is dispatched again and has no outcome yet.
    let effects = run.handle(
        Event::TimerFired {
            cmd: CommandIndex(0),
        },
        1_100,
    );
    assert!(matches!(
        effects[..],
        [Effect::DispatchStep {
            cmd: CommandIndex(0),
            ..
        }]
    ));
    assert!(run.state.steps()[0].outcome.is_none());

    // The second attempt fails too, then the third succeeds.
    run.fail_attempt(1_200);
    assert!(matches!(
        run.journal.last(),
        Some(Entry::StepRetryScheduled { attempt: 2, .. })
    ));
    run.handle(
        Event::TimerFired {
            cmd: CommandIndex(0),
        },
        1_300,
    );
    let effects = run.handle(
        Event::StepResult {
            cmd: CommandIndex(0),
            outcome: StepOutcome::Completed(Payload::new(*b"ok")),
        },
        1_400,
    );
    assert_eq!(effects, vec![Effect::Activate]);
    run.handle(Event::Decision(One.decide(&run.state.view())), 1_400);
    assert_eq!(run.state.status(), Status::Completed);
}

#[test]
fn a_late_result_completes_a_step_that_is_waiting_to_retry() {
    let mut run = Run::new();
    run.fail_attempt(0);
    run.handle(
        Event::StepResult {
            cmd: CommandIndex(0),
            outcome: StepOutcome::Completed(Payload::new(*b"late")),
        },
        50,
    );
    assert!(
        run.state.timers().is_empty(),
        "the pending retry is no longer needed"
    );
    assert!(
        run.handle(
            Event::TimerFired {
                cmd: CommandIndex(0)
            },
            100
        )
        .is_empty()
    );
}

#[test]
fn invalid_retry_entries_are_rejected() {
    let journal = |entries: Vec<Entry>| {
        let mut all = start("one", Payload::default()).entries;
        all.push(Entry::StepScheduled {
            cmd: CommandIndex(0),
            name: "call".into(),
            input: Payload::default(),
        });
        all.extend(entries);
        RunState::from_journal(&all)
    };
    let retry = |attempt| Entry::StepRetryScheduled {
        cmd: CommandIndex(0),
        attempt,
        error: "e".into(),
        wake_at: 5,
    };
    assert!(journal(vec![retry(1)]).is_ok());
    assert!(
        journal(vec![retry(2)]).is_err(),
        "attempts are numbered from 1"
    );
    assert!(
        journal(vec![retry(1), retry(2)]).is_err(),
        "a retry is already pending"
    );
    assert!(
        journal(vec![
            retry(1),
            Entry::TimerFired {
                cmd: CommandIndex(0)
            },
            retry(2)
        ])
        .is_ok()
    );
    assert!(
        journal(vec![Entry::TimerFired {
            cmd: CommandIndex(0)
        }])
        .is_err(),
        "no retry is pending"
    );
}
