//! Property tests for the run state machine.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use aqueduct_core::{
    Command, CommandIndex, Decider, Effect, Entry, Event, Payload, RunState, RunView, Status,
    StepOutcome, start,
};
use proptest::prelude::*;

/// Two concurrent steps, then `sequential` steps one after another, then complete with all results.
/// Any failed step fails the run.
struct Pipeline {
    sequential: u32,
}

impl Decider for Pipeline {
    fn decide(&self, view: &RunView<'_>) -> Vec<Command> {
        let input = view.input().clone();
        let mut commands = vec![
            Command::ScheduleStep {
                name: "left".into(),
                input: input.clone(),
            },
            Command::ScheduleStep {
                name: "right".into(),
                input,
            },
        ];
        let mut results = Vec::new();
        for cmd in 0..2 {
            match view.outcome(CommandIndex(cmd)) {
                None => return commands,
                Some(StepOutcome::Failed(error)) => {
                    commands.push(Command::FailRun {
                        error: error.clone(),
                    });
                    return commands;
                }
                Some(StepOutcome::Completed(result)) => results.push(result.clone()),
            }
        }
        // Results are combined in command order, never in completion order.
        let mut carry = Payload::new([results[0].as_bytes(), results[1].as_bytes()].concat());
        for i in 0..self.sequential {
            commands.push(Command::ScheduleStep {
                name: format!("seq-{i}"),
                input: carry.clone(),
            });
            match view.outcome(CommandIndex(2 + i)) {
                None => return commands,
                Some(StepOutcome::Failed(error)) => {
                    commands.push(Command::FailRun {
                        error: error.clone(),
                    });
                    return commands;
                }
                Some(StepOutcome::Completed(result)) => carry = result.clone(),
            }
        }
        commands.push(Command::CompleteRun { output: carry });
        commands
    }
}

fn execute(name: &str, input: &Payload) -> Payload {
    let mut bytes = input.as_bytes().to_vec();
    bytes.extend_from_slice(name.as_bytes());
    Payload::new(bytes)
}

/// A run driven by hand: applies transitions, checks the fold invariant after every event.
struct Harness {
    journal: Vec<Entry>,
    state: RunState,
}

impl Harness {
    fn new(input: Payload) -> (Harness, Vec<Effect>) {
        let first = start("test", input);
        let state = RunState::from_journal(&first.entries).expect("valid start");
        (
            Harness {
                journal: first.entries,
                state,
            },
            first.effects,
        )
    }

    fn handle(&mut self, event: Event) -> Vec<Effect> {
        let before = self.journal.len();
        let terminal = self.state.status().is_terminal();
        let transition = self.state.handle(event);
        for entry in &transition.entries {
            self.state
                .apply(entry)
                .expect("the state machine only produces valid entries");
        }
        self.journal.extend(transition.entries);
        assert_eq!(
            self.state,
            RunState::from_journal(&self.journal).expect("journal stays valid"),
            "state must equal the fold of the journal"
        );
        if terminal {
            assert_eq!(
                self.journal.len(),
                before,
                "nothing is appended after the end"
            );
        }
        transition.effects
    }

    fn activate(&mut self, decider: &dyn Decider) -> Vec<Effect> {
        let commands = decider.decide(&self.state.view());
        self.handle(Event::Decision(commands))
    }
}

#[derive(Clone, Debug)]
enum Op {
    /// Deliver a result for the pending command at this position (modulo), possibly a duplicate.
    Deliver {
        pick: u8,
        fail: bool,
        duplicate_done: bool,
    },
    /// Activate the run (as after a restart or a redundant wake-up).
    Activate,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (any::<u8>(), prop::bool::weighted(0.05), any::<bool>())
            .prop_map(|(pick, fail, duplicate_done)| Op::Deliver { pick, fail, duplicate_done }),
        1 => Just(Op::Activate),
    ]
}

/// Runs a pipeline to the end, delivering results in an order chosen by `ops`, then drains the rest.
fn drive(decider: &Pipeline, input: Payload, ops: &[Op]) -> Harness {
    let (mut run, _) = Harness::new(input);
    run.activate(decider);
    for op in ops {
        match op {
            Op::Activate => {
                run.activate(decider);
            }
            Op::Deliver {
                pick,
                fail,
                duplicate_done,
            } => {
                let steps = run.state.steps();
                if steps.is_empty() {
                    continue;
                }
                let candidates: Vec<usize> = (0..steps.len())
                    .filter(|&i| *duplicate_done || steps[i].outcome.is_none())
                    .collect();
                if candidates.is_empty() {
                    continue;
                }
                let index = candidates[usize::from(*pick) % candidates.len()];
                let step = &steps[index];
                let outcome = if *fail {
                    StepOutcome::Failed(format!("{} failed", step.name))
                } else {
                    StepOutcome::Completed(execute(&step.name, &step.input))
                };
                let effects = run.handle(Event::StepResult {
                    cmd: CommandIndex(index as u32),
                    outcome,
                });
                if effects.contains(&Effect::Activate) {
                    run.activate(decider);
                }
            }
        }
    }
    // Drain: complete every pending step successfully until the run ends.
    while !run.state.status().is_terminal() {
        let pending = run.state.pending_steps();
        assert!(
            !pending.is_empty(),
            "a running run always has pending steps after activation"
        );
        for effect in pending {
            if let Effect::DispatchStep { cmd, name, input } = effect {
                let outcome = StepOutcome::Completed(execute(&name, &input));
                run.handle(Event::StepResult { cmd, outcome });
                run.activate(decider);
            }
        }
    }
    run
}

proptest! {
    #[test]
    fn journal_stays_valid_and_ends(
        sequential in 0u32..6,
        input in prop::collection::vec(any::<u8>(), 0..8),
        ops in prop::collection::vec(op(), 0..60),
    ) {
        let decider = Pipeline { sequential };
        let run = drive(&decider, Payload::new(input), &ops);
        prop_assert!(run.state.status().is_terminal());
        prop_assert_ne!(run.state.status(), Status::Parked);
    }

    #[test]
    fn completion_order_does_not_change_the_output(
        sequential in 0u32..6,
        input in prop::collection::vec(any::<u8>(), 0..8),
        a in prop::collection::vec(op(), 0..40),
        b in prop::collection::vec(op(), 0..40),
    ) {
        // Without failures, two different delivery orders must give the same output.
        let no_failures = |ops: Vec<Op>| -> Vec<Op> {
            ops.into_iter().map(|op| match op {
                Op::Deliver { pick, duplicate_done, .. } => Op::Deliver { pick, fail: false, duplicate_done },
                other => other,
            }).collect()
        };
        let decider = Pipeline { sequential };
        let first = drive(&decider, Payload::new(input.clone()), &no_failures(a));
        let second = drive(&decider, Payload::new(input), &no_failures(b));
        prop_assert_eq!(first.state.status(), Status::Completed);
        prop_assert_eq!(first.state.output(), second.state.output());
    }

    #[test]
    fn recovery_from_any_prefix_matches(
        sequential in 0u32..4,
        input in prop::collection::vec(any::<u8>(), 0..8),
        ops in prop::collection::vec(op(), 0..40),
        cut in any::<prop::sample::Index>(),
    ) {
        // Any prefix of a valid journal is a valid journal, and recovery from it finishes the run
        // with the same output as an uninterrupted run (when no step fails).
        let decider = Pipeline { sequential };
        let ops: Vec<Op> = ops.into_iter().map(|op| match op {
            Op::Deliver { pick, duplicate_done, .. } => Op::Deliver { pick, fail: false, duplicate_done },
            other => other,
        }).collect();
        let full = drive(&decider, Payload::new(input), &ops);
        let prefix = &full.journal[..=cut.index(full.journal.len())];
        let state = RunState::from_journal(prefix).expect("a prefix of a valid journal is valid");
        let mut run = Harness { journal: prefix.to_vec(), state };
        for effect in run.state.recovery_effects() {
            if effect == Effect::Activate {
                run.activate(&decider);
            }
        }
        while !run.state.status().is_terminal() {
            for effect in run.state.pending_steps() {
                if let Effect::DispatchStep { cmd, name, input } = effect {
                    run.handle(Event::StepResult { cmd, outcome: StepOutcome::Completed(execute(&name, &input)) });
                    run.activate(&decider);
                }
            }
        }
        prop_assert_eq!(run.state.output(), full.state.output());
    }
}

#[test]
fn diverging_logic_parks_the_run() {
    let (mut run, _) = Harness::new(Payload::new(*b"x"));
    run.activate(&Pipeline { sequential: 1 });

    struct Renamed;
    impl Decider for Renamed {
        fn decide(&self, view: &RunView<'_>) -> Vec<Command> {
            vec![Command::ScheduleStep {
                name: "renamed".into(),
                input: view.input().clone(),
            }]
        }
    }
    let effects = run.activate(&Renamed);
    assert_eq!(effects, vec![Effect::Finished(Status::Parked)]);
    assert_eq!(run.state.status(), Status::Parked);
    assert!(run.state.error().is_some_and(|e| e.contains("diverged")));
}

#[test]
fn fewer_commands_than_journaled_parks_the_run() {
    let (mut run, _) = Harness::new(Payload::new(*b"x"));
    run.activate(&Pipeline { sequential: 1 });

    struct Forgetful;
    impl Decider for Forgetful {
        fn decide(&self, view: &RunView<'_>) -> Vec<Command> {
            vec![Command::ScheduleStep {
                name: "left".into(),
                input: view.input().clone(),
            }]
        }
    }
    run.activate(&Forgetful);
    assert_eq!(run.state.status(), Status::Parked);
}

#[test]
fn duplicate_results_are_recorded_once() {
    let (mut run, _) = Harness::new(Payload::new(*b"x"));
    run.activate(&Pipeline { sequential: 0 });
    let result = StepOutcome::Completed(Payload::new(*b"first"));
    run.handle(Event::StepResult {
        cmd: CommandIndex(0),
        outcome: result,
    });
    let effects = run.handle(Event::StepResult {
        cmd: CommandIndex(0),
        outcome: StepOutcome::Completed(Payload::new(*b"second")),
    });
    assert!(effects.is_empty());
    assert_eq!(
        run.state.steps()[0].outcome,
        Some(StepOutcome::Completed(Payload::new(*b"first")))
    );
}

#[test]
fn invalid_journals_are_rejected() {
    use aqueduct_core::JournalError;
    let started = Entry::Started {
        workflow: "test".into(),
        input: Payload::default(),
        budget_tokens: None,
    };
    let scheduled = |cmd| Entry::StepScheduled {
        cmd: CommandIndex(cmd),
        name: "s".into(),
        input: Payload::default(),
    };
    let completed = |cmd| Entry::StepCompleted {
        cmd: CommandIndex(cmd),
        result: Payload::default(),
    };

    assert_eq!(RunState::from_journal(&[]), Err(JournalError::Empty));
    assert_eq!(
        RunState::from_journal(&[scheduled(0)]),
        Err(JournalError::NotStarted)
    );
    assert_eq!(
        RunState::from_journal(&[started.clone(), started.clone()]),
        Err(JournalError::DuplicateStart)
    );
    assert!(matches!(
        RunState::from_journal(&[started.clone(), scheduled(1)]),
        Err(JournalError::CommandOutOfOrder { .. })
    ));
    assert_eq!(
        RunState::from_journal(&[started.clone(), completed(0)]),
        Err(JournalError::UnknownCommand(CommandIndex(0)))
    );
    assert_eq!(
        RunState::from_journal(&[started.clone(), scheduled(0), completed(0), completed(0)]),
        Err(JournalError::DuplicateCompletion(CommandIndex(0)))
    );
    assert_eq!(
        RunState::from_journal(&[
            started,
            Entry::RunFailed { error: "e".into() },
            scheduled(0)
        ]),
        Err(JournalError::EntryAfterEnd)
    );
}
