//! Agent calls: bounded model calls whose responses are journaled.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use aqueduct_core::{
    AgentResponse, Command, CommandIndex, Effect, Entry, Event, Payload, RunState, StepOutcome,
    cancelled_error, start,
};

fn call(budget_tokens: u64) -> Command {
    Command::CallAgent {
        name: "classify".into(),
        request: Payload::new(*b"{\"input\":\"x\"}"),
        budget_tokens,
    }
}

fn answer(tokens: (u64, u64)) -> AgentResponse {
    AgentResponse {
        model: "test/model@1".into(),
        params: Payload::new(*b"{\"temperature\":0}"),
        response: Payload::new(*b"\"invoice\""),
        input_tokens: tokens.0,
        output_tokens: tokens.1,
    }
}

struct Run {
    state: RunState,
    journal: Vec<Entry>,
}

impl Run {
    fn new() -> Run {
        let journal = start("w", Payload::default()).entries;
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
}

fn result(outcome: Result<AgentResponse, String>) -> Event {
    Event::AgentResult {
        cmd: CommandIndex(0),
        outcome,
    }
}

#[test]
fn an_agent_call_is_dispatched_and_its_answer_recorded_once_with_its_cost() {
    let mut run = Run::new();
    let effects = run.handle(Event::Decision(vec![call(1_000)]));
    assert!(matches!(
        &effects[..],
        [Effect::DispatchAgent { name, budget_tokens: 1_000, .. }] if name == "classify"
    ));
    assert_eq!(
        run.handle(result(Ok(answer((300, 50))))),
        vec![Effect::Activate]
    );
    assert!(matches!(
        run.journal.last(),
        Some(Entry::AgentCallCompleted { response, .. }) if response.model == "test/model@1"
    ));
    assert_eq!(
        run.state.view().outcome(CommandIndex(0)),
        Some(&StepOutcome::Completed(Payload::new(*b"\"invoice\"")))
    );
    assert_eq!(run.state.tokens_used(), 350);
    // A second answer (a repeated call) is dropped; replay issues nothing new.
    assert!(run.handle(result(Ok(answer((1, 1))))).is_empty());
    assert!(run.handle(Event::Decision(vec![call(1_000)])).is_empty());
    assert!(
        run.state.pending_steps().is_empty(),
        "never dispatched again"
    );
}

#[test]
fn an_answer_over_the_budget_fails_the_call() {
    let mut run = Run::new();
    run.handle(Event::Decision(vec![call(100)]));
    run.handle(result(Ok(answer((90, 20)))));
    let Some(StepOutcome::Failed(error)) = run.state.view().outcome(CommandIndex(0)) else {
        panic!("over budget must fail: {:?}", run.journal.last());
    };
    assert!(
        error.contains("110 tokens, over its budget of 100"),
        "{error}"
    );
    assert_eq!(
        run.state.tokens_used(),
        0,
        "a failed call's cost is not counted as an answer"
    );
}

#[test]
fn agent_calls_are_retried_like_steps_and_recovered_after_a_restart() {
    let mut run = Run::new();
    run.handle(Event::Decision(vec![call(1_000)]));
    // After a restart, a call without an answer is dispatched again.
    assert!(matches!(
        &run.state.recovery_effects()[..],
        [Effect::Activate, Effect::DispatchAgent { .. }]
    ));
    run.handle(Event::StepAttemptFailed {
        cmd: CommandIndex(0),
        error: "rate limited".into(),
        retry_after_ms: 500,
    });
    assert_eq!(run.state.timers(), vec![(CommandIndex(0), 1_500)]);
    let effects = run.handle(Event::TimerFired {
        cmd: CommandIndex(0),
    });
    assert!(matches!(&effects[..], [Effect::DispatchAgent { .. }]));
    run.handle(result(Err("the model refused".into())));
    assert_eq!(
        run.state.view().outcome(CommandIndex(0)),
        Some(&StepOutcome::Failed("the model refused".into()))
    );
}

#[test]
fn a_changed_budget_parks_and_cancellation_closes_a_pending_call() {
    let mut run = Run::new();
    run.handle(Event::Decision(vec![call(1_000)]));
    let parked = run.state.handle(Event::Decision(vec![call(2_000)]));
    assert!(matches!(parked.entries[..], [Entry::Parked { .. }]));
    run.handle(Event::Cancel {
        reason: "stop".into(),
    });
    assert_eq!(
        run.state.view().outcome(CommandIndex(0)),
        Some(&StepOutcome::Failed(cancelled_error("stop")))
    );
    assert!(run.handle(result(Ok(answer((1, 1))))).is_empty());
}

mod budgets {
    use super::*;
    use aqueduct_core::BUDGET_ESCALATION;

    fn budgeted(tokens: u64) -> Run {
        let journal =
            aqueduct_core::start_with_budget("w", Payload::default(), Some(tokens)).entries;
        Run {
            state: RunState::from_journal(&journal).unwrap(),
            journal,
        }
    }

    fn named(name: &str, budget_tokens: u64) -> Command {
        Command::CallAgent {
            name: name.into(),
            request: Payload::new(*b"{}"),
            budget_tokens,
        }
    }

    fn dispatched(effects: &[Effect]) -> Vec<CommandIndex> {
        effects
            .iter()
            .filter_map(|e| match e {
                Effect::DispatchAgent { cmd, .. } => Some(*cmd),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn calls_that_fit_are_made_and_reserve_their_budget() {
        let mut run = budgeted(1_000);
        // Two concurrent calls of 400 fit; a third does not, because the first two are reserved.
        let effects = run.handle(Event::Decision(vec![
            named("a", 400),
            named("b", 400),
            named("c", 400),
        ]));
        assert_eq!(dispatched(&effects), [CommandIndex(0), CommandIndex(1)]);
        assert!(matches!(
            run.journal.last(),
            Some(Entry::AgentCallEscalated {
                cmd: CommandIndex(2),
                budget_tokens: 400,
                available: 200,
                ..
            })
        ));
        let open = run.state.open_escalations();
        assert_eq!(open.len(), 1);
        assert_eq!((open[0].0, open[0].1), (CommandIndex(2), BUDGET_ESCALATION));
        assert_eq!(open[0].2.as_bytes(), b"{\"needed\":400,\"available\":200}");
        assert!(
            run.state.pending_steps().len() == 2,
            "the waiting call is not re-dispatched"
        );
    }

    #[test]
    fn approval_raises_the_budget_just_enough_and_makes_the_call() {
        let mut run = budgeted(300);
        run.handle(Event::Decision(vec![named("a", 400)]));
        let effects = run.handle(Event::ResolveEscalation {
            cmd: CommandIndex(0),
            outcome: Payload::new(*b"true"),
        });
        assert_eq!(dispatched(&effects), [CommandIndex(0)]);
        assert_eq!(run.state.budget(), Some(400));
        assert!(run.state.open_escalations().is_empty());
        // Answers spend from the budget; the next call that does not fit waits again.
        run.handle(Event::AgentResult {
            cmd: CommandIndex(0),
            outcome: Ok(answer((150, 50))),
        });
        assert_eq!(run.state.tokens_used(), 200);
        let effects = run.handle(Event::Decision(vec![named("a", 400), named("b", 300)]));
        assert!(dispatched(&effects).is_empty());
        assert!(matches!(
            run.journal.last(),
            Some(Entry::AgentCallEscalated {
                budget_tokens: 300,
                available: 200,
                ..
            })
        ));
    }

    #[test]
    fn declining_fails_the_call() {
        let mut run = budgeted(100);
        run.handle(Event::Decision(vec![named("a", 400)]));
        run.handle(Event::ResolveEscalation {
            cmd: CommandIndex(0),
            outcome: Payload::new(*b"false"),
        });
        let Some(StepOutcome::Failed(error)) = run.state.view().outcome(CommandIndex(0)) else {
            panic!("declined budget must fail the call");
        };
        assert!(error.contains("not approved"), "{error}");
        assert_eq!(run.state.budget(), Some(100), "the budget is unchanged");
    }

    #[test]
    fn without_a_budget_every_call_is_made() {
        let mut run = Run::new();
        let effects = run.handle(Event::Decision(vec![named("a", 1_000_000)]));
        assert_eq!(dispatched(&effects), [CommandIndex(0)]);
    }
}

mod answers {
    use super::*;
    use aqueduct_core::SCHEMA_ESCALATION;

    #[test]
    fn a_rejected_answer_holds_the_call_until_someone_supplies_one() {
        let mut run = Run::new();
        run.handle(Event::Decision(vec![call(1_000)]));
        // The engine rejected the answers (schema) and spent the retries.
        let effects = run.handle(Event::AgentAnswerRejected {
            cmd: CommandIndex(0),
            error:
                "at '': \"poem\" is not one of [\"invoice\"]\nwith a \"quote\" and a \\ backslash"
                    .into(),
            answer: Payload::new(b"\"poem\"\t\x01".to_vec()),
        });
        assert!(effects.is_empty(), "nothing is dispatched or decided");
        let open = run.state.open_escalations();
        assert_eq!((open[0].0, open[0].1), (CommandIndex(0), SCHEMA_ESCALATION));
        // The payload is valid JSON however odd the answer and the error are.
        let payload: serde_json::Value = serde_json::from_slice(open[0].2.as_bytes()).unwrap();
        assert_eq!(payload["answer"], "\"poem\"\t\u{1}");
        assert!(
            payload["error"]
                .as_str()
                .unwrap()
                .contains("\"quote\" and a \\ backslash")
        );
        assert!(
            run.state.pending_steps().is_empty(),
            "held: not dispatched again"
        );
        let effects = run.handle(Event::ResolveEscalation {
            cmd: CommandIndex(0),
            outcome: Payload::new(*b"\"invoice\""),
        });
        assert_eq!(effects, vec![Effect::Activate]);
        assert_eq!(
            run.state.view().outcome(CommandIndex(0)),
            Some(&StepOutcome::Completed(Payload::new(*b"\"invoice\"")))
        );
        assert!(run.state.open_escalations().is_empty());
    }

    #[test]
    fn a_late_valid_answer_completes_a_held_call() {
        // An answer of an earlier attempt, checked by the engine, may still arrive: it is the answer.
        let mut run = Run::new();
        run.handle(Event::Decision(vec![call(1_000)]));
        run.handle(Event::AgentAnswerRejected {
            cmd: CommandIndex(0),
            error: "off schema".into(),
            answer: Payload::new(*b"1"),
        });
        assert_eq!(
            run.handle(result(Ok(answer((1, 1))))),
            vec![Effect::Activate]
        );
        assert!(run.state.open_escalations().is_empty());
        assert!(run.state.steps()[0].held.is_none());
    }
}
