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
