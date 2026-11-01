//! Model-drift replay (BRIEF.md section 5.2, mode 4; ADR 0027): ask the agent calls of past runs again,
//! live, with a new model or prompt, and compare the answers with the recorded ones under each call's
//! output schema.
//!
//! Nothing is written: the runs are read, the executor is called, and a report comes back. The calls cost
//! real tokens, so a replay can be capped by calls and by tokens; a call that does not fit is skipped,
//! not made.

use std::time::Duration;

use aqueduct_core::{AgentResponse, CommandIndex, Entry, Payload};
use serde_json::Value;

use crate::agent::{AgentAttempt, AgentCall, AgentExecutor};
use crate::schema::{self, SchemaError};

/// What a run recorded for one of its agent calls.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Recorded {
    /// The model's answer.
    Answer(AgentResponse),
    /// The answers did not match the schema, and a person or an agent supplied this one.
    Supplied(Payload),
    /// The call failed.
    Failed(String),
    /// Nothing yet: the call is in flight, waiting for budget, or waiting for an answer.
    Pending,
}

impl Recorded {
    /// The answer the run continued with, if there is one.
    pub fn answer(&self) -> Option<&Payload> {
        match self {
            Recorded::Answer(response) => Some(&response.response),
            Recorded::Supplied(answer) => Some(answer),
            Recorded::Failed(_) | Recorded::Pending => None,
        }
    }
}

/// An agent call as a run recorded it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedCall {
    /// The run's run id.
    pub run: String,
    /// The call's command.
    pub cmd: CommandIndex,
    /// The call's name.
    pub name: String,
    /// The request (JSON: input, prompt, output schema).
    pub request: Payload,
    /// The call's token budget.
    pub budget_tokens: u64,
    /// What came of it.
    pub recorded: Recorded,
}

/// The agent calls in a run's journal, in the order they were issued.
pub fn agent_calls(run: &str, journal: &[Entry]) -> Vec<RecordedCall> {
    let mut calls: Vec<RecordedCall> = Vec::new();
    let find = |calls: &mut Vec<RecordedCall>, cmd: CommandIndex| {
        calls.iter_mut().position(|call| call.cmd == cmd)
    };
    for entry in journal {
        match entry {
            Entry::AgentCallScheduled {
                cmd,
                name,
                request,
                budget_tokens,
            }
            | Entry::AgentCallEscalated {
                cmd,
                name,
                request,
                budget_tokens,
                ..
            } => calls.push(RecordedCall {
                run: run.to_owned(),
                cmd: *cmd,
                name: name.clone(),
                request: request.clone(),
                budget_tokens: *budget_tokens,
                recorded: Recorded::Pending,
            }),
            Entry::AgentCallCompleted { cmd, response } => {
                if let Some(i) = find(&mut calls, *cmd) {
                    calls[i].recorded = Recorded::Answer(response.clone());
                }
            }
            Entry::StepFailed { cmd, error } => {
                if let Some(i) = find(&mut calls, *cmd) {
                    calls[i].recorded = Recorded::Failed(error.clone());
                }
            }
            // Only an answer escalation resolves an agent call's own command with an answer; a budget
            // escalation's resolution is recorded as `BudgetApproved`.
            Entry::EscalationResolved { cmd, outcome } => {
                if let Some(i) = find(&mut calls, *cmd) {
                    calls[i].recorded = Recorded::Supplied(outcome.clone());
                }
            }
            _ => {}
        }
    }
    calls
}

/// What a replayed call came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Replayed {
    /// The model answered. `schema_error` says why the answer does not match the call's output schema,
    /// if it does not.
    Answer {
        /// The answer, with the model that gave it and what it cost.
        response: AgentResponse,
        /// Why the answer does not match the output schema, if it does not.
        schema_error: Option<String>,
    },
    /// The call failed (also after its retries, or over its budget).
    Failed(String),
    /// The call was not made: it would have gone over the replay's limits.
    Skipped(String),
}

/// How a replayed call compares with the recorded one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Verdict {
    /// The same answer (as JSON) as the one the run continued with.
    Same,
    /// A different answer that matches the output schema.
    Changed,
    /// An answer, and the run recorded none to compare it with.
    New,
    /// An answer that does not match the output schema.
    Invalid,
    /// The call failed.
    Failed,
    /// The call was not made.
    Skipped,
}

impl Verdict {
    /// The verdict's name, as reports print it.
    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::Same => "same",
            Verdict::Changed => "changed",
            Verdict::New => "new",
            Verdict::Invalid => "invalid",
            Verdict::Failed => "failed",
            Verdict::Skipped => "skipped",
        }
    }
}

/// One call, recorded and replayed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriftCall {
    /// The call as the run recorded it.
    pub call: RecordedCall,
    /// The call asked again.
    pub replayed: Replayed,
}

impl DriftCall {
    /// How the replayed call compares with the recorded one.
    pub fn verdict(&self) -> Verdict {
        match &self.replayed {
            Replayed::Skipped(_) => Verdict::Skipped,
            Replayed::Failed(_) => Verdict::Failed,
            Replayed::Answer {
                schema_error: Some(_),
                ..
            } => Verdict::Invalid,
            Replayed::Answer { response, .. } => match self.call.recorded.answer() {
                None => Verdict::New,
                Some(recorded) if same_json(recorded, &response.response) => Verdict::Same,
                Some(_) => Verdict::Changed,
            },
        }
    }
}

/// Whether two payloads are the same JSON value (object key order aside); otherwise the same bytes.
fn same_json(a: &Payload, b: &Payload) -> bool {
    match (
        serde_json::from_slice::<Value>(a.as_bytes()),
        serde_json::from_slice::<Value>(b.as_bytes()),
    ) {
        (Ok(a), Ok(b)) => a == b,
        _ => a == b,
    }
}

/// Limits on a replay. Calls cost real tokens.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriftOptions {
    /// The most calls to make; the rest are skipped.
    pub max_calls: Option<usize>,
    /// The most tokens the replay may spend, counted by the calls' budgets before each call is made (so
    /// it is never exceeded); calls that do not fit are skipped.
    pub max_tokens: Option<u64>,
    /// Attempts per call when the executor asks for a retry.
    pub attempts: u32,
    /// The wait before a retry, multiplied by the attempt's number.
    pub retry_delay: Duration,
}

impl Default for DriftOptions {
    fn default() -> Self {
        DriftOptions {
            max_calls: None,
            max_tokens: None,
            attempts: 3,
            retry_delay: Duration::from_secs(2),
        }
    }
}

/// A model-drift replay's results.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DriftReport {
    /// Every call, in order.
    pub calls: Vec<DriftCall>,
}

impl DriftReport {
    /// How many calls have this verdict.
    pub fn count(&self, verdict: Verdict) -> usize {
        self.calls.iter().filter(|c| c.verdict() == verdict).count()
    }

    /// The tokens the replayed answers used.
    pub fn tokens_used(&self) -> u64 {
        self.calls
            .iter()
            .filter_map(|c| match &c.replayed {
                Replayed::Answer { response, .. } => Some(response.tokens()),
                _ => None,
            })
            .sum()
    }

    /// Whether every replayed call gave the recorded answer (or, with no recorded answer, a valid one).
    pub fn holds(&self) -> bool {
        self.calls
            .iter()
            .all(|c| matches!(c.verdict(), Verdict::Same | Verdict::New))
    }
}

/// Asks every call again with `executor`, within `options`' limits, and compares the answers.
pub fn replay_agent_calls(
    calls: Vec<RecordedCall>,
    executor: &dyn AgentExecutor,
    options: &DriftOptions,
) -> DriftReport {
    let mut report = DriftReport::default();
    let mut made = 0usize;
    let mut reserved = 0u64;
    for call in calls {
        let over_calls = options.max_calls.is_some_and(|max| made >= max);
        let over_tokens = options
            .max_tokens
            .is_some_and(|max| reserved.saturating_add(call.budget_tokens) > max);
        let replayed = if over_calls {
            Replayed::Skipped("the replay's call limit is reached".into())
        } else if over_tokens {
            Replayed::Skipped(format!(
                "its budget of {} tokens does not fit in the replay's token limit",
                call.budget_tokens
            ))
        } else {
            made += 1;
            reserved = reserved.saturating_add(call.budget_tokens);
            ask(&call, executor, options)
        };
        report.calls.push(DriftCall { call, replayed });
    }
    report
}

fn ask(call: &RecordedCall, executor: &dyn AgentExecutor, options: &DriftOptions) -> Replayed {
    let request = AgentCall {
        run: &call.run,
        name: &call.name,
        request: &call.request,
        budget_tokens: call.budget_tokens,
    };
    let attempts = options.attempts.max(1);
    for attempt in 1..=attempts {
        match executor.call(&request) {
            AgentAttempt::Completed(response) => {
                if response.tokens() > call.budget_tokens {
                    return Replayed::Failed(format!(
                        "the answer used {} tokens, over the call's budget of {}",
                        response.tokens(),
                        call.budget_tokens
                    ));
                }
                let schema_error = schema::output_schema(&call.request).and_then(|schema| {
                    match schema::check(&schema, &response.response) {
                        Ok(()) => None,
                        Err(SchemaError::Mismatch(why) | SchemaError::Schema(why)) => Some(why),
                    }
                });
                return Replayed::Answer {
                    response,
                    schema_error,
                };
            }
            AgentAttempt::Fail(why) => return Replayed::Failed(why),
            AgentAttempt::Retry(why) if attempt == attempts => {
                return Replayed::Failed(format!("{why} (after {attempts} attempts)"));
            }
            AgentAttempt::Retry(_) => std::thread::sleep(options.retry_delay * attempt),
        }
    }
    Replayed::Failed("no attempt was made".into())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    fn payload(text: &str) -> Payload {
        Payload::new(text.as_bytes().to_vec())
    }

    fn answer(model: &str, text: &str, tokens: u64) -> AgentResponse {
        AgentResponse {
            model: model.into(),
            params: payload("{}"),
            response: payload(text),
            input_tokens: tokens,
            output_tokens: 0,
        }
    }

    fn scheduled(cmd: u32, name: &str) -> Entry {
        Entry::AgentCallScheduled {
            cmd: CommandIndex(cmd),
            name: name.into(),
            request: payload(r#"{"input":1,"prompt":"p","output_schema":{"enum":["a","b"]}}"#),
            budget_tokens: 100,
        }
    }

    /// Answers from a script, one per call.
    struct Script(Mutex<Vec<AgentAttempt>>);
    impl AgentExecutor for Script {
        fn call(&self, _: &AgentCall<'_>) -> AgentAttempt {
            self.0.lock().unwrap().remove(0)
        }
    }

    fn journal() -> Vec<Entry> {
        vec![
            Entry::Started {
                workflow: "w".into(),
                input: payload("1"),
                budget_tokens: None,
                version: None,
            },
            scheduled(0, "one"),
            Entry::AgentCallCompleted {
                cmd: CommandIndex(0),
                response: answer("old/m", r#""a""#, 10),
            },
            scheduled(1, "two"),
            Entry::AgentAnswerEscalated {
                cmd: CommandIndex(1),
                error: "e".into(),
                answer: payload(r#""z""#),
            },
            Entry::EscalationResolved {
                cmd: CommandIndex(1),
                outcome: payload(r#""b""#),
            },
            scheduled(2, "three"),
            Entry::StepFailed {
                cmd: CommandIndex(2),
                error: "refused".into(),
            },
            scheduled(3, "four"),
            scheduled(4, "five"),
            scheduled(5, "six"),
        ]
    }

    #[test]
    fn recorded_calls_are_read_from_the_journal() {
        let calls = agent_calls("r", &journal());
        let recorded: Vec<_> = calls
            .iter()
            .map(|c| (c.name.as_str(), &c.recorded))
            .collect();
        assert_eq!(recorded.len(), 6);
        assert!(matches!(recorded[0], ("one", Recorded::Answer(a)) if a.model == "old/m"));
        assert_eq!(recorded[1], ("two", &Recorded::Supplied(payload(r#""b""#))));
        assert_eq!(recorded[2], ("three", &Recorded::Failed("refused".into())));
        assert_eq!(recorded[3], ("four", &Recorded::Pending));
    }

    #[test]
    fn answers_are_compared_under_the_schema_within_the_limits() {
        let script = Script(Mutex::new(vec![
            AgentAttempt::Completed(answer("new/m", r#" "a" "#, 10)),
            AgentAttempt::Completed(answer("new/m", r#""a""#, 10)),
            AgentAttempt::Retry("overloaded".into()),
            AgentAttempt::Completed(answer("new/m", r#""poem""#, 10)),
            AgentAttempt::Completed(answer("new/m", r#""b""#, 10)),
            AgentAttempt::Completed(answer("new/m", r#""b""#, 500)),
            AgentAttempt::Completed(answer("new/m", r#""a""#, 10)),
        ]));
        let options = DriftOptions {
            max_tokens: Some(500),
            retry_delay: Duration::ZERO,
            ..DriftOptions::default()
        };
        let report = replay_agent_calls(agent_calls("r", &journal()), &script, &options);
        let verdicts: Vec<_> = report.calls.iter().map(DriftCall::verdict).collect();
        assert_eq!(
            verdicts,
            [
                Verdict::Same,
                Verdict::Changed,
                Verdict::Invalid,
                Verdict::New,
                Verdict::Failed,
                Verdict::Skipped,
            ]
        );
        assert!(!report.holds());
        assert_eq!(report.count(Verdict::Same), 1);
        assert_eq!(report.tokens_used(), 40);
        // The over-budget answer is a failure, as the core would record it.
        assert!(
            matches!(&report.calls[4].replayed, Replayed::Failed(why) if why.contains("over the call's budget"))
        );
        // Five calls of 100 tokens fit in 500; the sixth is never made.
        assert_eq!(script.0.lock().unwrap().len(), 1);
    }

    #[test]
    fn retries_are_bounded() {
        let script = Script(Mutex::new(vec![
            AgentAttempt::Retry("429".into()),
            AgentAttempt::Retry("429".into()),
        ]));
        let options = DriftOptions {
            max_calls: Some(1),
            attempts: 2,
            retry_delay: Duration::ZERO,
            ..DriftOptions::default()
        };
        let report = replay_agent_calls(agent_calls("r", &journal()), &script, &options);
        assert!(
            matches!(&report.calls[0].replayed, Replayed::Failed(why) if why.contains("after 2 attempts"))
        );
        assert_eq!(report.count(Verdict::Skipped), 5);
    }
}
