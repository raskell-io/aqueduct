//! Counterfactual replay (BRIEF.md section 5.2, mode 3; ADR 0028): run a past run's input against a *new*
//! procedure or code version, answering what it asks for with what the past run recorded, and report
//! where the outcome differs.
//!
//! Nothing runs for real and nothing is written. The new version drives the core's state machine
//! ([`RunState`]) on its own; each command it issues is answered from the old run's journal:
//!
//! - a step by a recorded result of a step with the same name and input, an agent call by a recorded
//!   answer to the same name and request (checked against the new call's output schema), an escalation by
//!   the resolution of one with the same reason and payload, a budget escalation by whether more budget was
//!   approved for the same call; each recorded result answers at most one command;
//! - the old run's signals arrive in their order, when nothing else can make progress; then timers fire
//!   (time is skipped, not waited for); then a cancellation, if the old run had one.
//!
//! A command the old run never made is answered from a stub (a result by step or agent call name), or
//! left unresolved: the replay goes on without it as far as it can, and the report names it. Divergence is
//! the output, not an error.

use std::collections::{BTreeMap, VecDeque};

use aqueduct_core::{
    AgentResponse, CommandIndex, CommandKind, Decider, Entry, Env, Event, Hold, Payload, RunState,
    ScheduledStep, Status, StepOutcome, start_with_budget,
};
use serde_json::Value;

use crate::schema;

/// What a run came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// It completed with this output.
    Completed(Payload),
    /// It failed with this error.
    Failed(String),
    /// It continued as new with this input.
    Continued(Payload),
    /// It was parked: its logic diverged from its journal (only an old run can be).
    Parked(String),
    /// It has not ended: it waits for something (the old run: still running; the new one: for something
    /// the old run never provided).
    Waiting,
}

impl Outcome {
    fn of(state: &RunState, journal: &[Entry]) -> Outcome {
        match state.status() {
            Status::Completed => Outcome::Completed(state.output().cloned().unwrap_or_default()),
            Status::Failed => Outcome::Failed(state.error().unwrap_or_default().to_owned()),
            Status::Continued => {
                Outcome::Continued(state.continued_with().cloned().unwrap_or_default())
            }
            Status::Parked => Outcome::Parked(
                journal
                    .iter()
                    .rev()
                    .find_map(|entry| match entry {
                        Entry::Parked { reason } => Some(reason.clone()),
                        _ => None,
                    })
                    .unwrap_or_default(),
            ),
            Status::Running => Outcome::Waiting,
        }
    }

    /// Whether two outcomes are the same (payloads compared as JSON values).
    pub fn same_as(&self, other: &Outcome) -> bool {
        match (self, other) {
            (Outcome::Completed(a), Outcome::Completed(b))
            | (Outcome::Continued(a), Outcome::Continued(b)) => same_json(a, b),
            (Outcome::Failed(a), Outcome::Failed(b)) | (Outcome::Parked(a), Outcome::Parked(b)) => {
                a == b
            }
            (Outcome::Waiting, Outcome::Waiting) => true,
            _ => false,
        }
    }
}

/// A command of the new version that nothing in the old run could answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unresolved {
    /// The command's index in the new run.
    pub cmd: CommandIndex,
    /// What it is and why it could not be answered.
    pub what: String,
}

/// Where the new version's commands first differ from the old run's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Difference {
    /// The command index.
    pub cmd: CommandIndex,
    /// The old run's command there, if it had one.
    pub old: Option<String>,
    /// The new version's command there, if it issued one.
    pub new: Option<String>,
}

/// How a counterfactual run compares with the old one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Verdict {
    /// The same outcome.
    Same,
    /// A different outcome, reached with only recorded results and stubs.
    Different,
    /// The new version asked for something nothing could answer; its outcome is as far as it got.
    Unresolved,
}

impl Verdict {
    /// The verdict's name, as reports print it.
    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::Same => "same",
            Verdict::Different => "different",
            Verdict::Unresolved => "unresolved",
        }
    }
}

/// One past run, replayed against a new version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CounterfactualRun {
    /// The old run's run id.
    pub run: String,
    /// What the old run came to.
    pub old: Outcome,
    /// What the new version came to.
    pub new: Outcome,
    /// Where the commands first differ, if they do.
    pub first_difference: Option<Difference>,
    /// Commands nothing could answer.
    pub unresolved: Vec<Unresolved>,
    /// Commands answered from stubs, by name.
    pub stubbed: Vec<String>,
    /// The new run's journal, as it would have been written.
    pub journal: Vec<Entry>,
}

impl CounterfactualRun {
    /// How the new outcome compares with the old one.
    pub fn verdict(&self) -> Verdict {
        if !self.unresolved.is_empty() {
            Verdict::Unresolved
        } else if self.old.same_as(&self.new) {
            Verdict::Same
        } else {
            Verdict::Different
        }
    }
}

/// Options for a counterfactual replay.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CounterfactualOptions {
    /// Results for steps and agent calls the old run never made, by name. An agent call's stub is its
    /// answer.
    pub stubs: BTreeMap<String, Payload>,
}

/// The most events a counterfactual run handles before it is given up as waiting (a guard against a
/// version that loops on its own timers).
const MAX_EVENTS: usize = 100_000;

/// What the old run recorded, to answer the new version's commands with.
#[derive(Default)]
struct Evidence {
    steps: BTreeMap<(String, Vec<u8>), VecDeque<StepOutcome>>,
    agents: BTreeMap<(String, Vec<u8>), VecDeque<AgentEvidence>>,
    escalations: BTreeMap<(String, Vec<u8>), VecDeque<Payload>>,
    signals: VecDeque<(String, String, Payload)>,
    cancel: Option<String>,
}

#[derive(Clone)]
struct AgentEvidence {
    /// Whether more budget was approved for it (`None`: it was never held for budget).
    approved: Option<bool>,
    /// What it came to, if anything.
    outcome: Option<AgentOutcome>,
}

#[derive(Clone)]
enum AgentOutcome {
    Answer(AgentResponse),
    Supplied(Payload),
    Failed(String),
}

impl Evidence {
    fn from_journal(journal: &[Entry]) -> Evidence {
        let mut evidence = Evidence::default();
        // Commands by index: what kind of record their outcome goes to.
        let mut commands: BTreeMap<CommandIndex, Pending> = BTreeMap::new();
        let mut agents: BTreeMap<CommandIndex, ((String, Vec<u8>), AgentEvidence)> =
            BTreeMap::new();
        for entry in journal {
            match entry {
                Entry::StepScheduled { cmd, name, input } => {
                    commands.insert(*cmd, Pending::Step(name.clone(), input.as_bytes().to_vec()));
                }
                Entry::AgentCallScheduled {
                    cmd, name, request, ..
                }
                | Entry::AgentCallEscalated {
                    cmd, name, request, ..
                } => {
                    let held = matches!(entry, Entry::AgentCallEscalated { .. });
                    agents.insert(
                        *cmd,
                        (
                            (name.clone(), request.as_bytes().to_vec()),
                            AgentEvidence {
                                approved: held.then_some(false),
                                outcome: None,
                            },
                        ),
                    );
                }
                Entry::Escalated {
                    cmd,
                    reason,
                    payload,
                } => {
                    commands.insert(
                        *cmd,
                        Pending::Escalation(reason.clone(), payload.as_bytes().to_vec()),
                    );
                }
                Entry::BudgetApproved { cmd, .. } => {
                    if let Some((_, agent)) = agents.get_mut(cmd) {
                        agent.approved = Some(true);
                    }
                }
                Entry::AgentCallCompleted { cmd, response } => {
                    if let Some((_, agent)) = agents.get_mut(cmd) {
                        agent.outcome = Some(AgentOutcome::Answer(response.clone()));
                    }
                }
                Entry::StepCompleted { cmd, result } => {
                    if let Some(Pending::Step(name, input)) = commands.get(cmd) {
                        evidence
                            .steps
                            .entry((name.clone(), input.clone()))
                            .or_default()
                            .push_back(StepOutcome::Completed(result.clone()));
                    }
                }
                Entry::StepFailed { cmd, error } => {
                    if let Some((_, agent)) = agents.get_mut(cmd) {
                        agent.outcome = Some(AgentOutcome::Failed(error.clone()));
                    } else if let Some(Pending::Step(name, input)) = commands.get(cmd) {
                        evidence
                            .steps
                            .entry((name.clone(), input.clone()))
                            .or_default()
                            .push_back(StepOutcome::Failed(error.clone()));
                    }
                }
                Entry::EscalationResolved { cmd, outcome } => {
                    if let Some((_, agent)) = agents.get_mut(cmd) {
                        agent.outcome = Some(AgentOutcome::Supplied(outcome.clone()));
                    } else if let Some(Pending::Escalation(reason, payload)) = commands.get(cmd) {
                        evidence
                            .escalations
                            .entry((reason.clone(), payload.clone()))
                            .or_default()
                            .push_back(outcome.clone());
                    }
                }
                Entry::SignalReceived { name, id, payload } => {
                    evidence
                        .signals
                        .push_back((name.clone(), id.clone(), payload.clone()));
                }
                Entry::CancelRequested { reason } => evidence.cancel = Some(reason.clone()),
                _ => {}
            }
        }
        for (key, agent) in agents.into_values() {
            evidence.agents.entry(key).or_default().push_back(agent);
        }
        evidence
    }
}

enum Pending {
    Step(String, Vec<u8>),
    Escalation(String, Vec<u8>),
}

/// Replays a past run (its run id and journal) against `decider`, the new version.
pub fn replay_counterfactual(
    run: &str,
    journal: &[Entry],
    decider: &dyn Decider,
    options: &CounterfactualOptions,
) -> Result<CounterfactualRun, String> {
    let old_state = RunState::from_journal(journal).map_err(|e| e.to_string())?;
    let Some(Entry::Started {
        workflow,
        input,
        budget_tokens,
    }) = journal.first()
    else {
        return Err(format!(
            "the journal of `{run}` does not start with a start"
        ));
    };
    let mut evidence = Evidence::from_journal(journal);
    let key = aqueduct_core::split_run_id(run).0.to_owned();
    // The clock the new version sees: the old run's first recorded time, if it read one.
    let now_ms = journal
        .iter()
        .find_map(|entry| match entry {
            Entry::ValueRecorded { name, value, .. } if name == "now" => {
                serde_json::from_slice::<u64>(value.as_bytes()).ok()
            }
            _ => None,
        })
        .unwrap_or(0);
    let env = Env { now_ms, seed: 0 };

    let started = start_with_budget(workflow.clone(), input.clone(), *budget_tokens);
    let mut new = Replay {
        state: RunState::from_journal(&started.entries).map_err(|e| e.to_string())?,
        journal: started.entries,
        unresolved: Vec::new(),
        stubbed: Vec::new(),
        now_ms,
    };

    let mut events = 0;
    let mut cancelled = false;
    while new.state.status() == Status::Running && events < MAX_EVENTS {
        events += 1;
        let commands = decider.decide(&new.state.view_at(&key, env));
        new.handle(Event::Decision(commands))?;
        if new.state.status() != Status::Running {
            break;
        }
        if new.answer_commands(&mut evidence, options)? {
            continue;
        }
        if let Some((name, id, payload)) = evidence.signals.pop_front() {
            new.handle(Event::Signal { name, id, payload })?;
            continue;
        }
        if let Some((cmd, _)) = new.state.timers().into_iter().min_by_key(|(_, at)| *at) {
            new.handle(Event::TimerFired { cmd })?;
            continue;
        }
        if let (false, Some(reason)) = (cancelled, evidence.cancel.clone()) {
            cancelled = true;
            new.handle(Event::Cancel { reason })?;
            continue;
        }
        break;
    }

    let first_difference = first_difference(old_state.steps(), new.state.steps());
    Ok(CounterfactualRun {
        run: run.to_owned(),
        old: Outcome::of(&old_state, journal),
        new: Outcome::of(&new.state, &new.journal),
        first_difference,
        unresolved: new.unresolved,
        stubbed: new.stubbed,
        journal: new.journal,
    })
}

struct Replay {
    state: RunState,
    journal: Vec<Entry>,
    unresolved: Vec<Unresolved>,
    stubbed: Vec<String>,
    /// The clock the new version runs at (time does not pass: timers are fired when nothing else can
    /// make progress).
    now_ms: u64,
}

impl Replay {
    fn handle(&mut self, event: Event) -> Result<(), String> {
        let transition = self.state.handle_at(event, self.now_ms);
        for entry in transition.entries {
            self.state.apply(&entry).map_err(|e| e.to_string())?;
            self.journal.push(entry);
        }
        Ok(())
    }

    fn is_unresolved(&self, cmd: CommandIndex) -> bool {
        self.unresolved.iter().any(|u| u.cmd == cmd)
    }

    fn unresolvable(&mut self, cmd: CommandIndex, what: String) {
        if !self.is_unresolved(cmd) {
            self.unresolved.push(Unresolved { cmd, what });
        }
    }

    /// Answers every command without an outcome that the evidence or a stub can answer. Returns whether
    /// anything was answered.
    fn answer_commands(
        &mut self,
        evidence: &mut Evidence,
        options: &CounterfactualOptions,
    ) -> Result<bool, String> {
        let open: Vec<(CommandIndex, ScheduledStep)> = self
            .state
            .steps()
            .iter()
            .zip(0u32..)
            .filter(|(step, _)| step.outcome.is_none())
            .map(|(step, i)| (CommandIndex(i), step.clone()))
            .filter(|(cmd, _)| !self.is_unresolved(*cmd))
            .collect();
        let mut progress = false;
        for (cmd, step) in open {
            let key = (step.name.clone(), step.input.as_bytes().to_vec());
            let event = match (&step.kind, &step.held) {
                (CommandKind::Step, _) => {
                    if let Some(outcome) =
                        evidence.steps.get_mut(&key).and_then(VecDeque::pop_front)
                    {
                        Some(Event::StepResult { cmd, outcome })
                    } else if let Some(stub) = options.stubs.get(&step.name) {
                        self.stubbed.push(step.name.clone());
                        Some(Event::StepResult {
                            cmd,
                            outcome: StepOutcome::Completed(stub.clone()),
                        })
                    } else {
                        self.unresolvable(
                            cmd,
                            format!(
                                "step `{}` with input {}: the old run has no result for it",
                                step.name,
                                text(&step.input)
                            ),
                        );
                        None
                    }
                }
                (CommandKind::Agent { .. }, Some(Hold::Budget(_))) => {
                    match evidence.agents.get(&key).and_then(VecDeque::front) {
                        Some(AgentEvidence {
                            approved: Some(approved),
                            ..
                        }) => Some(Event::ResolveEscalation {
                            cmd,
                            outcome: Payload::new(if *approved {
                                b"true".to_vec()
                            } else {
                                b"false".to_vec()
                            }),
                        }),
                        _ => {
                            self.unresolvable(
                                cmd,
                                format!(
                                    "agent call `{}` would exceed the run's budget, and the old run never asked for more for it",
                                    step.name
                                ),
                            );
                            None
                        }
                    }
                }
                (CommandKind::Agent { .. }, Some(Hold::Answer(_))) => {
                    self.unresolvable(
                        cmd,
                        format!(
                            "agent call `{}`: the recorded answer does not match the new output schema",
                            step.name
                        ),
                    );
                    None
                }
                (CommandKind::Agent { .. }, None) => {
                    self.answer_agent(cmd, &step, evidence, options)
                }
                (CommandKind::Escalation, _) => {
                    if let Some(outcome) = evidence
                        .escalations
                        .get_mut(&key)
                        .and_then(VecDeque::pop_front)
                    {
                        Some(Event::ResolveEscalation { cmd, outcome })
                    } else {
                        self.unresolvable(
                            cmd,
                            format!(
                                "escalation `{}` with {}: the old run has no resolution for it",
                                step.name,
                                text(&step.input)
                            ),
                        );
                        None
                    }
                }
                // Timers and signals are answered by the replay's loop; recorded values are born complete.
                _ => None,
            };
            if let Some(event) = event {
                self.handle(event)?;
                progress = true;
            }
        }
        Ok(progress)
    }

    fn answer_agent(
        &mut self,
        cmd: CommandIndex,
        step: &ScheduledStep,
        evidence: &mut Evidence,
        options: &CounterfactualOptions,
    ) -> Option<Event> {
        let key = (step.name.clone(), step.input.as_bytes().to_vec());
        let recorded = evidence
            .agents
            .get_mut(&key)
            .and_then(|calls| {
                let at = calls.iter().position(|call| call.outcome.is_some())?;
                calls.remove(at)
            })
            .and_then(|call| call.outcome);
        let response = match recorded {
            Some(AgentOutcome::Failed(error)) => {
                return Some(Event::AgentResult {
                    cmd,
                    outcome: Err(error),
                });
            }
            Some(AgentOutcome::Answer(response)) => response,
            Some(AgentOutcome::Supplied(answer)) => supplied(answer, "recorded/supplied"),
            None => match options.stubs.get(&step.name) {
                Some(stub) => {
                    self.stubbed.push(step.name.clone());
                    supplied(stub.clone(), "stub")
                }
                None => {
                    self.unresolvable(
                        cmd,
                        format!(
                            "agent call `{}`: the old run has no answer to this request",
                            step.name
                        ),
                    );
                    return None;
                }
            },
        };
        if let Some(schema) = schema::output_schema(&step.input)
            && let Err(error) = schema::check(&schema, &response.response)
        {
            let why = match error {
                schema::SchemaError::Mismatch(why) | schema::SchemaError::Schema(why) => why,
            };
            self.unresolvable(
                cmd,
                format!(
                    "agent call `{}`: the recorded answer {} does not match the new output schema: {}",
                    step.name,
                    text(&response.response),
                    why.lines().map(str::trim).collect::<Vec<_>>().join(" ")
                ),
            );
            return None;
        }
        Some(Event::AgentResult {
            cmd,
            outcome: Ok(response),
        })
    }
}

/// An answer that no model gave in this replay (a person's, or a stub): no tokens.
fn supplied(answer: Payload, model: &str) -> AgentResponse {
    AgentResponse {
        model: model.to_owned(),
        params: Payload::new(b"{}".to_vec()),
        response: answer,
        input_tokens: 0,
        output_tokens: 0,
    }
}

/// The first command index where the two runs' commands differ.
fn first_difference(old: &[ScheduledStep], new: &[ScheduledStep]) -> Option<Difference> {
    let len = old.len().max(new.len());
    (0..len).find_map(|i| {
        let (a, b) = (old.get(i).map(describe), new.get(i).map(describe));
        (a != b).then(|| Difference {
            cmd: CommandIndex(u32::try_from(i).unwrap_or(u32::MAX)),
            old: a,
            new: b,
        })
    })
}

/// A command, for comparing and reporting: its kind, name and input.
pub fn describe(step: &ScheduledStep) -> String {
    let kind = match step.kind {
        CommandKind::Step => "step",
        CommandKind::Sleep { duration_ms, .. } => return format!("sleep {duration_ms} ms"),
        CommandKind::Signal { timeout_ms, .. } if timeout_ms > 0 => {
            return format!("wait for signal `{}` ({timeout_ms} ms)", step.name);
        }
        CommandKind::Signal { .. } => return format!("wait for signal `{}`", step.name),
        CommandKind::Recorded => return format!("value `{}`", step.name),
        CommandKind::Escalation => "escalation",
        CommandKind::Agent { .. } => "agent call",
    };
    format!("{kind} `{}` {}", step.name, text(&step.input))
}

fn text(payload: &Payload) -> String {
    String::from_utf8_lossy(payload.as_bytes()).into_owned()
}

fn same_json(a: &Payload, b: &Payload) -> bool {
    match (
        serde_json::from_slice::<Value>(a.as_bytes()),
        serde_json::from_slice::<Value>(b.as_bytes()),
    ) {
        (Ok(a), Ok(b)) => a == b,
        _ => a == b,
    }
}

#[cfg(test)]
mod tests {
    use aqueduct_core::{Command, RunView};

    use super::*;

    fn payload(text: &str) -> Payload {
        Payload::new(text.as_bytes().to_vec())
    }

    /// A past run: double the input, have it reviewed, wait for `go`, return the reviewed value.
    fn old_journal() -> Vec<Entry> {
        vec![
            Entry::Started {
                workflow: "w".into(),
                input: payload("5"),
                budget_tokens: None,
            },
            Entry::StepScheduled {
                cmd: CommandIndex(0),
                name: "double".into(),
                input: payload("5"),
            },
            Entry::StepCompleted {
                cmd: CommandIndex(0),
                result: payload("10"),
            },
            Entry::Escalated {
                cmd: CommandIndex(1),
                reason: "review".into(),
                payload: payload("10"),
            },
            Entry::EscalationResolved {
                cmd: CommandIndex(1),
                outcome: payload("11"),
            },
            Entry::SignalAwaited {
                cmd: CommandIndex(2),
                name: "go".into(),
                timeout_ms: 0,
                wake_at: 0,
            },
            Entry::SignalReceived {
                name: "go".into(),
                id: String::new(),
                payload: payload("true"),
            },
            Entry::RunCompleted {
                output: payload("11"),
            },
        ]
    }

    /// A version of the logic: which step it calls first, and what it adds to the reviewed value.
    struct Version {
        step: &'static str,
        add: u64,
    }

    impl Decider for Version {
        fn decide(&self, view: &RunView<'_>) -> Vec<Command> {
            let completed = |i: u32| match view.outcome(CommandIndex(i)) {
                Some(StepOutcome::Completed(value)) => Some(value.clone()),
                _ => None,
            };
            let mut commands = vec![Command::ScheduleStep {
                name: self.step.into(),
                input: view.input().clone(),
            }];
            let Some(value) = completed(0) else {
                return commands;
            };
            commands.push(Command::Escalate {
                reason: "review".into(),
                payload: value,
            });
            let Some(reviewed) = completed(1) else {
                return commands;
            };
            commands.push(Command::AwaitSignal {
                name: "go".into(),
                timeout_ms: 0,
            });
            if completed(2).is_none() {
                return commands;
            }
            let n: u64 = serde_json::from_slice(reviewed.as_bytes()).unwrap();
            commands.push(Command::CompleteRun {
                output: payload(&(n + self.add).to_string()),
            });
            commands
        }
    }

    fn replay(version: &Version, stubs: &[(&str, &str)]) -> CounterfactualRun {
        let options = CounterfactualOptions {
            stubs: stubs
                .iter()
                .map(|(name, value)| ((*name).to_owned(), payload(value)))
                .collect(),
        };
        replay_counterfactual("r", &old_journal(), version, &options).unwrap()
    }

    #[test]
    fn the_same_logic_reaches_the_same_outcome_from_the_record() {
        let run = replay(
            &Version {
                step: "double",
                add: 0,
            },
            &[],
        );
        assert_eq!(run.verdict(), Verdict::Same, "{run:?}");
        assert_eq!(run.new, Outcome::Completed(payload("11")));
        assert_eq!(run.first_difference, None);
        assert!(run.stubbed.is_empty());
    }

    #[test]
    fn a_changed_outcome_is_reported_not_raised() {
        let run = replay(
            &Version {
                step: "double",
                add: 1,
            },
            &[],
        );
        assert_eq!(run.verdict(), Verdict::Different);
        assert_eq!(
            (&run.old, &run.new),
            (
                &Outcome::Completed(payload("11")),
                &Outcome::Completed(payload("12"))
            )
        );
        // The commands are the same; only the output differs.
        assert_eq!(run.first_difference, None);
    }

    #[test]
    fn a_step_the_old_run_never_made_is_unresolved_or_stubbed() {
        let run = replay(
            &Version {
                step: "triple",
                add: 0,
            },
            &[],
        );
        assert_eq!(run.verdict(), Verdict::Unresolved);
        assert_eq!(run.new, Outcome::Waiting);
        assert!(
            run.unresolved[0]
                .what
                .contains("step `triple` with input 5")
        );
        let difference = run.first_difference.unwrap();
        assert_eq!(difference.old.as_deref(), Some("step `double` 5"));
        assert_eq!(difference.new.as_deref(), Some("step `triple` 5"));

        // Stubbed with the old result, the review recorded for it answers the escalation.
        let run = replay(
            &Version {
                step: "triple",
                add: 0,
            },
            &[("triple", "10")],
        );
        assert_eq!(run.verdict(), Verdict::Same, "{run:?}");
        assert_eq!(run.stubbed, ["triple"]);
        // Stubbed with another result, the escalation is a new one: nothing resolves it.
        let run = replay(
            &Version {
                step: "triple",
                add: 0,
            },
            &[("triple", "15")],
        );
        assert_eq!(run.verdict(), Verdict::Unresolved);
        assert!(
            run.unresolved[0]
                .what
                .contains("escalation `review` with 15")
        );
    }
}
