//! The workload: a small pipeline with concurrent steps and a failure path, plus its reference model.

use std::sync::Arc;

use aqueduct_procedure::Procedure;

use crate::WorkloadKind;
use crate::rng::Rng;

use aqueduct_core::{
    Command, CommandIndex, Decider, Effect, Event, Payload, RunState, RunView, Status, StepOutcome,
};

/// `extract`, then `map_a` and `map_b` concurrently, then `merge` of both (in command order), then
/// `validate`, then complete with the validated value. Any failed step fails the run.
pub struct Pipeline;

const EXTRACT: CommandIndex = CommandIndex(0);
const MAP_A: CommandIndex = CommandIndex(1);
const MAP_B: CommandIndex = CommandIndex(2);
const MERGE: CommandIndex = CommandIndex(3);
const VALIDATE: CommandIndex = CommandIndex(4);

impl Decider for Pipeline {
    fn decide(&self, view: &RunView<'_>) -> Vec<Command> {
        let mut commands = vec![schedule("extract", view.input().clone())];
        let extracted = match outcome(view, EXTRACT, &mut commands) {
            Some(result) => result,
            None => return commands,
        };
        commands.push(schedule("map_a", extracted.clone()));
        commands.push(schedule("map_b", extracted));
        let a = outcome(view, MAP_A, &mut commands);
        if commands.last().is_some_and(is_terminal) {
            return commands;
        }
        let b = outcome(view, MAP_B, &mut commands);
        let (Some(a), Some(b)) = (a, b) else {
            return commands;
        };
        let mut merged = a.as_bytes().to_vec();
        merged.push(b'#');
        merged.extend_from_slice(b.as_bytes());
        commands.push(schedule("merge", Payload::new(merged)));
        let Some(merged) = outcome(view, MERGE, &mut commands) else {
            return commands;
        };
        commands.push(schedule("validate", merged));
        let Some(valid) = outcome(view, VALIDATE, &mut commands) else {
            return commands;
        };
        commands.push(Command::CompleteRun { output: valid });
        commands
    }
}

fn schedule(name: &str, input: Payload) -> Command {
    Command::ScheduleStep {
        name: name.into(),
        input,
    }
}

fn is_terminal(command: &Command) -> bool {
    matches!(
        command,
        Command::CompleteRun { .. } | Command::FailRun { .. }
    )
}

/// The completed result of a command; on failure, pushes `FailRun` and returns `None`.
fn outcome(view: &RunView<'_>, cmd: CommandIndex, commands: &mut Vec<Command>) -> Option<Payload> {
    match view.outcome(cmd)? {
        StepOutcome::Completed(result) => Some(result.clone()),
        StepOutcome::Failed(error) => {
            if !commands.last().is_some_and(is_terminal) {
                commands.push(Command::FailRun {
                    error: error.clone(),
                });
            }
            None
        }
    }
}

/// Executes a step body. Deterministic, so repeated executions return the same outcome.
pub fn execute(name: &str, input: &Payload) -> StepOutcome {
    let bytes = input.as_bytes();
    match name {
        "extract" if bytes.is_empty() => StepOutcome::Failed("nothing to extract".into()),
        "validate" if bytes.iter().map(|&b| u32::from(b)).sum::<u32>() % 5 == 0 => {
            StepOutcome::Failed("validation failed".into())
        }
        "validate" => StepOutcome::Completed(input.clone()),
        _ => {
            let mut out = bytes.to_vec();
            out.push(b'|');
            out.extend_from_slice(name.as_bytes());
            StepOutcome::Completed(Payload::new(out))
        }
    }
}

/// The same pipeline as [`Pipeline`], written as a Starlark procedure, with an agent call that classifies
/// what was extracted. Payloads are JSON strings.
pub const STARLARK_PIPELINE: &str = r##"
def run(input):
    extracted = step("extract", input)
    label = agent("classify", extracted, prompt = "prompts/classify.md", budget_tokens = 1000)
    a, b = parallel([["map_a", extracted], ["map_b", extracted]])
    merged = step("merge", a + "#" + b + "#" + label)
    return step("validate", merged)
"##;

/// The simulated model: answers deterministically, so the reference model gets the same answers, and
/// reports a usage that depends on the request.
pub fn fake_model(name: &str, request: &Payload) -> aqueduct_core::AgentResponse {
    let input = serde_json::from_slice::<serde_json::Value>(request.as_bytes())
        .ok()
        .and_then(|r| r.get("input").and_then(|i| i.as_str()).map(str::to_owned))
        .unwrap_or_default();
    let label = format!("{name}:{}", input.len() % 3);
    aqueduct_core::AgentResponse {
        model: "sim/fake-model@1".into(),
        params: Payload::new(*b"{\"temperature\":0}"),
        response: Payload::new(serde_json::Value::String(label).to_string().into_bytes()),
        input_tokens: request.as_bytes().len() as u64,
        output_tokens: 7,
    }
}

/// The pipeline as a Rust workflow written with the SDK: the same steps and data flow as
/// [`STARLARK_PIPELINE`], replayed by re-running the `async fn` on every activation. It first stamps
/// the run with a recorded UUID, passed to a step: a value used before it was durable, then recorded
/// again differently after a crash, shows up as one command dispatched with two inputs. Then it races
/// two steps with `select_all`, continues as new once (the second run of the key does the rest), and
/// undoes `merge` with a saga when validation fails.
pub async fn rust_pipeline(
    ctx: aqueduct_sdk::WorkflowContext,
    input: String,
) -> Result<String, aqueduct_sdk::WorkflowError> {
    let stamp = ctx.uuid();
    let _: String = ctx.step("stamp", &stamp.to_string()).await?;
    // A race whose winner depends on which result the journal recorded first. The winner feeds the next
    // step's input, so a replay that chose differently would diverge and park the run.
    let (winner, probe) = aqueduct_sdk::select_all(vec![
        ctx.step::<String, _>("probe_a", &input),
        ctx.step::<String, _>("probe_b", &input),
    ])
    .await;
    probe?; // a cancellation during the race surfaces here
    let _: String = ctx.step("winner", &winner.to_string()).await?;
    // The first run continues as new: the key's next run does the rest, with a marked input. Its
    // commands, timers and results must never mix with the first run's.
    if !input.starts_with('>') {
        return Err(ctx.continue_as_new(&format!(">{input}")));
    }
    let extracted: String = ctx.step("extract", &input).await?;
    let (a, b) = aqueduct_sdk::join(
        ctx.step::<String, _>("map_a", &extracted),
        ctx.step::<String, _>("map_b", &extracted),
    )
    .await;
    // A saga: `merge` is undone by `unmerge` when validation fails.
    let mut saga = ctx.saga();
    let joined = format!("{}#{}", a?, b?);
    let merged: String = saga.step("merge", &joined, "unmerge", &joined).await?;
    match ctx.step("validate", &merged).await {
        Ok(valid) => Ok(valid),
        Err(error) => Err(saga.abort(error).await),
    }
}

/// A procedure that sleeps one simulated second, waits for the `go` signal, then escalates a review and
/// goes on only if the reviewer approves.
pub const STARLARK_TIMERS: &str = r##"
def run(input):
    extracted = step("extract", input)
    sleep(1)
    go = wait(signal("go"))
    decision = escalate("review", extracted)
    if decision != "approve":
        fail("declined by the reviewer")
    return step("validate", extracted + "#" + go)
"##;

/// The token budget a run with this input starts with: for a third of runs too small for the Starlark
/// pipeline's agent call (which then escalates), for a third ample, for a third none.
pub fn run_budget(input: &Payload) -> Option<u64> {
    let sum: u32 = input.as_bytes().iter().map(|&b| u32::from(b)).sum();
    match sum % 3 {
        0 => Some(500),
        1 => Some(5_000),
        _ => None,
    }
}

/// How the simulated reviewer resolves an escalation with this reason, for a run with this input: a
/// budget escalation with `true` or `false`, a review with `"approve"` or `"decline"`.
pub fn resolution(reason: &str, input: &Payload) -> Payload {
    let decision = review_decision(input);
    if reason == aqueduct_core::BUDGET_ESCALATION {
        let approve = decision.as_bytes() == b"\"approve\"";
        Payload::new(if approve {
            b"true".to_vec()
        } else {
            b"false".to_vec()
        })
    } else {
        decision
    }
}

/// The simulated reviewer's decision for a run with this input: deterministic, so the reference model
/// resolves exactly as the client does. About one in four runs is declined.
pub fn review_decision(input: &Payload) -> Payload {
    let sum: u32 = input.as_bytes().iter().map(|&b| u32::from(b)).sum();
    if sum.is_multiple_of(4) {
        Payload::new(*b"\"decline\"")
    } else {
        Payload::new(*b"\"approve\"")
    }
}

/// Why the simulated client cancels runs.
pub const CANCEL_REASON: &str = "the client gave up";

/// The payload of the `go` signal the simulated client sends to every run.
pub const GO: &[u8] = b"\"go\"";

/// Step bodies for the Starlark pipeline: the same rules as [`execute`], on JSON strings.
pub fn execute_json(name: &str, input: &Payload) -> StepOutcome {
    let Ok(serde_json::Value::String(text)) = serde_json::from_slice(input.as_bytes()) else {
        return StepOutcome::Failed(format!("{name}: input is not a JSON string"));
    };
    match execute(name, &Payload::new(text.into_bytes())) {
        StepOutcome::Completed(result) => {
            let text = String::from_utf8_lossy(result.as_bytes()).into_owned();
            StepOutcome::Completed(Payload::new(
                serde_json::Value::String(text).to_string().into_bytes(),
            ))
        }
        failed => failed,
    }
}

/// Which decider and step bodies the simulated world runs.
pub struct Workload {
    pub kind: WorkloadKind,
    /// Whether runs wait for a `go` signal from the client.
    pub signals: bool,
    /// Whether runs escalate, for the client to resolve.
    pub escalations: bool,
    pub decider: Arc<dyn Decider + Send + Sync>,
    pub execute: fn(&str, &Payload) -> StepOutcome,
}

impl Workload {
    pub fn new(kind: WorkloadKind) -> Result<Workload, String> {
        Ok(match kind {
            WorkloadKind::Pipeline => Workload {
                kind,
                signals: false,
                escalations: false,
                decider: Arc::new(Pipeline),
                execute,
            },
            WorkloadKind::RustPipeline => Workload {
                kind,
                signals: false,
                escalations: false,
                decider: Arc::new(aqueduct_sdk::workflow(rust_pipeline)),
                execute: execute_json,
            },
            WorkloadKind::StarlarkTimers => Workload {
                kind,
                signals: true,
                escalations: true,
                decider: Arc::new(
                    Procedure::compile("timers.star", STARLARK_TIMERS)
                        .map_err(|e| e.to_string())?,
                ),
                execute: execute_json,
            },
            WorkloadKind::StarlarkPipeline => Workload {
                kind,
                signals: false,
                // Runs whose budget is too small for the agent call escalate for more.
                escalations: true,
                decider: Arc::new(
                    Procedure::compile("pipeline.star", STARLARK_PIPELINE)
                        .map_err(|e| e.to_string())?,
                ),
                execute: execute_json,
            },
        })
    }

    /// A random run input: up to five bytes, or for Starlark, a JSON string of up to five letters.
    pub fn input(&self, rng: &mut Rng) -> Payload {
        let len = rng.below(6);
        match self.kind {
            WorkloadKind::Pipeline => {
                Payload::new((0..len).map(|_| rng.below(256) as u8).collect::<Vec<u8>>())
            }
            WorkloadKind::StarlarkPipeline
            | WorkloadKind::StarlarkTimers
            | WorkloadKind::RustPipeline => {
                let text: String = (0..len)
                    .map(|_| char::from(b'a' + rng.below(26) as u8))
                    .collect();
                Payload::new(serde_json::Value::String(text).to_string().into_bytes())
            }
        }
    }
}

/// The expected end of a run with this input, computed by sequential execution without faults.
pub fn reference(
    workload: &Workload,
    input: &Payload,
) -> Result<(Status, Option<Payload>, Option<String>), String> {
    let first =
        aqueduct_core::start_with_budget(crate::node::WORKFLOW, input.clone(), run_budget(input));
    let mut state = RunState::from_journal(&first.entries).map_err(|e| e.to_string())?;
    let mut work = first.effects;
    let mut signalled = false;
    loop {
        let event = match work.pop() {
            Some(Effect::Activate) => Event::Decision(workload.decider.decide(&state.view())),
            Some(Effect::DispatchStep { cmd, name, input }) => Event::StepResult {
                cmd,
                outcome: (workload.execute)(&name, &input),
            },
            Some(Effect::DispatchAgent {
                cmd, name, request, ..
            }) => Event::AgentResult {
                cmd,
                outcome: Ok(fake_model(&name, &request)),
            },
            Some(Effect::Finished(_)) => continue,
            // The run continued as new: the key's next run takes over.
            Some(Effect::ContinueAsNew { input }) => {
                let left = state
                    .budget()
                    .map(|budget| budget.saturating_sub(state.tokens_used()));
                let next = aqueduct_core::start_with_budget(crate::node::WORKFLOW, input, left);
                state = RunState::from_journal(&next.entries).map_err(|e| e.to_string())?;
                work = next.effects;
                continue;
            }
            None if state.status().is_terminal() => break,
            // Nothing to do but wait: let time pass (fire timers), or deliver the client's signal.
            None => match state.timers().first() {
                Some(&(cmd, _)) => Event::TimerFired { cmd },
                None if workload.signals && !signalled => {
                    signalled = true;
                    Event::Signal {
                        name: "go".into(),
                        id: "go".into(),
                        payload: Payload::new(GO.to_vec()),
                    }
                }
                // The reviewer resolves the open escalation.
                None if !state.open_escalations().is_empty() => {
                    let (cmd, reason, _) = state.open_escalations()[0];
                    Event::ResolveEscalation {
                        cmd,
                        outcome: resolution(reason, input),
                    }
                }
                None => return Err("the reference run is stuck".into()),
            },
        };
        let transition = state.handle(event);
        for entry in &transition.entries {
            state.apply(entry).map_err(|e| e.to_string())?;
        }
        work.extend(transition.effects);
    }
    Ok((
        state.status(),
        state.output().cloned(),
        state.error().map(str::to_owned),
    ))
}
