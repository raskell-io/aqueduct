//! The workload: a small pipeline with concurrent steps and a failure path, plus its reference model.

use std::sync::Arc;

use aqueduct_procedure::Procedure;

use crate::WorkloadKind;
use crate::rng::Rng;

use aqueduct_core::{
    Command, CommandIndex, Decider, Effect, Event, Payload, RunState, RunView, Status, StepOutcome,
    start,
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

/// The same pipeline as [`Pipeline`], written as a Starlark procedure. Payloads are JSON strings.
pub const STARLARK_PIPELINE: &str = r##"
def run(input):
    extracted = step("extract", input)
    a, b = parallel([["map_a", extracted], ["map_b", extracted]])
    merged = step("merge", a + "#" + b)
    return step("validate", merged)
"##;

/// The pipeline as a Rust workflow written with the SDK: the same steps and data flow as
/// [`STARLARK_PIPELINE`], replayed by re-running the `async fn` on every activation. It first stamps
/// the run with a recorded UUID, passed to a step: a value used before it was durable, then recorded
/// again differently after a crash, shows up as one command dispatched with two inputs.
pub async fn rust_pipeline(
    ctx: aqueduct_sdk::WorkflowContext,
    input: String,
) -> Result<String, aqueduct_sdk::WorkflowError> {
    let stamp = ctx.uuid();
    let _: String = ctx.step("stamp", &stamp.to_string()).await?;
    let extracted: String = ctx.step("extract", &input).await?;
    let (a, b) = aqueduct_sdk::join(
        ctx.step::<String, _>("map_a", &extracted),
        ctx.step::<String, _>("map_b", &extracted),
    )
    .await;
    let merged: String = ctx.step("merge", &format!("{}#{}", a?, b?)).await?;
    ctx.step("validate", &merged).await
}

/// A procedure that sleeps one simulated second, then waits for the `go` signal.
pub const STARLARK_TIMERS: &str = r##"
def run(input):
    extracted = step("extract", input)
    sleep(1)
    go = wait(signal("go"))
    return step("validate", extracted + "#" + go)
"##;

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
    pub decider: Arc<dyn Decider + Send + Sync>,
    pub execute: fn(&str, &Payload) -> StepOutcome,
}

impl Workload {
    pub fn new(kind: WorkloadKind) -> Result<Workload, String> {
        Ok(match kind {
            WorkloadKind::Pipeline => Workload {
                kind,
                signals: false,
                decider: Arc::new(Pipeline),
                execute,
            },
            WorkloadKind::RustPipeline => Workload {
                kind,
                signals: false,
                decider: Arc::new(aqueduct_sdk::workflow(rust_pipeline)),
                execute: execute_json,
            },
            WorkloadKind::StarlarkTimers => Workload {
                kind,
                signals: true,
                decider: Arc::new(
                    Procedure::compile("timers.star", STARLARK_TIMERS)
                        .map_err(|e| e.to_string())?,
                ),
                execute: execute_json,
            },
            WorkloadKind::StarlarkPipeline => Workload {
                kind,
                signals: false,
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
    let first = start(crate::node::WORKFLOW, input.clone());
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
            Some(Effect::Finished(_)) => continue,
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
