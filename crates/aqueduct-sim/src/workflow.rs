//! The workload: a small pipeline with concurrent steps and a failure path, plus its reference model.

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

/// The expected end of a run with this input, computed by sequential execution without faults.
pub fn reference(input: &Payload) -> Result<(Status, Option<Payload>, Option<String>), String> {
    let first = start(input.clone());
    let mut state = RunState::from_journal(&first.entries).map_err(|e| e.to_string())?;
    let mut work = first.effects;
    while let Some(effect) = work.pop() {
        let event = match effect {
            Effect::Activate => Event::Decision(Pipeline.decide(&state.view())),
            Effect::DispatchStep { cmd, name, input } => Event::StepResult {
                cmd,
                outcome: execute(&name, &input),
            },
            Effect::Finished(_) => continue,
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
