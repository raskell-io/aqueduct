//! Aqueduct procedures: hermetic Starlark scripts interpreted as deterministic deciders (ADR 0008).
//!
//! A procedure is a Starlark module that defines `run(input)`. The engine calls `run` on every
//! activation with the run's input (decoded from JSON). The script interacts with the world only
//! through engine-provided functions:
//!
//! - `step(name, input = None, fallible = False)` runs a step and returns its result. With
//!   `fallible = True` it returns `{"ok": True, "value": ...}` or `{"ok": False, "error": ...}`
//!   instead of failing the run when the step fails.
//! - `parallel([[name, input], ...])` runs several steps concurrently and returns their results in
//!   the order given.
//! - `agent(name, input, prompt = ..., output_schema = ..., budget_tokens = ...)` runs a bounded agent
//!   step (in this spike, a step named `agent:<name>`).
//! - `escalate(reason, payload = None)` ends the run with an escalation (in this spike, a failure
//!   whose error starts with `escalated:`).
//!
//! Replay works by re-execution: each call to `step` is a command with the next command index. If the
//! journal has an outcome for that index, the call returns it; otherwise the command is issued and the
//! script is suspended. Because the interpreter has no clock, randomness, file system, network or
//! `load`, the same journal always produces the same commands.
//!
//! ```
//! use aqueduct_core::{Command, Payload, RunState, Decider, start};
//! use aqueduct_procedure::Procedure;
//!
//! let procedure = Procedure::compile("greet.star", r#"
//! def run(input):
//!     name = step("lookup_name", input["user"])
//!     return {"greeting": "hello " + name}
//! "#).unwrap();
//!
//! let journal = start(Payload::new(br#"{"user": "u-1"}"#.to_vec())).entries;
//! let state = RunState::from_journal(&journal).unwrap();
//! let commands = procedure.decide(&state.view());
//! assert_eq!(commands, vec![Command::ScheduleStep {
//!     name: "lookup_name".into(),
//!     input: Payload::new(br#""u-1""#.to_vec()),
//! }]);
//! ```

#![forbid(unsafe_code)]

use std::cell::RefCell;

use aqueduct_core::{Command, Decider, Payload, RunView, StepOutcome};
use starlark::PrintHandler;
use starlark::codemap::FileSpanRef;
use starlark::environment::{Globals, GlobalsBuilder, Module};
use starlark::eval::{BeforeStmtFunc, BeforeStmtFuncDyn, Evaluator};
use starlark::starlark_module;
use starlark::syntax::{AstModule, Dialect};
use starlark::values::Value;
use starlark::values::none::NoneType;

/// Resource limits for one activation of a procedure.
///
/// starlark-rust allows recursion (unlike the Starlark specification), so termination is enforced by
/// a statement budget rather than by the language.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Statements executed per activation.
    pub max_statements: u64,
    /// Starlark heap size in bytes.
    pub max_heap_bytes: usize,
    /// Call stack depth.
    pub max_callstack: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_statements: 100_000,
            max_heap_bytes: 64 << 20,
            max_callstack: 50,
        }
    }
}

/// A procedure that does not compile.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("procedure {name}: {message}")]
pub struct ProcedureError {
    /// The procedure's file name.
    pub name: String,
    /// What is wrong.
    pub message: String,
}

/// A compiled procedure: a decider backed by a hermetic Starlark interpreter.
pub struct Procedure {
    name: String,
    source: String,
    globals: Globals,
    limits: Limits,
}

impl std::fmt::Debug for Procedure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Procedure")
            .field("name", &self.name)
            .field("limits", &self.limits)
            .finish()
    }
}

impl Procedure {
    /// Parses a procedure and checks that it defines `run`, with default limits.
    pub fn compile(name: &str, source: &str) -> Result<Procedure, ProcedureError> {
        Procedure::compile_with_limits(name, source, Limits::default())
    }

    /// Parses a procedure and checks that it defines `run`.
    pub fn compile_with_limits(
        name: &str,
        source: &str,
        limits: Limits,
    ) -> Result<Procedure, ProcedureError> {
        let procedure = Procedure {
            name: name.to_owned(),
            source: source.to_owned(),
            globals: GlobalsBuilder::standard().with(aqueduct_functions).build(),
            limits,
        };
        let error = |message: String| ProcedureError {
            name: name.to_owned(),
            message,
        };
        let ast = procedure.parse().map_err(error)?;
        let installed = Installed::new(Activation::default());
        let checked = Module::with_temp_heap(|module| -> Result<(), String> {
            let mut eval = procedure.evaluator(&module)?;
            eval.eval_module(ast, &procedure.globals)
                .map_err(|e| e.to_string())?;
            let run = module
                .get("run")
                .ok_or("the procedure does not define `run(input)`")?;
            if run.get_type() != "function" {
                return Err("`run` is not a function".into());
            }
            Ok(())
        });
        let activation = installed.finish();
        checked.map_err(error)?;
        if !activation.commands.is_empty() {
            return Err(error(
                "engine functions may only be called inside `run`".into(),
            ));
        }
        Ok(procedure)
    }

    /// The procedure's file name.
    pub fn name(&self) -> &str {
        &self.name
    }

    fn parse(&self) -> Result<AstModule, String> {
        let dialect = Dialect {
            enable_load: false,
            enable_f_strings: true,
            ..Dialect::Standard
        };
        AstModule::parse(&self.name, self.source.clone(), &dialect).map_err(|e| e.to_string())
    }

    fn evaluator<'v, 'a>(&self, module: &'a Module<'v>) -> Result<Evaluator<'v, 'a, 'a>, String> {
        let mut eval = Evaluator::new(module);
        eval.set_print_handler(&SilentPrint);
        eval.set_max_callstack_size(self.limits.max_callstack)
            .map_err(|e| e.to_string())?;
        eval.set_max_heap_size(self.limits.max_heap_bytes)
            .map_err(|e| e.to_string())?;
        // The statement hook is the only way to bound execution in starlark-rust 0.14; it is marked
        // as debugger API (ADR 0008, consequences).
        eval.before_stmt_for_dap(BeforeStmtFunc::from_dyn(Box::new(Budget {
            remaining: self.limits.max_statements,
        })));
        Ok(eval)
    }

    /// Runs `run(input)` once against the installed activation.
    fn activate(&self, input: &Payload) -> Result<Payload, ScriptError> {
        let input: serde_json::Value = serde_json::from_slice(input.as_bytes())
            .map_err(|e| ScriptError::Other(format!("the run's input is not JSON: {e}")))?;
        let ast = self.parse().map_err(ScriptError::Other)?;
        Module::with_temp_heap(|module| {
            let mut eval = self.evaluator(&module).map_err(ScriptError::Other)?;
            eval.eval_module(ast, &self.globals).map_err(classify)?;
            let run = module
                .get("run")
                .ok_or_else(|| ScriptError::Other("`run` disappeared".into()))?;
            let input = module.heap().alloc(input);
            with_activation(|act| {
                act.in_run = true;
                Ok(())
            })
            .map_err(|e| ScriptError::Other(e.to_string()))?;
            let output = eval.eval_function(run, &[input], &[]).map_err(classify)?;
            let json = output
                .to_json()
                .map_err(|e| ScriptError::Other(e.to_string()))?;
            Ok(Payload::new(json.into_bytes()))
        })
    }
}

impl Decider for Procedure {
    fn decide(&self, view: &RunView<'_>) -> Vec<Command> {
        let installed = Installed::new(Activation {
            outcomes: view.outcomes().map(|o| o.cloned()).collect(),
            ..Activation::default()
        });
        let result = self.activate(view.input());
        let activation = installed.finish();
        let mut commands = activation.commands;
        let terminal = match (result, activation.stop) {
            (Ok(output), _) => Some(Command::CompleteRun { output }),
            (Err(_), Some(Stop::Suspend)) => None,
            (Err(_), Some(Stop::StepFailed(error))) => Some(Command::FailRun { error }),
            (Err(_), Some(Stop::Escalate(reason))) => Some(Command::FailRun {
                error: format!("escalated: {reason}"),
            }),
            (Err(ScriptError::Fail(error)), None) => Some(Command::FailRun { error }),
            (Err(ScriptError::Other(reason)), None) => Some(Command::Park {
                reason: format!("procedure {}: {reason}", self.name),
            }),
        };
        commands.extend(terminal);
        commands
    }
}

/// Why `run` stopped, when it stopped because of an engine function.
#[derive(Debug)]
enum Stop {
    /// A step has no outcome yet; the run waits.
    Suspend,
    /// A step failed and was not `fallible`.
    StepFailed(String),
    /// The script escalated.
    Escalate(String),
}

#[derive(Debug)]
enum ScriptError {
    /// The script called `fail(...)`: a deliberate failure of the run.
    Fail(String),
    /// Anything else (type errors, budget exhausted): a bug, so the run is parked.
    Other(String),
}

fn classify(error: starlark::Error) -> ScriptError {
    match error.kind() {
        starlark::ErrorKind::Fail(message) => ScriptError::Fail(message.to_string()),
        _ => ScriptError::Other(error.to_string()),
    }
}

/// What one activation knows and what it has issued.
#[derive(Debug, Default)]
struct Activation {
    outcomes: Vec<Option<StepOutcome>>,
    commands: Vec<Command>,
    next: usize,
    stop: Option<Stop>,
    in_run: bool,
}

impl Activation {
    /// Issues the next step command; returns its recorded outcome, if any.
    fn issue(&mut self, name: String, input: Payload) -> anyhow::Result<Option<StepOutcome>> {
        if !self.in_run {
            anyhow::bail!("engine functions may only be called inside `run`");
        }
        let index = self.next;
        self.next += 1;
        self.commands.push(Command::ScheduleStep { name, input });
        Ok(self.outcomes.get(index).cloned().flatten())
    }

    fn stop(&mut self, stop: Stop) -> anyhow::Error {
        let message = match &stop {
            Stop::Suspend => "suspended: waiting for a step".to_owned(),
            Stop::StepFailed(error) => format!("step failed: {error}"),
            Stop::Escalate(reason) => format!("escalated: {reason}"),
        };
        self.stop = Some(stop);
        anyhow::anyhow!(message)
    }
}

// The activation in progress on this thread. Evaluation is synchronous, so a thread-local is enough,
// and it avoids `Evaluator::extra`, whose `ProvidesStaticType` derive generates `unsafe` code.
thread_local! {
    static ACTIVATION: RefCell<Option<Activation>> = const { RefCell::new(None) };
}

/// Installs an activation for the current thread and removes it again, even on panic.
struct Installed;

impl Installed {
    fn new(activation: Activation) -> Installed {
        ACTIVATION.with(|slot| *slot.borrow_mut() = Some(activation));
        Installed
    }

    fn finish(self) -> Activation {
        ACTIVATION
            .with(|slot| slot.borrow_mut().take())
            .unwrap_or_default()
    }
}

impl Drop for Installed {
    fn drop(&mut self) {
        ACTIVATION.with(|slot| slot.borrow_mut().take());
    }
}

fn with_activation<R>(f: impl FnOnce(&mut Activation) -> anyhow::Result<R>) -> anyhow::Result<R> {
    ACTIVATION.with(|slot| {
        let mut slot = slot.borrow_mut();
        let activation = slot
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("engine functions are not available here"))?;
        f(activation)
    })
}

fn suspend_or_fail<T>(stop: Stop) -> anyhow::Result<T> {
    Err(with_activation(|act| Ok(act.stop(stop)))?)
}

fn to_payload(value: Value<'_>) -> anyhow::Result<Payload> {
    Ok(Payload::new(value.to_json()?.into_bytes()))
}

fn from_payload(payload: &Payload) -> anyhow::Result<serde_json::Value> {
    serde_json::from_slice(payload.as_bytes())
        .map_err(|e| anyhow::anyhow!("a step result is not JSON: {e}"))
}

#[starlark_module]
fn aqueduct_functions(builder: &mut GlobalsBuilder) {
    /// Runs a step and returns its result.
    fn step<'v>(
        name: &str,
        #[starlark(default = NoneType)] input: Value<'v>,
        #[starlark(require = named, default = false)] fallible: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> anyhow::Result<Value<'v>> {
        let input = to_payload(input)?;
        let outcome = with_activation(|act| act.issue(name.to_owned(), input))?;
        let value = match outcome {
            None => return suspend_or_fail(Stop::Suspend),
            Some(StepOutcome::Completed(result)) if fallible => {
                serde_json::json!({ "ok": true, "value": from_payload(&result)? })
            }
            Some(StepOutcome::Completed(result)) => from_payload(&result)?,
            Some(StepOutcome::Failed(error)) if fallible => {
                serde_json::json!({ "ok": false, "error": error })
            }
            Some(StepOutcome::Failed(error)) => return suspend_or_fail(Stop::StepFailed(error)),
        };
        Ok(eval.heap().alloc(value))
    }

    /// Runs steps concurrently: `parallel([[name, input], ...])`. Returns results in the given order.
    fn parallel<'v>(
        calls: Value<'v>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> anyhow::Result<Value<'v>> {
        let calls: serde_json::Value = serde_json::from_str(&calls.to_json()?)?;
        let calls = calls
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("parallel expects a list of [name, input] pairs"))?;
        let mut outcomes = Vec::with_capacity(calls.len());
        for call in calls {
            let (Some(name), Some(input)) = (call.get(0).and_then(|n| n.as_str()), call.get(1))
            else {
                anyhow::bail!("parallel expects a list of [name, input] pairs");
            };
            let input = Payload::new(input.to_string().into_bytes());
            outcomes.push(with_activation(|act| act.issue(name.to_owned(), input))?);
        }
        // All commands are issued before waiting, so the steps run concurrently.
        let mut results = Vec::with_capacity(outcomes.len());
        for outcome in outcomes {
            match outcome {
                None => return suspend_or_fail(Stop::Suspend),
                Some(StepOutcome::Failed(error)) => {
                    return suspend_or_fail(Stop::StepFailed(error));
                }
                Some(StepOutcome::Completed(result)) => results.push(from_payload(&result)?),
            }
        }
        Ok(eval.heap().alloc(serde_json::Value::Array(results)))
    }

    /// Runs a bounded agent step. In this spike: a step named `agent:<name>` carrying its bounds.
    fn agent<'v>(
        name: &str,
        input: Value<'v>,
        #[starlark(require = named)] prompt: &str,
        #[starlark(require = named, default = NoneType)] output_schema: Value<'v>,
        #[starlark(require = named, default = 0)] budget_tokens: i32,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> anyhow::Result<Value<'v>> {
        let request = serde_json::json!({
            "input": serde_json::from_str::<serde_json::Value>(&input.to_json()?)?,
            "prompt": prompt,
            "output_schema": serde_json::from_str::<serde_json::Value>(&output_schema.to_json()?)?,
            "budget_tokens": budget_tokens,
        });
        let request = Payload::new(request.to_string().into_bytes());
        let outcome = with_activation(|act| act.issue(format!("agent:{name}"), request))?;
        match outcome {
            None => suspend_or_fail(Stop::Suspend),
            Some(StepOutcome::Failed(error)) => suspend_or_fail(Stop::StepFailed(error)),
            Some(StepOutcome::Completed(result)) => Ok(eval.heap().alloc(from_payload(&result)?)),
        }
    }

    /// Ends the run with an escalation.
    fn escalate<'v>(
        reason: &str,
        #[starlark(default = NoneType)] payload: Value<'v>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> anyhow::Result<NoneType> {
        let _ = (payload, eval);
        suspend_or_fail(Stop::Escalate(reason.to_owned()))
    }
}

/// Discards `print` output: procedures have no side effects except through engine functions.
struct SilentPrint;

impl PrintHandler for SilentPrint {
    fn println(&self, _text: &str) -> starlark::Result<()> {
        Ok(())
    }
}

/// Stops evaluation after a fixed number of statements.
struct Budget {
    remaining: u64,
}

impl<'e> BeforeStmtFuncDyn<'e> for Budget {
    fn call<'v>(
        &mut self,
        _span: FileSpanRef,
        _continued: bool,
        _eval: &mut Evaluator<'v, '_, 'e>,
    ) -> starlark::Result<()> {
        if self.remaining == 0 {
            return Err(starlark::Error::new_other(anyhow::anyhow!(
                "the activation exceeded its statement budget"
            )));
        }
        self.remaining -= 1;
        Ok(())
    }
}
