//! Virtual objects (ADR 0043): keyed, durable state whose handlers run one at a time per key, with their
//! state changes committed atomically with their completion.
//!
//! Objects are built from runs, so they have no journal entries of their own:
//!
//! - **An object key is a long-lived run** of the [`ObjectLoop`] under `$object/<object>/<key>`. Its inbox
//!   is the run's signals named `invoke`: durable, in order, and deduplicated by id.
//! - **A call is a child run** of the workflow `<object>.<handler>` under
//!   `$call/<object>/<key>/<id>`, started only once the previous call has ended: one at a time.
//! - **A handler's input is `{"args": ..., "state": {...}}`, and its output `{"output": ..., "writes":
//!   {...}}`** (a `null` write removes the name). The output is one `RunCompleted` record, so its writes
//!   commit atomically with the handler's completion, and a failed handler writes nothing.
//! - **The state is the fold of the outputs** in the loop's journal. Every so often the loop continues
//!   as new with the state as its input, so the journal stays short; only with an empty inbox, because
//!   signals kept by a generation do not reach the next.

use std::collections::BTreeMap;

use aqueduct_core::{Command, CommandIndex, Decider, Payload, RunView, StepOutcome};
use serde_json::{Map, Value, json};

/// The signal that puts a call in an object's inbox.
pub const INVOKE: &str = "invoke";

/// The query that answers with an object's state.
pub const STATE_QUERY: &str = "state";

/// Calls after which the loop continues as new, once its inbox is empty.
const COMPACT_EVERY: usize = 64;

/// The workflow name of an object's loop.
pub fn loop_workflow(object: &str) -> String {
    format!("$object/{object}")
}

/// The workflow name of a handler.
pub fn handler_workflow(object: &str, handler: &str) -> String {
    format!("{object}.{handler}")
}

/// The run key of an object key's loop.
pub fn object_run_key(object: &str, key: &str) -> String {
    format!("$object/{object}/{key}")
}

/// The run key of a call.
pub fn call_key(object: &str, key: &str, id: &str) -> String {
    format!("$call/{object}/{key}/{id}")
}

/// Whether a run key is reserved for objects: applications cannot start or signal runs under it.
pub fn is_reserved_key(key: &str) -> bool {
    key.starts_with("$object/") || key.starts_with("$call/")
}

/// The logic of an object key: takes calls from its inbox one at a time, and folds their writes.
#[derive(Clone, Debug)]
pub struct ObjectLoop {
    object: String,
}

impl ObjectLoop {
    /// The loop of `object`.
    pub fn new(object: &str) -> ObjectLoop {
        ObjectLoop {
            object: object.to_owned(),
        }
    }

    /// The commands issued so far, and the state committed so far.
    fn fold(&self, view: &RunView<'_>) -> (Vec<Command>, Map<String, Value>) {
        let mut state = serde_json::from_slice::<Value>(view.input().as_bytes())
            .ok()
            .and_then(|input| input.get("state").and_then(Value::as_object).cloned())
            .unwrap_or_default();
        let journaled = view.outcomes().count();
        let mut commands = Vec::new();
        let mut calls = 0;
        loop {
            // Compaction is decided only at the end of the journal, where nothing is replayed: earlier,
            // the journal says what the loop did, whatever the inbox holds now.
            if commands.len() >= journaled
                && calls >= COMPACT_EVERY
                && view.buffered_signals(INVOKE) == 0
            {
                let input = json!({ "state": state }).to_string().into_bytes();
                commands.push(Command::ContinueAsNew {
                    input: Payload::new(input),
                });
                return (commands, state);
            }
            let wait = CommandIndex(commands.len() as u32);
            commands.push(Command::AwaitSignal {
                name: INVOKE.to_owned(),
                timeout_ms: 0,
            });
            let call = match view.outcome(wait) {
                None => return (commands, state),
                // Only a cancellation fails a wait without a timeout.
                Some(StepOutcome::Failed(error)) => {
                    commands.push(Command::FailRun {
                        error: error.clone(),
                    });
                    return (commands, state);
                }
                Some(StepOutcome::Completed(call)) => call,
            };
            let call: Value = serde_json::from_slice(call.as_bytes()).unwrap_or_default();
            let (Some(handler), Some(key)) = (call["handler"].as_str(), call["key"].as_str())
            else {
                commands.push(Command::Park {
                    reason: "an object's inbox holds a call without a handler".into(),
                });
                return (commands, state);
            };
            let child = CommandIndex(commands.len() as u32);
            commands.push(Command::StartChild {
                workflow: handler_workflow(&self.object, handler),
                input: Payload::new(
                    json!({ "args": call["args"], "state": state })
                        .to_string()
                        .into_bytes(),
                ),
                key: Some(key.to_owned()),
            });
            match view.outcome(child) {
                None => return (commands, state),
                // A failed handler commits nothing.
                Some(StepOutcome::Failed(_)) => {}
                Some(StepOutcome::Completed(output)) => {
                    let output: Value =
                        serde_json::from_slice(output.as_bytes()).unwrap_or_default();
                    if let Some(writes) = output.get("writes").and_then(Value::as_object) {
                        for (name, value) in writes {
                            if value.is_null() {
                                state.remove(name);
                            } else {
                                state.insert(name.clone(), value.clone());
                            }
                        }
                    }
                }
            }
            calls += 1;
        }
    }
}

impl Decider for ObjectLoop {
    fn decide(&self, view: &RunView<'_>) -> Vec<Command> {
        self.fold(view).0
    }

    fn query(&self, view: &RunView<'_>, name: &str) -> Option<Payload> {
        (name == STATE_QUERY).then(|| {
            let state: BTreeMap<String, Value> = self.fold(view).1.into_iter().collect();
            Payload::new(json!(state).to_string().into_bytes())
        })
    }
}
