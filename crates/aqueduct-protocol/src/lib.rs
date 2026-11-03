//! Aqueduct's service protocol, version 1 (draft, ADR 0038): the messages and services defined in
//! `proto/aqueduct/v1`, generated with prost and tonic.
//!
//! - [`v1::ingress_server`] and [`v1::ingress_client`]: starting runs and talking to them (start, wait,
//!   describe, signal, awakeables, cancel, queries, updates, escalations).
//! - [`v1::workers_server`] and [`v1::workers_client`]: step workers, which implement steps by name in
//!   any language and receive invocations over one stream.
//!
//! - [`v1::workflows_server`] and [`v1::workflows_client`]: workflow workers, which run code workflows
//!   outside the server: they receive activations (a run's history) and answer with decisions
//!   (ADR 0039). [`commands`] converts between the core's commands and their protobuf form.
//!
//! The protocol is a draft until phase 5: messages may still change. `docs/protocol.md` describes it,
//! with the HTTP/JSON form of the same calls.

#![forbid(unsafe_code)]

/// Version 1 of the protocol (package `aqueduct.v1`).
#[allow(missing_docs, clippy::all, clippy::pedantic)]
pub mod v1 {
    tonic::include_proto!("aqueduct.v1");
}

/// Conversions between the core's [`Command`](aqueduct_core::Command) and the protocol's
/// [`v1::WorkflowCommand`].
pub mod commands {
    use aqueduct_core::{Command, CommandIndex, Payload};

    use crate::v1;
    use crate::v1::workflow_command::Command as C;

    /// A command as the protocol carries it.
    pub fn to_proto(command: &Command) -> v1::WorkflowCommand {
        let command = match command.clone() {
            Command::ScheduleStep { name, input } => C::ScheduleStep(v1::ScheduleStep {
                name,
                input: input.0,
            }),
            Command::CallAgent {
                name,
                request,
                budget_tokens,
            } => C::CallAgent(v1::CallAgent {
                name,
                request: request.0,
                budget_tokens,
            }),
            Command::Sleep { duration_ms } => C::Sleep(v1::Sleep { duration_ms }),
            Command::AwaitSignal { name, timeout_ms } => {
                C::AwaitSignal(v1::AwaitSignal { name, timeout_ms })
            }
            Command::Record { name, value } => C::Record(v1::Record {
                name,
                value: value.0,
            }),
            Command::Escalate { reason, payload } => C::Escalate(v1::Escalate {
                reason,
                payload: payload.0,
            }),
            Command::StartChild {
                workflow,
                input,
                key,
            } => C::StartChild(v1::StartChild {
                workflow,
                input: input.0,
                key,
            }),
            Command::AwaitUpdate { name, timeout_ms } => {
                C::AwaitUpdate(v1::AwaitUpdate { name, timeout_ms })
            }
            Command::Respond { update, result } => C::Respond(v1::Respond {
                update: update.0,
                result: Some(match result {
                    Ok(result) => v1::respond::Result::Completed(result.0),
                    Err(error) => v1::respond::Result::Rejected(error),
                }),
            }),
            Command::CompleteRun { output } => C::CompleteRun(v1::CompleteRun { output: output.0 }),
            Command::ContinueAsNew { input } => {
                C::ContinueAsNew(v1::ContinueAsNew { input: input.0 })
            }
            Command::FailRun { error } => C::FailRun(v1::FailRun { error }),
            Command::Park { reason } => C::Park(v1::Park { reason }),
        };
        v1::WorkflowCommand {
            command: Some(command),
        }
    }

    /// A command from the protocol, or why it is not one.
    pub fn from_proto(command: v1::WorkflowCommand) -> Result<Command, String> {
        Ok(match command.command.ok_or("a command without a kind")? {
            C::ScheduleStep(c) => Command::ScheduleStep {
                name: c.name,
                input: Payload::new(c.input),
            },
            C::CallAgent(c) => Command::CallAgent {
                name: c.name,
                request: Payload::new(c.request),
                budget_tokens: c.budget_tokens,
            },
            C::Sleep(c) => Command::Sleep {
                duration_ms: c.duration_ms,
            },
            C::AwaitSignal(c) => Command::AwaitSignal {
                name: c.name,
                timeout_ms: c.timeout_ms,
            },
            C::Record(c) => Command::Record {
                name: c.name,
                value: Payload::new(c.value),
            },
            C::Escalate(c) => Command::Escalate {
                reason: c.reason,
                payload: Payload::new(c.payload),
            },
            C::StartChild(c) => Command::StartChild {
                workflow: c.workflow,
                input: Payload::new(c.input),
                key: c.key,
            },
            C::AwaitUpdate(c) => Command::AwaitUpdate {
                name: c.name,
                timeout_ms: c.timeout_ms,
            },
            C::Respond(c) => Command::Respond {
                update: CommandIndex(c.update),
                result: match c.result.ok_or("a response without a result")? {
                    v1::respond::Result::Completed(result) => Ok(Payload::new(result)),
                    v1::respond::Result::Rejected(error) => Err(error),
                },
            },
            C::CompleteRun(c) => Command::CompleteRun {
                output: Payload::new(c.output),
            },
            C::ContinueAsNew(c) => Command::ContinueAsNew {
                input: Payload::new(c.input),
            },
            C::FailRun(c) => Command::FailRun { error: c.error },
            C::Park(c) => Command::Park { reason: c.reason },
        })
    }
}
