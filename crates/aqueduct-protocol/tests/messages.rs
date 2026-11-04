//! The generated messages encode and decode, and the services exist on both sides.

#![allow(clippy::unwrap_used)]

use aqueduct_protocol::v1::{
    StartRequest, StepInvocation, StepResult, WorkerMessage, step_result, worker_message,
};
use prost::Message;

#[test]
fn messages_round_trip() {
    let start = StartRequest {
        key: "claim-7".into(),
        workflow: "claims".into(),
        input: br#"{"amount": 400}"#.to_vec(),
        budget_tokens: 5_000,
        ..StartRequest::default()
    };
    assert_eq!(
        StartRequest::decode(start.encode_to_vec().as_slice()).unwrap(),
        start
    );

    let invocation = StepInvocation {
        invocation_id: "claim-7/0/1/1".into(),
        run_id: "claim-7".into(),
        command: 1,
        step: "lookup_policy".into(),
        input: br#""P-1""#.to_vec(),
        idempotency_key: "claim-7/0/1".into(),
        attempt: 1,
        lease_ms: 30_000,
    };
    assert_eq!(
        StepInvocation::decode(invocation.encode_to_vec().as_slice()).unwrap(),
        invocation
    );

    let result = WorkerMessage {
        message: Some(worker_message::Message::Result(StepResult {
            invocation_id: invocation.invocation_id.clone(),
            outcome: Some(step_result::Outcome::Retry("rate limited".into())),
        })),
    };
    assert_eq!(
        WorkerMessage::decode(result.encode_to_vec().as_slice()).unwrap(),
        result
    );
}

/// The server traits and clients are generated (compiles only if they exist).
#[allow(dead_code)]
fn services_exist() {
    fn server<T: aqueduct_protocol::v1::ingress_server::Ingress>() {}
    fn workers<T: aqueduct_protocol::v1::workers_server::Workers>() {}
    let _ = aqueduct_protocol::v1::ingress_client::IngressClient::<tonic::transport::Channel>::new;
    let _ = aqueduct_protocol::v1::workers_client::WorkersClient::<tonic::transport::Channel>::new;
}

#[test]
fn every_command_round_trips() {
    use aqueduct_core::{Command, CommandIndex, Payload};
    use aqueduct_protocol::commands::{from_proto, to_proto};
    let p = |s: &str| Payload::new(s.as_bytes().to_vec());
    let commands = vec![
        Command::ScheduleStep {
            name: "charge".into(),
            input: p("1"),
        },
        Command::CallAgent {
            name: "classify".into(),
            request: p("{}"),
            budget_tokens: 900,
        },
        Command::Sleep { duration_ms: 1_000 },
        Command::AwaitSignal {
            name: "go".into(),
            timeout_ms: 5,
        },
        Command::Record {
            name: "now".into(),
            value: p("12"),
        },
        Command::Escalate {
            reason: "review".into(),
            payload: p("null"),
        },
        Command::StartChild {
            workflow: "payment".into(),
            input: p("2"),
            key: None,
        },
        Command::StartChild {
            workflow: "payment".into(),
            input: p("2"),
            key: Some("pay:1".into()),
        },
        Command::AwaitUpdate {
            name: "address".into(),
            timeout_ms: 0,
        },
        Command::Respond {
            update: CommandIndex(4),
            result: Ok(p("\"ok\"")),
        },
        Command::Respond {
            update: CommandIndex(4),
            result: Err("no".into()),
        },
        Command::CompleteRun { output: p("3") },
        Command::ContinueAsNew { input: p("4") },
        Command::FailRun {
            error: "boom".into(),
        },
        Command::Park {
            reason: "stuck".into(),
        },
        Command::Send {
            object: "scores".into(),
            key: "git".into(),
            handler: "record".into(),
            args: p("1"),
        },
    ];
    for command in commands {
        assert_eq!(from_proto(to_proto(&command)).unwrap(), command);
        let encoded = to_proto(&command).encode_to_vec();
        let decoded = aqueduct_protocol::v1::WorkflowCommand::decode(encoded.as_slice()).unwrap();
        assert_eq!(from_proto(decoded).unwrap(), command);
    }
}
