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
