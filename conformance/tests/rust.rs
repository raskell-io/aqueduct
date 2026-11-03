//! The Rust transports pass the suite: steps in process, and `aqueduct-worker` over gRPC.

use aqueduct_conformance::{Transport, run};

async fn passes(transport: Transport) {
    let checks = run(&transport)
        .await
        .unwrap_or_else(|e| panic!("{transport}: {e}"));
    let failed: Vec<_> = checks.iter().filter(|c| !c.passed).collect();
    assert!(failed.is_empty(), "{transport}: {failed:#?}");
    assert_eq!(checks.len(), aqueduct_conformance::scenarios().len());
}

#[tokio::test(flavor = "multi_thread")]
async fn in_process() {
    passes(Transport::InProcess).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rust_worker_over_grpc() {
    passes(Transport::RustGrpc).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn workflows_decided_by_a_remote_worker() {
    passes(Transport::RemoteWorkflows).await;
}
