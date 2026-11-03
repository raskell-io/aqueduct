//! Push endpoints (ADR 0040): a named queue's steps are posted to an HTTP endpoint; an error status or
//! a timeout is a retryable failure.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use aqueduct_engine::RetryPolicy;
use aqueduct_engine::embedded::Engine;
use aqueduct_procedure::Procedure;
use aqueduct_server::{Config, PushEndpoint};
use axum::Json;
use axum::http::{HeaderMap, StatusCode};
use serde_json::{Value, json};

const FLOW: &str = r#"
def run(x):
    return [step("flaky", x), step("slow", x)]
"#;

/// The endpoint: `flaky` answers 503 on its first attempt, `slow` takes too long on its first.
async fn endpoint(headers: HeaderMap, Json(invocation): Json<Value>) -> (StatusCode, Json<Value>) {
    assert_eq!(headers["authorization"], "Bearer test");
    assert_eq!(
        headers["aqueduct-idempotency-key"],
        invocation["idempotency_key"].as_str().unwrap()
    );
    let attempt = invocation["attempt"].as_u64().unwrap();
    match (invocation["step"].as_str().unwrap(), attempt) {
        ("flaky", 1) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "warming up"})),
        ),
        ("slow", 1) => {
            tokio::time::sleep(Duration::from_secs(2)).await;
            (StatusCode::OK, Json(json!({"completed": "too late"})))
        }
        (step, attempt) => (
            StatusCode::OK,
            Json(json!({"completed": format!("{step} on attempt {attempt}")})),
        ),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn steps_are_pushed_and_failed_requests_retried() {
    let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_addr = target.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            target,
            axum::Router::new().fallback(axum::routing::post(endpoint)),
        )
        .await
        .unwrap();
    });

    let config: Config = serde_json::from_value(json!({
        "queues": {"functions": ["flaky", "slow"]},
        "push": [{
            "queue": "functions",
            "url": format!("http://{target_addr}/run"),
            "timeout_ms": 300,
            "headers": {"authorization": "Bearer test"},
        }],
    }))
    .unwrap();
    let push: &PushEndpoint = &config.push[0];
    assert_eq!(push.concurrency, 8);

    let dir = std::env::temp_dir().join(format!("aqueduct-push-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let builder = Engine::builder(&dir)
        .workflow(
            "flow",
            Arc::new(Procedure::compile("flow.star", FLOW).unwrap()),
        )
        .default_retry_policy(RetryPolicy {
            max_attempts: 5,
            initial_backoff_ms: 10,
            multiplier: 1,
            max_backoff_ms: 10,
        })
        .remote_steps(Duration::from_secs(30));
    let engine = Arc::new(config.apply(builder).unwrap().open().unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    {
        let (engine, config) = (Arc::clone(&engine), config.clone());
        tokio::spawn(async move {
            aqueduct_server::serve_with(engine, listener, &config, std::future::pending()).await
        });
    }

    let waiting = Arc::clone(&engine);
    let result = tokio::task::spawn_blocking(move || {
        waiting
            .start("f-1", "flow", aqueduct_core::Payload::new(*b"1"))
            .unwrap();
        waiting.wait("f-1", Duration::from_secs(20))
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        result.status,
        aqueduct_core::Status::Completed,
        "{:?}",
        result.error
    );
    let output: Value = serde_json::from_slice(result.output.unwrap().as_bytes()).unwrap();
    assert_eq!(output, json!(["flaky on attempt 2", "slow on attempt 2"]));

    // The failed attempts are in the journal, with why they failed.
    let (_, state) = engine.describe("f-1").unwrap();
    assert!(state.steps().iter().all(|s| s.failed_attempts == 1));
    let _ = std::fs::remove_dir_all(dir);
}
