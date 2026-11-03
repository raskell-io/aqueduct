//! The ingress, over gRPC and over HTTP/JSON, end to end against an embedded engine.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::sync::Arc;

use aqueduct_engine::embedded::{Attempt, Engine};
use aqueduct_procedure::Procedure;
use aqueduct_protocol::v1;
use aqueduct_protocol::v1::ingress_client::IngressClient;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ORDER: &str = r#"
def run(order):
    publish("stage", "charging")
    receipt = step("charge", order["amount"])
    publish("stage", "waiting for an address")
    request = wait(update("address"), timeout = 600)
    address = "unknown"
    if request != None:
        address = request["value"]
        respond(request, "shipping to " + address)
    publish("stage", "waiting to ship")
    approval = wait(signal("ship"), timeout = 600)
    if approval == "review":
        approval = escalate("manual", {"order": order["id"]})
    return {"receipt": receipt, "address": address, "approval": approval}
"#;

/// A server on a free local port, with its own data directory.
async fn server(name: &str) -> (SocketAddr, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("aqueduct-server-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Engine::builder(&dir)
        .workflow(
            "order",
            Arc::new(Procedure::compile("order.star", ORDER).unwrap()),
        )
        .step("charge", |amount: &aqueduct_core::Payload| {
            let amount = String::from_utf8_lossy(amount.as_bytes()).into_owned();
            Attempt::Completed(aqueduct_core::Payload::new(
                format!("\"charged {amount}\"").into_bytes(),
            ))
        })
        .open()
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(aqueduct_server::serve(
        Arc::new(engine),
        listener,
        std::future::pending(),
    ));
    (addr, dir)
}

/// Waits until the run's query `stage` answers `stage`.
async fn reaches(client: &mut IngressClient<tonic::transport::Channel>, key: &str, stage: &str) {
    for _ in 0..1_000 {
        let answer = client
            .query(v1::QueryRequest {
                key: key.into(),
                name: "stage".into(),
            })
            .await;
        if let Ok(answer) = answer
            && answer.into_inner().value == format!("\"{stage}\"").into_bytes()
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("{key} never reached `{stage}`");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_over_grpc() {
    let (addr, dir) = server("grpc").await;
    let mut client = IngressClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    let started = client
        .start(v1::StartRequest {
            key: "o-1".into(),
            workflow: "order".into(),
            input: br#"{"id": "o-1", "amount": 40}"#.to_vec(),
            ..v1::StartRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(started.run_id, "o-1");

    reaches(&mut client, "o-1", "waiting for an address").await;
    let answer = client
        .update(v1::UpdateRequest {
            key: "o-1".into(),
            name: "address".into(),
            id: "a-1".into(),
            payload: br#""Lisbon""#.to_vec(),
            timeout_ms: 10_000,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        answer.answer,
        Some(v1::update_response::Answer::Result(
            br#""shipping to Lisbon""#.to_vec()
        ))
    );
    // The run waits for a signal now, not an update.
    let refused = client
        .update(v1::UpdateRequest {
            key: "o-1".into(),
            name: "address".into(),
            payload: br#""Porto""#.to_vec(),
            ..v1::UpdateRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        matches!(&refused.answer, Some(v1::update_response::Answer::Rejected(why)) if why.contains("not waiting")),
        "{refused:?}"
    );

    client
        .signal(v1::SignalRequest {
            key: "o-1".into(),
            name: "ship".into(),
            id: "s-1".into(),
            payload: br#""review""#.to_vec(),
        })
        .await
        .unwrap();
    let escalation = loop {
        let open = client
            .list_escalations(v1::ListEscalationsRequest {})
            .await
            .unwrap()
            .into_inner()
            .escalations;
        if let Some(escalation) = open.into_iter().next() {
            break escalation;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    };
    assert_eq!(escalation.reason, "manual");
    client
        .resolve_escalation(v1::ResolveEscalationRequest {
            id: escalation.id,
            outcome: br#""approved by Grace""#.to_vec(),
        })
        .await
        .unwrap();

    let result = client
        .wait(v1::WaitRequest {
            key: "o-1".into(),
            timeout_ms: 10_000,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        result.status,
        v1::Status::Completed as i32,
        "{}",
        result.error
    );
    let output: Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(
        output,
        json!({"receipt": "charged 40", "address": "Lisbon", "approval": "approved by Grace"})
    );

    let described = client
        .describe(v1::DescribeRequest { key: "o-1".into() })
        .await
        .unwrap()
        .into_inner();
    let kinds: Vec<&str> = described.commands.iter().map(|c| c.kind.as_str()).collect();
    assert_eq!(
        kinds,
        ["step", "update", "response", "signal", "escalation"]
    );
    let runs = client
        .list_runs(v1::ListRunsRequest::default())
        .await
        .unwrap()
        .into_inner()
        .runs;
    assert_eq!(runs.len(), 1);

    // Errors carry their codes.
    let unknown = client
        .start(v1::StartRequest {
            key: "x".into(),
            workflow: "nope".into(),
            input: b"1".to_vec(),
            ..v1::StartRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(unknown.code(), tonic::Code::NotFound);
    let not_json = client
        .start(v1::StartRequest {
            key: "x".into(),
            workflow: "order".into(),
            input: b"{not json".to_vec(),
            ..v1::StartRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(not_json.code(), tonic::Code::InvalidArgument);
    let conflict = client
        .start(v1::StartRequest {
            key: "o-1".into(),
            workflow: "order".into(),
            input: br#"{"id": "o-1", "amount": 99}"#.to_vec(),
            ..v1::StartRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), tonic::Code::AlreadyExists);
    let _ = std::fs::remove_dir_all(dir);
}

/// One HTTP/1.1 request with a JSON body; the status and the JSON answer.
async fn http(addr: SocketAddr, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    http_text(addr, method, path, &body).await
}

async fn http_text(addr: SocketAddr, method: &str, path: &str, body: &str) -> (u16, Value) {
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    raw(addr, &request).await
}

/// Sends a raw request; the status and the JSON answer.
async fn raw(addr: SocketAddr, request: &str) -> (u16, Value) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response.split_once("\r\n\r\n").unwrap().1;
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

async fn http_reaches(addr: SocketAddr, key: &str, stage: &str) {
    for _ in 0..1_000 {
        let (status, body) =
            http(addr, "GET", &format!("/v1/runs/{key}/queries/stage"), None).await;
        if status == 200 && body["value"] == stage {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("{key} never reached `{stage}`");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_over_http() {
    let (addr, dir) = server("http").await;
    let (status, started) = http(
        addr,
        "POST",
        "/v1/runs",
        Some(json!({"key": "o-2", "workflow": "order", "input": {"id": "o-2", "amount": 7}})),
    )
    .await;
    assert_eq!((status, &started["run_id"]), (200, &json!("o-2")));

    http_reaches(addr, "o-2", "waiting for an address").await;
    let (status, answer) = http(
        addr,
        "POST",
        "/v1/runs/o-2/updates/address",
        Some(json!({"payload": "Faro", "id": "a-1"})),
    )
    .await;
    assert_eq!(
        (status, &answer["result"]),
        (200, &json!("shipping to Faro"))
    );
    let (status, refused) = http(
        addr,
        "POST",
        "/v1/runs/o-2/updates/address",
        Some(json!({"payload": "Porto"})),
    )
    .await;
    assert_eq!(status, 422, "{refused}");

    http_reaches(addr, "o-2", "waiting to ship").await;
    http(
        addr,
        "POST",
        "/v1/runs/o-2/signals/ship",
        Some(json!({"payload": "review"})),
    )
    .await;
    let id = loop {
        let (_, open) = http(addr, "GET", "/v1/escalations", None).await;
        if let Some(id) = open["escalations"][0]["id"].as_str() {
            break id.to_owned();
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    };
    // `#` in an id is percent-encoded in the path.
    let (status, _) = http(
        addr,
        "POST",
        &format!("/v1/escalations/{}", id.replace('#', "%23")),
        Some(json!({"outcome": "approved"})),
    )
    .await;
    assert_eq!(status, 200);

    let (status, result) = http(addr, "GET", "/v1/runs/o-2/result?timeout_ms=10000", None).await;
    assert_eq!(status, 200);
    assert_eq!(result["status"], "completed", "{result}");
    assert_eq!(
        result["output"],
        json!({"receipt": "charged 7", "address": "Faro", "approval": "approved"})
    );
    let (_, run) = http(addr, "GET", "/v1/runs/o-2", None).await;
    assert_eq!(
        run["commands"][0],
        json!({"index": 0, "kind": "step", "name": "charge", "input": 7, "completed": "charged 7"})
    );
    let (_, runs) = http(addr, "GET", "/v1/runs?status=completed", None).await;
    assert_eq!(runs["runs"][0]["run_id"], "o-2");

    // A body without a JSON content type (as `curl -d` sends it) is read as JSON; a body that is not
    // JSON is refused in the API's form.
    let (status, _) = raw(addr, "POST /v1/runs/o-2/cancel HTTP/1.1\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await;
    assert_eq!(status, 200);
    let (status, error) = http_text(addr, "POST", "/v1/runs", "{oops").await;
    assert_eq!(status, 400);
    assert!(
        error["error"].as_str().unwrap().contains("not valid"),
        "{error}"
    );
    let (status, error) = http(addr, "GET", "/v1/runs/missing", None).await;
    assert_eq!(status, 404);
    assert!(
        error["error"]
            .as_str()
            .unwrap()
            .contains("no run `missing`")
    );
    let _ = std::fs::remove_dir_all(dir);
}
