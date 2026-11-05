//! Overload protection (ADR 0045): rate limits, concurrent requests, connections, running runs, and
//! floods of refused requests.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use aqueduct_engine::embedded::Engine;
use aqueduct_procedure::Procedure;
use aqueduct_protocol::v1;
use aqueduct_protocol::v1::ingress_client::IngressClient;
use aqueduct_server::auth::create_token;
use aqueduct_server::{Config, Limits, Permission, Security};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const WAITS: &str = "def run(x):\n    return wait(signal(\"go\"), timeout = 600)\n";

const ADD: &str = r#"
def run(call):
    return {"output": call["args"], "writes": {"n": call["args"]}}
"#;

struct Answer {
    status: u16,
    retry_after: Option<u64>,
    body: Value,
}

async fn http(
    addr: SocketAddr,
    token: Option<&str>,
    method: &str,
    path: &str,
    body: Value,
) -> Answer {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = body.to_string();
    let authorization = token.map_or(String::new(), |t| format!("Authorization: Bearer {t}\r\n"));
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{authorization}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let (head, text) = response.split_once("\r\n\r\n").unwrap();
    let retry_after = head
        .lines()
        .find_map(|line| {
            line.to_lowercase()
                .strip_prefix("retry-after: ")
                .map(str::to_owned)
        })
        .map(|value| value.parse().unwrap());
    Answer {
        status: response[9..12].parse().unwrap(),
        retry_after,
        body: serde_json::from_str(text).unwrap_or(Value::Null),
    }
}

struct Server {
    addr: SocketAddr,
    dir: std::path::PathBuf,
}

async fn serve(name: &str, limits: Value, tokens: &[(&str, Permission)]) -> (Server, Vec<String>) {
    let dir = std::env::temp_dir().join(format!("aqueduct-limits-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config: Config = serde_json::from_value(json!({"limits": limits})).unwrap();
    config.check().unwrap();
    let builder = Engine::builder(&dir)
        .workflow(
            "waits",
            Arc::new(Procedure::compile("w.star", WAITS).unwrap()),
        )
        .object_handler(
            "counter",
            "add",
            Arc::new(Procedure::compile("a.star", ADD).unwrap()),
        )
        .remote_steps(Duration::from_secs(30));
    let engine = Arc::new(config.apply(builder).unwrap().open().unwrap());
    let secrets = tokens
        .iter()
        .map(|(name, permission)| {
            create_token(&dir, name, [*permission].into_iter().collect()).unwrap()
        })
        .collect();
    let security = Security::for_dir(&dir).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        aqueduct_server::serve_with(engine, listener, &config, security, std::future::pending())
            .await
    });
    (Server { addr, dir }, secrets)
}

fn start_body(key: &str) -> Value {
    json!({"key": key, "workflow": "waits", "input": 1})
}

#[tokio::test(flavor = "multi_thread")]
async fn a_caller_over_its_rate_is_told_when_to_retry() {
    let (server, secrets) = serve(
        "rate",
        json!({"requests_per_second": 1.0, "burst": 2.0}),
        &[
            ("busy", Permission::Admin),
            ("quiet", Permission::Read),
            ("worker", Permission::Work),
        ],
    )
    .await;
    let (busy, quiet, worker) = (&secrets[0], &secrets[1], &secrets[2]);
    for _ in 0..2 {
        let answer = http(server.addr, Some(busy), "GET", "/v1/runs", json!({})).await;
        assert_eq!(answer.status, 200, "{}", answer.body);
    }
    let over = http(server.addr, Some(busy), "GET", "/v1/runs", json!({})).await;
    assert_eq!(
        (over.status, over.retry_after),
        (429, Some(1)),
        "{}",
        over.body
    );
    assert!(over.body["error"].as_str().unwrap().contains("rate limit"));

    // Over gRPC too.
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", server.addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let header: tonic::metadata::MetadataValue<_> = format!("Bearer {busy}").parse().unwrap();
    let mut client =
        IngressClient::with_interceptor(channel, move |mut request: tonic::Request<()>| {
            request
                .metadata_mut()
                .insert("authorization", header.clone());
            Ok(request)
        });
    let refused = client
        .list_runs(v1::ListRunsRequest::default())
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::ResourceExhausted);
    assert_eq!(refused.metadata().get("retry-after").unwrap(), "1");

    // Other callers have their own bucket, and workers none.
    assert_eq!(
        http(server.addr, Some(quiet), "GET", "/v1/runs", json!({}))
            .await
            .status,
        200
    );
    for _ in 0..10 {
        let answer = http(
            server.addr,
            Some(worker),
            "POST",
            "/v1/workers/poll",
            json!({"worker": "w", "steps": ["s"], "wait_ms": 0}),
        )
        .await;
        assert_eq!(answer.status, 200, "{}", answer.body);
    }
    // It recovers.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(
        http(server.addr, Some(busy), "GET", "/v1/runs", json!({}))
            .await
            .status,
        200
    );
    let _ = std::fs::remove_dir_all(server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn requests_over_the_concurrency_limit_are_shed_at_once() {
    let (server, _) = serve("concurrent", json!({"max_concurrent_requests": 2}), &[]).await;
    let started = http(server.addr, None, "POST", "/v1/runs", start_body("w")).await;
    assert_eq!(started.status, 200, "{}", started.body);
    // Two waits hold both places.
    let hold = |addr| async move {
        http(
            addr,
            None,
            "GET",
            "/v1/runs/w/result?timeout_ms=700",
            json!({}),
        )
        .await
    };
    let (first, second) = (
        tokio::spawn(hold(server.addr)),
        tokio::spawn(hold(server.addr)),
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    let clock = std::time::Instant::now();
    let shed = http(server.addr, None, "GET", "/v1/runs", json!({})).await;
    assert_eq!(
        (shed.status, shed.retry_after),
        (503, Some(1)),
        "{}",
        shed.body
    );
    assert!(
        clock.elapsed() < Duration::from_millis(200),
        "not shed at once"
    );
    assert_eq!(first.await.unwrap().status, 504);
    assert_eq!(second.await.unwrap().status, 504);
    // The places come back.
    assert_eq!(
        http(server.addr, None, "GET", "/v1/runs", json!({}))
            .await
            .status,
        200
    );
    let _ = std::fs::remove_dir_all(server.dir);
}

/// The flood ADR 0042 recorded: every request is refused, but only a bounded number write to the log.
#[tokio::test(flavor = "multi_thread")]
async fn a_flood_of_refused_requests_does_not_grow_the_audit_log() {
    let (server, secrets) = serve(
        "flood",
        json!({"audited_refusals_per_second": 5.0}),
        &[("reader", Permission::Read)],
    )
    .await;
    let flood = 400;
    for _ in 0..flood {
        let answer = http(server.addr, Some("aqd_wrong"), "GET", "/v1/runs", json!({})).await;
        assert_eq!(answer.status, 401);
    }
    let path = aqueduct_server::audit::audit_path(&server.dir);
    let (lines, _) = aqueduct_server::audit::verify(&path).unwrap();
    assert!(
        lines.len() < 60,
        "{} lines for {flood} refusals",
        lines.len()
    );
    // Allowed requests are not affected.
    assert_eq!(
        http(server.addr, Some(&secrets[0]), "GET", "/v1/runs", json!({}))
            .await
            .status,
        200
    );
    // The next line recorded says how many were not.
    tokio::time::sleep(Duration::from_millis(400)).await;
    http(server.addr, None, "GET", "/v1/runs", json!({})).await;
    let (lines, _) = aqueduct_server::audit::verify(&path).unwrap();
    let outcomes: Vec<&str> = lines.iter().filter_map(|l| l["outcome"].as_str()).collect();
    assert!(
        outcomes
            .iter()
            .any(|o| o.contains("earlier refusals were not recorded")),
        "{outcomes:#?}"
    );
    let _ = std::fs::remove_dir_all(server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn new_work_is_refused_at_the_limit_of_running_runs() {
    let (server, _) = serve("running", json!({"max_running_runs": 2}), &[]).await;
    let start = |key: &'static str| http(server.addr, None, "POST", "/v1/runs", start_body(key));
    assert_eq!(start("a").await.status, 200);
    assert_eq!(start("b").await.status, 200);
    let refused = start("c").await;
    assert_eq!(
        (refused.status, refused.retry_after),
        (503, Some(1)),
        "{}",
        refused.body
    );
    // A retried start is not new work.
    assert_eq!(start("a").await.status, 200);
    // Nor can a new object call get past it.
    let call = http(
        server.addr,
        None,
        "POST",
        "/v1/objects/counter/k/add",
        json!({"args": 1, "id": "c1", "timeout_ms": 5_000}),
    )
    .await;
    assert_eq!(call.status, 503, "{}", call.body);

    // A run ends: there is room again.
    http(
        server.addr,
        None,
        "POST",
        "/v1/runs/a/signals/go",
        json!({"payload": true}),
    )
    .await;
    let ended = http(
        server.addr,
        None,
        "GET",
        "/v1/runs/a/result?timeout_ms=5000",
        json!({}),
    )
    .await;
    assert_eq!(ended.body["status"], "completed", "{}", ended.body);
    let call = http(
        server.addr,
        None,
        "POST",
        "/v1/objects/counter/k/add",
        json!({"args": 1, "id": "c1", "timeout_ms": 5_000}),
    )
    .await;
    assert_eq!(call.status, 200, "{}", call.body);
    // The object's own loop takes no place: its call has ended, so there is room for a run.
    assert_eq!(
        start("c").await.status,
        200,
        "the object's loop took a place"
    );
    let _ = std::fs::remove_dir_all(server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn connections_over_the_limit_wait_for_a_place() {
    let (server, _) = serve("connections", json!({"max_connections": 2}), &[]).await;
    let first = tokio::net::TcpStream::connect(server.addr).await.unwrap();
    let _second = tokio::net::TcpStream::connect(server.addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let waiting = tokio::spawn({
        let addr = server.addr;
        async move { http(addr, None, "GET", "/v1/runs", json!({})).await.status }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!waiting.is_finished(), "served over the connection limit");
    drop(first);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap(),
        200
    );
    let _ = std::fs::remove_dir_all(server.dir);
}

/// Silent clients cannot use up the places: a connection that says nothing, or stalls in its headers, is
/// closed at the deadline.
#[tokio::test(flavor = "multi_thread")]
async fn silent_and_stalled_connections_lose_their_place() {
    let (server, _) = serve(
        "silent",
        json!({"max_connections": 2, "header_timeout_ms": 200}),
        &[],
    )
    .await;
    let mut silent = tokio::net::TcpStream::connect(server.addr).await.unwrap();
    let mut stalled = tokio::net::TcpStream::connect(server.addr).await.unwrap();
    stalled
        .write_all(b"GET /v1/runs HTTP/1.1\r\nHost:")
        .await
        .unwrap();
    // Both are closed in time, and the server answers again.
    let closed = |mut stream: tokio::net::TcpStream| async move {
        let mut rest = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut rest))
            .await
            .expect("not closed at the deadline")
            .ok();
    };
    let mut buffer = [0u8; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), silent.read(&mut buffer))
            .await
            .expect("a silent connection was not closed")
            .unwrap_or(0),
        0
    );
    closed(stalled).await;
    let answer = tokio::time::timeout(
        Duration::from_secs(5),
        http(server.addr, None, "GET", "/v1/runs", json!({})),
    )
    .await
    .expect("still locked out");
    assert_eq!(answer.status, 200);
    let _ = std::fs::remove_dir_all(server.dir);
}

#[test]
fn limits_in_a_configuration_are_checked() {
    let config: Config = serde_json::from_value(json!({"limits": {"max_connections": 0}})).unwrap();
    assert!(config.check().is_err());
    assert!(serde_json::from_value::<Config>(json!({"limits": {"max_conections": 5}})).is_err());
    assert_eq!(Config::default().limits, Limits::default());
}
