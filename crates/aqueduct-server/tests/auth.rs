//! Authentication and authorisation (ADR 0041): every route needs its permission, over HTTP and gRPC;
//! what is not listed is closed; a revoked token stops working; and the audit log says who did what.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;

use aqueduct_engine::embedded::Engine;
use aqueduct_procedure::Procedure;
use aqueduct_protocol::v1;
use aqueduct_protocol::v1::ingress_client::IngressClient;
use aqueduct_server::auth::{create_token, revoke_token};
use aqueduct_server::{Config, Permission, Security};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const FLOW: &str = "def run(x):\n    return wait(signal(\"go\"), timeout = 600)\n";

async fn http(
    addr: SocketAddr,
    token: Option<&str>,
    method: &str,
    path: &str,
    body: Value,
) -> (u16, Value) {
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
    let status = response[9..12].parse().unwrap();
    let text = response.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    (status, serde_json::from_str(text).unwrap_or(Value::Null))
}

async fn server(name: &str) -> (SocketAddr, std::path::PathBuf, Arc<Engine>) {
    let dir = std::env::temp_dir().join(format!("aqueduct-auth-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Arc::new(
        Engine::builder(&dir)
            .workflow(
                "flow",
                Arc::new(Procedure::compile("flow.star", FLOW).unwrap()),
            )
            .remote_steps(std::time::Duration::from_secs(30))
            .open()
            .unwrap(),
    );
    ("127.0.0.1:0".parse().unwrap(), dir, engine)
}

async fn serve(dir: &std::path::Path, engine: Arc<Engine>) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let security = Security::for_dir(dir).unwrap();
    tokio::spawn(async move {
        aqueduct_server::serve_with(
            engine,
            listener,
            &Config::default(),
            security,
            std::future::pending(),
        )
        .await
    });
    addr
}

fn permissions(list: &[Permission]) -> BTreeSet<Permission> {
    list.iter().copied().collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_http_route_needs_its_permission() {
    let (_, dir, engine) = server("http").await;
    let admin = create_token(&dir, "ada", permissions(&[Permission::Admin])).unwrap();
    let addr = serve(&dir, engine).await;

    // Each route with the permission it needs. A token with every other permission is refused (403);
    // a token with this one gets past authorisation (anything but 401 and 403).
    let routes: Vec<(Permission, &str, &str, Value)> = vec![
        (
            Permission::Start,
            "POST",
            "/v1/runs",
            json!({"key": "r", "workflow": "flow", "input": 1}),
        ),
        (Permission::Read, "GET", "/v1/runs", json!({})),
        (Permission::Read, "GET", "/v1/runs/r", json!({})),
        (
            Permission::Read,
            "GET",
            "/v1/runs/r/result?timeout_ms=1",
            json!({}),
        ),
        (Permission::Read, "GET", "/v1/runs/r/queries/q", json!({})),
        (Permission::Read, "GET", "/v1/escalations", json!({})),
        (
            Permission::Signal,
            "POST",
            "/v1/runs/r/signals/other",
            json!({"payload": 1}),
        ),
        (
            Permission::Signal,
            "POST",
            "/v1/runs/r/updates/u",
            json!({"payload": 1, "timeout_ms": 1}),
        ),
        (
            Permission::Signal,
            "POST",
            "/v1/awakeables/r%231",
            json!({"payload": 1}),
        ),
        (
            Permission::Cancel,
            "POST",
            "/v1/runs/none/cancel",
            json!({}),
        ),
        (
            Permission::Resolve,
            "POST",
            "/v1/escalations/r%2399",
            json!({"outcome": 1}),
        ),
        (
            Permission::Work,
            "POST",
            "/v1/workers/poll",
            json!({"worker": "w", "steps": ["s"], "wait_ms": 1}),
        ),
        (
            Permission::Work,
            "POST",
            "/v1/workers/results",
            json!({"results": []}),
        ),
    ];
    let mut secrets = std::collections::BTreeMap::new();
    for permission in Permission::ALL {
        if permission != Permission::Admin {
            let secret =
                create_token(&dir, permission.as_str(), permissions(&[permission])).unwrap();
            secrets.insert(permission, secret);
        }
    }
    for (needed, method, path, body) in &routes {
        let (status, error) = http(addr, None, method, path, body.clone()).await;
        assert_eq!(status, 401, "{method} {path} without a token");
        assert!(error["error"].is_string(), "{error}");
        let (status, _) = http(addr, Some("aqd_wrong"), method, path, body.clone()).await;
        assert_eq!(status, 401, "{method} {path} with an unknown token");
        for (permission, secret) in &secrets {
            let (status, answer) = http(addr, Some(secret), method, path, body.clone()).await;
            if permission == needed {
                assert!(
                    status != 401 && status != 403,
                    "{method} {path} with {}: {status} {answer}",
                    permission.as_str()
                );
            } else {
                assert_eq!(status, 403, "{method} {path} with {}", permission.as_str());
            }
        }
        let (status, answer) = http(addr, Some(&admin), method, path, body.clone()).await;
        assert!(
            status != 401 && status != 403,
            "{method} {path} with admin: {status} {answer}"
        );
    }

    // What is not listed is closed, except to admin.
    let reader = &secrets[&Permission::Read];
    let (status, _) = http(addr, Some(reader), "GET", "/v1/secret", json!({})).await;
    assert_eq!(status, 403);
    let (status, _) = http(addr, Some(reader), "DELETE", "/v1/runs/r", json!({})).await;
    assert_eq!(status, 403);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_calls_are_refused_with_grpc_codes_and_audited() {
    let (_, dir, engine) = server("grpc").await;
    let reader = create_token(&dir, "reader", permissions(&[Permission::Read])).unwrap();
    let starter = create_token(&dir, "starter", permissions(&[Permission::Start])).unwrap();
    let addr = serve(&dir, engine).await;
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let with = |token: Option<String>| {
        IngressClient::with_interceptor(channel.clone(), move |mut request: tonic::Request<()>| {
            if let Some(token) = &token {
                request
                    .metadata_mut()
                    .insert("authorization", format!("Bearer {token}").parse().unwrap());
            }
            Ok(request)
        })
    };
    let start = || v1::StartRequest {
        key: "g-1".into(),
        workflow: "flow".into(),
        input: b"1".to_vec(),
        ..v1::StartRequest::default()
    };
    let refused = with(None).start(start()).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unauthenticated);
    let refused = with(Some(reader.clone())).start(start()).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);
    assert!(
        refused.message().contains("lacks the `start` permission"),
        "{refused}"
    );
    with(Some(starter.clone())).start(start()).await.unwrap();
    let described = with(Some(reader.clone()))
        .describe(v1::DescribeRequest { key: "g-1".into() })
        .await
        .unwrap();
    assert_eq!(described.into_inner().run_id, "g-1");

    // A worker without a token cannot open its stream.
    let refused = aqueduct_protocol::v1::workers_client::WorkersClient::new(channel.clone())
        .work(tokio_stream::iter(vec![v1::WorkerMessage {
            message: Some(v1::worker_message::Message::Hello(v1::Hello {
                worker: "w".into(),
                steps: vec!["s".into()],
                capacity: 1,
                queues: Vec::new(),
            })),
        }]))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unauthenticated);

    // Revoked: refused at once.
    revoke_token(&dir, "reader").unwrap();
    let refused = with(Some(reader))
        .describe(v1::DescribeRequest { key: "g-1".into() })
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unauthenticated);

    // The audit log says who did what, with the chain intact.
    let (lines, _) =
        aqueduct_server::audit::verify(&aqueduct_server::audit::audit_path(&dir)).unwrap();
    let summary: Vec<(String, String, String)> = lines
        .iter()
        .map(|line| {
            let field = |name: &str| line[name].as_str().unwrap().to_owned();
            (field("who"), field("action"), field("outcome"))
        })
        .collect();
    assert!(
        summary.contains(&("starter".into(), "start".into(), "ok".into())),
        "{summary:#?}"
    );
    assert!(
        summary
            .iter()
            .any(|(_, action, outcome)| action == "start" && outcome.contains("refused")),
        "{summary:#?}"
    );
    assert!(
        summary
            .iter()
            .any(|(_, action, outcome)| action == "work" && outcome.contains("refused")),
        "{summary:#?}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn without_tokens_requests_are_not_authenticated() {
    let (_, dir, engine) = server("open").await;
    let addr = serve(&dir, engine).await;
    let (status, _) = http(addr, None, "GET", "/v1/runs", json!({})).await;
    assert_eq!(status, 200);
    let _ = std::fs::remove_dir_all(dir);
}
