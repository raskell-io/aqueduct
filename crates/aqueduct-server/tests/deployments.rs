//! Deployments (ADR 0048): new runs start on the active build and stay pinned to it, only that build's
//! workers decide them, and a build drains before it is retired.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use aqueduct_engine::embedded::Engine;
use aqueduct_protocol::v1;
use aqueduct_protocol::v1::workflow_worker_message::Message;
use aqueduct_protocol::v1::workflows_client::WorkflowsClient;
use aqueduct_server::auth::create_token_in;
use aqueduct_server::{Config, Permission, Security};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// How long to watch for something that must not arrive.
const QUIET: Duration = Duration::from_millis(400);

struct Server {
    addr: SocketAddr,
    dir: PathBuf,
}

fn open(dir: &Path) -> Arc<Engine> {
    let config: Config = serde_json::from_value(json!({"remote_workflows": ["order"]})).unwrap();
    let builder = Engine::builder(dir).remote_steps(Duration::from_secs(30));
    Arc::new(config.apply(builder).unwrap().open().unwrap())
}

async fn listen(engine: Arc<Engine>, dir: &Path) -> SocketAddr {
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

async fn serve(name: &str) -> Server {
    let dir = std::env::temp_dir().join(format!("aqueduct-deploy-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let addr = listen(open(&dir), &dir).await;
    Server { addr, dir }
}

async fn http_as(
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
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{authorization}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let (_, text) = response.split_once("\r\n\r\n").unwrap();
    (
        response[9..12].parse().unwrap(),
        serde_json::from_str(text).unwrap_or(Value::Null),
    )
}

async fn http(addr: SocketAddr, method: &str, path: &str, body: Value) -> (u16, Value) {
    http_as(addr, None, method, path, body).await
}

/// Starts a run: the build it was pinned to.
async fn start(addr: SocketAddr, key: &str) -> String {
    let body = json!({"key": key, "workflow": "order", "input": 1});
    let (status, started) = http(addr, "POST", "/v1/runs", body).await;
    assert_eq!(status, 200, "{started}");
    started["version"].as_str().unwrap().to_owned()
}

async fn activate(addr: SocketAddr, build: &str) -> (u16, Value) {
    let path = format!("/v1/deployments/order/{build}/activate");
    http(addr, "POST", &path, json!({})).await
}

/// The list, as (build, state, open runs, workers).
async fn listed(addr: SocketAddr) -> Vec<(String, String, u64, u64)> {
    let (status, list) = http(addr, "GET", "/v1/deployments", Value::Null).await;
    assert_eq!(status, 200, "{list}");
    list["deployments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| {
            (
                d["build"].as_str().unwrap().to_owned(),
                d["state"].as_str().unwrap().to_owned(),
                d["open_runs"].as_u64().unwrap(),
                d["workers"].as_u64().unwrap(),
            )
        })
        .collect()
}

fn row(build: &str, state: &str, open: u64, workers: u64) -> (String, String, u64, u64) {
    (build.to_owned(), state.to_owned(), open, workers)
}

/// A workflow worker's session that decides by hand.
struct Session {
    out: tokio::sync::mpsc::Sender<v1::WorkflowWorkerMessage>,
    inbound: tonic::Streaming<v1::WorkflowServerMessage>,
}

impl Session {
    async fn connect(addr: SocketAddr, build: &str) -> Session {
        let mut client = WorkflowsClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        let (out, outbound) = tokio::sync::mpsc::channel(8);
        out.send(v1::WorkflowWorkerMessage {
            message: Some(Message::Hello(v1::WorkflowHello {
                worker: format!("worker-{build}"),
                workflows: vec!["order".to_owned()],
                capacity: 8,
                deployment: build.to_owned(),
            })),
        })
        .await
        .unwrap();
        let stream = tokio_stream::wrappers::ReceiverStream::new(outbound);
        let inbound = client.serve(stream).await.unwrap().into_inner();
        // It counts once the server has its Hello.
        tokio::time::sleep(Duration::from_millis(50)).await;
        Session { out, inbound }
    }

    /// The next activation, if one comes within `wait`.
    async fn next(&mut self, wait: Duration) -> Option<v1::Activation> {
        let message = tokio::time::timeout(wait, self.inbound.message())
            .await
            .ok()?
            .unwrap()?;
        let v1::workflow_server_message::Message::Activation(activation) = message.message?;
        Some(activation)
    }

    async fn take(&mut self) -> v1::Activation {
        self.next(Duration::from_secs(10))
            .await
            .expect("no activation")
    }

    async fn decide(&self, activation: &v1::Activation, command: v1::workflow_command::Command) {
        self.out
            .send(v1::WorkflowWorkerMessage {
                message: Some(Message::Decision(v1::Decision {
                    activation_id: activation.activation_id.clone(),
                    commands: vec![v1::WorkflowCommand {
                        command: Some(command),
                    }],
                })),
            })
            .await
            .unwrap();
    }

    /// Completes the run, saying who did.
    async fn complete(&self, activation: &v1::Activation, by: &str) {
        let output = format!("\"{by}\"").into_bytes();
        let command = v1::workflow_command::Command::CompleteRun(v1::CompleteRun { output });
        self.decide(activation, command).await;
    }
}

async fn result(addr: SocketAddr, key: &str) -> Value {
    let path = format!("/v1/runs/{key}/result?timeout_ms=10000");
    let (status, result) = http(addr, "GET", &path, Value::Null).await;
    assert_eq!(status, 200, "{result}");
    result
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rollout_leaves_old_runs_with_old_code() {
    let server = serve("rollout").await;
    let addr = server.addr;
    let mut old = Session::connect(addr, "v1").await;
    assert_eq!(activate(addr, "v1").await.0, 200);
    assert_eq!(start(addr, "first").await, "v1");
    let first = old.take().await;
    assert_eq!(first.run_id, "first");

    // The new build comes up, and new runs move to it. Until then it is sent nothing.
    let mut new = Session::connect(addr, "v2").await;
    assert_eq!(start(addr, "still-old").await, "v1");
    let still_old = old.take().await;
    assert!(new.next(QUIET).await.is_none(), "another build's run");
    assert_eq!(activate(addr, "v2").await.0, 200);
    assert_eq!(start(addr, "second").await, "v2");
    let second = new.take().await;
    assert_eq!(second.run_id, "second");
    assert!(old.next(QUIET).await.is_none(), "another build's run");

    assert_eq!(
        listed(addr).await,
        [row("v1", "draining", 2, 1), row("v2", "active", 1, 1)]
    );

    // Each is decided by its own build.
    old.complete(&first, "v1").await;
    old.complete(&still_old, "v1").await;
    new.complete(&second, "v2").await;
    for (key, build) in [("first", "v1"), ("still-old", "v1"), ("second", "v2")] {
        let result = result(addr, key).await;
        assert_eq!(result["output"], build, "{result}");
        assert_eq!(result["version"], build, "{result}");
    }
    assert_eq!(
        listed(addr).await,
        [row("v1", "draining", 0, 1), row("v2", "active", 0, 1)],
        "v1 is drained"
    );

    // Its workers can go; the list says so.
    drop(old);
    let begin = std::time::Instant::now();
    while listed(addr).await[0].3 != 0 {
        assert!(begin.elapsed() < Duration::from_secs(5), "still counted");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pinned_run_waits_for_its_own_build() {
    let server = serve("waits").await;
    let addr = server.addr;
    let old = Session::connect(addr, "v1").await;
    assert_eq!(activate(addr, "v1").await.0, 200);
    let mut old = old;
    start(addr, "k").await;
    let leased = old.take().await;
    let mut new = Session::connect(addr, "v2").await;
    assert_eq!(activate(addr, "v2").await.0, 200);

    // Its worker goes away with the run undecided: no other code picks it up.
    drop(old);
    drop(leased);
    assert!(new.next(QUIET).await.is_none(), "v2 was given v1's run");
    let mut unversioned = Session::connect(addr, "").await;
    assert!(
        unversioned.next(QUIET).await.is_none(),
        "a worker that says nothing was given a pinned run"
    );

    // Retiring it now would strand the run.
    let path = "/v1/deployments/order/v1/retire";
    let (status, refused) = http(addr, "POST", path, json!({})).await;
    assert_eq!(status, 422, "{refused}");
    assert!(
        refused["error"].as_str().unwrap().contains("1 open runs"),
        "{refused}"
    );

    // A worker of that build finishes it.
    let mut back = Session::connect(addr, "v1").await;
    let activation = back.take().await;
    assert_eq!(activation.run_id, "k");
    back.complete(&activation, "v1").await;
    assert_eq!(result(addr, "k").await["output"], "v1");

    let (status, _) = http(addr, "POST", path, json!({})).await;
    assert_eq!(status, 200);
    assert_eq!(listed(addr).await[0].1, "retired");

    // A retired build is gone: it cannot come back, and no run can start on it.
    assert_eq!(activate(addr, "v1").await.0, 422);
    let body = json!({"key": "pin", "workflow": "order", "input": 1, "version": "v1"});
    assert_eq!(http(addr, "POST", "/v1/runs", body).await.0, 404);
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn activating_is_refused_without_a_worker_and_the_active_build_stays() {
    let server = serve("guards").await;
    let addr = server.addr;
    let (status, refused) = activate(addr, "v1").await;
    assert_eq!(status, 422, "{refused}");
    assert!(
        refused["error"]
            .as_str()
            .unwrap()
            .contains("no connected worker"),
        "{refused}"
    );
    assert!(listed(addr).await.is_empty(), "a refusal changes nothing");

    // Forced, new runs wait for one.
    let path = "/v1/deployments/order/v1/activate";
    let (status, _) = http(addr, "POST", path, json!({"force": true})).await;
    assert_eq!(status, 200);
    assert_eq!(start(addr, "k").await, "v1");
    let mut late = Session::connect(addr, "v1").await;
    assert_eq!(late.take().await.run_id, "k");

    // The active build cannot be retired, and a workflow that is not remote has no builds.
    let (status, _) = http(addr, "POST", "/v1/deployments/order/v1/retire", json!({})).await;
    assert_eq!(status, 422);
    let path = "/v1/deployments/local/v1/activate";
    assert_eq!(
        http(addr, "POST", path, json!({"force": true})).await.0,
        404
    );
    let (status, _) = http(
        addr,
        "POST",
        "/v1/deployments/order/never/retire",
        json!({}),
    )
    .await;
    assert_eq!(status, 422);
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_sends_new_runs_back_and_leaves_pinned_runs_alone() {
    let server = serve("rollback").await;
    let addr = server.addr;
    let mut old = Session::connect(addr, "v1").await;
    let mut new = Session::connect(addr, "v2").await;
    assert_eq!(activate(addr, "v1").await.0, 200);
    assert_eq!(activate(addr, "v2").await.0, 200);
    assert_eq!(start(addr, "on-new").await, "v2");
    assert_eq!(new.take().await.run_id, "on-new");

    // v2 is bad: back to v1. What started on v2 stays there.
    assert_eq!(activate(addr, "v1").await.0, 200);
    assert_eq!(start(addr, "back").await, "v1");
    assert_eq!(old.take().await.run_id, "back");
    assert_eq!(
        listed(addr).await,
        [row("v1", "active", 1, 1), row("v2", "draining", 1, 1)]
    );
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_that_continues_as_new_moves_to_the_active_build() {
    let server = serve("continue").await;
    let addr = server.addr;
    let mut old = Session::connect(addr, "v1").await;
    assert_eq!(activate(addr, "v1").await.0, 200);
    start(addr, "long").await;
    let activation = old.take().await;
    let mut new = Session::connect(addr, "v2").await;
    assert_eq!(activate(addr, "v2").await.0, 200);

    let command = v1::workflow_command::Command::ContinueAsNew(v1::ContinueAsNew {
        input: b"2".to_vec(),
    });
    old.decide(&activation, command).await;
    let next = new.take().await;
    assert!(next.run_id.starts_with("long"), "{}", next.run_id);
    new.complete(&next, "v2").await;
    let finished = result(addr, "long").await;
    assert_eq!(finished["output"], "v2", "{finished}");
    assert_eq!(finished["version"], "v2", "{finished}");
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn runs_from_before_any_deployment_go_on_as_before() {
    let server = serve("unpinned").await;
    let addr = server.addr;
    let mut any = Session::connect(addr, "").await;
    assert_eq!(start(addr, "plain").await, "");
    let first = any.take().await;
    any.complete(&first, "any").await;
    assert_eq!(result(addr, "plain").await["output"], "any");

    // A worker that announces a build takes an unpinned run too: there is nobody else's code to protect.
    start(addr, "plain-2").await;
    drop(any);
    let mut built = Session::connect(addr, "v1").await;
    assert_eq!(built.take().await.run_id, "plain-2");
    let _ = std::fs::remove_dir_all(&server.dir);
}

async fn poll(addr: SocketAddr, build: Option<&str>) -> Vec<Value> {
    let mut body = json!({"worker": "w", "steps": ["charge"], "max": 8, "wait_ms": 200});
    if let Some(build) = build {
        body["deployment"] = json!(build);
    }
    let (status, polled) = http(addr, "POST", "/v1/workers/poll", body).await;
    assert_eq!(status, 200, "{polled}");
    polled["invocations"].as_array().unwrap().clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pinned_runs_steps_go_to_its_build() {
    let server = serve("steps").await;
    let addr = server.addr;
    let mut old = Session::connect(addr, "v1").await;
    assert_eq!(activate(addr, "v1").await.0, 200);
    start(addr, "k").await;
    let activation = old.take().await;
    let command = v1::workflow_command::Command::ScheduleStep(v1::ScheduleStep {
        name: "charge".to_owned(),
        input: b"1".to_vec(),
    });
    old.decide(&activation, command).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Another build's step worker is not sent it; its own is.
    assert!(
        poll(addr, Some("v2")).await.is_empty(),
        "another build's step"
    );
    let taken = poll(addr, Some("v1")).await;
    assert_eq!(taken.len(), 1, "{taken:?}");
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_that_announces_nothing_takes_any_step() {
    let server = serve("steps-any").await;
    let addr = server.addr;
    let mut old = Session::connect(addr, "v1").await;
    assert_eq!(activate(addr, "v1").await.0, 200);
    start(addr, "k").await;
    let activation = old.take().await;
    let command = v1::workflow_command::Command::ScheduleStep(v1::ScheduleStep {
        name: "charge".to_owned(),
        input: b"1".to_vec(),
    });
    old.decide(&activation, command).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(poll(addr, None).await.len(), 1);
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn deployments_survive_a_restart() {
    let dir = std::env::temp_dir().join(format!("aqueduct-deploy-restart-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    {
        let engine = open(&dir);
        engine.activate_deployment("order", "v1", true).unwrap();
        engine.activate_deployment("order", "v2", true).unwrap();
        engine
            .start_with(
                "old",
                "order",
                aqueduct_core::Payload::new(*b"1"),
                aqueduct_engine::StartOptions {
                    version: Some("v1".to_owned()),
                    ..aqueduct_engine::StartOptions::default()
                },
            )
            .unwrap();
    }
    let addr = listen(open(&dir), &dir).await;
    // The active build is still active, and the pinned run still goes to its own build.
    assert_eq!(start(addr, "new").await, "v2");
    let mut v2 = Session::connect(addr, "v2").await;
    assert_eq!(v2.take().await.run_id, "new");
    assert!(v2.next(QUIET).await.is_none(), "v1's run");
    let mut v1 = Session::connect(addr, "v1").await;
    assert_eq!(v1.take().await.run_id, "old");
    assert_eq!(
        listed(addr).await,
        [row("v1", "draining", 1, 1), row("v2", "active", 1, 1)]
    );
    // A draining build can still be named for a start, as before the restart.
    let body = json!({"key": "named", "workflow": "order", "input": 1, "version": "v1"});
    let (status, started) = http(addr, "POST", "/v1/runs", body).await;
    assert_eq!(
        (status, started["version"].as_str()),
        (200, Some("v1")),
        "{started}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn only_an_administrator_of_the_default_namespace_changes_deployments() {
    let dir = std::env::temp_dir().join(format!("aqueduct-deploy-auth-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let make = |name: &str, namespace: &str, permission: Permission| {
        create_token_in(&dir, name, [permission].into_iter().collect(), namespace).unwrap()
    };
    let reader = make("reader", "default", Permission::Read);
    let tenant = make("tenant", "acme", Permission::Admin);
    let ops = make("ops", "*", Permission::Admin);
    let addr = listen(open(&dir), &dir).await;
    let change = "/v1/deployments/order/v1/activate";
    let body = json!({"force": true});
    let (status, _) = http_as(addr, Some(&reader), "POST", change, body.clone()).await;
    assert_eq!(status, 403);
    let (status, _) = http_as(addr, Some(&tenant), "POST", change, body.clone()).await;
    assert_eq!(
        status, 403,
        "a tenant's administrator changes nothing for others"
    );
    let (status, _) = http_as(addr, Some(&tenant), "GET", "/v1/deployments", Value::Null).await;
    assert_eq!(status, 403);
    let (status, answer) = http_as(addr, Some(&ops), "POST", change, body).await;
    assert_eq!(status, 200, "{answer}");
    let audit = std::fs::read_to_string(dir.join("audit.log")).unwrap();
    assert!(audit.contains("activate deployment"), "{audit}");
    assert!(audit.contains("order v1"), "{audit}");
    let _ = std::fs::remove_dir_all(&dir);
}
