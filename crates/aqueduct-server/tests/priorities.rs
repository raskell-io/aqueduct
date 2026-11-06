//! Priorities (ADR 0049) on the real server: urgent runs' steps are taken first, a priority can change
//! while work waits, children and successors inherit it, and it survives a restart.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use aqueduct_engine::embedded::Engine;
use aqueduct_procedure::Procedure;
use aqueduct_server::auth::create_token_in;
use aqueduct_server::{Config, Permission, Security};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const JOB: &str = "def run(x):\n    return step(\"work\", x)\n";
const BOSS: &str = "def run(x):\n    return call(\"job\", x)\n";
const LOOPS: &str = r#"
def run(n):
    if n > 0:
        continue_as_new(n - 1)
    return step("work", n)
"#;

fn open(dir: &Path) -> Arc<Engine> {
    let mut builder = Engine::builder(dir).remote_steps(Duration::from_secs(60));
    for (name, source) in [("job", JOB), ("boss", BOSS), ("loops", LOOPS)] {
        builder = builder.workflow(
            name,
            Arc::new(Procedure::compile("p.star", source).unwrap()),
        );
    }
    Arc::new(builder.open().unwrap())
}

async fn listen(dir: &Path) -> SocketAddr {
    let engine = open(dir);
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

fn fresh(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("aqueduct-prio-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
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

async fn start(addr: SocketAddr, key: &str, workflow: &str, priority: Option<u32>) {
    start_with(addr, key, workflow, json!(key), priority).await;
}

async fn start_with(
    addr: SocketAddr,
    key: &str,
    workflow: &str,
    input: Value,
    priority: Option<u32>,
) {
    let mut body = json!({"key": key, "workflow": workflow, "input": input});
    if let Some(priority) = priority {
        body["priority"] = json!(priority);
    }
    let (status, answer) = http(addr, "POST", "/v1/runs", body).await;
    assert_eq!(status, 200, "{answer}");
}

async fn priority(addr: SocketAddr, key: &str) -> u64 {
    let (status, run) = http(addr, "GET", &format!("/v1/runs/{key}"), Value::Null).await;
    assert_eq!(status, 200, "{run}");
    run["priority"].as_u64().unwrap()
}

/// The runs whose steps a worker is given, one at a time, answering each.
async fn taken(addr: SocketAddr, count: usize) -> Vec<String> {
    let mut runs = Vec::new();
    while runs.len() < count {
        let body = json!({"worker": "w", "steps": ["work"], "max": 1, "wait_ms": 2000});
        let (status, polled) = http(addr, "POST", "/v1/workers/poll", body).await;
        assert_eq!(status, 200, "{polled}");
        for invocation in polled["invocations"].as_array().unwrap() {
            runs.push(invocation["run_id"].as_str().unwrap().to_owned());
            let answer = json!({"results": [
                {"invocation_id": invocation["invocation_id"], "completed": invocation["input"]}
            ]});
            http(addr, "POST", "/v1/workers/results", answer).await;
        }
    }
    runs
}

/// Lets the starts' first steps reach the queue.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn urgent_runs_steps_are_taken_first() {
    let dir = fresh("order");
    let addr = listen(&dir).await;
    start(addr, "routine-1", "job", None).await;
    start(addr, "urgent", "job", Some(5)).await;
    start(addr, "routine-2", "job", Some(0)).await;
    start(addr, "soon", "job", Some(2)).await;
    settle().await;
    assert_eq!(
        taken(addr, 4).await,
        ["urgent", "soon", "routine-1", "routine-2"]
    );
    assert_eq!(priority(addr, "urgent").await, 5);
    assert_eq!(priority(addr, "routine-1").await, 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_priority_changes_while_work_waits() {
    let dir = fresh("change");
    let addr = listen(&dir).await;
    start(addr, "first", "job", Some(1)).await;
    start(addr, "second", "job", Some(1)).await;
    settle().await;
    let (status, answer) = http(
        addr,
        "POST",
        "/v1/runs/second/priority",
        json!({"priority": 4}),
    )
    .await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(priority(addr, "second").await, 4);
    assert_eq!(taken(addr, 2).await, ["second", "first"]);

    // A start retried with a priority sets it on the run it finds: one lost before it was durable
    // converges.
    start(addr, "retried", "job", None).await;
    start(addr, "retried", "job", Some(6)).await;
    assert_eq!(priority(addr, "retried").await, 6);

    // A run that ended keeps what it had; an unknown key is no run.
    taken(addr, 1).await;
    let wait = "/v1/runs/retried/result?timeout_ms=5000";
    assert_eq!(
        http(addr, "GET", wait, Value::Null).await.1["status"],
        "completed"
    );
    let (status, _) = http(
        addr,
        "POST",
        "/v1/runs/retried/priority",
        json!({"priority": 1}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(priority(addr, "retried").await, 6);
    let (status, _) = http(
        addr,
        "POST",
        "/v1/runs/nobody/priority",
        json!({"priority": 1}),
    )
    .await;
    assert_eq!(status, 404);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn children_and_successors_inherit_it() {
    let dir = fresh("inherit");
    let addr = listen(&dir).await;
    start(addr, "boss", "boss", Some(7)).await;
    start_with(addr, "looping", "loops", json!(2), Some(3)).await;
    settle().await;
    let (_, runs) = http(addr, "GET", "/v1/runs", Value::Null).await;
    let mut seen = Vec::new();
    for run in runs["runs"].as_array().unwrap() {
        let id = run["run_id"].as_str().unwrap();
        let path = format!("/v1/runs/{}", id.replace('#', "%23").replace('\0', "%00"));
        let (status, described) = http(addr, "GET", &path, Value::Null).await;
        if status == 200 {
            seen.push((
                run["workflow"].as_str().unwrap().to_owned(),
                described["priority"].clone(),
            ));
        }
    }
    assert!(
        seen.contains(&("job".to_owned(), json!(7))),
        "the child: {seen:?}"
    );
    assert!(seen.contains(&("boss".to_owned(), json!(7))), "{seen:?}");
    // The key's latest run is the successor, two generations on.
    let (_, latest) = http(addr, "GET", "/v1/runs/looping", Value::Null).await;
    assert_eq!(latest["status"], "running", "{latest}");
    assert!(
        latest["run_id"].as_str().unwrap().contains('\0'),
        "not a successor: {latest}"
    );
    assert_eq!(latest["priority"], 3, "{latest}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_priority_survives_a_restart() {
    let dir = fresh("restart");
    {
        let engine = open(&dir);
        let options = |priority| aqueduct_engine::StartOptions {
            priority: Some(priority),
            ..aqueduct_engine::StartOptions::default()
        };
        let input = || aqueduct_core::Payload::new(*b"1");
        engine
            .start_with("low", "job", input(), options(0))
            .unwrap();
        engine
            .start_with("high", "job", input(), options(8))
            .unwrap();
        engine.set_priority("low", 9).unwrap();
    }
    // The queue is rebuilt from the journal: the run raised last is still first.
    let addr = listen(&dir).await;
    settle().await;
    assert_eq!(taken(addr, 2).await, ["low", "high"]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_priority_is_set_only_by_who_may_start_and_only_in_its_namespace() {
    let dir = fresh("auth");
    let make = |name: &str, namespace: &str, permissions: &[Permission]| {
        create_token_in(&dir, name, permissions.iter().copied().collect(), namespace).unwrap()
    };
    let acme = make("acme", "acme", &[Permission::Start, Permission::Read]);
    let reader = make("reader", "acme", &[Permission::Read]);
    let globex = make("globex", "globex", &[Permission::Start]);
    let addr = listen(&dir).await;
    let body = json!({"key": "k", "workflow": "job", "input": 1});
    assert_eq!(
        http_as(addr, Some(&acme), "POST", "/v1/runs", body).await.0,
        200
    );
    let set = "/v1/runs/k/priority";
    let raise = json!({"priority": 5});
    assert_eq!(
        http_as(addr, Some(&reader), "POST", set, raise.clone())
            .await
            .0,
        403
    );
    assert_eq!(
        http_as(addr, Some(&globex), "POST", set, raise.clone())
            .await
            .0,
        404,
        "another tenant's run is no run"
    );
    assert_eq!(http_as(addr, Some(&acme), "POST", set, raise).await.0, 200);
    let audit = std::fs::read_to_string(dir.join("audit.log")).unwrap();
    let changed = audit.lines().any(|line| {
        line.contains(r#""action":"set priority""#)
            && line.contains(r#""target":"acme: k to 5""#)
            && line.contains(r#""outcome":"ok""#)
    });
    assert!(changed, "{audit}");
    let _ = std::fs::remove_dir_all(&dir);
}
