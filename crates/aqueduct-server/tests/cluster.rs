//! Cluster mode (ADR 0056): three servers on this machine, each a replica of one partition, their Raft
//! messages over HTTP. A client asks any of them; those that do not lead forward to the one that does.
//! The leader's server stops, and another serves what it acknowledged.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aqueduct_engine::embedded::Engine;
use aqueduct_procedure::Procedure;
use aqueduct_server::{Config, Security};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const DOUBLE: &str = "def run(x):\n    return x * 2\n";

async fn http(addr: SocketAddr, method: &str, path: &str, body: Value) -> (u16, Value) {
    http_with(addr, method, path, body, "").await
}

async fn http_with(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Value,
    headers: &str,
) -> (u16, Value) {
    let Ok(mut stream) = tokio::net::TcpStream::connect(addr).await else {
        return (0, Value::Null);
    };
    let body = body.to_string();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    if stream.write_all(request.as_bytes()).await.is_err() {
        return (0, Value::Null);
    }
    let mut response = String::new();
    if stream.read_to_string(&mut response).await.is_err() || response.len() < 12 {
        return (0, Value::Null);
    }
    let text = response.split_once("\r\n\r\n").map_or("", |(_, t)| t);
    (
        response[9..12].parse().unwrap(),
        serde_json::from_str(text).unwrap_or(Value::Null),
    )
}

/// Asks every live node until one answers other than *unavailable*.
async fn ask(
    nodes: &BTreeMap<u64, SocketAddr>,
    method: &str,
    path: &str,
    body: Value,
) -> (u64, Value) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        for (id, addr) in nodes {
            let began = Instant::now();
            let (status, answer) = http(*addr, method, path, body.clone()).await;
            if began.elapsed() > Duration::from_secs(5) {
                eprintln!(
                    "SLOW {method} {path} at node {id}: {:?} -> {status} {answer}",
                    began.elapsed()
                );
            }
            if (200..300).contains(&status) {
                return (*id, answer);
            }
            assert!(
                status == 503 || status == 0,
                "node {id} answered {status}: {answer}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no node answered {method} {path} within 30 s");
}

const PARENT: &str = r#"
def run(n):
    shared = call("double", n % 2, key = "shared-" + str(n % 2))
    send("counter", "c" + str(n % 3), "add", 1)
    return shared + n
"#;

const ADD: &str = r#"
def run(call):
    n = call["state"].get("n", 0) + call["args"]
    return {"output": n, "writes": {"n": n}}
"#;

struct Servers {
    addrs: BTreeMap<u64, SocketAddr>,
    shards: BTreeMap<u64, Arc<aqueduct_engine::shards::Shards>>,
    stops: BTreeMap<u64, tokio::sync::oneshot::Sender<()>>,
    dirs: Vec<std::path::PathBuf>,
}

/// Three servers on this machine, each with `partitions` partitions replicated with the others.
async fn servers(name: &str, partitions: u32) -> Servers {
    let mut listeners = BTreeMap::new();
    for id in 1..=3u64 {
        listeners.insert(
            id,
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        );
    }
    let addrs: BTreeMap<u64, SocketAddr> = listeners
        .iter()
        .map(|(id, l)| (*id, l.local_addr().unwrap()))
        .collect();
    let peers: BTreeMap<String, String> = addrs
        .iter()
        .map(|(id, addr)| (id.to_string(), format!("http://{addr}")))
        .collect();
    let mut stops = BTreeMap::new();
    let mut dirs = Vec::new();
    let mut all = BTreeMap::new();
    for (id, listener) in listeners {
        let dir = std::env::temp_dir().join(format!(
            "aqueduct-cluster-{name}-{id}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let config: Config = serde_json::from_value(json!({
            "partitions": partitions,
            "objects": ["counter"],
            "cluster": {"id": id, "peers": peers, "secret": "a secret for the test", "tick_ms": 20}
        }))
        .unwrap();
        config.check().unwrap();
        let cluster = config.cluster.clone().unwrap();
        let shards = cluster
            .open(&dir, partitions, |path, _| {
                config
                    .apply(
                        Engine::builder(path)
                            .workflow(
                                "double",
                                Arc::new(Procedure::compile("double.star", DOUBLE).unwrap()),
                            )
                            .workflow(
                                "parent",
                                Arc::new(Procedure::compile("parent.star", PARENT).unwrap()),
                            )
                            .object_handler(
                                "counter",
                                "add",
                                Arc::new(Procedure::compile("add.star", ADD).unwrap()),
                            ),
                    )
                    .map_err(aqueduct_engine::embedded::EngineError::Open)
            })
            .unwrap();
        let security = Security::for_dir(&dir).unwrap();
        all.insert(id, Arc::clone(&shards));
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            aqueduct_server::serve_shards(shards, listener, &config, security, None, async {
                let _ = stopped.await;
            })
            .await
        });
        stops.insert(id, stop);
        dirs.push(dir);
    }
    Servers {
        addrs,
        shards: all,
        stops,
        dirs,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn three_servers_replicate_and_survive_their_leader() {
    let Servers {
        addrs,
        mut stops,
        dirs,
        ..
    } = servers("one", 1).await;
    let mut nodes = addrs.clone();

    let (leader, _) = ask(
        &nodes,
        "POST",
        "/v1/runs",
        json!({"key": "a", "workflow": "double", "input": 21}),
    )
    .await;
    let (_, result) = ask(&nodes, "GET", "/v1/runs/a/result", Value::Null).await;
    assert_eq!(result["output"], json!(42), "{result}");
    // Raft messages without the cluster's secret are refused.
    let (status, _) = http(nodes[&leader], "POST", "/cluster/v1/raft", Value::Null).await;
    assert_eq!(status, 401);
    // A node that does not lead forwards to the one that does: any node can be asked.
    // (A client retries an unavailable answer: leadership may be moving to the preferred node.)
    let follower = *nodes.keys().find(|id| **id != leader).unwrap();
    let only = BTreeMap::from([(follower, nodes[&follower])]);
    ask(
        &only,
        "POST",
        "/v1/runs",
        json!({"key": "b", "workflow": "double", "input": 4}),
    )
    .await;
    let (_, answer) = ask(&only, "GET", "/v1/runs/b/result", Value::Null).await;
    assert_eq!(answer["output"], json!(8), "{answer}");
    // A request already forwarded once is answered where it arrives, never forwarded on: the leader
    // answers it, the others are unavailable. (Leadership may move meanwhile: each node is asked.)
    let mut refused = 0;
    for addr in nodes.values() {
        let (status, answer) = http_with(
            *addr,
            "GET",
            "/v1/runs/b/result",
            Value::Null,
            "aqueduct-forwarded: 1\r\n",
        )
        .await;
        assert!(status == 200 || status == 503, "{status}: {answer}");
        refused += usize::from(status == 503);
    }
    assert!(
        refused >= 1,
        "every node answered a request already forwarded"
    );

    // The leader's server stops; another serves what it acknowledged, and new work.
    let _ = stops.remove(&leader).unwrap().send(());
    nodes.remove(&leader);
    let (_, result) = ask(&nodes, "GET", "/v1/runs/a/result", Value::Null).await;
    assert_eq!(result["output"], json!(42), "{result}");
    ask(
        &nodes,
        "POST",
        "/v1/runs",
        json!({"key": "c", "workflow": "double", "input": 50}),
    )
    .await;
    let (_, result) = ask(&nodes, "GET", "/v1/runs/c/result", Value::Null).await;
    assert_eq!(result["output"], json!(100), "{result}");

    for stop in stops.into_values() {
        let _ = stop.send(());
    }
    for dir in dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn partitions_replicate_on_three_servers_and_send_each_other_work() {
    let Servers {
        addrs,
        mut stops,
        dirs,
        ..
    } = servers("two", 2).await;
    let mut nodes = addrs.clone();
    for i in 0..8 {
        ask(
            &nodes,
            "POST",
            "/v1/runs",
            json!({"key": format!("p{i}"), "workflow": "parent", "input": i}),
        )
        .await;
    }
    for i in 0..8 {
        let (_, result) = ask(&nodes, "GET", &format!("/v1/runs/p{i}/result"), Value::Null).await;
        assert_eq!(result["output"], json!((i % 2) * 2 + i), "p{i}: {result}");
    }
    // One server stops; the others still have every partition, and every message counted once.
    let first = *nodes.keys().next().unwrap();
    let _ = stops.remove(&first).unwrap().send(());
    nodes.remove(&first);
    for k in 0..3 {
        let sent = (0..8).filter(|i| i % 3 == k).count();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let (_, state) = ask(
                &nodes,
                "GET",
                &format!("/v1/objects/counter/c{k}"),
                Value::Null,
            )
            .await;
            let n = state["state"]["n"]
                .as_u64()
                .or_else(|| state["n"].as_u64())
                .unwrap_or(0);
            if n == sent as u64 {
                break;
            }
            assert!(n < sent as u64, "counter c{k}: {state}");
            assert!(Instant::now() < deadline, "counter c{k}: {state}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    for stop in stops.into_values() {
        let _ = stop.send(());
    }
    for dir in dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_hands_over_before_it_stops_and_other_versions_are_refused() {
    let Servers {
        addrs,
        shards,
        stops,
        dirs,
    } = servers("drain", 1).await;
    ask(
        &addrs,
        "POST",
        "/v1/runs",
        json!({"key": "a", "workflow": "double", "input": 1}),
    )
    .await;
    let leading = |shards: &BTreeMap<u64, Arc<aqueduct_engine::shards::Shards>>| {
        shards
            .iter()
            .find(|(_, s)| s.engines()[0].leads())
            .map(|(id, _)| *id)
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    let leader = loop {
        if let Some(leader) = leading(&shards) {
            break leader;
        }
        assert!(Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // The leader's server hands over: another leads before any election would have.
    let draining = Arc::clone(&shards[&leader]);
    let began = Instant::now();
    let left = tokio::task::spawn_blocking(move || draining.step_down(Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(left, 0);
    let deadline = Instant::now() + Duration::from_secs(5);
    let next = loop {
        if let Some(next) = leading(&shards).filter(|id| *id != leader) {
            break next;
        }
        assert!(Instant::now() < deadline, "no one took over");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_ne!(next, leader);
    assert!(
        began.elapsed() < Duration::from_secs(2),
        "{:?}",
        began.elapsed()
    );
    let (_, result) = ask(&addrs, "GET", "/v1/runs/a/result", Value::Null).await;
    assert_eq!(result["output"], json!(2), "{result}");
    // A server of an incompatible version is refused.
    let (status, _) = http_with(
        addrs[&leader],
        "POST",
        "/cluster/v1/raft",
        Value::Null,
        "aqueduct-cluster-secret: a secret for the test\r\naqueduct-version: 9.0.0\r\n",
    )
    .await;
    assert_eq!(status, 409);
    for stop in stops.into_values() {
        let _ = stop.send(());
    }
    for dir in dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}
