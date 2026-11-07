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
            let (status, answer) = http(*addr, method, path, body.clone()).await;
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

#[tokio::test(flavor = "multi_thread")]
async fn three_servers_replicate_and_survive_their_leader() {
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
    for (id, listener) in listeners {
        let dir =
            std::env::temp_dir().join(format!("aqueduct-cluster-{id}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let config: Config = serde_json::from_value(json!({
            "cluster": {"id": id, "peers": peers, "secret": "a secret for the test", "tick_ms": 20}
        }))
        .unwrap();
        config.check().unwrap();
        let builder = Engine::builder(&dir).workflow(
            "double",
            Arc::new(Procedure::compile("double.star", DOUBLE).unwrap()),
        );
        let engine = Arc::new(config.apply(builder).unwrap().open().unwrap());
        let security = Security::for_dir(&dir).unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            aqueduct_server::serve_with(engine, listener, &config, security, async {
                let _ = stopped.await;
            })
            .await
        });
        stops.insert(id, stop);
        dirs.push(dir);
    }
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
    let follower = *nodes.keys().find(|id| **id != leader).unwrap();
    let (status, answer) = http(
        nodes[&follower],
        "POST",
        "/v1/runs",
        json!({"key": "b", "workflow": "double", "input": 4}),
    )
    .await;
    assert!((200..300).contains(&status), "{status}: {answer}");
    let (status, answer) = http(nodes[&follower], "GET", "/v1/runs/b/result", Value::Null).await;
    assert_eq!((status, &answer["output"]), (200, &json!(8)), "{answer}");
    // A request already forwarded once is answered where it arrives: unavailable, never forwarded on.
    let (status, answer) = http_with(
        nodes[&follower],
        "GET",
        "/v1/runs/b/result",
        Value::Null,
        "aqueduct-forwarded: 1\r\n",
    )
    .await;
    assert_eq!(status, 503, "{answer}");

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
