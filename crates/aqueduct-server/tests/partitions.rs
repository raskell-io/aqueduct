//! A server with many partitions (ADR 0057): runs spread over them by key, each answered by its own
//! partition, and listed together.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::sync::Arc;

use aqueduct_engine::embedded::Engine;
use aqueduct_engine::shards::Shards;
use aqueduct_procedure::Procedure;
use aqueduct_server::{Config, Security};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const DOUBLE: &str = "def run(x):\n    return x * 2\n";

async fn http(addr: SocketAddr, method: &str, path: &str, body: Value) -> (u16, Value) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = body.to_string();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let text = response.split_once("\r\n\r\n").map_or("", |(_, t)| t);
    (
        response[9..12].parse().unwrap(),
        serde_json::from_str(text).unwrap_or(Value::Null),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn runs_spread_over_partitions_and_are_listed_together() {
    let dir = std::env::temp_dir().join(format!("aqueduct-partitions-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config: Config = serde_json::from_value(json!({"partitions": 4})).unwrap();
    config.check().unwrap();
    let shards = Shards::open(&dir, config.partition_count(), |path, _| {
        config
            .apply(Engine::builder(path).workflow(
                "double",
                Arc::new(Procedure::compile("double.star", DOUBLE).unwrap()),
            ))
            .map_err(aqueduct_engine::embedded::EngineError::Open)
    })
    .unwrap();
    let homes: std::collections::BTreeSet<u32> = (0..12)
        .map(|i| shards.table().partition_of(&format!("k{i}")))
        .collect();
    assert!(homes.len() > 1, "the keys should be spread");
    let security = Security::for_dir(&dir).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        aqueduct_server::serve_shards(
            Arc::new(shards),
            listener,
            &config,
            security,
            None,
            std::future::pending(),
        )
        .await
    });

    for i in 0..12 {
        let (status, answer) = http(
            addr,
            "POST",
            "/v1/runs",
            json!({"key": format!("k{i}"), "workflow": "double", "input": i}),
        )
        .await;
        assert!((200..300).contains(&status), "{status}: {answer}");
    }
    for i in 0..12 {
        let (status, answer) =
            http(addr, "GET", &format!("/v1/runs/k{i}/result"), Value::Null).await;
        assert_eq!(
            (status, &answer["output"]),
            (200, &json!(i * 2)),
            "{answer}"
        );
    }
    let (status, listed) = http(addr, "GET", "/v1/runs", Value::Null).await;
    assert_eq!(status, 200, "{listed}");
    assert_eq!(listed["runs"].as_array().unwrap().len(), 12, "{listed}");
    let _ = std::fs::remove_dir_all(&dir);
}
