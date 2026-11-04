//! Virtual objects over the network (ADR 0043, ADR 0044): a server with objects in its configuration, a
//! handler in a worker, called over HTTP and gRPC, whose message reaches another object.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use aqueduct_engine::embedded::Engine;
use aqueduct_protocol::v1;
use aqueduct_protocol::v1::ingress_client::IngressClient;
use aqueduct_sdk::{ObjectContext, WorkflowError, object_handler};
use aqueduct_server::{Config, Security};
use aqueduct_worker::Worker;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn deposit(ctx: ObjectContext, amount: i64) -> Result<i64, WorkflowError> {
    let balance = ctx.get::<i64>("balance")?.unwrap_or_default() + amount;
    if balance < 0 {
        // Its write and its message are dropped with it.
        ctx.send("ledger", "main", "record", &amount)?;
        return Err(WorkflowError::failed("insufficient funds"));
    }
    ctx.set("balance", &balance)?;
    ctx.send("ledger", "main", "record", &amount)?;
    Ok(balance)
}

async fn record(ctx: ObjectContext, amount: i64) -> Result<usize, WorkflowError> {
    let mut entries: Vec<i64> = ctx.get("entries")?.unwrap_or_default();
    entries.push(amount);
    ctx.set("entries", &entries)?;
    Ok(entries.len())
}

async fn http(addr: SocketAddr, method: &str, path: &str, body: Value) -> (u16, Value) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = body.to_string();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let text = response.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    (
        response[9..12].parse().unwrap(),
        serde_json::from_str(text).unwrap_or(Value::Null),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn objects_are_served_over_the_network() {
    let dir = std::env::temp_dir().join(format!("aqueduct-net-objects-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config: Config = serde_json::from_value(json!({
        "objects": ["account", "ledger"],
        "remote_workflows": ["account.deposit", "ledger.record"],
    }))
    .unwrap();
    let engine = Arc::new(config.apply(Engine::builder(&dir)).unwrap().open().unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    {
        let (engine, config) = (Arc::clone(&engine), config.clone());
        tokio::spawn(async move {
            aqueduct_server::serve_with(
                engine,
                listener,
                &config,
                Security::none(),
                std::future::pending(),
            )
            .await
        });
    }
    let url = format!("http://{addr}");
    let worker = Worker::new("bank")
        .workflow("account.deposit", Arc::new(object_handler(deposit)))
        .workflow("ledger.record", Arc::new(object_handler(record)));
    let worker_url = url.clone();
    tokio::spawn(async move { worker.run(&worker_url).await });

    // Over HTTP.
    let (status, answer) = http(
        addr,
        "POST",
        "/v1/objects/account/ada/deposit",
        json!({"args": 10, "id": "d1", "timeout_ms": 10_000}),
    )
    .await;
    assert_eq!((status, &answer["output"]), (200, &json!(10)), "{answer}");
    // The same id: not run again.
    let (_, answer) = http(
        addr,
        "POST",
        "/v1/objects/account/ada/deposit",
        json!({"args": 10, "id": "d1", "timeout_ms": 10_000}),
    )
    .await;
    assert_eq!(answer["output"], json!(10));
    // A failed handler: 422, nothing committed, nothing sent.
    let (status, answer) = http(
        addr,
        "POST",
        "/v1/objects/account/ada/deposit",
        json!({"args": -100, "id": "d2", "timeout_ms": 10_000}),
    )
    .await;
    assert_eq!(status, 422, "{answer}");
    assert!(answer["error"].as_str().unwrap().contains("insufficient"));
    let (_, answer) = http(addr, "GET", "/v1/objects/account/ada", json!({})).await;
    assert_eq!(answer["state"], json!({"balance": 10}));
    let (status, _) = http(
        addr,
        "POST",
        "/v1/objects/account/ada/nope",
        json!({"args": 1}),
    )
    .await;
    assert_eq!(status, 404);

    // Over gRPC.
    let mut client = IngressClient::connect(url.clone()).await.unwrap();
    let output = client
        .call_object(v1::CallObjectRequest {
            object: "account".into(),
            key: "ada".into(),
            handler: "deposit".into(),
            id: "d3".into(),
            args: b"5".to_vec(),
            timeout_ms: 10_000,
        })
        .await
        .unwrap()
        .into_inner()
        .output;
    assert_eq!(output, b"15");

    // The messages of the two committed deposits reach the ledger, and the failed one's does not.
    for _ in 0..1_000 {
        let (_, answer) = http(addr, "GET", "/v1/objects/ledger/main", json!({})).await;
        if answer["state"]["entries"]
            .as_array()
            .is_some_and(|e| e.len() == 2)
        {
            assert_eq!(answer["state"]["entries"], json!([10, 5]));
            let _ = std::fs::remove_dir_all(dir);
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the ledger never got its two messages");
}
