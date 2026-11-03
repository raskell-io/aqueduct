//! Step workers against a real server: over gRPC with this crate, and over HTTP long polling; a worker
//! that stops while it holds a step releases it to another.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aqueduct_engine::embedded::Engine;
use aqueduct_procedure::Procedure;
use aqueduct_worker::{Outcome, Worker};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PIPELINE: &str = r#"
def run(n):
    a = step("double", n)
    b, c = parallel([["square", a], ["double", a]])
    return step("sum", [b, c])
"#;

fn number(input: &[u8]) -> i64 {
    serde_json::from_slice::<Value>(input)
        .unwrap()
        .as_i64()
        .unwrap()
}

async fn post(addr: SocketAddr, path: &str, body: Value) -> Value {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = body.to_string();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap()
}

/// A worker over HTTP long polling that implements `sum`.
async fn summer(addr: SocketAddr) {
    loop {
        let polled = post(
            addr,
            "/v1/workers/poll",
            json!({"worker": "summer", "steps": ["sum"], "max": 4, "wait_ms": 200}),
        )
        .await;
        let mut results = Vec::new();
        for invocation in polled["invocations"].as_array().unwrap() {
            let sum: i64 = invocation["input"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_i64().unwrap())
                .sum();
            results.push(json!({"invocation_id": invocation["invocation_id"], "completed": sum}));
        }
        if !results.is_empty() {
            post(addr, "/v1/workers/results", json!({ "results": results })).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_workers_run_a_procedures_steps() {
    let dir = std::env::temp_dir().join(format!("aqueduct-workers-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Engine::builder(&dir)
        .workflow(
            "pipeline",
            Arc::new(Procedure::compile("pipeline.star", PIPELINE).unwrap()),
        )
        .remote_steps(Duration::from_secs(30))
        .open()
        .unwrap();
    let engine = Arc::new(engine);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(aqueduct_server::serve(
        Arc::clone(&engine),
        listener,
        std::future::pending(),
    ));
    let url = format!("http://{addr}");

    // A worker that takes one `square` and never answers it, then stops.
    let (took, took_inbox) = std::sync::mpsc::channel::<()>();
    let took = Mutex::new(took);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    // The stuck step waits until the test ends (when `unstick` is dropped), so the runtime can stop.
    let (unstick, stuck_until) = std::sync::mpsc::channel::<()>();
    let stuck_until = Mutex::new(stuck_until);
    let stuck = Worker::new("stuck").capacity(1).step("square", move |_| {
        let _ = took.lock().unwrap().send(());
        let _ = stuck_until.lock().unwrap().recv();
        Outcome::Retry("never".into())
    });
    let stuck_url = url.clone();
    let stuck = tokio::spawn(async move {
        stuck
            .run_until(&stuck_url, async {
                let _ = stopped.await;
            })
            .await
    });

    for n in 1..=3 {
        let input = aqueduct_core::Payload::new(n.to_string().into_bytes());
        let engine = Arc::clone(&engine);
        tokio::task::spawn_blocking(move || engine.start(&format!("p-{n}"), "pipeline", input))
            .await
            .unwrap()
            .unwrap();
    }
    tokio::spawn(summer(addr));
    // `double` only, until the stuck worker holds a `square`.
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&attempts);
    let healthy = Worker::new("healthy")
        .step("double", |i| {
            Outcome::Completed((number(&i.input) * 2).to_string().into_bytes())
        })
        .step("square", move |i| {
            seen.lock().unwrap().push(i.attempt);
            Outcome::Completed(number(&i.input).pow(2).to_string().into_bytes())
        });
    let doubler = Worker::new("doubler").step("double", |i| {
        Outcome::Completed((number(&i.input) * 2).to_string().into_bytes())
    });
    let doubler_url = url.clone();
    let (stop_doubler, doubler_stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        doubler
            .run_until(&doubler_url, async {
                let _ = doubler_stopped.await;
            })
            .await
    });
    tokio::task::spawn_blocking(move || took_inbox.recv_timeout(Duration::from_secs(10)))
        .await
        .unwrap()
        .unwrap();
    let _ = stop_doubler.send(());
    // The stuck worker stops: its `square` goes to the healthy worker, as a second attempt.
    let _ = stop.send(());
    stuck.await.unwrap().unwrap();
    let healthy_url = url.clone();
    tokio::spawn(async move { healthy.run(&healthy_url).await });

    for n in 1..=3i64 {
        let engine = Arc::clone(&engine);
        let result = tokio::task::spawn_blocking(move || {
            engine.wait(&format!("p-{n}"), Duration::from_secs(20))
        })
        .await
        .unwrap()
        .unwrap();
        let a = n * 2;
        assert_eq!(
            result.output.unwrap().as_bytes(),
            (a * a + a * 2).to_string().as_bytes(),
            "p-{n}"
        );
    }
    assert!(
        attempts.lock().unwrap().contains(&2),
        "{:?}",
        attempts.lock().unwrap()
    );
    assert!(engine.step_queue().unwrap().is_empty());
    drop(unstick);
    let _ = std::fs::remove_dir_all(dir);
}
