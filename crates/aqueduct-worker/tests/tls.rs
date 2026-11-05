//! TLS on the server (ADR 0042): a worker that trusts the certificate is served; one that does not, and
//! a plain-text client, are not; and a client that never finishes its handshake holds nobody up.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use aqueduct_engine::embedded::Engine;
use aqueduct_procedure::Procedure;
use aqueduct_server::{Config, Security, Tls};
use aqueduct_worker::{Outcome, Worker};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const FLOW: &str = "def run(x):\n    return step(\"echo\", x)\n";

fn echo(name: &str) -> Worker {
    Worker::new(name).step("echo", |i| Outcome::Completed(i.input.clone()))
}

#[tokio::test(flavor = "multi_thread")]
async fn only_a_client_that_trusts_the_certificate_is_served() {
    let dir = std::env::temp_dir().join(format!("aqueduct-tls-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Arc::new(
        Engine::builder(&dir)
            .workflow(
                "flow",
                Arc::new(Procedure::compile("flow.star", FLOW).unwrap()),
            )
            .remote_steps(Duration::from_secs(30))
            .open()
            .unwrap(),
    );
    let issued = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()]).unwrap();
    let certificate = issued.cert.pem();
    let tls = Tls::from_pem(&certificate, &issued.signing_key.serialize_pem()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    {
        let engine = Arc::clone(&engine);
        tokio::spawn(async move {
            aqueduct_server::serve_tls(
                engine,
                listener,
                &Config::default(),
                Security::none(),
                tls,
                std::future::pending(),
            )
            .await
        });
    }
    let url = format!("https://{addr}");

    // A client that opens a connection and never starts its handshake.
    let _idle = tokio::net::TcpStream::connect(addr).await.unwrap();

    // Plain HTTP on the TLS port gets no HTTP answer.
    let mut plain = tokio::net::TcpStream::connect(addr).await.unwrap();
    plain
        .write_all(b"GET /v1/runs HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut answer = Vec::new();
    let _ = plain.read_to_end(&mut answer).await;
    assert!(!answer.starts_with(b"HTTP/"), "{answer:?}");

    // A worker that does not trust the certificate does not connect.
    let refused = echo("untrusting").run(&url).await;
    assert!(refused.is_err(), "{refused:?}");

    // One that does runs the step, past the idle connection.
    let trusting = echo("trusting").ca_certificate(&certificate);
    let worker_url = url.clone();
    tokio::spawn(async move { trusting.run(&worker_url).await });
    let waiting = Arc::clone(&engine);
    let result = tokio::task::spawn_blocking(move || {
        waiting
            .start("t-1", "flow", aqueduct_core::Payload::new(*b"7"))
            .unwrap();
        waiting.wait("t-1", Duration::from_secs(20))
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.output.unwrap().as_bytes(), b"7");
    let _ = std::fs::remove_dir_all(dir);
}

/// Over the TLS listener's connection limit, a new connection waits until a place is free.
#[tokio::test(flavor = "multi_thread")]
async fn the_connection_limit_holds_over_tls() {
    let dir = std::env::temp_dir().join(format!("aqueduct-tls-limit-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Arc::new(
        Engine::builder(&dir)
            .workflow(
                "flow",
                Arc::new(Procedure::compile("flow.star", FLOW).unwrap()),
            )
            .remote_steps(Duration::from_secs(30))
            .open()
            .unwrap(),
    );
    let issued = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()]).unwrap();
    let certificate = issued.cert.pem();
    let tls = Tls::from_pem(&certificate, &issued.signing_key.serialize_pem()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut config = Config::default();
    config.limits.max_connections = 1;
    {
        let engine = Arc::clone(&engine);
        tokio::spawn(async move {
            aqueduct_server::serve_tls(
                engine,
                listener,
                &config,
                Security::none(),
                tls,
                std::future::pending(),
            )
            .await
        });
    }
    let idle = tokio::net::TcpStream::connect(addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let url = format!("https://{addr}");
    let worker = echo("waiting").ca_certificate(&certificate);
    let running = tokio::spawn(async move { worker.run(&url).await });
    let waiting = Arc::clone(&engine);
    let result = tokio::task::spawn_blocking(move || {
        waiting
            .start("l-1", "flow", aqueduct_core::Payload::new(*b"1"))
            .unwrap();
        waiting.wait("l-1", Duration::from_millis(500))
    })
    .await
    .unwrap();
    assert!(result.is_err(), "served over the limit: {result:?}");
    // A place comes free: the worker's connection is taken, and the step runs.
    drop(idle);
    let waiting = Arc::clone(&engine);
    let result = tokio::task::spawn_blocking(move || waiting.wait("l-1", Duration::from_secs(20)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.output.unwrap().as_bytes(), b"1");
    running.abort();
    let _ = std::fs::remove_dir_all(dir);
}
