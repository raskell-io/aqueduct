//! The web console (ADR 0059): its page is served without a token, locked down by its headers; what it
//! shows still needs one.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use aqueduct_engine::embedded::Engine;
use aqueduct_server::auth::create_token;
use aqueduct_server::{Config, Permission, Security};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn get(addr: std::net::SocketAddr, path: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

#[tokio::test(flavor = "multi_thread")]
async fn the_page_is_served_and_locked_down_and_the_api_still_needs_a_token() {
    let dir = std::env::temp_dir().join(format!("aqueduct-console-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Arc::new(Engine::builder(&dir).open().unwrap());
    create_token(&dir, "reader", [Permission::Read].into_iter().collect()).unwrap();
    let security = Security::for_dir(&dir).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
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
    let page = get(addr, "/console").await;
    assert!(page.starts_with("HTTP/1.1 200"), "{page}");
    let head = page.split_once("\r\n\r\n").unwrap().0.to_lowercase();
    assert!(head.contains("content-type: text/html"), "{head}");
    assert!(
        head.contains("content-security-policy: default-src 'none'"),
        "{head}"
    );
    assert!(head.contains("frame-ancestors 'none'"), "{head}");
    assert!(page.contains("<title>Aqueduct console</title>"));
    // The page inserts what the server says as text only.
    assert!(!page.contains("innerHTML"));
    let api = get(addr, "/v1/runs").await;
    assert!(api.starts_with("HTTP/1.1 401"), "{api}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// For a look in a browser: serves a few runs on 127.0.0.1:7471 for ten minutes.
/// `cargo test -p aqueduct-server --test console -- --ignored --nocapture`, then open
/// <http://127.0.0.1:7471/console>.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "serves for ten minutes, for a person to look at"]
async fn serve_for_a_look() {
    let dir = std::env::temp_dir().join(format!("aqueduct-console-look-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Engine::builder(&dir)
        .workflow(
            "double",
            Arc::new(
                aqueduct_procedure::Procedure::compile("d.star", "def run(x):\n    return x * 2\n")
                    .unwrap(),
            ),
        )
        .workflow(
            "waits",
            Arc::new(
                aqueduct_procedure::Procedure::compile(
                    "w.star",
                    "def run(x):\n    return wait(signal(\"go\"), timeout = 600)\n",
                )
                .unwrap(),
            ),
        )
        .open()
        .unwrap();
    for i in 0..5 {
        engine
            .start(
                &format!("double-{i}"),
                "double",
                aqueduct_core::Payload::new(i.to_string().into_bytes()),
            )
            .unwrap();
    }
    engine
        .start("waiting", "waits", aqueduct_core::Payload::new(*b"1"))
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:7471")
        .await
        .unwrap();
    eprintln!("console at http://127.0.0.1:7471/console");
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(600),
        aqueduct_server::serve_with(
            Arc::new(engine),
            listener,
            &Config::default(),
            Security::for_dir(&dir).unwrap(),
            std::future::pending(),
        ),
    )
    .await;
    let _ = std::fs::remove_dir_all(&dir);
}
