//! History archival (ADR 0061): a server copies its finished runs' histories to object storage, where
//! each verifies on its own; runs still going are left until they end.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use aqueduct_core::{Payload, Status};
use aqueduct_engine::embedded::Engine;
use aqueduct_procedure::Procedure;
use aqueduct_server::archive;
use aqueduct_server::objects::{Store, StoreConfig};
use aqueduct_server::{Config, Security};
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
async fn finished_runs_are_archived_and_verify() {
    let base = std::env::temp_dir().join(format!("aqueduct-archive-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let (dir, objects) = (base.join("data"), base.join("objects"));
    let config: Config = serde_json::from_value(json!({
        "archive": {"store": {"kind": "dir", "path": objects}, "every_ms": 100}
    }))
    .unwrap();
    let engine = Engine::builder(&dir)
        .workflow(
            "double",
            Arc::new(Procedure::compile("d.star", "def run(x):\n    return x * 2\n").unwrap()),
        )
        .workflow(
            "waits",
            Arc::new(
                Procedure::compile(
                    "w.star",
                    "def run(x):\n    return wait(signal(\"go\"), timeout = 600)\n",
                )
                .unwrap(),
            ),
        )
        .open()
        .unwrap();
    for i in 0..3 {
        engine
            .start(
                &format!("$ns/acme/d{i}"),
                "double",
                Payload::new(i.to_string().into_bytes()),
            )
            .unwrap();
        engine
            .wait(&format!("$ns/acme/d{i}"), Duration::from_secs(10))
            .unwrap();
    }
    engine.start("w", "waits", Payload::new(*b"1")).unwrap();
    let engine = Arc::new(engine);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let served = Arc::clone(&engine);
    let security = Security::for_dir(&dir).unwrap();
    let serving = config.clone();
    tokio::spawn(async move {
        aqueduct_server::serve_with(served, listener, &serving, security, std::future::pending())
            .await
    });

    let store = Store::open(&StoreConfig::Dir {
        path: objects.clone(),
    })
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let keys = loop {
        let keys = store.list("runs/").await.unwrap();
        if keys.len() == 3 {
            break keys;
        }
        assert!(Instant::now() < deadline, "archived: {keys:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        !keys.contains(&archive::key("w")),
        "a running run was archived"
    );
    for i in 0..3 {
        let run = format!("$ns/acme/d{i}");
        let data = store.get(&archive::key(&run)).await.unwrap().unwrap();
        let entries = archive::decode(&run, &data).unwrap();
        assert_eq!(Some(entries), engine.history(&run).unwrap());
    }
    // Once the waiting run ends, it is archived too.
    engine.signal("w", "go", "", Payload::new(*b"1")).unwrap();
    assert_eq!(
        engine.wait("w", Duration::from_secs(10)).unwrap().status,
        Status::Completed
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while store.get(&archive::key("w")).await.unwrap().is_none() {
        assert!(Instant::now() < deadline, "the ended run was not archived");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = std::fs::remove_dir_all(&base);
}
