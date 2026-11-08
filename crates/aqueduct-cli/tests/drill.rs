//! `aqueduct drill` (ADR 0065) against three servers in this process, one of which stops part way: no
//! acknowledged run is lost, and the report says how long starts were refused.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use aqueduct_engine::embedded::Engine;
use aqueduct_procedure::Procedure;
use aqueduct_server::{Config, Security};
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
async fn a_drill_through_a_server_stopping_loses_nothing() {
    let base = std::env::temp_dir().join(format!("aqueduct-drill-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let mut listeners = BTreeMap::new();
    for id in 1..=3u64 {
        listeners.insert(
            id,
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        );
    }
    let urls: BTreeMap<u64, String> = listeners
        .iter()
        .map(|(id, l)| (*id, format!("http://{}", l.local_addr().unwrap())))
        .collect();
    let peers: BTreeMap<String, String> = urls
        .iter()
        .map(|(id, url)| (id.to_string(), url.clone()))
        .collect();
    let mut stops = BTreeMap::new();
    for (id, listener) in listeners {
        let config: Config = serde_json::from_value(json!({
            "cluster": {"id": id, "peers": peers, "secret": "a secret for the test", "tick_ms": 20}
        }))
        .unwrap();
        let shards = config
            .cluster
            .clone()
            .unwrap()
            .open(&base.join(id.to_string()), 1, |path, _| {
                Ok(Engine::builder(path).workflow(
                    "double",
                    Arc::new(
                        Procedure::compile("d.star", "def run(x):\n    return x * 2\n").unwrap(),
                    ),
                ))
            })
            .unwrap();
        let security = Security::for_dir(&base.join(id.to_string())).unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            aqueduct_server::serve_shards(shards, listener, &config, security, None, async {
                let _ = stopped.await;
            })
            .await
        });
        stops.insert(id, stop);
    }
    let mut args: Vec<String> = [
        "drill",
        "--workflow",
        "double",
        "--seconds",
        "4",
        "--rate",
        "20",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    for url in urls.values() {
        args.push("--server".into());
        args.push(url.clone());
    }
    let drill = tokio::task::spawn_blocking(move || aqueduct_cli::run(&args));
    // Server 1, the partition's preferred leader (ADR 0058), goes part way through.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let _ = stops.remove(&1).unwrap().send(());
    let report = drill.await.unwrap().unwrap();
    eprintln!("{report}");
    assert!(
        report.contains("acknowledged runs missing at the end: 0"),
        "{report}"
    );
    assert!(report.contains("80 starts sent"), "{report}");
    for stop in stops.into_values() {
        let _ = stop.send(());
    }
    let _ = std::fs::remove_dir_all(&base);
}
