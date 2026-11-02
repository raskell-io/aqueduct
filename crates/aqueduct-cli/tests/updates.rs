//! `aqueduct query` and `aqueduct update` (ADR 0036) on a data directory, and the MCP `query_run` tool.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use aqueduct_cli::mcp::Server;
use aqueduct_core::Payload;
use aqueduct_engine::embedded::{Attempt, Engine};
use serde_json::json;

const ORDER: &str = r#"
def run(order):
    address = order["address"]
    publish("address", address)
    for _ in range(10):
        request = wait(update("change"))
        if request["value"] == "ship":
            respond(request, "shipping to " + address)
            break
        if request["value"] == "":
            reject(request, "an address cannot be empty")
            continue
        address = step("normalise", request["value"])
        publish("address", address)
        respond(request, address)
    return address
"#;

fn cli(args: &[&str]) -> Result<String, String> {
    aqueduct_cli::run(&args.iter().map(|a| a.to_string()).collect::<Vec<_>>())
}

#[test]
fn queries_and_updates_from_the_command_line() {
    let dir = std::env::temp_dir().join(format!("aqueduct-cli-updates-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.to_str().unwrap();
    let file = dir.join("order.star");
    std::fs::write(&file, ORDER).unwrap();
    let registered = cli(&[
        "versions",
        "register",
        path,
        "order",
        file.to_str().unwrap(),
        "--by",
        "ada",
    ])
    .unwrap();
    let id = registered.split_whitespace().nth(3).unwrap().to_owned();
    for (sub, by) in [
        ("propose", "ada"),
        ("prove", "ada"),
        ("approve", "grace"),
        ("activate", "grace"),
    ] {
        cli(&["versions", sub, path, "order", &id, "--by", by]).unwrap();
    }
    cli(&["start", path, "order", "o-1", r#"{"address": "lisbon"}"#]).unwrap();

    assert_eq!(
        cli(&["query", path, "o-1", "address"]).unwrap(),
        "\"lisbon\"\n"
    );
    assert!(
        cli(&["query", path, "o-1", "eta"])
            .unwrap_err()
            .contains("has no query `eta`")
    );
    // Refused on receipt: answered at once, and nothing is written.
    assert_eq!(
        cli(&["update", path, "o-1", "change", "\"\""]).unwrap_err(),
        "update rejected: an address cannot be empty"
    );
    // This one needs a step first: accepted now, answered once the engine has run.
    let accepted = cli(&["update", path, "o-1", "change", "\"porto\"", "u-1"]).unwrap();
    assert!(accepted.contains("send it again with id u-1"), "{accepted}");
    {
        let engine = aqueduct_registry::Registry::open(&dir)
            .unwrap()
            .deploy(Engine::builder(&dir).step("normalise", |name: &Payload| {
                let name = String::from_utf8_lossy(name.as_bytes()).to_uppercase();
                Attempt::Completed(Payload::new(name.into_bytes()))
            }))
            .unwrap()
            .open()
            .unwrap();
        assert_eq!(
            engine
                .update(
                    "o-1",
                    "change",
                    "u-1",
                    Payload::new(*b"\"porto\""),
                    Duration::from_secs(10)
                )
                .unwrap(),
            Payload::new(*b"\"PORTO\"")
        );
    }
    assert_eq!(
        cli(&["update", path, "o-1", "change", "\"porto\"", "u-1"]).unwrap(),
        "\"PORTO\"\n"
    );
    let agent = Server::new(&dir, "agent/mcp", true);
    let response = agent
        .handle(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "query_run", "arguments": {"key": "o-1", "name": "address"}}
        }))
        .unwrap();
    assert_eq!(
        response["result"]["content"][0]["text"],
        json!("\"PORTO\"\n")
    );
    assert_eq!(
        cli(&["update", path, "o-1", "change", "\"ship\""]).unwrap(),
        "\"shipping to PORTO\"\n"
    );
    let described = cli(&["describe", path, "o-1"]).unwrap();
    assert!(described.contains("status    completed"), "{described}");
    let journal = cli(&["journal", path, "o-1"]).unwrap();
    assert!(
        journal.contains("update u-1 received: \"porto\""),
        "{journal}"
    );
    assert!(!journal.contains("\"\""), "{journal}");
    std::fs::remove_dir_all(&dir).unwrap();
}
