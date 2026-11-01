//! The MCP server: the protocol, and the phase 2 path end to end. A procedure an agent writes through
//! MCP is validated, replayed against past runs, approved by a person, and run by the embedded engine.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use aqueduct_cli::mcp::Server;
use aqueduct_core::Payload;
use aqueduct_engine::embedded::{Attempt, Engine};
use serde_json::{Value, json};

fn request(id: u64, method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

/// Calls a tool; its text, and whether it is an error.
fn call(server: &Server, tool: &str, arguments: Value) -> (String, bool) {
    let response = server
        .handle(&request(
            9,
            "tools/call",
            json!({"name": tool, "arguments": arguments}),
        ))
        .unwrap();
    let result = &response["result"];
    (
        result["content"][0]["text"].as_str().unwrap().to_owned(),
        result["isError"].as_bool().unwrap(),
    )
}

fn ok(server: &Server, tool: &str, arguments: Value) -> String {
    let (text, error) = call(server, tool, arguments);
    assert!(!error, "{tool}: {text}");
    text
}

fn cli(args: &[&str]) -> String {
    aqueduct_cli::run(&args.iter().map(|a| a.to_string()).collect::<Vec<_>>()).unwrap()
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("aqueduct-mcp-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn the_protocol_is_spoken_over_lines() {
    let dir = temp_dir("protocol");
    let server = Server::new(&dir, "agent/test", false);
    let input = [
        request(1, "initialize", json!({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}})).to_string(),
        json!({"jsonrpc": "2.0", "method": "notifications/initialized"}).to_string(),
        request(2, "tools/list", json!({})).to_string(),
        "{not json".to_owned(),
        request(3, "nope", json!({})).to_string(),
        request(4, "tools/call", json!({"name": "approve_version", "arguments": {}})).to_string(),
        request(5, "ping", json!({})).to_string(),
    ]
    .join("\n");
    let mut output = Vec::new();
    server.serve(input.as_bytes(), &mut output).unwrap();
    let responses: Vec<Value> = String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    // The notification gets no response.
    assert_eq!(responses.len(), 6);
    assert_eq!(responses[0]["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(responses[0]["result"]["serverInfo"]["name"], "aqueduct");
    let tools: Vec<&str> = responses[1]["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(tools.contains(&"propose_version") && tools.contains(&"resolve_escalation"));
    // Approval is a person's decision: there is no tool for it.
    assert!(
        !tools
            .iter()
            .any(|t| t.contains("approve") || t.contains("activate"))
    );
    assert_eq!(responses[2]["error"]["code"], -32700);
    assert_eq!(responses[3]["error"]["code"], -32601);
    assert_eq!(responses[4]["error"]["code"], -32602);
    assert_eq!(responses[5]["result"], json!({}));

    // Read-only: no tool that changes anything.
    let read_only = Server::new(&dir, "agent/test", true);
    let listed = read_only
        .handle(&request(1, "tools/list", json!({})))
        .unwrap();
    let tools: Vec<&str> = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(tools.contains(&"replay_draft") && !tools.contains(&"start_run"));
    // A hidden tool cannot be called either.
    let refused = read_only
        .handle(&request(
            2,
            "tools/call",
            json!({"name": "start_run", "arguments": {"workflow": "w", "key": "k", "input": 1}}),
        ))
        .unwrap();
    assert_eq!(refused["error"]["code"], -32602);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn an_agent_writes_a_procedure_that_is_proven_approved_and_run() {
    let dir = temp_dir("e2e");
    let path = dir.to_str().unwrap();
    let agent = Server::new(&dir, "agent/claims-explorer", false);
    let v1 = r#"
def run(claim):
    score = step("score", claim)
    if score > 50:
        return escalate("manual-review", {"claim": claim, "score": score})
    return "approve"
"#;
    let open = || {
        let builder = Engine::builder(&dir).step("score", |claim: &Payload| {
            let claim: Value = serde_json::from_slice(claim.as_bytes()).unwrap();
            Attempt::Completed(Payload::new(
                (claim["amount"].as_u64().unwrap() / 100)
                    .to_string()
                    .into_bytes(),
            ))
        });
        aqueduct_registry::Registry::open(&dir)
            .unwrap()
            .deploy(builder)
            .unwrap()
            .open()
            .unwrap()
    };

    // The agent checks its draft, then proposes and proves it.
    let (broken, error) = call(
        &agent,
        "validate_procedure",
        json!({"source": "def run(x):\n    return y\n"}),
    );
    assert!(error, "{broken}");
    assert!(
        ok(&agent, "validate_procedure", json!({"source": v1})).starts_with("compiles: version ")
    );
    let proposed = ok(
        &agent,
        "propose_version",
        json!({"workflow": "claims", "source": v1, "cases_studied": 37}),
    );
    let id1 = proposed.split_whitespace().nth(2).unwrap().to_owned();
    let proven = ok(
        &agent,
        "replay_version",
        json!({"workflow": "claims", "version": &id1[..10]}),
    );
    assert!(proven.starts_with("0 runs"), "{proven}");
    // It cannot start runs on a version nobody approved.
    let (refused, error) = call(
        &agent,
        "start_run",
        json!({"workflow": "claims", "key": "c-0", "input": {"amount": 1}}),
    );
    assert!(error, "{refused}");

    // A person approves and activates it.
    cli(&[
        "versions",
        "approve",
        path,
        "claims",
        &id1[..10],
        "--by",
        "grace",
    ]);
    cli(&[
        "versions",
        "activate",
        path,
        "claims",
        &id1[..10],
        "--by",
        "grace",
    ]);

    // The agent starts runs; the engine runs them; one escalates, and the agent resolves it.
    for (key, amount) in [("c-1", 3000), ("c-2", 9000)] {
        let started = ok(
            &agent,
            "start_run",
            json!({"workflow": "claims", "key": key, "input": {"amount": amount}}),
        );
        assert!(
            started.contains(&format!("on version {}", &id1[..12])),
            "{started}"
        );
    }
    {
        let engine = open();
        engine.wait("c-1", Duration::from_secs(10)).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while engine.escalations().unwrap().is_empty() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    let open_escalations = ok(&agent, "list_escalations", json!({}));
    assert!(
        open_escalations.starts_with("c-2#1  manual-review"),
        "{open_escalations}"
    );
    ok(
        &agent,
        "resolve_escalation",
        json!({"id": "c-2#1", "resolution": "settle"}),
    );
    {
        let engine = open();
        let done = engine.wait("c-2", Duration::from_secs(10)).unwrap();
        assert_eq!(done.output, Some(Payload::new(*b"\"settle\"")));
    }
    let described = ok(&agent, "describe_run", json!({"key": "c-2"}));
    assert!(described.contains("status    completed"), "{described}");
    assert!(ok(&agent, "run_journal", json!({"key": "c-1"})).contains("\"kind\":\"RunCompleted\""));

    // A second version: the agent sees what it would change before proposing it, then proves it.
    let v2 = v1.replace("score > 50", "score > 95");
    let draft = ok(
        &agent,
        "replay_draft",
        json!({"workflow": "claims", "source": v2}),
    );
    assert!(
        draft.contains("c-2  different") && draft.contains("2 runs: 1 same, 1 different"),
        "{draft}"
    );
    let proposed = ok(
        &agent,
        "propose_version",
        json!({"workflow": "claims", "source": v2, "note": "fewer reviews"}),
    );
    let id2 = proposed.split_whitespace().nth(2).unwrap().to_owned();
    ok(
        &agent,
        "replay_version",
        json!({"workflow": "claims", "version": &id2[..10]}),
    );
    let versions = ok(&agent, "list_versions", json!({"workflow": "claims"}));
    assert!(
        versions.contains(&format!("{} active", &id1[..12])),
        "{versions}"
    );
    assert!(
        versions.contains(&format!("{} proven    agent/claims-explorer", &id2[..12])),
        "{versions}"
    );
    let diff = ok(
        &agent,
        "diff_versions",
        json!({"workflow": "claims", "from": &id1[..10], "to": &id2[..10]}),
    );
    assert!(diff.contains("+     if score > 95:"), "{diff}");
    let metrics = ok(&agent, "metrics", json!({"workflow": "claims"}));
    assert!(
        metrics.contains("escalated   50% of runs (1): manual-review 1"),
        "{metrics}"
    );

    // Every change the agent made is in the registry under its identity.
    let log = cli(&["versions", "log", path]);
    assert!(log.contains("agent/claims-explorer proposed"), "{log}");
    assert!(log.contains("grace            approved"), "{log}");
    std::fs::remove_dir_all(&dir).unwrap();
}
