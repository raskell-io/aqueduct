//! A procedure crystallised from cases an agent handled, proven by counterfactual replay against them,
//! approved with a signature, and run (BRIEF.md section 13, phase 2). Run it with
//! `cargo run -p aqueduct --example crystallise`.
//!
//! 1. **Explore.** At first an agent triages every claim: the `claims` procedure looks the policy up and
//!    asks the agent (a bounded agent step), whose answers, model and token costs are journaled.
//! 2. **Crystallise.** The agent, through the MCP server, reads those runs and writes a procedure: the
//!    rules it followed decide the clear cases, and only the claims over the policy's limit still go to
//!    it. It validates the procedure, proposes it with its provenance, and replays it against the past
//!    runs. The report shows one run decided differently: a claim on a lapsed policy the agent approved.
//! 3. **Approve.** A person reads the report and approves the new version with her signing key, then
//!    activates it.
//! 4. **Run.** New claims run on the crystallised version: the agent is asked only about the one claim
//!    the rules leave to it, and the per-version metrics show the cost falling.
//!
//! The model is a stand-in that follows fixed rules (and makes one mistake), so the example needs no API
//! key and gives the same output every time. `aqueduct-ai` runs real models (ADR 0026).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aqueduct::embedded::{AgentAttempt, AgentCall, AgentExecutor, AgentResponse, Attempt, Engine};
use aqueduct::registry::{Provenance, Registry, ReplayEvidence, SecretKey, Signature};
use aqueduct::{Payload, Status};
use aqueduct_cli::mcp::Server;
use serde_json::{Value, json};

const DIR: &str = "./crystallise-data";

/// The first version: the agent decides every claim.
const EXPLORE: &str = r#"
TRIAGE = {
    "type": "object",
    "required": ["decision"],
    "properties": {"decision": {"enum": ["approve", "decline", "request-photos"]}},
}

def run(claim):
    policy = step("lookup_policy", claim["policy"])
    triage = agent(
        "triage_claim",
        {"claim": claim, "policy": policy},
        prompt = "prompts/triage-claim.md",
        output_schema = TRIAGE,
        budget_tokens = 2000,
    )
    return triage["decision"]
"#;

/// What the agent writes after studying its runs: the clear cases by rule, the rest still to it.
const CRYSTALLISED: &str = r#"
# Crystallised by agent/claims-explorer from the claims it triaged: a lapsed policy is declined, a claim
# within the policy's automatic limit is approved, and only larger claims still need the agent.

TRIAGE = {
    "type": "object",
    "required": ["decision"],
    "properties": {"decision": {"enum": ["approve", "decline", "request-photos"]}},
}

def run(claim):
    policy = step("lookup_policy", claim["policy"])
    if not policy["active"]:
        return "decline"
    if claim["amount"] <= policy["auto_limit"]:
        return "approve"
    triage = agent(
        "triage_claim",
        {"claim": claim, "policy": policy},
        prompt = "prompts/triage-claim.md",
        output_schema = TRIAGE,
        budget_tokens = 2000,
    )
    return triage["decision"]
"#;

/// The claims the agent handled while exploring: (key, policy, amount, photos).
const EXPLORED: &[(&str, &str, u32, bool)] = &[
    ("c-01", "P-1", 400, false),
    ("c-02", "P-1", 900, true),
    ("c-03", "P-1", 2500, true),
    ("c-04", "P-1", 3100, false),
    ("c-05", "P-2", 4200, false),
    ("c-06", "P-2", 7800, true),
    ("c-07", "P-3", 300, true),
    ("c-08", "P-3", 1200, false),
    ("c-09", "P-2", 450, false),
    ("c-10", "P-1", 6400, false),
    ("c-11", "P-3", 5100, true),
    ("c-12", "P-2", 9900, false),
];

/// New claims, after the crystallised version is active.
const NEW: &[(&str, &str, u32, bool)] = &[
    ("c-13", "P-1", 250, false),
    ("c-14", "P-3", 800, true),
    ("c-15", "P-2", 8800, true),
];

/// A stand-in for a model: it follows the rules the crystallised version spells out, asks for photos
/// of large claims without them, and once approves a claim on a lapsed policy.
struct Triage {
    calls: AtomicU64,
}

impl AgentExecutor for Triage {
    fn call(&self, call: &AgentCall<'_>) -> AgentAttempt {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let request: Value = serde_json::from_slice(call.request.as_bytes()).unwrap_or_default();
        let (claim, policy) = (&request["input"]["claim"], &request["input"]["policy"]);
        let decision = if claim["id"] == "c-07" {
            "approve" // the mistake: the policy has lapsed
        } else if policy["active"] != true {
            "decline"
        } else if claim["amount"].as_u64() <= policy["auto_limit"].as_u64()
            || claim["photos"] == true
        {
            "approve"
        } else {
            "request-photos"
        };
        AgentAttempt::Completed(AgentResponse {
            model: "example/triage-model".into(),
            params: Payload::new(br#"{"temperature":0}"#.to_vec()),
            response: Payload::new(json!({ "decision": decision }).to_string().into_bytes()),
            input_tokens: 1_200,
            output_tokens: 80,
        })
    }
}

fn policy(id: &str) -> Value {
    match id {
        "P-1" => json!({"id": id, "active": true, "auto_limit": 1000}),
        "P-2" => json!({"id": id, "active": true, "auto_limit": 5000}),
        _ => json!({"id": id, "active": false, "auto_limit": 0}),
    }
}

fn open(model: &Arc<Triage>) -> Result<Engine, Box<dyn std::error::Error>> {
    let builder =
        Engine::builder(DIR)
            .agent_executor(model.clone())
            .step("lookup_policy", |id: &Payload| {
                let id: String = serde_json::from_slice(id.as_bytes()).unwrap_or_default();
                Attempt::Completed(Payload::new(policy(&id).to_string().into_bytes()))
            });
    Ok(Registry::open(DIR.as_ref())?.deploy(builder)?.open()?)
}

/// Runs claims to their end; prints each decision.
fn run_claims(
    engine: &Engine,
    claims: &[(&str, &str, u32, bool)],
) -> Result<(), Box<dyn std::error::Error>> {
    for (key, policy, amount, photos) in claims {
        let claim = json!({"id": key, "policy": policy, "amount": amount, "photos": photos});
        engine.start(key, "claims", Payload::new(claim.to_string().into_bytes()))?;
    }
    for (key, policy, amount, _) in claims {
        let result = engine.wait(key, Duration::from_secs(10))?;
        assert_eq!(result.status, Status::Completed, "{:?}", result.error);
        let decision: String =
            serde_json::from_slice(result.output.unwrap_or_default().as_bytes())?;
        let version = result.version.unwrap_or_default();
        println!(
            "  {key}  {policy}  {amount:>5}  {decision:<16}  version {}",
            &version[..12]
        );
    }
    Ok(())
}

/// Calls an MCP tool as the agent; its text.
fn tool(agent: &Server, name: &str, arguments: Value) -> Result<String, String> {
    let response = agent
        .handle(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }))
        .ok_or("no response")?;
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    if response["result"]["isError"] == true {
        return Err(text);
    }
    Ok(text)
}

fn cli(args: &[&str]) -> Result<String, String> {
    aqueduct_cli::run(&args.iter().map(|a| a.to_string()).collect::<Vec<_>>())
}

fn indent(text: &str) -> String {
    text.lines().map(|line| format!("    {line}\n")).collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::fs::remove_dir_all(DIR);
    std::fs::create_dir_all(format!("{DIR}/registry"))?;
    // Grace approves with a signing key; the registry's identities.json says it is hers.
    let grace = SecretKey::from_seed([7; 32]);
    std::fs::write(
        format!("{DIR}/registry/identities.json"),
        json!({ "grace": [grace.public().to_string()] }).to_string(),
    )?;

    // The exploring version, set up by the team: written by Ada, approved by Grace.
    let mut registry = Registry::open(DIR.as_ref())?;
    let provenance = Provenance {
        author: "ada".into(),
        explored_by: None,
        cases_studied: None,
        note: Some("let the agent triage every claim, and learn from it".into()),
    };
    let explore = registry.register("claims", EXPLORE, provenance)?;
    registry.propose("claims", &explore, "ada")?;
    let nothing_to_replay = ReplayEvidence {
        runs: 0,
        same: 0,
        different: 0,
        unresolved: 0,
        report_sha256: String::new(),
    };
    registry.prove("claims", &explore, "ada", nothing_to_replay)?;
    let statement = registry.statement("claims", &explore, "grace", None)?;
    let signature = Signature {
        key: grace.public(),
        signature: grace.sign(statement.as_bytes()),
    };
    registry.approve_signed("claims", &explore, "grace", None, Some(signature))?;
    registry.activate("claims", &explore, "grace")?;
    drop(registry);

    println!(
        "1. The agent triages every claim (version {}):",
        &explore[..12]
    );
    let model = Arc::new(Triage {
        calls: AtomicU64::new(0),
    });
    {
        let engine = open(&model)?;
        run_claims(&engine, EXPLORED)?;
    }
    println!(
        "  {} agent calls, every answer journaled with its model and cost\n",
        model.calls.load(Ordering::SeqCst)
    );

    // The agent works through the MCP server, as `agent/claims-explorer`.
    println!("2. The agent crystallises what it did into a procedure, through MCP:");
    let agent = Server::new(DIR.as_ref(), "agent/claims-explorer", false);
    let studied = tool(&agent, "list_runs", json!({}))?.lines().count() - 1;
    let journal = tool(&agent, "run_journal", json!({"key": "c-07"}))?;
    println!(
        "  studied {studied} runs (c-07's journal has {} entries)",
        journal.lines().count()
    );
    print!(
        "{}",
        indent(&tool(
            &agent,
            "validate_procedure",
            json!({"source": CRYSTALLISED})
        )?)
    );
    let proposed = tool(
        &agent,
        "propose_version",
        json!({
            "workflow": "claims", "source": CRYSTALLISED, "cases_studied": studied,
            "note": "rules for lapsed policies and small claims; larger claims still to the agent",
        }),
    )?;
    let crystallised = proposed
        .split_whitespace()
        .nth(2)
        .ok_or("no version id")?
        .to_owned();
    print!("{}", indent(&proposed));
    // Counterfactual replay: the past runs, answered with what they recorded.
    let report = tool(
        &agent,
        "replay_version",
        json!({"workflow": "claims", "version": crystallised}),
    )?;
    // One line per run, and the totals (the full report also says where each run's commands differ).
    let summary: String = report
        .lines()
        .filter(|line| !line.trim_start().starts_with("first difference"))
        .map(|line| format!("{line}\n"))
        .collect();
    print!("{}", indent(&summary));
    // The agent cannot approve its own work.
    let refused = match cli(&[
        "versions",
        "approve",
        DIR,
        "claims",
        &crystallised,
        "--by",
        "agent/claims-explorer",
    ]) {
        Ok(_) => return Err("the agent approved its own version".into()),
        Err(refused) => refused,
    };
    println!("  approving it itself: {refused}\n");

    println!("3. Grace reads the report (c-07 was approved on a lapsed policy) and signs off:");
    let mut registry = Registry::open(DIR.as_ref())?;
    let note = Some("c-07 should have been declined; the new version does".to_owned());
    let statement = registry.statement("claims", &crystallised, "grace", note.as_deref())?;
    let signature = Signature {
        key: grace.public(),
        signature: grace.sign(statement.as_bytes()),
    };
    registry.approve_signed("claims", &crystallised, "grace", note, Some(signature))?;
    registry.activate("claims", &crystallised, "grace")?;
    drop(registry);
    print!("{}", indent(&cli(&["versions", "list", DIR, "claims"])?));

    println!("\n4. New claims run on the crystallised version:");
    let before = model.calls.load(Ordering::SeqCst);
    {
        let engine = open(&model)?;
        run_claims(&engine, NEW)?;
    }
    println!(
        "  {} agent call for {} claims: only the one over its policy's limit\n",
        model.calls.load(Ordering::SeqCst) - before,
        NEW.len()
    );
    println!("Per version:");
    print!("{}", indent(&cli(&["metrics", DIR, "claims"])?));
    Ok(())
}
