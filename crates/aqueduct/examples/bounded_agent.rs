//! A procedure with one bounded agent step, under a run budget (BRIEF.md section 13, phase 2). Run it
//! with `cargo run -p aqueduct --example bounded_agent`.
//!
//! The procedure asks a model to summarise a contract, with a prompt, an output schema and a budget of
//! 3,000 tokens for the call. Each run also has a token budget of its own. Three contracts:
//!
//! - `nda-1` fits its run's budget: the call is made, and its answer, model and tokens are journaled;
//! - `msa-2`'s run budget is too small for the call: it is not made; the run escalates (reason
//!   `budget`), and a person approves more;
//! - `lease-3`'s answers never match the output schema: after its retries the call escalates (reason
//!   `schema`), and a person supplies the summary.
//!
//! Then the engine restarts and the runs are read back: no model call is made again, because replay
//! reads the journaled answers. The model is a stand-in, so no API key is needed; `aqueduct-ai` runs real
//! ones (ADR 0026).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use aqueduct::embedded::{
    AgentAttempt, AgentCall, AgentExecutor, AgentResponse, Engine, EngineError,
};
use aqueduct::{Payload, Procedure, RetryPolicy, StartOptions, Status};
use serde_json::{Value, json};

const DIR: &str = "./bounded-agent-data";

const CONTRACTS: &str = r#"
SUMMARY = {
    "type": "object",
    "required": ["parties", "term_months", "risks"],
    "properties": {
        "parties": {"type": "array", "items": {"type": "string"}},
        "term_months": {"type": "integer"},
        "risks": {"type": "array", "items": {"type": "string"}},
    },
}

def run(contract):
    summary = agent(
        "summarise_contract",
        contract,
        prompt = "prompts/summarise-contract.md",
        output_schema = SUMMARY,
        budget_tokens = 3000,
    )
    if summary["risks"]:
        return {"summary": summary, "review": "legal"}
    return {"summary": summary, "review": "none"}
"#;

/// A stand-in for a model: summarises from the contract's fields, and leaves the term out for leases
/// (an answer that does not match the schema).
struct Summariser {
    calls: AtomicU64,
}

impl AgentExecutor for Summariser {
    fn call(&self, call: &AgentCall<'_>) -> AgentAttempt {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let request: Value = serde_json::from_slice(call.request.as_bytes()).unwrap_or_default();
        let contract = &request["input"];
        let mut summary = json!({
            "parties": contract["parties"],
            "term_months": contract["term_months"],
            "risks": contract["clauses"].as_array().map(|clauses| {
                clauses.iter().filter(|c| c.as_str().is_some_and(|c| c.contains("unlimited"))).cloned().collect::<Vec<_>>()
            }),
        });
        if contract["kind"] == "lease" {
            summary.as_object_mut().map(|s| s.remove("term_months"));
        }
        AgentAttempt::Completed(AgentResponse {
            model: "example/summary-model".into(),
            params: Payload::new(br#"{"temperature":0}"#.to_vec()),
            response: Payload::new(summary.to_string().into_bytes()),
            input_tokens: 1_800,
            output_tokens: 300,
        })
    }
}

fn open(model: &Arc<Summariser>) -> Result<Engine, Box<dyn std::error::Error>> {
    let procedure = Procedure::compile("contracts.star", CONTRACTS)?;
    for lint in procedure.lints() {
        println!("warning: {lint}");
    }
    Ok(Engine::builder(DIR)
        .workflow("contracts", Arc::new(procedure))
        .agent_executor(model.clone())
        // Two attempts for an answer that does not match the schema, then a person.
        .retry_policy(
            "summarise_contract",
            RetryPolicy {
                max_attempts: 2,
                initial_backoff_ms: 10,
                multiplier: 1,
                max_backoff_ms: 10,
            },
        )
        .open()?)
}

/// Waits until a run has an open escalation; returns its id and payload.
fn escalation(engine: &Engine, key: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(open) = engine.escalations()?.into_iter().find(|e| e.key == key) {
            let payload = String::from_utf8_lossy(open.payload.as_bytes()).into_owned();
            return Ok((open.id, format!("{} {payload}", open.reason)));
        }
        if Instant::now() > deadline {
            return Err(format!("{key} did not escalate").into());
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn outcome(engine: &Engine, key: &str) -> Result<String, EngineError> {
    let result = engine.wait(key, Duration::from_secs(10))?;
    assert_eq!(result.status, Status::Completed, "{:?}", result.error);
    Ok(String::from_utf8_lossy(result.output.unwrap_or_default().as_bytes()).into_owned())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::fs::remove_dir_all(DIR);
    let model = Arc::new(Summariser {
        calls: AtomicU64::new(0),
    });
    let contracts = [
        (
            "nda-1",
            10_000,
            json!({"kind": "nda", "parties": ["Acme", "Globex"], "term_months": 24, "clauses": ["confidentiality"]}),
        ),
        (
            "msa-2",
            2_000,
            json!({"kind": "msa", "parties": ["Acme", "Initech"], "term_months": 36, "clauses": ["unlimited liability"]}),
        ),
        (
            "lease-3",
            10_000,
            json!({"kind": "lease", "parties": ["Acme", "Hooli"], "term_months": 60, "clauses": []}),
        ),
    ];
    {
        let engine = open(&model)?;
        for (key, budget, contract) in &contracts {
            let options = StartOptions {
                budget_tokens: Some(*budget),
                ..StartOptions::default()
            };
            engine.start_with(
                key,
                "contracts",
                Payload::new(contract.to_string().into_bytes()),
                options,
            )?;
        }
        println!("nda-1 (run budget 10000): {}", outcome(&engine, "nda-1")?);

        let (id, why) = escalation(&engine, "msa-2")?;
        println!("msa-2 (run budget 2000): escalated, {why}");
        engine.resolve_escalation(&id, Payload::new(*b"true"))?;
        println!(
            "  a person approves more budget: {}",
            outcome(&engine, "msa-2")?
        );

        let (id, why) = escalation(&engine, "lease-3")?;
        println!("lease-3: escalated, {why}");
        let supplied = json!({"parties": ["Acme", "Hooli"], "term_months": 60, "risks": []});
        engine.resolve_escalation(&id, Payload::new(supplied.to_string().into_bytes()))?;
        println!(
            "  a person supplies the summary: {}",
            outcome(&engine, "lease-3")?
        );
    }
    let calls = model.calls.load(Ordering::SeqCst);
    println!("\n{calls} model calls (lease-3's answer was asked for twice)");

    // A restart replays the runs from their journals: the answers are read, not asked for again.
    {
        let engine = open(&model)?;
        for (key, _, _) in &contracts {
            outcome(&engine, key)?;
        }
    }
    println!(
        "after a restart: {} model calls made again",
        model.calls.load(Ordering::SeqCst) - calls
    );

    println!("\nmsa-2's journal:");
    let journal = aqueduct_cli::run(&["journal".into(), DIR.into(), "msa-2".into()])?;
    for line in journal.lines() {
        println!("  {}", line.chars().take(140).collect::<String>());
    }
    Ok(())
}
