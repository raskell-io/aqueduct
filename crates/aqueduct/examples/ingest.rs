//! The reference ingest pipeline from BRIEF.md: extract, map, resolve, score, wait for review.
//!
//! Run it with `cargo run -p aqueduct --example ingest -- --auto` (CI does): a thread plays the
//! reviewer, who approves the HR system through its awakeable, and the operator, who declines the CRM
//! platform once its review has timed out and escalated.
//!
//! Without `--auto`, nobody reviews, so both reviews time out (after a second or two here; seven days in
//! production) and escalate. Resolve them from the command line while the example is stopped, then run
//! it again; the runs continue with the operator's decisions:
//!
//! ```sh
//! cargo run -p aqueduct --example ingest                      # starts three runs; two escalate
//! cargo run -p aqueduct-cli -- escalations ingest-data
//! cargo run -p aqueduct-cli -- resolve ingest-data 'ingest:hr-system#10' '{"approve": true}'
//! cargo run -p aqueduct-cli -- resolve ingest-data 'ingest:crm-platform#10' '{"approve": false}'
//! cargo run -p aqueduct --example ingest                      # the runs finish
//! ```

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use aqueduct::embedded::{Attempt, Engine, EngineError};
use aqueduct::{Either, Payload, WorkflowContext, WorkflowError, join, select, workflow};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct IngestInput {
    source: String,
    /// How long a reviewer has (seven days in production; seconds here).
    review_timeout_secs: u64,
}

#[derive(Serialize, Deserialize)]
struct Resolved {
    records: u32,
    conflicts: u32,
}

#[derive(Serialize, Deserialize)]
struct Review {
    approve: bool,
}

#[derive(Serialize, Deserialize)]
struct IngestReport {
    source: String,
    records: u32,
    score: f64,
    reviewed_by: String,
    started_ms: u128,
    token: String,
}

async fn ingest_source(
    ctx: WorkflowContext,
    input: IngestInput,
) -> Result<IngestReport, WorkflowError> {
    // Recorded once, the same on every replay.
    let started = ctx.now();
    let token = ctx.uuid();

    let batch: String = ctx.step("extract", &input.source).await?;
    // Concurrent steps: issued together, completions journaled in the order they happen.
    let (mapped, owners) = join(
        ctx.step::<u32, _>("map", &batch),
        ctx.step::<u32, _>("owners", &batch),
    )
    .await;
    let resolved: Resolved = ctx.step("resolve", &(mapped?, owners?)).await?;
    let score: f64 = ctx.step("score", &resolved.records).await?;

    // Conflicts need a person: wait for a review, durably, up to the deadline; then escalate.
    let reviewed_by = if resolved.conflicts > 0 {
        let (id, review) = ctx.awakeable::<Review>();
        let _: () = ctx.step("notify_reviewers", &(&input.source, &id)).await?;
        let deadline = ctx.sleep(Duration::from_secs(input.review_timeout_secs));
        let (decision, by) = match select(review, deadline).await {
            Either::Left(review) => (review?, "reviewer"),
            // Nobody reviewed in time: an escalation, not an error. An operator decides.
            Either::Right(slept) => {
                slept?;
                let decision: Review = ctx.escalate("review-timeout", &resolved).await?;
                (decision, "operator")
            }
        };
        if !decision.approve {
            return Err(WorkflowError::failed(format!(
                "{}: {} conflicts rejected by the {by}",
                input.source, resolved.conflicts
            )));
        }
        by.to_owned()
    } else {
        "nobody (no conflicts)".to_owned()
    };

    Ok(IngestReport {
        source: input.source,
        records: resolved.records,
        score,
        reviewed_by,
        started_ms: started
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
        token: token.to_string(),
    })
}

fn json<T: Serialize>(value: &T) -> Payload {
    Payload::new(serde_json::to_vec(value).unwrap_or_default())
}

fn decode<T: for<'de> Deserialize<'de> + Default>(payload: &Payload) -> T {
    serde_json::from_slice(payload.as_bytes()).unwrap_or_default()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let auto = std::env::args().any(|arg| arg == "--auto");
    if auto {
        let _ = std::fs::remove_dir_all("./ingest-data");
    }
    let (review_ids, review_inbox) = std::sync::mpsc::channel::<(String, String)>();
    let review_ids = std::sync::Mutex::new(review_ids);
    let engine = Engine::builder("./ingest-data")
        .workflow("ingest_source", Arc::new(workflow(ingest_source)))
        .step("extract", |source: &Payload| {
            let source: String = decode(source);
            Attempt::Completed(json(&format!("batch:{source}")))
        })
        .step("map", |batch: &Payload| {
            Attempt::Completed(json(&(decode::<String>(batch).len() as u32 * 10)))
        })
        .step("owners", |batch: &Payload| {
            Attempt::Completed(json(&(decode::<String>(batch).len() as u32)))
        })
        .step("resolve", |input: &Payload| {
            let (mapped, owners): (u32, u32) = decode(input);
            // Sources with long names have conflicting identities, for the sake of the example.
            let conflicts = owners.saturating_sub(10);
            Attempt::Completed(json(&Resolved {
                records: mapped,
                conflicts,
            }))
        })
        .step("score", |records: &Payload| {
            let records: u32 = decode(records);
            Attempt::Completed(json(&(f64::from(records.min(200)) / 200.0)))
        })
        .step("notify_reviewers", move |input: &Payload| {
            let (source, id): (String, String) = decode(input);
            println!("reviewers asked to look at {source}; awakeable id: {id}");
            if let Ok(ids) = review_ids.lock() {
                let _ = ids.send((source, id));
            }
            Attempt::Completed(json(&()))
        })
        .open()?;

    let sources = [("git", 2), ("hr-system", 2), ("crm-platform", 1)];
    for (source, review_timeout_secs) in sources {
        let input = IngestInput {
            source: source.into(),
            review_timeout_secs,
        };
        engine.start(&format!("ingest:{source}"), "ingest_source", json(&input))?;
    }

    if auto {
        // The reviewer approves the HR system and never gets to the CRM; the operator then resolves the
        // CRM's escalation.
        for _ in 0..2 {
            let (source, id) = review_inbox.recv_timeout(Duration::from_secs(10))?;
            if source == "hr-system" {
                engine.complete_awakeable(&id, json(&Review { approve: true }))?;
            }
        }
        let escalation = loop {
            if let Some(open) = engine.escalations()?.into_iter().next() {
                break open;
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        println!(
            "escalated: {} ({}) {}",
            escalation.id,
            escalation.reason,
            String::from_utf8_lossy(escalation.payload.as_bytes())
        );
        engine.resolve_escalation(&escalation.id, json(&Review { approve: false }))?;
    }

    for (source, _) in sources {
        match engine.wait(&format!("ingest:{source}"), Duration::from_secs(5)) {
            Ok(result) => {
                let detail = result
                    .output
                    .map(|p| String::from_utf8_lossy(p.as_bytes()).into_owned())
                    .or(result.error)
                    .unwrap_or_default();
                println!("{source}: {:?} {detail}", result.status);
            }
            Err(EngineError::Timeout(_)) => println!("{source}: waiting (see the escalations)"),
            Err(e) => return Err(e.into()),
        }
    }
    if !auto {
        for escalation in engine.escalations()? {
            println!("open escalation {} ({})", escalation.id, escalation.reason);
        }
    }
    Ok(())
}
