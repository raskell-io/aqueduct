//! Human approval with an awakeable. Run it with `cargo run -p aqueduct --example approval`.
//!
//! The workflow asks a person to approve an expense and waits for the answer for up to a day. The
//! person's answer arrives from outside the engine, through the awakeable's id: here, from the command
//! line while the engine is stopped. The run survives in between.
//!
//! ```sh
//! cargo run -p aqueduct --example approval                 # starts the run, prints the id, stops
//! cargo run -p aqueduct-cli -- complete-awakeable approval-data 'expense:42#0' true
//! cargo run -p aqueduct --example approval                 # the run finishes: approved
//! ```
//!
//! With `--auto`, a thread plays the approver after half a second (used in CI).

use std::sync::Arc;
use std::time::Duration;

use aqueduct::embedded::{Attempt, Engine, EngineError};
use aqueduct::{Either, Payload, WorkflowContext, WorkflowError, select, workflow};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Expense {
    id: u32,
    amount: u32,
    approver: String,
}

async fn approve_expense(ctx: WorkflowContext, expense: Expense) -> Result<String, WorkflowError> {
    // The id names this run and this wait; hand it to whoever decides (an email link, a chat button).
    let (id, decision) = ctx.awakeable::<bool>();
    let _: () = ctx
        .step("ask_approver", &(expense.approver.as_str(), id.as_str()))
        .await?;
    let approved = match select(decision, ctx.sleep(Duration::from_secs(24 * 60 * 60))).await {
        Either::Left(decision) => decision?,
        Either::Right(slept) => slept.map(|()| false)?, // nobody answered within a day
    };
    if !approved {
        return Ok(format!("expense {} declined", expense.id));
    }
    let receipt: String = ctx.step("reimburse", &expense.amount).await?;
    Ok(format!("expense {} approved: {receipt}", expense.id))
}

fn json<T: Serialize>(value: &T) -> Payload {
    Payload::new(serde_json::to_vec(value).unwrap_or_default())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let auto = std::env::args().any(|arg| arg == "--auto");
    let (asked, asked_inbox) = std::sync::mpsc::channel::<String>();
    let asked = std::sync::Mutex::new(asked);
    let engine = Engine::builder("./approval-data")
        .workflow("approve_expense", Arc::new(workflow(approve_expense)))
        .step("ask_approver", move |input: &Payload| {
            let (approver, id): (String, String) =
                serde_json::from_slice(input.as_bytes()).unwrap_or_default();
            println!("asked {approver} to approve; awakeable id: {id}");
            if let Ok(asked) = asked.lock() {
                let _ = asked.send(id);
            }
            Attempt::Completed(json(&()))
        })
        .step("reimburse", |amount: &Payload| {
            let amount: u32 = serde_json::from_slice(amount.as_bytes()).unwrap_or_default();
            Attempt::Completed(json(&format!("paid {amount} EUR")))
        })
        .open()?;

    let expense = Expense {
        id: 42,
        amount: 180,
        approver: "grace".into(),
    };
    engine.start("expense:42", "approve_expense", json(&expense))?;
    if auto {
        let id = asked_inbox.recv_timeout(Duration::from_secs(10))?;
        std::thread::sleep(Duration::from_millis(500));
        engine.complete_awakeable(&id, json(&true))?;
    }
    match engine.wait("expense:42", Duration::from_secs(3)) {
        Ok(result) => {
            let output = result
                .output
                .map(|p| String::from_utf8_lossy(p.as_bytes()).into_owned());
            println!("{:?}: {}", result.status, output.unwrap_or_default());
        }
        Err(EngineError::Timeout(_)) => {
            println!(
                "waiting for approval. Approve from the command line, then run this again:\n  \
                 cargo run -p aqueduct-cli -- complete-awakeable approval-data 'expense:42#0' true"
            );
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}
