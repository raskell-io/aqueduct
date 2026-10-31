//! The README's quick start. Run it with `cargo run -p aqueduct --example quickstart`; kill it during
//! the sleep (Ctrl-C, or `kill -9`) and run it again: the run continues where it stopped.

use std::sync::Arc;
use std::time::Duration;

use aqueduct::embedded::{Attempt, Engine};
use aqueduct::{Payload, WorkflowContext, WorkflowError, workflow};

/// A workflow is an ordinary async function. Everything it does with the world goes through `ctx`.
async fn onboard(ctx: WorkflowContext, user: String) -> Result<String, WorkflowError> {
    let account: String = ctx.step("create_account", &user).await?; // runs at least once, recorded once
    ctx.sleep(Duration::from_secs(5)).await?; // durable: a restart does not start it again
    let note: String = ctx.step("send_welcome", &account).await?;
    Ok(note)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::builder("./quickstart-data")
        .workflow("onboard", Arc::new(workflow(onboard)))
        // Steps are plain functions of JSON payloads; they may fail and are retried with backoff.
        .step("create_account", |user: &Payload| {
            let user: String = serde_json::from_slice(user.as_bytes()).unwrap_or_default();
            Attempt::Completed(Payload::new(
                serde_json::to_vec(&format!("acct-{user}")).unwrap_or_default(),
            ))
        })
        .step("send_welcome", |account: &Payload| {
            let account: String = serde_json::from_slice(account.as_bytes()).unwrap_or_default();
            Attempt::Completed(Payload::new(
                serde_json::to_vec(&format!("welcomed {account}")).unwrap_or_default(),
            ))
        })
        .open()?;

    // Starting is idempotent per key: running the example again resumes the same run.
    engine.start("onboard:ada", "onboard", Payload::new(br#""ada""#.to_vec()))?;
    let result = engine.wait("onboard:ada", Duration::from_secs(60))?;
    let output = result
        .output
        .map(|p| String::from_utf8_lossy(p.as_bytes()).into_owned());
    println!("{:?}: {}", result.status, output.unwrap_or_default());
    Ok(())
}
