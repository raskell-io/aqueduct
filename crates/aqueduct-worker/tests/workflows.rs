//! A Rust SDK workflow run by a worker outside the server (ADR 0039), with its step: through a signal,
//! a value recorded remotely, and a worker that stops partway, replaced by another.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use aqueduct_engine::embedded::Engine;
use aqueduct_sdk::{WorkflowContext, WorkflowError, workflow};
use aqueduct_worker::{Outcome, Worker};

async fn order(ctx: WorkflowContext, amount: u32) -> Result<String, WorkflowError> {
    let receipt: String = ctx.step("charge", &amount).await?;
    let token = ctx.uuid();
    let shipped: Option<bool> = ctx
        .wait_signal("ship", Some(Duration::from_secs(600)))
        .await?;
    Ok(format!("{receipt}; shipped {shipped:?}; token {token}"))
}

fn worker(name: &str) -> Worker {
    Worker::new(name)
        .workflow("order", Arc::new(workflow(order)))
        .step("charge", |i| {
            let amount = String::from_utf8_lossy(&i.input).into_owned();
            Outcome::Completed(format!("\"charged {amount}\"").into_bytes())
        })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_code_workflow_runs_in_a_worker() {
    let dir =
        std::env::temp_dir().join(format!("aqueduct-remote-workflows-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Arc::new(
        Engine::builder(&dir)
            .remote_workflow("order")
            .remote_steps(Duration::from_secs(30))
            .open()
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(aqueduct_server::serve(
        Arc::clone(&engine),
        listener,
        std::future::pending(),
    ));
    let blocking = |f: Box<dyn FnOnce(&Engine) + Send>| {
        let engine = Arc::clone(&engine);
        tokio::task::spawn_blocking(move || f(&engine))
    };

    // The first worker charges and records the token, then stops while the run waits for its signal.
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let first_url = url.clone();
    let first = tokio::spawn(async move {
        worker("first")
            .run_until(&first_url, async {
                let _ = stopped.await;
            })
            .await
    });
    blocking(Box::new(|engine| {
        engine
            .start("o-1", "order", aqueduct_core::Payload::new(*b"40"))
            .unwrap();
    }))
    .await
    .unwrap();
    for _ in 0..1_000 {
        let engine = Arc::clone(&engine);
        let commands = tokio::task::spawn_blocking(move || {
            engine
                .describe("o-1")
                .map(|(_, s)| s.steps().len())
                .unwrap_or(0)
        })
        .await
        .unwrap();
        // charge, uuid, the signal wait.
        if commands == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let _ = stop.send(());
    first.await.unwrap().unwrap();

    // The signal arrives with no worker connected; a second worker takes over and finishes the run.
    blocking(Box::new(|engine| {
        engine
            .signal("o-1", "ship", "s-1", aqueduct_core::Payload::new(*b"true"))
            .unwrap();
    }))
    .await
    .unwrap();
    let second_url = url.clone();
    tokio::spawn(async move { worker("second").run(&second_url).await });
    let engine_wait = Arc::clone(&engine);
    let result =
        tokio::task::spawn_blocking(move || engine_wait.wait("o-1", Duration::from_secs(20)))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        result.status,
        aqueduct_core::Status::Completed,
        "{:?}",
        result.error
    );
    let output: String = serde_json::from_slice(result.output.unwrap().as_bytes()).unwrap();
    assert!(
        output.starts_with("charged 40; shipped Some(true); token "),
        "{output}"
    );
    // The token was recorded by the first worker and replayed by the second: it is in the journal.
    let (_, state) = engine.describe("o-1").unwrap();
    let recorded = state
        .steps()
        .iter()
        .find(|s| s.name == "uuid")
        .and_then(|s| s.outcome.clone());
    let Some(aqueduct_core::StepOutcome::Completed(token)) = recorded else {
        panic!("no recorded token");
    };
    let token: String = serde_json::from_slice(token.as_bytes()).unwrap();
    assert!(output.ends_with(&token), "{output} / {token}");
    assert!(engine.activation_queue().unwrap().is_empty());
    let _ = std::fs::remove_dir_all(dir);
}
