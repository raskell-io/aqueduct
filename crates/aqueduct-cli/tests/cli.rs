//! The CLI against a data directory written by the embedded engine.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use aqueduct_core::Payload;
use aqueduct_engine::RetryPolicy;
use aqueduct_engine::embedded::{Attempt, Engine};
use aqueduct_procedure::Procedure;

const APPROVAL: &str = r#"
def run(request):
    quote = step("quote", request)
    decision = wait(signal("approve"), timeout = 1)
    if decision == None:
        return {"quote": quote, "approved": False}
    return {"quote": quote, "approved": decision}
"#;

fn cli(args: &[&str]) -> Result<String, String> {
    aqueduct_cli::run(&args.iter().map(|a| a.to_string()).collect::<Vec<_>>())
}

#[test]
fn inspect_verify_export_and_replay() {
    let dir = std::env::temp_dir().join(format!("aqueduct-cli-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.to_str().unwrap();
    let procedure_file = dir.join("approval.star");

    {
        let calls = Arc::new(AtomicUsize::new(0));
        let engine = Engine::builder(&dir)
            .workflow(
                "approval",
                Arc::new(Procedure::compile("approval.star", APPROVAL).unwrap()),
            )
            .step("quote", move |_: &Payload| {
                // The first attempt fails, so the journal shows a retry.
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Attempt::Retry("busy".into())
                } else {
                    Attempt::Completed(Payload::new(*b"42"))
                }
            })
            .retry_policy(
                "quote",
                RetryPolicy {
                    max_attempts: 3,
                    initial_backoff_ms: 10,
                    multiplier: 2,
                    max_backoff_ms: 50,
                },
            )
            .open()
            .unwrap();
        engine
            .start(
                "order-1",
                "approval",
                Payload::new(*b"{\"item\": \"lamp\"}"),
            )
            .unwrap();
        engine
            .start(
                "order-2",
                "approval",
                Payload::new(*b"{\"item\": \"desk\"}"),
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(200));
        engine
            .signal("order-1", "approve", "a-1", Payload::new(*b"true"))
            .unwrap();
        engine.wait("order-1", Duration::from_secs(10)).unwrap();
        engine.wait("order-2", Duration::from_secs(10)).unwrap();

        // While the engine runs, the CLI refuses to open the data directory.
        assert!(
            cli(&["runs", path])
                .unwrap_err()
                .contains("is the engine running")
        );
    }
    std::fs::write(&procedure_file, APPROVAL).unwrap();

    let runs = cli(&["runs", path]).unwrap();
    assert!(
        runs.contains("order-1") && runs.contains("order-2") && runs.contains("completed"),
        "{runs}"
    );

    let described = cli(&["describe", path, "order-1"]).unwrap();
    assert!(described.contains("status    completed"), "{described}");
    assert!(
        described.contains("step quote: completed 42 after 1 failed attempts"),
        "{described}"
    );
    assert!(
        described.contains("wait for signal approve (timeout 1000 ms): completed true"),
        "{described}"
    );

    let journal = cli(&["journal", path, "order-2"]).unwrap();
    assert!(
        journal.contains("attempt 1 failed (busy)") || journal.contains("completed with 42"),
        "{journal}"
    );
    assert!(
        journal.contains("timer fired"),
        "order-2 timed out: {journal}"
    );

    let verified = cli(&["verify", path]).unwrap();
    assert!(verified.contains("2 runs verified"), "{verified}");

    let exported = cli(&["export", path, "order-1"]).unwrap();
    let first: serde_json::Value = serde_json::from_str(exported.lines().next().unwrap()).unwrap();
    assert_eq!(first["kind"], "Started");
    assert_eq!(first["input"]["item"], "lamp");
    assert!(exported.lines().any(|l| l.contains("\"SignalReceived\"")));

    let replayed = cli(&["replay", path, "order-1", procedure_file.to_str().unwrap()]).unwrap();
    assert!(
        replayed.starts_with("replays without divergence"),
        "{replayed}"
    );

    // A changed procedure is caught where it diverges.
    std::fs::write(
        &procedure_file,
        APPROVAL.replace("step(\"quote\"", "step(\"estimate\""),
    )
    .unwrap();
    let diverged = cli(&["replay", path, "order-1", procedure_file.to_str().unwrap()]).unwrap();
    assert!(
        diverged.contains("diverges") && diverged.contains("estimate"),
        "{diverged}"
    );

    assert!(
        cli(&["describe", path, "nobody"])
            .unwrap_err()
            .contains("no run")
    );
    std::fs::remove_dir_all(&dir).unwrap();
}
