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

#[test]
fn signals_and_awakeables_are_sent_while_the_engine_is_stopped() {
    use aqueduct_sdk::{WorkflowContext, WorkflowError, workflow};

    async fn two_waits(ctx: WorkflowContext, _: ()) -> Result<String, WorkflowError> {
        let (id, review) = ctx.awakeable::<String>();
        let _: () = ctx.step("publish", &id).await?;
        let go: Option<bool> = ctx.wait_signal("go", None).await?;
        Ok(format!("{} {}", review.await?, go.unwrap_or(false)))
    }
    let dir = std::env::temp_dir().join(format!("aqueduct-cli-signal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.to_str().unwrap();
    let open = || {
        Engine::builder(&dir)
            .workflow("waits", Arc::new(workflow(two_waits)))
            .step("publish", |_: &Payload| {
                Attempt::Completed(Payload::new(*b"null"))
            })
            .open()
            .unwrap()
    };
    {
        let engine = open();
        // Durable once `start` returns; a signal that arrives before its wait is kept for it.
        engine.start("w", "waits", Payload::new(*b"null")).unwrap();
    }
    // The engine is stopped: the CLI records both completions durably.
    assert!(
        cli(&["signal", path, "w", "go", "yes"])
            .unwrap_err()
            .contains("not JSON")
    );
    assert!(cli(&["signal", path, "nobody", "go", "true"]).is_err());
    assert!(
        cli(&["signal", path, "w", "go", "true", "go-1"])
            .unwrap()
            .contains("running")
    );
    assert!(
        cli(&["signal", path, "w", "go", "false", "go-1"]).is_ok(),
        "a retried send is accepted"
    );
    assert!(
        cli(&["complete-awakeable", path, "w", "\"x\""])
            .unwrap_err()
            .contains("not an awakeable")
    );
    cli(&["complete-awakeable", path, "w#0", "\"looks good\""]).unwrap();
    let journal = cli(&["journal", path, "w"]).unwrap();
    assert_eq!(
        journal.matches("signal go (go-1) received").count(),
        1,
        "{journal}"
    );

    let engine = open();
    let done = engine.wait("w", Duration::from_secs(10)).unwrap();
    assert_eq!(done.output, Some(Payload::new(*b"\"looks good true\"")));
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_run_is_cancelled_while_the_engine_is_stopped() {
    let dir = std::env::temp_dir().join(format!("aqueduct-cli-cancel-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.to_str().unwrap();
    let open = || {
        Engine::builder(&dir)
            .workflow(
                "approval",
                Arc::new(Procedure::compile("approval.star", APPROVAL).unwrap()),
            )
            .step("quote", |_: &Payload| {
                Attempt::Completed(Payload::new(*b"42"))
            })
            .open()
            .unwrap()
    };
    open().start("c", "approval", Payload::new(*b"1")).unwrap();
    assert!(
        cli(&["cancel", path, "c", "out of budget"])
            .unwrap()
            .contains("recorded")
    );
    assert!(cli(&["cancel", path, "nobody"]).is_err());
    let engine = open();
    let done = engine.wait("c", Duration::from_secs(10)).unwrap();
    assert_eq!(done.error.as_deref(), Some("cancelled: out of budget"));
    drop(engine);
    assert!(
        cli(&["journal", path, "c"])
            .unwrap()
            .contains("cancel requested: out of budget")
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn escalations_are_listed_and_resolved_while_the_engine_is_stopped() {
    let source = r#"
def run(claim):
    decision = escalate("manual-review", claim)
    if decision != "settle":
        fail("declined: " + decision)
    return "settled"
"#;
    let dir = std::env::temp_dir().join(format!("aqueduct-cli-esc-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.to_str().unwrap();
    let open = || {
        Engine::builder(&dir)
            .workflow(
                "claims",
                Arc::new(Procedure::compile("claims.star", source).unwrap()),
            )
            .open()
            .unwrap()
    };
    {
        let engine = open();
        engine
            .start("c-7", "claims", Payload::new(*b"{\"amount\":9000}"))
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while engine.escalations().unwrap().is_empty() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
        let open = engine.escalations().unwrap();
        assert_eq!(
            (open[0].id.as_str(), open[0].reason.as_str()),
            ("c-7#0", "manual-review")
        );
    }
    let listed = cli(&["escalations", path]).unwrap();
    assert!(
        listed.contains("c-7#0  manual-review  {\"amount\":9000}"),
        "{listed}"
    );
    assert!(
        cli(&["resolve", path, "c-7#0", "settle"])
            .unwrap_err()
            .contains("not JSON")
    );
    assert!(
        cli(&["resolve", path, "c-7", "1"])
            .unwrap_err()
            .contains("not an escalation id")
    );
    cli(&["resolve", path, "c-7#0", "\"settle\""]).unwrap();
    cli(&["resolve", path, "c-7#0", "\"decline\""]).unwrap(); // only the first counts
    assert!(cli(&["escalations", path]).unwrap().is_empty());
    let engine = open();
    let done = engine.wait("c-7", Duration::from_secs(10)).unwrap();
    assert_eq!(done.output, Some(Payload::new(*b"\"settled\"")));
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Answers agent calls from a list, as a recorded model did.
struct Recorded(Vec<&'static str>);
impl aqueduct_engine::AgentExecutor for Recorded {
    fn call(&self, call: &aqueduct_engine::AgentCall<'_>) -> aqueduct_engine::AgentAttempt {
        let input: serde_json::Value = serde_json::from_slice(call.request.as_bytes()).unwrap();
        let i = input["input"].as_u64().unwrap() as usize;
        aqueduct_engine::AgentAttempt::Completed(aqueduct_core::AgentResponse {
            model: "test/old".into(),
            params: Payload::new(*b"{}"),
            response: Payload::new(self.0[i].as_bytes().to_vec()),
            input_tokens: 5,
            output_tokens: 5,
        })
    }
}

/// An OpenAI-compatible endpoint that answers each request from a list, in order.
fn endpoint(
    answers: Vec<&'static str>,
) -> (String, std::thread::JoinHandle<Vec<serde_json::Value>>) {
    use std::io::{BufRead, BufReader, Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    let handle = std::thread::spawn(move || {
        let mut seen = Vec::new();
        for answer in answers {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line.trim_end().is_empty() {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    length = value.trim().parse().unwrap();
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            seen.push(serde_json::from_slice(&body).unwrap());
            let reply = serde_json::json!({
                "model": "llama-next",
                "choices": [{"message": {"role": "assistant", "content": answer}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 40, "completion_tokens": 2}
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
                reply.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        }
        seen
    });
    (url, handle)
}

#[test]
fn agent_calls_are_replayed_against_a_new_model() {
    let source = r#"
def run(docs):
    labels = []
    for i in range(docs):
        labels.append(agent("classify", i, prompt = "classify.md",
                            output_schema = {"enum": ["invoice", "receipt"]}, budget_tokens = 400))
    return labels
"#;
    let dir = std::env::temp_dir().join(format!("aqueduct-cli-drift-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.to_str().unwrap();
    {
        let engine = Engine::builder(&dir)
            .workflow(
                "label",
                Arc::new(Procedure::compile("label.star", source).unwrap()),
            )
            .agent_executor(Arc::new(Recorded(vec![
                "\"invoice\"",
                "\"receipt\"",
                "\"invoice\"",
            ])))
            .open()
            .unwrap();
        engine
            .start("batch-1", "label", Payload::new(*b"3"))
            .unwrap();
        engine.wait("batch-1", Duration::from_secs(10)).unwrap();
    }
    let prompts = dir.join("prompts");
    std::fs::create_dir_all(&prompts).unwrap();
    std::fs::write(prompts.join("classify.md"), "Invoice or receipt?").unwrap();

    assert!(
        cli(&["drift", path, "--model", "ollama/llama-next"])
            .unwrap_err()
            .contains("--max-tokens")
    );
    assert!(
        cli(&[
            "drift",
            path,
            "nope",
            "--model",
            "x/y",
            "--base-url",
            "http://127.0.0.1:9",
            "--max-tokens",
            "1"
        ])
        .unwrap_err()
        .contains("no run `nope`")
    );

    // The new model agrees on the first call, disagrees on the second, and breaks the schema on the third.
    let (url, server) = endpoint(vec!["\"invoice\"", "\"invoice\"", "\"memo\""]);
    let report = cli(&[
        "drift",
        path,
        "batch-1",
        "--model",
        "ollama/llama-next",
        "--base-url",
        &url,
        "--prompts",
        prompts.to_str().unwrap(),
        "--max-tokens",
        "1200",
    ])
    .unwrap();
    let lines: Vec<&str> = report.lines().collect();
    assert!(
        lines[0].starts_with("batch-1#0  classify  same  recorded by test/old"),
        "{report}"
    );
    assert!(
        lines[1].starts_with("batch-1#1  classify  changed")
            && lines[1].contains("\"receipt\" -> \"invoice\" (ollama/llama-next)"),
        "{report}"
    );
    assert!(
        lines[2].starts_with("batch-1#2  classify  invalid  "),
        "{report}"
    );
    assert_eq!(
        lines[3],
        "3 agent calls: 1 same, 1 changed, 0 new, 1 invalid, 0 failed, 0 skipped; 126 tokens used"
    );
    let sent = server.join().unwrap();
    let prompt = sent[0]["messages"][0]["content"].as_str().unwrap();
    assert!(prompt.starts_with("Invoice or receipt?"), "{prompt}");

    // The token cap is never exceeded: only two calls of 400 fit in 1000.
    let (url, server) = endpoint(vec!["\"invoice\"", "\"receipt\""]);
    let capped = cli(&[
        "drift",
        path,
        "--model",
        "ollama/llama-next",
        "--base-url",
        &url,
        "--max-tokens",
        "1000",
    ])
    .unwrap();
    assert!(
        capped.contains("2 same, 0 changed, 0 new, 0 invalid, 0 failed, 1 skipped"),
        "{capped}"
    );
    assert_eq!(server.join().unwrap().len(), 2);

    // A replay writes nothing: the journal is as it was.
    assert!(cli(&["verify", path]).unwrap().contains("batch-1"));
    std::fs::remove_dir_all(&dir).unwrap();
}
