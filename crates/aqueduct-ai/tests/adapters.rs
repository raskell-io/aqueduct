//! The adapters against a local HTTP server: what they send, and how they read what comes back. No real
//! provider is called.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aqueduct_ai::anthropic::Anthropic;
use aqueduct_ai::openai::OpenAiCompatible;
use aqueduct_ai::{AiExecutor, Model};
use aqueduct_core::{Payload, Status};
use aqueduct_engine::embedded::Engine;
use aqueduct_engine::{AgentAttempt, AgentCall, AgentExecutor, RetryPolicy};
use serde_json::{Value, json};

/// A request the server saw.
#[derive(Clone, Debug)]
struct Seen {
    path: String,
    headers: Vec<(String, String)>,
    body: Value,
}

impl Seen {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Serves `replies` in order, one per connection, and records what it received.
fn server(replies: Vec<(u16, Value)>) -> (String, Arc<Mutex<Vec<Seen>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
    let log = Arc::clone(&seen);
    std::thread::spawn(move || {
        for (status, body) in replies {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let path = line.split_whitespace().nth(1).unwrap_or("").to_owned();
            let mut headers = Vec::new();
            let mut length = 0;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                let header = header.trim_end();
                if header.is_empty() {
                    break;
                }
                let (name, value) = header.split_once(':').unwrap();
                if name.eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse().unwrap();
                }
                headers.push((name.to_owned(), value.trim().to_owned()));
            }
            let mut raw = vec![0; length];
            reader.read_exact(&mut raw).unwrap();
            log.lock().unwrap().push(Seen {
                path,
                headers,
                body: serde_json::from_slice(&raw).unwrap_or(Value::Null),
            });
            let text = body.to_string();
            let reply = format!(
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{text}",
                text.len()
            );
            stream.write_all(reply.as_bytes()).unwrap();
        }
    });
    (url, seen)
}

fn request(schema: Value) -> Payload {
    let body = json!({"input": {"doc": "d1"}, "prompt": "Classify it.", "output_schema": schema});
    Payload::new(body.to_string().into_bytes())
}

fn call(request: &Payload) -> AgentCall<'_> {
    AgentCall {
        run: "r",
        name: "classify",
        request,
        budget_tokens: 2_000,
    }
}

fn message(model: &str, stop_reason: &str, text: &str) -> Value {
    json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": model,
        "content": [{"type": "thinking", "thinking": ""}, {"type": "text", "text": text}],
        "stop_reason": stop_reason, "stop_details": null,
        "usage": {"input_tokens": 120, "output_tokens": 30}
    })
}

#[test]
fn anthropic_sends_a_current_request_and_reads_the_text_blocks() {
    let (url, seen) = server(vec![(
        200,
        message("claude-opus-5-5", "end_turn", "{\"label\":\"invoice\"}"),
    )]);
    let claude = Anthropic::new("sk-test", "claude-opus-5-5")
        .unwrap()
        .base_url(&url);
    let executor = AiExecutor::new(Arc::new(claude));
    let schema = json!({
        "type": "object", "required": ["label"],
        "properties": {"label": {"type": "string", "minLength": 3}}
    });
    let request = request(schema);
    let AgentAttempt::Completed(answer) = executor.call(&call(&request)) else {
        panic!("the call completes");
    };
    assert_eq!(answer.model, "anthropic/claude-opus-5-5");
    assert_eq!(answer.response, Payload::new(*b"{\"label\":\"invoice\"}"));
    assert_eq!((answer.input_tokens, answer.output_tokens), (120, 30));

    let sent = seen.lock().unwrap()[0].clone();
    assert_eq!(sent.path, "/v1/messages");
    assert_eq!(sent.header("x-api-key"), Some("sk-test"));
    assert_eq!(sent.header("anthropic-version"), Some("2026-10-21"));
    assert_eq!(
        sent.header("anthropic-beta"),
        Some("server-side-fallback-2026-07-01")
    );
    let body = &sent.body;
    assert_eq!(body["model"], "claude-opus-5-5");
    assert_eq!(body["fallbacks"], "default");
    assert!(body.get("temperature").is_none() && body.get("top_p").is_none());
    assert!(body["max_tokens"].as_u64().unwrap() < 2_000);
    assert_eq!(body["messages"][0]["role"], "user");
    let format = &body["output_config"]["format"];
    assert_eq!(format["type"], "json_schema");
    assert_eq!(format["schema"]["additionalProperties"], false);
    assert!(
        format["schema"]["properties"]["label"]
            .get("minLength")
            .is_none()
    );
}

#[test]
fn anthropic_errors_are_retried_or_final() {
    let error = |kind: &str| json!({"type": "error", "error": {"type": kind, "message": "m"}});
    let (url, _) = server(vec![
        (529, error("overloaded_error")),
        (429, error("rate_limit_error")),
        (400, error("invalid_request_error")),
        (
            200,
            json!({"model": "claude-opus-5-5", "content": [], "stop_reason": "refusal",
                     "stop_details": {"type": "refusal", "category": "cyber"},
                     "usage": {"input_tokens": 1, "output_tokens": 0}}),
        ),
        (200, message("claude-opus-5-5", "max_tokens", "{\"label\":")),
        // A server-side fallback answered: the model that answered is recorded.
        (200, message("claude-opus-4-8", "end_turn", "\"ok\"")),
    ]);
    let executor = AiExecutor::new(Arc::new(
        Anthropic::new("k", "claude-opus-5-5")
            .unwrap()
            .base_url(&url),
    ));
    let request = request(Value::Null);
    let outcomes: Vec<AgentAttempt> = (0..6).map(|_| executor.call(&call(&request))).collect();
    assert!(matches!(&outcomes[0], AgentAttempt::Retry(why) if why.contains("529")));
    assert!(matches!(&outcomes[1], AgentAttempt::Retry(_)));
    assert!(
        matches!(&outcomes[2], AgentAttempt::Fail(why) if why.contains("invalid_request_error"))
    );
    assert!(
        matches!(&outcomes[3], AgentAttempt::Fail(why) if why.contains("declined (category cyber)"))
    );
    assert!(matches!(&outcomes[4], AgentAttempt::Fail(why) if why.contains("cut off")));
    assert!(
        matches!(&outcomes[5], AgentAttempt::Completed(a) if a.model == "anthropic/claude-opus-4-8")
    );
}

#[test]
fn models_without_server_side_fallback_do_not_ask_for_it() {
    let (url, seen) = server(vec![(
        200,
        message("claude-haiku-5-5", "end_turn", "\"ok\""),
    )]);
    let haiku = Anthropic::new("k", "claude-haiku-5-5")
        .unwrap()
        .base_url(&url);
    let request = request(Value::Null);
    let completion = haiku
        .complete(
            &AiExecutor::new(Arc::new(Anthropic::new("k", "x").unwrap()))
                .request(&call(&request))
                .unwrap(),
        )
        .unwrap();
    assert_eq!(completion.text, "\"ok\"");
    let sent = seen.lock().unwrap()[0].clone();
    assert!(sent.body.get("fallbacks").is_none());
    assert!(sent.header("anthropic-beta").is_none());
}

#[test]
fn openai_compatible_endpoints_get_chat_completions() {
    let completion = |content: &str, finish: &str| {
        json!({"model": "llama3.3", "choices": [{"message": {"role": "assistant", "content": content},
               "finish_reason": finish}], "usage": {"prompt_tokens": 80, "completion_tokens": 12}})
    };
    let (url, seen) = server(vec![
        (200, completion("{\"label\":\"receipt\"}", "stop")),
        (200, completion("{\"lab", "length")),
        (503, json!({"error": {"message": "loading model"}})),
    ]);
    let local = OpenAiCompatible::new(&format!("{url}/v1"), "llama3.3")
        .unwrap()
        .provider("ollama")
        .api_key("local");
    let executor = AiExecutor::new(Arc::new(local)).system("Be brief.");
    let request = request(json!({"type": "object"}));
    let AgentAttempt::Completed(answer) = executor.call(&call(&request)) else {
        panic!("the call completes");
    };
    assert_eq!(answer.model, "ollama/llama3.3");
    assert_eq!(answer.response, Payload::new(*b"{\"label\":\"receipt\"}"));
    assert!(
        matches!(executor.call(&call(&request)), AgentAttempt::Fail(why) if why.contains("cut off"))
    );
    assert!(
        matches!(executor.call(&call(&request)), AgentAttempt::Retry(why) if why.contains("503"))
    );

    let sent = seen.lock().unwrap()[0].clone();
    assert_eq!(sent.path, "/v1/chat/completions");
    assert_eq!(sent.header("authorization"), Some("Bearer local"));
    assert_eq!(
        sent.body["messages"][0],
        json!({"role": "system", "content": "Be brief."})
    );
    assert_eq!(sent.body["response_format"]["type"], "json_schema");
    assert!(sent.body["max_tokens"].as_u64().unwrap() > 0);
}

#[test]
fn a_workflow_calls_claude_through_the_engine() {
    use aqueduct_sdk::{AgentOptions, WorkflowContext, WorkflowError, workflow};

    async fn label(ctx: WorkflowContext, doc: String) -> Result<String, WorkflowError> {
        let options = AgentOptions {
            prompt: "prompts/classify.md".into(),
            output_schema: Some(json!({"enum": ["invoice", "receipt"]})),
            budget_tokens: 4_000,
        };
        ctx.agent("classify", &doc, &options).await
    }
    // The first answer is off schema, and the engine retries the call; the second is right.
    let (url, seen) = server(vec![
        (200, message("claude-opus-5-5", "end_turn", "\"poem\"")),
        (200, message("claude-opus-5-5", "end_turn", "\"invoice\"")),
    ]);
    let agents = AiExecutor::new(Arc::new(
        Anthropic::new("k", "claude-opus-5-5")
            .unwrap()
            .base_url(&url),
    ))
    .prompt("prompts/classify.md", "Is this an invoice or a receipt?");
    let dir = std::env::temp_dir().join(format!("aqueduct-ai-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Engine::builder(&dir)
        .workflow("label", Arc::new(workflow(label)))
        .agent_executor(Arc::new(agents))
        .default_retry_policy(RetryPolicy {
            max_attempts: 3,
            initial_backoff_ms: 10,
            multiplier: 1,
            max_backoff_ms: 10,
        })
        .open()
        .unwrap();
    engine
        .start("doc-1", "label", Payload::new(*b"\"d1\""))
        .unwrap();
    let done = engine.wait("doc-1", Duration::from_secs(10)).unwrap();
    assert_eq!(
        (done.status, done.output),
        (Status::Completed, Some(Payload::new(*b"\"invoice\"")))
    );
    let first = seen.lock().unwrap()[0].clone();
    let prompt = first.body["messages"][0]["content"].as_str().unwrap();
    assert!(
        prompt.starts_with("Is this an invoice or a receipt?"),
        "{prompt}"
    );
    assert_eq!(seen.lock().unwrap().len(), 2);
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}
