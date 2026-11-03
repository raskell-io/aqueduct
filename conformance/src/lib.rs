//! Aqueduct's conformance suite for step workers (`conformance/README.md`): scenarios that every
//! step-worker library must pass, run against a real server.
//!
//! [`run`] starts a server with the scenarios' procedures, starts the worker under test over a
//! [`Transport`], runs every scenario and checks its outcome.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use aqueduct_core::Payload;
use aqueduct_engine::RetryPolicy;
use aqueduct_engine::embedded::{Attempt, Builder, Engine};
use aqueduct_procedure::Procedure;
use serde_json::{Value, json};

/// How the steps reach the server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Transport {
    /// Implemented in the server's own process, as step functions.
    InProcess,
    /// Implemented by `aqueduct-worker`, over gRPC.
    RustGrpc,
    /// Implemented by a worker started with this shell command, with `AQUEDUCT_URL` set.
    Command(String),
}

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Transport::InProcess => f.write_str("in process"),
            Transport::RustGrpc => f.write_str("aqueduct-worker over gRPC"),
            Transport::Command(command) => write!(f, "`{command}`"),
        }
    }
}

/// One scenario: a procedure, its input and the output it must complete with.
pub struct Scenario {
    /// Its name: the workflow's and the run's.
    pub name: &'static str,
    /// The procedure.
    pub source: &'static str,
    /// The run's input.
    pub input: Value,
    /// The output the run must complete with.
    pub expected: Value,
}

/// Every scenario, as `conformance/README.md` lists them.
pub fn scenarios() -> Vec<Scenario> {
    let value = json!({
        "text": "Grüße, 世界 \"quoted\" \\ back",
        "numbers": [0, -1, 2.5, 1234567890],
        "nested": {"empty_list": [], "empty_object": {}, "null": null, "yes": true, "no": false},
    });
    vec![
        Scenario {
            name: "json",
            source: "def run(value):\n    return step(\"conformance.echo\", value)\n",
            input: value.clone(),
            expected: value,
        },
        Scenario {
            name: "failure",
            source: "def run(x):\n    return step(\"conformance.fail\", \"boom\", fallible = True)\n",
            input: Value::Null,
            expected: json!({"ok": false, "error": "boom"}),
        },
        Scenario {
            name: "retry",
            source: "def run(x):\n    return step(\"conformance.flaky\", {\"succeed_on\": 3})\n",
            input: Value::Null,
            expected: json!({"attempt": 3}),
        },
        Scenario {
            name: "exception",
            source: "def run(x):\n    return step(\"conformance.throws\", None)\n",
            input: Value::Null,
            expected: json!("recovered"),
        },
        Scenario {
            name: "identity",
            source: "def run(x):\n    return step(\"conformance.whoami\", None)\n",
            input: Value::Null,
            expected: json!({
                "run_id": "identity", "command": 0, "step": "conformance.whoami",
                "attempt": 1, "idempotency_key": "identity/0/0",
            }),
        },
        Scenario {
            name: "parallel",
            source: "def run(x):\n    slept = parallel([[\"conformance.sleep\", 200]] * 4)\n    return step(\"conformance.echo\", slept)\n",
            input: Value::Null,
            expected: json!([200, 200, 200, 200]),
        },
    ]
}

/// What a scenario came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Check {
    /// The scenario's name.
    pub scenario: &'static str,
    /// Whether it passed.
    pub passed: bool,
    /// What happened, when it did not.
    pub detail: String,
}

/// The steps, in Rust: what every conformance worker implements.
fn step(name: &str, input: &[u8], run: &str, command: u32, attempt: u32, key: &str) -> Attempt {
    let input: Value = serde_json::from_slice(input).unwrap_or(Value::Null);
    let done = |value: Value| Attempt::Completed(Payload::new(value.to_string().into_bytes()));
    match name {
        "conformance.echo" => done(input),
        "conformance.fail" => Attempt::Fail(input.as_str().unwrap_or_default().to_owned()),
        "conformance.flaky" => {
            let succeed_on = input["succeed_on"].as_u64().unwrap_or(1);
            if u64::from(attempt) < succeed_on {
                Attempt::Retry(format!("attempt {attempt} of {succeed_on}"))
            } else {
                done(json!({ "attempt": attempt }))
            }
        }
        // A panic on the first attempt: unclassified, so a retry.
        "conformance.throws" if attempt == 1 => panic!("conformance.throws: first attempt"),
        "conformance.throws" => done(json!("recovered")),
        "conformance.whoami" => done(json!({
            "run_id": run, "command": command, "step": name, "attempt": attempt,
            "idempotency_key": key,
        })),
        "conformance.sleep" => {
            let ms = input.as_u64().unwrap_or(0);
            std::thread::sleep(Duration::from_millis(ms));
            done(json!(ms))
        }
        other => Attempt::Fail(format!("unknown step {other}")),
    }
}

const STEPS: [&str; 6] = [
    "conformance.echo",
    "conformance.fail",
    "conformance.flaky",
    "conformance.throws",
    "conformance.whoami",
    "conformance.sleep",
];

fn engine(dir: &std::path::Path, transport: &Transport) -> Result<Engine, String> {
    let mut builder: Builder = Engine::builder(dir).default_retry_policy(RetryPolicy {
        max_attempts: 10,
        initial_backoff_ms: 10,
        multiplier: 1,
        max_backoff_ms: 10,
    });
    for scenario in scenarios() {
        let procedure = Procedure::compile(&format!("{}.star", scenario.name), scenario.source)
            .map_err(|e| e.to_string())?;
        builder = builder.workflow(scenario.name, Arc::new(procedure));
    }
    builder = match transport {
        Transport::InProcess => STEPS.iter().fold(builder.workers(8), |builder, name| {
            builder.step_with(name, |call| {
                step(
                    call.step,
                    call.input.as_bytes(),
                    call.run,
                    call.cmd.0,
                    call.attempt,
                    call.idempotency_key().as_str(),
                )
            })
        }),
        _ => builder.remote_steps(Duration::from_secs(10)),
    };
    builder.open().map_err(|e| e.to_string())
}

/// Runs every scenario over `transport`.
pub async fn run(transport: &Transport) -> Result<Vec<Check>, String> {
    static RUNS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "aqueduct-conformance-{}-{}",
        std::process::id(),
        RUNS.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Arc::new(engine(&dir, transport)?);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    let url = format!(
        "http://{}",
        listener.local_addr().map_err(|e| e.to_string())?
    );
    let server = tokio::spawn(aqueduct_server::serve(
        Arc::clone(&engine),
        listener,
        std::future::pending(),
    ));

    // The worker under test.
    let mut child = None;
    let mut worker = None;
    match transport {
        Transport::InProcess => {}
        Transport::RustGrpc => {
            let built = STEPS.iter().fold(
                aqueduct_worker::Worker::new("conformance").capacity(8),
                |worker, name| {
                    worker.step(name, |i| {
                        match step(
                            &i.step,
                            &i.input,
                            &i.run_id,
                            i.command,
                            i.attempt,
                            &i.idempotency_key,
                        ) {
                            Attempt::Completed(result) => {
                                aqueduct_worker::Outcome::Completed(result.0)
                            }
                            Attempt::Fail(error) => aqueduct_worker::Outcome::Failed(error),
                            Attempt::Retry(error) => aqueduct_worker::Outcome::Retry(error),
                        }
                    })
                },
            );
            let url = url.clone();
            worker = Some(tokio::spawn(async move { built.run(&url).await }));
        }
        Transport::Command(command) => {
            child = Some(
                tokio::process::Command::new("sh")
                    .arg("-c")
                    .arg(command)
                    .env("AQUEDUCT_URL", &url)
                    .kill_on_drop(true)
                    .spawn()
                    .map_err(|e| format!("cannot start `{command}`: {e}"))?,
            );
        }
    }

    let mut checks = Vec::new();
    for scenario in scenarios() {
        let input = Payload::new(scenario.input.to_string().into_bytes());
        let engine = Arc::clone(&engine);
        let name = scenario.name;
        let result = tokio::task::spawn_blocking(move || {
            engine.start(name, name, input)?;
            engine.wait(name, Duration::from_secs(30))
        })
        .await
        .map_err(|e| e.to_string())?;
        let check = match result {
            Err(error) => Check {
                scenario: name,
                passed: false,
                detail: error.to_string(),
            },
            Ok(result) => {
                let output: Value = result
                    .output
                    .as_ref()
                    .and_then(|o| serde_json::from_slice(o.as_bytes()).ok())
                    .unwrap_or(Value::Null);
                let passed = result.status == aqueduct_core::Status::Completed
                    && output == scenario.expected;
                Check {
                    scenario: name,
                    passed,
                    detail: if passed {
                        String::new()
                    } else {
                        format!(
                            "{:?} with {output}{}, expected {}",
                            result.status,
                            result.error.map(|e| format!(" ({e})")).unwrap_or_default(),
                            scenario.expected
                        )
                    },
                }
            }
        };
        checks.push(check);
    }

    if let Some(mut child) = child {
        let _ = child.kill().await;
    }
    if let Some(worker) = worker {
        worker.abort();
    }
    server.abort();
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(checks)
}
