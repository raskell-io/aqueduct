//! Step workers (ADR 0038, `worker.proto`): a gRPC stream with credit, and HTTP long polling, both over
//! the engine's step queue.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use aqueduct_engine::embedded::{Attempt, Engine};
use aqueduct_engine::queue::{Invocation, StepQueue};
use aqueduct_protocol::v1;
use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Notify, mpsc};

use crate::{Api, Body, Failure, Kind, json_payload, value};

/// How long one take waits for work before looking at the session again.
const TAKE_WAIT: Duration = Duration::from_millis(500);
/// The longest an HTTP poll may wait.
const MAX_POLL_WAIT: Duration = Duration::from_secs(30);

/// Sessions so far: each gets its own name, so that ending one releases only its leases.
static SESSIONS: AtomicU64 = AtomicU64::new(0);

impl Api {
    fn queue(&self) -> Result<Arc<StepQueue>, Failure> {
        self.engine.step_queue().cloned().ok_or(Failure {
            kind: Kind::Unavailable,
            message: "this server runs no remote steps".into(),
        })
    }
}

/// Records a worker's result, if the queue still waits for it.
pub(crate) fn record(
    engine: &Engine,
    queue: &StepQueue,
    id: &str,
    attempt: Attempt,
) -> Result<(), Failure> {
    if let Some((run, cmd)) = queue.complete(id) {
        engine.step_result(&run, cmd, attempt)?;
    }
    Ok(())
}

fn invocation(invocation: Invocation) -> v1::StepInvocation {
    v1::StepInvocation {
        invocation_id: invocation.id,
        run_id: invocation.run,
        command: invocation.cmd.0,
        step: invocation.step,
        input: invocation.input.0,
        idempotency_key: invocation.idempotency_key,
        attempt: invocation.attempt,
        lease_ms: invocation.lease_ms,
    }
}

fn attempt(outcome: Option<v1::step_result::Outcome>) -> Result<Attempt, Failure> {
    match outcome {
        Some(v1::step_result::Outcome::Completed(result)) => {
            Ok(Attempt::Completed(json_payload("the result", result)?))
        }
        Some(v1::step_result::Outcome::Failed(error)) => Ok(Attempt::Fail(error)),
        Some(v1::step_result::Outcome::Retry(error)) => Ok(Attempt::Retry(error)),
        None => Err(Failure::invalid("a result needs an outcome")),
    }
}

type WorkStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<v1::ServerMessage, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl v1::workers_server::Workers for Api {
    type WorkStream = WorkStream;

    async fn work(
        &self,
        request: tonic::Request<tonic::Streaming<v1::WorkerMessage>>,
    ) -> Result<tonic::Response<WorkStream>, tonic::Status> {
        let queue = self.queue()?;
        let mut inbound = request.into_inner();
        let hello = match inbound.message().await? {
            Some(v1::WorkerMessage {
                message: Some(v1::worker_message::Message::Hello(hello)),
            }) => hello,
            _ => {
                return Err(tonic::Status::invalid_argument(
                    "a worker's first message is its Hello",
                ));
            }
        };
        let session = format!(
            "{}#{}",
            hello.worker,
            SESSIONS.fetch_add(1, Ordering::Relaxed)
        );
        // The queues it takes from: those of its step names, and the named ones.
        let mut queues = hello.steps;
        queues.extend(hello.queues);
        let steps = Arc::new(queues);
        let credit = Arc::new(AtomicU32::new(hello.capacity.max(1)));
        let closed = Arc::new(AtomicBool::new(false));
        let changed = Arc::new(Notify::new());
        let (out, stream) = mpsc::channel(64);

        // Results and credit from the worker.
        {
            let (engine, queue) = (Arc::clone(&self.engine), Arc::clone(&queue));
            let (credit, closed, changed) = (credit.clone(), closed.clone(), changed.clone());
            let (session, gone) = (session.clone(), out.clone());
            tokio::spawn(async move {
                loop {
                    // The session ends when the worker's stream ends, or it stops reading ours.
                    let message = tokio::select! {
                        message = inbound.message() => message,
                        () = gone.closed() => break,
                    };
                    let Ok(Some(message)) = message else {
                        break;
                    };
                    match message.message {
                        Some(v1::worker_message::Message::Result(result)) => {
                            if let Ok(outcome) = attempt(result.outcome) {
                                let _ = record(&engine, &queue, &result.invocation_id, outcome);
                            }
                            credit.fetch_add(1, Ordering::SeqCst);
                        }
                        Some(v1::worker_message::Message::Credit(more)) => {
                            credit.fetch_add(more, Ordering::SeqCst);
                        }
                        _ => {}
                    }
                    changed.notify_one();
                }
                // The session ended: what it holds is offered to others at once.
                closed.store(true, Ordering::SeqCst);
                changed.notify_one();
                queue.release(&session);
            });
        }

        // Invocations to the worker, never more than its credit.
        tokio::spawn(async move {
            serve_invocations(queue, session, steps, credit, closed, changed, out).await;
        });
        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(stream),
        )))
    }
}

/// Sends a session invocations while it has credit. When the session ends, whatever a take in flight
/// leased to it is released too.
async fn serve_invocations(
    queue: Arc<StepQueue>,
    session: String,
    steps: Arc<Vec<String>>,
    credit: Arc<AtomicU32>,
    closed: Arc<AtomicBool>,
    changed: Arc<Notify>,
    out: mpsc::Sender<Result<v1::ServerMessage, tonic::Status>>,
) {
    'session: {
        while !closed.load(Ordering::SeqCst) {
            let available = credit.load(Ordering::SeqCst);
            if available == 0 {
                changed.notified().await;
                continue;
            }
            let (queue, steps, name) = (Arc::clone(&queue), Arc::clone(&steps), session.clone());
            let taken = tokio::task::spawn_blocking(move || {
                queue.wait_take(&name, &steps, available as usize, TAKE_WAIT)
            })
            .await
            .unwrap_or_default();
            for taken in taken {
                credit.fetch_sub(1, Ordering::SeqCst);
                let message = v1::ServerMessage {
                    message: Some(v1::server_message::Message::Invocation(invocation(taken))),
                };
                if out.send(Ok(message)).await.is_err() {
                    break 'session;
                }
            }
        }
    }
    queue.release(&session);
}

// HTTP long polling.

#[derive(Deserialize)]
struct Poll {
    worker: String,
    #[serde(default)]
    steps: Vec<String>,
    /// Named task queues, besides the queues of its step names.
    #[serde(default)]
    queues: Vec<String>,
    #[serde(default = "one")]
    max: usize,
    #[serde(default)]
    wait_ms: u64,
}

fn one() -> usize {
    1
}

async fn http_poll(State(api): State<Api>, Body(poll): Body<Poll>) -> Result<Json<Value>, Failure> {
    let queue = api.queue()?;
    let wait = Duration::from_millis(poll.wait_ms).min(MAX_POLL_WAIT);
    let mut queues = poll.steps;
    queues.extend(poll.queues);
    let taken =
        tokio::task::spawn_blocking(move || queue.wait_take(&poll.worker, &queues, poll.max, wait))
            .await
            .unwrap_or_default();
    let invocations: Vec<Value> = taken
        .into_iter()
        .map(|i| {
            json!({
                "invocation_id": i.id,
                "run_id": i.run,
                "command": i.cmd.0,
                "step": i.step,
                "input": value(i.input.as_bytes()),
                "idempotency_key": i.idempotency_key,
                "attempt": i.attempt,
                "lease_ms": i.lease_ms,
            })
        })
        .collect();
    Ok(Json(json!({ "invocations": invocations })))
}

#[derive(Deserialize)]
struct Results {
    /// Objects with an `invocation_id` and exactly one of `completed` (any JSON, `null` included),
    /// `failed` and `retry`.
    results: Vec<serde_json::Map<String, Value>>,
}

async fn http_results(
    State(api): State<Api>,
    Body(results): Body<Results>,
) -> Result<Json<Value>, Failure> {
    let queue = api.queue()?;
    for result in results.results {
        let text = |field: &str| result.get(field).map(|v| v.as_str().map(str::to_owned));
        let outcome = match (result.get("completed"), text("failed"), text("retry")) {
            (Some(value), None, None) => {
                Attempt::Completed(aqueduct_core::Payload::new(value.to_string().into_bytes()))
            }
            (None, Some(Some(error)), None) => Attempt::Fail(error),
            (None, None, Some(Some(error))) => Attempt::Retry(error),
            _ => {
                return Err(Failure::invalid(
                    "a result has exactly one of completed (any JSON), failed and retry (strings)",
                ));
            }
        };
        let Some(id) = result.get("invocation_id").and_then(Value::as_str) else {
            return Err(Failure::invalid("a result needs its invocation_id"));
        };
        record(&api.engine, &queue, id, outcome)?;
    }
    Ok(Json(json!({})))
}

/// The workers' HTTP long-polling routes (their gRPC service is mounted with the ingress's).
pub(crate) fn routes(api: Api) -> Router {
    Router::new()
        .route("/v1/workers/poll", post(http_poll))
        .route("/v1/workers/results", post(http_results))
        .with_state(api)
}
