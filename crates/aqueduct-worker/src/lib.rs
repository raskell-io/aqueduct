//! A step worker for Aqueduct (ADR 0038): implement steps by name in Rust, and serve them to a server
//! over the step-worker protocol (`worker.proto`, draft).
//!
//! The worker says which steps it implements and how many it runs at once, then runs what the server
//! sends it, each on a blocking thread, and sends back the outcome. A step may run more than once (a
//! lease runs out, a connection drops), so it should be idempotent: [`Invocation::idempotency_key`] is
//! the same for every attempt of a step.
//!
//! ```no_run
//! use aqueduct_worker::{Outcome, Worker};
//!
//! # async fn example() -> Result<(), aqueduct_worker::WorkerError> {
//! Worker::new("billing")
//!     .capacity(8)
//!     .step("charge", |invocation| {
//!         // The input is JSON; so is the result.
//!         Outcome::Completed(format!("{{\"charged\": {}}}", String::from_utf8_lossy(&invocation.input)).into_bytes())
//!     })
//!     .run("http://127.0.0.1:7470")
//!     .await
//! # }
//! ```

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use aqueduct_protocol::v1;
use aqueduct_protocol::v1::workers_client::WorkersClient;
use tokio::sync::mpsc;

/// What one attempt of a step came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The step's result, in the run's codec (JSON by default).
    Completed(Vec<u8>),
    /// A terminal failure: the step fails, whatever its retry policy.
    Failed(String),
    /// A retryable failure: the server retries the step by its retry policy, with durable backoff.
    Retry(String),
}

/// One attempt of a step, as the server sent it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Invocation {
    /// The run's run id.
    pub run_id: String,
    /// The step's command index in the run.
    pub command: u32,
    /// The step's name.
    pub step: String,
    /// The step's input, in the run's codec (JSON by default).
    pub input: Vec<u8>,
    /// The same for every attempt of this step: pass it to external systems to deduplicate.
    pub idempotency_key: String,
    /// 1 for the first attempt.
    pub attempt: u32,
}

/// A step's implementation.
pub type StepFn = Arc<dyn Fn(&Invocation) -> Outcome + Send + Sync>;

/// Why a worker stopped.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// The server could not be reached.
    #[error("cannot connect to {url}: {error}")]
    Connect {
        /// The server's address.
        url: String,
        /// Why.
        error: String,
    },
    /// The server ended the session or refused it.
    #[error("the server ended the session: {0}")]
    Session(String),
}

/// A step worker: a set of steps by name, run against a server.
pub struct Worker {
    name: String,
    capacity: u32,
    steps: BTreeMap<String, StepFn>,
}

impl Worker {
    /// A worker called `name` (for logs and the console), running 4 steps at once.
    pub fn new(name: &str) -> Worker {
        Worker {
            name: name.to_owned(),
            capacity: 4,
            steps: BTreeMap::new(),
        }
    }

    /// How many steps it runs at once.
    pub fn capacity(mut self, capacity: u32) -> Worker {
        self.capacity = capacity.max(1);
        self
    }

    /// Implements step `name`. The function runs on a blocking thread; a panic is a retryable
    /// failure.
    pub fn step(
        mut self,
        name: &str,
        step: impl Fn(&Invocation) -> Outcome + Send + Sync + 'static,
    ) -> Worker {
        self.steps.insert(name.to_owned(), Arc::new(step));
        self
    }

    /// Serves the steps to the server at `url` until the session ends.
    pub async fn run(self, url: &str) -> Result<(), WorkerError> {
        self.run_until(url, std::future::pending()).await
    }

    /// Serves the steps until the session ends or `stop` completes. Steps in flight when it stops are
    /// not answered; the server offers them again once their leases run out.
    pub async fn run_until(
        self,
        url: &str,
        stop: impl Future<Output = ()> + Send,
    ) -> Result<(), WorkerError> {
        let mut client =
            WorkersClient::connect(url.to_owned())
                .await
                .map_err(|e| WorkerError::Connect {
                    url: url.to_owned(),
                    error: e.to_string(),
                })?;
        let (out, outbound) = mpsc::channel(64);
        let hello = v1::WorkerMessage {
            message: Some(v1::worker_message::Message::Hello(v1::Hello {
                worker: self.name.clone(),
                steps: self.steps.keys().cloned().collect(),
                capacity: self.capacity,
            })),
        };
        out.send(hello)
            .await
            .map_err(|e| WorkerError::Session(e.to_string()))?;
        let mut inbound = client
            .work(tokio_stream::wrappers::ReceiverStream::new(outbound))
            .await
            .map_err(|e| WorkerError::Session(e.message().to_owned()))?
            .into_inner();
        let steps = Arc::new(self.steps);
        // The steps in flight: aborted when the worker stops, so that nothing keeps its stream open.
        let mut running = tokio::task::JoinSet::new();
        let session = async {
            loop {
                let message = inbound
                    .message()
                    .await
                    .map_err(|e| WorkerError::Session(e.message().to_owned()))?;
                let Some(v1::ServerMessage {
                    message: Some(v1::server_message::Message::Invocation(invocation)),
                }) = message
                else {
                    if message.is_none() {
                        return Ok(());
                    }
                    continue;
                };
                while running.try_join_next().is_some() {}
                let (steps, out) = (Arc::clone(&steps), out.clone());
                running.spawn(async move {
                    let id = invocation.invocation_id.clone();
                    let invocation = Invocation {
                        run_id: invocation.run_id,
                        command: invocation.command,
                        step: invocation.step,
                        input: invocation.input,
                        idempotency_key: invocation.idempotency_key,
                        attempt: invocation.attempt,
                    };
                    let outcome = tokio::task::spawn_blocking(move || execute(&steps, &invocation))
                        .await
                        .unwrap_or_else(|e| Outcome::Retry(e.to_string()));
                    let outcome = match outcome {
                        Outcome::Completed(result) => v1::step_result::Outcome::Completed(result),
                        Outcome::Failed(error) => v1::step_result::Outcome::Failed(error),
                        Outcome::Retry(error) => v1::step_result::Outcome::Retry(error),
                    };
                    let _ = out
                        .send(v1::WorkerMessage {
                            message: Some(v1::worker_message::Message::Result(v1::StepResult {
                                invocation_id: id,
                                outcome: Some(outcome),
                            })),
                        })
                        .await;
                });
            }
        };
        let result = tokio::select! {
            result = session => result,
            () = stop => Ok(()),
        };
        running.abort_all();
        result
    }
}

fn execute(steps: &BTreeMap<String, StepFn>, invocation: &Invocation) -> Outcome {
    match steps.get(&invocation.step) {
        // The server sends only the steps a worker named; a mismatch heals when another worker takes it.
        None => Outcome::Retry(format!(
            "this worker does not implement `{}`",
            invocation.step
        )),
        Some(step) => catch_unwind(AssertUnwindSafe(|| step(invocation)))
            .unwrap_or_else(|_| Outcome::Retry(format!("step `{}` panicked", invocation.step))),
    }
}
