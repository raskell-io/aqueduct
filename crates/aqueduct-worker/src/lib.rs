//! A worker for Aqueduct: implement steps, and run code workflows, in a process of your own, served to a
//! server over the worker protocols (`worker.proto`, ADR 0038; `workflow.proto`, ADR 0039; drafts).
//!
//! **Steps.** The worker says which steps it implements and how many it runs at once, then runs what the
//! server sends it, each on a blocking thread, and sends back the outcome. A step may run more than once
//! (a lease runs out, a connection drops), so it should be idempotent: [`Invocation::idempotency_key`]
//! is the same for every attempt of a step.
//!
//! **Workflows.** A code workflow (any [`Decider`], such as one made with `aqueduct_sdk::workflow`) runs
//! here instead of in the server: the server sends the run's history, the worker verifies its hash chain,
//! replays it through the workflow and sends back the commands the workflow issued.
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

use aqueduct_core::{Decider, Env, RunState};
use aqueduct_protocol::v1;
use aqueduct_protocol::v1::workers_client::WorkersClient;
use aqueduct_protocol::v1::workflows_client::WorkflowsClient;
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

/// A workflow's logic.
pub type WorkflowLogic = Arc<dyn Decider + Send + Sync>;

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

/// A worker: steps and workflows by name, served to a server.
pub struct Worker {
    name: String,
    capacity: u32,
    steps: BTreeMap<String, StepFn>,
    workflows: BTreeMap<String, WorkflowLogic>,
    queues: Vec<String>,
}

impl Worker {
    /// A worker called `name` (for logs and the console), running 4 steps (and 4 activations) at once.
    pub fn new(name: &str) -> Worker {
        Worker {
            name: name.to_owned(),
            capacity: 4,
            steps: BTreeMap::new(),
            workflows: BTreeMap::new(),
            queues: Vec::new(),
        }
    }

    /// How many steps it runs at once, and how many activations.
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

    /// Takes steps from the named task queue `queue` too (ADR 0040), besides the queues of its step
    /// names. The server routes steps to queues; this worker must implement the steps routed there.
    pub fn queue(mut self, queue: &str) -> Worker {
        self.queues.push(queue.to_owned());
        self
    }

    /// Runs workflow `name`'s logic here: the server must have it registered as a remote workflow
    /// (`Builder::remote_workflow`). Each activation is decided on a blocking thread.
    pub fn workflow(mut self, name: &str, logic: WorkflowLogic) -> Worker {
        self.workflows.insert(name.to_owned(), logic);
        self
    }

    /// Serves the steps and workflows to the server at `url` until a session ends.
    pub async fn run(self, url: &str) -> Result<(), WorkerError> {
        self.run_until(url, std::future::pending()).await
    }

    /// Serves until a session ends or `stop` completes. What is in flight when it stops is not
    /// answered; the server offers it again once its lease runs out.
    pub async fn run_until(
        self,
        url: &str,
        stop: impl Future<Output = ()> + Send,
    ) -> Result<(), WorkerError> {
        let steps = async {
            if self.steps.is_empty() {
                return std::future::pending().await;
            }
            serve_steps(url, &self.name, self.capacity, &self.steps, &self.queues).await
        };
        let workflows = async {
            if self.workflows.is_empty() {
                return std::future::pending().await;
            }
            serve_workflows(url, &self.name, self.capacity, &self.workflows).await
        };
        tokio::select! {
            result = steps => result,
            result = workflows => result,
            () = stop => Ok(()),
        }
    }
}

fn connect_error(url: &str) -> impl Fn(tonic::transport::Error) -> WorkerError + '_ {
    move |e| WorkerError::Connect {
        url: url.to_owned(),
        error: e.to_string(),
    }
}

/// A session of the step-worker protocol. Dropping it aborts the steps in flight (their tasks hold the
/// stream open otherwise).
async fn serve_steps(
    url: &str,
    name: &str,
    capacity: u32,
    steps: &BTreeMap<String, StepFn>,
    queues: &[String],
) -> Result<(), WorkerError> {
    let mut client = WorkersClient::connect(url.to_owned())
        .await
        .map_err(connect_error(url))?;
    let (out, outbound) = mpsc::channel(64);
    let hello = v1::WorkerMessage {
        message: Some(v1::worker_message::Message::Hello(v1::Hello {
            worker: name.to_owned(),
            steps: steps.keys().cloned().collect(),
            capacity,
            queues: queues.to_vec(),
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
    let steps = Arc::new(steps.clone());
    let mut running = tokio::task::JoinSet::new();
    loop {
        let message = inbound
            .message()
            .await
            .map_err(|e| WorkerError::Session(e.message().to_owned()))?;
        let Some(message) = message else {
            return Ok(());
        };
        let Some(v1::server_message::Message::Invocation(invocation)) = message.message else {
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

/// A session of the workflow-worker protocol.
async fn serve_workflows(
    url: &str,
    name: &str,
    capacity: u32,
    workflows: &BTreeMap<String, WorkflowLogic>,
) -> Result<(), WorkerError> {
    let mut client = WorkflowsClient::connect(url.to_owned())
        .await
        .map_err(connect_error(url))?;
    let (out, outbound) = mpsc::channel(64);
    let hello = v1::WorkflowWorkerMessage {
        message: Some(v1::workflow_worker_message::Message::Hello(
            v1::WorkflowHello {
                worker: name.to_owned(),
                workflows: workflows.keys().cloned().collect(),
                capacity,
            },
        )),
    };
    out.send(hello)
        .await
        .map_err(|e| WorkerError::Session(e.to_string()))?;
    let mut inbound = client
        .serve(tokio_stream::wrappers::ReceiverStream::new(outbound))
        .await
        .map_err(|e| WorkerError::Session(e.message().to_owned()))?
        .into_inner();
    let workflows = Arc::new(workflows.clone());
    let mut running = tokio::task::JoinSet::new();
    loop {
        let message = inbound
            .message()
            .await
            .map_err(|e| WorkerError::Session(e.message().to_owned()))?;
        let Some(message) = message else {
            return Ok(());
        };
        let Some(v1::workflow_server_message::Message::Activation(activation)) = message.message
        else {
            continue;
        };
        while running.try_join_next().is_some() {}
        let (workflows, out) = (Arc::clone(&workflows), out.clone());
        running.spawn(async move {
            let id = activation.activation_id.clone();
            let decided = tokio::task::spawn_blocking(move || decide(&workflows, &activation))
                .await
                .ok()
                .flatten();
            let message = match decided {
                Some(commands) => v1::workflow_worker_message::Message::Decision(v1::Decision {
                    activation_id: id,
                    commands: commands
                        .iter()
                        .map(aqueduct_protocol::commands::to_proto)
                        .collect(),
                }),
                // No decision (the history did not verify, or the logic panicked): the capacity is
                // free again, and the activation is offered again once its lease runs out.
                None => v1::workflow_worker_message::Message::Credit(1),
            };
            let _ = out
                .send(v1::WorkflowWorkerMessage {
                    message: Some(message),
                })
                .await;
        });
    }
}

/// Verifies an activation's history against its run id, replays it and decides.
fn decide(
    workflows: &BTreeMap<String, WorkflowLogic>,
    activation: &v1::Activation,
) -> Option<Vec<aqueduct_core::Command>> {
    let logic = workflows.get(&activation.workflow)?;
    let verified = aqueduct_codec::verify_chain(
        &activation.run_id,
        activation.history.iter().map(Vec::as_slice),
    )
    .ok()?;
    let state = RunState::from_journal(&verified.entries).ok()?;
    let env = Env {
        now_ms: activation.now_ms,
        seed: activation.seed,
    };
    catch_unwind(AssertUnwindSafe(|| {
        logic.decide(&state.view_at(&activation.run_id, env))
    }))
    .ok()
}
