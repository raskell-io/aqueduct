//! The embedded engine: a partition, step workers and clients in one process, on a durable file log.
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use aqueduct_core::{Payload, StepOutcome};
//! use aqueduct_engine::embedded::Engine;
//! # fn decider() -> Arc<dyn aqueduct_core::Decider + Send + Sync> { unimplemented!() }
//!
//! let engine = Engine::builder("./data")
//!     .workflow("greet", decider())
//!     .step("lookup_name", |input: &Payload| StepOutcome::Completed(input.clone()))
//!     .open()?;
//! engine.start("greet:u-1", "greet", Payload::new(br#""u-1""#.to_vec()))?; // durable when it returns
//! let result = engine.wait("greet:u-1", Duration::from_secs(10))?;
//! # Ok::<(), aqueduct_engine::embedded::EngineError>(())
//! ```
//!
//! Threads: one partition thread owns the log and the runs. It drains every waiting request, applies
//! them, and syncs once (group commit) before releasing acknowledgements, dispatches and results. A
//! pool of worker threads executes steps. `start` returns only after the run's start is durable.
//!
//! Timers fire no earlier than their wake time, by the wall clock; signals are durable when
//! [`Engine::signal`] returns.
//!
//! Steps return an [`Attempt`]: retryable failures are retried by the step's [`RetryPolicy`] with
//! durable, journaled backoff; terminal failures fail the step at once.
//!
//! Every `checkpoint_every` log records, and on shutdown, the engine saves a checkpoint of the runs that
//! changed to `state.redb` (ADR 0009); recovery loads it and replays only the log after it, and log
//! segments it covers are deleted.
//!
//! Finished runs leave memory once a checkpoint holds them; `wait`, repeated starts and signals read them
//! back from `state.redb`. Timers are kept in an index ordered by wake time.
//!
//! Not yet: loading only active runs at startup (recovery still folds every run in the checkpoint)
//! (recovery replays the whole log; checkpoints in redb come with ADR 0009's state store).

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use aqueduct_core::{CommandIndex, Decider, Payload, RunState, Status, StepOutcome};
use aqueduct_storage::{FileLog, FileLogOptions, LogStore, RedbStateStore};

use crate::{
    AgentAttempt, AgentCall, AgentExecutor, Deciders, Observer, Outbound, Partition,
    PartitionError, RetryPolicy, ReusePolicy, StartOptions, UpdateStatus,
};

/// A step implementation.
pub type StepFn = Arc<dyn Fn(&StepCall<'_>) -> Attempt + Send + Sync>;

/// One attempt of a step, as a step function registered with [`Builder::step_with`] sees it.
#[derive(Clone, Copy, Debug)]
pub struct StepCall<'a> {
    /// The run's run id.
    pub run: &'a str,
    /// The step's command index in the run.
    pub cmd: CommandIndex,
    /// The step's name.
    pub step: &'a str,
    /// The step's input.
    pub input: &'a Payload,
    /// 1 for the first attempt: the attempts the run has recorded as failed, plus one.
    pub attempt: u32,
}

impl StepCall<'_> {
    /// The same for every attempt of this step: pass it to external systems to deduplicate.
    pub fn idempotency_key(&self) -> aqueduct_core::IdempotencyKey {
        aqueduct_core::IdempotencyKey::new(aqueduct_core::split_run_id(self.run).0, 0, self.cmd)
    }
}

/// The result of one attempt of a step (ADR 0012).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Attempt {
    /// The step completed with a result.
    Completed(Payload),
    /// The attempt failed, but another attempt may succeed (a timeout, an unavailable service). The
    /// step's retry policy decides whether and when it runs again.
    Retry(String),
    /// The step failed and retrying cannot help (invalid input, a rejected request).
    Fail(String),
}

/// A step returning [`StepOutcome`] is unclassified: `Failed` is treated as retryable.
impl From<StepOutcome> for Attempt {
    fn from(outcome: StepOutcome) -> Self {
        match outcome {
            StepOutcome::Completed(result) => Attempt::Completed(result),
            StepOutcome::Failed(error) => Attempt::Retry(error),
        }
    }
}

/// How a run ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunResult {
    /// The terminal status.
    pub status: Status,
    /// The output, if the run completed.
    pub output: Option<Payload>,
    /// The error or parking reason, if the run failed or was parked.
    pub error: Option<String>,
    /// The version the run was pinned to, if it ran on a registered version (ADR 0029).
    pub version: Option<String>,
}

/// An escalation waiting for a person or an agent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Escalation {
    /// The id to resolve it with: `<run key>#<command index>`.
    pub id: String,
    /// The run key.
    pub key: String,
    /// Why the run escalated.
    pub reason: String,
    /// What the resolver needs to decide.
    pub payload: Payload,
}

/// A run, as [`Engine::runs`] lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunSummary {
    /// The run id: the key for its first run, `<key>\0<generation>` after that.
    pub run_id: String,
    /// The workflow it runs.
    pub workflow: String,
    /// Its status.
    pub status: Status,
    /// The version it is pinned to, if any.
    pub version: Option<String>,
    /// Its journal's length, in entries.
    pub entries: usize,
}

/// A snapshot of the engine's size, for operations and tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EngineStats {
    /// Runs held in memory (active runs, and finished runs not yet in a checkpoint).
    pub runs_in_memory: usize,
    /// Timers that have not fired.
    pub pending_timers: usize,
    /// The LSN the next log record will get.
    pub log_lsn: u64,
}

/// Errors from the embedded engine.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    /// The workflow is not registered.
    #[error("unknown workflow `{0}`")]
    UnknownWorkflow(String),
    /// The workflow has no version with this id.
    #[error("workflow `{workflow}` has no version `{version}`")]
    UnknownVersion {
        /// The workflow.
        workflow: String,
        /// The version asked for.
        version: String,
    },
    /// No run with this key exists.
    #[error("no run `{0}`")]
    UnknownRun(String),
    /// The run did not finish within the timeout.
    #[error("timed out waiting for run `{0}`")]
    Timeout(String),
    /// The engine stopped, for example after a storage failure. Reopen it to recover.
    #[error("the engine has stopped: {0}")]
    Stopped(String),
    /// A run with this key exists with another workflow or input.
    #[error("run `{0}` exists with another workflow or input")]
    KeyConflict(String),
    /// Run keys must not be empty or contain NUL.
    #[error("invalid run key {0:?}")]
    InvalidKey(String),
    /// A resolution was refused (for example, an answer that does not match the output schema).
    #[error("{0}")]
    InvalidResolution(String),
    /// A run with this key exists, and the start used [`ReusePolicy::Reject`].
    #[error("run `{0}` already exists")]
    AlreadyStarted(String),
    /// The run rejected the update, could not take it (it has ended, or waits for no update of this
    /// name), or ended before answering it.
    #[error("update rejected: {0}")]
    UpdateRejected(String),
    /// The request needs something not available for this run yet (for example, a query on a workflow
    /// decided remotely).
    #[error("{0}")]
    Unsupported(String),
    /// The engine is running as many runs as it was told to; try again later.
    #[error("{0}")]
    Overloaded(String),
    /// An object's handler failed; it committed nothing.
    #[error("handler failed: {0}")]
    HandlerFailed(String),
    /// A payload is not JSON.
    #[error("{0}")]
    InvalidPayload(String),
    /// The run's logic has no such query.
    #[error("run `{key}` has no query `{name}`")]
    UnknownQuery {
        /// The run key.
        key: String,
        /// The query asked for.
        name: String,
    },
    /// The id is not an escalation id (`<run key>#<command index>`).
    #[error("`{0}` is not an escalation id")]
    InvalidEscalation(String),
    /// The id is not an awakeable id (`<run key>#<command index>`).
    #[error("`{0}` is not an awakeable id")]
    InvalidAwakeable(String),
    /// Opening or recovering the engine failed.
    #[error("cannot open the engine: {0}")]
    Open(String),
}

/// Configures and opens an [`Engine`].
pub struct Builder {
    dir: PathBuf,
    deciders: Deciders,
    steps: BTreeMap<String, StepFn>,
    workers: usize,
    redispatch: Duration,
    log: FileLogOptions,
    checkpoint_every: u64,
    ack_before_sync: bool,
    retry_policies: BTreeMap<String, RetryPolicy>,
    default_retry: RetryPolicy,
    observer: Option<Arc<dyn Observer>>,
    agents: Option<Arc<dyn AgentExecutor>>,
    activations: Vec<(String, String)>,
    remote: Option<Duration>,
    /// Whether some workflow is decided remotely, and the leases of its activations.
    remote_workflows: Option<Duration>,
    /// Remote steps routed to named task queues, by step name.
    routes: BTreeMap<String, String>,
    max_running: Option<usize>,
}

impl Builder {
    /// Hands every step without a function of its own to remote step workers (ADR 0038), through the
    /// engine's [`StepQueue`](crate::queue::StepQueue), with leases of `lease`. A step that no worker
    /// implements waits until one connects. Without this, such a step is retried as a deployment
    /// error.
    pub fn remote_steps(mut self, lease: Duration) -> Self {
        self.remote = Some(lease);
        self
    }

    /// Registers a virtual object whose handlers are workflows registered under `<object>.<handler>` some
    /// other way: as registry versions, or remote workflows (ADR 0043).
    pub fn object(mut self, object: &str) -> Self {
        self.deciders.insert(
            crate::objects::loop_workflow(object),
            Arc::new(crate::objects::ObjectLoop::new(object)),
        );
        self
    }

    /// Registers a handler of a virtual object (ADR 0043): `decider` (for a Rust handler,
    /// `aqueduct_sdk::object_handler`) runs for each call of `object`'s `handler`, one at a time per key.
    pub fn object_handler(
        mut self,
        object: &str,
        handler: &str,
        decider: Arc<dyn Decider + Send + Sync>,
    ) -> Self {
        self.deciders
            .insert(crate::objects::handler_workflow(object, handler), decider);
        self.deciders.insert(
            crate::objects::loop_workflow(object),
            Arc::new(crate::objects::ObjectLoop::new(object)),
        );
        self
    }

    /// Refuses starts of new keys and new object calls with [`EngineError::Overloaded`] while this many
    /// runs are running. Retried starts are not refused, nor what runs already do (child runs,
    /// continue-as-new, messages). An object key's own loop does not count; its calls do.
    pub fn max_running_runs(mut self, limit: usize) -> Self {
        self.max_running = Some(limit);
        self
    }

    /// Routes remote step `step` to the named task queue `queue` (ADR 0040): it goes to workers that
    /// serve that queue, and push endpoints configured for it. Without a route, a step waits in the
    /// queue of its own name.
    pub fn step_route(mut self, step: &str, queue: &str) -> Self {
        self.routes.insert(step.to_owned(), queue.to_owned());
        self
    }

    /// Registers a workflow that a remote workflow worker decides (ADR 0039): its activations go to the
    /// engine's [`ActivationQueue`](crate::activations::ActivationQueue), with leases of 30 s (see
    /// [`Builder::activation_lease`]), and its decisions come back through [`Engine::decision`]. Runs
    /// wait until a worker for the workflow connects.
    pub fn remote_workflow(mut self, name: &str) -> Self {
        self.deciders.insert_remote(name);
        self.remote_workflows.get_or_insert(Duration::from_secs(30));
        self
    }

    /// How long a remote workflow worker has to decide an activation before it is offered again.
    pub fn activation_lease(mut self, lease: Duration) -> Self {
        self.remote_workflows = Some(lease);
        self
    }

    /// Sets the executor of agent calls (`aqueduct-ai` provides them for model providers). Without one,
    /// agent calls are retried until one is registered.
    pub fn agent_executor(mut self, executor: Arc<dyn AgentExecutor>) -> Self {
        self.agents = Some(executor);
        self
    }

    /// Tells `observer` about durable entries, step executions and flushes, for telemetry
    /// (`aqueduct-otel` turns them into OpenTelemetry traces and metrics).
    pub fn observer(mut self, observer: Arc<dyn Observer>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Registers a workflow (a procedure or any other decider), unversioned: its runs are decided by
    /// whatever is registered under the name when they resume.
    pub fn workflow(mut self, name: &str, decider: Arc<dyn Decider + Send + Sync>) -> Self {
        self.deciders.insert(name, decider);
        self
    }

    /// Registers a version of a workflow (ADR 0029; for a procedure, `procedure.version()`). Runs
    /// started on it are pinned to it, and wait while it is not registered. Several versions of one
    /// workflow can be registered at once, so old runs finish on the version they started on.
    pub fn workflow_version(
        mut self,
        name: &str,
        version: &str,
        decider: Arc<dyn Decider + Send + Sync>,
    ) -> Self {
        self.deciders.insert_version(name, version, decider);
        self
    }

    /// Makes a registered version the one new runs of the workflow start on. Opening fails if the
    /// version is not registered.
    pub fn active_version(mut self, name: &str, version: &str) -> Self {
        self.activations.push((name.to_owned(), version.to_owned()));
        self
    }

    /// Registers a step implementation. It may return an [`Attempt`], or a [`StepOutcome`] whose
    /// failures are treated as retryable.
    pub fn step<R: Into<Attempt>>(
        mut self,
        name: &str,
        step: impl Fn(&Payload) -> R + Send + Sync + 'static,
    ) -> Self {
        self.steps.insert(
            name.to_owned(),
            Arc::new(move |call: &StepCall<'_>| step(call.input).into()),
        );
        self
    }

    /// Registers a step function that also sees which run, command and attempt it runs for, and the
    /// step's idempotency key ([`StepCall`]).
    pub fn step_with<R: Into<Attempt>>(
        mut self,
        name: &str,
        step: impl Fn(&StepCall<'_>) -> R + Send + Sync + 'static,
    ) -> Self {
        self.steps.insert(
            name.to_owned(),
            Arc::new(move |call: &StepCall<'_>| step(call).into()),
        );
        self
    }

    /// Sets the retry policy of one step.
    pub fn retry_policy(mut self, step: &str, policy: RetryPolicy) -> Self {
        self.retry_policies.insert(step.to_owned(), policy);
        self
    }

    /// Sets the retry policy for steps without their own (default: 3 attempts, 1 s backoff doubling up
    /// to 60 s).
    pub fn default_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.default_retry = policy;
        self
    }

    /// Number of step worker threads (default 4).
    pub fn workers(mut self, workers: usize) -> Self {
        self.workers = workers.max(1);
        self
    }

    /// Interval at which steps without a recorded outcome are dispatched again (default 30 s).
    pub fn redispatch_every(mut self, interval: Duration) -> Self {
        self.redispatch = interval;
        self
    }

    /// Saves a checkpoint after this many log records (default 10,000), and on shutdown. Recovery
    /// loads the last checkpoint and replays only the log after it; older log segments are deleted.
    pub fn checkpoint_every(mut self, records: u64) -> Self {
        self.checkpoint_every = records.max(1);
        self
    }

    /// What an acknowledgement waits for: `Durability::Fsync` (the default; survives a power loss) or
    /// `Durability::Os` (written to the operating system; survives a crash of the process only).
    pub fn durability(mut self, durability: aqueduct_storage::Durability) -> Self {
        self.log.durability = durability;
        self
    }

    /// Log segment size (default 64 MiB).
    pub fn segment_bytes(mut self, bytes: u64) -> Self {
        self.log.segment_bytes = bytes;
        self
    }

    /// Testing only: acknowledge before syncing, a deliberate bug the crash harness must catch.
    #[doc(hidden)]
    pub fn inject_ack_before_sync(mut self) -> Self {
        self.ack_before_sync = true;
        self
    }

    /// Opens the engine: recovers the log, resumes every run, and starts the threads.
    pub fn open(mut self) -> Result<Engine, EngineError> {
        for (name, version) in std::mem::take(&mut self.activations) {
            self.deciders
                .activate(&name, &version)
                .map_err(|e| EngineError::Open(e.to_string()))?;
        }
        let log = FileLog::open(self.dir.join("log"), self.log)
            .map_err(|e| EngineError::Open(e.to_string()))?;
        let state = RedbStateStore::open(&self.dir.join("state.redb"))
            .map_err(|e| EngineError::Open(e.to_string()))?;
        let mut partition = Partition::recover_with_state(log, state, self.deciders)
            .map_err(|e| EngineError::Open(e.to_string()))?;
        partition.inject_ack_before_sync(self.ack_before_sync);
        partition.set_default_retry_policy(self.default_retry);
        partition.set_evict_finished(true);
        if let Some(observer) = &self.observer {
            partition.set_observer(Arc::clone(observer));
        }
        for (step, policy) in &self.retry_policies {
            partition.set_retry_policy(step, *policy);
        }

        let (requests, inbox) = mpsc::channel::<Request>();
        let (jobs, job_inbox) = mpsc::channel::<Job>();
        let job_inbox = Arc::new(Mutex::new(job_inbox));
        let local: std::collections::BTreeSet<String> = self.steps.keys().cloned().collect();
        let routes = std::mem::take(&mut self.routes);
        let queue = self
            .remote
            .map(|lease| Arc::new(crate::queue::StepQueue::with_routes(lease, routes)));
        let activation_queue = self
            .remote_workflows
            .map(|lease| Arc::new(crate::activations::ActivationQueue::new(lease)));
        let steps = Arc::new(self.steps);
        let workers = (0..self.workers)
            .map(|_| {
                let inbox = Arc::clone(&job_inbox);
                let steps = Arc::clone(&steps);
                let results = requests.clone();
                let observer = self.observer.clone();
                let agents = self.agents.clone();
                std::thread::spawn(move || {
                    worker(
                        &inbox,
                        &steps,
                        agents.as_deref(),
                        &results,
                        observer.as_deref(),
                    );
                })
            })
            .collect();
        let last_checkpoint = partition.log().next_lsn();
        let processor = Processor {
            partition,
            checkpoint_every: self.checkpoint_every,
            last_checkpoint,
            jobs,
            waiters: BTreeMap::new(),
            starts: BTreeMap::new(),
            signals: BTreeMap::new(),
            updates: Vec::new(),
            answers: Vec::new(),
            deferred: Vec::new(),
            remote: queue.clone().map(|queue| (queue, local)),
            activations: activation_queue.clone(),
            max_running: self.max_running,
            running: 0,
        };
        let redispatch = self.redispatch;
        let thread = std::thread::spawn(move || processor.run(&inbox, redispatch));
        Ok(Engine {
            requests,
            thread: Some(thread),
            workers,
            queue,
            activations: activation_queue,
        })
    }
}

/// An embedded engine. Dropping it stops the threads; the log keeps everything acknowledged.
pub struct Engine {
    requests: Sender<Request>,
    thread: Option<JoinHandle<Result<(), String>>>,
    workers: Vec<JoinHandle<()>>,
    queue: Option<Arc<crate::queue::StepQueue>>,
    activations: Option<Arc<crate::activations::ActivationQueue>>,
}

impl Engine {
    /// The queue of steps for remote workers, if the engine has remote steps
    /// ([`Builder::remote_steps`]).
    pub fn step_queue(&self) -> Option<&Arc<crate::queue::StepQueue>> {
        self.queue.as_ref()
    }

    /// The queue of activations for remote workflow workers, if a workflow is decided remotely
    /// ([`Builder::remote_workflow`]).
    pub fn activation_queue(&self) -> Option<&Arc<crate::activations::ActivationQueue>> {
        self.activations.as_ref()
    }

    /// Records a remote workflow worker's decision: the commands run `run`'s logic issued given the
    /// first `at` entries of its history. A stale decision (for another length) is dropped and the run
    /// activated again. Not acknowledged; a lost one is made up for by the activation being offered
    /// again.
    pub fn decision(
        &self,
        run: &str,
        at: usize,
        commands: Vec<aqueduct_core::Command>,
    ) -> Result<(), EngineError> {
        self.send(Request::Decision {
            key: run.to_owned(),
            at,
            commands,
        })
    }

    /// Records the result of an attempt of a step that a remote worker executed. Like a local step's
    /// result, it is recorded at most once: a late or repeated one is dropped. Results are not
    /// acknowledged; one that is lost is made up for by the step being offered again.
    pub fn step_result(
        &self,
        run: &str,
        cmd: CommandIndex,
        attempt: Attempt,
    ) -> Result<(), EngineError> {
        let (key, request) = (run.to_owned(), attempt);
        self.send(match request {
            Attempt::Completed(result) => Request::StepResult {
                key,
                cmd,
                outcome: StepOutcome::Completed(result),
            },
            Attempt::Fail(error) => Request::StepResult {
                key,
                cmd,
                outcome: StepOutcome::Failed(error),
            },
            Attempt::Retry(error) => Request::AttemptFailed { key, cmd, error },
        })
    }

    /// Starts configuring an engine whose data lives in `dir`.
    pub fn builder(dir: impl Into<PathBuf>) -> Builder {
        Builder {
            dir: dir.into(),
            deciders: Deciders::new(),
            steps: BTreeMap::new(),
            workers: 4,
            redispatch: Duration::from_secs(30),
            log: FileLogOptions::default(),
            checkpoint_every: 10_000,
            ack_before_sync: false,
            retry_policies: BTreeMap::new(),
            default_retry: RetryPolicy::default(),
            observer: None,
            agents: None,
            activations: Vec::new(),
            remote: None,
            remote_workflows: None,
            routes: BTreeMap::new(),
            max_running: None,
        }
    }

    /// Starts a run. Idempotent per key: starting an existing key returns once that run's start is
    /// durable, without starting it again. Returns after the start is durable.
    pub fn start(&self, key: &str, workflow: &str, input: Payload) -> Result<(), EngineError> {
        self.start_with(key, workflow, input, ReusePolicy::ReturnExisting)
    }

    /// Starts a run with explicit [`StartOptions`]: a [`ReusePolicy`] for a key that already has one,
    /// and a token budget for its agent calls. A `ReusePolicy` alone converts into options.
    pub fn start_with(
        &self,
        key: &str,
        workflow: &str,
        input: Payload,
        options: impl Into<StartOptions>,
    ) -> Result<(), EngineError> {
        if crate::objects::is_reserved_key(key) {
            return Err(EngineError::InvalidKey(key.to_owned()));
        }
        let (reply, answer) = mpsc::channel();
        self.send(Request::Start {
            key: key.to_owned(),
            workflow: workflow.to_owned(),
            input,
            options: options.into(),
            reply,
        })?;
        answer.recv().map_err(|_| self.stopped())?
    }

    /// Sends a signal to a run. Returns once the signal is durable. Sending again with the same `id`
    /// (for example, after a timeout) records it once; an empty `id` disables that.
    pub fn signal(
        &self,
        key: &str,
        name: &str,
        id: &str,
        payload: Payload,
    ) -> Result<(), EngineError> {
        if crate::objects::is_reserved_key(key) {
            return Err(EngineError::InvalidKey(key.to_owned()));
        }
        let (reply, answer) = mpsc::channel();
        self.send(Request::Signal {
            key: key.to_owned(),
            name: name.to_owned(),
            id: id.to_owned(),
            payload,
            exact: false,
            reply,
        })?;
        answer.recv().map_err(|_| self.stopped())?
    }

    /// Sends an update to a run (a request its logic answers, ADR 0036) and waits for the answer: the
    /// result, or [`EngineError::UpdateRejected`]. A run that has ended or is not waiting for an update
    /// of this name rejects it, and so does logic that rejects it on receipt; neither leaves a trace in
    /// the journal. An accepted update is durable, and so is its answer, when this returns. Sending
    /// again with the same `id` (for example, after a [`EngineError::Timeout`]) delivers it once and
    /// returns the first one's answer; an empty `id` disables that.
    pub fn update(
        &self,
        key: &str,
        name: &str,
        id: &str,
        payload: Payload,
        timeout: Duration,
    ) -> Result<Payload, EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::Update {
            key: key.to_owned(),
            name: name.to_owned(),
            id: id.to_owned(),
            payload,
            reply,
        })?;
        match answer.recv_timeout(timeout) {
            Ok(result) => result,
            Err(RecvTimeoutError::Timeout) => Err(EngineError::Timeout(key.to_owned())),
            Err(RecvTimeoutError::Disconnected) => Err(self.stopped()),
        }
    }

    /// Asks a run's logic query `name` (a read-only look at its state, ADR 0036): the last value the
    /// logic published under that name, at the point the run has reached. Nothing is journaled; the
    /// answer reflects only what is durable.
    pub fn query(&self, key: &str, name: &str) -> Result<Payload, EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::Query {
            key: key.to_owned(),
            name: name.to_owned(),
            reply,
        })?;
        answer.recv().map_err(|_| self.stopped())?
    }

    /// A key's latest run, as far as it is durable: its run id, and its state (status, input, output
    /// and commands).
    pub fn describe(&self, key: &str) -> Result<(String, RunState), EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::Describe {
            key: key.to_owned(),
            reply,
        })?;
        answer.recv().map_err(|_| self.stopped())?
    }

    /// Every run, in run id order: those running and those that have ended.
    pub fn runs(&self) -> Result<Vec<RunSummary>, EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::Runs { reply })?;
        answer.recv().map_err(|_| self.stopped())?
    }

    /// Calls a handler of a virtual object (ADR 0043) and waits for its output. Calls to one key run one
    /// at a time, in the order they arrive; a call's state changes commit atomically with its
    /// completion, and a failed handler commits nothing ([`EngineError::HandlerFailed`]). Calling again
    /// with the same `id` (for example, after a [`EngineError::Timeout`]) runs it once and returns its
    /// outcome; an empty `id` makes the call a new one every time.
    pub fn call_object(
        &self,
        object: &str,
        key: &str,
        handler: &str,
        id: &str,
        args: &Payload,
        timeout: Duration,
    ) -> Result<Payload, EngineError> {
        static CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let args: serde_json::Value = serde_json::from_slice(args.as_bytes())
            .map_err(|e| EngineError::InvalidPayload(format!("the arguments are not JSON: {e}")))?;
        let id = if id.is_empty() {
            use std::hash::{BuildHasher, Hasher};
            let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
            hasher.write_u64(now_ms());
            let call = CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            format!("{:016x}-{call}", hasher.finish())
        } else {
            id.to_owned()
        };
        let (reply, answer) = mpsc::channel();
        self.send(Request::CallObject {
            object: object.to_owned(),
            key: key.to_owned(),
            handler: handler.to_owned(),
            id,
            args,
            reply,
        })?;
        let result = match answer.recv_timeout(timeout) {
            Ok(result) => result?,
            Err(RecvTimeoutError::Timeout) => return Err(EngineError::Timeout(key.to_owned())),
            Err(RecvTimeoutError::Disconnected) => return Err(self.stopped()),
        };
        if result.status != Status::Completed {
            return Err(EngineError::HandlerFailed(
                result
                    .error
                    .unwrap_or_else(|| format!("{:?}", result.status)),
            ));
        }
        let output: serde_json::Value = result
            .output
            .as_ref()
            .and_then(|output| serde_json::from_slice(output.as_bytes()).ok())
            .unwrap_or_default();
        Ok(Payload::new(output["output"].to_string().into_bytes()))
    }

    /// A virtual object key's committed state, as a JSON object (ADR 0043).
    pub fn object_state(&self, object: &str, key: &str) -> Result<Payload, EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::Query {
            key: crate::objects::object_run_key(object, key),
            name: crate::objects::STATE_QUERY.to_owned(),
            reply,
        })?;
        answer.recv().map_err(|_| self.stopped())?
    }

    /// Asks a run to stop. Returns once the request is durable. Every call the workflow is waiting for
    /// fails with a cancellation; the workflow decides what to do next (typically undo its work with a
    /// saga and fail). Only the first cancellation of a run counts.
    pub fn cancel(&self, key: &str, reason: &str) -> Result<(), EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::Cancel {
            key: key.to_owned(),
            reason: reason.to_owned(),
            reply,
        })?;
        answer.recv().map_err(|_| self.stopped())?
    }

    /// Escalations waiting for a resolution, across all runs.
    pub fn escalations(&self) -> Result<Vec<Escalation>, EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::Escalations { reply })?;
        answer.recv().map_err(|_| self.stopped())
    }

    /// Resolves an escalation by id; the run continues with `outcome`. Returns once the resolution is
    /// durable. Only the first resolution counts.
    pub fn resolve_escalation(&self, id: &str, outcome: Payload) -> Result<(), EngineError> {
        let (key, cmd) = aqueduct_core::parse_command_id(id)
            .ok_or_else(|| EngineError::InvalidEscalation(id.to_owned()))?;
        let (reply, answer) = mpsc::channel();
        self.send(Request::Resolve {
            key: key.to_owned(),
            cmd,
            outcome,
            reply,
        })?;
        answer.recv().map_err(|_| self.stopped())?
    }

    /// Completes an awakeable with a payload, given the id the workflow handed out. Returns once the
    /// completion is durable; completing it again records the first payload only.
    pub fn complete_awakeable(&self, id: &str, payload: Payload) -> Result<(), EngineError> {
        let (run, name) = aqueduct_core::parse_awakeable_id(id)
            .ok_or_else(|| EngineError::InvalidAwakeable(id.to_owned()))?;
        let (reply, answer) = mpsc::channel();
        // The id names an exact run: a completion for an earlier generation of the key never reaches
        // a later one.
        self.send(Request::Signal {
            key: run,
            name,
            id: id.to_owned(),
            payload,
            exact: true,
            reply,
        })?;
        answer.recv().map_err(|_| self.stopped())?
    }

    /// Waits until a run has finished and its end is durable.
    pub fn wait(&self, key: &str, timeout: Duration) -> Result<RunResult, EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::Wait {
            key: key.to_owned(),
            reply,
        })?;
        match answer.recv_timeout(timeout) {
            Ok(result) => result,
            Err(RecvTimeoutError::Timeout) => Err(EngineError::Timeout(key.to_owned())),
            Err(RecvTimeoutError::Disconnected) => Err(self.stopped()),
        }
    }

    /// The engine's current size.
    pub fn stats(&self) -> Result<EngineStats, EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::Stats { reply })?;
        answer.recv().map_err(|_| self.stopped())
    }

    /// Stops the engine and waits for its threads. Reports a fatal error, if one stopped it earlier.
    pub fn shutdown(mut self) -> Result<(), EngineError> {
        self.stop()
    }

    fn stop(&mut self) -> Result<(), EngineError> {
        let _ = self.requests.send(Request::Shutdown);
        let result = match self.thread.take().map(JoinHandle::join) {
            Some(Ok(Err(message))) => Err(EngineError::Stopped(message)),
            Some(Err(_)) => Err(EngineError::Stopped("the partition thread panicked".into())),
            _ => Ok(()),
        };
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
        result
    }

    fn send(&self, request: Request) -> Result<(), EngineError> {
        self.requests.send(request).map_err(|_| self.stopped())
    }

    fn stopped(&self) -> EngineError {
        EngineError::Stopped("the partition thread is no longer running".into())
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

enum Request {
    Start {
        key: String,
        workflow: String,
        input: Payload,
        options: StartOptions,
        reply: Sender<Result<(), EngineError>>,
    },
    Wait {
        key: String,
        reply: Sender<Result<RunResult, EngineError>>,
    },
    Signal {
        key: String,
        name: String,
        id: String,
        payload: Payload,
        /// `key` is a run id naming an exact run, not a key (whose latest run is meant).
        exact: bool,
        reply: Sender<Result<(), EngineError>>,
    },
    Cancel {
        key: String,
        reason: String,
        reply: Sender<Result<(), EngineError>>,
    },
    Update {
        key: String,
        name: String,
        id: String,
        payload: Payload,
        reply: Sender<Result<Payload, EngineError>>,
    },
    Query {
        key: String,
        name: String,
        reply: Sender<Result<Payload, EngineError>>,
    },
    CallObject {
        object: String,
        key: String,
        handler: String,
        id: String,
        args: serde_json::Value,
        reply: Sender<Result<RunResult, EngineError>>,
    },
    Decision {
        key: String,
        at: usize,
        commands: Vec<aqueduct_core::Command>,
    },
    Describe {
        key: String,
        reply: Sender<Result<(String, RunState), EngineError>>,
    },
    Runs {
        reply: Sender<Result<Vec<RunSummary>, EngineError>>,
    },
    Resolve {
        key: String,
        cmd: CommandIndex,
        outcome: Payload,
        reply: Sender<Result<(), EngineError>>,
    },
    Escalations {
        reply: Sender<Vec<Escalation>>,
    },
    StepResult {
        key: String,
        cmd: CommandIndex,
        outcome: StepOutcome,
    },
    AgentResult {
        key: String,
        cmd: CommandIndex,
        outcome: Result<aqueduct_core::AgentResponse, String>,
    },
    AttemptFailed {
        key: String,
        cmd: CommandIndex,
        error: String,
    },
    Stats {
        reply: Sender<EngineStats>,
    },
    Shutdown,
}

struct Job {
    key: String,
    cmd: CommandIndex,
    name: String,
    input: Payload,
    /// For an agent call: its token budget. `None` for a step.
    agent: Option<u64>,
    /// The attempt: the run's recorded failed attempts of the command, plus one.
    attempt: u32,
}

struct Processor {
    partition: Partition<FileLog, RedbStateStore>,
    checkpoint_every: u64,
    last_checkpoint: u64,
    jobs: Sender<Job>,
    waiters: BTreeMap<String, Vec<Sender<Result<RunResult, EngineError>>>>,
    starts: BTreeMap<String, Vec<Sender<Result<(), EngineError>>>>,
    signals: BTreeMap<String, Vec<Sender<Result<(), EngineError>>>>,
    /// Accepted updates waiting for their answer: (run id, the wait that received it, reply).
    updates: Vec<(String, CommandIndex, AnswerSender)>,
    /// Answers to send after the next flush, once what they reflect is durable.
    answers: Vec<(AnswerSender, Result<Payload, EngineError>)>,
    /// Other replies to send after the next flush, for the same reason.
    deferred: Vec<Box<dyn FnOnce() + Send>>,
    /// With remote steps: the queue, and the steps that have a function here.
    remote: Option<(
        Arc<crate::queue::StepQueue>,
        std::collections::BTreeSet<String>,
    )>,
    /// With remote workflows: their activations.
    activations: Option<Arc<crate::activations::ActivationQueue>>,
    /// The cap on running runs, and how many there are: counted after each flush, and kept up by the
    /// starts admitted since.
    max_running: Option<usize>,
    running: usize,
}

/// Where the answer to an update or a query goes.
type AnswerSender = Sender<Result<Payload, EngineError>>;

impl Processor {
    fn run(mut self, inbox: &Receiver<Request>, redispatch: Duration) -> Result<(), String> {
        // Recovery may have produced entries and dispatches: make them durable and send them.
        self.partition.set_time(now_ms());
        self.partition
            .fire_due_timers()
            .map_err(|e| e.to_string())?;
        self.flush()?;
        let mut next_redispatch = now_ms().saturating_add(millis(redispatch));
        loop {
            let now = now_ms();
            let wake = self
                .partition
                .next_timer()
                .map_or(next_redispatch, |t| t.min(next_redispatch));
            let first = match inbox.recv_timeout(Duration::from_millis(wake.saturating_sub(now))) {
                Ok(request) => Some(request),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => return Ok(()),
            };
            let now = now_ms();
            self.partition.set_time(now);
            if now >= next_redispatch {
                self.partition.redispatch();
                next_redispatch = now.saturating_add(millis(redispatch));
            }
            self.partition
                .fire_due_timers()
                .map_err(|e| e.to_string())?;
            // Group commit: take everything that is waiting, then sync once.
            for request in first.into_iter().chain(inbox.try_iter()) {
                if !self.handle(request)? {
                    self.flush()?;
                    self.checkpoint()?;
                    return Ok(());
                }
            }
            self.flush()?;
            if self.partition.log().next_lsn() - self.last_checkpoint >= self.checkpoint_every {
                self.checkpoint()?;
            }
        }
    }

    /// Saves a checkpoint. Only called right after a flush, when everything in memory is durable.
    fn checkpoint(&mut self) -> Result<(), String> {
        self.last_checkpoint = self
            .partition
            .checkpoint()
            .map_err(|e| format!("checkpoint failed: {e}"))?;
        Ok(())
    }

    /// Applies one request; false on shutdown.
    fn handle(&mut self, request: Request) -> Result<bool, String> {
        match request {
            Request::Start {
                key,
                workflow,
                input,
                options,
                reply,
            } => {
                if self.overloaded(&key)? {
                    let _ = reply.send(Err(overloaded()));
                    return Ok(true);
                }
                match self.partition.start_with(&key, &workflow, input, options) {
                    Ok(()) => self.starts.entry(key).or_default().push(reply),
                    Err(PartitionError::UnknownWorkflow(name)) => {
                        let _ = reply.send(Err(EngineError::UnknownWorkflow(name)));
                    }
                    Err(PartitionError::UnknownVersion { workflow, version }) => {
                        let _ = reply.send(Err(EngineError::UnknownVersion { workflow, version }));
                    }
                    Err(PartitionError::KeyConflict(key)) => {
                        let _ = reply.send(Err(EngineError::KeyConflict(key)));
                    }
                    Err(PartitionError::AlreadyStarted(key)) => {
                        let _ = reply.send(Err(EngineError::AlreadyStarted(key)));
                    }
                    Err(PartitionError::InvalidKey(key)) => {
                        let _ = reply.send(Err(EngineError::InvalidKey(key)));
                    }
                    Err(error) => return Err(error.to_string()),
                }
            }
            Request::Wait { key, reply } => {
                // The key's latest run; one that continues as new is followed to its successor.
                let Some(id) = self.partition.latest(&key).map_err(|e| e.to_string())? else {
                    let _ = reply.send(Err(EngineError::UnknownRun(key)));
                    return Ok(true);
                };
                if self.partition.run(&id).is_some() {
                    // Answered after the next flush, once the run's end is durable.
                    self.waiters.entry(id).or_default().push(reply);
                } else {
                    // Not in memory: an unloaded finished run (durable in a checkpoint).
                    let answer = match self.partition.lookup_run(&id).map_err(|e| e.to_string())? {
                        Some(state) => Ok(RunResult {
                            status: state.status(),
                            output: state.output().cloned(),
                            error: state.error().map(str::to_owned),
                            version: state.version().map(str::to_owned),
                        }),
                        None => Err(EngineError::UnknownRun(key)),
                    };
                    let _ = reply.send(answer);
                }
            }
            Request::Signal {
                key,
                name,
                id,
                payload,
                exact,
                reply,
            } => match if exact {
                self.partition.signal_run(&key, &name, &id, payload)
            } else {
                self.partition.signal(&key, &name, &id, payload)
            } {
                Ok(()) => self.signals.entry(key).or_default().push(reply),
                Err(PartitionError::UnknownRun(key)) => {
                    let _ = reply.send(Err(EngineError::UnknownRun(key)));
                }
                Err(error) => return Err(error.to_string()),
            },
            Request::Resolve {
                key,
                cmd,
                outcome,
                reply,
            } => match self.partition.resolve_escalation(&key, cmd, outcome) {
                Ok(()) => self.signals.entry(key).or_default().push(reply),
                Err(PartitionError::UnknownRun(key)) => {
                    let _ = reply.send(Err(EngineError::UnknownRun(key)));
                }
                Err(PartitionError::InvalidResolution(why)) => {
                    let _ = reply.send(Err(EngineError::InvalidResolution(why)));
                }
                Err(error) => return Err(error.to_string()),
            },
            Request::Escalations { reply } => {
                let open = self
                    .partition
                    .open_escalations()
                    .into_iter()
                    .map(|(key, cmd, reason, payload)| Escalation {
                        id: aqueduct_core::awakeable_id(&key, cmd),
                        key,
                        reason,
                        payload,
                    })
                    .collect();
                let _ = reply.send(open);
            }
            Request::Cancel { key, reason, reply } => match self.partition.cancel(&key, &reason) {
                Ok(()) => self.signals.entry(key).or_default().push(reply),
                Err(PartitionError::UnknownRun(key)) => {
                    let _ = reply.send(Err(EngineError::UnknownRun(key)));
                }
                Err(error) => return Err(error.to_string()),
            },
            Request::Update {
                key,
                name,
                id,
                payload,
                reply,
            } => match self.partition.update(&key, &name, &id, payload) {
                Ok(UpdateStatus::Answered(answer)) => self
                    .answers
                    .push((reply, answer.map_err(EngineError::UpdateRejected))),
                Ok(UpdateStatus::Accepted { run, cmd }) => self.updates.push((run, cmd, reply)),
                Err(PartitionError::UnknownRun(key)) => {
                    let _ = reply.send(Err(EngineError::UnknownRun(key)));
                }
                Err(error) => return Err(error.to_string()),
            },
            Request::Query { key, name, reply } => {
                let answer = match self.partition.query(&key, &name) {
                    Ok(Some(answer)) => Ok(answer),
                    Ok(None) => Err(EngineError::UnknownQuery { key, name }),
                    Err(PartitionError::UnknownRun(key)) => Err(EngineError::UnknownRun(key)),
                    Err(PartitionError::UnknownWorkflow(name)) => {
                        Err(EngineError::UnknownWorkflow(name))
                    }
                    Err(PartitionError::UnknownVersion { workflow, version }) => {
                        Err(EngineError::UnknownVersion { workflow, version })
                    }
                    Err(PartitionError::Unsupported(why)) => Err(EngineError::Unsupported(why)),
                    Err(error) => return Err(error.to_string()),
                };
                self.answers.push((reply, answer));
            }
            Request::CallObject {
                object,
                key,
                handler,
                id,
                args,
                reply,
            } if self.overloaded(&crate::objects::call_key(&object, &key, &id))? => {
                let _ = (object, key, handler, id, args);
                let _ = reply.send(Err(overloaded()));
            }
            Request::CallObject {
                object,
                key,
                handler,
                id,
                args,
                reply,
            } => match self
                .partition
                .call_object(&object, &key, &handler, &id, &args)
            {
                Ok(call) => {
                    let unloaded = match self.partition.run(&call) {
                        Some(_) => None,
                        None => self
                            .partition
                            .lookup_run(&call)
                            .map_err(|e| e.to_string())?,
                    };
                    match unloaded {
                        // An ended call whose run has left memory (durable in a checkpoint).
                        Some(state) => {
                            let _ = reply.send(Ok(RunResult {
                                status: state.status(),
                                output: state.output().cloned(),
                                error: state.error().map(str::to_owned),
                                version: state.version().map(str::to_owned),
                            }));
                        }
                        // Running, or still in the inbox: answered after the flush once it has ended.
                        None => self.waiters.entry(call).or_default().push(reply),
                    }
                }
                Err(PartitionError::UnknownWorkflow(name)) => {
                    let _ = reply.send(Err(EngineError::UnknownWorkflow(name)));
                }
                Err(PartitionError::InvalidKey(key)) => {
                    let _ = reply.send(Err(EngineError::InvalidKey(key)));
                }
                Err(PartitionError::KeyConflict(key)) => {
                    let _ = reply.send(Err(EngineError::KeyConflict(key)));
                }
                Err(error) => return Err(error.to_string()),
            },
            Request::Decision { key, at, commands } => {
                self.partition
                    .decision(&key, at, commands)
                    .map_err(|e| e.to_string())?;
            }
            Request::Describe { key, reply } => {
                let answer = match self.partition.latest(&key).map_err(|e| e.to_string())? {
                    Some(id) => match self.partition.lookup_run(&id).map_err(|e| e.to_string())? {
                        Some(state) => Ok((id, state)),
                        None => Err(EngineError::UnknownRun(key)),
                    },
                    None => Err(EngineError::UnknownRun(key)),
                };
                self.deferred.push(Box::new(move || {
                    let _ = reply.send(answer);
                }));
            }
            Request::Runs { reply } => {
                let mut runs = Vec::new();
                for id in self.partition.run_ids().map_err(|e| e.to_string())? {
                    let state = match self.partition.run(&id) {
                        Some(run) => Some(run.state().clone()),
                        None => self.partition.lookup_run(&id).map_err(|e| e.to_string())?,
                    };
                    if let Some(state) = state {
                        runs.push(RunSummary {
                            run_id: id,
                            workflow: state.workflow().to_owned(),
                            status: state.status(),
                            version: state.version().map(str::to_owned),
                            entries: state.len(),
                        });
                    }
                }
                self.deferred.push(Box::new(move || {
                    let _ = reply.send(Ok(runs));
                }));
            }
            Request::StepResult { key, cmd, outcome } => {
                self.partition
                    .step_result(&key, cmd, outcome)
                    .map_err(|e| e.to_string())?;
            }
            Request::AgentResult { key, cmd, outcome } => {
                self.partition
                    .agent_result(&key, cmd, outcome)
                    .map_err(|e| e.to_string())?;
            }
            Request::AttemptFailed { key, cmd, error } => {
                self.partition
                    .step_attempt_failed(&key, cmd, error)
                    .map_err(|e| e.to_string())?;
            }
            Request::Stats { reply } => {
                let _ = reply.send(EngineStats {
                    runs_in_memory: self.partition.runs().len(),
                    pending_timers: self.partition.pending_timers(),
                    log_lsn: self.partition.log().next_lsn(),
                });
            }
            Request::Shutdown => return Ok(false),
        }
        // Only non-empty under the injected acknowledge-before-sync bug.
        let early = self.partition.take_unsynced();
        self.deliver(early);
        Ok(true)
    }

    fn flush(&mut self) -> Result<(), String> {
        let outbound = self
            .partition
            .flush()
            .map_err(|e| format!("storage failed: {e}"))?;
        self.deliver(outbound);
        if self.max_running.is_some() {
            self.running = self
                .partition
                .runs()
                .iter()
                .filter(|(key, run)| {
                    !run.state().status().is_terminal() && !key.starts_with("$object/")
                })
                .count();
        }
        self.answer_waiters();
        self.answer_updates().map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Whether a request that would create a run under `key` must be refused: the cap is reached and the
    /// key has no run. Otherwise, it is counted.
    fn overloaded(&mut self, key: &str) -> Result<bool, String> {
        let Some(limit) = self.max_running else {
            return Ok(false);
        };
        if self
            .partition
            .latest(key)
            .map_err(|e| e.to_string())?
            .is_some()
        {
            return Ok(false);
        }
        if self.running >= limit {
            return Ok(true);
        }
        self.running += 1;
        Ok(false)
    }

    /// The attempt a dispatch of `cmd` is: the run's recorded failed attempts of it, plus one.
    fn attempt(&self, key: &str, cmd: CommandIndex) -> u32 {
        self.partition
            .run(key)
            .and_then(|run| run.state().steps().get(cmd.0 as usize))
            .map_or(1, |step| step.failed_attempts + 1)
    }

    /// Called right after a flush: sends the answers computed before it, and answers accepted updates
    /// whose run has answered them, or has ended without doing so.
    fn answer_updates(&mut self) -> Result<(), PartitionError> {
        for (reply, answer) in self.answers.drain(..) {
            let _ = reply.send(answer);
        }
        for reply in self.deferred.drain(..) {
            reply();
        }
        let mut waiting = Vec::new();
        for (run, cmd, reply) in std::mem::take(&mut self.updates) {
            let answer = match self.partition.run(&run) {
                Some(held) => update_answer(held.state(), cmd),
                // Not in memory: it has ended, and its checkpoint holds it.
                None => self
                    .partition
                    .lookup_run(&run)?
                    .and_then(|state| update_answer(&state, cmd)),
            };
            match answer {
                Some(answer) => {
                    let _ = reply.send(answer);
                }
                None => waiting.push((run, cmd, reply)),
            }
        }
        self.updates = waiting;
        Ok(())
    }

    fn deliver(&mut self, outbound: Vec<Outbound>) {
        for message in outbound {
            match message {
                Outbound::Started { key } => {
                    for reply in self.starts.remove(&key).unwrap_or_default() {
                        let _ = reply.send(Ok(()));
                    }
                }
                Outbound::Dispatch {
                    key,
                    cmd,
                    name,
                    input,
                } if self
                    .remote
                    .as_ref()
                    .is_some_and(|(_, local)| !local.contains(&name)) =>
                {
                    let attempt = self.attempt(&key, cmd);
                    if let Some((queue, _)) = &self.remote {
                        queue.offer(&key, cmd, &name, input, attempt, queue.now_ms());
                    }
                }
                Outbound::Dispatch {
                    key,
                    cmd,
                    name,
                    input,
                } => {
                    let attempt = self.attempt(&key, cmd);
                    let _ = self.jobs.send(Job {
                        key,
                        cmd,
                        name,
                        input,
                        agent: None,
                        attempt,
                    });
                }
                Outbound::DispatchAgent {
                    key,
                    cmd,
                    name,
                    request,
                    budget_tokens,
                } => {
                    let attempt = self.attempt(&key, cmd);
                    let _ = self.jobs.send(Job {
                        key,
                        cmd,
                        name,
                        input: request,
                        agent: Some(budget_tokens),
                        attempt,
                    });
                }
                Outbound::Signaled { key }
                | Outbound::Cancelled { key }
                | Outbound::Resolved { key } => {
                    // One acknowledgement per signal or cancellation, in the order they were sent.
                    if let Some(replies) = self.signals.get_mut(&key)
                        && !replies.is_empty()
                    {
                        let _ = replies.remove(0).send(Ok(()));
                    }
                }
                Outbound::Activation {
                    key,
                    workflow,
                    at,
                    env,
                } => {
                    let (Some(queue), Some(run)) = (&self.activations, self.partition.run(&key))
                    else {
                        continue;
                    };
                    // The history as the remote side receives it: the records, chained to the run id.
                    let mut chain = aqueduct_codec::ChainWriter::new(&key);
                    let history = run
                        .journal()
                        .iter()
                        .take(at)
                        .map(|entry| chain.append(entry))
                        .collect();
                    queue.offer(&key, &workflow, at, history, env, queue.now_ms());
                }
                Outbound::ResultRecorded { .. }
                | Outbound::AttemptRecorded { .. }
                | Outbound::Finished { .. } => {}
            }
        }
    }

    /// Called right after a flush, when everything in memory is durable: answers waiters whose runs
    /// have ended.
    fn answer_waiters(&mut self) {
        // Waiters of a run that continued as new wait for its successor (perhaps continued again).
        loop {
            let continued: Vec<String> = self
                .waiters
                .keys()
                .filter(|id| {
                    self.partition
                        .run(id)
                        .is_some_and(|run| run.state().status() == Status::Continued)
                })
                .cloned()
                .collect();
            if continued.is_empty() {
                break;
            }
            for id in continued {
                let (key, generation) = aqueduct_core::split_run_id(&id);
                let next = aqueduct_core::run_id(key, generation + 1);
                let replies = self.waiters.remove(&id).unwrap_or_default();
                self.waiters.entry(next).or_default().extend(replies);
            }
        }
        let finished: Vec<String> = self
            .waiters
            .keys()
            .filter(|key| {
                self.partition
                    .run(key)
                    .is_some_and(|run| run.state().status().is_terminal())
            })
            .cloned()
            .collect();
        for key in finished {
            let Some(run) = self.partition.run(&key) else {
                continue;
            };
            let state = run.state();
            let result = RunResult {
                status: state.status(),
                output: state.output().cloned(),
                error: state.error().map(str::to_owned),
                version: state.version().map(str::to_owned),
            };
            for reply in self.waiters.remove(&key).unwrap_or_default() {
                let _ = reply.send(Ok(result.clone()));
            }
        }
    }
}

fn overloaded() -> EngineError {
    EngineError::Overloaded("the engine is at its limit of running runs; try again later".into())
}

/// The answer to the update received by the wait `cmd`, once there is one: the logic's response, or a
/// rejection if the run ended without one.
fn update_answer(state: &RunState, cmd: CommandIndex) -> Option<Result<Payload, EngineError>> {
    match state.response(cmd) {
        Some(answer) => Some(answer.clone().map_err(EngineError::UpdateRejected)),
        None if state.status().is_terminal() => Some(Err(EngineError::UpdateRejected(
            "the run ended without answering the update".into(),
        ))),
        None => None,
    }
}

/// The engine's clock: milliseconds since the Unix epoch.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn worker(
    inbox: &Mutex<Receiver<Job>>,
    steps: &BTreeMap<String, StepFn>,
    agents: Option<&dyn AgentExecutor>,
    results: &Sender<Request>,
    observer: Option<&dyn Observer>,
) {
    loop {
        let job = match inbox.lock() {
            Ok(inbox) => match inbox.recv() {
                Ok(job) => job,
                Err(_) => return,
            },
            Err(_) => return,
        };
        let (started, clock) = (std::time::SystemTime::now(), std::time::Instant::now());
        if let Some(budget_tokens) = job.agent {
            let call = AgentCall {
                run: &job.key,
                name: &job.name,
                request: &job.input,
                budget_tokens,
            };
            let attempt = match agents {
                None => AgentAttempt::Retry("no agent executor is registered".into()),
                Some(agents) => catch_unwind(AssertUnwindSafe(|| agents.call(&call)))
                    .unwrap_or_else(|_| {
                        AgentAttempt::Retry(format!("agent call `{}` panicked", job.name))
                    }),
            };
            if let Some(observer) = observer {
                let end = match &attempt {
                    AgentAttempt::Completed(_) => crate::StepEnd::Completed,
                    AgentAttempt::Fail(_) => crate::StepEnd::Failed,
                    AgentAttempt::Retry(_) => crate::StepEnd::Retry,
                };
                let name = format!("agent:{}", job.name);
                observer.step(&job.key, job.cmd, &name, started, clock.elapsed(), end);
            }
            let (key, cmd) = (job.key, job.cmd);
            let request = match attempt {
                AgentAttempt::Completed(response) => Request::AgentResult {
                    key,
                    cmd,
                    outcome: Ok(response),
                },
                AgentAttempt::Fail(error) => Request::AgentResult {
                    key,
                    cmd,
                    outcome: Err(error),
                },
                AgentAttempt::Retry(error) => Request::AttemptFailed { key, cmd, error },
            };
            if results.send(request).is_err() {
                return;
            }
            continue;
        }
        let attempt = match steps.get(&job.name) {
            // A deployment problem, not a property of the input: retry, so registering the step heals it.
            None => Attempt::Retry(format!("no step named `{}` is registered", job.name)),
            Some(step) => catch_unwind(AssertUnwindSafe(|| {
                step(&StepCall {
                    run: &job.key,
                    cmd: job.cmd,
                    step: &job.name,
                    input: &job.input,
                    attempt: job.attempt,
                })
            }))
            .unwrap_or_else(|_| Attempt::Retry(format!("step `{}` panicked", job.name))),
        };
        if let Some(observer) = observer {
            let end = match &attempt {
                Attempt::Completed(_) => crate::StepEnd::Completed,
                Attempt::Fail(_) => crate::StepEnd::Failed,
                Attempt::Retry(_) => crate::StepEnd::Retry,
            };
            observer.step(&job.key, job.cmd, &job.name, started, clock.elapsed(), end);
        }
        let (key, cmd) = (job.key, job.cmd);
        let request = match attempt {
            Attempt::Completed(result) => Request::StepResult {
                key,
                cmd,
                outcome: StepOutcome::Completed(result),
            },
            Attempt::Fail(error) => Request::StepResult {
                key,
                cmd,
                outcome: StepOutcome::Failed(error),
            },
            Attempt::Retry(error) => Request::AttemptFailed { key, cmd, error },
        };
        if results.send(request).is_err() {
            return;
        }
    }
}
