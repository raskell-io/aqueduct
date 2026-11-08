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

use std::collections::{BTreeMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use aqueduct_core::{CommandIndex, Decider, Payload, RunState, Status, StepOutcome};
use aqueduct_storage::{FileLog, FileLogOptions, LogStore, RedbStateStore, StateStore};

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
    /// A replicated engine that does not lead its partition (ADR 0055): ask the leader, if one is
    /// known (its node id), or try again.
    #[error("this node does not lead the partition{}", .0.map(|n| format!("; node {n} does")).unwrap_or_default())]
    NotLeader(Option<u64>),
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
    /// A deployment cannot be changed that way (ADR 0048).
    #[error("{0}")]
    Deployment(String),
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
    namespace_running: BTreeMap<String, usize>,
    weights: BTreeMap<String, f64>,
    replication: Option<Replication>,
    placement: Option<crate::placement::Placement>,
    route: Option<RemoteRoute>,
    shared_queue: Option<Arc<crate::queue::StepQueue>>,
    shared_activations: Option<Arc<crate::activations::ActivationQueue>>,
}

/// Where an engine sends messages for other partitions (ADR 0052): the home they are for, and the
/// message. See [`Builder::partition_of`].
pub type RemoteRoute = Arc<dyn Fn(String, crate::Remote) + Send + Sync>;

/// How a replicated engine takes part in its partition's Raft group (ADR 0055).
#[derive(Clone)]
pub struct Replication {
    /// This replica's node id.
    pub id: u64,
    /// The group it starts in: its node id among them, or none if a leader will add it.
    pub members: Vec<u64>,
    /// How it sends messages to the others.
    pub transport: Transport,
    /// How often Raft's clock ticks.
    pub tick: Duration,
    /// Raft's timing, in ticks.
    pub config: aqueduct_raft::Config,
    /// The node that should lead this partition when it can (ADR 0058): the leader hands over to it
    /// once it is caught up, so that leaders spread over the nodes.
    pub preferred_leader: Option<u64>,
    /// Entries applied between snapshots. Default 1,000.
    pub snapshot_every: u64,
    /// Called with every snapshot this replica takes, after it is on disk (ADR 0062): to copy it to
    /// object storage, for example.
    pub on_snapshot: Option<SnapshotHook>,
}

/// What a replica does with a snapshot it took.
pub type SnapshotHook = Arc<dyn Fn(&aqueduct_raft::Snapshot) + Send + Sync>;

impl std::fmt::Debug for Replication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replication")
            .field("id", &self.id)
            .field("members", &self.members)
            .field("tick", &self.tick)
            .finish_non_exhaustive()
    }
}

/// What a processor is opened on. Built once, so the size of its variants does not matter.
#[allow(clippy::large_enum_variant)]
enum Opened {
    Local(Local),
    Replicated(Replicated),
}

impl Builder {
    /// Makes the engine one partition of many (ADR 0052): `placement` says which homes are its own, and
    /// messages for the others (children with chosen keys, messages to objects) go to `route`, which
    /// must hand them to the partition they are for ([`Engine::receive`]). See [`crate::shards`].
    pub fn partition_of(
        mut self,
        placement: crate::placement::Placement,
        route: RemoteRoute,
    ) -> Self {
        self.placement = Some(placement);
        self.route = Some(route);
        self
    }

    /// Hands remote steps to this queue rather than one of the engine's own: several partitions in a
    /// process share one, so that workers poll one queue. Implies remote steps.
    pub fn shared_step_queue(mut self, queue: Arc<crate::queue::StepQueue>) -> Self {
        self.shared_queue = Some(queue);
        self
    }

    /// The same, for remote workflows' activations.
    pub fn shared_activation_queue(
        mut self,
        queue: Arc<crate::activations::ActivationQueue>,
    ) -> Self {
        self.shared_activations = Some(queue);
        self
    }

    /// Runs the partition as a replica of a Raft group (ADR 0055): its log is the group's, kept in
    /// `raft/` under the data directory, and it takes requests only while it leads. Imports are refused.
    pub fn replicated(mut self, replication: Replication) -> Self {
        self.replication = Some(replication);
        self
    }

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

    /// Sets a namespace's quota and its share of workers that serve several namespaces (ADR 0046).
    /// `max_running` applies as [`Builder::max_running_runs`] does, to that namespace's runs. `weight` is
    /// relative to other namespaces, which have 1.
    pub fn namespace(mut self, namespace: &str, max_running: Option<usize>, weight: f64) -> Self {
        if let Some(limit) = max_running {
            self.namespace_running.insert(namespace.to_owned(), limit);
        }
        self.weights.insert(namespace.to_owned(), weight);
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

    /// The replica a replicated engine runs: its Raft state from `raft/`, and a setup that gives every
    /// partition it builds what [`Builder::open`] gives a local one.
    fn open_replica(&self, replication: Replication) -> Result<Replicated, EngineError> {
        let (store, recovered) = aqueduct_storage::RaftStore::open(self.dir.join("raft"))
            .map_err(|e| EngineError::Open(e.to_string()))?;
        let default_retry = self.default_retry;
        let policies = self.retry_policies.clone();
        let observer = self.observer.clone();
        let placement = self.placement.clone();
        let setup: crate::replica::Setup = Arc::new(move |partition, _term| {
            if let Some(placement) = &placement {
                partition.set_placement(Arc::clone(placement));
            }
            partition.set_default_retry_policy(default_retry);
            for (step, policy) in &policies {
                partition.set_retry_policy(step, *policy);
            }
            if let Some(observer) = &observer {
                partition.set_observer(Arc::clone(observer));
            }
        });
        let seed = {
            use std::hash::{BuildHasher, Hasher};
            let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
            hasher.write_u64(replication.id);
            hasher.finish()
        };
        let replica = crate::replica::Replica::restart(
            replication.id,
            &replication.members,
            replication.config,
            seed,
            recovered.hard_state,
            recovered.snapshot,
            recovered.entries,
            self.deciders.clone(),
            setup,
        )
        .map_err(EngineError::Open)?;
        Ok(Replicated {
            replica,
            store,
            send: replication.transport,
            tick: replication.tick,
            released: Vec::new(),
            preferred: replication.preferred_leader,
            ticks: 0,
            snapshot_every: replication.snapshot_every.max(1),
            on_snapshot: replication.on_snapshot,
        })
    }

    /// Opens the engine: recovers the log, resumes every run, and starts the threads.
    pub fn open(mut self) -> Result<Engine, EngineError> {
        if let Some(frozen) = crate::import::frozen(&self.dir) {
            return Err(EngineError::Open(frozen));
        }
        for (name, version) in std::mem::take(&mut self.activations) {
            self.deciders
                .activate(&name, &version)
                .map_err(|e| EngineError::Open(e.to_string()))?;
        }
        let deployments =
            crate::deployments::Deployments::load(&self.dir).map_err(EngineError::Open)?;
        deployments
            .apply(&mut self.deciders)
            .map_err(EngineError::Open)?;
        let (opened, import) = match self.replication.take() {
            None => {
                let log = FileLog::open(self.dir.join("log"), self.log)
                    .map_err(|e| EngineError::Open(e.to_string()))?;
                let state = RedbStateStore::open(&self.dir.join("state.redb"))
                    .map_err(|e| EngineError::Open(e.to_string()))?;
                let mut partition = Partition::load_with_state(log, state, self.deciders)
                    .map_err(|e| EngineError::Open(e.to_string()))?;
                // Before any run acts: an import is applied whole (ADR 0047).
                let import = crate::import::apply_pending(&mut partition, &self.dir)
                    .map_err(EngineError::Open)?;
                if let Some(placement) = &self.placement {
                    partition.set_placement(Arc::clone(placement));
                }
                partition
                    .resume()
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
                let last_checkpoint = partition.log().next_lsn();
                let host = Local {
                    partition,
                    checkpoint_every: self.checkpoint_every,
                    last_checkpoint,
                };
                (Opened::Local(host), import)
            }
            Some(replication) => (Opened::Replicated(self.open_replica(replication)?), None),
        };

        let (requests, inbox) = mpsc::channel::<Request>();
        let (jobs, job_inbox) = mpsc::channel::<Job>();
        let job_inbox = Arc::new(Mutex::new(job_inbox));
        let local: std::collections::BTreeSet<String> = self.steps.keys().cloned().collect();
        let routes = std::mem::take(&mut self.routes);
        let weights = std::mem::take(&mut self.weights);
        let queue = self.shared_queue.take().or_else(|| {
            self.remote.map(|lease| {
                Arc::new(crate::queue::StepQueue::with_routes(lease, routes).with_weights(weights))
            })
        });
        let activation_queue = self.shared_activations.take().or_else(|| {
            self.remote_workflows
                .map(|lease| Arc::new(crate::activations::ActivationQueue::new(lease)))
        });
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
        // A replicated engine leads only once its processor says so.
        let leading = Arc::new(Mutex::new((matches!(opened, Opened::Local(_)), None)));
        let redispatch = self.redispatch;
        macro_rules! spawn {
            ($host:expr) => {{
                let processor = Processor {
                    host: $host,
                    gated: VecDeque::new(),
                    route: self.route.clone(),
                    leading: Arc::clone(&leading),
                    jobs,
                    waiters: BTreeMap::new(),
                    starts: BTreeMap::new(),
                    signals: BTreeMap::new(),
                    updates: Vec::new(),
                    answers: Vec::new(),
                    deferred: Vec::new(),
                    remote: queue.clone().map(|queue| (queue, local)),
                    activations: activation_queue.clone(),
                    deployments,
                    dir: self.dir.clone(),
                    max_running: self.max_running,
                    running: 0,
                    namespace_max: self.namespace_running,
                    namespace_count: BTreeMap::new(),
                };
                std::thread::spawn(move || processor.run(&inbox, redispatch))
            }};
        }
        let thread = match opened {
            Opened::Local(host) => spawn!(host),
            Opened::Replicated(host) => spawn!(host),
        };
        Ok(Engine {
            requests,
            leading,
            thread: Some(thread),
            workers,
            queue,
            activations: activation_queue,
            dir: self.dir,
            import,
        })
    }
}

/// An embedded engine. Dropping it stops the threads; the log keeps everything acknowledged.
pub struct Engine {
    requests: Sender<Request>,
    /// Whether the partition is here, and who leads it if not (a replicated engine).
    leading: Arc<Mutex<(bool, Option<u64>)>>,
    thread: Option<JoinHandle<Result<(), String>>>,
    workers: Vec<JoinHandle<()>>,
    queue: Option<Arc<crate::queue::StepQueue>>,
    activations: Option<Arc<crate::activations::ActivationQueue>>,
    dir: PathBuf,
    import: Option<crate::import::ImportReport>,
}

impl Engine {
    /// What opening did with a pending import (ADR 0047), if there was one.
    pub fn import_report(&self) -> Option<&crate::import::ImportReport> {
        self.import.as_ref()
    }

    /// Starts an upload into this engine's data directory (ADR 0047).
    pub fn begin_import(&self) -> Result<crate::import::Stage, String> {
        crate::import::Stage::begin(&self.dir)
    }

    /// Sets the priority of a key's latest run (ADR 0049), durably: its waiting steps and activations
    /// move to their new place at once. A run that has ended is left as it is.
    pub fn set_priority(&self, key: &str, priority: u32) -> Result<(), EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::SetPriority {
            key: key.to_owned(),
            priority,
            reply,
        })?;
        answer.recv().map_err(|_| self.stopped())?
    }

    /// Every build of every remote workflow (ADR 0048).
    pub fn deployments(&self) -> Result<Vec<crate::deployments::DeploymentInfo>, EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::Deployments { reply })?;
        answer.recv().map_err(|_| self.stopped())?
    }

    /// Makes a build of a remote workflow the one new runs start on, durably, and the one before it
    /// drain. Also a rollback. Unless `force`, a connected worker must announce it: otherwise new runs
    /// would wait for one.
    pub fn activate_deployment(
        &self,
        workflow: &str,
        build: &str,
        force: bool,
    ) -> Result<(), EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::ActivateDeployment {
            workflow: workflow.to_owned(),
            build: build.to_owned(),
            force,
            reply,
        })?;
        answer.recv().map_err(|_| self.stopped())?
    }

    /// Retires a draining build with no open runs.
    pub fn retire_deployment(&self, workflow: &str, build: &str) -> Result<(), EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::RetireDeployment {
            workflow: workflow.to_owned(),
            build: build.to_owned(),
            reply,
        })?;
        answer.recv().map_err(|_| self.stopped())?
    }

    /// A run's journal, by its run id.
    pub fn history(&self, run: &str) -> Result<Option<Vec<aqueduct_core::Entry>>, EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::History {
            run: run.to_owned(),
            reply,
        })?;
        answer.recv().map_err(|_| self.stopped())?
    }

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
            namespace_running: BTreeMap::new(),
            weights: BTreeMap::new(),
            replication: None,
            placement: None,
            route: None,
            shared_queue: None,
            shared_activations: None,
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
        let running = self.thread.as_ref().is_some_and(|t| !t.is_finished());
        if running
            && let Ok(leading) = self.leading.lock()
            && !leading.0
        {
            return EngineError::NotLeader(leading.1);
        }
        EngineError::Stopped("the partition thread is no longer running".into())
    }

    /// A message from another partition (ADR 0052): a child to start or its result, a message to an
    /// object or its delivery. Repeats are harmless.
    pub fn receive(&self, message: crate::Remote) {
        let _ = self.requests.send(Request::Remote(message));
    }

    /// A Raft message for this replica, from another (a replicated engine, ADR 0055).
    pub fn raft_message(&self, envelope: aqueduct_raft::Envelope) {
        let _ = self.requests.send(Request::Raft(envelope));
    }

    /// Where Raft messages for this replica go: for a transport to deliver into.
    pub fn raft_inbox(&self) -> RaftInbox {
        RaftInbox(self.requests.clone())
    }

    /// Before stopping a replicated engine: hands its leadership to a caught-up replica, so that its
    /// partition is not left to an election (ADR 0060). It no longer prefers to lead. Returns at once;
    /// [`Engine::leads`] says when it has handed over.
    pub fn step_down(&self) {
        let _ = self.requests.send(Request::StepDown);
    }

    /// Forgets an ended run (retention, ADR 0066): appends `Forgotten` to its journal and no longer holds
    /// it. Returns once that is durable; whether there was such a run. A run still going is refused.
    pub fn forget(&self, run: &str, reason: &str) -> Result<bool, EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::Forget {
            run: run.to_owned(),
            reason: reason.to_owned(),
            reply,
        })?;
        answer.recv().map_err(|_| self.stopped())?
    }

    /// Proposes a change to the partition's group, on the leader (ADR 0063): add or remove one node. It
    /// takes effect once committed; another change waits until then.
    pub fn change_members(&self, change: aqueduct_raft::Change) -> Result<(), EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::ChangeMembers { change, reply })?;
        answer.recv().map_err(|_| self.stopped())?
    }

    /// The partition's group and its leader, as this replica knows them: an empty group if it is not
    /// replicated; no leader if it leads, or knows none.
    pub fn members(&self) -> Result<(Vec<u64>, Option<u64>), EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::Members { reply })?;
        answer.recv().map_err(|_| self.stopped())
    }

    /// Whether this engine has its partition here: it leads, or is not replicated.
    pub fn leads(&self) -> bool {
        self.leading.lock().is_ok_and(|leading| leading.0)
    }

    /// The node that leads, when this replicated engine does not and knows which does.
    pub fn leader(&self) -> Option<u64> {
        self.leading
            .lock()
            .ok()
            .and_then(|leading| if leading.0 { None } else { leading.1 })
    }
}

/// Delivers Raft messages to a replicated engine; holds no more than its request channel.
#[derive(Clone)]
pub struct RaftInbox(Sender<Request>);

impl std::fmt::Debug for RaftInbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RaftInbox")
    }
}

impl RaftInbox {
    /// Delivers a message; dropped if the engine has stopped, as the network may drop it.
    pub fn deliver(&self, envelope: aqueduct_raft::Envelope) {
        let _ = self.0.send(Request::Raft(envelope));
    }

    /// Delivers a message from another partition (ADR 0052); dropped if the engine has stopped or does
    /// not lead its partition: its sender repeats it.
    pub fn deliver_remote(&self, message: crate::Remote) {
        let _ = self.0.send(Request::Remote(message));
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

enum Request {
    SetPriority {
        key: String,
        priority: u32,
        reply: Sender<Result<(), EngineError>>,
    },
    Deployments {
        reply: Sender<Result<Vec<crate::deployments::DeploymentInfo>, EngineError>>,
    },
    ActivateDeployment {
        workflow: String,
        build: String,
        force: bool,
        reply: Sender<Result<(), EngineError>>,
    },
    RetireDeployment {
        workflow: String,
        build: String,
        reply: Sender<Result<(), EngineError>>,
    },
    History {
        run: String,
        reply: Sender<Result<Option<Vec<aqueduct_core::Entry>>, EngineError>>,
    },
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
    /// A Raft message from another replica (ADR 0055).
    Raft(aqueduct_raft::Envelope),
    /// A message from another partition (ADR 0052).
    Remote(crate::Remote),
    /// Hand leadership over before stopping (ADR 0060).
    StepDown,
    /// Forget an ended run (retention, ADR 0066).
    Forget {
        run: String,
        reason: String,
        reply: Sender<Result<bool, EngineError>>,
    },
    /// Change the partition's group (ADR 0063).
    ChangeMembers {
        change: aqueduct_raft::Change,
        reply: Sender<Result<(), EngineError>>,
    },
    /// The group, and who leads it, as this replica knows them.
    Members {
        reply: Sender<(Vec<u64>, Option<u64>)>,
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

/// What a processor runs its partition on (ADR 0055): a local log, or (later) a Raft group.
pub(crate) trait Host: Send + 'static {
    /// The partition's log.
    type Log: LogStore + Send + 'static;
    /// Its state store.
    type State: StateStore + Send + 'static;
    /// The partition, where it is here to take requests.
    fn partition(&self) -> Option<&Partition<Self::Log, Self::State>>;
    /// The same, to change.
    fn partition_mut(&mut self) -> Option<&mut Partition<Self::Log, Self::State>>;
    /// Writes the batch; returns the messages released by it.
    fn flush(&mut self) -> Result<Vec<Outbound>, String>;
    /// Saves a checkpoint if one is due. Only called right after a flush.
    fn checkpoint(&mut self, force: bool) -> Result<(), String>;
    /// Changes the workflows and builds the partition decides with.
    fn update_deciders(
        &mut self,
        change: &mut dyn FnMut(&mut Deciders) -> Result<(), String>,
    ) -> Result<(), String>;
    /// How often the host's clock ticks, if it has one (Raft).
    fn tick_every(&self) -> Option<Duration> {
        None
    }
    /// One tick.
    fn tick(&mut self) {}
    /// A message from another replica.
    fn step(&mut self, _envelope: aqueduct_raft::Envelope) {}
    /// An index that, once committed, proves everything written so far is durable and this host still
    /// led then: `None` where a flush already proves it (a local log).
    fn barrier(&mut self) -> Option<u64> {
        None
    }
    /// How far the log is committed.
    fn committed(&self) -> u64 {
        u64::MAX
    }
    /// The leader, as far as this host knows, when it does not lead.
    fn leader(&self) -> Option<u64> {
        None
    }
    /// Hands leadership to a caught-up replica, before stopping.
    fn step_down(&mut self) {}
    /// Proposes a change to the group: `None` if the partition is not replicated.
    fn change_members(
        &mut self,
        _change: aqueduct_raft::Change,
    ) -> Option<Result<u64, aqueduct_raft::ChangeError>> {
        None
    }
    /// The group as this replica knows it: empty if the partition is not replicated.
    fn members(&self) -> Vec<u64> {
        Vec::new()
    }
}

/// A partition on a local log: the embedded engine and the single server.
pub(crate) struct Local {
    partition: Partition<FileLog, RedbStateStore>,
    checkpoint_every: u64,
    last_checkpoint: u64,
}

impl Host for Local {
    type Log = FileLog;
    type State = RedbStateStore;

    fn partition(&self) -> Option<&Partition<FileLog, RedbStateStore>> {
        Some(&self.partition)
    }

    fn partition_mut(&mut self) -> Option<&mut Partition<FileLog, RedbStateStore>> {
        Some(&mut self.partition)
    }

    fn flush(&mut self) -> Result<Vec<Outbound>, String> {
        self.partition
            .flush()
            .map_err(|e| format!("storage failed: {e}"))
    }

    fn checkpoint(&mut self, force: bool) -> Result<(), String> {
        if force || self.partition.log().next_lsn() - self.last_checkpoint >= self.checkpoint_every
        {
            self.last_checkpoint = self
                .partition
                .checkpoint()
                .map_err(|e| format!("checkpoint failed: {e}"))?;
        }
        Ok(())
    }

    fn update_deciders(
        &mut self,
        change: &mut dyn FnMut(&mut Deciders) -> Result<(), String>,
    ) -> Result<(), String> {
        change(self.partition.deciders_mut())
    }
}

/// A partition whose log is a Raft group (ADR 0055): here only while this replica leads.
pub(crate) struct Replicated {
    replica: crate::replica::Replica,
    store: aqueduct_storage::RaftStore,
    send: Transport,
    tick: Duration,
    released: Vec<Outbound>,
    preferred: Option<u64>,
    ticks: u64,
    snapshot_every: u64,
    on_snapshot: Option<SnapshotHook>,
}

/// How a replica sends a Raft message to another.
pub type Transport = Arc<dyn Fn(aqueduct_raft::Envelope) + Send + Sync>;

/// Ticks between a leader's checks that it is the preferred one (ADR 0058).
const PREFER_EVERY: u64 = 40;

/// Entries applied between snapshots of a replicated engine, by default.
pub const SNAPSHOT_EVERY: u64 = 1_000;

impl Replicated {
    /// Does what the replica's readies ask: persists, sends, and collects what is released.
    fn pump(&mut self) -> Result<(), String> {
        while self.replica.has_ready() {
            let ready = self.replica.ready()?;
            self.store
                .persist(&ready.raft)
                .map_err(|e| format!("storage failed: {e}"))?;
            for envelope in ready.raft.messages {
                (self.send)(envelope);
            }
            self.released.extend(ready.released);
            self.replica.advance()?;
        }
        Ok(())
    }
}

impl Host for Replicated {
    type Log = crate::replica::BatchLog;
    type State = aqueduct_storage::NoStateStore;

    fn partition(&self) -> Option<&crate::replica::ReplicaPartition> {
        if self.replica.leads() {
            self.replica.partition()
        } else {
            None
        }
    }

    fn partition_mut(&mut self) -> Option<&mut crate::replica::ReplicaPartition> {
        self.replica.partition_mut().ok()
    }

    fn flush(&mut self) -> Result<Vec<Outbound>, String> {
        self.replica.flush()?;
        self.pump()?;
        Ok(std::mem::take(&mut self.released))
    }

    fn checkpoint(&mut self, _force: bool) -> Result<(), String> {
        if self.replica.applied() >= self.replica.raft().snapshot_index() + self.snapshot_every {
            self.replica.compact();
            if let Some(snapshot) = self.replica.raft().snapshot().cloned() {
                self.store
                    .save_snapshot(&snapshot)
                    .map_err(|e| format!("storage failed: {e}"))?;
                if let Some(hook) = &self.on_snapshot {
                    hook(&snapshot);
                }
            }
        }
        Ok(())
    }

    fn update_deciders(
        &mut self,
        change: &mut dyn FnMut(&mut Deciders) -> Result<(), String>,
    ) -> Result<(), String> {
        change(self.replica.deciders_mut())?;
        if let Ok(partition) = self.replica.partition_mut() {
            change(partition.deciders_mut())?;
        }
        Ok(())
    }

    fn tick_every(&self) -> Option<Duration> {
        Some(self.tick)
    }

    fn tick(&mut self) {
        self.replica.tick();
        self.ticks += 1;
        // Now and then, the leader hands over to the preferred node, once it is caught up.
        if let Some(preferred) = self.preferred
            && self.ticks.is_multiple_of(PREFER_EVERY)
        {
            self.replica.prefer_leader(preferred);
        }
    }

    fn step(&mut self, envelope: aqueduct_raft::Envelope) {
        self.replica.step(envelope.from, envelope.message);
    }

    fn barrier(&mut self) -> Option<u64> {
        // Not leading: nothing can be answered, and the callers are let go.
        Some(self.replica.barrier().unwrap_or(u64::MAX))
    }

    fn committed(&self) -> u64 {
        self.replica.raft().commit()
    }

    fn leader(&self) -> Option<u64> {
        self.replica.raft().leader()
    }

    fn step_down(&mut self) {
        self.preferred = None;
        self.replica.step_down();
    }

    fn change_members(
        &mut self,
        change: aqueduct_raft::Change,
    ) -> Option<Result<u64, aqueduct_raft::ChangeError>> {
        Some(self.replica.propose_change(change))
    }

    fn members(&self) -> Vec<u64> {
        self.replica.raft().members().to_vec()
    }
}

/// A reply to send once what it reflects is committed.
type Reply = Box<dyn FnOnce() + Send>;

struct Processor<H: Host> {
    host: H,
    /// Replies waiting for an index to commit (a replicated host), oldest first.
    gated: VecDeque<(u64, Vec<Reply>)>,
    /// Where messages for other partitions go.
    route: Option<RemoteRoute>,
    /// Whether this engine leads its partition, and who does if not: for callers turned away.
    leading: Arc<Mutex<(bool, Option<u64>)>>,
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
    deployments: crate::deployments::Deployments,
    dir: PathBuf,
    max_running: Option<usize>,
    running: usize,
    /// The same, per namespace.
    namespace_max: BTreeMap<String, usize>,
    namespace_count: BTreeMap<String, usize>,
}

/// Where the answer to an update or a query goes.
type AnswerSender = Sender<Result<Payload, EngineError>>;

impl<H: Host> Processor<H> {
    /// The partition, to change; only asked for where it is here.
    fn part(&mut self) -> Result<&mut Partition<H::Log, H::State>, String> {
        self.host
            .partition_mut()
            .ok_or_else(|| "the partition is not here".to_owned())
    }

    fn run(mut self, inbox: &Receiver<Request>, redispatch: Duration) -> Result<(), String> {
        // Recovery may have produced entries and dispatches: make them durable and send them.
        if let Some(partition) = self.host.partition_mut() {
            partition.set_time(now_ms());
            partition.fire_due_timers().map_err(|e| e.to_string())?;
        }
        self.flush()?;
        let mut next_redispatch = now_ms().saturating_add(millis(redispatch));
        let tick = self.host.tick_every();
        let mut next_tick = tick.map(|t| now_ms().saturating_add(millis(t)));
        // Messages between partitions are repeated every second when unanswered (ADR 0052).
        let repeat = Duration::from_secs(1);
        let mut next_repeat = now_ms().saturating_add(millis(repeat));
        loop {
            let now = now_ms();
            let wake = self
                .host
                .partition()
                .and_then(Partition::next_timer)
                .map_or(next_redispatch, |t| t.min(next_redispatch));
            let wake = next_tick.map_or(wake, |t| t.min(wake)).min(next_repeat);
            let first = match inbox.recv_timeout(Duration::from_millis(wake.saturating_sub(now))) {
                Ok(request) => Some(request),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => return Ok(()),
            };
            let now = now_ms();
            if let (Some(every), Some(at)) = (tick, next_tick)
                && now >= at
            {
                self.host.tick();
                next_tick = Some(now.saturating_add(millis(every)));
            }
            if let Some(partition) = self.host.partition_mut() {
                partition.set_time(now);
                if now >= next_redispatch {
                    partition.redispatch();
                } else if now >= next_repeat {
                    partition.repeat_remote();
                }
                partition.fire_due_timers().map_err(|e| e.to_string())?;
            }
            if now >= next_redispatch {
                next_redispatch = now.saturating_add(millis(redispatch));
            }
            if now >= next_repeat {
                next_repeat = now.saturating_add(millis(repeat));
            }
            // Group commit: take everything that is waiting, then sync once.
            for request in first.into_iter().chain(inbox.try_iter()) {
                if !self.handle(request)? {
                    self.flush()?;
                    self.host.checkpoint(true)?;
                    return Ok(());
                }
            }
            self.flush()?;
            self.host.checkpoint(false)?;
        }
    }

    /// Applies one request; false on shutdown.
    fn handle(&mut self, request: Request) -> Result<bool, String> {
        let request = match request {
            Request::Raft(envelope) => {
                self.host.step(envelope);
                return Ok(true);
            }
            Request::Shutdown => return Ok(false),
            Request::StepDown => {
                self.host.step_down();
                return Ok(true);
            }
            Request::Members { reply } => {
                let leader = if self.host.partition().is_some() {
                    None
                } else {
                    self.host.leader()
                };
                let _ = reply.send((self.host.members(), leader));
                return Ok(true);
            }
            Request::Stats { reply } if self.host.partition().is_none() => {
                let _ = reply.send(EngineStats {
                    runs_in_memory: 0,
                    pending_timers: 0,
                    log_lsn: 0,
                });
                return Ok(true);
            }
            // Not here: the caller's reply is dropped, and it is told who leads.
            _ if self.host.partition().is_none() => return Ok(true),
            request => request,
        };
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
                match self.part()?.start_with(&key, &workflow, input, options) {
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
                let Some(id) = self.part()?.latest(&key).map_err(|e| e.to_string())? else {
                    let _ = reply.send(Err(EngineError::UnknownRun(key)));
                    return Ok(true);
                };
                if self.part()?.run(&id).is_some() {
                    // Answered after the next flush, once the run's end is durable.
                    self.waiters.entry(id).or_default().push(reply);
                } else {
                    // Not in memory: an unloaded finished run (durable in a checkpoint).
                    let answer = match self.part()?.lookup_run(&id).map_err(|e| e.to_string())? {
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
                self.part()?.signal_run(&key, &name, &id, payload)
            } else {
                self.part()?.signal(&key, &name, &id, payload)
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
            } => match self.part()?.resolve_escalation(&key, cmd, outcome) {
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
                    .part()?
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
            Request::Cancel { key, reason, reply } => match self.part()?.cancel(&key, &reason) {
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
            } => match self.part()?.update(&key, &name, &id, payload) {
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
                let answer = match self.part()?.query(&key, &name) {
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
                .part()?
                .call_object(&object, &key, &handler, &id, &args)
            {
                Ok(call) => {
                    let unloaded = match self.part()?.run(&call) {
                        Some(_) => None,
                        None => self.part()?.lookup_run(&call).map_err(|e| e.to_string())?,
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
                self.part()?
                    .decision(&key, at, commands)
                    .map_err(|e| e.to_string())?;
            }
            Request::Describe { key, reply } => {
                let answer = match self.part()?.latest(&key).map_err(|e| e.to_string())? {
                    Some(id) => match self.part()?.lookup_run(&id).map_err(|e| e.to_string())? {
                        Some(state) => Ok((id, state)),
                        None => Err(EngineError::UnknownRun(key)),
                    },
                    None => Err(EngineError::UnknownRun(key)),
                };
                self.deferred.push(Box::new(move || {
                    let _ = reply.send(answer);
                }));
            }
            Request::SetPriority {
                key,
                priority,
                reply,
            } => {
                let answer = match self.part()?.set_priority(&key, priority) {
                    Ok(Some(run)) => {
                        // Waiting work moves at once; the answer waits until the change is durable.
                        if let Some((queue, _)) = &self.remote {
                            queue.reprioritize(&run, priority);
                        }
                        if let Some(queue) = &self.activations {
                            queue.reprioritize(&run, priority);
                        }
                        Ok(())
                    }
                    Ok(None) => Ok(()),
                    Err(PartitionError::UnknownRun(key)) => Err(EngineError::UnknownRun(key)),
                    Err(error) => return Err(error.to_string()),
                };
                self.deferred.push(Box::new(move || {
                    let _ = reply.send(answer);
                }));
            }
            Request::Deployments { reply } => {
                let _ = reply.send(Ok(self.deployment_list()));
            }
            Request::ActivateDeployment {
                workflow,
                build,
                force,
                reply,
            } => {
                let _ = reply.send(self.activate_deployment(&workflow, &build, force));
            }
            Request::RetireDeployment {
                workflow,
                build,
                reply,
            } => {
                let _ = reply.send(self.retire_deployment(&workflow, &build));
            }
            Request::History { run, reply } => {
                let history = self.part()?.history(&run).map_err(|e| e.to_string())?;
                self.deferred.push(Box::new(move || {
                    let _ = reply.send(Ok(history));
                }));
            }
            Request::Runs { reply } => {
                let mut runs = Vec::new();
                for id in self.part()?.run_ids().map_err(|e| e.to_string())? {
                    let state = match self.part()?.run(&id) {
                        Some(run) => Some(run.state().clone()),
                        None => self.part()?.lookup_run(&id).map_err(|e| e.to_string())?,
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
                self.part()?
                    .step_result(&key, cmd, outcome)
                    .map_err(|e| e.to_string())?;
            }
            Request::AgentResult { key, cmd, outcome } => {
                self.part()?
                    .agent_result(&key, cmd, outcome)
                    .map_err(|e| e.to_string())?;
            }
            Request::AttemptFailed { key, cmd, error } => {
                self.part()?
                    .step_attempt_failed(&key, cmd, error)
                    .map_err(|e| e.to_string())?;
            }
            Request::Stats { reply } => {
                let _ = reply.send(EngineStats {
                    runs_in_memory: self.part()?.runs().len(),
                    pending_timers: self.part()?.pending_timers(),
                    log_lsn: self.part()?.log().next_lsn(),
                });
            }
            Request::Remote(message) => {
                self.part()?.receive(message).map_err(|e| e.to_string())?;
            }
            Request::Forget { run, reason, reply } => {
                let answer = match self.part()?.forget(&run, &reason) {
                    Ok(forgotten) => Ok(forgotten),
                    Err(PartitionError::Unsupported(why)) => Err(EngineError::Unsupported(why)),
                    Err(error) => return Err(error.to_string()),
                };
                // Answered once `Forgotten` is durable (committed, on a replica).
                self.deferred.push(Box::new(move || {
                    let _ = reply.send(answer);
                }));
            }
            Request::ChangeMembers { change, reply } => {
                use aqueduct_raft::ChangeError;
                let answer = match self.host.change_members(change) {
                    None => Err(EngineError::Unsupported(
                        "this engine's partition is not replicated".into(),
                    )),
                    Some(Ok(_)) => Ok(()),
                    Some(Err(ChangeError::NotLeader(not))) => {
                        Err(EngineError::NotLeader(not.leader))
                    }
                    Some(Err(ChangeError::Pending)) => Err(EngineError::Unsupported(
                        "another change of the group is not committed yet: one at a time".into(),
                    )),
                    Some(Err(ChangeError::NotReady)) => Err(EngineError::Unsupported(
                        "the leader is new: try again in a moment".into(),
                    )),
                    Some(Err(ChangeError::Pointless)) => Err(EngineError::Unsupported(
                        "that change would change nothing, or leave no one".into(),
                    )),
                };
                let _ = reply.send(answer);
            }
            Request::Shutdown | Request::Raft(_) | Request::StepDown | Request::Members { .. } => {
                return Ok(false);
            }
        }
        // Only non-empty under the injected acknowledge-before-sync bug.
        let early = self.part()?.take_unsynced();
        self.deliver(early);
        Ok(true)
    }

    fn deployment_list(&self) -> Vec<crate::deployments::DeploymentInfo> {
        use crate::deployments::DeploymentInfo;
        let mut listed: BTreeMap<(String, String), DeploymentInfo> = BTreeMap::new();
        let mut entry = |workflow: &str, build: &str| -> (String, String) {
            let key = (workflow.to_owned(), build.to_owned());
            listed.entry(key.clone()).or_insert(DeploymentInfo {
                workflow: workflow.to_owned(),
                build: build.to_owned(),
                state: None,
                open_runs: 0,
                workers: 0,
            });
            key
        };
        let mut states = Vec::new();
        for (workflow, builds) in &self.deployments.builds {
            for (build, state) in builds {
                states.push((entry(workflow, build), *state));
            }
        }
        let mut open = Vec::new();
        for ((workflow, build), count) in self
            .host
            .partition()
            .map(Partition::open_builds)
            .unwrap_or_default()
        {
            open.push((entry(&workflow, &build), count));
        }
        let mut workers = Vec::new();
        if let Some(queue) = &self.activations {
            for ((workflow, build), count) in queue.sessions() {
                if let Some(build) = build {
                    workers.push((entry(&workflow, &build), count));
                }
            }
        }
        for (key, state) in states {
            if let Some(info) = listed.get_mut(&key) {
                info.state = Some(state);
            }
        }
        for (key, count) in open {
            if let Some(info) = listed.get_mut(&key) {
                info.open_runs = count;
            }
        }
        for (key, count) in workers {
            if let Some(info) = listed.get_mut(&key) {
                info.workers = count;
            }
        }
        listed.into_values().collect()
    }

    fn activate_deployment(
        &mut self,
        workflow: &str,
        build: &str,
        force: bool,
    ) -> Result<(), EngineError> {
        let mut remote = false;
        self.host
            .update_deciders(&mut |deciders| {
                remote = deciders.is_remote(workflow);
                Ok(())
            })
            .map_err(EngineError::Deployment)?;
        if !remote {
            return Err(EngineError::UnknownWorkflow(workflow.to_owned()));
        }
        if build.is_empty() {
            return Err(EngineError::Deployment("a build needs a name".to_owned()));
        }
        let connected = self.activations.as_ref().is_some_and(|queue| {
            queue
                .sessions()
                .contains_key(&(workflow.to_owned(), Some(build.to_owned())))
        });
        if !connected && !force {
            return Err(EngineError::Deployment(format!(
                "no connected worker announces build `{build}` of `{workflow}`: new runs would wait for one"
            )));
        }
        // Durable before it takes effect: a run started on it must find it there after a crash.
        self.deployments
            .activate(&self.dir, workflow, build)
            .map_err(EngineError::Deployment)?;
        self.host
            .update_deciders(&mut |deciders| {
                deciders
                    .activate_build(workflow, build)
                    .map_err(|e| e.to_string())
            })
            .map_err(|_| EngineError::UnknownWorkflow(workflow.to_owned()))
    }

    fn retire_deployment(&mut self, workflow: &str, build: &str) -> Result<(), EngineError> {
        let open = self
            .host
            .partition()
            .map(Partition::open_builds)
            .unwrap_or_default()
            .get(&(workflow.to_owned(), build.to_owned()))
            .copied()
            .unwrap_or(0);
        self.deployments
            .retire(&self.dir, workflow, build, open)
            .map_err(EngineError::Deployment)?;
        self.host
            .update_deciders(&mut |deciders| {
                deciders.forbid_build(workflow, build);
                Ok(())
            })
            .map_err(EngineError::Deployment)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), String> {
        let outbound = self.host.flush()?;
        self.deliver(outbound);
        if (self.max_running.is_some() || !self.namespace_max.is_empty())
            && let Some(partition) = self.host.partition()
        {
            self.namespace_count.clone_from(partition.running());
            self.running = self.namespace_count.values().sum();
        }
        let leads = self.host.partition().is_some();
        *self.leading.lock().map_err(|_| "a lock was poisoned")? = (leads, self.host.leader());
        if !leads {
            // Whoever waits is let go, and told who leads; their requests can be repeated there.
            self.starts.clear();
            self.signals.clear();
            self.waiters.clear();
            self.updates.clear();
            self.answers.clear();
            self.deferred.clear();
            self.gated.clear();
            return Ok(());
        }
        let mut replies: Vec<Reply> = Vec::new();
        self.answer_waiters(&mut replies);
        self.answer_updates(&mut replies)
            .map_err(|e| e.to_string())?;
        if !replies.is_empty() {
            match self.host.barrier() {
                None => replies.into_iter().for_each(|reply| reply()),
                Some(index) => self.gated.push_back((index, replies)),
            }
        }
        let committed = self.host.committed();
        while self
            .gated
            .front()
            .is_some_and(|(index, _)| *index <= committed)
        {
            if let Some((_, replies)) = self.gated.pop_front() {
                replies.into_iter().for_each(|reply| reply());
            }
        }
        Ok(())
    }

    /// Whether a request that would create a run under `key` must be refused: the cap is reached and the
    /// key has no run. Otherwise, it is counted.
    fn overloaded(&mut self, key: &str) -> Result<bool, String> {
        let namespace = crate::namespaces::namespace_of(key);
        let namespace_limit = self.namespace_max.get(namespace).copied();
        if self.max_running.is_none() && namespace_limit.is_none() {
            return Ok(false);
        }
        if self
            .part()?
            .latest(key)
            .map_err(|e| e.to_string())?
            .is_some()
        {
            return Ok(false);
        }
        let in_namespace = self.namespace_count.get(namespace).copied().unwrap_or(0);
        if self.max_running.is_some_and(|limit| self.running >= limit)
            || namespace_limit.is_some_and(|limit| in_namespace >= limit)
        {
            return Ok(true);
        }
        self.running += 1;
        *self
            .namespace_count
            .entry(namespace.to_owned())
            .or_default() += 1;
        Ok(false)
    }

    /// The attempt a dispatch of `cmd` is: the run's recorded failed attempts of it, plus one.
    fn attempt(&self, key: &str, cmd: CommandIndex) -> u32 {
        self.host
            .partition()
            .and_then(|partition| partition.run(key))
            .and_then(|run| run.state().steps().get(cmd.0 as usize))
            .map_or(1, |step| step.failed_attempts + 1)
    }

    /// Called right after a flush: sends the answers computed before it, and answers accepted updates
    /// whose run has answered them, or has ended without doing so.
    fn answer_updates(&mut self, replies: &mut Vec<Reply>) -> Result<(), PartitionError> {
        for (reply, answer) in self.answers.drain(..) {
            replies.push(Box::new(move || {
                let _ = reply.send(answer);
            }));
        }
        replies.append(&mut self.deferred);
        let Some(partition) = self.host.partition_mut() else {
            return Ok(());
        };
        let mut waiting = Vec::new();
        for (run, cmd, reply) in std::mem::take(&mut self.updates) {
            let answer = match partition.run(&run) {
                Some(held) => update_answer(held.state(), cmd),
                // Not in memory: it has ended, and its checkpoint holds it.
                None => partition
                    .lookup_run(&run)?
                    .and_then(|state| update_answer(&state, cmd)),
            };
            match answer {
                Some(answer) => replies.push(Box::new(move || {
                    let _ = reply.send(answer);
                })),
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
                        let partition = self.host.partition();
                        let build = partition.and_then(|p| p.pinned_build(&key));
                        let priority = partition.map_or(0, |p| p.priority(&key));
                        let now = queue.now_ms();
                        queue.offer_pinned(&key, cmd, &name, input, attempt, build, priority, now);
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
                    let (Some(queue), Some(run)) = (
                        &self.activations,
                        self.host.partition().and_then(|p| p.run(&key)),
                    ) else {
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
                    let partition = self.host.partition();
                    let build = partition.and_then(|p| p.pinned_build(&key));
                    let priority = partition.map_or(0, |p| p.priority(&key));
                    let now = queue.now_ms();
                    queue.offer_pinned(&key, &workflow, build, priority, at, history, env, now);
                }
                Outbound::ResultRecorded { .. }
                | Outbound::AttemptRecorded { .. }
                | Outbound::Finished { .. } => {}
                Outbound::Remote { to, message } => {
                    if let Some(route) = &self.route {
                        route(to, message);
                    }
                }
            }
        }
    }

    /// Called right after a flush, when everything in memory is durable: answers waiters whose runs
    /// have ended.
    fn answer_waiters(&mut self, replies: &mut Vec<Reply>) {
        // Waiters of a run that continued as new wait for its successor (perhaps continued again).
        loop {
            let continued: Vec<String> = self
                .waiters
                .keys()
                .filter(|id| {
                    self.host
                        .partition()
                        .and_then(|p| p.run(id))
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
                self.host
                    .partition()
                    .and_then(|p| p.run(key))
                    .is_some_and(|run| run.state().status().is_terminal())
            })
            .cloned()
            .collect();
        for key in finished {
            let Some(run) = self.host.partition().and_then(|p| p.run(&key)) else {
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
                let result = result.clone();
                replies.push(Box::new(move || {
                    let _ = reply.send(Ok(result));
                }));
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
