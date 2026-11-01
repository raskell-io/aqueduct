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

use aqueduct_core::{CommandIndex, Decider, Payload, Status, StepOutcome};
use aqueduct_storage::{FileLog, FileLogOptions, LogStore, RedbStateStore};

use crate::{Deciders, Observer, Outbound, Partition, PartitionError, RetryPolicy, ReusePolicy};

/// A step implementation.
pub type StepFn = Arc<dyn Fn(&Payload) -> Attempt + Send + Sync>;

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
    /// A run with this key exists, and the start used [`ReusePolicy::Reject`].
    #[error("run `{0}` already exists")]
    AlreadyStarted(String),
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
}

impl Builder {
    /// Tells `observer` about durable entries, step executions and flushes, for telemetry
    /// (`aqueduct-otel` turns them into OpenTelemetry traces and metrics).
    pub fn observer(mut self, observer: Arc<dyn Observer>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Registers a workflow (a procedure or any other decider).
    pub fn workflow(mut self, name: &str, decider: Arc<dyn Decider + Send + Sync>) -> Self {
        self.deciders.insert(name, decider);
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
            Arc::new(move |input: &Payload| step(input).into()),
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
    pub fn open(self) -> Result<Engine, EngineError> {
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
        let steps = Arc::new(self.steps);
        let workers = (0..self.workers)
            .map(|_| {
                let inbox = Arc::clone(&job_inbox);
                let steps = Arc::clone(&steps);
                let results = requests.clone();
                let observer = self.observer.clone();
                std::thread::spawn(move || worker(&inbox, &steps, &results, observer.as_deref()))
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
        };
        let redispatch = self.redispatch;
        let thread = std::thread::spawn(move || processor.run(&inbox, redispatch));
        Ok(Engine {
            requests,
            thread: Some(thread),
            workers,
        })
    }
}

/// An embedded engine. Dropping it stops the threads; the log keeps everything acknowledged.
pub struct Engine {
    requests: Sender<Request>,
    thread: Option<JoinHandle<Result<(), String>>>,
    workers: Vec<JoinHandle<()>>,
}

impl Engine {
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
        }
    }

    /// Starts a run. Idempotent per key: starting an existing key returns once that run's start is
    /// durable, without starting it again. Returns after the start is durable.
    pub fn start(&self, key: &str, workflow: &str, input: Payload) -> Result<(), EngineError> {
        self.start_with(key, workflow, input, ReusePolicy::ReturnExisting)
    }

    /// Starts a run with an explicit [`ReusePolicy`] for a key that already has one.
    pub fn start_with(
        &self,
        key: &str,
        workflow: &str,
        input: Payload,
        policy: ReusePolicy,
    ) -> Result<(), EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::Start {
            key: key.to_owned(),
            workflow: workflow.to_owned(),
            input,
            policy,
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
        policy: ReusePolicy,
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
}

struct Processor {
    partition: Partition<FileLog, RedbStateStore>,
    checkpoint_every: u64,
    last_checkpoint: u64,
    jobs: Sender<Job>,
    waiters: BTreeMap<String, Vec<Sender<Result<RunResult, EngineError>>>>,
    starts: BTreeMap<String, Vec<Sender<Result<(), EngineError>>>>,
    signals: BTreeMap<String, Vec<Sender<Result<(), EngineError>>>>,
}

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
                policy,
                reply,
            } => match self.partition.start_with(&key, &workflow, input, policy) {
                Ok(()) => self.starts.entry(key).or_default().push(reply),
                Err(PartitionError::UnknownWorkflow(name)) => {
                    let _ = reply.send(Err(EngineError::UnknownWorkflow(name)));
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
            },
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
            Request::StepResult { key, cmd, outcome } => {
                self.partition
                    .step_result(&key, cmd, outcome)
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
        self.answer_waiters();
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
                } => {
                    let _ = self.jobs.send(Job {
                        key,
                        cmd,
                        name,
                        input,
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
            };
            for reply in self.waiters.remove(&key).unwrap_or_default() {
                let _ = reply.send(Ok(result.clone()));
            }
        }
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
        let attempt = match steps.get(&job.name) {
            // A deployment problem, not a property of the input: retry, so registering the step heals it.
            None => Attempt::Retry(format!("no step named `{}` is registered", job.name)),
            Some(step) => catch_unwind(AssertUnwindSafe(|| step(&job.input)))
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
