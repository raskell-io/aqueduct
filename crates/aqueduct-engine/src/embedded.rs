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
//! Not yet: retry policies (a failed step is terminal), timers and signals, a materialised state store
//! (recovery replays the whole log; checkpoints in redb come with ADR 0009's state store).

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use aqueduct_core::{CommandIndex, Decider, Payload, Status, StepOutcome};
use aqueduct_storage::{FileLog, FileLogOptions};

use crate::{Deciders, Outbound, Partition, PartitionError};

/// A step implementation.
pub type StepFn = Arc<dyn Fn(&Payload) -> StepOutcome + Send + Sync>;

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
    ack_before_sync: bool,
}

impl Builder {
    /// Registers a workflow (a procedure or any other decider).
    pub fn workflow(mut self, name: &str, decider: Arc<dyn Decider + Send + Sync>) -> Self {
        self.deciders.insert(name, decider);
        self
    }

    /// Registers a step implementation.
    pub fn step(
        mut self,
        name: &str,
        step: impl Fn(&Payload) -> StepOutcome + Send + Sync + 'static,
    ) -> Self {
        self.steps.insert(name.to_owned(), Arc::new(step));
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
        let mut partition =
            Partition::recover(log, self.deciders).map_err(|e| EngineError::Open(e.to_string()))?;
        partition.inject_ack_before_sync(self.ack_before_sync);

        let (requests, inbox) = mpsc::channel::<Request>();
        let (jobs, job_inbox) = mpsc::channel::<Job>();
        let job_inbox = Arc::new(Mutex::new(job_inbox));
        let steps = Arc::new(self.steps);
        let workers = (0..self.workers)
            .map(|_| {
                let inbox = Arc::clone(&job_inbox);
                let steps = Arc::clone(&steps);
                let results = requests.clone();
                std::thread::spawn(move || worker(&inbox, &steps, &results))
            })
            .collect();
        let processor = Processor {
            partition,
            jobs,
            waiters: BTreeMap::new(),
            starts: BTreeMap::new(),
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
            ack_before_sync: false,
        }
    }

    /// Starts a run. Idempotent per key: starting an existing key returns once that run's start is
    /// durable, without starting it again. Returns after the start is durable.
    pub fn start(&self, key: &str, workflow: &str, input: Payload) -> Result<(), EngineError> {
        let (reply, answer) = mpsc::channel();
        self.send(Request::Start {
            key: key.to_owned(),
            workflow: workflow.to_owned(),
            input,
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
        reply: Sender<Result<(), EngineError>>,
    },
    Wait {
        key: String,
        reply: Sender<Result<RunResult, EngineError>>,
    },
    StepResult {
        key: String,
        cmd: CommandIndex,
        outcome: StepOutcome,
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
    partition: Partition<FileLog>,
    jobs: Sender<Job>,
    waiters: BTreeMap<String, Vec<Sender<Result<RunResult, EngineError>>>>,
    starts: BTreeMap<String, Vec<Sender<Result<(), EngineError>>>>,
}

impl Processor {
    fn run(mut self, inbox: &Receiver<Request>, redispatch: Duration) -> Result<(), String> {
        // Recovery may have produced entries and dispatches: make them durable and send them.
        self.flush()?;
        loop {
            let first = match inbox.recv_timeout(redispatch) {
                Ok(request) => Some(request),
                Err(RecvTimeoutError::Timeout) => {
                    self.partition.redispatch();
                    None
                }
                Err(RecvTimeoutError::Disconnected) => return Ok(()),
            };
            // Group commit: take everything that is waiting, then sync once.
            for request in first.into_iter().chain(inbox.try_iter()) {
                if !self.handle(request)? {
                    self.flush()?;
                    return Ok(());
                }
            }
            self.flush()?;
        }
    }

    /// Applies one request; false on shutdown.
    fn handle(&mut self, request: Request) -> Result<bool, String> {
        match request {
            Request::Start {
                key,
                workflow,
                input,
                reply,
            } => match self.partition.start(&key, &workflow, input) {
                Ok(()) => self.starts.entry(key).or_default().push(reply),
                Err(PartitionError::UnknownWorkflow(name)) => {
                    let _ = reply.send(Err(EngineError::UnknownWorkflow(name)));
                }
                Err(error) => return Err(error.to_string()),
            },
            Request::Wait { key, reply } => {
                if self.partition.run(&key).is_none() {
                    let _ = reply.send(Err(EngineError::UnknownRun(key)));
                } else {
                    // Answered after the next flush, once the run's end is durable.
                    self.waiters.entry(key).or_default().push(reply);
                }
            }
            Request::StepResult { key, cmd, outcome } => {
                self.partition
                    .step_result(&key, cmd, outcome)
                    .map_err(|e| e.to_string())?;
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
                Outbound::ResultRecorded { .. } | Outbound::Finished { .. } => {}
            }
        }
    }

    /// Called right after a flush, when everything in memory is durable: answers waiters whose runs
    /// have ended.
    fn answer_waiters(&mut self) {
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

fn worker(
    inbox: &Mutex<Receiver<Job>>,
    steps: &BTreeMap<String, StepFn>,
    results: &Sender<Request>,
) {
    loop {
        let job = match inbox.lock() {
            Ok(inbox) => match inbox.recv() {
                Ok(job) => job,
                Err(_) => return,
            },
            Err(_) => return,
        };
        let outcome = match steps.get(&job.name) {
            None => StepOutcome::Failed(format!("no step named `{}` is registered", job.name)),
            Some(step) => catch_unwind(AssertUnwindSafe(|| step(&job.input)))
                .unwrap_or_else(|_| StepOutcome::Failed(format!("step `{}` panicked", job.name))),
        };
        if results
            .send(Request::StepResult {
                key: job.key,
                cmd: job.cmd,
                outcome,
            })
            .is_err()
        {
            return;
        }
    }
}
