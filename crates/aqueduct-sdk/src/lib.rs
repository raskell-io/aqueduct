//! The Aqueduct Rust SDK: durable workflows as ordinary `async fn`s (BRIEF.md section 4.2, ADR 0014).
//!
//! A workflow receives a [`WorkflowContext`] and its input, and talks to the world only through the
//! context: [`WorkflowContext::step`], [`WorkflowContext::sleep`], [`WorkflowContext::wait_signal`] and
//! [`WorkflowContext::awakeable`].
//! [`workflow`] turns it into a [`Decider`] the engine runs: every activation runs the function from the
//! start; each context call gets the next command index the first time it is polled, resolves at once
//! if the journal holds its outcome, and is issued as a command otherwise. Concurrent calls (with
//! [`join`] or [`join_all`]) are issued together; [`select`] and [`select_all`] wait for the first to
//! finish, in the order the journal recorded them (ADR 0016).
//!
//! ```
//! use aqueduct_core::{Command, Decider, Payload, RunState, start};
//! use aqueduct_sdk::{WorkflowContext, WorkflowError, join, workflow};
//!
//! async fn greet(ctx: WorkflowContext, user: String) -> Result<String, WorkflowError> {
//!     let name: String = ctx.step("lookup_name", &user).await?;
//!     let (en, de) = join(ctx.step::<String, _>("greet_en", &name), ctx.step::<String, _>("greet_de", &name)).await;
//!     Ok(format!("{} / {}", en?, de?))
//! }
//!
//! let decider = workflow(greet);
//! let journal = start("greet", Payload::new(br#""u-1""#.to_vec())).entries;
//! let state = RunState::from_journal(&journal).unwrap();
//! assert_eq!(
//!     decider.decide(&state.view()),
//!     vec![Command::ScheduleStep { name: "lookup_name".into(), input: Payload::new(br#""u-1""#.to_vec()) }],
//! );
//! ```
//!
//! Determinism is checked, not enforced: a workflow must not read the clock, random numbers, the
//! environment or anything else outside its context. Replay detects divergence and parks the run. For
//! the outside world, the context records what the workflow saw: [`WorkflowContext::now`],
//! [`WorkflowContext::random`], [`WorkflowContext::uuid`] and, for anything else,
//! [`WorkflowContext::record`]. Each value is journaled the first time and read back on replay.

#![forbid(unsafe_code)]

use std::cell::RefCell;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aqueduct_core::{Command, Decider, Env, Payload, RunView, StepOutcome};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Why a workflow call or the workflow itself failed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WorkflowError {
    /// A step failed terminally (after its retry policy).
    #[error("step `{name}` failed: {error}")]
    StepFailed {
        /// The step's name.
        name: String,
        /// The step's error.
        error: String,
    },
    /// A value could not be encoded or decoded as JSON.
    #[error("payload: {0}")]
    Payload(String),
    /// The workflow failed for its own reason.
    #[error("{0}")]
    Failed(String),
    /// The run was cancelled while this call was waiting (`Engine::cancel`). The workflow may still
    /// issue calls afterwards, for example to undo its work with a [`Saga`].
    #[error("cancelled: {0}")]
    Cancelled(String),
}

impl WorkflowError {
    /// A workflow-defined failure.
    pub fn failed(message: impl Into<String>) -> Self {
        WorkflowError::Failed(message.into())
    }
}

#[derive(Default)]
struct Activation {
    outcomes: Vec<Option<StepOutcome>>,
    /// Each command's position in the journal's completion order, if it has an outcome.
    ranks: Vec<Option<usize>>,
    /// Which commands a cancellation completed.
    cancelled: Vec<bool>,
    cancel_reason: String,
    commands: Vec<Command>,
    env: Env,
    key: String,
    /// The latest completion (by rank) among the calls resolved since [`select`] last reset it; `None`
    /// when none was. Values recorded in this activation rank after everything in the journal.
    resolved: Option<usize>,
}

impl Activation {
    /// The cancellation error, if a cancellation completed this command.
    fn cancellation(&self, index: usize) -> Option<WorkflowError> {
        self.cancelled
            .get(index)
            .copied()
            .unwrap_or(false)
            .then(|| WorkflowError::Cancelled(self.cancel_reason.clone()))
    }

    fn resolve(&mut self, index: usize) {
        let rank = self
            .ranks
            .get(index)
            .copied()
            .flatten()
            .unwrap_or(usize::MAX);
        self.resolved = Some(self.resolved.map_or(rank, |r| r.max(rank)));
    }
}

/// SplitMix64's output function: spreads a seed's bits.
fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The workflow's only door to the world. Cheap to clone.
#[derive(Clone)]
pub struct WorkflowContext {
    activation: Rc<RefCell<Activation>>,
}

impl WorkflowContext {
    /// Issues the next command; returns its index and its recorded outcome, if any.
    fn issue(&self, command: Command) -> (usize, Option<StepOutcome>) {
        let mut activation = self.activation.borrow_mut();
        let index = activation.commands.len();
        activation.commands.push(command);
        (index, activation.outcomes.get(index).cloned().flatten())
    }

    /// Records a value from outside the workflow: the journaled value when there is one, otherwise
    /// `fresh(env, command index)`, which is then journaled.
    fn record_with(&self, name: &str, fresh: impl FnOnce(Env, u64) -> Payload) -> Payload {
        // The command index is taken before `fresh` runs: a `record` closure may use the context too,
        // and its calls come after this one.
        let (index, recorded, env) = {
            let mut activation = self.activation.borrow_mut();
            let index = activation.commands.len();
            let recorded = match activation.outcomes.get(index) {
                Some(Some(StepOutcome::Completed(recorded))) => Some(recorded.clone()),
                _ => None,
            };
            activation.commands.push(Command::Record {
                name: name.to_owned(),
                value: Payload::default(),
            });
            (index, recorded, activation.env)
        };
        // Not recorded yet (or the journal diverges here, which the engine reports by parking).
        let value = recorded.unwrap_or_else(|| fresh(env, index as u64));
        let mut activation = self.activation.borrow_mut();
        if let Some(Command::Record { value: slot, .. }) = activation.commands.get_mut(index) {
            *slot = value.clone();
        }
        activation.resolve(index);
        value
    }

    /// The current time, recorded: the engine's clock the first time, the same instant on every replay.
    pub fn now(&self) -> SystemTime {
        let value = self.record_with("now", |env, _| {
            Payload::new(env.now_ms.to_string().into_bytes())
        });
        let ms = std::str::from_utf8(value.as_bytes())
            .ok()
            .and_then(|text| text.parse::<u64>().ok())
            .unwrap_or_default();
        UNIX_EPOCH + Duration::from_millis(ms)
    }

    /// 64 random bits, recorded. Seeded by the engine from the operating system; not for secrets.
    pub fn random(&self) -> u64 {
        let value = self.record_with("random", |env, index| {
            let bits = mix(env.seed ^ mix(index.wrapping_add(1)));
            Payload::new(bits.to_string().into_bytes())
        });
        std::str::from_utf8(value.as_bytes())
            .ok()
            .and_then(|text| text.parse::<u64>().ok())
            .unwrap_or_default()
    }

    /// A random (version 4) UUID, recorded. Unique for practical purposes; not for secrets.
    pub fn uuid(&self) -> uuid::Uuid {
        let value = self.record_with("uuid", |env, index| {
            let high = mix(env.seed ^ mix(index.wrapping_add(1)));
            let low = mix(high ^ 0x7575_6964_7575_6964);
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&high.to_be_bytes());
            bytes[8..].copy_from_slice(&low.to_be_bytes());
            let id = uuid::Builder::from_random_bytes(bytes).into_uuid();
            Payload::new(format!("\"{id}\"").into_bytes())
        });
        std::str::from_utf8(value.as_bytes())
            .ok()
            .and_then(|text| uuid::Uuid::try_parse(text.trim_matches('"')).ok())
            .unwrap_or_default()
    }

    /// Runs `compute` once and records its result; replay returns the recorded result without running
    /// it again. For quick, local reads of the outside world (configuration, the environment, a local
    /// file). Anything slow, remote or with side effects belongs in a step, which has retries.
    pub fn record<T, F>(&self, name: &str, compute: F) -> Result<T, WorkflowError>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> T,
    {
        let mut encode_error = None;
        let value = self.record_with(name, |_, _| match serde_json::to_vec(&compute()) {
            Ok(bytes) => Payload::new(bytes),
            Err(e) => {
                encode_error = Some(WorkflowError::Payload(e.to_string()));
                Payload::default()
            }
        });
        if let Some(error) = encode_error {
            return Err(error);
        }
        serde_json::from_slice(value.as_bytes()).map_err(|e| WorkflowError::Payload(e.to_string()))
    }

    /// Runs a step (registered with the engine by name) with a JSON-encoded input and decodes its result.
    /// At least once execution, exactly once recording.
    pub fn step<T, I>(
        &self,
        name: &str,
        input: &I,
    ) -> impl Future<Output = Result<T, WorkflowError>> + use<T, I>
    where
        T: DeserializeOwned,
        I: Serialize + ?Sized,
    {
        let encoded = serde_json::to_vec(input).map_err(|e| WorkflowError::Payload(e.to_string()));
        let command = encoded.map(|input| Command::ScheduleStep {
            name: name.to_owned(),
            input: Payload::new(input),
        });
        let name = name.to_owned();
        self.call(command, move |outcome| match outcome {
            StepOutcome::Completed(result) => serde_json::from_slice(result.as_bytes())
                .map_err(|e| WorkflowError::Payload(e.to_string())),
            StepOutcome::Failed(error) => Err(WorkflowError::StepFailed { name, error }),
        })
    }

    /// Sleeps durably. The wake time is journaled when the sleep is first issued, so a restart does not
    /// extend it. Fails only if the run is cancelled during the sleep.
    pub fn sleep(
        &self,
        duration: Duration,
    ) -> impl Future<Output = Result<(), WorkflowError>> + use<> {
        let duration_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.call(Ok(Command::Sleep { duration_ms }), |_| Ok(()))
    }

    /// Waits for the next signal with this name and decodes its payload, or returns `None` when the
    /// timeout passes first. Without a timeout it waits forever.
    pub fn wait_signal<T: DeserializeOwned>(
        &self,
        name: &str,
        timeout: Option<Duration>,
    ) -> impl Future<Output = Result<Option<T>, WorkflowError>> + use<T> {
        let timeout_ms = timeout.map_or(0, |t| {
            u64::try_from(t.as_millis()).unwrap_or(u64::MAX).max(1)
        });
        self.call(
            Ok(Command::AwaitSignal {
                name: name.to_owned(),
                timeout_ms,
            }),
            |outcome| match outcome {
                StepOutcome::Completed(payload) => serde_json::from_slice(payload.as_bytes())
                    .map(Some)
                    .map_err(|e| WorkflowError::Payload(e.to_string())),
                StepOutcome::Failed(_) => Ok(None),
            },
        )
    }

    /// Escalates a decision to a person or an agent and waits for the resolution, which it decodes.
    /// An escalation is an outcome, not an error: the run waits durably (for days if need be), shows up
    /// in `Engine::escalations`, and continues with whatever the resolver decides. Escalations are
    /// counted by `reason`.
    pub fn escalate<T, P>(
        &self,
        reason: &str,
        payload: &P,
    ) -> impl Future<Output = Result<T, WorkflowError>> + use<T, P>
    where
        T: DeserializeOwned,
        P: Serialize + ?Sized,
    {
        let command = serde_json::to_vec(payload)
            .map_err(|e| WorkflowError::Payload(e.to_string()))
            .map(|payload| Command::Escalate {
                reason: reason.to_owned(),
                payload: Payload::new(payload),
            });
        self.call(command, |outcome| match outcome {
            StepOutcome::Completed(resolution) => serde_json::from_slice(resolution.as_bytes())
                .map_err(|e| WorkflowError::Payload(e.to_string())),
            StepOutcome::Failed(error) => Err(WorkflowError::Failed(error)),
        })
    }

    /// Starts a [`Saga`]: steps with compensations.
    pub fn saga(&self) -> Saga {
        Saga {
            ctx: self.clone(),
            compensations: Vec::new(),
        }
    }

    /// Creates an awakeable: a wait that whoever holds its id completes from outside, for example a
    /// person following a link, or a webhook. Returns the id to hand out and a future of the payload it
    /// is completed with (`Engine::complete_awakeable` in the embedded engine). Completing it twice
    /// records the first payload only. The id names the run and the command, so it is the same on every
    /// replay. To give up after a while, [`select`] it against a [`WorkflowContext::sleep`].
    pub fn awakeable<T: DeserializeOwned>(
        &self,
    ) -> (
        String,
        impl Future<Output = Result<T, WorkflowError>> + use<T>,
    ) {
        let (index, key) = {
            let activation = self.activation.borrow();
            (activation.commands.len(), activation.key.clone())
        };
        let cmd = aqueduct_core::CommandIndex(u32::try_from(index).unwrap_or(u32::MAX));
        let (_, outcome) = self.issue(Command::AwaitSignal {
            name: aqueduct_core::awakeable_signal(cmd),
            timeout_ms: 0,
        });
        let context = self.clone();
        let mut outcome = outcome;
        let future = poll_fn(move |_| match outcome.take() {
            Some(_) if context.activation.borrow().cancellation(index).is_some() => {
                let mut activation = context.activation.borrow_mut();
                activation.resolve(index);
                Poll::Ready(Err(activation
                    .cancellation(index)
                    .unwrap_or_else(|| WorkflowError::Cancelled(String::new()))))
            }
            Some(StepOutcome::Completed(payload)) => {
                context.activation.borrow_mut().resolve(index);
                Poll::Ready(
                    serde_json::from_slice(payload.as_bytes())
                        .map_err(|e| WorkflowError::Payload(e.to_string())),
                )
            }
            Some(StepOutcome::Failed(error)) => {
                context.activation.borrow_mut().resolve(index);
                Poll::Ready(Err(WorkflowError::Failed(error)))
            }
            None => Poll::Pending,
        });
        (aqueduct_core::awakeable_id(&key, cmd), future)
    }

    /// A future that issues `command` when first polled and resolves once its outcome is recorded.
    fn call<R, G>(
        &self,
        command: Result<Command, WorkflowError>,
        finish: G,
    ) -> impl Future<Output = Result<R, WorkflowError>> + use<R, G>
    where
        G: FnOnce(StepOutcome) -> Result<R, WorkflowError>,
    {
        let context = self.clone();
        let mut state = Some((command, finish));
        let mut outcome: Option<Option<StepOutcome>> = None;
        let mut index = 0;
        poll_fn(move |_| {
            if outcome.is_none() {
                let Some((command, _)) = state.as_mut() else {
                    return Poll::Pending;
                };
                let command = match std::mem::replace(command, Err(WorkflowError::failed("issued")))
                {
                    Ok(command) => command,
                    Err(error) => return Poll::Ready(Err(error)),
                };
                let (issued, recorded) = context.issue(command);
                index = issued;
                outcome = Some(recorded);
            }
            match outcome.as_mut().and_then(Option::take) {
                Some(result) => match state.take() {
                    Some((_, finish)) => {
                        let mut activation = context.activation.borrow_mut();
                        activation.resolve(index);
                        match activation.cancellation(index) {
                            Some(cancelled) => Poll::Ready(Err(cancelled)),
                            None => {
                                drop(activation);
                                Poll::Ready(finish(result))
                            }
                        }
                    }
                    None => Poll::Pending,
                },
                // Not recorded yet: the run waits, and this activation ends here.
                None => Poll::Pending,
            }
        })
    }
}

/// A saga: steps that each register a compensation, undone in reverse order when the workflow gives up.
///
/// Compensations are ordinary steps, so they are journaled, retried by their retry policy and replayed
/// like any other step; give them a generous policy, since a saga cannot undo an undo. Create one with
/// [`WorkflowContext::saga`].
///
/// ```
/// use aqueduct_sdk::{WorkflowContext, WorkflowError};
///
/// async fn order(ctx: WorkflowContext, id: String) -> Result<String, WorkflowError> {
///     let mut saga = ctx.saga();
///     let result = async {
///         saga.step::<(), _, _>("reserve_stock", &id, "release_stock", &id).await?;
///         saga.step::<String, _, _>("charge_card", &id, "refund_card", &id).await?;
///         ctx.step::<String, _>("book_shipment", &id).await
///     }
///     .await;
///     match result {
///         Ok(tracking) => Ok(tracking),
///         // Refund, then release: the reverse of the order they were done in.
///         Err(error) => Err(saga.abort(error).await),
///     }
/// }
/// ```
pub struct Saga {
    ctx: WorkflowContext,
    compensations: Vec<(String, serde_json::Value)>,
}

impl Saga {
    /// Runs a step and, once it has succeeded, registers `compensation` (a step) with its input. A step
    /// that may have had an effect even when it fails should register its compensation first, with
    /// [`Saga::compensate_with`].
    pub async fn step<T, I, C>(
        &mut self,
        name: &str,
        input: &I,
        compensation: &str,
        compensation_input: &C,
    ) -> Result<T, WorkflowError>
    where
        T: DeserializeOwned,
        I: Serialize + ?Sized,
        C: Serialize + ?Sized,
    {
        let undo = serde_json::to_value(compensation_input)
            .map_err(|e| WorkflowError::Payload(e.to_string()))?;
        let output = self.ctx.step(name, input).await?;
        self.compensations.push((compensation.to_owned(), undo));
        Ok(output)
    }

    /// Registers a compensation (a step) to run if the saga is undone.
    pub fn compensate_with<C: Serialize + ?Sized>(
        &mut self,
        compensation: &str,
        input: &C,
    ) -> Result<(), WorkflowError> {
        let undo =
            serde_json::to_value(input).map_err(|e| WorkflowError::Payload(e.to_string()))?;
        self.compensations.push((compensation.to_owned(), undo));
        Ok(())
    }

    /// The compensations registered so far, in the order they were registered.
    pub fn compensations(&self) -> impl Iterator<Item = &str> {
        self.compensations.iter().map(|(name, _)| name.as_str())
    }

    /// Runs every registered compensation, last first, one after the other. A compensation that fails
    /// does not stop the others; the error names every one that failed.
    ///
    /// Compensations cannot be cancelled: one that a cancellation completes is issued again, and runs
    /// (a run is cancelled at most once, so this happens at most once per saga).
    pub async fn compensate(self) -> Result<(), WorkflowError> {
        let mut failed = Vec::new();
        for (name, input) in self.compensations.into_iter().rev() {
            let mut outcome = self.ctx.step::<serde_json::Value, _>(&name, &input).await;
            if matches!(outcome, Err(WorkflowError::Cancelled(_))) {
                outcome = self.ctx.step::<serde_json::Value, _>(&name, &input).await;
            }
            if let Err(error) = outcome {
                failed.push(error.to_string());
            }
        }
        if failed.is_empty() {
            Ok(())
        } else {
            Err(WorkflowError::Failed(format!(
                "compensation failed: {}",
                failed.join("; ")
            )))
        }
    }

    /// Undoes the saga and returns the error to fail the workflow with: `error`, or, if a compensation
    /// failed too, both.
    pub async fn abort(self, error: WorkflowError) -> WorkflowError {
        match self.compensate().await {
            Ok(()) => error,
            Err(undo) => WorkflowError::Failed(format!("{error}; {undo}")),
        }
    }
}

/// Runs two workflow calls concurrently: both are issued before either is awaited.
pub async fn join<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let mut a = Box::pin(a);
    let mut b = Box::pin(b);
    let (mut ra, mut rb) = (None, None);
    poll_fn(move |cx| {
        if ra.is_none()
            && let Poll::Ready(value) = a.as_mut().poll(cx)
        {
            ra = Some(value);
        }
        if rb.is_none()
            && let Poll::Ready(value) = b.as_mut().poll(cx)
        {
            rb = Some(value);
        }
        match (ra.take(), rb.take()) {
            (Some(a), Some(b)) => Poll::Ready((a, b)),
            (a, b) => {
                ra = a;
                rb = b;
                Poll::Pending
            }
        }
    })
    .await
}

/// Runs workflow calls concurrently and returns their outputs in order.
pub async fn join_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut futures: Vec<Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    let mut outputs: Vec<Option<F::Output>> = futures.iter().map(|_| None).collect();
    poll_fn(move |cx| {
        for (future, output) in futures.iter_mut().zip(outputs.iter_mut()) {
            if output.is_none()
                && let Poll::Ready(value) = future.as_mut().poll(cx)
            {
                *output = Some(value);
            }
        }
        if outputs.iter().all(Option::is_some) {
            Poll::Ready(outputs.iter_mut().filter_map(Option::take).collect())
        } else {
            Poll::Pending
        }
    })
    .await
}

/// One of two outcomes, from [`select`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Either<L, R> {
    /// The first future finished first.
    Left(L),
    /// The second future finished first.
    Right(R),
}

thread_local! {
    /// The activation of the workflow being polled on this thread, for [`select`].
    static CURRENT: RefCell<Option<Rc<RefCell<Activation>>>> = const { RefCell::new(None) };
}

/// Polls `future` and returns its output with the rank of the latest completion it consumed.
fn poll_ranked<F: Future + ?Sized>(
    future: Pin<&mut F>,
    cx: &mut Context<'_>,
) -> Poll<(F::Output, Option<usize>)> {
    let activation = CURRENT.with(|current| current.borrow().clone());
    let saved = activation
        .as_ref()
        .and_then(|a| a.borrow_mut().resolved.take());
    let polled = future.poll(cx);
    let rank = activation.as_ref().and_then(|a| {
        let mut a = a.borrow_mut();
        let rank = a.resolved;
        // The caller's own record continues with whatever this branch consumed.
        a.resolved = match (saved, rank) {
            (Some(s), Some(r)) => Some(s.max(r)),
            (s, r) => s.or(r),
        };
        rank
    });
    polled.map(|output| (output, rank))
}

/// Waits for whichever future finishes first. Both are issued at once. The winner is decided by the
/// order in which the journal recorded their outcomes, never by polling order, so a replay that finds
/// both finished takes the same branch as the first execution.
///
/// The other future is dropped, but its commands stay issued: a losing step still runs, a losing sleep
/// still fires, and a losing signal wait still takes the next signal with its name. Prefer
/// [`WorkflowContext::wait_signal`]'s timeout for timed signal waits.
pub async fn select<A: Future, B: Future>(a: A, b: B) -> Either<A::Output, B::Output> {
    let (index, output) = select_all(vec![
        Box::pin(async move { Either::Left(a.await) })
            as Pin<Box<dyn Future<Output = Either<A::Output, B::Output>>>>,
        Box::pin(async move { Either::Right(b.await) }),
    ])
    .await;
    let _ = index;
    output
}

/// Waits for the first of several futures to finish and returns its position and output. The winner is
/// the one whose outcome the journal recorded first; see [`select`].
pub async fn select_all<F: Future>(futures: Vec<F>) -> (usize, F::Output) {
    let mut futures: Vec<Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    poll_fn(move |cx| {
        let mut best: Option<(Option<usize>, usize, F::Output)> = None;
        for (position, future) in futures.iter_mut().enumerate() {
            if let Poll::Ready((output, rank)) = poll_ranked(future.as_mut(), cx) {
                // A branch that consumed no completion is ready at once and wins outright.
                let earlier = match &best {
                    None => true,
                    Some((best_rank, _, _)) => match (rank, best_rank) {
                        (None, Some(_)) => true,
                        (Some(r), Some(b)) => r < *b,
                        _ => false,
                    },
                };
                if earlier {
                    best = Some((rank, position, output));
                }
            }
        }
        match best {
            Some((rank, position, output)) => {
                CURRENT.with(|current| {
                    if let Some(a) = current.borrow().as_ref() {
                        let mut a = a.borrow_mut();
                        a.resolved = match (a.resolved, rank) {
                            (Some(s), Some(r)) => Some(s.max(r)),
                            (s, r) => s.or(r),
                        };
                    }
                });
                Poll::Ready((position, output))
            }
            None => Poll::Pending,
        }
    })
    .await
}

/// A workflow function turned into a [`Decider`].
pub struct Workflow<F, I, O, Fut> {
    function: F,
    types: std::marker::PhantomData<fn(I) -> (Fut, O)>,
}

/// Turns `async fn(WorkflowContext, I) -> Result<O, WorkflowError>` into a [`Decider`]. The input and
/// output are JSON (ADR 0010).
pub fn workflow<F, Fut, I, O>(function: F) -> Workflow<F, I, O, Fut>
where
    F: Fn(WorkflowContext, I) -> Fut + Send + Sync,
    Fut: Future<Output = Result<O, WorkflowError>>,
    I: DeserializeOwned,
    O: Serialize,
{
    Workflow {
        function,
        types: std::marker::PhantomData,
    }
}

impl<F, Fut, I, O> Decider for Workflow<F, I, O, Fut>
where
    F: Fn(WorkflowContext, I) -> Fut + Send + Sync,
    Fut: Future<Output = Result<O, WorkflowError>>,
    I: DeserializeOwned,
    O: Serialize,
{
    fn decide(&self, view: &RunView<'_>) -> Vec<Command> {
        let outcomes: Vec<Option<StepOutcome>> = view.outcomes().map(|o| o.cloned()).collect();
        let mut ranks = vec![None; outcomes.len()];
        for (rank, cmd) in view.completion_order().iter().enumerate() {
            if let Some(slot) = usize::try_from(cmd.0).ok().and_then(|i| ranks.get_mut(i)) {
                *slot = Some(rank);
            }
        }
        let cancelled = (0..outcomes.len())
            .map(|index| view.cancelled(aqueduct_core::CommandIndex(index as u32)))
            .collect();
        let activation = Rc::new(RefCell::new(Activation {
            outcomes,
            ranks,
            cancelled,
            cancel_reason: view.cancel_reason().unwrap_or_default().to_owned(),
            commands: Vec::new(),
            env: view.env(),
            key: view.key().to_owned(),
            resolved: None,
        }));
        let input: I = match serde_json::from_slice(view.input().as_bytes()) {
            Ok(input) => input,
            Err(e) => {
                return vec![Command::Park {
                    reason: format!("the run's input does not decode: {e}"),
                }];
            }
        };
        let context = WorkflowContext {
            activation: Rc::clone(&activation),
        };
        let mut future = Box::pin((self.function)(context, input));
        // One poll runs the workflow as far as the journal allows: every call with a recorded outcome
        // resolves at once, and the first call without one leaves the future pending.
        CURRENT.with(|current| *current.borrow_mut() = Some(Rc::clone(&activation)));
        let polled = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        CURRENT.with(|current| *current.borrow_mut() = None);
        drop(future);
        let mut commands = std::mem::take(&mut activation.borrow_mut().commands);
        match polled {
            Poll::Pending => {}
            Poll::Ready(Ok(output)) => match serde_json::to_vec(&output) {
                Ok(output) => commands.push(Command::CompleteRun {
                    output: Payload::new(output),
                }),
                Err(e) => commands.push(Command::Park {
                    reason: format!("the output does not encode: {e}"),
                }),
            },
            Poll::Ready(Err(WorkflowError::Payload(e))) => {
                commands.push(Command::Park {
                    reason: format!("a payload does not match its type: {e}"),
                });
            }
            Poll::Ready(Err(error)) => commands.push(Command::FailRun {
                error: error.to_string(),
            }),
        }
        commands
    }
}
