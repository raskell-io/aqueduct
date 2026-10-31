//! The Aqueduct Rust SDK: durable workflows as ordinary `async fn`s (BRIEF.md section 4.2, ADR 0014).
//!
//! A workflow receives a [`WorkflowContext`] and its input, and talks to the world only through the
//! context: [`WorkflowContext::step`], [`WorkflowContext::sleep`] and [`WorkflowContext::wait_signal`].
//! [`workflow`] turns it into a [`Decider`] the engine runs: every activation runs the function from the
//! start; each context call gets the next command index the first time it is polled, resolves at once
//! if the journal holds its outcome, and is issued as a command otherwise. Concurrent calls (with
//! [`join`] or [`join_all`]) are issued together.
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
    commands: Vec<Command>,
    env: Env,
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
    /// Issues the next command, or returns its recorded outcome.
    fn issue(&self, command: Command) -> Option<StepOutcome> {
        let mut activation = self.activation.borrow_mut();
        let index = activation.commands.len();
        activation.commands.push(command);
        activation.outcomes.get(index).cloned().flatten()
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
        if let Some(Command::Record { value: slot, .. }) =
            self.activation.borrow_mut().commands.get_mut(index)
        {
            *slot = value.clone();
        }
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
    /// extend it.
    pub fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + use<> {
        let duration_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        let call = self.call(Ok(Command::Sleep { duration_ms }), |_| {
            Ok::<(), WorkflowError>(())
        });
        async move {
            let _ = call.await;
        }
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
                outcome = Some(context.issue(command));
            }
            match outcome.as_mut().and_then(Option::take) {
                Some(result) => match state.take() {
                    Some((_, finish)) => Poll::Ready(finish(result)),
                    None => Poll::Pending,
                },
                // Not recorded yet: the run waits, and this activation ends here.
                None => Poll::Pending,
            }
        })
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
        let activation = Rc::new(RefCell::new(Activation {
            outcomes: view.outcomes().map(|o| o.cloned()).collect(),
            commands: Vec::new(),
            env: view.env(),
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
        let polled = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
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
