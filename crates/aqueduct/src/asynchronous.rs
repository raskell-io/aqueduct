//! The embedded engine from async code on Tokio (the `tokio` feature).

use std::sync::Arc;
use std::time::Duration;

use aqueduct_core::{CommandIndex, Payload, RunState};
use aqueduct_engine::StartOptions;
use aqueduct_engine::embedded::{
    Attempt, Engine, EngineError, EngineStats, Escalation, RunResult, RunSummary,
};

/// An [`Engine`] for async code: the same calls, each run on Tokio's blocking pool, so that it never
/// holds up an async thread. Cheap to clone.
///
/// Dropping a future does not undo its call: a call already handed to the engine still happens. So
/// retry a timed-out or dropped `signal` or `update` with the same id, as with [`Engine`].
///
/// A [`AsyncEngine::wait`] or [`AsyncEngine::update`] holds a blocking thread for as long as it waits,
/// and Tokio's pool is limited (512 threads by default), so very many long waits at once can queue up.
///
/// ```no_run
/// # async fn example(engine: aqueduct::embedded::Engine) -> Result<(), aqueduct::embedded::EngineError> {
/// use aqueduct::Payload;
/// use aqueduct::embedded::AsyncEngine;
///
/// let engine = AsyncEngine::new(engine);
/// engine.start("greet:ada", "greet", Payload::new(br#""ada""#.to_vec())).await?;
/// let result = engine.wait("greet:ada", std::time::Duration::from_secs(10)).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct AsyncEngine {
    engine: Arc<Engine>,
}

impl std::fmt::Debug for AsyncEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AsyncEngine")
    }
}

impl From<Arc<Engine>> for AsyncEngine {
    fn from(engine: Arc<Engine>) -> AsyncEngine {
        AsyncEngine { engine }
    }
}

impl AsyncEngine {
    /// Wraps an open engine. The engine stops when the last clone is dropped.
    pub fn new(engine: Engine) -> AsyncEngine {
        AsyncEngine {
            engine: Arc::new(engine),
        }
    }

    /// The engine itself, for what has no async form here (the remote queues, for example).
    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Engine) -> Result<T, EngineError> + Send + 'static,
    ) -> Result<T, EngineError> {
        let engine = Arc::clone(&self.engine);
        tokio::task::spawn_blocking(move || f(&engine))
            .await
            .map_err(|e| EngineError::Stopped(e.to_string()))?
    }

    /// See [`Engine::start`].
    pub async fn start(
        &self,
        key: &str,
        workflow: &str,
        input: Payload,
    ) -> Result<(), EngineError> {
        let (key, workflow) = (key.to_owned(), workflow.to_owned());
        self.call(move |e| e.start(&key, &workflow, input)).await
    }

    /// See [`Engine::start_with`].
    pub async fn start_with(
        &self,
        key: &str,
        workflow: &str,
        input: Payload,
        options: impl Into<StartOptions>,
    ) -> Result<(), EngineError> {
        let (key, workflow, options) = (key.to_owned(), workflow.to_owned(), options.into());
        self.call(move |e| e.start_with(&key, &workflow, input, options))
            .await
    }

    /// See [`Engine::wait`].
    pub async fn wait(&self, key: &str, timeout: Duration) -> Result<RunResult, EngineError> {
        let key = key.to_owned();
        self.call(move |e| e.wait(&key, timeout)).await
    }

    /// See [`Engine::signal`].
    pub async fn signal(
        &self,
        key: &str,
        name: &str,
        id: &str,
        payload: Payload,
    ) -> Result<(), EngineError> {
        let (key, name, id) = (key.to_owned(), name.to_owned(), id.to_owned());
        self.call(move |e| e.signal(&key, &name, &id, payload))
            .await
    }

    /// See [`Engine::update`].
    pub async fn update(
        &self,
        key: &str,
        name: &str,
        id: &str,
        payload: Payload,
        timeout: Duration,
    ) -> Result<Payload, EngineError> {
        let (key, name, id) = (key.to_owned(), name.to_owned(), id.to_owned());
        self.call(move |e| e.update(&key, &name, &id, payload, timeout))
            .await
    }

    /// See [`Engine::query`].
    pub async fn query(&self, key: &str, name: &str) -> Result<Payload, EngineError> {
        let (key, name) = (key.to_owned(), name.to_owned());
        self.call(move |e| e.query(&key, &name)).await
    }

    /// See [`Engine::describe`].
    pub async fn describe(&self, key: &str) -> Result<(String, RunState), EngineError> {
        let key = key.to_owned();
        self.call(move |e| e.describe(&key)).await
    }

    /// See [`Engine::runs`].
    pub async fn runs(&self) -> Result<Vec<RunSummary>, EngineError> {
        self.call(Engine::runs).await
    }

    /// See [`Engine::cancel`].
    pub async fn cancel(&self, key: &str, reason: &str) -> Result<(), EngineError> {
        let (key, reason) = (key.to_owned(), reason.to_owned());
        self.call(move |e| e.cancel(&key, &reason)).await
    }

    /// See [`Engine::escalations`].
    pub async fn escalations(&self) -> Result<Vec<Escalation>, EngineError> {
        self.call(Engine::escalations).await
    }

    /// See [`Engine::resolve_escalation`].
    pub async fn resolve_escalation(&self, id: &str, outcome: Payload) -> Result<(), EngineError> {
        let id = id.to_owned();
        self.call(move |e| e.resolve_escalation(&id, outcome)).await
    }

    /// See [`Engine::complete_awakeable`].
    pub async fn complete_awakeable(&self, id: &str, payload: Payload) -> Result<(), EngineError> {
        let id = id.to_owned();
        self.call(move |e| e.complete_awakeable(&id, payload)).await
    }

    /// See [`Engine::step_result`].
    pub async fn step_result(
        &self,
        run: &str,
        cmd: CommandIndex,
        attempt: Attempt,
    ) -> Result<(), EngineError> {
        let run = run.to_owned();
        self.call(move |e| e.step_result(&run, cmd, attempt)).await
    }

    /// See [`Engine::stats`].
    pub async fn stats(&self) -> Result<EngineStats, EngineError> {
        self.call(Engine::stats).await
    }
}
