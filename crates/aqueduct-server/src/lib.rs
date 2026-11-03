//! The Aqueduct server (ADR 0038): an embedded engine behind the ingress API of protocol v1 (draft),
//! over gRPC and HTTP/JSON on one listener.
//!
//! The engine keeps its partition thread; the network runs on tokio and calls the engine from blocking
//! tasks. Every answer is sent once what it reflects is durable, as in the embedded engine.
//!
//! ```no_run
//! # async fn example(engine: aqueduct_engine::embedded::Engine) -> std::io::Result<()> {
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:7470").await?;
//! aqueduct_server::serve(std::sync::Arc::new(engine), listener, std::future::pending()).await
//! # }
//! ```
//!
//! [`docs/protocol.md`](https://github.com/raskell-io/aqueduct/blob/main/docs/protocol.md) lists the
//! calls and their HTTP/JSON routes.

#![forbid(unsafe_code)]

pub mod audit;
pub mod auth;
mod push;
mod tls;
mod workers;
mod workflows;

pub use audit::Audit;
pub use auth::{Authenticator, Permission, Principal, Refusal};
pub use push::{Config, PushEndpoint};
pub use tls::Tls;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use aqueduct_core::{CommandKind, Payload, RunState, Status, StepOutcome};
use aqueduct_engine::embedded::{Engine, EngineError, RunSummary};
use aqueduct_engine::{ReusePolicy, StartOptions};
use aqueduct_protocol::v1;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

/// The default port: `127.0.0.1:7470`.
pub const DEFAULT_PORT: u16 = 7470;

/// How long a wait or an update waits when the caller gives no timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
/// The longest a single wait or update may wait.
const MAX_TIMEOUT: Duration = Duration::from_secs(3_600);

/// Whether an address is on the loopback interface: until authentication exists (ADR 0038), the
/// server listens elsewhere only when asked to explicitly.
pub fn is_loopback(addr: &SocketAddr) -> bool {
    addr.ip().is_loopback()
}

/// Serves the engine on `listener` until `shutdown` completes: gRPC (`aqueduct.v1.Ingress`) and
/// HTTP/JSON (`/v1/...`) on the same port.
pub async fn serve(
    engine: Arc<Engine>,
    listener: tokio::net::TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(listener, router(engine))
        .with_graceful_shutdown(shutdown)
        .await
}

/// [`serve`], with a [`Config`] and [`Security`]: its push endpoints are started (the engine's step
/// routes must already be set from it, with [`Config::apply`]), and requests are authenticated and
/// audited.
pub async fn serve_with(
    engine: Arc<Engine>,
    listener: tokio::net::TcpListener,
    config: &Config,
    security: Security,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    push::start(&engine, config).map_err(std::io::Error::other)?;
    axum::serve(listener, router_secured(engine, security))
        .with_graceful_shutdown(shutdown)
        .await
}

/// [`serve_with`], over TLS (ADR 0042).
pub async fn serve_tls(
    engine: Arc<Engine>,
    listener: tokio::net::TcpListener,
    config: &Config,
    security: Security,
    tls: Tls,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    push::start(&engine, config).map_err(std::io::Error::other)?;
    tls::serve(router_secured(engine, security), listener, tls, shutdown).await
}

/// The routes, gRPC and HTTP/JSON, for serving with axum or another tower server.
pub fn router(engine: Arc<Engine>) -> Router {
    router_secured(engine, Security::none())
}

/// [`router`], with authentication and an audit log (ADR 0041).
pub fn router_secured(engine: Arc<Engine>, security: Security) -> Router {
    let api = Api {
        engine,
        security: Arc::new(security),
    };
    // One set of gRPC routes for both services: each set has its own fallback, and a router can have
    // only one.
    let grpc = tonic::service::Routes::new(v1::ingress_server::IngressServer::new(api.clone()))
        .add_service(v1::workers_server::WorkersServer::new(api.clone()))
        .add_service(v1::workflows_server::WorkflowsServer::new(api.clone()))
        .into_axum_router();
    let http = Router::new()
        .route("/v1/runs", post(http_start).get(http_list))
        .route("/v1/runs/{key}", get(http_describe))
        .route("/v1/runs/{key}/result", get(http_wait))
        .route("/v1/runs/{key}/signals/{name}", post(http_signal))
        .route("/v1/runs/{key}/cancel", post(http_cancel))
        .route("/v1/runs/{key}/queries/{name}", get(http_query))
        .route("/v1/runs/{key}/updates/{name}", post(http_update))
        .route("/v1/awakeables/{id}", post(http_complete_awakeable))
        .route("/v1/escalations", get(http_escalations))
        .route("/v1/escalations/{id}", post(http_resolve))
        .with_state(api.clone());
    grpc.merge(http)
        .merge(workers::routes(api.clone()))
        .layer(axum::middleware::from_fn_with_state(api, authorize))
}

/// Why a call failed, in terms both transports can say.
#[derive(Debug)]
struct Failure {
    kind: Kind,
    message: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    NotFound,
    Conflict,
    Invalid,
    Timeout,
    Rejected,
    Unavailable,
    Internal,
    Unauthenticated,
    Forbidden,
}

/// The server's authentication and audit log (ADR 0041).
#[derive(Debug)]
pub struct Security {
    /// Who may do what.
    pub auth: Authenticator,
    /// What was done.
    pub audit: Audit,
}

impl Security {
    /// By a data directory's tokens file, if it has one, and its audit log.
    pub fn for_dir(dir: &std::path::Path) -> Result<Security, String> {
        Ok(Security {
            auth: Authenticator::for_dir(dir),
            audit: Audit::open(dir)?,
        })
    }

    /// No authentication, and no audit log.
    pub fn none() -> Security {
        Security {
            auth: Authenticator::none(),
            audit: Audit::none(),
        }
    }
}

tokio::task_local! {
    /// Who made the request being handled.
    static PRINCIPAL: Principal;
}

/// The permission a route needs, and its name in the audit log. A route not listed needs `admin`, so a
/// new route is closed until it is given a permission here.
fn required(method: &axum::http::Method, path: &str) -> (Permission, &'static str) {
    use axum::http::Method;
    if let Some(call) = path.strip_prefix("/aqueduct.v1.Ingress/") {
        return match call {
            "Start" => (Permission::Start, "start"),
            "Signal" => (Permission::Signal, "signal"),
            "CompleteAwakeable" => (Permission::Signal, "complete awakeable"),
            "Update" => (Permission::Signal, "update"),
            "Cancel" => (Permission::Cancel, "cancel"),
            "ResolveEscalation" => (Permission::Resolve, "resolve"),
            "Wait" | "Describe" | "ListRuns" | "Query" | "ListEscalations" => {
                (Permission::Read, "read")
            }
            _ => (Permission::Admin, "unknown call"),
        };
    }
    if path == "/aqueduct.v1.Workers/Work" || path == "/aqueduct.v1.Workflows/Serve" {
        return (Permission::Work, "work");
    }
    if path.starts_with("/v1/workers/") {
        return (Permission::Work, "work");
    }
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    match (method, parts.as_slice()) {
        (&Method::POST, ["v1", "runs"]) => (Permission::Start, "start"),
        (&Method::POST, ["v1", "runs", _, "signals", _]) => (Permission::Signal, "signal"),
        (&Method::POST, ["v1", "runs", _, "updates", _]) => (Permission::Signal, "update"),
        (&Method::POST, ["v1", "runs", _, "cancel"]) => (Permission::Cancel, "cancel"),
        (&Method::POST, ["v1", "awakeables", _]) => (Permission::Signal, "complete awakeable"),
        (&Method::POST, ["v1", "escalations", _]) => (Permission::Resolve, "resolve"),
        (&Method::GET, ["v1", "runs", ..] | ["v1", "escalations"]) => (Permission::Read, "read"),
        _ => (Permission::Admin, "unknown route"),
    }
}

/// Checks every request, gRPC and HTTP alike, before it reaches a handler.
async fn authorize(
    State(api): State<Api>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let (needed, action) = required(request.method(), request.uri().path());
    let header = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    let refusal = match api.security.auth.check(header, needed) {
        Ok(principal) => return PRINCIPAL.scope(principal, next.run(request)).await,
        Err(refusal) => refusal,
    };
    let path = request.uri().path().to_owned();
    let grpc = request
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/grpc"));
    // No handler runs, so no principal is set: the log is told who was refused. A caller without a
    // valid token is `unauthenticated`, whatever it claimed.
    let (kind, who, message) = match refusal {
        Refusal::Unauthenticated(message) => {
            (Kind::Unauthenticated, "unauthenticated".to_owned(), message)
        }
        Refusal::Forbidden(who, message) => (Kind::Forbidden, who, message),
    };
    PRINCIPAL
        .scope(
            Principal::Token(who),
            api.audit(action, path, format!("refused: {message}")),
        )
        .await;
    let failure = Failure { kind, message };
    if grpc {
        return tonic::Status::from(failure).into_http();
    }
    failure.into_response()
}

impl Failure {
    fn invalid(message: impl Into<String>) -> Failure {
        Failure {
            kind: Kind::Invalid,
            message: message.into(),
        }
    }
}

impl From<EngineError> for Failure {
    fn from(error: EngineError) -> Failure {
        let kind = match &error {
            EngineError::UnknownWorkflow(_)
            | EngineError::UnknownVersion { .. }
            | EngineError::UnknownRun(_)
            | EngineError::UnknownQuery { .. } => Kind::NotFound,
            EngineError::KeyConflict(_) | EngineError::AlreadyStarted(_) => Kind::Conflict,
            EngineError::InvalidKey(_)
            | EngineError::InvalidEscalation(_)
            | EngineError::InvalidAwakeable(_) => Kind::Invalid,
            EngineError::Timeout(_) => Kind::Timeout,
            EngineError::UpdateRejected(_)
            | EngineError::InvalidResolution(_)
            | EngineError::Unsupported(_) => Kind::Rejected,
            EngineError::Stopped(_) => Kind::Unavailable,
            EngineError::Open(_) => Kind::Internal,
        };
        let message = match error {
            EngineError::UpdateRejected(reason) => reason,
            other => other.to_string(),
        };
        Failure { kind, message }
    }
}

impl From<Failure> for tonic::Status {
    fn from(failure: Failure) -> tonic::Status {
        let code = match failure.kind {
            Kind::NotFound => tonic::Code::NotFound,
            Kind::Conflict => tonic::Code::AlreadyExists,
            Kind::Invalid => tonic::Code::InvalidArgument,
            Kind::Timeout => tonic::Code::DeadlineExceeded,
            Kind::Rejected => tonic::Code::FailedPrecondition,
            Kind::Unavailable => tonic::Code::Unavailable,
            Kind::Internal => tonic::Code::Internal,
            Kind::Unauthenticated => tonic::Code::Unauthenticated,
            Kind::Forbidden => tonic::Code::PermissionDenied,
        };
        tonic::Status::new(code, failure.message)
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        let status = match self.kind {
            Kind::NotFound => StatusCode::NOT_FOUND,
            Kind::Conflict => StatusCode::CONFLICT,
            Kind::Invalid => StatusCode::BAD_REQUEST,
            Kind::Timeout => StatusCode::GATEWAY_TIMEOUT,
            Kind::Rejected => StatusCode::UNPROCESSABLE_ENTITY,
            Kind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Kind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            Kind::Unauthenticated => StatusCode::UNAUTHORIZED,
            Kind::Forbidden => StatusCode::FORBIDDEN,
        };
        (status, Json(json!({ "error": self.message }))).into_response()
    }
}

/// The calls, shared by both transports. Each runs the engine's blocking call on a blocking task.
#[derive(Clone)]
struct Api {
    engine: Arc<Engine>,
    security: Arc<Security>,
}

impl Api {
    /// Records an action in the audit log, for whoever made this request.
    async fn audit(&self, action: &'static str, target: String, outcome: String) {
        if !self.security.audit.is_enabled() {
            return;
        }
        let who = PRINCIPAL
            .try_with(|principal| principal.name().to_owned())
            .unwrap_or_else(|_| "anyone".to_owned());
        let security = Arc::clone(&self.security);
        // Lines are synced to disk: not on the async threads.
        let _ = tokio::task::spawn_blocking(move || {
            security.audit.record(&who, action, &target, &outcome);
        })
        .await;
    }

    /// Records the outcome of an action, and passes it on.
    async fn audited<T>(
        &self,
        action: &'static str,
        target: String,
        result: Result<T, Failure>,
    ) -> Result<T, Failure> {
        let outcome = match &result {
            Ok(_) => "ok".to_owned(),
            Err(failure) => format!("failed: {}", failure.message),
        };
        self.audit(action, target, outcome).await;
        result
    }

    async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Engine) -> Result<T, EngineError> + Send + 'static,
    ) -> Result<T, Failure> {
        let engine = Arc::clone(&self.engine);
        tokio::task::spawn_blocking(move || f(&engine))
            .await
            .map_err(|e| Failure {
                kind: Kind::Internal,
                message: e.to_string(),
            })?
            .map_err(Failure::from)
    }

    async fn start(&self, start: v1::StartRequest) -> Result<v1::StartResponse, Failure> {
        let target = start.key.clone();
        let result = self.do_start(start).await;
        self.audited("start", target, result).await
    }

    async fn do_start(&self, start: v1::StartRequest) -> Result<v1::StartResponse, Failure> {
        let input = json_payload("the input", start.input)?;
        let reuse = match v1::ReusePolicy::try_from(start.reuse) {
            Ok(v1::ReusePolicy::ReturnExisting) => ReusePolicy::ReturnExisting,
            Ok(v1::ReusePolicy::Reject) => ReusePolicy::Reject,
            Ok(v1::ReusePolicy::AllowAfterCompletion) => ReusePolicy::AllowAfterCompletion,
            Err(_) => return Err(Failure::invalid("unknown reuse policy")),
        };
        let options = StartOptions {
            reuse,
            budget_tokens: (start.budget_tokens > 0).then_some(start.budget_tokens),
            version: (!start.version.is_empty()).then_some(start.version),
        };
        let key = start.key;
        self.call(move |engine| {
            engine.start_with(&key, &start.workflow, input, options)?;
            engine.describe(&key)
        })
        .await
        .map(|(run_id, state)| v1::StartResponse {
            run_id,
            version: state.version().unwrap_or_default().to_owned(),
        })
    }

    async fn wait(&self, key: String, timeout_ms: u64) -> Result<v1::RunResult, Failure> {
        let timeout = timeout(timeout_ms);
        let result = self.call(move |engine| engine.wait(&key, timeout)).await?;
        Ok(v1::RunResult {
            status: status(result.status) as i32,
            output: result.output.map(|p| p.0).unwrap_or_default(),
            error: result.error.unwrap_or_default(),
            version: result.version.unwrap_or_default(),
        })
    }

    async fn describe(&self, key: String) -> Result<v1::RunDescription, Failure> {
        let (run_id, state) = self.call(move |engine| engine.describe(&key)).await?;
        Ok(description(run_id, &state))
    }

    async fn list(&self, filter: v1::ListRunsRequest) -> Result<v1::ListRunsResponse, Failure> {
        let runs = self.call(Engine::runs).await?;
        let limit = if filter.limit == 0 {
            usize::MAX
        } else {
            filter.limit as usize
        };
        let runs = runs
            .into_iter()
            .filter(|run| filter.workflow.is_empty() || run.workflow == filter.workflow)
            .filter(|run| filter.status == 0 || status(run.status) as i32 == filter.status)
            .take(limit)
            .map(|run: RunSummary| v1::RunSummary {
                run_id: run.run_id,
                workflow: run.workflow,
                status: status(run.status) as i32,
                entries: run.entries as u64,
            })
            .collect();
        Ok(v1::ListRunsResponse { runs })
    }

    async fn signal(&self, signal: v1::SignalRequest) -> Result<v1::Ack, Failure> {
        let target = format!("{} {}", signal.key, signal.name);
        let result = self.do_signal(signal).await;
        self.audited("signal", target, result).await
    }

    async fn do_signal(&self, signal: v1::SignalRequest) -> Result<v1::Ack, Failure> {
        let payload = json_payload("the payload", signal.payload)?;
        self.call(move |engine| engine.signal(&signal.key, &signal.name, &signal.id, payload))
            .await?;
        Ok(v1::Ack {})
    }

    async fn complete_awakeable(&self, id: String, payload: Vec<u8>) -> Result<v1::Ack, Failure> {
        let target = id.clone();
        let result = self.do_complete_awakeable(id, payload).await;
        self.audited("complete awakeable", target, result).await
    }

    async fn do_complete_awakeable(
        &self,
        id: String,
        payload: Vec<u8>,
    ) -> Result<v1::Ack, Failure> {
        let payload = json_payload("the payload", payload)?;
        self.call(move |engine| engine.complete_awakeable(&id, payload))
            .await?;
        Ok(v1::Ack {})
    }

    async fn cancel(&self, key: String, reason: String) -> Result<v1::Ack, Failure> {
        let target = key.clone();
        let result = self.do_cancel(key, reason).await;
        self.audited("cancel", target, result).await
    }

    async fn do_cancel(&self, key: String, reason: String) -> Result<v1::Ack, Failure> {
        let reason = if reason.is_empty() {
            "cancelled through the ingress".to_owned()
        } else {
            reason
        };
        self.call(move |engine| engine.cancel(&key, &reason))
            .await?;
        Ok(v1::Ack {})
    }

    async fn query(&self, key: String, name: String) -> Result<v1::QueryResponse, Failure> {
        let value = self.call(move |engine| engine.query(&key, &name)).await?;
        Ok(v1::QueryResponse { value: value.0 })
    }

    async fn update(&self, update: v1::UpdateRequest) -> Result<v1::UpdateResponse, Failure> {
        let target = format!("{} {}", update.key, update.name);
        let result = self.do_update(update).await;
        if let Ok(v1::UpdateResponse {
            answer: Some(v1::update_response::Answer::Rejected(why)),
        }) = &result
        {
            self.audit("update", target, format!("rejected: {why}"))
                .await;
            return result;
        }
        self.audited("update", target, result).await
    }

    async fn do_update(&self, update: v1::UpdateRequest) -> Result<v1::UpdateResponse, Failure> {
        let payload = json_payload("the payload", update.payload)?;
        let timeout = timeout(update.timeout_ms);
        let answer = self
            .call(move |engine| {
                engine.update(&update.key, &update.name, &update.id, payload, timeout)
            })
            .await;
        let answer = match answer {
            Ok(result) => v1::update_response::Answer::Result(result.0),
            Err(Failure {
                kind: Kind::Rejected,
                message,
            }) => v1::update_response::Answer::Rejected(message),
            Err(other) => return Err(other),
        };
        Ok(v1::UpdateResponse {
            answer: Some(answer),
        })
    }

    async fn escalations(&self) -> Result<v1::ListEscalationsResponse, Failure> {
        let open = self.call(Engine::escalations).await?;
        Ok(v1::ListEscalationsResponse {
            escalations: open
                .into_iter()
                .map(|e| v1::Escalation {
                    id: e.id,
                    key: e.key,
                    reason: e.reason,
                    payload: e.payload.0,
                })
                .collect(),
        })
    }

    async fn resolve(&self, id: String, outcome: Vec<u8>) -> Result<v1::Ack, Failure> {
        let target = id.clone();
        let result = self.do_resolve(id, outcome).await;
        self.audited("resolve", target, result).await
    }

    async fn do_resolve(&self, id: String, outcome: Vec<u8>) -> Result<v1::Ack, Failure> {
        let outcome = json_payload("the outcome", outcome)?;
        self.call(move |engine| engine.resolve_escalation(&id, outcome))
            .await?;
        Ok(v1::Ack {})
    }
}

/// A payload, which must be JSON (the default codec, ADR 0010).
fn json_payload(what: &str, bytes: Vec<u8>) -> Result<Payload, Failure> {
    serde_json::from_slice::<serde::de::IgnoredAny>(&bytes)
        .map_err(|e| Failure::invalid(format!("{what} is not JSON: {e}")))?;
    Ok(Payload::new(bytes))
}

fn timeout(ms: u64) -> Duration {
    if ms == 0 {
        DEFAULT_TIMEOUT
    } else {
        Duration::from_millis(ms).min(MAX_TIMEOUT)
    }
}

fn status(status: Status) -> v1::Status {
    match status {
        Status::Running => v1::Status::Running,
        Status::Completed => v1::Status::Completed,
        Status::Failed => v1::Status::Failed,
        Status::Parked => v1::Status::Parked,
        Status::Continued => v1::Status::Continued,
    }
}

fn description(run_id: String, state: &RunState) -> v1::RunDescription {
    let commands = state
        .steps()
        .iter()
        .zip(0u32..)
        .map(|(step, index)| {
            let kind = match step.kind {
                CommandKind::Step => "step",
                CommandKind::Agent { .. } => "agent",
                CommandKind::Sleep { .. } => "sleep",
                CommandKind::Signal { .. } => "signal",
                CommandKind::Update { .. } => "update",
                CommandKind::Recorded => "record",
                CommandKind::Escalation => "escalation",
                CommandKind::Child => "child",
                CommandKind::Response { .. } => "response",
            };
            v1::Command {
                index,
                kind: kind.to_owned(),
                name: step.name.clone(),
                input: step.input.0.clone(),
                outcome: step.outcome.as_ref().map(|outcome| match outcome {
                    StepOutcome::Completed(value) => {
                        v1::command::Outcome::Completed(value.0.clone())
                    }
                    StepOutcome::Failed(error) => v1::command::Outcome::Failed(error.clone()),
                }),
            }
        })
        .collect();
    v1::RunDescription {
        run_id,
        workflow: state.workflow().to_owned(),
        version: state.version().unwrap_or_default().to_owned(),
        status: status(state.status()) as i32,
        input: state.input().0.clone(),
        output: state.output().map(|p| p.0.clone()).unwrap_or_default(),
        error: state.error().unwrap_or_default().to_owned(),
        commands,
        entries: state.len() as u64,
    }
}

// gRPC.

type Answer<T> = Result<tonic::Response<T>, tonic::Status>;

fn answer<T>(result: Result<T, Failure>) -> Answer<T> {
    result
        .map(tonic::Response::new)
        .map_err(tonic::Status::from)
}

#[tonic::async_trait]
impl v1::ingress_server::Ingress for Api {
    async fn start(&self, request: tonic::Request<v1::StartRequest>) -> Answer<v1::StartResponse> {
        answer(Api::start(self, request.into_inner()).await)
    }

    async fn wait(&self, request: tonic::Request<v1::WaitRequest>) -> Answer<v1::RunResult> {
        let request = request.into_inner();
        answer(Api::wait(self, request.key, request.timeout_ms).await)
    }

    async fn describe(
        &self,
        request: tonic::Request<v1::DescribeRequest>,
    ) -> Answer<v1::RunDescription> {
        answer(Api::describe(self, request.into_inner().key).await)
    }

    async fn list_runs(
        &self,
        request: tonic::Request<v1::ListRunsRequest>,
    ) -> Answer<v1::ListRunsResponse> {
        answer(self.list(request.into_inner()).await)
    }

    async fn signal(&self, request: tonic::Request<v1::SignalRequest>) -> Answer<v1::Ack> {
        answer(Api::signal(self, request.into_inner()).await)
    }

    async fn complete_awakeable(
        &self,
        request: tonic::Request<v1::CompleteAwakeableRequest>,
    ) -> Answer<v1::Ack> {
        let request = request.into_inner();
        answer(Api::complete_awakeable(self, request.id, request.payload).await)
    }

    async fn cancel(&self, request: tonic::Request<v1::CancelRequest>) -> Answer<v1::Ack> {
        let request = request.into_inner();
        answer(Api::cancel(self, request.key, request.reason).await)
    }

    async fn query(&self, request: tonic::Request<v1::QueryRequest>) -> Answer<v1::QueryResponse> {
        let request = request.into_inner();
        answer(Api::query(self, request.key, request.name).await)
    }

    async fn update(
        &self,
        request: tonic::Request<v1::UpdateRequest>,
    ) -> Answer<v1::UpdateResponse> {
        answer(Api::update(self, request.into_inner()).await)
    }

    async fn list_escalations(
        &self,
        _: tonic::Request<v1::ListEscalationsRequest>,
    ) -> Answer<v1::ListEscalationsResponse> {
        answer(self.escalations().await)
    }

    async fn resolve_escalation(
        &self,
        request: tonic::Request<v1::ResolveEscalationRequest>,
    ) -> Answer<v1::Ack> {
        let request = request.into_inner();
        answer(self.resolve(request.id, request.outcome).await)
    }
}

// HTTP/JSON: the same calls, with payloads embedded as JSON values.

/// A payload as JSON: embedded if it is JSON, otherwise a string of its bytes in hex.
fn value(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| Value::String(bytes.iter().map(|b| format!("{b:02x}")).collect()))
}

fn bytes(value: &Value) -> Vec<u8> {
    value.to_string().into_bytes()
}

fn status_name(code: i32) -> &'static str {
    match v1::Status::try_from(code) {
        Ok(v1::Status::Running) => "running",
        Ok(v1::Status::Completed) => "completed",
        Ok(v1::Status::Failed) => "failed",
        Ok(v1::Status::Parked) => "parked",
        Ok(v1::Status::Continued) => "continued",
        _ => "unspecified",
    }
}

fn status_code(name: &str) -> Result<i32, Failure> {
    Ok(match name {
        "running" => v1::Status::Running,
        "completed" => v1::Status::Completed,
        "failed" => v1::Status::Failed,
        "parked" => v1::Status::Parked,
        "continued" => v1::Status::Continued,
        other => return Err(Failure::invalid(format!("unknown status `{other}`"))),
    } as i32)
}

/// A JSON request body, whatever its content type (so `curl -d` works), with errors in the API's form.
struct Body<T>(T);

impl<S: Send + Sync, T: serde::de::DeserializeOwned> axum::extract::FromRequest<S> for Body<T> {
    type Rejection = Failure;

    async fn from_request(request: axum::extract::Request, state: &S) -> Result<Self, Failure> {
        let bytes = axum::body::Bytes::from_request(request, state)
            .await
            .map_err(|e| Failure::invalid(e.to_string()))?;
        let bytes = if bytes.iter().all(u8::is_ascii_whitespace) {
            axum::body::Bytes::from_static(b"{}")
        } else {
            bytes
        };
        serde_json::from_slice(&bytes)
            .map(Body)
            .map_err(|e| Failure::invalid(format!("the request body is not valid: {e}")))
    }
}

#[derive(Deserialize)]
struct StartBody {
    key: String,
    workflow: String,
    #[serde(default)]
    input: Value,
    #[serde(default)]
    version: String,
    #[serde(default)]
    budget_tokens: u64,
    #[serde(default)]
    reuse: Option<String>,
}

async fn http_start(
    State(api): State<Api>,
    Body(body): Body<StartBody>,
) -> Result<Json<Value>, Failure> {
    let reuse = match body.reuse.as_deref() {
        None | Some("return_existing") => v1::ReusePolicy::ReturnExisting,
        Some("reject") => v1::ReusePolicy::Reject,
        Some("allow_after_completion") => v1::ReusePolicy::AllowAfterCompletion,
        Some(other) => return Err(Failure::invalid(format!("unknown reuse policy `{other}`"))),
    };
    let started = api
        .start(v1::StartRequest {
            key: body.key,
            workflow: body.workflow,
            input: bytes(&body.input),
            version: body.version,
            budget_tokens: body.budget_tokens,
            reuse: reuse as i32,
        })
        .await?;
    Ok(Json(
        json!({ "run_id": started.run_id, "version": started.version }),
    ))
}

#[derive(Deserialize)]
struct Timeout {
    #[serde(default)]
    timeout_ms: u64,
}

async fn http_wait(
    State(api): State<Api>,
    Path(key): Path<String>,
    Query(query): Query<Timeout>,
) -> Result<Json<Value>, Failure> {
    let result = api.wait(key, query.timeout_ms).await?;
    Ok(Json(json!({
        "status": status_name(result.status),
        "output": if result.output.is_empty() { Value::Null } else { value(&result.output) },
        "error": result.error,
        "version": result.version,
    })))
}

async fn http_describe(
    State(api): State<Api>,
    Path(key): Path<String>,
) -> Result<Json<Value>, Failure> {
    let run = api.describe(key).await?;
    let commands: Vec<Value> = run
        .commands
        .iter()
        .map(|command| {
            let mut object = json!({
                "index": command.index,
                "kind": command.kind,
                "name": command.name,
                "input": if command.input.is_empty() { Value::Null } else { value(&command.input) },
            });
            match &command.outcome {
                Some(v1::command::Outcome::Completed(result)) => {
                    object["completed"] = value(result);
                }
                Some(v1::command::Outcome::Failed(error)) => object["failed"] = json!(error),
                None => object["pending"] = json!(true),
            }
            object
        })
        .collect();
    Ok(Json(json!({
        "run_id": run.run_id,
        "workflow": run.workflow,
        "version": run.version,
        "status": status_name(run.status),
        "input": value(&run.input),
        "output": if run.output.is_empty() { Value::Null } else { value(&run.output) },
        "error": run.error,
        "entries": run.entries,
        "commands": commands,
    })))
}

#[derive(Deserialize)]
struct ListFilter {
    #[serde(default)]
    workflow: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    limit: u32,
}

async fn http_list(
    State(api): State<Api>,
    Query(filter): Query<ListFilter>,
) -> Result<Json<Value>, Failure> {
    let status = filter
        .status
        .as_deref()
        .map(status_code)
        .transpose()?
        .unwrap_or(0);
    let runs = api
        .list(v1::ListRunsRequest {
            workflow: filter.workflow,
            status,
            limit: filter.limit,
        })
        .await?;
    let runs: Vec<Value> = runs
        .runs
        .iter()
        .map(|run| {
            json!({
                "run_id": run.run_id,
                "workflow": run.workflow,
                "status": status_name(run.status),
                "entries": run.entries,
            })
        })
        .collect();
    Ok(Json(json!({ "runs": runs })))
}

#[derive(Deserialize)]
struct PayloadBody {
    #[serde(default)]
    payload: Value,
    #[serde(default)]
    id: String,
    #[serde(default)]
    timeout_ms: u64,
}

async fn http_signal(
    State(api): State<Api>,
    Path((key, name)): Path<(String, String)>,
    Body(body): Body<PayloadBody>,
) -> Result<Json<Value>, Failure> {
    api.signal(v1::SignalRequest {
        key,
        name,
        id: body.id,
        payload: bytes(&body.payload),
    })
    .await?;
    Ok(Json(json!({})))
}

async fn http_complete_awakeable(
    State(api): State<Api>,
    Path(id): Path<String>,
    Body(body): Body<PayloadBody>,
) -> Result<Json<Value>, Failure> {
    api.complete_awakeable(id, bytes(&body.payload)).await?;
    Ok(Json(json!({})))
}

#[derive(Deserialize, Default)]
struct CancelBody {
    #[serde(default)]
    reason: String,
}

async fn http_cancel(
    State(api): State<Api>,
    Path(key): Path<String>,
    Body(body): Body<CancelBody>,
) -> Result<Json<Value>, Failure> {
    let reason = body.reason;
    api.cancel(key, reason).await?;
    Ok(Json(json!({})))
}

async fn http_query(
    State(api): State<Api>,
    Path((key, name)): Path<(String, String)>,
) -> Result<Json<Value>, Failure> {
    let answer = api.query(key, name).await?;
    Ok(Json(json!({ "value": value(&answer.value) })))
}

async fn http_update(
    State(api): State<Api>,
    Path((key, name)): Path<(String, String)>,
    Body(body): Body<PayloadBody>,
) -> Result<Json<Value>, Failure> {
    let answer = api
        .update(v1::UpdateRequest {
            key,
            name,
            id: body.id,
            payload: bytes(&body.payload),
            timeout_ms: body.timeout_ms,
        })
        .await?;
    match answer.answer {
        Some(v1::update_response::Answer::Result(result)) => {
            Ok(Json(json!({ "result": value(&result) })))
        }
        Some(v1::update_response::Answer::Rejected(message)) => Err(Failure {
            kind: Kind::Rejected,
            message,
        }),
        None => Err(Failure {
            kind: Kind::Internal,
            message: "no answer".into(),
        }),
    }
}

async fn http_escalations(State(api): State<Api>) -> Result<Json<Value>, Failure> {
    let open = api.escalations().await?;
    let escalations: Vec<Value> = open
        .escalations
        .iter()
        .map(
            |e| json!({"id": e.id, "key": e.key, "reason": e.reason, "payload": value(&e.payload)}),
        )
        .collect();
    Ok(Json(json!({ "escalations": escalations })))
}

#[derive(Deserialize)]
struct OutcomeBody {
    #[serde(default)]
    outcome: Value,
}

async fn http_resolve(
    State(api): State<Api>,
    Path(id): Path<String>,
    Body(body): Body<OutcomeBody>,
) -> Result<Json<Value>, Failure> {
    api.resolve(id, bytes(&body.outcome)).await?;
    Ok(Json(json!({})))
}
