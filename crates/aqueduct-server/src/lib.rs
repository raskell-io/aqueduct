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
mod deployments;
mod imports;
mod limits;
mod push;
mod tls;
mod workers;
mod workflows;

pub use audit::Audit;
pub use auth::{Authenticator, Permission, Principal, Refusal};
pub use limits::Limits;
pub use push::{Config, PushEndpoint};
pub use tls::Tls;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use aqueduct_core::{CommandKind, Payload, RunState, Status, StepOutcome};
use aqueduct_engine::embedded::{Engine, EngineError, RunSummary};
use aqueduct_engine::namespaces::{self, Scope};
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
    let connections = tls::Connections {
        places: Arc::new(tokio::sync::Semaphore::new(config.limits.max_connections)),
        header_timeout: Duration::from_millis(config.limits.header_timeout_ms),
    };
    let security = security.with_limits(config.limits.clone());
    tls::serve(
        router_secured(engine, security),
        listener,
        None,
        connections,
        shutdown,
    )
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
    let connections = tls::Connections {
        places: Arc::new(tokio::sync::Semaphore::new(config.limits.max_connections)),
        header_timeout: Duration::from_millis(config.limits.header_timeout_ms),
    };
    let security = security.with_limits(config.limits.clone());
    tls::serve(
        router_secured(engine, security),
        listener,
        Some(tls),
        connections,
        shutdown,
    )
    .await
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
        .add_service(v1::deployments_server::DeploymentsServer::new(api.clone()))
        .add_service(
            v1::imports_server::ImportsServer::new(api.clone())
                .max_decoding_message_size(imports::MOST_IN_A_MESSAGE),
        )
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
        .route("/v1/objects/{object}/{key}", get(http_object_state))
        .route(
            "/v1/objects/{object}/{key}/{handler}",
            post(http_call_object),
        )
        .with_state(api.clone());
    grpc.merge(http)
        .merge(workers::routes(api.clone()))
        .merge(deployments::routes(api.clone()))
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
    RateLimited,
}

/// The server's authentication and audit log (ADR 0041).
#[derive(Debug)]
pub struct Security {
    /// Who may do what.
    pub auth: Authenticator,
    /// What was done.
    pub audit: Audit,
    limiter: limits::Limiter,
}

impl Security {
    /// By a data directory's tokens file, if it has one, and its audit log.
    pub fn for_dir(dir: &std::path::Path) -> Result<Security, String> {
        Ok(Security {
            auth: Authenticator::for_dir(dir),
            audit: Audit::open(dir)?,
            limiter: limits::Limiter::new(Limits::default()),
        })
    }

    /// No authentication, and no audit log.
    pub fn none() -> Security {
        Security {
            auth: Authenticator::none(),
            audit: Audit::none(),
            limiter: limits::Limiter::new(Limits::none()),
        }
    }

    /// Applies these limits.
    pub fn with_limits(mut self, limits: Limits) -> Security {
        self.limiter = limits::Limiter::new(limits);
        self
    }
}

tokio::task_local! {
    /// Who made the request being handled.
    static PRINCIPAL: Principal;
    /// What the request being handled may reach.
    static CALLER: Caller;
}

/// What a request may reach (ADR 0046).
#[derive(Clone, Debug)]
struct Caller {
    /// The namespace its keys are in.
    namespace: String,
    /// The namespaces it serves, if it is a worker.
    workers: Scope,
}

/// The namespace of the request being handled.
fn namespace() -> String {
    CALLER
        .try_with(|caller| caller.namespace.clone())
        .unwrap_or_else(|_| namespaces::DEFAULT.to_owned())
}

/// The namespaces the worker making this request serves.
pub(crate) fn worker_scope() -> Scope {
    CALLER
        .try_with(|caller| caller.workers.clone())
        .unwrap_or(Scope::All)
}

/// What a request may reach, from its token's namespace and the header naming one. A token bound to a
/// namespace cannot name another; only a `*` token, or a server without tokens, can choose.
fn caller(token: Option<&str>, header: Option<&str>) -> Result<Caller, String> {
    if let Some(name) = header
        && !namespaces::valid_namespace(name)
    {
        return Err(format!("`{name}` is not a namespace"));
    }
    match (token, header) {
        (None | Some("*"), Some(name)) => Ok(Caller {
            namespace: name.to_owned(),
            workers: Scope::One(name.to_owned()),
        }),
        (None | Some("*"), None) => Ok(Caller {
            namespace: namespaces::DEFAULT.to_owned(),
            workers: Scope::All,
        }),
        (Some(bound), Some(name)) if bound != name => Err(format!(
            "this token works in namespace `{bound}`, not `{name}`"
        )),
        (Some(bound), _) => Ok(Caller {
            namespace: bound.to_owned(),
            workers: Scope::One(bound.to_owned()),
        }),
    }
}

/// A key from a client, as stored: in its namespace, whatever it says. A key that names another
/// namespace, or an internal run outside this one, is answered as what it is to this caller: no run.
fn run_key(key: &str) -> Result<String, Failure> {
    let namespace = namespace();
    let internal = key.starts_with("$ns/") || aqueduct_engine::objects::is_reserved_key(key);
    if internal && namespaces::namespace_of(key) == namespace {
        return Ok(key.to_owned());
    }
    if internal || namespaces::names_namespace(key) {
        return Err(Failure {
            kind: Kind::NotFound,
            message: format!("no run `{key}`"),
        });
    }
    Ok(namespaces::scoped(&namespace, key))
}

/// A part of an object call's address from a client: it must not name a namespace.
fn object_part<'a>(what: &str, part: &'a str) -> Result<&'a str, Failure> {
    if namespaces::names_namespace(part) {
        return Err(Failure::invalid(format!(
            "this {what} cannot be used: `{part}`"
        )));
    }
    Ok(part)
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
            "CallObject" => (Permission::Signal, "call object"),
            "Wait" | "Describe" | "ListRuns" | "Query" | "ListEscalations" | "GetObjectState" => {
                (Permission::Read, "read")
            }
            _ => (Permission::Admin, "unknown call"),
        };
    }
    if path.starts_with("/aqueduct.v1.Deployments/") || path.starts_with("/v1/deployments") {
        return (Permission::Admin, "deployment");
    }
    if path == "/aqueduct.v1.Imports/Upload" {
        return (Permission::Admin, "import");
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
        (&Method::POST, ["v1", "objects", _, _, _]) => (Permission::Signal, "call object"),
        (&Method::GET, ["v1", "objects", _, _]) => (Permission::Read, "read"),
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
    let path = request.uri().path().to_owned();
    let grpc = request
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/grpc"));
    let (principal, token_namespace) = match api.security.auth.check_in(header, needed) {
        Ok(checked) => checked,
        // A caller without a valid token is `unauthenticated`, whatever it claimed.
        Err(Refusal::Unauthenticated(message)) => {
            refused(&api, "unauthenticated", action, path, &message).await;
            let kind = Kind::Unauthenticated;
            return refuse(request, respond(Failure { kind, message }, grpc, None)).await;
        }
        Err(Refusal::Forbidden(who, message)) => {
            refused(&api, &who, action, path, &message).await;
            let kind = Kind::Forbidden;
            return refuse(request, respond(Failure { kind, message }, grpc, None)).await;
        }
    };
    // What it may reach: a token bound to a namespace cannot name another.
    let named = request
        .headers()
        .get("aqueduct-namespace")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let context = match caller(token_namespace.as_deref(), named.as_deref()) {
        Ok(context) => context,
        Err(message) => {
            refused(&api, principal.name(), action, path, &message).await;
            let kind = Kind::Forbidden;
            return refuse(request, respond(Failure { kind, message }, grpc, None)).await;
        }
    };
    let limiter = &api.security.limiter;
    if needed != Permission::Work
        && let Err(wait) = limiter.admit_namespace(&context.namespace)
    {
        let message = format!("namespace `{}` is over its rate limit", context.namespace);
        refused(&api, principal.name(), action, path, &message).await;
        let kind = Kind::RateLimited;
        return refuse(
            request,
            respond(Failure { kind, message }, grpc, Some(wait)),
        )
        .await;
    }
    // Workers are limited by their capacity: a rate limit would only delay finished work.
    if needed != Permission::Work
        && let Err(wait) = limiter.admit(principal.name())
    {
        let message = format!("`{}` is over its rate limit", principal.name());
        refused(&api, principal.name(), action, path, &message).await;
        let kind = Kind::RateLimited;
        return refuse(
            request,
            respond(Failure { kind, message }, grpc, Some(wait)),
        )
        .await;
    }
    // Held until the answer: a stream's permit goes back when its headers are sent.
    let Ok(_permit) = Arc::clone(&limiter.requests).try_acquire_owned() else {
        let message = "the server is at its limit of concurrent requests".to_owned();
        refused(&api, principal.name(), action, path, &message).await;
        let kind = Kind::Unavailable;
        return refuse(
            request,
            respond(Failure { kind, message }, grpc, Some(RETRY_SOON)),
        )
        .await;
    };
    let scope = CALLER.scope(context, PRINCIPAL.scope(principal, next.run(request)));
    scope.await
}

/// Answers a request it refused. An HTTP/2 answer sent before the request's body has been read must reset
/// the stream, and a client still sending fails that request with a transport error rather than this
/// answer, however retryable the answer was; h2 also closes a connection after enough resets, which fails
/// every other request on it. So a small body is read first, without waiting long for one that never ends.
async fn refuse(request: axum::extract::Request, response: Response) -> Response {
    const MOST: usize = 64 * 1024;
    let body = axum::body::to_bytes(request.into_body(), MOST);
    let _ = tokio::time::timeout(Duration::from_millis(20), body).await;
    response
}

/// The retry hint when nothing says how long the wait will be.
const RETRY_SOON: Duration = Duration::from_secs(1);

/// Records a refused request, unless too many are arriving to record them all.
async fn refused(api: &Api, who: &str, action: &'static str, path: String, message: &str) {
    let Some(unrecorded) = api.security.limiter.record_refusal() else {
        return;
    };
    let outcome = if unrecorded == 0 {
        format!("refused: {message}")
    } else {
        format!("refused: {message} ({unrecorded} earlier refusals were not recorded)")
    };
    PRINCIPAL
        .scope(
            Principal::Token(who.to_owned()),
            api.audit(action, path, outcome),
        )
        .await;
}

/// A refusal in its transport's form, with a retry hint if there is one.
fn respond(failure: Failure, grpc: bool, retry: Option<Duration>) -> Response {
    let seconds = retry.map(|retry| retry.as_secs_f64().ceil().max(1.0) as u64);
    let mut response = if grpc {
        tonic::Status::from(failure).into_http()
    } else {
        failure.into_response()
    };
    if let Some(seconds) = seconds {
        response
            .headers_mut()
            .insert(axum::http::header::RETRY_AFTER, seconds.into());
    }
    response
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
            | EngineError::Deployment(_)
            | EngineError::InvalidResolution(_)
            | EngineError::HandlerFailed(_)
            | EngineError::Unsupported(_) => Kind::Rejected,
            EngineError::InvalidPayload(_) => Kind::Invalid,
            EngineError::Stopped(_) | EngineError::Overloaded(_) => Kind::Unavailable,
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
            Kind::RateLimited => tonic::Code::ResourceExhausted,
        };
        tonic::Status::new(code, failure.message)
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        let retry = self.kind == Kind::Unavailable;
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
            Kind::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        };
        let mut response = (status, Json(json!({ "error": self.message }))).into_response();
        if retry {
            response
                .headers_mut()
                .insert(axum::http::header::RETRY_AFTER, 1.into());
        }
        response
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
        // The log says which namespace each action was in.
        let target = match namespace().as_str() {
            namespaces::DEFAULT => target,
            namespace => format!("{namespace}: {target}"),
        };
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
        let key = run_key(&start.key)?;
        let namespace = namespace();
        self.call(move |engine| {
            engine.start_with(&key, &start.workflow, input, options)?;
            engine.describe(&key)
        })
        .await
        .map(|(run_id, state)| v1::StartResponse {
            run_id: namespaces::unscoped(&namespace, &run_id).to_owned(),
            version: state.version().unwrap_or_default().to_owned(),
        })
    }

    async fn wait(&self, key: String, timeout_ms: u64) -> Result<v1::RunResult, Failure> {
        let timeout = timeout(timeout_ms);
        let key = run_key(&key)?;
        let result = self.call(move |engine| engine.wait(&key, timeout)).await?;
        Ok(v1::RunResult {
            status: status(result.status) as i32,
            output: result.output.map(|p| p.0).unwrap_or_default(),
            error: result.error.unwrap_or_default(),
            version: result.version.unwrap_or_default(),
        })
    }

    async fn describe(&self, key: String) -> Result<v1::RunDescription, Failure> {
        let key = run_key(&key)?;
        let namespace = namespace();
        let (run_id, state) = self.call(move |engine| engine.describe(&key)).await?;
        Ok(description(
            namespaces::unscoped(&namespace, &run_id).to_owned(),
            &state,
        ))
    }

    async fn list(&self, filter: v1::ListRunsRequest) -> Result<v1::ListRunsResponse, Failure> {
        let namespace = namespace();
        let runs = self.call(Engine::runs).await?;
        let limit = if filter.limit == 0 {
            usize::MAX
        } else {
            filter.limit as usize
        };
        let runs = runs
            .into_iter()
            .filter(|run| namespaces::namespace_of(&run.run_id) == namespace)
            .filter(|run| filter.workflow.is_empty() || run.workflow == filter.workflow)
            .filter(|run| filter.status == 0 || status(run.status) as i32 == filter.status)
            .take(limit)
            .map(|run: RunSummary| v1::RunSummary {
                run_id: namespaces::unscoped(&namespace, &run.run_id).to_owned(),
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
        let key = run_key(&signal.key)?;
        self.call(move |engine| engine.signal(&key, &signal.name, &signal.id, payload))
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
        let id = run_key(&id)?;
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
        let key = run_key(&key)?;
        self.call(move |engine| engine.cancel(&key, &reason))
            .await?;
        Ok(v1::Ack {})
    }

    async fn query(&self, key: String, name: String) -> Result<v1::QueryResponse, Failure> {
        let key = run_key(&key)?;
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
        let key = run_key(&update.key)?;
        let answer = self
            .call(move |engine| engine.update(&key, &update.name, &update.id, payload, timeout))
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

    async fn call_object(
        &self,
        call: v1::CallObjectRequest,
    ) -> Result<v1::CallObjectResponse, Failure> {
        let target = format!("{} {} {}", call.object, call.key, call.handler);
        let result = self.do_call_object(call).await;
        self.audited("call object", target, result).await
    }

    async fn do_call_object(
        &self,
        call: v1::CallObjectRequest,
    ) -> Result<v1::CallObjectResponse, Failure> {
        let args = json_payload("the arguments", call.args)?;
        let timeout = timeout(call.timeout_ms);
        object_part("object", &call.object)?;
        object_part("handler", &call.handler)?;
        object_part("id", &call.id)?;
        let key = namespaces::scoped(&namespace(), object_part("key", &call.key)?);
        let output = self
            .call(move |engine| {
                engine.call_object(&call.object, &key, &call.handler, &call.id, &args, timeout)
            })
            .await?;
        Ok(v1::CallObjectResponse { output: output.0 })
    }

    async fn object_state(
        &self,
        object: String,
        key: String,
    ) -> Result<v1::GetObjectStateResponse, Failure> {
        object_part("object", &object)?;
        let key = namespaces::scoped(&namespace(), object_part("key", &key)?);
        let state = self
            .call(move |engine| engine.object_state(&object, &key))
            .await?;
        Ok(v1::GetObjectStateResponse { state: state.0 })
    }

    async fn escalations(&self) -> Result<v1::ListEscalationsResponse, Failure> {
        let namespace = namespace();
        let open = self.call(Engine::escalations).await?;
        Ok(v1::ListEscalationsResponse {
            escalations: open
                .into_iter()
                .filter(|e| namespaces::namespace_of(&e.key) == namespace)
                .map(|e| v1::Escalation {
                    id: namespaces::unscoped(&namespace, &e.id).to_owned(),
                    key: namespaces::unscoped(&namespace, &e.key).to_owned(),
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
        let id = run_key(&id)?;
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
                CommandKind::Sent => "send",
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

    async fn call_object(
        &self,
        request: tonic::Request<v1::CallObjectRequest>,
    ) -> Answer<v1::CallObjectResponse> {
        answer(Api::call_object(self, request.into_inner()).await)
    }

    async fn get_object_state(
        &self,
        request: tonic::Request<v1::GetObjectStateRequest>,
    ) -> Answer<v1::GetObjectStateResponse> {
        let request = request.into_inner();
        answer(self.object_state(request.object, request.key).await)
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

#[derive(Deserialize)]
struct ObjectCallBody {
    #[serde(default)]
    args: Value,
    #[serde(default)]
    id: String,
    #[serde(default)]
    timeout_ms: u64,
}

async fn http_call_object(
    State(api): State<Api>,
    Path((object, key, handler)): Path<(String, String, String)>,
    Body(body): Body<ObjectCallBody>,
) -> Result<Json<Value>, Failure> {
    let answer = api
        .call_object(v1::CallObjectRequest {
            object,
            key,
            handler,
            id: body.id,
            args: bytes(&body.args),
            timeout_ms: body.timeout_ms,
        })
        .await?;
    Ok(Json(json!({ "output": value(&answer.output) })))
}

async fn http_object_state(
    State(api): State<Api>,
    Path((object, key)): Path<(String, String)>,
) -> Result<Json<Value>, Failure> {
    let answer = api.object_state(object, key).await?;
    Ok(Json(json!({ "state": value(&answer.state) })))
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
