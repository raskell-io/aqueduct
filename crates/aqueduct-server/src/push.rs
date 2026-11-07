//! Named task queues and push endpoints (ADR 0040): the server's configuration of where remote steps
//! go, and the pushers that deliver a queue's steps to an HTTP endpoint (a serverless function, a web
//! service), one request per invocation.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use aqueduct_engine::embedded::{Attempt, Builder};
use aqueduct_engine::namespaces::Scope;
use aqueduct_engine::queue::{Invocation, StepQueue};
use axum::body::Bytes;
use http_body_util::{BodyExt, Full};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::Limits;
use crate::workers::record;

/// The server's configuration of remote steps: named task queues, and push endpoints.
///
/// ```json
/// {
///   "queues": {"billing": ["charge", "refund"]},
///   "push": [{"queue": "billing", "url": "https://billing.example/aqueduct", "concurrency": 8,
///             "timeout_ms": 30000, "headers": {"authorization": "Bearer ..."}}]
/// }
/// ```
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Named task queues and the steps routed to each. A step in no queue waits in the queue of its
    /// own name.
    #[serde(default)]
    pub queues: BTreeMap<String, Vec<String>>,
    /// Endpoints that queues' steps are pushed to.
    #[serde(default)]
    pub push: Vec<PushEndpoint>,
    /// Virtual objects (ADR 0043). Their handlers are workflows named `<object>.<handler>`: approved
    /// registry versions, or remote workflows.
    #[serde(default)]
    pub objects: Vec<String>,
    /// Workflows that workflow workers run (ADR 0039).
    #[serde(default)]
    pub remote_workflows: Vec<String>,
    /// What the server accepts under load (ADR 0045).
    #[serde(default)]
    pub limits: Limits,
    /// Cluster mode (ADR 0056): this server is one replica of a Raft group of servers.
    #[serde(default)]
    pub cluster: Option<crate::cluster::Cluster>,
    /// How many partitions the data directory holds (ADR 0057). Default 1. Fixed once runs exist: a
    /// directory laid out for another number is refused.
    #[serde(default)]
    pub partitions: Option<u32>,
}

impl Config {
    /// How many partitions to run.
    pub fn partition_count(&self) -> u32 {
        self.partitions.unwrap_or(1)
    }
}

/// An HTTP endpoint a queue's steps are pushed to.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PushEndpoint {
    /// The queue whose steps it receives (a named queue, or a step's own name).
    pub queue: String,
    /// Where to `POST` each invocation: `http://` or `https://`.
    pub url: String,
    /// How many invocations it is sent at once. Default 8.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// How long one request may take before it counts as a retryable failure, in milliseconds.
    /// Default 30,000; keep it below the lease (30 s in `aqueduct server`).
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
    /// Headers sent with every request (for example `authorization`).
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// The namespace whose steps it receives, or `*` for all (ADR 0046). Default: the default namespace,
    /// so that no tenant's payloads reach an endpoint that did not ask for them.
    #[serde(default)]
    pub namespace: Option<String>,
}

fn default_concurrency() -> usize {
    8
}

fn default_timeout() -> u64 {
    30_000
}

impl Config {
    /// Reads a configuration file (JSON).
    pub fn read(path: &std::path::Path) -> Result<Config, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let config: Config =
            serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        config.check()?;
        Ok(config)
    }

    /// Checks that no step is in two queues, and that every push endpoint has a usable URL.
    pub fn check(&self) -> Result<(), String> {
        self.routes()?;
        self.limits.check()?;
        if let Some(cluster) = &self.cluster {
            cluster.check()?;
        }
        let partitions = self.partition_count();
        if partitions == 0 || partitions > u32::from(aqueduct_engine::placement::SLOTS) {
            return Err(format!(
                "partitions must be between 1 and {}",
                aqueduct_engine::placement::SLOTS
            ));
        }

        for endpoint in &self.push {
            if !(endpoint.url.starts_with("http://") || endpoint.url.starts_with("https://")) {
                return Err(format!(
                    "push endpoint for `{}`: the URL must start with http:// or https://, not `{}`",
                    endpoint.queue, endpoint.url
                ));
            }
            if endpoint.concurrency == 0 {
                return Err(format!(
                    "push endpoint for `{}`: concurrency must be at least 1",
                    endpoint.queue
                ));
            }
        }
        Ok(())
    }

    /// The routes: the queue of each step that is in a named one.
    pub fn routes(&self) -> Result<BTreeMap<String, String>, String> {
        let mut routes = BTreeMap::new();
        for (queue, steps) in &self.queues {
            for step in steps {
                if let Some(other) = routes.insert(step.clone(), queue.clone()) {
                    return Err(format!(
                        "step `{step}` is in two queues: `{other}` and `{queue}`"
                    ));
                }
            }
        }
        Ok(routes)
    }

    /// Routes the steps on an engine builder.
    pub fn apply(&self, mut builder: Builder) -> Result<Builder, String> {
        for (step, queue) in self.routes()? {
            builder = builder.step_route(&step, &queue);
        }
        for workflow in &self.remote_workflows {
            builder = builder.remote_workflow(workflow);
        }
        for object in &self.objects {
            builder = builder.object(object);
        }
        for (name, namespace) in &self.limits.namespaces {
            builder = builder.namespace(name, namespace.max_running_runs, namespace.weight);
        }
        if let Some(limit) = self.limits.max_running_runs {
            builder = builder.max_running_runs(limit);
        }
        Ok(builder)
    }
}

pub(crate) type Client = hyper_util::client::legacy::Client<
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    Full<Bytes>,
>;

/// An HTTP(S) client: rustls on ring, with the operating system's root certificates (ADR 0038).
pub(crate) fn client() -> Result<Client, String> {
    use hyper_rustls::ConfigBuilderExt;
    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| e.to_string())?
    .with_native_roots()
    .map_err(|e| e.to_string())?
    .with_no_client_auth();
    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls)
        .https_or_http()
        .enable_http1()
        .build();
    Ok(
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build(connector),
    )
}

/// Starts a pusher for every configured endpoint. A pusher is a worker for its queue: it takes
/// invocations within its concurrency, posts each, and records the outcome.
pub(crate) fn start(
    engine: &Arc<aqueduct_engine::shards::Shards>,
    config: &Config,
) -> Result<(), String> {
    if config.push.is_empty() {
        return Ok(());
    }
    let queue = engine
        .step_queue()
        .cloned()
        .ok_or("push endpoints need an engine with remote steps")?;
    let client = client()?;
    for endpoint in config.push.clone() {
        tokio::spawn(pusher(
            Arc::clone(engine),
            Arc::clone(&queue),
            client.clone(),
            endpoint,
        ));
    }
    Ok(())
}

async fn pusher(
    engine: Arc<aqueduct_engine::shards::Shards>,
    queue: Arc<StepQueue>,
    client: Client,
    endpoint: PushEndpoint,
) {
    let name = format!("push:{}", endpoint.url);
    let scope = Arc::new(match endpoint.namespace.as_deref() {
        Some("*") => Scope::All,
        Some(namespace) => Scope::One(namespace.to_owned()),
        None => Scope::One(aqueduct_engine::namespaces::DEFAULT.to_owned()),
    });
    let queues = Arc::new(vec![endpoint.queue.clone()]);
    let endpoint = Arc::new(endpoint);
    let permits = Arc::new(tokio::sync::Semaphore::new(endpoint.concurrency));
    loop {
        let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
            return;
        };
        let (taking, names, worker, scope) = (
            Arc::clone(&queue),
            Arc::clone(&queues),
            name.clone(),
            Arc::clone(&scope),
        );
        let taken = tokio::task::spawn_blocking(move || {
            taking.wait_take_in(&scope, &worker, &names, 1, Duration::from_millis(500))
        })
        .await
        .unwrap_or_default();
        let Some(invocation) = taken.into_iter().next() else {
            continue;
        };
        let (engine, queue, client, endpoint) = (
            Arc::clone(&engine),
            Arc::clone(&queue),
            client.clone(),
            Arc::clone(&endpoint),
        );
        tokio::spawn(async move {
            let _permit = permit;
            let id = invocation.id.clone();
            let attempt = deliver(&client, &endpoint, invocation).await;
            let _ = record(&engine, &queue, &Scope::All, &id, attempt);
        });
    }
}

/// Posts one invocation; its outcome. Anything but a well-formed answer is a retryable failure.
async fn deliver(client: &Client, endpoint: &PushEndpoint, invocation: Invocation) -> Attempt {
    let body = json!({
        "invocation_id": invocation.id,
        "run_id": invocation.run,
        "command": invocation.cmd.0,
        "step": invocation.step,
        "input": crate::value(invocation.input.as_bytes()),
        "idempotency_key": invocation.idempotency_key,
        "attempt": invocation.attempt,
        "lease_ms": invocation.lease_ms,
    });
    let mut request = axum::http::Request::post(&endpoint.url)
        .header("content-type", "application/json")
        .header("aqueduct-idempotency-key", &invocation.idempotency_key);
    for (name, value) in &endpoint.headers {
        request = request.header(name, value);
    }
    let request = match request.body(Full::new(Bytes::from(body.to_string()))) {
        Ok(request) => request,
        Err(e) => return Attempt::Retry(format!("push endpoint {}: {e}", endpoint.url)),
    };
    let timeout = Duration::from_millis(endpoint.timeout_ms);
    let response = match tokio::time::timeout(timeout, client.request(request)).await {
        Err(_) => {
            return Attempt::Retry(format!(
                "push endpoint {} did not answer within {} ms",
                endpoint.url, endpoint.timeout_ms
            ));
        }
        Ok(Err(e)) => return Attempt::Retry(format!("push endpoint {}: {e}", endpoint.url)),
        Ok(Ok(response)) => response,
    };
    let status = response.status();
    let bytes = match tokio::time::timeout(timeout, response.into_body().collect()).await {
        Ok(Ok(body)) => body.to_bytes(),
        _ => {
            return Attempt::Retry(format!(
                "push endpoint {}: the answer did not arrive",
                endpoint.url
            ));
        }
    };
    if !status.is_success() {
        let text = String::from_utf8_lossy(&bytes);
        return Attempt::Retry(format!(
            "push endpoint {} answered {status}: {}",
            endpoint.url,
            text.chars().take(300).collect::<String>()
        ));
    }
    outcome(&bytes)
        .unwrap_or_else(|why| Attempt::Retry(format!("push endpoint {}: {why}", endpoint.url)))
}

/// The outcome in an endpoint's answer: `{"completed": <any JSON>}`, `{"failed": "..."}` or
/// `{"retry": "..."}`.
fn outcome(bytes: &[u8]) -> Result<Attempt, String> {
    let answer: serde_json::Map<String, Value> = serde_json::from_slice(bytes)
        .map_err(|e| format!("the answer is not a JSON object: {e}"))?;
    let text = |field: &str| answer.get(field).map(|v| v.as_str().map(str::to_owned));
    match (answer.get("completed"), text("failed"), text("retry")) {
        (Some(value), None, None) => Ok(Attempt::Completed(aqueduct_core::Payload::new(
            value.to_string().into_bytes(),
        ))),
        (None, Some(Some(error)), None) => Ok(Attempt::Fail(error)),
        (None, None, Some(Some(error))) => Ok(Attempt::Retry(error)),
        _ => Err("the answer needs exactly one of completed, failed and retry".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_step_in_two_queues_is_refused() {
        let config: Config = serde_json::from_value(json!({
            "queues": {"billing": ["charge"], "payments": ["charge"]},
        }))
        .unwrap_or_default();
        assert!(config.check().unwrap_err().contains("two queues"));
        let config: Config = serde_json::from_value(json!({
            "push": [{"queue": "billing", "url": "ftp://x"}],
        }))
        .unwrap_or_default();
        assert!(config.check().unwrap_err().contains("http://"));
    }

    #[test]
    fn answers_are_read_strictly() {
        assert_eq!(
            outcome(br#"{"completed": null}"#),
            Ok(Attempt::Completed(aqueduct_core::Payload::new(*b"null")))
        );
        assert_eq!(
            outcome(br#"{"failed": "no"}"#),
            Ok(Attempt::Fail("no".into()))
        );
        assert!(outcome(br#"{"completed": 1, "retry": "x"}"#).is_err());
        assert!(outcome(b"oops").is_err());
    }
}
