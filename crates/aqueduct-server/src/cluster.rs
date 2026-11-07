//! Cluster mode (ADR 0056): the server's partition is a replica of a Raft group whose other replicas
//! are other servers. Raft messages travel as `POST /cluster/v1/raft` between them, authenticated by a
//! shared secret; a server that does not lead answers *unavailable* and names the node that does.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use aqueduct_engine::embedded::{Builder, Engine, RaftInbox, Replication, Transport};
use aqueduct_raft::{Envelope, wire};
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use http_body_util::Full;
use serde::Deserialize;

/// The header a forwarded request carries: it is answered where it arrives, never forwarded again.
const FORWARDED_HEADER: &str = "aqueduct-forwarded";

/// The largest request body forwarded to the leader (16 MiB).
const MOST_FORWARDED: usize = 16 << 20;

/// The header that carries the cluster's secret.
const SECRET_HEADER: &str = "aqueduct-cluster-secret";

/// Messages waiting for one peer before new ones are dropped (Raft sends them again).
const QUEUE: usize = 4096;

/// Messages in one request to a peer.
const BATCH: usize = 256;

/// The `"cluster"` section of the server configuration.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Cluster {
    /// This server's node id.
    pub id: u64,
    /// Every node of the group, this one included: its id and base URL (`http://host:port`).
    pub peers: BTreeMap<u64, String>,
    /// A secret every node shares, at least 16 characters: Raft messages without it are refused.
    pub secret: String,
    /// How often Raft's clock ticks, in milliseconds. Default 50: elections after 0.5 to 1 s of silence.
    #[serde(default = "default_tick")]
    pub tick_ms: u64,
}

fn default_tick() -> u64 {
    50
}

impl Cluster {
    /// Checks the section on its own.
    pub fn check(&self) -> Result<(), String> {
        if !self.peers.contains_key(&self.id) {
            return Err(format!("cluster: node {} is not among its peers", self.id));
        }
        if self.peers.len() < 3 || self.peers.len().is_multiple_of(2) {
            return Err("cluster: an odd number of nodes, at least 3".into());
        }
        if self.secret.chars().count() < 16 {
            return Err("cluster: the secret needs at least 16 characters".into());
        }
        if self.tick_ms == 0 {
            return Err("cluster: tick_ms must be positive".into());
        }
        for url in self.peers.values() {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err(format!("cluster: a peer's URL must be http(s): {url}"));
            }
        }
        Ok(())
    }

    /// Makes the engine a replica of the group, with a transport to the other nodes. Must be called
    /// inside a tokio runtime: each peer gets a task that ships its messages.
    pub fn apply(&self, builder: Builder) -> Result<Builder, String> {
        self.check()?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| "cluster mode needs a running tokio runtime".to_owned())?;
        let client = crate::push::client()?;
        let mut queues = BTreeMap::new();
        for (peer, url) in &self.peers {
            if *peer == self.id {
                continue;
            }
            let (sender, receiver) = tokio::sync::mpsc::channel(QUEUE);
            runtime.spawn(ship(
                client.clone(),
                format!("{}/cluster/v1/raft", url.trim_end_matches('/')),
                self.secret.clone(),
                receiver,
            ));
            queues.insert(*peer, sender);
        }
        let transport: Transport = Arc::new(move |envelope: Envelope| {
            if let Some(queue) = queues.get(&envelope.to) {
                // A full queue drops the message, as a network would; Raft sends again.
                let _ = queue.try_send(envelope);
            }
        });
        Ok(builder.replicated(Replication {
            id: self.id,
            members: self.peers.keys().copied().collect(),
            transport,
            tick: Duration::from_millis(self.tick_ms),
            config: aqueduct_raft::Config::default(),
        }))
    }
}

/// Encodes envelopes as one request body: each a little-endian `u32` length and its bytes.
pub fn encode(envelopes: &[Envelope]) -> Vec<u8> {
    let mut body = Vec::new();
    for envelope in envelopes {
        let bytes = wire::encode_envelope(envelope);
        let length = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
        body.extend_from_slice(&length.to_le_bytes());
        body.extend_from_slice(&bytes);
    }
    body
}

/// Decodes a body from [`encode`].
pub fn decode(mut body: &[u8]) -> Result<Vec<Envelope>, String> {
    let mut envelopes = Vec::new();
    while !body.is_empty() {
        let (length, rest) = body
            .split_first_chunk::<4>()
            .ok_or("a body ends inside a length")?;
        let length = u32::from_le_bytes(*length) as usize;
        let bytes = rest.get(..length).ok_or("a body ends inside a message")?;
        envelopes.push(wire::decode_envelope(bytes).map_err(|e| e.to_string())?);
        body = &rest[length..];
    }
    Ok(envelopes)
}

/// Ships one peer's messages, in batches, as they come; a failed request drops its batch.
async fn ship(
    client: crate::push::Client,
    url: String,
    secret: String,
    mut queue: tokio::sync::mpsc::Receiver<Envelope>,
) {
    while let Some(first) = queue.recv().await {
        let mut batch = vec![first];
        while batch.len() < BATCH {
            match queue.try_recv() {
                Ok(envelope) => batch.push(envelope),
                Err(_) => break,
            }
        }
        let Ok(request) = axum::http::Request::post(&url)
            .header("content-type", "application/octet-stream")
            .header(SECRET_HEADER, &secret)
            .body(Full::new(Bytes::from(encode(&batch))))
        else {
            continue;
        };
        let _ = tokio::time::timeout(Duration::from_secs(1), client.request(request)).await;
    }
}

#[derive(Clone)]
struct Peer {
    inbox: RaftInbox,
    id: u64,
    secret: Arc<String>,
}

/// The route other nodes send Raft messages to. Merged outside token authentication: the cluster's
/// secret authenticates it.
pub(crate) fn routes(engine: &Engine, cluster: &Cluster) -> Router {
    Router::new()
        .route("/cluster/v1/raft", post(receive))
        .with_state(Peer {
            inbox: engine.raft_inbox(),
            id: cluster.id,
            secret: Arc::new(cluster.secret.clone()),
        })
}

#[derive(Clone)]
struct Forward {
    engine: Arc<Engine>,
    peers: Arc<BTreeMap<u64, String>>,
    client: crate::push::Client,
}

/// Forwards HTTP/JSON requests (`/v1/...`) that arrive at a node that does not lead to the node that
/// does, and returns its answer: clients may ask any node. Applied outside authentication, so the
/// leader authenticates them; every node needs the same tokens. gRPC calls are not forwarded: a node
/// that does not lead answers `UNAVAILABLE`.
pub(crate) fn forwarding(
    router: Router,
    engine: &Arc<Engine>,
    cluster: &Cluster,
) -> Result<Router, String> {
    let forward = Forward {
        engine: Arc::clone(engine),
        peers: Arc::new(cluster.peers.clone()),
        client: crate::push::client()?,
    };
    Ok(router.layer(axum::middleware::from_fn_with_state(
        forward,
        forward_request,
    )))
}

async fn forward_request(
    State(forward): State<Forward>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let grpc = request
        .headers()
        .get("content-type")
        .is_some_and(|v| v.as_bytes().starts_with(b"application/grpc"));
    let target = forward
        .engine
        .leader()
        .and_then(|leader| forward.peers.get(&leader).cloned());
    let Some(target) = target.filter(|_| {
        !grpc
            && request.uri().path().starts_with("/v1/")
            && !request.headers().contains_key(FORWARDED_HEADER)
            && !forward.engine.leads()
    }) else {
        return next.run(request).await;
    };
    let (parts, body) = request.into_parts();
    let unavailable = |why: String| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({ "error": why })),
        )
            .into_response()
    };
    let Ok(body) = axum::body::to_bytes(body, MOST_FORWARDED).await else {
        return unavailable("the request is too large to forward to the leader".into());
    };
    let path = parts
        .uri
        .path_and_query()
        .map_or("/", axum::http::uri::PathAndQuery::as_str);
    let mut outgoing = axum::http::Request::builder()
        .method(parts.method)
        .uri(format!("{}{path}", target.trim_end_matches('/')));
    for (name, value) in &parts.headers {
        if name != axum::http::header::HOST {
            outgoing = outgoing.header(name, value);
        }
    }
    let Ok(outgoing) = outgoing.header(FORWARDED_HEADER, "1").body(Full::new(body)) else {
        return unavailable("the request could not be forwarded to the leader".into());
    };
    match forward.client.request(outgoing).await {
        Ok(answer) => answer.map(axum::body::Body::new),
        Err(e) => unavailable(format!("the leader at {target} did not answer: {e}")),
    }
}

/// Whether two secrets are equal, in time that does not depend on where they differ.
fn same(a: &[u8], b: &[u8]) -> bool {
    use sha2::{Digest, Sha256};
    let (a, b) = (Sha256::digest(a), Sha256::digest(b));
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

async fn receive(State(peer): State<Peer>, headers: HeaderMap, body: Bytes) -> StatusCode {
    let secret = headers
        .get(SECRET_HEADER)
        .map(|value| value.as_bytes())
        .unwrap_or_default();
    if !same(secret, peer.secret.as_bytes()) {
        return StatusCode::UNAUTHORIZED;
    }
    let Ok(envelopes) = decode(&body) else {
        return StatusCode::BAD_REQUEST;
    };
    for envelope in envelopes {
        if envelope.to == peer.id {
            peer.inbox.deliver(envelope);
        }
    }
    StatusCode::NO_CONTENT
}

#[cfg(test)]
mod tests {
    use super::*;
    use aqueduct_raft::Message;

    #[test]
    fn bodies_round_trip_and_damage_is_refused() {
        let envelopes = vec![
            Envelope {
                from: 1,
                to: 2,
                message: Message::AppendReply {
                    term: 3,
                    success: true,
                    index: 9,
                },
            },
            Envelope {
                from: 1,
                to: 3,
                message: Message::Vote {
                    term: 3,
                    granted: false,
                    pre_vote: true,
                },
            },
        ];
        let body = encode(&envelopes);
        assert_eq!(decode(&body), Ok(envelopes));
        assert!(decode(&body[..body.len() - 1]).is_err());
        assert!(same(b"a secret", b"a secret"));
        assert!(!same(b"a secret", b"a secreT"));
    }

    #[test]
    fn a_cluster_section_is_checked() {
        let cluster = Cluster {
            id: 1,
            peers: (1..=3)
                .map(|n| (n, format!("http://10.0.0.{n}:7470")))
                .collect(),
            secret: "sixteen chars ok".into(),
            tick_ms: 50,
        };
        assert_eq!(cluster.check(), Ok(()));
        let two = Cluster {
            peers: (1..=2).map(|n| (n, format!("http://h{n}"))).collect(),
            ..cluster.clone()
        };
        assert!(two.check().is_err());
        let short = Cluster {
            secret: "short".into(),
            ..cluster.clone()
        };
        assert!(short.check().is_err());
        let elsewhere = Cluster { id: 7, ..cluster };
        assert!(elsewhere.check().is_err());
    }
}
