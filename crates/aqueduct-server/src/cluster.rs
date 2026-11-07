//! Cluster mode (ADR 0056, 0057): each of the server's partitions is a replica of a Raft group whose
//! other replicas are on the other servers. Between servers, authenticated by a shared secret:
//!
//! - Raft messages travel as `POST /cluster/v1/raft`, each tagged with its partition;
//! - messages between partitions (ADR 0052) as `POST /cluster/v1/remote`, to the server that leads the
//!   partition they are for;
//! - an HTTP request that reaches a partition this server does not lead is forwarded to the server that
//!   does.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use aqueduct_engine::Remote;
use aqueduct_engine::embedded::{Builder, EngineError, RemoteRoute, Replication, Transport};
use aqueduct_engine::shards::Shards;
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

/// The header an *unavailable* answer names the leader in, when it knows it.
pub(crate) const LEADER_HEADER: &str = "aqueduct-leader";

/// The largest request body forwarded to the leader (16 MiB).
const MOST_FORWARDED: usize = 16 << 20;

/// The header that carries the cluster's secret.
const SECRET_HEADER: &str = "aqueduct-cluster-secret";

/// Messages waiting for one peer before new ones are dropped (Raft, and senders, repeat them).
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
    /// A secret every node shares, at least 16 characters: messages between servers without it are
    /// refused.
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

    /// How partition `partition` takes part in its Raft group: its transport ships messages to the
    /// other nodes, tagged with the partition. Must be called inside a tokio runtime.
    pub fn replication(&self, partition: u32) -> Result<Replication, String> {
        self.check()?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| "cluster mode needs a running tokio runtime".to_owned())?;
        let client = crate::push::client()?;
        let mut queues = BTreeMap::new();
        for (peer, url) in &self.peers {
            if *peer == self.id {
                continue;
            }
            let (sender, receiver) = tokio::sync::mpsc::channel::<Envelope>(QUEUE);
            let url = format!("{}/cluster/v1/raft", url.trim_end_matches('/'));
            runtime.spawn(ship(
                client.clone(),
                url,
                self.secret.clone(),
                receiver,
                move |batch: &[Envelope]| encode(partition, batch),
            ));
            queues.insert(*peer, sender);
        }
        let transport: Transport = Arc::new(move |envelope: Envelope| {
            if let Some(queue) = queues.get(&envelope.to) {
                // A full queue drops the message, as a network would; Raft sends again.
                let _ = queue.try_send(envelope);
            }
        });
        Ok(Replication {
            id: self.id,
            members: self.peers.keys().copied().collect(),
            transport,
            tick: Duration::from_millis(self.tick_ms),
            config: aqueduct_raft::Config::default(),
        })
    }

    /// Opens the server's partitions, each a replica of its Raft group (ADR 0057). Messages for another
    /// partition go to this server's engine for it when that leads, and to the server that leads it
    /// otherwise. Must be called inside a tokio runtime.
    pub fn open(
        &self,
        dir: &Path,
        partitions: u32,
        mut build: impl FnMut(PathBuf, u32) -> Result<Builder, EngineError>,
    ) -> Result<Arc<Shards>, EngineError> {
        let shards: Arc<OnceLock<Weak<Shards>>> = Arc::default();
        let client = crate::push::client().map_err(EngineError::Open)?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| EngineError::Open("cluster mode needs a running tokio runtime".into()))?;
        let route = |partition: u32| -> Option<RemoteRoute> {
            let (sender, receiver) = tokio::sync::mpsc::channel::<(String, Remote)>(QUEUE);
            runtime.spawn(ship_remote(
                client.clone(),
                self.secret.clone(),
                partition,
                receiver,
            ));
            let shards = Arc::clone(&shards);
            let peers = self.peers.clone();
            Some(Arc::new(move |_to: String, message: Remote| {
                let Some(shards) = shards.get().and_then(Weak::upgrade) else {
                    return;
                };
                let Some(engine) = shards.engines().get(partition as usize) else {
                    return;
                };
                match engine.leader().and_then(|leader| peers.get(&leader)) {
                    Some(url) => {
                        let url = format!("{}/cluster/v1/remote", url.trim_end_matches('/'));
                        let _ = sender.try_send((url, message));
                    }
                    // Here, or no leader known: this server's engine takes it, or drops it if it does
                    // not lead, and the sender repeats it.
                    None => engine.receive(message),
                }
            }))
        };
        let opened = Shards::open_routed(
            dir,
            partitions,
            |path, partition| {
                let replication = self.replication(partition).map_err(EngineError::Open)?;
                Ok(build(path, partition)?.replicated(replication))
            },
            route,
        )?;
        let opened = Arc::new(opened);
        let _ = shards.set(Arc::downgrade(&opened));
        Ok(opened)
    }
}

/// Encodes Raft messages of one partition as one request body: the partition, then each envelope as a
/// little-endian `u32` length and its bytes.
pub fn encode(partition: u32, envelopes: &[Envelope]) -> Vec<u8> {
    let mut body = partition.to_le_bytes().to_vec();
    for envelope in envelopes {
        let bytes = wire::encode_envelope(envelope);
        let length = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
        body.extend_from_slice(&length.to_le_bytes());
        body.extend_from_slice(&bytes);
    }
    body
}

/// Decodes a body from [`encode`].
pub fn decode(body: &[u8]) -> Result<(u32, Vec<Envelope>), String> {
    let (partition, mut body) = body
        .split_first_chunk::<4>()
        .ok_or("a body without its partition")?;
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
    Ok((u32::from_le_bytes(*partition), envelopes))
}

fn request(url: &str, secret: &str, body: Vec<u8>) -> Option<axum::http::Request<Full<Bytes>>> {
    axum::http::Request::post(url)
        .header("content-type", "application/octet-stream")
        .header(SECRET_HEADER, secret)
        .body(Full::new(Bytes::from(body)))
        .ok()
}

/// Ships one peer's Raft messages for one partition, in batches, as they come; a failed request drops
/// its batch.
async fn ship(
    client: crate::push::Client,
    url: String,
    secret: String,
    mut queue: tokio::sync::mpsc::Receiver<Envelope>,
    encode: impl Fn(&[Envelope]) -> Vec<u8>,
) {
    while let Some(first) = queue.recv().await {
        let mut batch = vec![first];
        while batch.len() < BATCH {
            match queue.try_recv() {
                Ok(envelope) => batch.push(envelope),
                Err(_) => break,
            }
        }
        if let Some(request) = request(&url, &secret, encode(&batch)) {
            let _ = tokio::time::timeout(Duration::from_secs(1), client.request(request)).await;
        }
    }
}

/// Ships messages for one partition to the server that leads it; a failed request drops the message,
/// and its sender repeats it.
async fn ship_remote(
    client: crate::push::Client,
    secret: String,
    partition: u32,
    mut queue: tokio::sync::mpsc::Receiver<(String, Remote)>,
) {
    while let Some((url, message)) = queue.recv().await {
        let mut body = partition.to_le_bytes().to_vec();
        body.extend_from_slice(&message.encode());
        if let Some(request) = request(&url, &secret, body) {
            let _ = tokio::time::timeout(Duration::from_secs(1), client.request(request)).await;
        }
    }
}

#[derive(Clone)]
struct Peer {
    shards: Arc<Shards>,
    secret: Arc<String>,
}

/// The routes other servers send to. Merged outside token authentication: the cluster's secret
/// authenticates them.
pub(crate) fn routes(shards: &Arc<Shards>, cluster: &Cluster) -> Router {
    Router::new()
        .route("/cluster/v1/raft", post(receive_raft))
        .route("/cluster/v1/remote", post(receive_remote))
        .with_state(Peer {
            shards: Arc::clone(shards),
            secret: Arc::new(cluster.secret.clone()),
        })
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

fn authentic(peer: &Peer, headers: &HeaderMap) -> bool {
    let secret = headers
        .get(SECRET_HEADER)
        .map(|value| value.as_bytes())
        .unwrap_or_default();
    same(secret, peer.secret.as_bytes())
}

async fn receive_raft(State(peer): State<Peer>, headers: HeaderMap, body: Bytes) -> StatusCode {
    if !authentic(&peer, &headers) {
        return StatusCode::UNAUTHORIZED;
    }
    let Ok((partition, envelopes)) = decode(&body) else {
        return StatusCode::BAD_REQUEST;
    };
    let Some(engine) = peer.shards.engines().get(partition as usize) else {
        return StatusCode::BAD_REQUEST;
    };
    let inbox = engine.raft_inbox();
    for envelope in envelopes {
        inbox.deliver(envelope);
    }
    StatusCode::NO_CONTENT
}

async fn receive_remote(State(peer): State<Peer>, headers: HeaderMap, body: Bytes) -> StatusCode {
    if !authentic(&peer, &headers) {
        return StatusCode::UNAUTHORIZED;
    }
    let Some((partition, message)) = body.split_first_chunk::<4>() else {
        return StatusCode::BAD_REQUEST;
    };
    let Ok(message) = Remote::decode(message) else {
        return StatusCode::BAD_REQUEST;
    };
    match peer
        .shards
        .engines()
        .get(u32::from_le_bytes(*partition) as usize)
    {
        Some(engine) => {
            engine.receive(message);
            StatusCode::NO_CONTENT
        }
        None => StatusCode::BAD_REQUEST,
    }
}

#[derive(Clone)]
struct Forward {
    peers: Arc<BTreeMap<u64, String>>,
    client: crate::push::Client,
}

/// Forwards HTTP/JSON requests (`/v1/...`) that reach a partition this server does not lead to the
/// server that does, and returns its answer: clients may ask any server. The request is tried here
/// first; an *unavailable* answer naming the leader ([`LEADER_HEADER`]) sends it there. Applied outside
/// authentication, so that the leader authenticates it; every server needs the same tokens. gRPC calls
/// are not forwarded.
pub(crate) fn forwarding(router: Router, cluster: &Cluster) -> Result<Router, String> {
    let forward = Forward {
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
    if grpc
        || !request.uri().path().starts_with("/v1/")
        || request.headers().contains_key(FORWARDED_HEADER)
    {
        return next.run(request).await;
    }
    let (parts, body) = request.into_parts();
    let Ok(body) = axum::body::to_bytes(body, MOST_FORWARDED).await else {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            axum::Json(serde_json::json!({ "error": "the request is too large" })),
        )
            .into_response();
    };
    let here = next
        .run(axum::extract::Request::from_parts(
            parts.clone(),
            axum::body::Body::from(body.clone()),
        ))
        .await;
    let leader = here
        .headers()
        .get(LEADER_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let target = leader.and_then(|leader| forward.peers.get(&leader));
    let (Some(target), StatusCode::SERVICE_UNAVAILABLE) = (target, here.status()) else {
        return here;
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
        return here;
    };
    match forward.client.request(outgoing).await {
        Ok(answer) => answer.map(axum::body::Body::new),
        Err(_) => here,
    }
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
        let body = encode(7, &envelopes);
        assert_eq!(decode(&body), Ok((7, envelopes)));
        assert!(decode(&body[..body.len() - 1]).is_err());
        assert!(decode(&[1, 0]).is_err());
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
