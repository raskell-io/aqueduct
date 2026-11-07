# 0056: The server in a cluster

- **Status:** accepted
- **Date:** 2026-11-07

## Context

ADR 0055 runs the engine on a replicated partition, with a transport the caller provides. The server
must provide one between machines, and clients must find the node that leads.

## Decision

- **Configuration:** a `"cluster"` section in the server configuration: this node's `id`, every node's
  base URL under `peers` (this one included; an odd number, at least three), a `secret` of at least 16
  characters that every node shares, and `tick_ms` (default 50: an election after 0.5 to 1 s without a
  leader). `aqueduct server` with it runs the data directory's partition as a replica; its Raft state is
  in `raft/` under the directory.
- **Raft between nodes** travels as `POST /cluster/v1/raft` on the server's own listener: a batch of
  `aqueduct_raft::wire` envelopes, each length-prefixed, with the secret in the
  `aqueduct-cluster-secret` header. The route sits outside token authentication; a request without the
  secret is refused (401). Each peer has a queue of 4,096 messages and a task that ships them in
  batches of up to 256; a full queue or a failed request drops messages, which Raft sends again.
- **Clients** may ask any node. Over HTTP/JSON, a node that does not lead forwards the request to the
  node that does and returns its answer, marked `aqueduct-forwarded` so that it is never forwarded
  twice. Forwarding happens before authentication: the leader authenticates, so every node needs the
  same tokens. Over gRPC, and when no leader is known, a node that does not lead answers *unavailable*
  (HTTP 503, gRPC `UNAVAILABLE`), naming the leader's node id when it knows it; the client asks another
  node. gRPC forwarding, and SDK clients that follow the leader on their own, are later.
- **Workers** connect to every node: steps are dispatched by whichever node leads.

## Consequences

- Tested with three servers in one process over HTTP (`tests/cluster.rs`): a follower forwards a start
  and a wait to the leader; a request already forwarded is not forwarded again (503); Raft messages
  without the secret are refused; the leader's server stops and another serves what it acknowledged,
  and new work. Turning forwarding off fails the test.
- The secret is a shared bearer secret, not mutual TLS: the cluster network must be private, or the
  servers behind TLS (ADR 0042) with the peers' URLs on `https://`. Mutual TLS between nodes is part of
  the security review before 1.0.
- Several partitions per server: ADR 0057.
- The membership is the configuration's; moving replicas (ADR 0053) is not wired into the server yet.
