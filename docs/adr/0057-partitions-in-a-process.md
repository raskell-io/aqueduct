# 0057: Many partitions in one process

- **Status:** accepted
- **Date:** 2026-11-07

## Context

ADR 0052 lets partitions send each other children and messages; the simulator runs them. The engine
runs one partition. A server with many partitions (the brief's default is 64) needs one engine per
partition, calls routed by key, and the messages between partitions carried from engine to engine.

## Decision

- **`aqueduct_engine::shards::Shards`** holds one `Engine` per partition, each in
  `<data dir>/partitions/<index>`, and offers the engine's calls, routed by the home of their key (or run
  id, awakeable id or escalation id). Listings (runs, escalations) gather every partition's. Deployments
  are activated and retired on every partition. With one partition it is the plain engine in the data
  directory itself, so a single server's layout does not change.
- **An engine is told it is one partition of many** (`Builder::partition_of`): its placement, and a
  route for messages to other partitions (`Outbound::Remote`), which `Shards` hands to the engine of the
  partition they are for (`Engine::receive`). In a cluster, `Shards::open_routed` lets the caller send
  them to the node that leads that partition instead.
- **Workers poll one queue.** The engines share one step queue and one activation queue
  (`Builder::shared_step_queue`, `shared_activation_queue`); a result is routed by its run id.
- **Object keys and names hold no `/`** (beyond a namespace's `$ns/<namespace>/` before the key), so
  that a call's run key `$call/<object>/<key>/<id>` says whose call it is. An object key's loop and its
  calls live at `<object>/<key>` (ADR 0052). A call with such a key is refused as an invalid key. Stored
  keys do not change.

## Consequences

- Tested with four partitions in one process (`tests/shards.rs`): parents spread over partitions call a
  child shared under a chosen key and send to counters on other partitions; every parent ends right,
  every counter counts each message once, each shared child lives only at its home, and everything is
  there after reopening. A route that delivers nothing fails it.
- **The server runs on `Shards`:** `"partitions": N` in its configuration (default 1, at most 4,096)
  opens N partitions under the data directory; every call is routed by key, listings are gathered,
  push endpoints and workers share one queue. A directory laid out for another number of partitions is
  refused. Imports need a server with one partition. Tested over HTTP with four partitions
  (`aqueduct-server/tests/partitions.rs`).
- **In a cluster** (`Cluster::open`), each partition is a replica of its own Raft group across the
  servers. Raft messages carry their partition (`POST /cluster/v1/raft`). A message for another
  partition goes to this server's engine for it if that leads, and otherwise to the server that leads it
  (`POST /cluster/v1/remote`, the message as JSON, authenticated by the cluster's secret). An HTTP request
  is tried where it arrives; an *unavailable* answer naming the leader (`aqueduct-leader`) forwards it
  there, so each partition's requests reach its own leader.
- Tested with three servers of two partitions each (`aqueduct-server/tests/cluster.rs`): parents with
  shared children and messages to counters, asked of any server, end right; after a server stops, every
  counter has counted each message once.
- **Not covered by that test:** messages sent to another server's partition. The servers start one after
  another and the first wins every partition's election, so all leaders are on one server; making
  `/cluster/v1/remote` a no-op is not caught. Leadership balancing (step 7) will spread leaders, and the
  test with them.
- Moving slots between partitions (resharding) is still later (ADR 0053).
