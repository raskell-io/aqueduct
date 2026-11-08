# 0063: Moving replicas between servers

- **Status:** accepted
- **Date:** 2026-11-08

## Context

Replacing a machine means moving each partition's replica from the old server to a new one while the
partition keeps running. Raft changes a group one server at a time (ADR 0051, step 3), and the metadata
store can drive such moves in the simulator (ADR 0053). The server had no way to ask for one.

## Decision

- **An initial group:** the cluster section's `"members"` (by default every peer) is the group each
  partition starts with. A peer outside it starts with no group and waits for a leader to add it; it then
  catches up from entries or a snapshot (its own store's, ADR 0062, or the leader's).
- **An admin call** (admin permission, audited): `POST /v1/admin/partitions/<p>/members` with
  `{"add": <node>}` or `{"remove": <node>}` proposes one change on the partition's leader; any server
  forwards it there. One change at a time: another is refused (422) until the last is committed.
  `GET /v1/admin/partitions` shows each partition's group and leader as the asked server knows them.
- **The procedure** to replace server A by B for a partition: add B, wait until it is in the group, then
  remove A. A leader that removes itself steps down once that commits. When A has left every partition,
  it can be stopped.

## Consequences

- Tested with four servers whose partition starts on 1, 2 and 3: server 4 is added, server 1 removed,
  server 1 stopped, and the partition on 2, 3 and 4 serves the run it acknowledged and takes new ones.
- The moves are the operator's, partition by partition. Driving them from the metadata store (ADR 0053),
  as the simulator does, is later; so is moving a partition's preferred leader (ADR 0058) when its
  server leaves.
- A peer added to `"peers"` needs every server's configuration to know its URL, and a restart to read
  it.
