# 0054: Raft on disk and on the wire

- **Status:** accepted
- **Date:** 2026-11-07

## Context

ADR 0051 step 6 runs the cluster on real machines: a Raft node's state must survive a crash on disk, and
its messages must cross the network. The node is sans-I/O; its driver persists and sends what each
`Ready` asks. Both need bytes.

## Decision

- **`aqueduct_raft::wire`** encodes entries, hard states, snapshots and envelopes (a message with its
  sender and receiver): LEB128 varints, length-prefixed byte strings and lists, and a version byte first
  in every value. Decoding refuses truncated or trailing bytes, unknown versions and unknown message
  kinds.
- **`aqueduct_storage::RaftStore`** keeps one node's state in a directory:
  - `hard_state` and `snapshot`, each one frame (length and CRC-32, as the partition log's records),
    replaced atomically: written under a temporary name, synced, renamed, and the directory synced;
  - `entries`, framed entries after the snapshot. Opening cuts off a torn tail; a conflict truncates the
    file; a snapshot rewrites it without the entries it covers (atomically, as above).
  - `persist(&Ready)` writes the snapshot, then truncates and appends the entries and syncs them, then
    replaces the hard state, so its commit index never runs ahead of the entries on disk. The driver
    sends the ready's messages only after it returns.
- Neither is part of the history format (`docs/history-format.md`): the partition's records inside the
  entries are, unchanged (ADR 0051). The Raft encodings may change before 1.0 with a version byte bump.

## Consequences

- Tested: a round trip of every message kind and refusal of every truncation; reopening after
  conflicts and snapshots; a torn tail; and 400 random persists, snapshots and reopenings against a
  model of what the disk must hold (the rules the simulators' disks follow). Making truncation a no-op
  fails two of these.
- A snapshot rewrites the entries file, which costs a copy of the log after it: fine while snapshots are
  rarer than appends.
- The transport (gRPC between nodes) and the engine driving replicas on real disks are the next parts of
  step 6.
