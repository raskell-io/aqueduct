# 0062: Snapshots in object storage

- **Status:** accepted
- **Date:** 2026-11-07

## Context

A partition's replica compacts its Raft log into a snapshot of the committed records every so often
(ADR 0051, 0055). A server that starts on an empty data directory (a replaced disk, a new machine) is
otherwise sent the leader's snapshot and entries over the cluster network. ADR 0051 step 7 asks for
snapshots in object storage; the store is ADR 0061's.

## Decision

- **Snapshots are copied as they are taken.** With `"snapshots": <store>` in the cluster section, each
  partition's replica uploads every snapshot it takes, after it is on its own disk, to
  `snapshots/<partition>/latest` (`aqueduct_raft::wire` encoding), replacing the one before. Only
  committed state is ever compacted, so any of them is safe to start from. `"snapshot_every"` sets the
  entries applied between snapshots (default 1,000).
- **A server on an empty directory starts from the latest.** Before a partition opens, an empty Raft
  directory is filled with the store's latest snapshot of that partition (`cluster::bootstrap`); the
  replica then needs only the entries after it from the leader. A directory that holds anything already
  is left as it is: its own state is never replaced by the store's.

## Consequences

- Tested: three servers with snapshots every five entries copy a snapshot of the right index and
  members to a directory store; an empty directory is filled from the latest snapshot and a full one is
  not touched.
- Not tested: a server replacing a lost disk end to end on real machines, which belongs to the
  failover drill (step 8), and a real S3 bucket.
- An older upload finishing after a newer one can leave the store a snapshot behind: still safe, the
  replica is sent more entries.
