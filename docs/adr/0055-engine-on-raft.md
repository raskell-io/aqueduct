# 0055: The engine on a replicated partition

- **Status:** accepted
- **Date:** 2026-11-07

## Context

The embedded engine (ADR 0007) runs one partition on a local log: a processor thread takes requests,
writes a batch, syncs once, and then answers. The server is built on it. ADR 0051 step 6 runs the same
engine where the partition's log is a Raft group (`Replica`, step 4), stored on disk and sent between
machines (ADR 0054). The processor touches its partition in some forty places, and several of its
answers rest on assumptions a replica breaks: that a flush makes things durable at once, that the
partition is always there, and that finished runs live on in a checkpoint.

## Decision

- **The processor runs on a host** (`embedded::Host`): what owns the partition, lends it to the
  processor when it is here, writes its batches, and saves checkpoints. `Local` is today's: a file log
  and a redb checkpoint, always here. A replicated host comes next. Nothing else in the processor
  changes for it.
- **For the replicated host, decided now:**
  - The partition is here only on the leader whose no-op is committed. Elsewhere every request is
    answered *not leader*, with the leader when it is known, and the caller retries there. Reads too:
    a follower holds only the committed records, not a partition.
  - Answers that rest on memory (a run's end for a wait, an update's answer, a query, a description)
    are sent only once everything written so far is committed, and after an entry of the leader's own
    commits, as `Replica` already does for acknowledgements (ADR 0051). Reads are therefore
    linearizable, at the price of a round of replication.
  - A leader that loses its leadership answers every caller still waiting with *not leader*: their
    requests are idempotent (start ids, signal ids, update ids), so retrying elsewhere is safe.
  - No checkpoints and no eviction of finished runs: the Raft snapshot holds the committed records
    instead, and the partition is rebuilt from them on each election.
  - Deployments stay local files: an operator deploys on every machine. Moving them into the metadata
    store (ADR 0053) is later.
  - Imports (ADR 0047) are refused on a replicated engine.

## Consequences

- This change is the refactor alone: the processor is generic over its host, with `Local` the only one,
  and every existing test passes unchanged.
- The replicated host, an in-process test of several engines with failover, and the server's cluster
  mode follow.
