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

- The processor is generic over its host; `Local` keeps every existing test passing unchanged.
- **The replicated host** (`Builder::replicated`) keeps its Raft state in `raft/` with `RaftStore`
  (ADR 0054), ticks Raft's clock in the processor loop, takes messages from other replicas through
  `Engine::raft_inbox`, and sends through a `Transport` the caller provides. Replies gated behind an
  entry of its own wait in the processor until that index commits. A caller turned away gets
  `EngineError::NotLeader`, with the leader when known. Snapshots replace checkpoints every 1,000
  applied entries.
- **Tested** with three engines in one process (`tests/replicated.rs`): runs survive the leader's loss,
  a follower says who leads, a node that returns from its disk catches up, and after every machine stops
  and starts again nothing acknowledged is lost. Not persisting fails the last check.
- **Not caught by that test:** answering reads without the barrier. A stale read needs a leader cut
  off while it still believes it leads; the simulator covers that case for the replica itself, not for
  the engine's reads.
- The server's cluster mode (a network transport, and routing to the leader) follows.
