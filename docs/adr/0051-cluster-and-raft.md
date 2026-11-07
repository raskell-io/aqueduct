# 0051: The cluster, and Raft of our own

- **Status:** accepted
- **Date:** 2026-11-06

## Context

Phase 4 (BRIEF.md section 15) replicates the engine: each partition's log becomes a Raft group of three or five
replicas, so a partition survives the loss of a minority of its nodes (section 5.5). It is done when a simulated
five-node cluster survives network partitions, crashes and clock skew for 100,000 seeds nightly, when cluster targets
are measured, and after a failover drill on real machines.

Open question 5 asked whether to use openraft or write Raft ourselves. ADR 0006 already decided how the engine is
built: every component is a synchronous state machine that consumes messages and returns messages and storage
operations, so that one seeded event queue can drive the whole system and any failure reproduces from its seed.

## Options

1. **openraft.** Very active, MIT or Apache-2.0. It is asynchronous and built on Tokio: the simulator could only drive
   it with a deterministic async executor, which ADR 0006 rejected. 0.10 is still in alpha (alpha.37, October 2026)
   with frequent API changes, and it brings Tokio, futures, chrono, clap and tracing into the core.
2. **raft-rs (TiKV).** Proven in production, and sans-I/O (`RawNode`, `Ready`), so it fits the simulator. Its last
   release is 0.7 from March 2023. It depends on protobuf 2, slog, fxhash and rand 0.8; adapting its storage trait to
   our log is work as well.
3. **Our own, sans-I/O.** A Raft written as a pure state machine, in the shape of the partition. No new dependencies,
   and the simulator drives it exactly. The cost: Raft's correctness is ours to establish.

The owner chose option 3 (2026-11-06).

## Decision

**`aqueduct-raft`: Raft as a pure state machine.** A node takes messages, clock ticks and proposals, and produces a
`Ready`: the term, vote and commit index to persist, the entries to persist (after truncating a conflicting suffix), the
messages to send, and the entries committed and durable here, to apply. The driver persists what a `Ready` asks before
sending its messages, and reports back with `advance`. The node never touches a disk, a socket or a clock.

The algorithm follows the Raft paper and thesis (Ongaro, 2014):

- **Leader election** with randomised timeouts, a **pre-vote** phase (thesis 9.6) so that a node cut off from the
  others cannot raise terms and disrupt a working leader when it returns, and **check quorum**: a leader that has not
  heard from a majority within an election timeout steps down.
- **Log replication** with the consistency check on the previous entry, conflict hints to find the agreement point
  quickly, and commit only of entries from the leader's own term (thesis 3.6.2), helped by a no-op entry at the start of
  each term.
- **Durability:** a vote is granted, and an append acknowledged, only after it is persisted; a leader counts its own
  entries towards a majority only once it has persisted them. A node applies only entries both committed and
  durable on it.

**Built in this order, each step simulated before the next:**

1. Election, replication and commit; the simulator with five nodes, partitions, crashes and clock skew (this ADR's PR).
2. Snapshots and log trimming; replicas that catch up from a snapshot.
3. Membership changes, one server at a time (thesis 4.1).
4. A replicated partition: the engine's batches become Raft entries, the leader runs the partition, followers
   materialise its state, and acknowledgements wait for commit instead of a local sync.
5. Many partitions, the slot table (ADR 0011) and a metadata store as a Raft group of its own.
6. The cluster transport over gRPC, and routing at ingress.
7. Leadership balancing, snapshots to object storage, history archival and retention, rolling upgrades.
8. The web console, the 100,000-seed nightly simulation, cluster measurements and a failover drill.

**The replicated partition (step 4)** is `aqueduct_engine::replica::Replica`, sans-I/O like the partition and the
Raft node it pairs:

- Each batch the partition writes becomes one Raft entry, holding the batch's records as they are written to a local
  log today. Everything the batch releases (start and result acknowledgements, dispatches) is held until the entry
  is committed: nothing is acknowledged or dispatched that a majority does not hold.
- A follower's state machine is the committed records. It runs no partition.
- A new leader builds its partition from the committed records, as crash recovery does, but only once its own no-op
  is committed: then everything in its log is committed, and it never acts on an entry a majority might not have.
  Until then it answers *not ready*, and a replica that does not lead answers *not leader*, with the leader if it
  knows one.
- A leader that loses its leadership, or sees a newer term, drops its partition and everything it holds: none of it
  was acknowledged. Whoever leads next rebuilds from what was committed.
- A snapshot holds the committed records; a replica restored from one rebuilds from them.
- **A deposed leader that does not know it yet still holds its partition.** What it answers from its own
  state without writing anything (a result no longer needed, a start that already exists) could be
  answered from an old state: a newer leader may have dispatched the command it does not know. Such
  answers wait for an empty entry of their own to commit, which proves the replica still leads.
  Dispatches need no proof. Found by the simulator with several partitions (ADR 0052).
- **A leader can step down on a tick** (check quorum) with nothing to persist or send: its partition is
  dropped at once, not at its next ready. Found by the simulator moving replicas (ADR 0053).

`cargo xtask sim --replicas 3` runs the simulator's workloads on such a group, with crashes, restarts from what each
replica persisted, network splits, pauses, clock skew, and lost, duplicated and reordered messages. After every
event, replicas must agree on every committed record they share, and a leader's partition must be the fold of its
journals. At the end, every acknowledged start is a run, every acknowledged result is recorded once (or its run ended
without it), every run ends as the sequential reference says, the leader's partition equals one rebuilt from the
committed records, and every replica has applied the same records. Workloads with signals, escalations, updates,
child runs and objects are not run replicated yet: their client side in the simulator assumes one node.

**Reads** go to the leader and confirm leadership with a round of heartbeats (read index, thesis 6.4) rather than a
lease, so that correctness does not depend on bounded clock drift. **Timers** keep their meaning (BRIEF.md section 5.3):
they fire no earlier than their wake time by the current leader's clock. After a failover, a new leader whose clock is
ahead of the old one's can fire a timer early by that difference; that is stated, not hidden.

## Consequences

- The simulator can drive whole clusters deterministically, and a failure reproduces from its seed, as for one node.
- **Raft's correctness is ours.** It is checked after every simulated event: at most one leader per term, logs that
  agree up to any entry they share, every committed entry in every later leader's log, the same entry applied at each
  index everywhere, and no acknowledged proposal lost. A Jepsen-style test on real machines comes before 1.0
  (section 10). Until that, the cluster must not be called production-ready.
- No dependency to keep up with, and none of its async runtime in the core.
- Performance work (pipelining, batching appends, flow control) is ours too. The first version sends one append at a
  time per follower, with all entries it is missing.
- The Raft log stores the partition's batches as they are now, so the history format does not change.
