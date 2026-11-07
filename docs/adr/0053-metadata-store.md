# 0053: The metadata store, and moving replicas

- **Status:** accepted
- **Date:** 2026-11-07

## Context

BRIEF.md section 6.1 puts membership and the partition map in a small Raft group of its own. A cluster
must be able to replace a machine while it runs: move each replica it holds to another machine without
stopping the partition. Raft can change a group one member at a time (ADR 0051, step 3); something must
decide where replicas should be, and every host must learn it.

## Decision

- **The metadata store** (`aqueduct_engine::metadata`) is a state machine replicated by its own Raft
  group (`MetadataNode`, sans-I/O like a partition's replica). It holds an epoch (entries applied), the
  number of partitions (so the slot table, ADR 0052) and, for each partition, the machines its replicas
  should be on. A cluster is created with every partition on the same initial machines; commands change
  that. The first command is `Move { partition, from, to }`. Entries are JSON: metadata is small, rare and
  read by people.
- **Hosts watch it.** The store's leader pushes the metadata to every machine; a machine keeps the
  newest epoch it has seen. Nothing waits for it: a host with old metadata reconciles by it until it
  learns better. Partitions do not yet refuse requests for keys that are not theirs; while the slot
  table cannot change, routing cannot be stale.
- **A partition's leader reconciles** its group towards the metadata it has seen
  (`Replica::reconcile`): one membership change at a time, adding the new machine before removing the
  old, so the group never has fewer replicas than it should. A new replica starts empty, is added by the
  leader's log, and catches up from entries or a snapshot (ADR 0051, steps 2 and 3). A leader that
  removes itself steps down once the change commits, and another replica takes over.

## Consequences

- **Simulated:** `cargo xtask sim --replicas 3 --partitions 3 --spares 2 --workload fanout` adds spare
  machines and an operator who, during faults, asks the store to move random replicas to machines that
  have none. Metadata nodes crash and restart with their machines. At the end every partition must be on
  the machines the store says, besides every check of ADR 0051 and 0052.
- Moving slots between partitions (resharding) needs more: a slot's runs must move with it. Not yet.
- The store does not yet hold versions, approvals or namespaces: they stay in the registry and server
  configuration (phase 3) until the cluster server uses the store.
- The store's own group is fixed to the initial machines. Moving it is the same mechanism, not yet wired.
