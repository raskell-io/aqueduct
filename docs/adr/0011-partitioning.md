# 0011: Partitioning: fixed hash slots mapped to partitions

- **Status:** accepted
- **Date:** 2026-10-31

## Context

Run keys and object keys must map to partitions so that everything about a key happens on one partition
(BRIEF.md section 6.2). Open question 4 asked whether to fix the partition count at cluster creation and add
resharding later, or to use virtual partitions from the start. Changing how keys hash after data exists is
a migration of every key, so the mapping must be decided now, even though phase 1 runs a single partition.

## Decision

- Every key hashes to one of **4,096 slots**:
  `slot = u64_be(SHA-256("aqueduct-slot-v1" || 0x00 || key)[0..8]) mod 4096`, where `key` is the UTF-8 run
  key or object key. The function is part of the format specification and never changes for a given
  cluster.
- Slots map to **partitions** (one Raft group each, phase 4) through a slot table in the metadata store.
  Resharding moves slots between partitions; keys never rehash.
- Embedded mode and single server have one partition that owns all slots; nothing about slots is visible to
  users.
- Cross-partition interactions use durable outboxes (BRIEF.md section 6.2), addressed by slot.

## Consequences

- Clusters can grow and rebalance without rewriting keys, at the cost of a slot table and a migration
  protocol for moving a slot (phase 4).
- 4,096 slots bound the number of partitions; with the brief's default of 64 partitions that is 64 slots
  each. Raising the slot count later would need a new slot function version, which this decision avoids.
- SHA-256 is slower than a non-cryptographic hash but runs once per request and is already used by the
  history format; a second hash function would be another thing to specify.
