# 0052: Many partitions, and messages between them

- **Status:** accepted
- **Date:** 2026-11-07

## Context

ADR 0011 fixed how keys map to slots and slots to partitions; ADR 0051 replicates one partition with Raft.
A cluster runs many partitions (BRIEF.md section 6.2, default 64). Most of what a run does involves only
its own key, but three things reach other keys: child runs (ADR 0034), messages to objects (ADR 0044) and
their cancellation. Within one partition these are function calls inside a batch. Between partitions they
must become messages that survive loss, duplication, reordering and either side failing over, with the
same guarantees: a child is started once and its outcome reaches every parent waiting for it; a message
reaches its object exactly once; a run's end is never logged before its messages are delivered.

## Decision

**Where a run lives.** A key's *home* is the key up to its first `#` (with the run id's generation
removed); an object key's loop and its calls live at `<object>/<key>` (object names and keys hold
no `/`, ADR 0057). The home's slot is
`aqueduct_codec::slot` (ADR 0011), and the slot table maps it to a partition. So:

- derived children (`<parent>#<command>`), awakeables and successors (continued as new) live with their
  run, and never cross partitions;
- an object's calls live with its loop;
- a child with a key logic chose, and a message to an object, may be on another partition.

**A partition is told which homes are its own** (`Partition::set_placement`); without a placement, as in
the embedded engine and the single server, every home is. What is not its own becomes
`Outbound::Remote { to, message }`, addressed by home; the host routes it by the slot table. Remote
messages are released only once what caused them is durable, like every outbound message.

**The protocol** (`aqueduct_engine::Remote`), designed so that no new journal entries are needed:

| Message | From | Does | Answered by |
|---|---|---|---|
| `StartChild` | a parent | starts the child, or finds the run its key has (idempotent) | `ChildResult`, when the child ends (at once, if it has) |
| `ChildResult` | the child's partition | records the outcome in the parent, if it still waits | nothing: the parent asks again until it has one |
| `CancelChild` | a cancelled parent | cancels the child, unless another parent still waits for it | `ChildResult` (the parent no longer waits, so it is only an answer) |
| `Deliver` | a run that sent | puts the call in the object's inbox (idempotent, by the id derived from sender and command) | `Delivered`, once that is durable |
| `Delivered` | the object's partition | releases the sender | |

- **The sender repeats until answered.** A parent waiting for a remote child sends `StartChild` again
  with every redispatch, and after recovery (its recovery effects start pending children again). The
  child's partition keeps the parents waiting for its runs in memory only: one lost in a failover asks
  again, and gets the outcome then.
- **A run's end waits for delivery.** A run whose messages are not yet `Delivered` is not activated: its
  next decision, which may end it, waits. Recovery delivers a running run's messages again before it is
  activated, as for one partition. A message to an object not deployed is acknowledged as it is within one
  partition: delivered again only when the sender recovers while still running.

**Simulated:** `cargo xtask sim --replicas 3 --partitions 4 --workload fanout` runs partitions as Raft
groups on three machines (machine *m* holds replica *m* of each), under the faults of ADR 0051. The fanout
workload calls a child of its own, a child shared under a chosen key, and sends to an object. Besides the
checks of one partition, every run must be on the partition of its home, and every object must have run
exactly one call per message. An engine test (`tests/partitions.rs`) does the same with partitions that
crash and recover from their logs over a network that loses, repeats and reorders messages.

## Consequences

- Within one partition nothing changes, and the history format does not change.
- **Stated gap:** a cancelling parent's partition repeats `CancelChild` with every redispatch until
  answered, but only while it remembers it: after it fails over, a child shared under a chosen key on
  another partition can run to its end without a parent. Only children with chosen keys can be remote.
- **Stated gap:** a run that ends by other means than its own decision (cancelled, for example) while
  its messages are in flight keeps repeating them until delivered, but only while its partition
  remembers them.
- **Stated gap:** a child's partition decides "another parent still waits" from the parents it has heard
  from since it last became leader.
- A remote child or message costs a round trip between partitions and delays the sender's next decision
  by it.
- The slot table is static in this step; moving slots between partitions (resharding) is later.
