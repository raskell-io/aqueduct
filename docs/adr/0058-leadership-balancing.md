# 0058: Leadership transfer and balancing

- **Status:** accepted
- **Date:** 2026-11-07

## Context

With many partitions on a few servers, whichever server wins elections first leads every partition:
in tests, the first server started led them all. One server then does all the work, and messages
between partitions never leave it. ADR 0051 step 7 asks for leadership balancing.

## Decision

- **Raft leadership transfer** (thesis 3.10): `RaftNode::transfer_leadership(to)` on the leader. It stops
  taking proposals, brings `to` up to date, and sends it `TimeoutNow`, upon which `to` stands for election
  at once without a pre-vote. Voters grant it as any up-to-date candidate of a newer term. A
  `TimeoutNow` counts only from this term's leader, to a member. A transfer that has not happened within
  an election timeout is given up, and the leader takes proposals again. The wire encoding gains the
  message (kind 5).
- **A preferred leader per partition:** in a cluster, partition `p` prefers the `p`-th node of the sorted
  peers, round robin (`Replication::preferred_leader`). Every 40 ticks (2 s at the default tick), a
  leader that is not the preferred node hands over to it, but only if that node is caught up: a node
  that is down or behind is never handed to, so a transfer does not stall a partition for nothing.
- **Messages between partitions are repeated every second** when unanswered (`Partition::repeat_remote`),
  rather than with the step re-dispatch every 30 s: a message lost while leadership moves would
  otherwise hold its sender up that long.

## Consequences

- The Raft simulator's operator hands leadership to a random member during faults; 20,000 seeds of five
  nodes and 5,000 of three pass with transfers, membership changes, crashes and splits together. Unit
  tests: a transfer to a lagging follower that it first brings up to date, proposals refused while it
  is under way, and a transfer given up after an election timeout.
- In the three-server test with two partitions, the partitions are led by different servers, so
  messages between partitions cross servers: making `/cluster/v1/remote` a no-op now fails it.
- While a partition hands over, its requests are briefly answered *unavailable*; clients retry, as for
  any change of leader.
- The preference is static. Balancing by load, and moving the preference when a node is replaced
  (ADR 0053), are later.
