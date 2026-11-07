# 0060: Rolling upgrades

- **Status:** accepted
- **Date:** 2026-11-07

## Context

BRIEF.md section 11 asks for rolling upgrades of the cluster, with a documented compatibility window of
one minor version between nodes. Two things make a rolling upgrade safe: servers of different versions
must know whether they may work together, and a server that stops must not leave the partitions it leads
waiting for an election.

## Decision

- **Every message between servers carries the sender's version** (`aqueduct-version`). A server takes
  messages only from the same major version and a minor version at most one away; others are refused
  (409). Raft's encoding and the Raft store carry version bytes of their own (ADR 0054), and the history
  format is unchanged by any of this.
- **A server hands over before it stops.** On SIGTERM or Ctrl-C in cluster mode, each partition it leads
  hands its leadership to a caught-up replica (`Replica::step_down`, a leadership transfer, ADR 0058), and
  the server waits up to 5 s for that before it stops. A partition with no caught-up replica elects a new
  leader as before.
- **The procedure**, one server at a time: stop it (it hands over), replace the binary, start it, and
  wait until it has caught up (its partitions show it as a follower that answers) before the next. Every
  server's version stays within one minor version of every other's throughout.

## Consequences

- Tested with three servers: the leader's server steps down and another leads well before an election
  timeout, and acknowledged runs are served; a message from version 9.0.0 is refused.
- Before 1.0, a minor version may change formats: the window is enforced, but what it promises is
  stated only once the formats are frozen (phase 5).
- After an upgrade, leadership returns to each partition's preferred server on its own (ADR 0058).
- Not yet: an upgrade test that runs two real binaries of different versions against each other.
