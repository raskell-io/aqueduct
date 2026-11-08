# 0065: The drill tool

- **Status:** accepted
- **Date:** 2026-11-08

## Context

Phase 4 ends with a failover drill on real machines and measured cluster targets (BRIEF.md sections 9
and 14). Both need load against a running cluster and an exact account of what it acknowledged.

## Decision

- **`aqueduct drill`** starts runs at a steady rate (`--rate` a second for `--seconds`) on the given
  servers in turn, over gRPC (ingress calls are forwarded to each partition's leader, ADR 0056). A
  server that does not answer is passed over for the next, as a client would: a start is idempotent, so
  it counts as refused only if no server took it. Each acknowledged run is then waited for on any server.
- **It reports:** starts sent, acknowledged and refused; runs that failed; p50, p99 and maximum latency
  of a start and of a run from start to end; runs ended a second; the longest time from a refused start
  to the next acknowledged one (what a failover costs); and how many acknowledged runs no server knows
  at the end. **Any such run fails the drill** (an error exit naming them): the cluster lost
  something it acknowledged.
- **The procedure** for real machines is `docs/failover-drill.md`: four ways to take a leader's machine
  away (stop, kill, cut off, power off), a lost disk, and the throughput and latency measurements.

## Consequences

- Tested with three servers in one process, one of which stops part way: no acknowledged run is lost,
  and refusals are limited to the failover.
- The drill measures the engine with a workflow that returns at once; measuring with steps needs workers
  on the machines too.
- The drill on real machines, and the measurements, are the owner's to run: they need the machines.
