# 0050: The single-server target, as measured

- **Status:** accepted
- **Date:** 2026-11-06

## Context

BRIEF.md section 9 sets a target for a single server: 10,000 run starts per second over gRPC. Targets are starting
points, and "are revised by ADR with measured numbers". The first measurements
([M4, 2026-11-05](../measurements/2026-11-05-server-m4.md)) met it without tokens and missed it with tokens and the
audit log, on a machine that was not idle. Phase 3 is done when single-server targets are measured.

## The measurements

Starts per second over gRPC, 256 clients, `Durability::Fsync`:

| | no tokens | tokens and audit log |
|---|---:|---:|
| Apple M4, macOS, busy (two runs, 2026-11-05) | 12,489 to 14,656 | 8,272 to 9,394 |
| Apple M4, macOS, less busy (two runs, 2026-11-06) | | 9,598 to 10,290 |
| GitHub-hosted Linux runner, 4 vCPUs (one run, [2026-11-06](../measurements/2026-11-06-server-linux-runner.md)) | 17,203 | 14,446 |

An audited start waits for two syncs in turn: the engine's, then the audit log's. On macOS a sync is an `F_FULLFSYNC`,
about 4 ms (ADR 0003); on the Linux runner, about 0.18 ms. Group commit shares each sync among concurrent starts, so
throughput depends on how many share one, and the second sync on macOS roughly halves what is left.

## Decision

- **The target stands at 10,000, with tokens and the audit log**, because that is how a server beyond loopback runs
  (ADR 0042). It is met on Linux, and about met on the M4: at 256 clients, between 8,300 and 10,300 depending on what
  else the machine was doing.
- **No change to the audit log's durability for this.** Its line is synced before its request is answered (ADR 0041).
  Overlapping the two syncs, or putting audit lines in the engine's log, would raise the macOS number, at a cost in
  complexity or in what the audit log promises; neither is worth it for a target already met where servers usually run.
- **What counts as measured:** the M4 numbers, with the load they ran under, and a hosted Linux runner. Neither is a quiet
  reference machine.

## Consequences

- Phase 3's measurement criterion is met as far as these machines allow. A measurement on a quiet Linux server with a
  known disk is still open, and would replace the runner's numbers.
- On macOS, an audited server needs more concurrent clients than an unaudited one to reach the same throughput.
- The probe workflow can rerun the Linux measurement (`gh workflow run probe.yml`).
