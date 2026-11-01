# 0003: Durability baseline and latency targets

- **Status:** accepted
- **Date:** 2026-10-31

## Context

BRIEF.md section 9 set "start to first step dispatched, p99, under 5 ms" with fsync on, and 50,000 journal
appends per second with group commit, on the owner's Apple M4. The brief asked for raw fsync latency to be
measured before these targets were trusted. The measurement is in
[docs/measurements/2026-10-31-fsync-m4.md](../measurements/2026-10-31-fsync-m4.md): one `F_FULLFSYNC` costs
about 4 ms at p50 and 5 to 8 ms at p99, with device spikes around 25 ms.

## Options

1. Keep the targets and use plain `fsync` on macOS, which returns before data reaches stable storage.
2. Keep the targets, but only for `Durability::Os`.
3. Derive fsync-on targets from the measurement.

## Decision

- `Durability::Fsync` (the default) means `F_FULLFSYNC` on macOS and `fdatasync` on Linux. We never
  silently weaken it.
- A request is acknowledged only after the batch containing its entries is synced. On macOS, the
  fsync-on latency floor is therefore about one flush (around 4 ms) plus queueing.
- Revised embedded targets on the M4:
  - start to first step dispatched, p99, `Durability::Fsync`: **under 15 ms**;
  - start to first step dispatched, p99, `Durability::Os`: **under 5 ms** (unchanged);
  - journal appends per second, `Durability::Fsync`, group commit: **50,000+** (unchanged; needs batches
    of about 200 entries, so the group commit window adapts to load rather than flushing per request).
- Linux targets are set when the Linux reference machine is measured.

## Consequences

- Group commit must be adaptive: flush immediately when idle, batch up while a flush is in flight.
- Benchmarks report results per durability mode and per OS. A benchmark that does not say which
  durability mode it used is invalid.
- **Measured on 2026-10-31** ([docs/measurements/2026-10-31-engine-m4.md](../measurements/2026-10-31-engine-m4.md)),
  end to end with the embedded engine: start to first step p99 5.7 to 8.3 ms with `Fsync` and under
  0.1 ms with `Os`; 72,000 to 180,000 journal entries per second with `Fsync` and group commit, from 64
  and 256 concurrent clients. All three targets are met on the M4. `Durability::Os` is now selectable with
  `Builder::durability`.
