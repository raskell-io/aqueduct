# fsync and storage on a GitHub-hosted Linux runner

- **Date:** 2026-10-31
- **Source:** the `Probe` workflow (`.github/workflows/probe.yml`), run 41209481379, job `probe (ubuntu-latest)`
- **Caveat:** hosted runners use virtualised, shared disks whose write cache behaviour is not documented.
  These numbers are a first Linux data point, not a reference for targets (ADR 0003, ADR 0009).

## fsync (`cargo xtask fsync`, 256-byte entries)

| | p50 | p99 |
|---|---:|---:|
| `sync_all` | 0.234 ms | 0.577 ms |
| `sync_data` (`fdatasync`) | 0.261 ms | 0.548 ms |

| group commit batch | entries/s | batch p99 |
|---:|---:|---:|
| 1 | 3,520 | 0.475 ms |
| 64 | 162,379 | 0.555 ms |
| 1,024 | 1,434,019 | 3.392 ms |

## Storage benchmark (durable appends, records/s)

| batch | plain log | redb | fjall |
|---:|---:|---:|---:|
| 1 | 3,677 | 4,104 | 9,065 |
| 16 | 47,741 | 34,451 | 51,974 |
| 128 | 320,144 | 207,089 | 268,892 |
| 1,024 | 1,466,329 | 504,883 | 708,465 |

Without fsync (batch 128): plain log 3.6 million records/s, redb 1.2 million, fjall 1.3 million.
State: redb reopens in 0.25 ms and reads 482,000 states/s; fjall reopens in 714 ms and reads 392,000/s.

## Observations

- A flush is about 17 times cheaper here than `F_FULLFSYNC` on the M4, so latency targets differ by
  platform as ADR 0003 expected.
- The plain log keeps its lead at large batches, which is the case group commit produces under load.
- fjall commits single records faster than a plain `fdatasync` on our log (0.07 ms against 0.23 ms). A likely
  reason is that fjall preallocates its journal, so `fdatasync` has no file size change to persist.
  Preallocating log segments is a follow-up to measure for `FileLog`.
