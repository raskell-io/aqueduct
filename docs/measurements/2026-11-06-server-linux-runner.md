# Single server benchmark: GitHub-hosted Linux runner

- **Date:** 2026-11-06
- **Source:** the `Probe` workflow (`.github/workflows/probe.yml`), run 42201459880, job `server benchmark (ubuntu-latest)`
- **Machine:** a hosted `ubuntu-latest` runner: 4 vCPUs (AMD EPYC 9V74), 15 GB, a virtualised disk shared with other
  tenants. `fdatasync` on Linux.
- **Tool and setup:** as for [the M4 measurement](2026-11-05-server-m4.md): `cargo run --release -p aqueduct-bench-server`,
  the load generator in the same process, 5 seconds per row.

**How far to trust these.** One run, on a shared virtual machine whose disk's write cache is not documented (as the
[earlier Linux probe](2026-10-31-linux-runner.md) says). The disk is fast to sync, so this measures the engine and the
CPU more than storage. It is a Linux data point, not a reference machine.

| `fsync` (`cargo xtask fsync`, one entry per sync) | p50 | p99 |
|---|---:|---:|
| sync_all | 0.184 ms | 0.778 ms |
| sync_data | 0.179 ms | 0.625 ms |

## Against BRIEF.md section 9

| target | Linux runner | |
|---|---:|---|
| run starts per second over gRPC, no tokens, `Fsync`: 10,000+ | 17,203 | met (256 clients) |
| the same, with tokens and the audit log | 14,446 | met (256 clients) |

## Reading the numbers

- **A sync costs about 0.18 ms here, against about 4 ms on the M4.** So one client gets 1,486 starts per second
  (about 200 on the M4), and the audit log's second sync costs little: 14,446 audited starts per second against 17,203
  without.
- **Syncs still cost something, and CPU is short.** `Os` durability (no sync at all) is 1.6 to 2.2 times faster than
  `Fsync` (26,029 to 28,160 per second), and p99s are high: 4 vCPUs are shared by the server and the load generator.
- **HTTP/JSON was faster than gRPC at 256 clients** (27,930 against 17,203), the other way round from 16 clients. One run
  on a shared machine does not say why; it is not a finding.
- **Overload holds:** with 16 times as many clients as places, no errors, as on the M4.

## Run

### Run starts

| what | clients | per second | p50 ms | p99 ms | max ms | shed | errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| gRPC, `Fsync`, no tokens | 1 | 1486 | 0.39 | 2.44 | 294.51 | 0 | 0 |
| gRPC, `Fsync`, no tokens | 16 | 9377 | 1.15 | 16.98 | 111.67 | 0 | 0 |
| gRPC, `Fsync`, no tokens | 64 | 12062 | 2.70 | 51.60 | 336.04 | 0 | 0 |
| gRPC, `Fsync`, no tokens | 256 | 17203 | 8.56 | 188.72 | 285.46 | 0 | 0 |
| gRPC, `Os`, no tokens | 64 | 26029 | 1.90 | 20.06 | 99.71 | 0 | 0 |
| gRPC, `Os`, no tokens | 256 | 28160 | 7.00 | 63.86 | 129.83 | 0 | 0 |
| HTTP/JSON, `Fsync`, no tokens | 1 | 2306 | 0.30 | 0.88 | 91.79 | 0 | 0 |
| HTTP/JSON, `Fsync`, no tokens | 16 | 7999 | 0.98 | 31.05 | 191.04 | 0 | 0 |
| HTTP/JSON, `Fsync`, no tokens | 64 | 18647 | 2.30 | 25.96 | 184.65 | 0 | 0 |
| HTTP/JSON, `Fsync`, no tokens | 256 | 27930 | 7.21 | 38.29 | 61.17 | 0 | 0 |

### Tokens and the audit log

| what | clients | per second | p50 ms | p99 ms | max ms | shed | errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| starts, tokens and audit, `Fsync` | 1 | 1086 | 0.63 | 4.33 | 142.41 | 0 | 0 |
| starts, tokens and audit, `Fsync` | 16 | 5687 | 1.81 | 21.64 | 149.70 | 0 | 0 |
| starts, tokens and audit, `Fsync` | 64 | 9936 | 3.94 | 60.38 | 184.64 | 0 | 0 |
| starts, tokens and audit, `Fsync` | 256 | 14446 | 13.41 | 97.19 | 208.70 | 0 | 0 |
| starts, tokens and audit, `Os` | 64 | 7776 | 3.62 | 142.78 | 264.57 | 0 | 0 |
| starts, tokens and audit, `Os` | 256 | 17599 | 11.90 | 57.62 | 161.68 | 0 | 0 |
| reads, no tokens | 64 | 40515 | 1.37 | 3.55 | 44.97 | 0 | 0 |
| reads, 1 token | 64 | 36776 | 1.51 | 3.86 | 43.98 | 0 | 0 |
| reads, 1,000 tokens | 64 | 30687 | 1.49 | 30.48 | 73.99 | 0 | 0 |

### Namespaces and running-run caps

| what | clients | per second | p50 ms | p99 ms | max ms | shed | errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| default namespace | 256 | 23923 | 8.06 | 68.69 | 131.69 | 0 | 0 |
| named namespace, by header | 256 | 25482 | 8.37 | 37.13 | 91.96 | 0 | 0 |
| no cap, nothing parked | 256 | 23212 | 8.28 | 59.03 | 124.63 | 0 | 0 |
| no cap, 10000 parked | 256 | 20663 | 8.73 | 70.52 | 129.01 | 0 | 0 |
| no cap, 100000 parked | 256 | 15233 | 8.69 | 190.21 | 233.83 | 0 | 0 |
| a cap configured, nothing parked | 256 | 21699 | 8.77 | 55.14 | 189.46 | 0 | 0 |
| a cap configured, 10000 parked | 256 | 13765 | 9.47 | 173.97 | 335.98 | 0 | 0 |
| a cap configured, 100000 parked | 256 | 15269 | 8.94 | 223.71 | 272.82 | 0 | 0 |

### Overload (default limits: 256 concurrent requests)

| what | clients | per second | p50 ms | p99 ms | max ms | shed | errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| more clients than places | 256 | 19085 | 8.24 | 112.62 | 436.02 | 0 | 0 |
| more clients than places | 1024 | 12151 | 17.89 | 170.18 | 252.25 | 303239 | 0 |
| more clients than places | 4096 | 10357 | 56.53 | 226.10 | 476.59 | 353955 | 0 |
| 64 concurrent requests allowed | 1024 | 6568 | 9.78 | 264.71 | 369.35 | 424722 | 0 |

### Steps on remote workers (`Fsync`)

| what | clients | per second | p50 ms | p99 ms | max ms | shed | errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| 3-step run, start to result | 1 | 397 | 1.38 | 41.84 | 370.27 | 0 | 0 |
| start to first step at the worker | 1 | 397 | 0.39 | 2.30 | 368.99 | | |
| 3-step run, start to result | 16 | 1941 | 3.53 | 85.53 | 181.65 | 0 | 0 |
| start to first step at the worker | 16 | 1941 | 0.89 | 40.25 | 138.87 | | |
| 3-step run, start to result | 64 | 4150 | 7.82 | 134.86 | 192.50 | 0 | 0 |
| start to first step at the worker | 64 | 4150 | 2.02 | 90.76 | 147.31 | | |
| 3-step run, start to result | 256 | 4655 | 27.23 | 326.94 | 338.60 | 0 | 0 |
| start to first step at the worker | 256 | 4655 | 6.14 | 153.13 | 316.29 | | |
| 1-step run on HTTP pollers, start to result | 16 | 2160 | 2.48 | 198.54 | 697.53 | 0 | 0 |
| 1-step run on HTTP pollers, start to result | 64 | 5903 | 5.60 | 130.99 | 235.11 | 0 | 0 |

### Draining a queued backlog (`Fsync`, 4 gRPC workers)

| steps | namespaces | handed to workers per second | runs completed per second |
|---:|---:|---:|---:|
| 1984 | 1 | 40989 | 38254 |
| 1984 | 8 | 36288 | 35533 |
| 1984 | 64 | 39871 | 11898 |
| 19968 | 1 | 143968 | 112028 |
| 19968 | 8 | 127844 | 84826 |
| 19968 | 64 | 93460 | 62167 |
| 99968 | 1 | 175328 | 112527 |
| 99968 | 8 | 146628 | 97213 |
| 99968 | 64 | 91136 | 66293 |
