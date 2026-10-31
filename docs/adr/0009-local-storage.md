# 0009: Local storage: our own segmented log, redb for materialised state

- **Status:** accepted
- **Date:** 2026-10-31

## Context

BRIEF.md section 6.3 asked phase 0 to benchmark redb (B-tree) and fjall (LSM tree) on the journal append and
state read workloads, and to use one local engine for both the log and the state. The benchmark
(`benches/storage`, results in
[docs/measurements/2026-10-31-storage-m4.md](../measurements/2026-10-31-storage-m4.md)) included a third
candidate: a plain append-only file of records framed with length and CRC-32, which we already specified
and implemented for recovery (`aqueduct-storage::frame`, `docs/history-format.md` section 6).

What the engine needs from local storage:

- **The log** is the source of truth: append-only, read sequentially on recovery, durable at every
  acknowledgement (group commit, ADR 0003), trimmed from the front after snapshots.
- **The state** (runs, timers, idempotency, object state, inboxes) is materialised from the log: point
  reads and transactional updates, never the only copy of anything.

## Options

1. **redb for both.** Single file, ACID, stable format, fast to reopen, fast point reads. Slowest for large
   durable batches and without fsync; about 1.7x space amplification.
2. **fjall for both.** Fast writes and compact (LZ4), but reopening replays its journal (about 1 to 1.5 s for
   200,000 records), and its background flush and compaction threads are harder to bring under
   deterministic simulation.
3. **Our own log plus an engine for state.** The log as segmented framed files; state in redb or fjall,
   committed without fsync and rebuilt from the log after a crash.

## Decision

Option 3, with redb for state:

- **The log** is a sequence of segment files of framed log records (`docs/history-format.md` section 6),
  written with group commit and synced with `sync_data` (`F_FULLFSYNC` on macOS, `fdatasync` on Linux).
  Recovery scans the last segment and truncates a torn tail; older segments are deleted after a snapshot
  covers them. It matched or beat both engines in every append measurement, and its format is ours to keep
  readable for decades.
- **The state** lives in redb, written with `Durability::None` and made durable periodically by a
  checkpoint that records the log position it reflects. After a crash, the engine opens redb (milliseconds),
  reads the checkpoint, and replays the log from there. State never needs an fsync per acknowledgement.
- This replaces "one local storage engine for both log and state" in BRIEF.md section 6.3.

## Consequences

- We own the log's correctness: segmenting, torn-tail recovery and trimming. The simulator already tests
  framing and recovery under torn and corrupted writes; the real-disk crash harness (BRIEF.md section 10.2)
  starts with this file log.
- Two files to keep consistent: the checkpoint in redb must never claim a log position that is not
  durable. The simulator gets a state store with lagging durability and a crash invariant for this.
- redb 4.4 and fjall 3.1 need Rust 1.90, so the workspace MSRV rises from 1.85 to 1.90 (released about a
  year before this decision).
- fjall remains the candidate if state turns out to be write-heavy enough for redb's write amplification to
  matter; the `StateStore` trait keeps that change local.
- Linux numbers are still missing; the CI probe (`.github/workflows/probe.yml`) gives a first, not
  representative, data point from a GitHub runner. The cluster reference machine must be measured before
  phase 4 targets are set.
- One writer per data directory: the file log takes an exclusive lock (`log/LOCK`, `File::try_lock`,
  stable since Rust 1.89) before reading anything, and holds it until it is closed; the operating system
  releases it when the process dies. Added after noticing that a second opener (a second engine, or the CLI)
  would have truncated the first writer's write in progress as if it were a torn tail, before redb's own
  lock refused it.
