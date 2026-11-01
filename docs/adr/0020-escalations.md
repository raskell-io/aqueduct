# 0020: Escalations are commands that wait for a resolution

- **Status:** accepted
- **Date:** 2026-10-31

## Context

BRIEF.md section 4.1: an escalation is a first-class outcome, not an error, that routes a run, or a
decision within it, to an agent or a person, and is counted per procedure version. Section 4.3 lists the
journal entries `Escalated { cmd, reason, payload }` and `EscalationResolved { cmd, outcome }`. The
phase 0 spike ended a procedure with a failure (`escalated: <reason>`) instead, and the Rust SDK had no
escalations at all.

## Decision

- **A new command, `Command::Escalate { reason, payload }`, journaled as kind 15 `Escalated`.** It takes
  the next command index and waits, like a signal wait without a timeout, for kind 16
  `EscalationResolved { cmd, outcome }`. Nothing is dispatched: an escalation is for whoever looks at the
  open escalations. Replay compares the reason and the payload; another reason parks the run.
- **Resolving:** `Event::ResolveEscalation { cmd, outcome }`. Only the first resolution counts; a second,
  or one for a command that is not an open escalation, is dropped (and still acknowledged). A
  cancellation (ADR 0018) closes an open escalation like any other waiting command.
- **Ids:** `<run key>#<command index>`, the same form as awakeable ids (ADR 0016);
  `aqueduct_core::parse_command_id` splits them.
- **Interfaces:** `RunState::open_escalations`, `Partition::open_escalations` and
  `Partition::resolve_escalation` (acknowledged with `Outbound::Resolved`); `Engine::escalations()`
  lists them across runs and `Engine::resolve_escalation(id, outcome)` resolves one, once durable; the CLI
  has `escalations <dir>` and `resolve <dir> <id> <json>`.
- **SDK:** `ctx.escalate::<T, _>(reason, &payload)` returns the decoded resolution. **Procedures:**
  `escalate(reason, payload)` now waits and returns the resolution, instead of failing the run; a
  procedure that wants to stop does so explicitly (`fail("declined: " + decision)`).

## Consequences

- Open escalations are found by scanning runs in memory. That is enough for the embedded engine, where
  every running run is in memory; a server needs an index (phase 3).
- Counting escalations per reason and per procedure version is a query over journals for now; metrics
  come with OpenTelemetry.
- The claims procedure (`water_damage_photo_evidence.star`) now waits for a person on both of its
  escalations and settles or declines on their decision. The reference ingest pipeline
  (`crates/aqueduct/examples/ingest.rs`, run in CI) escalates when a review times out and continues with
  the operator's resolution.
- The simulator exercises escalations: its timers workload escalates a review after the `go` signal, and the
  simulated reviewer polls the open escalations and resolves each (about one in four declined) over the
  lossy network, retrying until acknowledged. Invariants: an acknowledged resolution is never lost, and every
  run ends as the reference, which resolves the same way, says it should. With the injected bug that
  releases messages before the sync, it finds lost resolutions.
