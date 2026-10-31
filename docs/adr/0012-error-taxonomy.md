# 0012: Error taxonomy

- **Status:** accepted
- **Date:** 2026-10-31

## Context

A durable execution engine sees errors from user code, from steps, from its own infrastructure and from
clients. Treating them alike either fails runs that should have waited, or retries forever what can never
succeed. The phase 0 code already distinguishes failing a run from parking it (ADR 0004, ADR 0008).

## Decision

The rule: **errors about a run's logic go into its journal; errors about the engine's infrastructure never
do.**

| Class | Examples | What happens | Journaled |
|---|---|---|---|
| **Retryable step error** | timeout, connection refused, HTTP 503, rate limited | retried per the step's policy; when attempts are exhausted it becomes terminal | no (attempts are counted in state) |
| **Terminal step error** | validation rejected, HTTP 4xx, `fallible` marked as final by the worker | `StepFailed` | yes |
| **Run failure** | the logic returns an error, a procedure calls `fail()`, a non-fallible step fails | `RunFailed` | yes |
| **Escalation** (phase 2) | an agent step needs a person, a timeout escalates | `Escalated`, a first-class outcome | yes |
| **Parked run** | non-determinism, a procedure bug, an exceeded budget | `Parked`; an operator fixes and resumes or resets | yes |
| **Node fatal** | fsync failure, I/O error on the log, disk full | the node stops and recovers from the log (ADR 0003); never surfaced as a run failure | no |
| **Corruption** | a hash chain that does not verify after framing succeeded | the partition refuses to start, with a precise report | no |
| **Client error** | not found, already exists (per reuse policy), invalid argument, unauthorised, overloaded with retry-after | returned to the caller, typed | no |

- Step workers classify their errors as retryable or terminal; unclassified errors are retryable.
- In Rust, each crate has typed errors (`thiserror`), and errors that can be retried say so
  (`is_retryable()`), so callers never parse messages.

## Consequences

- An infrastructure outage delays runs but never fails them; only logic and terminal step errors do.
- Parked runs need tooling (phase 1 CLI: list, describe, resume, reset) and an alert, or they accumulate
  silently.
- The protocol (phase 3) carries the retryable or terminal classification explicitly.
