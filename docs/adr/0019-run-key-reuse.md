# 0019: Run key reuse: return existing or reject now; new runs under a used key later

- **Status:** accepted (the third policy is proposed)
- **Date:** 2026-10-31

## Context

BRIEF.md section 5.3: starting a run with an existing key follows the configured reuse policy (reject,
return existing, allow after completion) and is idempotent. Until now, starting an existing key always
returned the existing run, even with another workflow or another input. A caller that reused a key by
mistake got someone else's result without an error.

## Decision

- `ReusePolicy::ReturnExisting` (the default, `Engine::start`): a start with the same workflow and input as
  the existing run is a retried start and returns it (once its start is durable). A start with another
  workflow or input fails with `KeyConflict`. Starts stay idempotent; mistakes become visible.
- `ReusePolicy::Reject` (`Engine::start_with`): any existing run fails the start with `AlreadyStarted`.
  A caller that retries its own start after a timeout cannot tell whether its first attempt succeeded;
  `ReturnExisting` is the right policy for that.
- Both apply to finished runs that have left memory: the partition looks them up in the checkpoint.

## Proposed: `AllowAfterCompletion`

Starting a key whose run has ended would start a new run under the same key. Today a key names exactly
one run: the hash chain's genesis, the log records, the checkpoint, every API (`wait`, `signal`, `cancel`,
the CLI) and the awakeable ids (ADR 0016) are keyed by it. A new run under a used key therefore needs a
**generation**:

- A run is identified by `(key, generation)`, generation 0 for the first. Its history is bound to both:
  `genesis(key, generation)` (generation 0 keeps today's genesis, so existing histories stay valid).
- The partition keeps the latest generation per key, in the checkpoint and recovered from `Started`
  records in the log. APIs that take a key address its latest run; older runs stay readable and
  exportable by `(key, generation)`.
- Awakeable ids and step idempotency keys include the generation, so a completion or a retried step of an
  old run never reaches a new one.

This changes the history format and the log record format, so it gets its own change and its own
simulation workload (keys reused while old runs' late messages are still in flight).

## Consequences

- `PartitionError` and `EngineError` gain `KeyConflict` and `AlreadyStarted`; `ReusePolicy` is exported
  from the `aqueduct` crate.
- Every start of an existing key compares workflow and input; for a run that has left memory this reads
  and verifies its history from the checkpoint. Starts of new keys cost the same lookup they cost before.
