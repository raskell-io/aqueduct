# 0017: Sagas are compensating steps in the SDK, not a core feature

- **Status:** accepted
- **Date:** 2026-10-31

## Context

BRIEF.md phase 1 asks for sagas and an order saga example: a sequence of steps where a failure undoes
the steps already done, in reverse order (refund the card, then release the stock).

## Decision

- A saga is built in the SDK from things the core already has. `ctx.saga()` returns a `Saga`;
  `saga.step(name, input, compensation, compensation_input)` runs a step and, once it succeeds,
  registers its compensation; `saga.compensate_with(compensation, input)` registers one directly (for a step
  that may have had an effect even when it failed); `saga.compensate()` runs the registered compensations
  last first, one after the other; `saga.abort(error)` compensates and returns the error to fail with.
- Compensations are ordinary registered steps. They are journaled, retried by their retry policy, recorded
  once and replayed like any other step, so a crash halfway through undoing resumes the undoing.
- A compensation that fails terminally does not stop the others. The error names the original failure and
  every compensation that failed, so an operator can finish the job by hand.
- Nothing is added to the history format: a saga's journal is the journal of its steps.

## Consequences

- Compensation steps should have generous retry policies; a saga cannot undo a failed undo.
- Compensations run one after the other. Running them concurrently is easy to add (`join_all`) if a use case
  needs it, but reverse order is what most sagas mean.
- The simulator's Rust workload undoes `merge` with `unmerge` whenever validation fails, under crashes,
  retries and duplicated messages; `crates/aqueduct/examples/order_saga.rs` is the brief's order saga and
  runs in CI.
