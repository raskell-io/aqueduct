# 0018: Cancellation is cooperative: what a run waits for fails, and the run decides

- **Status:** accepted
- **Date:** 2026-10-31

## Context

BRIEF.md phase 1 asks for cancellation. A run must be stoppable from outside (a customer withdraws an
order, an operator gives up on a stuck process), and a losing branch of `select` (ADR 0016) should not
have to run to its end. Killing a run outright is easy but leaves its work half done: a held room stays
held, a charged card stays charged. Sagas (ADR 0017) exist to undo exactly that.

## Decision

- **One entry, kind 14 `CancelRequested { reason }`.** It completes every command without an outcome
  (steps, including steps waiting to retry, sleeps, signal waits) as failed with `cancelled: <reason>` and
  marks them cancelled. Late step results and timers for them are dropped, as for any completed command.
  If nothing is waiting, the next waiting command the run issues takes the cancellation; recorded values
  (ADR 0015) are not waits and are never cancelled.
- **At most once per run.** Later cancellations, and cancellations of ended runs, are acknowledged and
  dropped. This is what makes undoing possible: after the cancellation, the run's new commands run
  normally.
- **The run decides.** In the Rust SDK the waiting call returns `WorkflowError::Cancelled(reason)`; the
  workflow typically lets it propagate and undoes its work with `saga.abort(error)`, failing with
  `cancelled: <reason>`. `ctx.sleep` now returns `Result<(), WorkflowError>`, because a sleep can be
  cancelled. Saga compensations cannot be cancelled: one that the cancellation reaches is issued again.
  Starlark procedures cannot catch errors, so a procedure fails with `cancelled: <reason>` at the first
  call the cancellation completed, after issuing the rest of a `parallel` so its commands still match the
  journal.
- **Interfaces:** `Event::Cancel`, `Partition::cancel` (acknowledged with `Outbound::Cancelled`),
  `Engine::cancel(key, reason)` (returns once durable), and `aqueduct cancel <dir> <key> [<reason>]` for a
  stopped engine's data directory.

## Consequences

- Cancellation is cooperative: a workflow that discards a call's error (for example, the losing result of
  a `select_all`) can carry on as if nothing happened. The SDK documentation says so; the simulator's
  workload was itself fixed for this.
- The simulator's client cancels every third run at a random moment, retrying until acknowledged. New
  invariants: an acknowledged cancellation is never lost, and a cancelled run fails with the reason, or,
  when the cancellation only reached a compensation, ends as the reference says. It found three bugs before
  this was merged: `parallel` in procedures stopped issuing at the first cancelled call (and so diverged
  from its journal), the workload above swallowed cancellations, and a cancellation could skip a saga's
  compensation.
- Cancelling a step does not stop a worker already executing it; its result is dropped. Interrupting
  running steps (and passing cancellation to child runs) comes with the protocol in phase 3.
- A hard stop ("terminate": end the run now, undo nothing) is not provided; it is easy to add when an
  operator needs it.
