# 0024: Run budgets: reserve before calling, escalate instead of overspending

- **Status:** accepted
- **Date:** 2026-11-01

## Context

BRIEF.md section 12: a run that would exceed its budget suspends and escalates for approval instead of
overspending. ADR 0023 gave each agent call a hard token budget and kept a run's total of answered tokens;
a run still had no limit of its own.

## Decision

- **A run may start with a token budget** (`StartOptions { budget_tokens, .. }` on `Engine::start_with`;
  a `ReusePolicy` alone still converts into options). The budget is part of the start record itself:
  `Started` gains `budget_tokens`, encoded as kind 1 without a budget (unchanged bytes, so existing
  histories and hash chains are untouched) and kind 22 with one.
- **Reservation.** An agent call is made only if its own budget fits on top of what the run has committed:
  the tokens of its answers plus the budgets of its calls in flight. Concurrent calls cannot overspend
  together, and no call can exceed what is left.
- **Escalation, in one record.** A call that does not fit is journaled as kind 20 `AgentCallEscalated`:
  scheduled and waiting, not made. It appears among the open escalations with reason `budget` and payload
  `{"needed": ..., "available": ...}`, so `Engine::escalations`, `resolve_escalation` and the CLI need
  nothing new. Resolving it with `true` records kind 21 `BudgetApproved` (the budget raised exactly
  enough for the call) and makes the call; anything else fails the call. A cancellation closes it.
- **Continue-as-new carries the rest.** A run that continues as new starts its successor with what is left
  of its budget, so a loop cannot escape its budget by continuing.

## Consequences

- The simulator found two ways a crash could lose a budget, both from facts split over two records of one
  batch that a torn write can separate: a start kept without its budget (`BudgetSet` after `Started`),
  and a call kept without the escalation that should have held it (`AgentCallScheduled`, then
  `BudgetEscalated`). Both are now single records. The rule, for every future entry: a fact that changes
  how another entry of the same transition must be read is written in the same record. (The torn chain of
  ADR 0022 could be repaired instead, because its second record is fully determined by the first.)
- The simulator's Starlark pipeline starts runs with no budget, an ample one or one too small for its
  agent call; the reviewer approves or declines budget escalations, and the reference model does the same.
  10,000 seeds pass with about 16,000 budget escalations.
- Tokens spent by an answer over its own call budget are not counted in the run's total (the answer is
  recorded as a failure, without its usage). Executors should cap output to the call budget.
- Budgets by procedure version or namespace (policy), and budgets in other units than tokens (money,
  time), are not in this change.
