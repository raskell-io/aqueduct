# 0013: Retries are journaled as durable timers

- **Status:** accepted; supersedes the "Retryable step error" row of ADR 0012
- **Date:** 2026-10-31

## Context

ADR 0012 said retryable step errors are retried per policy and are not journaled, with attempts counted in
state. With timers in the core (PR #8), a retry is just a durable timer for a step, and keeping attempts
only in memory would lose the backoff and the attempt count on every crash.

## Decision

- A retryable failure of a step attempt records `StepRetryScheduled { cmd, attempt, error, wake_at }`
  (history format kind 12). When its timer fires (`TimerFired`), the step is dispatched again. The attempt
  count is the number of such entries.
- The engine owns the policy (`RetryPolicy`: maximum attempts, exponential backoff with a cap), per step
  name with a default. The core only records what the engine decided: another attempt after a backoff, or
  a terminal `StepFailed` once attempts run out.
- Workers classify each attempt as `Completed`, `Retry` or `Fail` (`Attempt` in the embedded engine);
  unclassified failures, a missing step implementation and a panicking step are retryable.
- The engine acknowledges retryable reports separately (`Outbound::AttemptRecorded`) from outcomes
  (`Outbound::ResultRecorded`), so a sender always knows which report was processed.

## Consequences

- Backoff and attempt counts survive restarts; every attempt's error is visible in the history, which
  auditors and debugging both want.
- The journal grows by one entry per failed attempt plus one per retry timer, bounded by the policy.
- Reports carry no attempt number yet, so a delayed duplicate report of an earlier failed attempt can count
  as one more failure. Adding an attempt number to dispatches and reports would remove this.
- The simulator injects transient failures (10 percent of executions while faults are on) with an unlimited
  policy; about 29,500 retries per 10,000 seeds pass every invariant.
