# 0023: Agent steps in the core: journaled responses, model identity, a hard token budget

- **Status:** accepted
- **Date:** 2026-11-01

## Context

Phase 2 (BRIEF.md section 14) puts agent steps in the core: a bounded model call inside a procedure or
workflow, the essay's *locks* in the canal. Section 12 splits it: the step kind, its journal entries,
recorded model identity, budget accounting and enforcement, output schema validation and replay belong in
the core; provider adapters, prompts, caching and streaming belong in `aqueduct-ai`. The phase is done when,
among other things, no recorded agent call is re-issued under crash injection. Until now `agent()` in
procedures was a step named `agent:<name>`, with nowhere to record the model or its cost.

## Decision

- **A command and two entries.** `Command::CallAgent { name, request, budget_tokens }`, journaled as kind
  18 `AgentCallScheduled`; its answer is kind 19 `AgentCallCompleted { cmd, model, params, response,
  input_tokens, output_tokens }`. The model identity (`<provider>/<model>[@<version>]`) and the parameters
  are recorded with every answer, so replay never calls the model again, and model drift can be traced.
  Terminal failures reuse `StepFailed`, and retries reuse the step retry entries: an agent call is
  scheduled, retried, recovered after a restart and cancelled like a step.
- **The budget is a hard limit, enforced in the core.** An answer that used more tokens than its
  `budget_tokens` is recorded as a failure, not as an answer. Executors should cap the model's output to
  stay inside it; the core does not trust them to. The run keeps the total of its answers' tokens
  (`RunState::tokens_used`), the basis for run budgets.
- **Execution is a hook.** The engine dispatches `Outbound::DispatchAgent`; the embedded engine runs it with
  an `AgentExecutor` (`Builder::agent_executor`), which returns `Completed(AgentResponse)`, `Retry` or
  `Fail`. Without an executor, agent calls are retried until one is registered. The engine has no
  provider code.
- **Interfaces:** `ctx.agent::<T, _>(name, &input, &AgentOptions { prompt, output_schema, budget_tokens })`
  in the Rust SDK; `agent(name, input, prompt = ..., output_schema = ..., budget_tokens = ...)` in
  procedures, now a real agent call (a positive budget is required). The request is JSON:
  `{ "input", "prompt", "output_schema" }`, interpreted by the executor.
- **Dispatches are not released for answered calls.** A dispatch waits for its batch to be durable; if a
  late answer of an earlier attempt is recorded meanwhile, the dispatch is dropped instead of released.
  This applies to steps too.

## Consequences

- The simulator's Starlark pipeline makes an agent call per run against a deterministic fake model, under
  crashes, torn writes, lost and duplicated messages and transient failures. A new invariant checks, when
  the node sends each agent dispatch, that the call has no recorded answer. It found the stale dispatch
  above on its first runs (a retry's dispatch released after a late answer was recorded); that case is now
  an injected bug (`release_stale_dispatches`) the simulator must catch. 10,000 seeds pass with about
  56,000 agent calls.
- Agent calls are at least once, like steps: a call whose answer was not yet recorded when the engine
  crashed is made again. Only recorded answers are final.
- Not yet: run-level budgets that suspend and escalate instead of overspending, output schema validation
  (a schema failure as a step failure, then an escalation), model-drift replay, and the provider adapters
  in `aqueduct-ai`. Each comes in its own change.
