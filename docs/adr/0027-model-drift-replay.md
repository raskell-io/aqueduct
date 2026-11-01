# 0027: Model-drift replay

- **Status:** accepted
- **Date:** 2026-11-01

## Context

BRIEF.md section 5.2 names four replay modes. The fourth, model-drift replay, re-runs the agent steps of
past runs against a new model or prompt, live, and compares the outputs with the recorded ones under the
output schema; it should run whenever a model changes, not only when a procedure does. Agent calls record
their request (input, prompt, output schema), their budget and their answer with the model that gave it
(ADR 0023), and answers are checked against their schema (ADR 0025), so everything needed is in the
journal.

## Decision

- **Read, ask, compare; write nothing.** `aqueduct_engine::drift::agent_calls` reads a journal's agent
  calls with what each came to: the model's answer, an answer a person supplied after a schema escalation,
  a failure, or nothing yet. `replay_agent_calls` asks each again through any `AgentExecutor` and returns a
  `DriftReport`. The runs are not changed, and no journal entry is written: drift replay is a
  measurement, not recovery.
- **The new model or prompt is the executor.** The replay sends each call's recorded request unchanged;
  which model answers, and with which prompt text, is how the executor is configured (an `AiExecutor` with
  another model, or other prompts under the same names). One mechanism covers both, and any provider.
- **Verdicts.** Each call is `same` (the same JSON value as the answer the run continued with), `changed`
  (a different answer that matches the schema), `new` (a valid answer where the run recorded none),
  `invalid` (the answer does not match the schema), `failed` (the call failed, after its retries, or its
  answer cost more than the call's budget, as the core would record it) or `skipped`. A report *holds*
  when every call is `same` or `new`. Divergence is the output, not an error: the command succeeds either
  way.
- **Spending is capped up front.** Calls cost real tokens. A replay takes a call limit and a token limit;
  each call reserves its full budget before it is made, so the limit is never exceeded, and a call that
  does not fit is skipped. The CLI requires `--max-tokens`.
- **The CLI:** `aqueduct drift <dir> --model <provider>/<model> --max-tokens <n> [<key>...]`, with
  `anthropic/…` (the Claude API), `ollama/…`, or any OpenAI-compatible endpoint with `--base-url`.
  `--prompts <dir>` reads each call's prompt from a file named after it, so a prompt change is tested by
  pointing at the new files. One line per call: its id, name, verdict, the recorded model, and the
  answers.
- **Fixed alongside:** procedure replay (`aqueduct replay`) now also checks the procedure's decision
  before an agent call is journaled, not only before steps, timers and waits.

## Consequences

- A model upgrade can be checked against real past cases before it is rolled out, with a known worst-case
  cost.
- Exact JSON equality is strict: free-text answers will mostly be `changed`. Answers that should be
  compared by meaning need a schema that pins the parts that matter (an enum, a label), or a semantic
  comparison, which is not here.
- The report is a library value (`DriftReport`), so the MCP server and the version lifecycle can use it:
  a procedure version's approval can require a drift report that holds.
- Calls are made one at a time; a large corpus is slow but predictable. Parallelism can come later
  without changing the report.
