# 0025: Agent answers are checked against their output schema; MIT-0 is allowed

- **Status:** accepted
- **Date:** 2026-11-01

## Context

BRIEF.md section 12: output schema validation is a core semantic of agent steps; "a schema failure is a
step failure with retry policy, then an escalation". Agent calls (ADR 0023) carry an `output_schema` in
their request, which nothing checked. Checking JSON Schema needs a validator, and the two mature Rust
validators (`jsonschema`, `boon`) both depend on `borrow-or-share`, licensed MIT-0, which was not on the
licence allowlist.

## Decision

- **The engine checks every answer** against the call's output schema (JSON Schema draft 2020-12 by
  default; a schema may declare another draft with `$schema`), with `boon`. The core stays free of JSON;
  it learns only the outcome. Checking is hermetic: the schema is compiled from the request alone, with no
  loaders, so a `$ref` to anything outside it is an error rather than a network or file read.
- **A mismatch is a failed attempt.** It is retried under the call's retry policy (by the call's name),
  like a transient failure, and acknowledged to the worker as a processed attempt, not a recorded result.
- **When the retries are spent, the call escalates** instead of failing: kind 23
  `AgentAnswerEscalated { cmd, error, answer }` holds it, and it appears among the open escalations with
  reason `schema` and payload `{"error": ..., "answer": ...}`. Resolving it supplies the answer to use,
  recorded as `EscalationResolved`; the engine refuses (`InvalidResolution`) a supplied answer that does
  not match the schema either. A late valid answer of an earlier attempt also completes it. A cancellation
  closes it.
- **A schema that does not compile fails the call**: it is a mistake in the workflow or procedure, and no
  answer can fix it.
- **MIT-0 is allowed** (the owner accepted it on 2026-11-01). It is MIT without the requirement to keep the
  copyright notice: strictly more permissive than MIT, OSI-approved, and compatible with Aqueduct's
  Apache-2.0 OR MIT licence. `boon` was chosen over `jsonschema` for its smaller dependency tree.

## Consequences

- Schemas must be self-contained. References like `{"$ref": "schemas/Classification.json"}` (as in the
  brief's example procedure) need a schema registry that ships schemas with a procedure version; until
  then they fail the call. That arrives with procedure versions.
- Tokens spent on answers that did not match are not counted in the run's total, as for answers over
  their call budget (ADR 0023).
- The simulator's Starlark pipeline gives its agent call an output schema and a two-attempt policy; under
  faults the model answers off schema about 30% of the time, and the reviewer supplies the correct answer
  when the retries are spent. 10,000 seeds pass with about 7,700 answers escalated. The invariant "an
  acknowledged resolution is never lost" now tells escalations apart by reason, because one call can
  escalate twice (for its budget, then for its answer).
