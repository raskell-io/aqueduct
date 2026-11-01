# 0028: Counterfactual replay

- **Status:** accepted
- **Date:** 2026-11-01

## Context

BRIEF.md section 5.2, mode 3: replay past runs' inputs and recorded external results against a *new*
procedure or code version, and report where outcomes differ. Steps the old run never called are stubbed
or marked unresolvable. Divergence is the output, not an error. This is the replay report a proposed
procedure version needs before approval. The determinism check (`aqueduct replay`) answers a different
question: whether the *same* history is reproduced; any difference parks it.

## Decision

- **The new version drives a fresh run of the core state machine**, started with the old run's input
  and budget, in memory. Nothing runs for real (no step bodies, no models, no waiting), and nothing is
  written.
- **Commands are answered from the old run's record, by content, not by position:**
  - a step by a recorded result (completed or failed) of a step with the same name and input;
  - an agent call by a recorded answer to the same name and request, the model's or one a person
    supplied, checked against the new call's output schema;
  - an escalation by the resolution of one with the same reason and payload;
  - a budget escalation by whether the old run approved more for the same call.

  Each recorded result answers at most one command, in the old run's order, so a version that calls a
  step twice needs two recorded results. Matching by content means a version that reorders or adds work
  still reuses everything it shares with the old run.
- **Events from outside arrive when nothing else can make progress:** the old run's signals in order,
  then timers (time is skipped), then the old run's cancellation, if it had one.
- **Unanswerable commands** are answered from **stubs** (results by step or agent call name) or left
  **unresolved**. The replay goes as far as it can without them and reports each one.
- **The report per run:**
  - the old and new outcomes: completed, failed, continued as new, parked, or waiting;
  - a verdict: `same`, `different`, or `unresolved` (anything unresolved);
  - the first command where the two command lists differ;
  - what was stubbed;
  - the new journal as it would have been written.
- **The CLI:** `aqueduct counterfactual <dir> <procedure.star> (<key>... | --workflow <name>)
  [--stubs <file.json>]`. Divergence is the output: the command succeeds whatever the verdicts.
- **The library:** `aqueduct_engine::counterfactual::replay_counterfactual(run, journal, decider,
  options)` takes any `Decider`, so Rust workflows can be replayed too, and the version lifecycle and the
  MCP server can build on it.

## Consequences

- A proposed procedure version can be tested against a corpus of past runs, offline, and the runs it
  would decide differently are listed with where they diverge.
- Recorded values are not carried over: the new version sees the old run's first recorded time as its
  clock and a zero seed, so a version whose decisions depend on `now()` or `random()` may diverge for that
  reason alone.
- Signals are delivered in their recorded order but not at their recorded times relative to timers: a
  signal that arrived after a timeout in the old run arrives before it in the replay.
- Exact JSON equality decides `same`; outcomes that should be compared more loosely need a comparison of
  their own on top of the report.
