# 0035: `patched`: new code under running runs

- **Status:** accepted
- **Date:** 2026-11-02

## Context

BRIEF.md section 4.5: for long-running code workflows that must move to new code, an explicit patch API
(`if ctx.patched("add-review-step") { ... }`) recorded in the journal, plus a replay compatibility check.
Version pinning (ADR 0029) is the default: a pinned run is only ever decided by its own version, and new
runs start on the new one. A patch is for the runs that cannot wait to drain: an unversioned code
workflow redeployed, or a version whose runs live for months.

## Decision

- **`patched(id)`** returns whether the run takes the patch's new code. It looks at the next command
  index in the run's journal:
  - nothing there: the run has not reached this point, so it takes the new code, and the choice is
    journaled as a marker command;
  - the patch's marker: it took the new code before, and does again;
  - another command: the run passed this point with the old code. `patched` returns false and issues
    nothing, so the old code's commands keep their indexes and the journal replays.
- **The marker** is a recorded value (kind 13) named `$patch/<id>` with the value `true`, a reserved
  name, as awakeables reserve `$awakeable/` signal names. No new entry kind; the history format reserves
  the prefix. The core offers `RunView::patched` and `RunView::patch_at` and `patch_marker`; the Rust SDK
  `ctx.patched(id)`, procedures `patched(id)`.
- **Removing the old code:** once no run on the old path is left (none whose journal lacks the marker
  where it would be), the `else` branch can be deleted. The `patched` call stays as long as runs that took
  the new code may replay, since their journals hold the marker.
- **The replay compatibility check** moves from the CLI into the engine: `replay::check(key, journal,
  decider)` replays a journal against any decider and reports the first divergence, before every entry
  the logic wrote and at the end. `aqueduct replay` uses it for procedures; tests use it for code
  workflows, over a corpus of real histories. The list of entries the logic writes now includes recorded
  values, escalations, child starts and continue-as-new, which the CLI's check had missed.

## Consequences

- A long-running workflow can change course without parking its runs: an old run keeps to the path it
  was on, every other run takes the new one, and which one a run took is in its journal.
- Deployed without a patch, the same change parks the old runs (non-determinism), as before; the check
  shows that before deployment.
- Procedures are pinned by their source hash, so a changed procedure is a new version; `patched` matters
  for them only where a version is replaced under its runs.
- Counterfactual replay runs new code from a fresh journal, so it always takes a patch's new path: what
  the new code would have done, which is what a replay report is for.
- Corpus files: `aqueduct export` writes journals as JSON Lines; reading them back into a test (rather
  than from a data directory) is still to do.
