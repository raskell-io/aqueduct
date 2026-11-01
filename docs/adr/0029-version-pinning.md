# 0029: Version pinning

- **Status:** accepted
- **Date:** 2026-11-01

## Context

BRIEF.md section 4.5: every run is pinned to the version it started on. Registering a new procedure
version or new code creates a new version; old runs finish on the old one, and old versions are drained
and retired. Until now a workflow name mapped to one decider, and a run resumed on whatever was
registered under its name. A changed procedure parked runs that had started on the old one. This ADR is
the first part of the version lifecycle: identity and pinning. The registry of lifecycle states,
provenance, approvals and policies builds on it.

## Decision

- **A version is an id.** For a procedure it is `Procedure::version()`, the lowercase hex SHA-256 of its
  source's exact bytes: the same source is the same version everywhere, and any change is a new one.
  Provenance and approvals are recorded about a version, not inside its id, so approving a version does
  not change it. Code workflows choose their own ids (a build or deployment id).
- **The start records the version, in the same record.** `Started` gains `version: Option<String>`,
  encoded as kinds 24 (pinned) and 25 (pinned, with a budget) next to kinds 1 and 22. As with the budget
  (ADR 0024), a crash can never keep a start and lose its version.
- **The engine registers versions side by side.** `Builder::workflow_version(name, version, decider)`
  registers one; `Builder::active_version(name, version)` chooses the one new runs start on;
  `StartOptions::version` starts a run on another registered version (a canary, a rerun). Unversioned
  `workflow(name, decider)` registrations keep working: their runs are not pinned.
- **A pinned run is only ever decided by its version.** If that version is not registered, the run waits
  (it is not parked, and not decided by another version) until a deployment registers it again. A run
  without a version is decided by the unversioned decider, or by the active version.
- **Continue-as-new moves to the active version.** The successor of a run that continued as new is a new
  run, so it starts on the workflow's active version. That is how a long-lived run moves to new code
  without a patch. A successor repaired after a crash (ADR 0022) also starts on the version active at
  repair; nothing had observed it.
- **Visible everywhere:** `RunResult::version`, `aqueduct describe` and `journal` and `export` show the
  version, and `aqueduct replay` says when the procedure being checked is not the version the run is
  pinned to.
- **The simulator pins every run** and checks that each run, after any crash, is still pinned to its
  version.

## Consequences

- Deploying a new procedure version no longer parks running runs: deploy both, activate the new one, and
  the old runs finish on the old one.
- A deployment that drops a version still in use leaves its runs waiting, visibly, instead of breaking
  them. Knowing when a version may be dropped (no runs left on it, so it is drained) is the registry's
  job, next.
- Activation is fixed when the engine opens. Activating a version at run time, with approvals and
  policies, comes with the registry.
