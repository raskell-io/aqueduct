# 0030: The version registry

- **Status:** accepted
- **Date:** 2026-11-01

## Context

BRIEF.md section 4.5: versions move through `draft -> proposed -> proven -> approved -> active ->
draining -> retired`; a policy per namespace decides which transitions need which evidence; provenance
and approvals are stored with the version and recorded as journaled, signed events in a registry. Version
pinning (ADR 0029) gives versions identity; counterfactual replay (ADR 0028) gives the evidence. This ADR
connects them.

## Decision

- **A registry per data directory**, in `registry/` (the `aqueduct-registry` crate):
  - `sources/<id>.star`: each version's source, named by its id (its SHA-256), checked against it
    whenever it is read;
  - `events.log`: the lifecycle as an append-only log, one JSON event per line. Each event names who did
    it and when, and is chained to the one before:
    `hash = SHA-256(previous hash ":" event JSON)`, stored in front of the JSON. Opening the registry
    verifies the chain, so an edited, reordered or removed event is detected, and replays it to the
    versions' states;
  - `policy.json`: the policy, written by hand.
- **Transitions are checked when they are made**, against the state they start from and the policy, and
  again when the log is replayed (states only, since the policy may have changed since).
  - `register` (draft) needs a source that compiles and a provenance with an author: a person or an
    agent, the agent that explored the cases, and how many cases were studied.
  - `propose`.
  - `prove` replays every past run of the workflow in the data directory against the version and records
    the counts and the SHA-256 of the full report. A workflow's first version has no past runs, and its
    report says so.
  - `approve` records one person's approval; once the approvals meet the policy, the version is
    approved.
  - `activate` makes an approved version active and moves the previous active one to draining. A
    draining version can be activated again, as a rollback.
  - `retire` is allowed for any version that is not active, once no unfinished run is pinned to it.
- **The policy**, per workflow with a default:
  - `require_replay` (default true);
  - `min_runs` (0);
  - `max_unresolved` (0);
  - `max_different` (unlimited: a new version usually means to decide some cases differently, and the
    report shows which);
  - `approvals` (1);
  - `approvers` (anyone);
  - `author_may_approve` (false: four eyes).

  Namespaces do not exist yet; when they do, the policy moves to them.
- **Deployment:** `Registry::deploy(builder)` registers the approved, active and draining versions with an
  engine and activates the active one. Approved versions are there for canaries (`StartOptions::version`),
  and draining ones so their runs can finish. Activation takes effect when an engine next opens
  (ADR 0029).
- **The CLI:** `aqueduct versions list | show | register | propose | prove | approve | activate | retire
  | diff | log`. Every change takes `--by <name>`, and versions can be named by a prefix of their id.
  `list` shows, per version, how many runs are pinned to it and how many of those are unfinished, so a
  draining version that never drains (a zombie) is visible. `diff` is a line diff of two versions'
  sources.

## Consequences

- A version goes from an agent's draft to production with its provenance, the replay report that proves
  it, and the approvals that let it through, all in one verified log next to the runs it decides.
- **Not signed yet.** Events are attributed (`by`) and tamper-evident (hash-chained), but a name is not a
  signature: anyone who can write to the directory can append an event in someone else's name, and a
  rewritten chain is undetectable without an outside anchor. Signing approvals with a person's key, and
  an identity to check it against, is the next step; the event format leaves room for a signature field.
- The registry is single-writer, like the rest of the data directory: the CLI changes it while the engine
  is stopped, or between deployments.
- Per-version metrics (escalation rate, cost, divergence, runs per version) build on the pinned versions
  next.
