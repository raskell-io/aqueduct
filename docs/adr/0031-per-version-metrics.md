# 0031: Per-version metrics

- **Status:** accepted
- **Date:** 2026-11-01

## Context

BRIEF.md sections 3 and 11: escalations, timeouts, cost and divergence are tracked per procedure
version. That means escalation rate, timeout rate, cost per run (tokens and compute), divergence on
replay, time to repair, and runs per version, the last to find zombies. Versions are pinned (ADR 0029)
and registered (ADR 0030), so every run knows its version.

## Decision

- **Computed from the record.** `aqueduct_engine::metrics::version_metrics(runs)` folds runs and their
  journals into `VersionMetrics` per workflow and version (unversioned runs under the workflow alone).
  It counts:
  - runs, by status (running, completed, failed, parked, continued), and cancelled runs;
  - runs that escalated, and escalations by reason (`budget` and `schema` included);
  - runs in which a signal wait timed out;
  - steps, retried attempts, agent calls, and tokens in and out.

  The rates follow from these counts. Because the metrics come from the journals, they cover every run
  the data directory holds, including runs from before any collector was attached.
- **Live, too.** The `Observer::entry` hook now receives the run's version, and `aqueduct-otel` adds
  `aqueduct.version` (the first twelve characters of the id) to its run metrics: started, ended,
  escalations and signals. This is a breaking change to the hook, which is acceptable before 1.0.
- **The CLI:** `aqueduct metrics <dir> [<workflow>]` prints each version's figures. Where the data
  directory has a registry, it also prints the version's lifecycle state and the divergence its replay
  found (same, different, unresolved). A draining version whose runs never finish shows as `running`
  under a `draining` state.
- **Parked runs stand for divergence in production.** They are runs whose logic diverged from their
  journal; divergence before production is the replay evidence.

## Consequences

- **Time to repair is not measured.** Journal entries carry no timestamps (only the wake times of
  timers), so the time from an escalation to its resolution, or from a park to its fix, cannot be read
  from the record. Live telemetry has the times. Adding a timestamp to entries is a format change, to be
  decided separately.
- "Compute" is counted in steps and retried attempts, not CPU time; step durations are in the live
  `aqueduct.step.duration` histogram.
