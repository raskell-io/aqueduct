# 0049: Per-run priorities

- **Status:** accepted
- **Date:** 2026-11-06

## Context

BRIEF.md section 6.6 asks for priorities beside concurrency limits, rate limits and fair scheduling. A server takes steps
and activations in the order they were offered, within a namespace (ADR 0046). An urgent run waits behind every routine
run started before it. A priority has to survive a restart, a crash and a migration, like the rest of a run.

## Options

1. **A journal entry.** Durable, in the hash chain, moves with `aqueduct migrate`, and can change while the run runs. It
   changes the history format, which needs the owner's approval.
2. **A priority per workflow or namespace, in configuration.** No format change, but one urgent run cannot be raised.
3. **A side table in the state store.** Per run, but outside the history: not verified, not exported, lost by a migration.

The owner chose the journal entry (2026-11-06).

## Decision

**`PrioritySet { priority }` is entry kind 35**, additive: a reader that does not know it rejects it, as for any
unknown kind. It may occur anywhere after `Started` while the run has not ended; the last one counts, and before any the
priority is 0. It is not a command, and the run's logic never sees it, so a priority never changes what a run decides or
how it replays.

**Setting it:**

- `priority` on a start (`StartOptions::priority`, `"priority"` over HTTP, `--priority` on `aqueduct start`), journaled
  in the same batch as the start, so a run is never acknowledged without it.
- Starting a key that has a running run sets that run's priority. A start retried after its first attempt was lost
  before it was durable therefore converges.
- `SetPriority` (`POST /v1/runs/{key}/priority`, `Engine::set_priority`) on a running run, with the `start` permission,
  in the caller's namespace, audited. On a run that has ended it does nothing.
- **Inherited:** a child run starts with its parent's priority, and a successor (continue-as-new, including one
  repaired after a crash) with its predecessor's.

**Scheduling:**

- **Higher goes first:** a worker is given the highest priority among the waiting steps and activations of a namespace,
  and among equals the one offered first.
- **Nothing starves:** waiting raises an item's priority by one every 10 seconds (`queue::AGING_MS`). Work of priority 0
  that has waited a minute ranks with fresh work of priority 6.
- **Changing a priority moves waiting work at once,** keeping how long it has waited.
- **Priorities do not cross namespaces.** Stride scheduling between namespaces (ADR 0046) is unchanged, so one tenant's
  urgent work takes no share from another.

## Consequences

- An urgent run no longer waits behind a backlog. A backlog of priority 0 is still served, at least one level per 10
  seconds, so its wait is bounded by how much higher the urgent work is.
- **The history format changed:** kind 35. Version 0 is still a draft, so readers of other implementations must add it.
  Histories without priorities are unchanged.
- **Local steps are not prioritized.** An embedded engine's own step pool runs steps in order; priorities apply to steps
  taken by remote workers, and to activations taken by workflow workers.
- **Agent calls are not prioritized** either: they run on the engine's agent executor in order.
- **Aging is fixed at 10 seconds per level,** and not configurable. Any `u32` is accepted, so a very high priority
  outranks old work for a long time: at 1,000, about 2 hours and 47 minutes.
- **No per-tenant cap on priorities:** within a namespace anyone with `start` can raise their runs above others'.
- **Not simulated:** tested on the queues, the engine and the server; the simulator's workloads use priority 0.

## Alternatives considered

- **Strict priority without aging.** Simple, but a steady stream of urgent work would starve the rest for ever.
- **Priority classes (high, normal, low).** Easier to explain, but they would still need an order within a class, and a
  number is what the journal records anyway.
