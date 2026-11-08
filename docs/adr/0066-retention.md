# 0066: Retention: forgetting ended runs

- **Status:** accepted
- **Date:** 2026-11-08

## Context

A server keeps every run it ever ran, so its storage only grows. BRIEF.md section 11 asks for per
namespace retention and archived histories. Archiving (ADR 0061) copies histories out; deleting needs
more. In a cluster, replicas agree by applying the same log: a deletion that is not in the log is undone
by the next replica that rebuilds from it. So a deletion must be an entry, which is a change to the
history format. The owner delegated the decision ("make the most sound and forward looking decision");
this records it.

## Decision

- **A new journal entry, `Forgotten { reason }`** (history format kind 36). It may follow `RunCompleted`,
  `RunFailed` or `ContinuedAsNew`, once, and nothing follows it. A parked run is never forgotten: it may
  still be repaired. Histories without the entry read exactly as before.
- **What forgetting does:** `Partition::forget` appends the entry and drops the run from memory. The next
  checkpoint saves its history ending in `Forgotten`, then removes it from the state store; a store that
  cannot remove keeps a history that reads as forgotten. Loading skips forgotten runs. A replica's next
  snapshot leaves out every record of a forgotten run, so the space comes back in a cluster too.
- **No two chains share a run id.** While a forgotten run's records may still be in the log or the
  store, a new start of its key takes a later generation; once a checkpoint has removed them, the id is
  free again.
- **The policy** is the server's `"retention"` section: `after_ms` (how long after a run ended),
  `every_ms`, and `archive_first` (default true: a run is forgotten only once its archived history is in
  the store, ADR 0061; without an `"archive"` section the configuration is refused). The time is counted
  from when the server leading the partition first saw the run ended, so a run is never forgotten
  sooner than `after_ms` after its end, and after a change of leader possibly later.
- `Engine::forget` and `Shards::forget` forget one run on request.

## Consequences

- Tested: a run forgotten is gone from describe, history and listings, through reopening, with a
  checkpoint after nearly every record and with none; a running run is refused; forgetting twice does
  nothing; a key forgotten and started again in the same stretch of the log reopens correctly (reusing
  the generation, as a deliberate bug, breaks it); a replica's snapshot leaves out a forgotten run's
  records; the server archives a run, then forgets it, and leaves a running run alone; the codec's
  property test covers the entry.
- Not yet: retention per namespace (one policy for the server), and retention in the simulator's
  workloads.
- The entry is part of the history format draft that phase 5 freezes.
