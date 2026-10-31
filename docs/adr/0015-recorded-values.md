# 0015: Values from outside the logic are recorded commands

- **Status:** accepted
- **Date:** 2026-10-31

## Context

A run's logic must be deterministic (BRIEF.md section 3, principle 3), yet real workflows need the time,
random numbers, identifiers and small reads of the world (a configuration value, an environment variable).
Reading them directly diverges on replay, and the engine parks the run. Steps could carry them, but a step
is dispatched to a worker, retried and acknowledged: far too heavy for reading the clock. BRIEF.md phase 1
asks for `now`, `uuid` and `random` in the Rust SDK.

## Decision

- A new command, `Command::Record { name, value }`, and a new journal entry, kind 13
  `ValueRecorded { cmd, name, value }` (`docs/history-format.md`). It takes the next command index, like any
  command, and is born completed: its outcome is the value.
- The first time the command is issued, the value the logic offers is journaled. When the journal already
  holds the command, the journaled value is its outcome and the offered value is ignored: replay compares
  only the name. Another name, or another kind of command at that index, parks the run.
- The logic may use a new value in the same activation that records it. Because nothing is released before
  the entries of an activation are durable, no step, timer or acknowledgement can depend on a value that a
  crash could take back. The core still asks for one more activation after recording, for logic that waits
  for the outcome instead.
- The engine offers each activation an `Env { now_ms, seed }` through `RunState::view_at`: its clock, and
  64 random bits drawn from a generator per partition. The engine seeds that generator from the operating
  system (through the standard library's `RandomState`, so no new dependency); the simulator sets a fixed
  seed, so a simulation seed still replays exactly. `RunState::view()` offers an empty `Env`, for replay and
  tests, where the values come from the journal.
- The Rust SDK builds on it: `ctx.now()` records the engine's clock, `ctx.random()` 64 bits mixed from the
  seed and the command index, `ctx.uuid()` a version 4 UUID from the same source, and
  `ctx.record(name, || ...)` runs a closure once and records its JSON result. These calls are synchronous:
  their value is known at once, so there is nothing to await.

## Consequences

- Recorded values are not secrets: the seed comes from a non-cryptographic generator, and every value is in
  the journal, which is exported and shared. Tokens and keys belong in steps.
- `ctx.record` is for quick, local, side-effect-free reads. Anything slow, remote or with side effects
  belongs in a step, which has retries, timeouts and idempotency keys.
- The simulator's Rust workload stamps every run with a recorded UUID passed to a step, and a new invariant
  checks that a command is never dispatched with two different inputs and that every dispatch matches the
  final journal. With the injected bug that releases messages before the sync, it finds runs whose stamp
  was dispatched, lost in a crash and recorded again differently.
- Starlark procedures do not have `now()` or `uuid()` yet. They can use the same command when they need it.
