# 0004: Execution model: commands, completions and deciders

- **Status:** accepted
- **Date:** 2026-10-31

## Context

A run's logic (a code workflow or an interpreted procedure) issues commands: schedule a step, start a timer,
complete the run. The outside world answers with completions. With concurrency inside a run (two steps in
flight at once), the order in which completions arrive is not deterministic, while the order in which the
logic issues commands is. Matching replay "by journal index" (BRIEF.md, original section 5.1) breaks as soon
as two steps run concurrently.

## Decision

1. **Commands get a command index (`cmd`) in issue order.** The index is deterministic across replays.
   Completions refer to their command by index and are journaled in the order they actually arrive.
2. **The run's logic is a decider:** a pure function from what the run has observed (its input and the
   outcomes recorded so far) to the full list of commands the run has issued, in issue order. A code
   workflow becomes a decider by replaying its future against the journal; a procedure interpreter is a
   decider directly.
3. **Replay matches commands, not entries.** On each activation the core compares the decider's commands with
   the journaled ones by command index. Matching commands produce nothing new; new commands at the end are
   journaled and dispatched; any mismatch (different kind, name or input, fewer commands than journaled, a
   terminal command that is not last) **parks** the run with a `Parked` entry. Parking is never silent and
   never fails the run.
4. **The state machine is a fold.** `RunState` is defined as the fold of its journal
   (`RunState::from_journal`). `RunState::handle(event)` is pure and returns the entries to append and the
   effects to perform; the caller appends the entries (applying them to the state) and performs the effects
   only after the entries are durable. Therefore state always equals the fold of the durable journal plus
   the not-yet-durable tail, and recovery is just a fold.
5. **Completions are recorded at most once.** A completion for a command that already has an outcome, an
   unknown command, or a run that has ended produces no entry. Steps may execute many times; their result is
   recorded once.
6. **Recovery is derived, not stored.** `RunState::recovery_effects` re-derives what must happen after a
   restart (activate the run; re-dispatch steps without an outcome). Activations are idempotent because of
   rule 3.
7. **Idempotency keys** are `run_key/epoch/cmd`. The epoch increments on operator reset (later phase), so a
   re-issued command after a reset never reuses an old key.

## Consequences

- `join`, `select` and races between a step and a timer are deterministic on replay, as long as the logic
  derives its decisions from outcomes by command index rather than from arrival order. Code that branches on
  arrival order (for example, `select`) must have that order recorded; `select` will get its own journal
  entry when timers land in phase 1.
- Because deciders return the full command list on every activation, activation cost grows with the length
  of the run. Sticky execution (keeping live futures) avoids this in the common case; continue-as-new bounds it.
- Comparing step input as well as name is stricter than Temporal. It catches more divergence and may park
  runs whose code changed only cosmetically; revisit with real workloads.
