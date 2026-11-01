# Invariants

What must never break, and what checks it. If a change makes one of these checks fail, the change is wrong
until shown otherwise. Do not weaken, skip or delete a check, an invariant or an injected bug to make a
build pass; if a check is itself wrong, say so in the pull request and fix it in its own commit with the
reasoning.

## Durability

- **Acknowledged work is never lost.** A start, signal, cancellation, resolution or step result is
  acknowledged only after the entries it caused are durable. Messages that depend on entries (dispatches,
  acknowledgements) are released after the sync, never before.
  - Checked by: the simulator (`check_acknowledged` after every crash), `cargo xtask crash` and
    `cargo xtask crash-engine` (real `kill -9`), the injected bug `ack_before_sync`.
- **One writer per data directory.** The file log holds an exclusive lock for its lifetime.
- **Recovery never repairs damage silently.** Only a torn tail of the last segment is truncated; anything
  else is reported as corruption.

## Recording

- **A step's result is recorded at most once.** Steps run at least once; late or duplicate results are
  dropped. Idempotency is the step's responsibility, and the docs say so.
- **Signals, completions and resolutions are recorded once per id.**
- **A command is never dispatched with two different inputs.** Values recorded from outside
  (`ctx.now()`, `ctx.uuid()`, races) must be durable before anything that depends on them is released.
  - Checked by: the simulator's dispatch invariant.

## Determinism

- **The core is pure.** Same state and event, same entries and effects.
- **Replay never diverges silently.** A decider that issues different commands than the journal holds
  parks the run with a reason.
- **Procedures are deterministic by construction**; Rust workflows read time, randomness and the
  environment only through the context.
- **`select` follows the journal's completion order**, never polling order.
- **Simulation seeds replay exactly** (`seeds_reproduce_exactly`).

## The journal

- A run's state is the fold of its journal (`check_fold` in the simulator).
- Hash chains verify; the durable log equals the in-memory journals at quiescence.
- The format in `docs/history-format.md` is what the code writes. Changing either means changing both, with
  an ADR.

## Timers and waits

- Timers never fire early, and keep their wake time across restarts.
- A run that has ended has no pending timer that can affect it.
