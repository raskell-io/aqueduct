# 0022: Generations: several runs under one key; continue-as-new

- **Status:** accepted
- **Date:** 2026-11-01

## Context

BRIEF.md phase 1 asks for continue-as-new: a long-lived process (a loop over days, a subscription, a poller)
ends its run and starts a new one with fresh input, so no journal grows without bound. ADR 0019 proposed a
third reuse policy, a new run under a key whose run has ended, and sketched generations for it. Both need
a key to have more than one run over time.

## Decision

- **Run ids.** A key's runs are its generations, 0 for the first. Generation 0's run id is the key itself;
  generation *n*'s is `<key>\0<n>`. Keys must not be empty or contain NUL (`InvalidKey` otherwise), so ids
  are unambiguous. Everything stored is keyed by run id (log records, hash chains, checkpoints), so
  generation 0 is byte for byte what a run was before, and no index is added: a key's latest generation is
  the highest among its runs in memory and in the checkpoint (`StateStore::generations`, a range scan in
  redb). A running run is always the latest, which keeps the common lookup in memory.
- **Keys and ids.** APIs that take a key mean its **latest** run: `start`, `signal`, `cancel`, `wait`,
  `lookup`. APIs that take an id mean an **exact** run: awakeable and escalation ids are `<key>#<cmd>` for
  generation 0 and `<key>#<n>.<cmd>` after, and step results carry the run id they were dispatched with. A
  completion meant for an earlier generation therefore never reaches a later one; it is dropped, as for any
  ended run.
- **Continue-as-new.** `Command::ContinueAsNew { input }` is terminal and journaled as kind 17
  `ContinuedAsNew { input }`; the run's status is `Continued`. In the same batch, the partition starts the
  key's next generation with the same workflow and `input`. `Engine::wait` on the key follows the chain to
  the run that really ends. The SDK has `return Err(ctx.continue_as_new(&next))`; procedures have
  `continue_as_new(input)`.
- **Recovery repairs a torn chain.** `ContinuedAsNew` and the successor's `Started` are separate records of
  one batch, and a crash can keep the first and tear off the second (nothing was acknowledged, but the
  chain would hang). On resume, a run that continued as new without a successor gets it started again:
  the successor is fully determined by the journaled `ContinuedAsNew`.
- **Reuse policies.** `ReusePolicy::AllowAfterCompletion` starts the next generation when the key's latest
  run has ended (and behaves as `ReturnExisting` while it runs). `ReturnExisting` compares a start against
  the first run of the key's latest chain (runs linked by continue-as-new), so a retried start stays
  idempotent after the run has continued as new any number of times.

## Consequences

- The simulator's Rust workload continues as new once per run, under crashes, torn writes, cancellations
  and duplicated messages; its invariants look at a key's latest run, as a client does. It found the torn
  chain above on the first run, which is now an injected bug (`skip_successor_repair`) it must catch.
- A cancellation recorded while a run has no pending command waits for that run's next command; if the run
  continues as new first, the successor does not inherit it. Runs always have a pending command once
  their workflow is registered, so this only affects runs cancelled before their workflow existed.
- Each generation is its own OpenTelemetry trace (ADR 0021), since traces are derived from run ids.
- The CLI shows a key's latest run; older generations are not yet addressable from the command line.
