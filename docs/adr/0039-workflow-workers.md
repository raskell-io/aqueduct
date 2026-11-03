# 0039: Code workflows over the network

- **Status:** accepted (the protocol is a draft until phase 5)
- **Date:** 2026-11-03

## Context

BRIEF.md section 8: the service protocol carries, for code workflows, activations (journal entries to
replay, new events) and commands. A code workflow's logic is a decider: a pure function from the run's
journal to every command it has issued. In the embedded engine and the server so far, the partition
calls it synchronously. A workflow written in a program of its own (the Rust SDK in another process,
later other languages) cannot be called that way: the call crosses a network, can be lost, late or
repeated, and must not stall the partition.

## Decision

- **Remote workflows.** A workflow registered with `Builder::remote_workflow` (`Deciders::insert_remote`)
  is decided by a **workflow worker**. Where the partition would call the decider, it emits an
  **activation** instead (`Outbound::Activation`). Like every outbound message, it is released only
  after the flush, so a worker only ever sees durable history.
- **An activation** carries:
  - the run id and the workflow;
  - `at`, the number of journal entries the decision is for;
  - those entries as **history-format records, hash-chained to the run id** (`docs/history-format.md`):
    the worker verifies the chain before deciding, so a garbled or misrouted history is never decided;
  - the clock and seed for values the logic records (as `Env` gives an embedded decider).
- **The worker** replays the history through the workflow's logic and answers with a **decision**: every
  command the logic has issued, in order, in the protocol's `WorkflowCommand` form. The partition checks
  it against the journal exactly as it checks a local decider's commands; non-determinism parks the run
  as before.
- **Stale decisions are dropped.** `Partition::decision(run, at, commands)` applies a decision only if
  the run's journal still has exactly `at` entries. Otherwise the run has moved on (or the decision is a
  repeat), and the partition activates it again for where it is now. Applied late, an older decision
  would issue fewer commands than the journal has and park the run; a test checks that it is dropped.
- **The activation queue** (`aqueduct_engine::activations`) holds at most one activation per run (a
  newer one replaces an older one) and leases each to one worker. One whose decision does not come back
  in time, or whose worker's session ends, is offered again. Once a run's activation is decided, the ids
  of its older ones are forgotten, so most stale decisions never reach the partition.
- **The transport** is `workflow.proto`'s `Workflows.Serve`: one stream per worker, with a hello naming
  its workflows and a capacity, activations within its credit, decisions and credit back. The session
  rules are the step workers': a session ends when either stream closes, and its leases are released.
  A worker that cannot decide an activation (the history does not verify, the logic panics) returns its
  credit without a decision.
- **`aqueduct-worker`** serves workflows as well as steps: `Worker::workflow(name, logic)` takes any
  `Decider`, so a Rust SDK workflow (`aqueduct_sdk::workflow(f)`) or a procedure runs in the worker's
  process unchanged.
- **Not yet for remote workflows:**
  - queries: they need the logic, and are refused as unsupported;
  - validation of updates on receipt: an update is journaled, and a rejection by the logic is recorded
    like any answer;
  - HTTP long polling for workflow workers.

## Consequences

- A code workflow can run in its own program, deployed and scaled apart from the server. The server
  keeps only journals, timers and queues.
- Every activation sends the whole history, so the cost of a long run's activations grows with its
  length. Caching histories in workers (sending only what is new) is an optimisation for later.
- A deterministic simulation drives a partition and its activation queue with workers that lose
  decisions, answer late or twice, and answer old activations after newer ones, and a server that
  crashes and loses the queue: over 1,000 seeds every run completes with the right output, and none
  parks.
- The conformance suite runs its scenarios with their workflow logic in a remote `aqueduct-worker`,
  besides in process.
