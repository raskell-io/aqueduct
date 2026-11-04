# 0043: Virtual objects

- **Status:** accepted
- **Date:** 2026-11-04

## Context

BRIEF.md section 4.1: a virtual object is keyed, durable state with handlers that run one at a time per
key, and state changes commit atomically with the handler's completion. Section 10.1 adds the invariant:
object state per key equals the sequential application of its committed handlers.

## Decision

Objects are built from runs, so they add no journal entry kinds and inherit durability, hash chains,
recovery and retries.

- **An object key is a long-lived run** of a built-in decider, `ObjectLoop`, under the reserved key
  `$object/<object>/<key>`.
- **Its inbox is that run's signals named `invoke`:** durable, in arrival order, deduplicated by id.
- **A call is a child run** of the workflow `<object>.<handler>` under `$call/<object>/<key>/<id>`. The
  loop starts it only after the previous call has ended, so calls to a key run one at a time. Different
  keys run concurrently.
- **Reads:** a handler's input is `{"args": ..., "state": {...}}`, the state as committed when it starts.
- **Writes:** a handler's output is `{"output": ..., "writes": {...}}`, where `null` removes a name. That
  output is one `RunCompleted` record, so writes commit atomically with completion. A failed handler has
  no such output, and commits nothing.
- **The state is the fold of those outputs** over the loop's journal.
- **Compaction:** after 64 calls the loop continues as new with the state as its input. It does so only
  with an empty inbox, because signals kept by a generation do not reach the next, and only at the end
  of its journal, because the inbox's size is not part of what replay sees (`RunView::buffered_signals`
  documents this).
- **Exactly once per id.** `Engine::call_object(object, key, handler, id, args, timeout)` waits for the
  call's run. A call whose run exists is not delivered again; one still in the inbox is deduplicated as a
  signal. Compaction keeps this true: it happens only when the inbox is empty, so every earlier call
  already has its run. An empty id makes a new call every time.
- **API:** `aqueduct_sdk::object_handler(f)` and `ObjectContext` (`get`, `set`, `clear`, and through
  `Deref` everything a workflow can do), registered with `Builder::object_handler`. `Engine::object_state`
  reads the committed state; `$object/` and `$call/` keys cannot be started or signalled directly.
- **A handler is any workflow that follows this input and output.** The engine's tests use a Starlark
  procedure.

## Consequences

- **Tested:** calls run one at a time per key; a failed handler commits nothing; compaction with the
  inbox full and empty; recovery from prefixes of the log (every one near the start, the end, and on a
  stride between), past a compaction, with the same outputs and state as without a crash. Removing either
  compaction condition fails the tests.
- **A handler's journal copies the object's state** into its input, so large objects make every call
  larger. Keep object state small; keep large data elsewhere and store its reference.
- **A stuck handler blocks its key.** A parked handler, or one that sleeps or waits for a signal, holds up
  later calls to that key until it ends. Callers time out and can retry with their id.
- **A handler removed from the deployment** after its call entered the inbox fails to start: its call has
  no run, and its caller times out. A call to an unknown handler is refused up front.
- **Cancelling a loop run ends the object key.** Later calls to it are dropped until they time out.
- **Not done yet:**
  - durable outboxes: a handler sending messages atomically with its commit;
  - objects over the network: HTTP and gRPC routes, remote handlers, and registering them in
    `aqueduct server`, which so far means objects work only in a program that embeds the engine;
  - object scenarios in the conformance suite and the simulator's workloads.
