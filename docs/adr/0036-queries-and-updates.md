# 0036: Queries and updates

- **Status:** accepted
- **Date:** 2026-11-02

## Context

BRIEF.md section 4.1: a **query** is a read-only look at a run's state; an **update** is a synchronous,
validated, journaled request that returns a result. Both are in the phase 2 item with child runs (ADR
0034) and `patched` (ADR 0035), and in the CLI and ingress lists (sections 6 and 11). A run's logic is a
decider: a pure function from the journal to the commands issued so far, run from the start on every
activation. Its state lives in that replay, not in a process that could be asked.

## Decision

### Queries

- **Published state.** The logic publishes values for queries: `publish(name, value)` in a procedure,
  `ctx.publish(name, &value)` in Rust. Publishing is not a command and is not journaled. It can be done
  as often as the state it shows changes.
- **A query replays.** `Decider::query(view, name)` (a default method: no queries) runs the logic over
  the run's journal, as an activation does, and returns the last value published under `name`, or
  `None`. It is pure, and its commands are discarded. A run that has ended is replayed to its end, so it
  can still be queried. A pinned run is queried by its own version, like it is decided.
- **Only durable state is shown.** The embedded engine answers a query after the next flush, so an
  answer never reflects entries a crash could still lose. `Partition::query` reads memory; its callers
  do the same.

### Updates

- **The logic waits for an update** by name, with an optional timeout:
  `request = wait(update("change"), timeout = days(1))` in a procedure, `ctx.wait_update::<T>("change",
  timeout)` in Rust (`Command::AwaitUpdate`, journaled as `UpdateAwaited`, kind 30). The wait's outcome
  is the update's payload, or a timeout (`None`).
- **An update is only accepted while a wait for its name is pending.** It goes to the oldest such wait
  (`UpdateReceived`, kind 31, with the sender's id). Unlike a signal, it is never kept for later: the
  sender is waiting. An update to a run that has ended, or waits for no update of that name, is rejected
  at once.
- **The logic answers it:** `respond(request, result)` or `reject(request, reason)` (`ctx.respond`,
  `ctx.reject`): `Command::Respond`, journaled as `UpdateCompleted` (kind 32) or `UpdateRejected` (kind
  33). These are commands, born completed like recorded values, so replay checks that the logic gives
  the same answer. Answering what is not a received update, or the same update twice, parks the run.
- **Validation leaves no trace.** Before an update is journaled, the partition tries the logic's
  reaction on a copy of the run with the update received. If that reaction rejects the update, nothing
  is journaled and the rejection is the answer: the wait stays open for the next update. Otherwise the
  update and that same reaction are journaled together, so the logic is not asked twice. A rejection
  that comes later (after a step, say) is journaled like any answer. Commands issued in the same
  activation as a rejection on receipt are discarded with it; validation should only look at the update
  and the run's state.
- **Exactly once per id.** A sender gives an update an id; an update whose id the run has already
  received is not delivered again, and gets the first one's answer (or waits for it). Ids of updates
  rejected on receipt are not recorded, so a retry of one is validated again.
- **The sender waits for the answer.** `Engine::update(key, name, id, payload, timeout)` returns the
  answer once it is durable, or `EngineError::UpdateRejected`. If the run ends without answering, the
  update is rejected with that reason. On a timeout the update may still be answered later; sending it
  again with the same id reads the answer. `Partition::update` returns `UpdateStatus::Answered` or
  `UpdateStatus::Accepted { run, cmd }`, whose answer is `RunState::response(cmd)` once it is there.

### Elsewhere

- **CLI:** `aqueduct query <dir> <key> <name>` and `aqueduct update <dir> <key> <name> <json> [<id>]`
  work on a data directory with the registry's procedures. An update that needs work before its answer
  (a step) is accepted; once the engine has run, sending it again with its id prints the answer.
- **MCP:** a read-only `query_run` tool. Updates, like signals, are not exposed to agents yet.
- **Counterfactual replay** delivers the old run's updates in order with its signals; an update the new
  version does not wait for is dropped, as the engine would reject it.
- **The determinism check** (`replay::check`) counts update waits and answers as commands the logic
  wrote.

## Consequences

- A long-running run can show its progress and take changes while it runs, without a step or a signal
  per question, and senders get an answer they can rely on.
- A query costs a replay of the run's journal. Runs that are queried often should keep their journals
  short (continue-as-new).
- Updates and signals cannot be waited for at once yet: a procedure has no `select`, and the Rust SDK's
  `select` is the way to wait for either. A procedure that takes updates "until shipping" loops on its
  update wait, ending it with an update (or a timeout).
- The ingress (HTTP and gRPC) will carry queries and updates with the same semantics when the server
  exists (phase 3).
