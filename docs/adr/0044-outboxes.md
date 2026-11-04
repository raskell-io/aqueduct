# 0044: Durable outboxes

- **Status:** accepted
- **Date:** 2026-11-04

## Context

BRIEF.md section 6.2: a change of state and the message it causes must commit together, and the message
must arrive exactly once. Applications otherwise hand-write outbox tables for this. In the cluster
(phase 4), the same mechanism carries messages between partitions.

## Decision

- **`Command::Send`** calls a virtual object's handler without waiting for it. It is born completed, and
  journaled as `MessageSent` (history format kind 34). In Rust: `ctx.send(object, key, handler, &args)`; in
  procedures: `send(object, key, handler, args)`.
- **Inside a handler, messages commit with its writes.** `ObjectContext::send` puts them in the handler's
  output (`"sends"`), in the same record as its writes. A failed handler sends nothing. The object's loop
  issues them as commands when it folds that output. A handler written as a procedure returns `"sends"`;
  the `send` function there is immediate, not atomic with its writes.
- **Delivery is an effect, and is exactly once.** It puts the call in the target key's inbox, starting
  the key's loop if needed, under an id derived from the sender's run and command, so repeating it does
  nothing (ADR 0043).
- **A run's end is never logged before its messages,** because a crash can keep a prefix of a batch and
  an ended run is not recovered:
  - delivery comes before any activation the same decision asks for;
  - a decision that sends and ends the run ends it only in the next decision, after delivery;
  - recovery delivers a running run's messages again, before activating it.
- **A target that is not deployed** is not an error. The message stays undelivered, and recovery tries
  again while the sender is running.

This also puts objects on the network:

- **`aqueduct server` configuration:** `"objects"` and `"remote_workflows"`. A handler is a workflow named
  `<object>.<handler>`: an approved registry version, or a remote workflow served by `aqueduct-worker`.
- **Routes:** `CallObject` and `GetObjectState` over gRPC, and `POST /v1/objects/{object}/{key}/{handler}`
  and `GET /v1/objects/{object}/{key}` over HTTP. Calling needs the `signal` permission, reading `read`.
  Calls are recorded in the audit log.

## Consequences

- **Tested:** recovery from every prefix of the log of a workflow and a handler that send. If the sender's
  end survived, so did its message, and resending the same requests delivers each message exactly once.
  Planting each of three ordering bugs fails the tests: not holding back the end, not redelivering after
  a restart, and delivering after an activation.
- **Messages are one-way.** A sender gets no answer and no error. A handler that fails, or a call to a
  handler its target does not have, shows only in the target's runs and journal.
- **A message to a target not deployed is lost if its sender ends first:** nothing is left to deliver it
  again once the run has ended, and nothing reports the loss. Deploy targets before senders. A test covers
  delivery after redeploying while the sender still runs.
- **Messages go only to objects' handlers,** not to arbitrary runs' signals.
- **Everything is still one partition.** Delivery between partitions is phase 4; this is the same contract.
- Not yet in the conformance suite or the simulator's workloads.
