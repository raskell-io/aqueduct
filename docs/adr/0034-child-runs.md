# 0034: Child runs

- **Status:** accepted
- **Date:** 2026-11-02

## Context

BRIEF.md sections 3, 4 and 13 (phase 2): procedures call procedures and code workflows, and code
workflows call procedures, as durable child runs whose results are journaled; cancellation propagates to
child runs (section 5.4). The journal table names the entries `CallStarted` and `CallCompleted`.

## Decision

- **A child run is an ordinary run** with its own key, journal, hash chain, version pinning and budget
  (none, for now), started by a command of its parent. Its parent waits for it like for a step: the
  command's outcome is the child's output, or its failure.
- **The command:** `Command::StartChild { workflow, input, key }`. In procedures,
  `call(workflow, input, key = None, fallible = False)`, which fails the run when the child fails unless
  `fallible = True`, like `step`. In the Rust SDK, `ctx.child(workflow, &input)` and
  `ctx.child_with_key(workflow, key, &input)`, failing with `WorkflowError::ChildFailed`. Any registered
  workflow can be a child, whatever it is written in.
- **Its key:** without one, `<parent key>#<command>` (`<parent key>#<generation>.<command>` for a later
  generation), the same form as an awakeable id, read as "the run started by this command". It is unique
  per command, so the child is the parent's alone, and starting it again is idempotent. With a key, the
  child is **shared**: a run the key already has, of the same workflow with the same input, is the child
  (the start's `ReturnExisting` policy), and every parent waiting for the key gets its outcome. A key
  that has a run of another workflow or input fails the call, as does an unknown workflow.
- **Journal entries** (history format kinds 26 to 29): `ChildStarted { cmd, workflow, input, key? }`, a
  command (with a chosen key: its own kind, so the encoding stays canonical);
  `ChildCompleted { cmd, output }` and `ChildFailed { cmd, error }`, its outcomes. The derived key is
  not recorded: it follows from the parent's run id, which the chain is bound to.
- **The partition** keeps an index of which parents wait for which child keys, rebuilt from the
  parents' journals when it loads. A child's end (completed or failed; not continued as new, since its
  chain carries on, and not parked, since it can be repaired) is delivered to the parents waiting for
  its key as `Event::ChildResult`.
- **Crashes.** A parent and its child are separate runs whose records share a batch, so a crash can keep
  some and tear off the rest. Every way is repaired from what survived:
  - the parent's `ChildStarted` survived but the child's start did not: the child's start is an
    effect, and recovery performs it again for every child a running parent still waits for;
  - the child's end survived but the parent's record of it did not: that same start finds the child
    ended and delivers its outcome;
  - cancellation: the effects that cancel the parent's children come before its own reaction, so a
    batch cut after the parent's end has the children's cancellation too; a parent that is still
    running cancels its cancelled children again on recovery (cancelling is idempotent).

  A test recovers from every prefix of the log of a parent with two children, and of a cancellation,
  and checks that the work completes, or that a cancelled parent never leaves its child running.
- **Cancellation** of a parent completes its waiting child commands as cancelled and cancels those
  children, with the reason `its parent run <key> was cancelled`, unless another parent still waits for
  a shared child.
- **A flat stack.** Effects raised while effects are being performed (a child's start, a parent's
  result, a successor's start) are queued and performed in turn by the outermost call, so a deep chain
  of children (a procedure calling itself 200 times, in a test) does not deepen the stack.
- **Counterfactual replay** answers a child run from the old run's child of the same workflow and input
  (or a stub by workflow name); a child the old run never had is unresolved.

## Consequences

- Procedures and code workflows compose in both directions, durably, in one embedded engine.
- Children run in the same partition as their parents (the embedded engine has one). Across partitions
  and servers (phase 3), `StartChild` and `ChildResult` become messages between partitions, with the same
  idempotent start and the same repair rule.
- A child has no budget of its own and does not draw on its parent's; per-namespace budgets (phase 3)
  will cover both.
- The simulator's workloads do not start children yet; the crash-prefix test covers recovery for now.
- Queries, updates and `patched` (the rest of the phase 2 item) are separate decisions.
