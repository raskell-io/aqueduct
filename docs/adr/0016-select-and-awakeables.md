# 0016: `select` follows the journal's completion order; awakeables are signals with ids

- **Status:** accepted
- **Date:** 2026-10-31

## Context

BRIEF.md phase 1 asks for `select` (wait for the first of several calls) and awakeables (a wait that
someone outside the engine completes by id), and lists "human approval with an awakeable" among the
examples. Both must replay deterministically.

A `select` that picks the first branch that is ready when polled is not deterministic. Say `b` finishes,
the workflow picks `b` and goes on, and then `a` finishes. A replay that finds both finished would pick
`a` and diverge.

## Decision

- **`select(a, b)` and `select_all(futures)`** in the Rust SDK pick the branch whose outcome the journal
  recorded first. The SDK knows each command's position in the completion order (`RunView::completion_order`).
  Every call that resolves notes its rank; `select` polls each branch, takes the latest rank the branch
  consumed as the time it finished, and picks the earliest. A value recorded in the current activation
  (ADR 0015) ranks after everything in the journal, and a branch that consumed nothing is ready at once.
  Ties go to the earlier branch.
- Losing branches are dropped, but their commands stay issued: a losing step still runs, a losing sleep
  still fires, and a losing signal wait still takes the next signal with its name. Cancelling commands is
  a separate, later feature.
- **Awakeables** are signal waits under a reserved name, `$awakeable/<command index>`. Their id is
  `<run key>#<command index>`: it carries everything needed to complete it, so no index is kept anywhere.
  It is also the signal's deduplication id, so completing an awakeable twice records the first payload
  only. `RunView::key()` gives deciders the run key for this. `aqueduct_core::parse_awakeable_id` splits an
  id; the embedded engine has `Engine::complete_awakeable(id, payload)`, and the CLI has
  `complete-awakeable <dir> <id> <json>`.
- `ctx.awakeable()` issues its command when called, not when first polled, because its id must be known
  before the workflow awaits it (to hand it out). Its position is still deterministic: the workflow's code
  decides when it is called.
- The CLI gains `signal <dir> <key> <name> <json> [<id>]` and `complete-awakeable`. Both write to a stopped
  engine's data directory (the log lock, ADR 0009, refuses a running one). The run continues when the
  engine next opens the directory.

## Consequences

- The simulator's Rust workload races two steps with `select_all` and passes the winner to the next step.
  A replay that chose differently would diverge and park the run; 10,000 seeds pass. With the injected bug
  that releases messages before the sync, the simulator now also catches a race decided on results that a
  crash then took back.
- Rejecting an awakeable with an error is not supported yet: a workflow that needs it completes the
  awakeable with a payload that says so.
- `crates/aqueduct/examples/approval.rs` is the human approval example: it starts a run, stops, takes the
  approval from the CLI, and finishes on the next start. CI runs it with `--auto`.
