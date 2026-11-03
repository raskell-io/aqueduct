# 0037: Procedure checks, the watchdog and a frozen module

- **Status:** accepted
- **Date:** 2026-11-02

## Context

BRIEF.md section 4.4 and the phase 2 plan: procedures are "linted, then proven". Static checks on the
syntax tree (unbounded waits, missing timeouts, unhandled escalations, recursion) run before
registration, and procedures get a supported termination mechanism, a wall-clock watchdog and AST
caching. ADR 0008 left these open. starlark-rust allows recursion, and the statement budget relies on a
hook marked as debugger API. The budget does not count work inside one built-in call, and every
activation parsed the script and ran its top level again.

## Decision

- **Recursion does not compile.** The check builds a call graph of the procedure's functions (`def`s,
  named and anonymous lambdas) and refuses any cycle, naming it: `` `a` calls `b` (line 2), calls `a`
  (line 5) ``. Functions as values are handled conservatively:
  - a function used other than by calling it by name **escapes** (passed to `sorted`, stored in a dict),
    and so does every anonymous lambda;
  - a call of anything that is not a known function's name (a parameter, `fs[0]()`) may call any
    escaped function.

  Method calls (`name.lower()`) are calls of built-in methods: standard Starlark has no user-defined
  attributes. Without recursion and without `while`, a procedure's loops run over finite collections,
  so every activation ends whatever the statement budget. The budget stays as a second line.
- **Lints are warnings.** The procedure compiles; `Procedure::lints` lists them, and `aqueduct check`,
  `versions register` and the MCP `validate_procedure` tool print them:
  - `unbounded-wait`: `wait(...)` without a timeout;
  - `unused-escalation`: the resolution of `escalate(...)` is not used;
  - `unchecked-failure`: the result of `step` or `call` with `fallible = True` is not used, so its
    failure is ignored;
  - `unanswered-update`: `wait(update(...))` whose request is dropped, or a procedure that waits for
    updates but never responds to or rejects one.

  A lint is silenced on its line with `# starlark-lint-disable <name>` (starlark-rust's suppression
  comment), for a wait that is meant to be unbounded.
- **The watchdog** is a wall-clock limit per activation, `Limits::max_wall_time`, 5 s by default. The
  statement hook checks the clock every 1,024 statements; past the limit, the activation fails and the
  run parks. The parking is journaled, so replay stays deterministic. Simulations can turn it off (`None`)
  to keep only deterministic limits.
- **The module is evaluated once and frozen.** `Procedure::compile` parses the script, runs the checks,
  evaluates the top level (where engine functions are refused), and freezes the module. Each activation
  calls the frozen `run` in a fresh heap. Globals are therefore immutable inside `run`: one activation can
  no longer leave state behind for the next. On the M4, one activation of a procedure with 1 replayed
  step went from 6.9 µs to 1.0 µs, and with 10 steps from 7.2 to 2.2 µs. At 1,000 steps the replay
  itself dominates (151 to 148 µs).

## Consequences

- A registered procedure cannot recurse, and its review shows what a person should look at (waits that
  can last forever, decisions that are ignored).
- The recursion check refuses some programs that would terminate: a function that calls a parameter,
  in a procedure where some function escapes, is assumed to call it. Such procedures are rare; they can
  be written with explicit calls.
- The watchdog still cannot interrupt a single long built-in call (`sum(range(10**9))` runs to its end
  before the next statement); a production engine that must bound that runs activations on a thread it
  can abandon. The heap limit bounds memory.
- Schema lints (checking a step's output schema against how its result is used) are not done; output
  schemas are checked at run time (ADR 0025).
- The statement budget still uses the debugger hook; a supported fuel API upstream would replace it.
