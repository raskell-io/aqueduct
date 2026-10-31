# 0008: Procedures are hermetic Starlark scripts

- **Status:** accepted for phase 2, based on the phase 0 spike in `crates/aqueduct-procedure`
- **Date:** 2026-10-31

## Context

BRIEF.md section 4.4 described procedures as declarative YAML documents with CEL expressions:
deterministic by construction, statically analysable, short enough to approve in one sitting. The owner
decided that procedures need an expressive language: real branching, computation and composition, not
configuration with expressions. The essay's lifecycle (explore, crystallise, prove, approve, run) still
needs procedures that are deterministic, replayable, reviewable and safe to run when written by an agent.

Options considered:

| | Starlark | Rhai | Rune | JavaScript (Boa) | YAML + CEL |
|---|---|---|---|---|---|
| Expressive | yes | yes | yes | yes | no |
| Hermetic and deterministic | by design | by configuration | no (async runtime) | after removing `Date`, `Math.random`, ... | yes |
| Termination | no `while`; see below | operation limits | limits needed | limits needed | yes |
| Written specification, other implementations | yes (Go, Java, Rust) | Rust only | Rust only | yes | yes |
| Agents write it fluently | yes (Python-like) | good | fair | yes | yes |
| Rust crate | `starlark` 0.14, Meta, Apache-2.0 | `rhai` 1.27, MIT/Apache | 0.14, MSRV 1.88 | 0.22, MSRV 1.91 | `cel` crate (unchecked) |

## Decision

1. **Procedures are Starlark modules that define `run(input)`.** They are interpreted by
   `aqueduct-procedure` as a `Decider` (ADR 0004). Inputs and results cross the boundary as JSON.
2. **The interpreter is hermetic:** no `load`, no clock, no randomness, no file system or network, `print`
   discarded. The only effects are engine functions: `step`, `parallel`, `agent`, `escalate` in the spike;
   `wait`, `signal`, `call`, timers and sagas as the core gains them.
3. **Replay is by re-execution.** Each activation runs `run` from the start; the n-th engine call is
   command n. A call with a recorded outcome returns it; a call without one issues the command and suspends
   the script. Determinism therefore holds by construction, and changing a procedure under a running run is
   detected as divergence and parks the run.
4. **Failures:** a failed step fails the run unless called with `fallible = True` (Starlark has no
   exceptions). `fail(...)` fails the run deliberately. Every other script error (type errors, exceeded
   budgets, stack overflow) parks the run through the new `Command::Park`, because it is a bug an operator
   can fix, not a business outcome.
5. **Limits per activation:** a statement budget, a heap limit and a call stack limit.
6. **Metadata** (name, version, origin) is a `PROCEDURE` dict in the module; approvals and replay evidence
   live in a signed record next to the file (phase 2), not in it.

## Evidence from the spike

- The claims procedure from BRIEF.md section 4.4 runs end to end in tests, including the escalation
  paths (`crates/aqueduct-procedure/examples/water_damage_photo_evidence.star`).
- The simulator's pipeline, rewritten as a four-line Starlark procedure, passes the same invariants as the
  native decider over 10,000 seeds with crashes, torn writes, fsync failures and lossy networks (33.6 s,
  versus 14 s native). `cargo xtask sim --workload starlark`.
- Hermeticity is enforced at compile time: `load`, `now()`, `random()` and engine calls outside `run` are
  rejected before a procedure can be registered.
- Activation cost on the M4 (`cargo run --release -p aqueduct-procedure --example activation_cost`): 7 µs
  for one replayed step, 46 µs for 100, 337 µs for 1,000.

## Consequences

- **Termination is not guaranteed by the language in starlark-rust.** It allows recursion, unlike the
  Starlark specification. The statement budget relies on `Evaluator::before_stmt_for_dap`, which is marked
  as debugger API, not public API. Before phase 2 we either get a supported fuel API upstream, or reject
  recursion with our own check on the syntax tree. The budget also does not count work inside a single
  built-in call (for example `sum(range(10**9))`); the heap limit bounds memory but not time, so production
  also needs a wall-clock watchdog whose expiry parks the run. The parking is journaled, so replay stays
  deterministic.
- **Replay cost is quadratic in run length**, because every activation re-runs the script and Starlark
  frames cannot be kept alive between activations (no sticky execution for procedures). Long-running
  procedures must use continue-as-new; the journal size limit (BRIEF.md section 5.4) bounds the worst case.
  The AST could be cached instead of re-parsed on every activation.
- **Static analysis becomes partial.** Checks such as "every wait has a timeout" become lints on the
  syntax tree, and counterfactual replay carries more of the burden of proof before approval.
- **Dependencies:** `starlark` brings about 285 crates, and depends unconditionally on `rustyline` (a
  terminal line editor), which pulls in two Windows-only crates under BSL-1.0 (the Boost Software License,
  permissive and OSI-approved, not the Business Source License). The owner approved BSL-1.0 for exactly
  those two crates on 2026-10-31 (`deny.toml`). Making `rustyline` optional upstream would remove both the
  exception and much of the weight.
- **Unmaintained transitive crates:** `cargo deny` reports four RustSec "unmaintained" notices (not
  vulnerabilities) that come in through `starlark`: `derivative` (RUSTSEC-2024-0388), `fxhash`
  (RUSTSEC-2025-0057), `paste`, and `atomic-polyfill` (RUSTSEC-2023-0089, via `pagable` and `postcard`).
  The owner accepted them on 2026-10-31; they are ignored by ID in `deny.toml`, and remain a signal to
  track upstream health.
- **The API is pre-1.0.** `aqueduct-procedure` is the only crate that touches it, so upgrades stay local.
- Passing per-activation state through `Evaluator::extra` requires `ProvidesStaticType`, whose derive
  generates `unsafe` code that `#![forbid(unsafe_code)]` rejects. The spike uses a thread-local instead,
  which is safe because evaluation is synchronous.
- Starlark's written specification and Go and Java implementations mean a procedure can be read, linted
  and replayed by tools outside this codebase, which supports "histories outlive the engine". Our engine
  functions need their own specification alongside the history format.
