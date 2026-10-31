# 0014: Rust workflows replay by re-running the `async fn`

- **Status:** accepted
- **Date:** 2026-10-31

## Context

BRIEF.md section 4.2 sketches code workflows as `async fn`s with a context. The engine's model is a
decider: a pure function from what a run has observed to the commands it has issued (ADR 0004). Starlark
procedures already work this way by re-execution (ADR 0008).

## Decision

- `aqueduct_sdk::workflow(f)` turns `async fn(WorkflowContext, I) -> Result<O, WorkflowError>` into a
  decider. Every activation creates a fresh context from the run's recorded outcomes, calls `f`, and polls
  the future once with a no-op waker. Inputs and outputs are JSON (ADR 0010).
- Each context call (`step`, `sleep`, `wait_signal`) takes the next command index the first time it is
  polled. If the journal holds its outcome, it resolves in the same poll; otherwise the command is issued and
  the future stays pending, which ends the activation. `join` and `join_all` poll every branch before
  waiting, so concurrent calls are issued together, in a deterministic order.
- Steps are registered with the engine by name, like procedure steps, rather than written as closures inside
  the workflow (the brief's `ctx.step(name).run(|attempt| ...)`). A closure would only exist while an
  activation runs, so a step re-dispatched after a restart would have no body until the run is activated
  again; registered steps keep dispatch independent of activation and are shared with procedures.
- A step failure surfaces as `WorkflowError::StepFailed`, so a workflow handles it with `?` or matches it. A
  payload that does not decode into the expected type parks the run: it is a code or data change, not a
  business outcome.

## Consequences

- Determinism is checked, not enforced: a workflow that reads the clock, random numbers or the environment
  diverges on replay and is parked. Lints and documentation must make this clear.
- Every activation re-runs the workflow from the start, so cost grows with the square of a run's length, as
  for procedures. Sticky execution (keeping live futures between activations, BRIEF.md 5.1) is a later
  optimisation for the embedded engine.
- The simulator runs the same pipeline as a Rust workflow (`--workload rust`); it passes 10,000 seeds and
  ends with exactly the same outcomes as the Starlark version.
- `ctx.now()`, `ctx.uuid()`, child workflows, sagas, queries and updates are not in the core yet.
