# Architecture

How Aqueduct is put together, and where new code belongs. The why is in the ADRs; this is the map.

## Crates

| crate | what it is | depends on |
|---|---|---|
| `aqueduct-core` | The pure state machine: commands, journal entries, `RunState::handle_at(event) -> (entries, effects)`. No I/O, no clock, no randomness, no async. | nothing of ours |
| `aqueduct-codec` | The history format: canonical encoding, the SHA-256 hash chain per run, log records. | core |
| `aqueduct-storage` | The segmented, framed file log (one writer, locked) and the redb state store for checkpoints. | codec |
| `aqueduct-engine` | `Partition` (journals, group commit, timers, retries, checkpoints, outbound messages released only after durability) and the embedded `Engine` (threads, workers, the public API). The `Observer` hook for telemetry. | core, codec, storage |
| `aqueduct-procedure` | Starlark procedures as deciders: hermetic, replayed by re-execution, budgeted. | core |
| `aqueduct-sdk` | Rust workflows as `async fn`s: `WorkflowContext`, `join`, `select`, sagas. Each activation re-runs the function against the journal. | core |
| `aqueduct-otel` | OpenTelemetry traces and metrics through the engine's `Observer`. | engine |
| `aqueduct-cli` | The `aqueduct` command: inspect, verify, export, replay, signal, cancel, resolve, on a stopped engine's data directory. | engine, procedure |
| `aqueduct-sim` | The deterministic simulator: the real `Partition` on a simulated disk, network, clock and workers, with crashes, torn writes, failed syncs, lost and duplicated messages. | engine, sdk, procedure |
| `aqueduct` | The facade users depend on, with the runnable examples. | all of the above |
| `benches/*` | Storage and end-to-end engine benchmarks. Results go to `docs/measurements/`. | |
| `xtask` | `cargo xtask sim`, `crash`, `crash-engine`, `fsync`. | |

## How a run flows

1. `Engine::start` reaches the partition thread. `start` produces `Started` and an `Activate` effect.
2. Activation asks the run's decider (procedure or Rust workflow) for every command it has issued, given
   the journal. The core compares them with the journal: new commands become entries
   (`StepScheduled`, `TimerStarted`, ...), a mismatch parks the run.
3. Entries are appended to the batch; effects that release anything (dispatching a step, acknowledging a
   request) wait until the batch is synced (group commit).
4. Workers run steps and report results; results become entries in the order they arrive.
5. After a crash, the partition loads the last checkpoint, replays the log after it, verifies every hash
   chain, and performs recovery effects.

## Where new code belongs

- A new kind of command or journal entry: core (rules and tests), codec (encoding, round trip test),
  `docs/history-format.md`, CLI rendering, the observer in `aqueduct-otel`, an ADR.
- Anything with I/O, time or randomness: never in core. The engine passes it in (`now_ms`, `Env`).
- A user-facing workflow feature: the SDK (and, if procedures need it, `aqueduct-procedure`), an example in
  `crates/aqueduct/examples/` that runs in CI.
- A new failure mode or recovery path: a simulator workload or invariant that would catch it, ideally with
  an injected bug (`aqueduct_sim::Bugs`) and a test that the simulator finds it.
