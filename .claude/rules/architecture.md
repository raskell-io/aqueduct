# Architecture

How Aqueduct is put together, and where new code belongs. The why is in the ADRs; this is the map.

## Crates

| crate | what it is | depends on |
|---|---|---|
| `aqueduct-core` | The pure state machine: commands, journal entries, `RunState::handle_at(event) -> (entries, effects)`. No I/O, no clock, no randomness, no async. | nothing of ours |
| `aqueduct-codec` | The history format: canonical encoding, the SHA-256 hash chain per run, log records. | core |
| `aqueduct-storage` | The segmented, framed file log (one writer, locked) and the redb state store for checkpoints. | codec |
| `aqueduct-engine` | `Partition` (journals, group commit, timers, retries, checkpoints, outbound messages released only after durability) and the embedded `Engine` (threads, workers, the public API). The `Observer` hook for telemetry. `Replica`: a partition whose log is a Raft group; `placement` and `metadata` for many partitions. | core, codec, storage, raft |
| `aqueduct-procedure` | Starlark procedures as deciders: hermetic, replayed by re-execution, budgeted. | core |
| `aqueduct-sdk` | Rust workflows as `async fn`s: `WorkflowContext`, `join`, `select`, sagas. Each activation re-runs the function against the journal. | core |
| `aqueduct-otel` | OpenTelemetry traces and metrics through the engine's `Observer`. | engine |
| `aqueduct-registry` | Procedure versions: provenance, evidence, approvals, the lifecycle, and the hash-chained log. | engine, procedure |
| `aqueduct-ai` | Agent steps on Claude and OpenAI-compatible endpoints. | engine |
| `aqueduct-protocol` | The protobuf service protocol, and commands to and from it. | core |
| `aqueduct-server` | The ingress, step and workflow workers, imports, tokens, TLS, audit, limits, namespaces. | engine, protocol |
| `aqueduct-worker` | A Rust library for step and workflow workers. | protocol, sdk |
| `aqueduct-cli` | The `aqueduct` command: inspect, verify, export, replay, versions, `server`, `migrate`, `tokens`, `mcp`. Most commands work on a stopped engine's directory. | all of the above |
| `aqueduct-sim` | The deterministic simulator: the real `Partition` on a simulated disk, network, clock and workers, with crashes, torn writes, failed syncs, lost and duplicated messages; with `--replicas`, a partition replicated by Raft (`aqueduct_engine::replica`). | engine, sdk, procedure, raft |
| `aqueduct-raft` | Raft as a pure state machine, and its own deterministic simulator (ADR 0051). The engine's `replica` module pairs it with a partition; the server does not use it yet. | |
| `aqueduct` | The facade users depend on, with the runnable examples. | all of the above |
| `benches/*` | Storage, embedded engine and server benchmarks. Results go to `docs/measurements/`. | |
| `conformance` | The suite every worker library passes, polling and as a push endpoint. | |
| `sdks/*` | Step-worker libraries for Python and TypeScript, with no dependencies. | |
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
