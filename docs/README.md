# Documentation

Start with the [README](../README.md). The [build brief](../BRIEF.md) is the full design and the phased plan.

| | |
|---|---|
| [FAQ](faq.md) | Library or server, whether it is ready, how fast it is, what "exactly once" means here |
| [Service protocol](protocol.md) | The draft protocol: ingress, step workers, workflow workers, imports, deployments. The protobuf in [`proto/`](../proto/aqueduct/v1) is the source of truth |
| [History format](history-format.md) | The journal, specified independently of the code, so that anyone can implement it (CC-BY-4.0) |
| [Measurements](#measurements) | What was measured, on what, and how far to trust it |
| [Decision records](#decision-records) | Why it is the way it is |
| [Brand](brand/README.md) | The mark, the palette and the rules for using them |

Worker libraries: [Rust](../crates/aqueduct-worker/src/lib.rs), [Python](../sdks/python/README.md),
[TypeScript](../sdks/typescript/README.md), and the [conformance suite](../conformance/README.md) they are held to.
To contribute: [CONTRIBUTING](../CONTRIBUTING.md).

## Measurements

| | |
|---|---|
| [Single server, Linux runner](measurements/2026-11-06-server-linux-runner.md) | The same on a hosted Linux runner |
| [Single server](measurements/2026-11-05-server-m4.md) | Starts, tokens and the audit log, overload, workers, draining a backlog |
| [Embedded engine](measurements/2026-10-31-engine-m4.md) | Start to first step, throughput, by durability mode |
| [Storage](measurements/2026-10-31-storage-m4.md) | The plain log, redb and fjall (ADR 0009) |
| [fsync](measurements/2026-10-31-fsync-m4.md) | What a flush costs on the reference machine |
| [Linux runner](measurements/2026-10-31-linux-runner.md) | The same on a CI runner |
| [Crash harness](measurements/2026-10-31-crash-m4.md) | Real `kill -9`s |
| [Fifteen-minute path](measurements/2026-11-01-fifteen-minute-path.md) | Following the README from nothing |

## Decision records

A record says what was true and decided at its date: later records may change what an earlier one says. [Template](adr/0000-template.md).


### Foundations

- [0001: Record architecture decisions](adr/0001-record-architecture-decisions.md)
- [0002: Project name: Aqueduct](adr/0002-project-name.md)
- [0003: Durability baseline and latency targets](adr/0003-durability-and-latency-targets.md)
- [0004: Execution model: commands, completions and deciders](adr/0004-execution-model.md)
- [0005: History format v0 and the hash chain](adr/0005-history-format-v0.md)
- [0006: Deterministic simulation and the async runtime](adr/0006-simulation-and-runtime.md)
- [0007: Licensing, contributions, trademarks and specifications](adr/0007-licensing-and-contributions.md)
- [0009: Local storage: our own segmented log, redb for materialised state](adr/0009-local-storage.md)
- [0010: Payload codec: JSON by default, declared per run](adr/0010-payload-codec.md)
- [0011: Partitioning: fixed hash slots mapped to partitions](adr/0011-partitioning.md)
- [0012: Error taxonomy](adr/0012-error-taxonomy.md)
- [0013: Retries are journaled as durable timers](adr/0013-journaled-retries.md)

### Procedures and workflows

- [0008: Procedures are hermetic Starlark scripts](adr/0008-procedure-language-starlark.md)
- [0014: Rust workflows replay by re-running the `async fn`](adr/0014-rust-sdk.md)
- [0015: Values from outside the logic are recorded commands](adr/0015-recorded-values.md)
- [0016: `select` follows the journal's completion order; awakeables are signals with ids](adr/0016-select-and-awakeables.md)
- [0017: Sagas are compensating steps in the SDK, not a core feature](adr/0017-sagas.md)
- [0018: Cancellation is cooperative: what a run waits for fails, and the run decides](adr/0018-cancellation.md)
- [0019: Run key reuse: return existing or reject now; new runs under a used key later](adr/0019-run-key-reuse.md)
- [0020: Escalations are commands that wait for a resolution](adr/0020-escalations.md)
- [0022: Generations: several runs under one key; continue-as-new](adr/0022-generations-and-continue-as-new.md)
- [0034: Child runs](adr/0034-child-runs.md)
- [0035: `patched`: new code under running runs](adr/0035-patched.md)
- [0036: Queries and updates](adr/0036-queries-and-updates.md)
- [0037: Procedure checks, the watchdog and a frozen module](adr/0037-procedure-checks.md)

### Agents

- [0023: Agent steps in the core: journaled responses, model identity, a hard token budget](adr/0023-agent-steps.md)
- [0024: Run budgets: reserve before calling, escalate instead of overspending](adr/0024-run-budgets.md)
- [0025: Agent answers are checked against their output schema; MIT-0 is allowed](adr/0025-output-schemas.md)
- [0026: Provider adapters in `aqueduct-ai`](adr/0026-provider-adapters.md)
- [0027: Model-drift replay](adr/0027-model-drift-replay.md)
- [0032: The MCP server](adr/0032-mcp-server.md)

### Versions and evidence

- [0028: Counterfactual replay](adr/0028-counterfactual-replay.md)
- [0029: Version pinning](adr/0029-version-pinning.md)
- [0030: The version registry](adr/0030-version-registry.md)
- [0031: Per-version metrics](adr/0031-per-version-metrics.md)
- [0033: Signed approvals](adr/0033-signed-approvals.md)

### Server

- [0038: The single server and protocol v1](adr/0038-server-and-protocol.md)
- [0039: Code workflows over the network](adr/0039-workflow-workers.md)
- [0040: Named task queues and push endpoints](adr/0040-task-queues-and-push.md)
- [0041: Authentication, authorisation and the audit log](adr/0041-authentication.md)
- [0042: TLS on the server](adr/0042-server-tls.md)
- [0043: Virtual objects](adr/0043-virtual-objects.md)
- [0044: Durable outboxes](adr/0044-outboxes.md)
- [0045: Overload protection](adr/0045-overload-protection.md)
- [0046: Namespaces](adr/0046-namespaces.md)
- [0047: Migrating a data directory to a server](adr/0047-migrate.md)
- [0048: Deployments across workers](adr/0048-deployments.md)
- [0049: Per-run priorities](adr/0049-priorities.md)
- [0050: The single-server target, as measured](adr/0050-single-server-target.md)

### Cluster

- [0051: The cluster, and Raft of our own](adr/0051-cluster-and-raft.md)
- [0052: Many partitions, and messages between them](adr/0052-partitions.md)
- [0053: The metadata store, and moving replicas](adr/0053-metadata-store.md)
- [0054: Raft on disk and on the wire](adr/0054-raft-storage-and-wire.md)
- [0055: The engine on a replicated partition](adr/0055-engine-on-raft.md)

### Observability

- [0021: Telemetry through an observer hook; one OpenTelemetry trace per run](adr/0021-opentelemetry.md)
