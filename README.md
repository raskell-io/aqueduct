<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/brand/aqueduct-lockup-light.svg">
    <img alt="aqueduct" src="docs/brand/aqueduct-lockup-dark.svg" width="480">
  </picture>
</h1>

<p align="center">
  <strong>A durable execution engine for procedures that agents write and people approve.</strong><br>
  It runs inside your program as a library, or as a server that workers in any language connect to.
  Every run leaves a history you can replay and audit.
</p>

> **Status: single server (phase 3 of the [build brief](BRIEF.md)).** The embedded engine, procedures, agent steps
> and the version registry are built. So is a server with workers in Rust, Python and TypeScript, deployments,
> priorities, tokens, TLS, an audit log, namespaces and overload protection: every item of phase 3. Work on a
> replicated cluster has begun (phase 4, [ADR 0051](docs/adr/0051-cluster-and-raft.md)): its Raft and a replicated partition
> run in a simulator; the server does not use them yet.
>
> You can try it, but do not depend on it yet: the APIs and the history format will still change, and nothing
> is published on crates.io. See [what is not done](#what-is-not-done), the [docs](docs/README.md) and the
> [FAQ](docs/faq.md).

## The idea

Agents are good at exploring: unfamiliar systems, ambiguous documents, cases nobody has seen before.
Much of the work organisations do is the opposite: the same case, handled the same way, every time, with a
record anyone can check afterwards. Aqueduct is built for the hand-over between the two:

1. Agents explore new kinds of work.
2. When work recurs, an agent writes it down as a **procedure**: short, typed, readable.
3. The procedure is **replayed against past cases** before anyone trusts it.
4. A named person **approves** it.
5. Aqueduct **runs it deterministically**, with bounded, budgeted **agent steps** where judgement is
   genuinely needed, and journals everything.
6. Escalations and drift send it back to an agent for **repair**, or it is **retired**.

Engineers who need full expressive power write **code workflows** in Rust instead; both share one engine,
one journal format and one set of guarantees.

## Where it runs

| mode | what runs | who it suits | state |
|---|---|---|---|
| **Embedded** | the engine, inside your program (`aqueduct` crate) | CLIs, desktop apps, single binary services, tests | built |
| **Single server** | `aqueduct server`, with workers or push endpoints | small teams, departments, staging | built |
| **Cluster** | several servers, replicated | production platforms | started: Raft and a replicated partition, simulated |

An embedded directory and a server's are the same format, and
`aqueduct migrate --from ./data --to https://server` moves one to a running server
([ADR 0047](docs/adr/0047-migrate.md)). It does not need a server to start with:
[it is closer to SQLite than to Tokio](docs/faq.md#can-i-use-aqueduct-as-a-library-like-tokio).

## Quick start: a library

Aqueduct is not on crates.io yet; depend on the repository (and on `serde_json`, which the example uses
to read and write step payloads):

```sh
cargo new hello-aqueduct && cd hello-aqueduct
cargo add aqueduct --git https://github.com/raskell-io/aqueduct
cargo add serde_json
```

A workflow is an ordinary `async fn`. Everything it does with the world goes through its context, so the
engine can journal it and replay it after a crash:

```rust
use std::sync::Arc;
use std::time::Duration;
use aqueduct::embedded::{Attempt, Engine};
use aqueduct::{Payload, WorkflowContext, WorkflowError, workflow};

async fn onboard(ctx: WorkflowContext, user: String) -> Result<String, WorkflowError> {
    let account: String = ctx.step("create_account", &user).await?; // runs at least once, recorded once
    ctx.sleep(Duration::from_secs(5)).await?;                        // durable: survives restarts
    ctx.step("send_welcome", &account).await
}

let engine = Engine::builder("./quickstart-data")
    .workflow("onboard", Arc::new(workflow(onboard)))
    .step("create_account", |user: &Payload| Attempt::Completed(/* ... */))
    .step("send_welcome", |account: &Payload| Attempt::Completed(/* ... */))
    .open()?;
engine.start("onboard:ada", "onboard", Payload::new(br#""ada""#.to_vec()))?; // idempotent per key
let result = engine.wait("onboard:ada", Duration::from_secs(60))?;
```

The complete program is [`crates/aqueduct/examples/quickstart.rs`](crates/aqueduct/examples/quickstart.rs):
copy it to `src/main.rs`. Run it, kill it during the five second sleep (Ctrl-C, or `kill -9`), and run it
again: the run continues where it stopped, and the sleep ends at its original time.

```sh
cargo run    # in a clone of this repository: cargo run -p aqueduct --example quickstart
```

The `aqueduct` command, used below, inspects and manages a data directory:
`cargo install --git https://github.com/raskell-io/aqueduct aqueduct-cli`.

Waiting for people works the same way. [`approval.rs`](crates/aqueduct/examples/approval.rs) asks for
approval through an **awakeable**, an id that anyone outside the engine can complete, and gives up after
a day with `select` ([ADR 0016](docs/adr/0016-select-and-awakeables.md)):

```rust
let (id, decision) = ctx.awakeable::<bool>();
ctx.step::<(), _>("ask_approver", &id).await?;            // hand the id to a person or a webhook
let approved = match select(decision, ctx.sleep(Duration::from_secs(86_400))).await {
    Either::Left(decision) => decision?,
    Either::Right(slept) => slept.map(|()| false)?,        // nobody answered within a day
};
```

Complete it from code with `engine.complete_awakeable(&id, payload)`, or from the command line while
the engine is stopped: `aqueduct complete-awakeable approval-data 'expense:42#0' true`.

Undoing work is a **saga**: each step registers a compensation, and a failure undoes the completed steps
in reverse order ([`order_saga.rs`](crates/aqueduct/examples/order_saga.rs),
[ADR 0017](docs/adr/0017-sagas.md)):

```rust
let mut saga = ctx.saga();
let shipped = async {
    saga.step::<(), _, _>("reserve_stock", &item, "release_stock", &item).await?;
    saga.step::<String, _, _>("charge_card", &id, "refund_card", &id).await?;
    ctx.step::<String, _>("book_shipment", &address).await
}
.await;
match shipped {
    Ok(tracking) => Ok(tracking),
    Err(error) => Err(saga.abort(error).await), // refund, then release
}
```

**Cancelling** a run (`engine.cancel(key, reason)`, or `aqueduct cancel` on a stopped engine's data
directory) makes whatever it is waiting for fail with `WorkflowError::Cancelled`, so the same saga undoes
the work and the run fails with the reason ([ADR 0018](docs/adr/0018-cancellation.md)).

## Procedures

The same engine runs **procedures**: hermetic Starlark scripts that an agent can write and a person can
review ([ADR 0008](docs/adr/0008-procedure-language-starlark.md)):

```python
def run(claim):
    docs = agent("classify_documents", claim["documents"], prompt = "prompts/classify.md", budget_tokens = 20000)
    if docs["missing_photos"]:
        step("notify.customer", {"template": "missing-photos", "to": claim["customer"]})
        photos = wait(signal("photos_received"), timeout = days(14))
        if photos == None:
            return escalate("no-photos", claim)
    return step("claims.settle", claim)
```

Register it with `.workflow("claims", Arc::new(aqueduct::Procedure::compile("claims.star", source)?))`.
`aqueduct check claims.star` reports its lints (a wait without a timeout, a result ignored); recursion
does not compile, so every activation ends ([ADR 0037](docs/adr/0037-procedure-checks.md)).
`escalate` hands the claim to a person and waits, durably, for their decision; list open escalations
with `engine.escalations()` or `aqueduct escalations`, and resolve them with `resolve_escalation`
([ADR 0020](docs/adr/0020-escalations.md)). [`ingest.rs`](crates/aqueduct/examples/ingest.rs) is the
reference pipeline with all of it: concurrent steps, a review through an awakeable with a deadline, and
an escalation when nobody reviews in time.

## From agents to procedures

Two examples show the path from agents to procedures, with a stand-in model so they need no API key:

- [`crystallise.rs`](crates/aqueduct/examples/crystallise.rs): an agent triages claims; then, through
  the MCP server, it writes a procedure from what it did (rules for the clear cases, the agent only for
  the rest), proposes it and replays it against its past runs. The report shows the one claim it got
  wrong; a person approves the version with a signature, and new claims cost a third of the tokens.
- [`bounded_agent.rs`](crates/aqueduct/examples/bounded_agent.rs): one agent step under a token budget
  and an output schema; a run whose budget is too small escalates for more, an answer that does not
  match the schema goes to a person, and a restart makes no model call again.

A version moves through a lifecycle before it runs: registered with its provenance, proposed, proven by replay
against past runs, approved under a policy (four eyes by default, and signed with an Ed25519 key if the policy says
so), activated, drained and retired, in a hash-chained log (`aqueduct versions ...`;
[ADR 0030](docs/adr/0030-version-registry.md), [ADR 0033](docs/adr/0033-signed-approvals.md)). Agents take part
through `aqueduct mcp <dir> --as <agent>`, which serves the Model Context Protocol: they validate, replay and propose
versions, start and inspect runs, and resolve escalations. Approval stays with people
([ADR 0032](docs/adr/0032-mcp-server.md)).

## Quick start: a server

A procedure has to be approved before a server will run it. `greet.star`:

```python
def run(name):
    return step("greet", name)
```

```sh
aqueduct check greet.star
aqueduct versions register ./data greet greet.star --by ada     # prints the version id; a prefix of it will do
aqueduct versions propose  ./data greet <version> --by ada
aqueduct versions prove    ./data greet <version> --by ada      # replays past runs: this one has none
aqueduct versions approve  ./data greet <version> --by grace    # four eyes: not the author
aqueduct versions activate ./data greet <version> --by grace
aqueduct server ./data                                          # 127.0.0.1:7470, gRPC and HTTP/JSON
```

Steps run on **workers**: processes in any language that say which steps they implement and run what they are
sent. In Python, with [`sdks/python`](sdks/python/README.md) from this repository on `PYTHONPATH`:

```python
from aqueduct_worker import Worker

worker = Worker("greeter", url="http://127.0.0.1:7470")

@worker.step("greet")
def greet(name, invocation):
    return f"hello {name}"

worker.run()
```

```sh
curl -s localhost:7470/v1/runs -d '{"key": "greet-1", "workflow": "greet", "input": "Ada"}'
curl -s 'localhost:7470/v1/runs/greet-1/result?timeout_ms=10000'
# {"error":"","output":"hello Ada","status":"completed","version":"c98c962e..."}
```

A worker holds each step under a lease. If it crashes or its result is lost, the step goes to another worker, and the
run records one result.

### What the server has

| | | |
|---|---|---|
| **Protocol** | gRPC and HTTP/JSON, draft | [protocol](docs/protocol.md), [ADR 0038](docs/adr/0038-server-and-protocol.md) |
| **Step workers** | [Rust](crates/aqueduct-worker/src/lib.rs), [Python](sdks/python/README.md) (standard library only), [TypeScript](sdks/typescript/README.md) (no dependencies), all held to a [conformance suite](conformance/README.md) | [ADR 0038](docs/adr/0038-server-and-protocol.md) |
| **Workflow workers** | Rust workflows' logic in another process; the server sends each activation the run's verified history | [ADR 0039](docs/adr/0039-workflow-workers.md) |
| **Deployments** | a worker announces its build; new runs start on the active one and stay pinned to it, only that build's workers decide them, and a build drains before it is retired | [ADR 0048](docs/adr/0048-deployments.md) |
| **Task queues, push endpoints** | steps grouped into named queues, or posted to an HTTP endpoint for serverless platforms | [ADR 0040](docs/adr/0040-task-queues-and-push.md) |
| **Tokens, audit log** | `aqueduct tokens create`; each route needs a permission; a hash-chained log of who did what, and of every refusal | [ADR 0041](docs/adr/0041-authentication.md) |
| **TLS** | `--tls-cert`, `--tls-key`; beyond loopback it needs tokens and a certificate | [ADR 0042](docs/adr/0042-server-tls.md) |
| **Overload protection** | rate limits, shedding, connection deadlines, a cap on running runs; refusals carry a retry hint | [ADR 0045](docs/adr/0045-overload-protection.md) |
| **Namespaces** | a token bound to a namespace reaches only its keys, runs, objects and steps, with its own quotas and share of a shared worker pool | [ADR 0046](docs/adr/0046-namespaces.md) |
| **Migration** | an embedded directory's runs, histories, versions and object state move to a server | [ADR 0047](docs/adr/0047-migrate.md) |

### How fast

On an Apple M4 with `F_FULLFSYNC` on every commit, on a machine that was not idle, with clients on the same machine
and steps that do nothing ([measurements](docs/measurements/2026-11-05-server-m4.md), which say how far to trust them):

| | |
|---|---|
| run starts per second over gRPC, 256 clients, no tokens | 12,500 to 16,700 (target: 10,000) |
| the same, with tokens and the audit log | 8,300 to 10,300: about the target, by load ([ADR 0050](docs/adr/0050-single-server-target.md)) |
| the same, one client | about 200, or 120 with tokens: each waits for one flush, or two |
| start to first step at a worker, p50, one client | about 4.5 ms |

On a GitHub-hosted Linux runner, where a sync costs about 0.18 ms rather than 4 ms, the same server starts 17,200 runs a
second, and 14,400 with tokens and the audit log ([measurement](docs/measurements/2026-11-06-server-linux-runner.md)).
The embedded engine's numbers are in [its own measurements](docs/measurements/2026-10-31-engine-m4.md). TLS is not
measured.

## Guarantees

What holds today, and what checks it. The simulator runs 18,000 seeds on every change; the crash harness sends real
`kill -9`s to the log and to the embedded engine.

| guarantee | checked by |
|---|---|
| **Acknowledged work is never lost.** A start, signal or step result is acknowledged only once durable. | simulator (crashes, torn writes, failed fsyncs); crash harness, including during first start |
| **A step's result is recorded exactly once,** though a step may run more than once, so steps with side effects should be idempotent. | simulator; conformance suite for workers |
| **Starts are idempotent per key.** A retried start returns the same run; a used key with another workflow or input is refused ([ADR 0019](docs/adr/0019-run-key-reuse.md)). | simulator, crash harness |
| **Determinism.** Procedures are deterministic by construction. Rust workflows are checked by replay, and a run whose code changed under it is parked, never silently corrupted. | 1,000+ seeds per workload; replay tests |
| **Timers never fire early,** and keep their wake time across restarts. | simulator (`timers` workload) |
| **Signals are recorded once per signal id.** | simulator, tests |
| **Runs are pinned to their version.** New runs start on the active version, and old runs finish on theirs ([ADR 0029](docs/adr/0029-version-pinning.md)). | tests |
| **Histories are tamper-evident:** a SHA-256 chain per run, specified independently of the code ([history format](docs/history-format.md)). | property tests; every load verifies |
| **A recorded model call is never made again.** Replay reads the answer, model and cost from the journal ([ADR 0023](docs/adr/0023-agent-steps.md)). | simulator, tests |
| **An audit line is synced before its request is answered,** and a changed line breaks the chain ([ADR 0041](docs/adr/0041-authentication.md)). | tests |

The simulator's `composite` workload covers child runs, updates and objects (messages arrive exactly once); it has no
updates waited for twice, so their deduplication is checked by tests only. The crash harness runs against the embedded
engine, not the server, whose transports have tests of their own, which are narrower.

## What the engine does

| | |
|---|---|
| **Durable waits** | timers, signals, awakeables with `select`, escalations to a person ([ADR 0016](docs/adr/0016-select-and-awakeables.md), [ADR 0020](docs/adr/0020-escalations.md)) |
| **Long-lived runs** | continue-as-new, and keys reused as generations ([ADR 0022](docs/adr/0022-generations-and-continue-as-new.md)) |
| **Priorities** | an urgent run's steps and activations go first within its namespace, waiting work ages so nothing starves, and children and successors inherit it ([ADR 0049](docs/adr/0049-priorities.md)) |
| **Recorded values** | `ctx.now()`, `ctx.random()`, `ctx.uuid()`: recorded once, read back on replay ([ADR 0015](docs/adr/0015-recorded-values.md)) |
| **Child runs** | `call("payment", input)`, `ctx.child("payment", &input)`; cancelling a parent cancels its children ([ADR 0034](docs/adr/0034-child-runs.md)) |
| **Virtual objects** | keyed state, handlers run one at a time per key, state changes commit with completion ([ADR 0043](docs/adr/0043-virtual-objects.md)); messages between objects commit with the writes and arrive once ([ADR 0044](docs/adr/0044-outboxes.md)) |
| **Queries and updates** | read a run's published state without journaling; a request whose sender waits for the run's answer ([ADR 0036](docs/adr/0036-queries-and-updates.md)) |
| **Changing code under running runs** | `ctx.patched("add-review")` keeps runs that passed that point on the old code ([ADR 0035](docs/adr/0035-patched.md)) |
| **Agent steps** | a hard token budget per call and per run, output schemas, escalation instead of overspending ([ADR 0023](docs/adr/0023-agent-steps.md), [ADR 0024](docs/adr/0024-run-budgets.md), [ADR 0025](docs/adr/0025-output-schemas.md)); Claude or any OpenAI-compatible endpoint ([ADR 0026](docs/adr/0026-provider-adapters.md)) |
| **Replay against the past** | counterfactual replay of a new version over past runs ([ADR 0028](docs/adr/0028-counterfactual-replay.md)); model-drift replay when a model changes ([ADR 0027](docs/adr/0027-model-drift-replay.md)) |
| **Observability** | OpenTelemetry traces and metrics ([ADR 0021](docs/adr/0021-opentelemetry.md)); per-version runs, escalations, timeouts, cost and replay (`aqueduct metrics`; [ADR 0031](docs/adr/0031-per-version-metrics.md)) |

## What is not done

- **A cluster:** no replication. A single server does not survive losing its disk.
- **Stable formats:** the protocol and the history format are drafts until phase 5.
- **Packages:** nothing on crates.io, PyPI or npm. Depend on the repository.
- **Identity beyond static tokens:** no OIDC or JWT, and no mutual TLS.
- **Version management over the network:** `aqueduct versions` works on the server's data directory, and a change
  takes effect when the server next starts.
- **Isolation of code and disk between tenants:** namespaces separate runs and data, not workflow definitions, and
  they share one partition ([ADR 0046](docs/adr/0046-namespaces.md)).
- **Workflow workers** have no HTTP long polling or push, and do not answer queries.
- **Measurements:** none on a quiet reference machine, with TLS, with real payloads, or with long runs.

## Testing it yourself

```sh
mise run ci                                      # everything CI checks, one after the other
cargo xtask sim --seeds 10000                    # deterministic simulation: crashes, torn writes, fsync failures, lossy networks
cargo xtask sim --seeds 2000 --workload timers   # workloads: native, starlark, timers, rust, composite
cargo xtask sim --seed 42 --workload rust        # reproduce one seed exactly
cargo xtask sim --seeds 5000 --replicas 3       # a partition replicated by Raft, under the same faults
cargo xtask raft --seeds 20000                   # Raft groups: partitions, crashes, clock skew and pauses, lossy networks
cargo xtask crash --kills 1000                   # SIGKILL a log writer; no acknowledged record may be lost
cargo xtask crash-engine --kills 200             # SIGKILL the engine mid-run; every acknowledged run must end correctly
cargo xtask crash-engine --startup --kills 300   # SIGKILL brand-new engines while they create their files
mise run conformance                             # worker libraries: Rust, Python and TypeScript, polling and push
mise run bench:server                            # the server under load
cargo xtask fsync                                # fsync latency and group commit throughput on this machine
```

## Inspecting a data directory

With the engine or server stopped (`aqueduct help` lists everything):

```sh
aqueduct runs ./data                              # every run: workflow, status, journal length
aqueduct describe ./data greet-1                  # status, input, output, each command and its attempts
aqueduct journal ./data greet-1                   # the journal, entry by entry
aqueduct verify ./data                            # check every hash chain, print each run's head
aqueduct export ./data greet-1                    # the journal as JSON Lines
aqueduct replay ./data greet-1 greet-v2.star      # would this version diverge from the run?
aqueduct counterfactual ./data greet-v3.star --workflow greet
                                                  # how would a new version have decided past runs?
aqueduct metrics ./data greet                     # per version: runs, escalations, timeouts, cost, replay
aqueduct versions list ./data                     # versions: state, approvals, replay, runs on them
aqueduct audit ./data                             # verify the audit log and print it
aqueduct drift ./data --model anthropic/claude-opus-5-5 --prompts . --max-tokens 50000
                                                  # ask past agent calls again: same answers?
```

## The name and the mark

An aqueduct carries water from wild sources across terrain nobody can change, on a gentle, constant
gradient, and some have done so for two thousand years. See [ADR 0002](docs/adr/0002-project-name.md).

<img align="right" src="docs/brand/aqueduct-icon.svg" width="96" alt="">

The mark is a fractal arch: one arch carrying two, each carrying two, standing on a line of water. It is
the Pont du Gard reduced to its idea, a structure that carries, repeated at every scale, which is also
how Aqueduct is meant to work: steps inside procedures, procedures inside larger ones, the same loop at
every scale. The water is what the structure exists to carry. The wordmark is set in lowercase
[Syne](https://gitlab.com/bonjour-monde/fonderie/syne-typeface). Every file, the palette and the rules for
using them are in the [brand guide](docs/brand/README.md); `python3 scripts/brand.py` regenerates them.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at
your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this
project by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions. Contributions require signing the [Contributor License Agreement](CLA.md);
see [CONTRIBUTING.md](CONTRIBUTING.md). The name is covered by the [trademark policy](TRADEMARKS.md),
and the [history format specification](docs/history-format.md) is licensed under CC-BY-4.0 so that anyone
can implement it.
