<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/brand/aqueduct-lockup-light.svg">
    <img alt="aqueduct" src="docs/brand/aqueduct-lockup-dark.svg" width="480">
  </picture>
</h1>

<p align="center">
  <strong>A deterministic engine for procedures that agents write and people approve.</strong><br>
  It starts as a library inside your binary, grows into a replicated cluster without changing your code,
  and every run leaves a history you can replay and audit.
</p>

> **Status: phase 1.** An embedded engine you can try, not yet one to depend on: APIs and the history
> format will still change. It runs [Starlark procedures](docs/adr/0008-procedure-language-starlark.md)
> and [Rust workflows](docs/adr/0014-rust-sdk.md) with durable timers, exactly-once signals, journaled
> retries and checkpoints, survives `kill -9`, and is tested by a deterministic simulator on every change.
> See the [build brief](BRIEF.md), the [decision records](docs/adr/) and the [FAQ](docs/faq.md).

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

## Quick start

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

Two examples show the path from agents to procedures, with a stand-in model so they need no API key:

- [`crystallise.rs`](crates/aqueduct/examples/crystallise.rs): an agent triages claims; then, through
  the MCP server, it writes a procedure from what it did (rules for the clear cases, the agent only for
  the rest), proposes it and replays it against its past runs. The report shows the one claim it got
  wrong; a person approves the version with a signature, and new claims cost a third of the tokens.
- [`bounded_agent.rs`](crates/aqueduct/examples/bounded_agent.rs): one agent step under a token budget
  and an output schema; a run whose budget is too small escalates for more, an answer that does not
  match the schema goes to a person, and a restart makes no model call again.

## Guarantees

What holds today, for the embedded engine, and how it is checked:

- **Acknowledged work is never lost.** A start, a signal or a step result is acknowledged only after it is
  durable. Checked by the simulator (crashes, torn writes, failed fsyncs) and by killing the engine with
  `kill -9` at random moments, including during its first start.
- **A step's result is recorded exactly once**, although a step may run more than once (at least once
  execution), so steps with side effects should be idempotent.
- **Starts are idempotent per key:** a retried start returns the same run; starting a used key with
  another workflow or input is refused ([ADR 0019](docs/adr/0019-run-key-reuse.md)). A run can
  **continue as new** (`return Err(ctx.continue_as_new(&next))`) to keep long-lived processes' journals
  short, and a key can be reused after its run ended; each run is a generation of its key
  ([ADR 0022](docs/adr/0022-generations-and-continue-as-new.md)).
- **Determinism:** procedures are deterministic by construction; Rust workflows are checked by replay, and
  a run whose code changed under it is parked, never silently corrupted. The time, random numbers and
  UUIDs come from the context (`ctx.now()`, `ctx.random()`, `ctx.uuid()`): recorded the first time,
  read back on replay ([ADR 0015](docs/adr/0015-recorded-values.md)).
- **Version pinning:** every run is pinned to the version it started on (a procedure's version is the
  SHA-256 of its source). New runs start on the active version, and old runs finish on theirs
  ([ADR 0029](docs/adr/0029-version-pinning.md)).
- **A version lifecycle:** procedure versions are registered with their provenance, proven by replay
  against past runs, approved under a policy (four eyes by default), activated, drained and retired, in
  a hash-chained registry log (`aqueduct versions ...`; [ADR 0030](docs/adr/0030-version-registry.md)).
- **Signed approvals:** an approval can be signed with the approver's Ed25519 key over the version's
  source hash and the replay report approved, checked against who holds which keys; a policy can require
  it, and deployment checks it again ([ADR 0033](docs/adr/0033-signed-approvals.md)).
- **Per-version metrics:** runs, escalation and timeout rates, cost and replay divergence for every
  version (`aqueduct metrics`), and the version on every live run metric
  ([ADR 0031](docs/adr/0031-per-version-metrics.md)).
- **For agents:** `aqueduct mcp <dir> --as <agent>` serves the Model Context Protocol, so agents
  validate, replay and propose procedure versions, start and inspect runs, and resolve escalations.
  Approval stays with people ([ADR 0032](docs/adr/0032-mcp-server.md)).
- **Counterfactual replay:** a new procedure version is replayed against past runs, answered with their
  recorded results, and the runs it would decide differently are reported with where they diverge
  ([ADR 0028](docs/adr/0028-counterfactual-replay.md)).
- **Child runs:** procedures and code workflows call each other (`call("payment", input)` in a
  procedure, `ctx.child("payment", &input)` in Rust) as durable child runs; cancelling a parent cancels
  its children ([ADR 0034](docs/adr/0034-child-runs.md)).
- **Changing code under running runs:** `if ctx.patched("add-review") { ... }` (or `patched(...)` in a
  procedure) keeps runs that already passed that point on the old code and sends every other run down
  the new one; `aqueduct_engine::replay::check` replays a corpus of journals against new code to prove
  it ([ADR 0035](docs/adr/0035-patched.md)).
- **Virtual objects:** keyed, durable state whose handlers run one at a time per key
  (`Builder::object_handler`, `Engine::call_object`); a handler's state changes commit atomically with
  its completion, and a failed handler commits nothing
  ([ADR 0043](docs/adr/0043-virtual-objects.md)). A handler's messages to other objects commit with its
  writes and arrive exactly once, so no hand-written outbox tables
  ([ADR 0044](docs/adr/0044-outboxes.md)).
- **Queries and updates:** a run publishes state (`publish("address", address)`, `ctx.publish(...)`)
  that queries read without journaling anything (`aqueduct query`, `Engine::query`); an update is a
  request whose sender waits for the run's answer (`wait(update("change"))`, then `respond` or
  `reject`; `Engine::update`, `aqueduct update`), journaled, deduplicated by id, and validated: an
  update rejected on receipt leaves no trace ([ADR 0036](docs/adr/0036-queries-and-updates.md)).
- **Timers never fire early**, and keep their wake time across restarts. **Signals are recorded exactly
  once per signal id.** **Retries** follow a policy with journaled backoff.
- **Agent steps:** a model call is a step whose answer, model identity and token cost are journaled, so
  replay never calls the model again, under a hard token budget
  ([ADR 0023](docs/adr/0023-agent-steps.md)). A run can have a budget of its own, and escalates to a
  person instead of overspending it ([ADR 0024](docs/adr/0024-run-budgets.md)); answers are checked
  against their output schema ([ADR 0025](docs/adr/0025-output-schemas.md)) When a model or prompt changes,
  model-drift replay asks past runs' calls again and reports which answers changed
  ([ADR 0027](docs/adr/0027-model-drift-replay.md)). `aqueduct-ai` runs them on Claude or any OpenAI-compatible
  endpoint, Ollama and vLLM included ([ADR 0026](docs/adr/0026-provider-adapters.md)).
- **Observable:** `aqueduct-otel` turns runs into OpenTelemetry traces (one trace per run, across
  restarts) and metrics ([ADR 0021](docs/adr/0021-opentelemetry.md)); the CLI shows any run's journal
  without setup.
- **Histories are tamper-evident** (a SHA-256 chain per run), exportable, and specified independently of the
  code ([history format](docs/history-format.md)).

**A server (phase 3, in progress):** `aqueduct server <dir>` serves a data directory's approved
procedures over gRPC and HTTP/JSON on `127.0.0.1:7470` ([protocol v1, draft](docs/protocol.md);
[ADR 0038](docs/adr/0038-server-and-protocol.md)):

```sh
aqueduct server ./data &
curl -s localhost:7470/v1/runs -d '{"key": "claim-7", "workflow": "claims", "input": {"amount": 400}}'
curl -s 'localhost:7470/v1/runs/claim-7/result?timeout_ms=10000'
```

**Tokens** turn authentication on: `aqueduct tokens create ./data --name billing-worker --permissions
work` prints a secret once and stores only its hash; every route then needs its permission, and a
hash-chained audit log records who did what, and every refused request (`aqueduct audit ./data`;
[ADR 0041](docs/adr/0041-authentication.md)). With `--tls-cert` and `--tls-key` it serves over TLS; it
listens beyond loopback only with tokens and a certificate
([ADR 0042](docs/adr/0042-server-tls.md)). A procedure's steps run
on **step workers**: processes in any language that connect to the server, say which steps they
implement, and run what they are sent, over a gRPC stream or HTTP long polling. A worker holds each
step under a lease; if it crashes or its result is lost, the step goes to another worker, and the run
records its result once. In Rust, [`aqueduct-worker`](crates/aqueduct-worker/src/lib.rs):

```rust
Worker::new("billing")
    .step("charge", |invocation| Outcome::Completed(charge(&invocation.input)))
    .run("http://127.0.0.1:7470")
    .await?;
```

Code workflows can run in a worker too: register the workflow as remote on the server
(`.remote_workflow("order")`) and serve its logic with `Worker::workflow("order", Arc::new(workflow(order)))`.
The server sends each activation the run's verified history and records the commands the workflow
issues ([ADR 0039](docs/adr/0039-workflow-workers.md)).

For serverless platforms, a queue's steps can be **pushed** instead: the server posts each invocation
to an HTTP endpoint and records the answer (`aqueduct server --config server.json`, with
`{"queues": {"billing": ["charge"]}, "push": [{"queue": "billing", "url": "https://..."}]}`;
[ADR 0040](docs/adr/0040-task-queues-and-push.md)).

Step-worker libraries for [Python](sdks/python/README.md) (3.9+, standard library only) and
[TypeScript](sdks/typescript/README.md) (Node 22.18+, no dependencies) do the same over HTTP. Every
worker library passes the [conformance suite](conformance/README.md) (`mise run conformance`), polling
and as a push endpoint (`worker.handle_push(body)` / `worker.handlePush(body)` in a function, or
`serve_push(port)` / `servePush(port)`).

Planned: the same semantics in a replicated cluster. The full design and phased plan are in [BRIEF.md](BRIEF.md).

## Testing it yourself

```sh
cargo xtask sim --seeds 10000                    # deterministic simulation: crashes, torn writes, fsync failures, lossy networks
cargo xtask sim --seeds 2000 --workload timers   # workloads: native, starlark, timers, rust
cargo xtask sim --seed 42 --workload rust        # reproduce one seed exactly
cargo xtask crash --kills 1000                   # SIGKILL a log writer; no acknowledged record may be lost
cargo xtask crash-engine --kills 200             # SIGKILL the engine mid-run; every acknowledged run must end correctly
cargo xtask crash-engine --startup --kills 300   # SIGKILL brand-new engines while they create their files
cargo xtask fsync                                # fsync latency and group commit throughput on this machine
```

## Inspecting a data directory

With the engine stopped, the `aqueduct` command line reads its data directory:

```sh
cargo run -p aqueduct-cli -- runs ./quickstart-data                   # every run: workflow, status, journal length
cargo run -p aqueduct-cli -- describe ./quickstart-data onboard:ada   # status, input, output, each command and its attempts
cargo run -p aqueduct-cli -- journal ./quickstart-data onboard:ada    # the journal, entry by entry
cargo run -p aqueduct-cli -- verify ./quickstart-data                 # check every hash chain, print each run's head
cargo run -p aqueduct-cli -- export ./quickstart-data onboard:ada     # the journal as JSON Lines
cargo run -p aqueduct-cli -- replay ./data claim-7 claims-v2.star     # would this procedure version diverge from the run?
cargo run -p aqueduct-cli -- counterfactual ./data claims-v3.star --workflow claims
                                                                      # how would a new version have decided past runs?
cargo run -p aqueduct-cli -- metrics ./data claims                     # per version: runs, escalations, timeouts, cost, replay
cargo run -p aqueduct-cli -- versions list ./data                      # procedure versions: state, approvals, replay, runs on them
cargo run -p aqueduct-cli -- keys generate grace.key --name grace      # a signing key; its public key goes into registry/identities.json
cargo run -p aqueduct-cli -- versions approve ./data claims 3f2a --by grace --key grace.key
                                                                      # a signed approval
cargo run -p aqueduct-cli -- mcp ./data --as agent/claims-explorer      # MCP on stdio, for an agent (claude mcp add aqueduct -- aqueduct mcp ...)
cargo run -p aqueduct-cli -- drift ./data --model anthropic/claude-opus-5-5 --prompts . --max-tokens 50000
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
