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
> See the [build brief](BRIEF.md) and the [decision records](docs/adr/).

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
            escalate("no-photos", claim)
    return step("claims.settle", claim)
```

Register it with `.workflow("claims", Arc::new(aqueduct::Procedure::compile("claims.star", source)?))`.
`escalate` hands the claim to a person and waits, durably, for their decision; list open escalations
with `engine.escalations()` or `aqueduct escalations`, and resolve them with `resolve_escalation`
([ADR 0020](docs/adr/0020-escalations.md)). [`ingest.rs`](crates/aqueduct/examples/ingest.rs) is the
reference pipeline with all of it: concurrent steps, a review through an awakeable with a deadline, and
an escalation when nobody reviews in time.

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
- **Timers never fire early**, and keep their wake time across restarts. **Signals are recorded exactly
  once per signal id.** **Retries** follow a policy with journaled backoff.
- **Observable:** `aqueduct-otel` turns runs into OpenTelemetry traces (one trace per run, across
  restarts) and metrics ([ADR 0021](docs/adr/0021-opentelemetry.md)); the CLI shows any run's journal
  without setup.
- **Histories are tamper-evident** (a SHA-256 chain per run), exportable, and specified independently of the
  code ([history format](docs/history-format.md)).

Planned: the same semantics on a single server and in a replicated cluster, agent steps with budgets, and
the procedure lifecycle (proposal, replay against past cases, approval, retirement). The full design and
phased plan are in [BRIEF.md](BRIEF.md).

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
