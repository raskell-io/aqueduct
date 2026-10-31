# Aqueduct: build brief for a durable execution engine in Rust

> **Aqueduct**: a structure that carries water from wild sources across terrain nobody can change, on a
> gentle and constant gradient, for centuries. The name and its reasoning are recorded in
> [ADR 0002](docs/adr/0002-project-name.md). The crate name `aqueduct` was free on crates.io on 2026-10-31;
> a trademark search in software classes is still open.

This document is the complete brief for building the project. It is written for a Claude Code agent (or a
human engineer) starting from an empty repository. Read it fully before writing code. When this brief and
your own judgement disagree, write an ADR (architecture decision record) in `docs/adr/` explaining the
disagreement and ask the owner before acting on it.

The project's long-term direction comes from the essay *The River and the Canal* on raskell.io: agents
explore the rugged world, and workflows are what exploration leaves behind. Section 1 summarises the
argument; read the essay for the full reasoning.

---

## 0. How to work on this

- **Work in phases** (section 14). Each phase ends with a working demo, green CI, updated docs and ADRs.
  Do not start a later phase before the previous phase's acceptance criteria pass.
- **Correctness before features, features before speed.** A durable execution engine that loses or
  duplicates work is worse than none. The deterministic simulation harness (section 10) is built in phase 0
  and grows with every feature.
- **Keep the core pure.** The state machine that decides what happens to a run has no I/O, no clock, no
  randomness, no async. Everything with side effects sits behind traits so it can be simulated.
- **Design for decades.** Histories are audit records that must stay readable long after the code that
  wrote them is gone. Formats and semantics are specified separately from the implementation (section 6.4).
- **Record decisions.** Every non-obvious choice gets a short ADR: context, options, decision, consequences.
- **Ask the owner before:** changing the license, adding a dependency that is not Apache-2.0, MIT, BSD,
  ISC, Zlib or Unicode licensed, freezing a wire protocol, procedure format or storage format, publishing
  crates, or cutting a release. The public repository is `github.com/raskell-io/aqueduct`.

---

## 1. Mission

Build the deterministic engine that runs the procedures agents discover and people approve, that anyone
can adopt in an afternoon, and that can still be trusted in production many years later.

Durable execution means: you describe a process (call APIs, wait for people, sleep for days, retry on
failure), and the engine guarantees that it runs to completion despite crashes, deployments, network
failures and machine loss, without hand-written state machines, queues, cron jobs, outbox tables or retry
loops.

**One sentence:** *Aqueduct is a deterministic engine for procedures that agents write and people approve;
it starts as a library inside your binary, grows into a cluster, and every run leaves a history you can
replay and audit.*

### 1.1 Why a workflow engine in the age of agents

Agents are good at open problems: unfamiliar terrain, ambiguity, cases nobody has seen. Many businesses,
and most of what banks, insurers and administrations sell, are closed problems: the value is that the same
case is handled the same way every time, and that anyone can check afterwards what happened. An agent that
is right 95 percent of the time per step completes a twenty-step process correctly about 36 percent of the
time; a deterministic procedure does not compound errors on its deterministic steps.

The design follows from that division of labour (the essay calls it the meta-factory):

1. **Explore.** Agents handle new kinds of work directly.
2. **Crystallise.** When work recurs, an agent writes a *procedure*: a short, typed, readable definition.
3. **Prove.** The procedure is replayed against past cases; divergences are explained or fixed.
4. **Approve.** A named person approves it; the approval is part of the procedure's record.
5. **Run.** The engine runs it deterministically. Steps that genuinely need judgement are *agent steps*:
   bounded, budgeted, schema-checked, journaled.
6. **Observe.** Escalations, timeouts, cost and divergence are tracked per procedure version.
7. **Repair or retire.** Rising escalations send a procedure back to an agent; finished work is retired.

The loop is the same at every scale: a step, a procedure, a department's routines. An engine supports it
at all scales as long as a procedure can contain agent steps and an agent can start, inspect and propose
procedures.

### 1.2 Two ways to describe work, one engine

- **Procedures** (section 4.4): hermetic Starlark scripts that an agent can write, the engine interprets
  deterministically, and a person can review in one sitting. Determinism is guaranteed by construction,
  because the interpreter has no source of non-determinism. This is the primary authoring path for agents
  and the main polyglot path.
- **Code workflows** (Rust SDK, section 4.2): ordinary async Rust for engineers who need full expressive
  power. Determinism is checked by replay, not guaranteed.

Both produce the same journal, use the same core state machine and get the same guarantees. A procedure
step can call a code workflow and vice versa.

### 1.3 The niche (as of 2026-10; verify cells before publishing comparisons)

| | Temporal | Restate | DBOS | Aqueduct |
|---|---|---|---|---|
| Deployment to start | server cluster plus database | server binary | library plus PostgreSQL | **library with an embedded store, no database** |
| Grows into | large cluster | replicated cluster | PostgreSQL-backed fleet | **replicated cluster, same core** |
| License | MIT | BSL 1.1 (server) | MIT | **Apache-2.0 OR MIT** |
| Agent-authored procedures | no | no | no | **hermetic Starlark procedures, provenance, approval** |
| Testing story | replay tests | | | **deterministic simulation of the whole engine in CI** |

Other neighbours to track: Obelisk (Rust, WASM components, deterministic workflows), Resonate (durable
promises), Golem (WASM, durable), Cloudflare Workflows, Inngest, Hatchet.

The differentiators, in order of importance:

1. **Procedures agents can write and people can trust.** Declarative, deterministic by construction,
   versioned with provenance and approval, provable by replay against past cases.
2. **Embedded to cluster on one code path.** `Engine::embedded("./data")` for a CLI, a desktop app, a single
   binary service or tests; the same procedures and workflows later run on a cluster.
3. **Reliability and auditability you can see.** Deterministic simulation testing (FoundationDB,
   TigerBeetle tradition) of the whole engine, plus tamper-evident, replayable histories in a documented
   format.
4. **Permissive license, no strings.** Banks, insurers and governments can adopt it without a license fight.
5. **Smart defaults.** Version pinning, sagas, per-key concurrency and fairness, time-travel debugging,
   first-class agent steps with budgets and human approval.

### 1.4 First consumer

The first real application is a single-binary metadata platform by the same owner, which ingests IT
systems' metadata through connectors, resolves identities, scores sources and builds artifacts. Its
workflows (ingest, resolve, score, build artifacts, wait for human confirmation) are the reference workload
for phase 1. Design for that, but never special-case it.

---

## 2. Users and use cases

- **Application developers** who need reliable background processes: payments, onboarding, provisioning,
  order handling, data pipelines with external calls, notifications with retries.
- **Agents** that explore work, propose procedures, start and inspect runs, and investigate escalations,
  through the MCP server and the ingress API.
- **Process owners** who review, approve, observe and retire procedures. They read procedures; they do not
  have to write Rust.
- **Platform teams** who run it as shared infrastructure for many services and languages.
- **Tool builders** who embed it in a single binary (CLIs, desktop apps, edge devices, self-hosted products)
  and want durability without operating a database or a cluster.
- **Regulated organisations** that need traceability by construction (for example EU AI Act Article 12
  record-keeping for high-risk systems).

The litmus test for "anyone can use it": a developer who has never heard of durable execution can go from
`cargo add aqueduct` to a workflow that survives `kill -9` in under fifteen minutes, following the README.

---

## 3. Principles (non-negotiable)

1. **Never lose acknowledged work.** If the engine said "started", "signal accepted" or "step completed",
   that fact survives any single failure the deployment mode claims to tolerate.
2. **Never record a step twice.** A step's result enters the journal at most once. Steps may *execute* more
   than once (at-least-once); the engine says so loudly and gives idempotency keys.
3. **Determinism is guaranteed for procedures and checked for code.** Procedures are interpreted
   deterministically by construction. Code workflows route every non-deterministic input through the
   context; replay divergence is detected and reported, never silently ignored.
4. **The core is a pure state machine.** `(state, event) -> (state, effects)`. All I/O is outside.
5. **Same semantics in every mode.** Embedded, single server and cluster differ in availability and scale,
   never in behaviour.
6. **Histories outlive the engine.** The journal format is specified independently of the code, versioned,
   tamper-evident and exportable, so a history written today can be read and replayed in ten years.
7. **Model-neutral.** Agent steps work with any model provider. The core never depends on a model API.
8. **Boring dependencies, permissive licenses.** Enforced with `cargo-deny`.
9. **Observable by default.** Every run, step, timer, signal and agent call is traceable with
   OpenTelemetry and inspectable from the CLI without extra setup.
10. **Upgrades are part of the product.** Storage, protocol and procedure formats are versioned from the
    first commit, with an upgrade path for every change after 1.0.

---

## 4. Concepts and programming model

### 4.1 Vocabulary

- **Procedure**: a hermetic Starlark script describing a process, interpreted by the engine (section 4.4).
- **Workflow**: a durable async function in code (Rust SDK), identified by a type name.
- **Run**: one execution of a procedure or workflow, identified by a **run key** chosen by the caller (or
  generated). Starting the same key twice follows an id reuse policy. Has a journal, a status, and a pinned
  version.
- **Step** (Temporal: activity; Restate: `ctx.run`): a side-effecting operation whose result is journaled.
  Retries with policy. At-least-once execution, exactly-once recording.
- **Agent step**: a step that calls a model with bounded scope: a budget, an output schema, a recorded model
  identity. Its full response is journaled. The essay calls these the *locks* in the canal.
- **Escalation**: a first-class outcome (not an error) that routes a run, or a decision within it, to an
  agent or a person, and is counted per procedure version.
- **Journal**: the ordered, append-only record of everything a run did that affects its control flow.
- **Timer**: durable sleep or deadline.
- **Signal**: an asynchronous message into a run. **Query**: a read-only look at a run's state.
  **Update**: a synchronous, validated, journaled request that returns a result.
- **Awakeable** (durable promise): an id the run hands out; something external completes it later. Human
  approvals and webhooks are modelled this way.
- **Virtual object**: keyed, durable state with handlers that run one at a time per key (actor style).
  State changes commit atomically with the handler's completion.
- **Child run** and **service call**: durable calls to other procedures, workflows or handlers, with results
  journaled.
- **Version**: an immutable, registered revision of a procedure or a set of code workflows (a
  **deployment**). New runs use the active version; existing runs stay pinned to the version they started on.
- **Provenance**: where a procedure version came from (author or exploring agent, cases studied, replay
  evidence). **Approval**: a signed, recorded decision by a named person to activate a version.
- **Namespace**: an isolation unit for tenants, with quotas, policies and access control.

### 4.2 Rust SDK shape (target, refine in ADRs)

```rust
use aqueduct::prelude::*;

#[derive(Serialize, Deserialize)]
struct IngestInput { source: String }

#[workflow]
async fn ingest_source(ctx: WorkflowContext, input: IngestInput) -> Result<IngestReport, WorkflowError> {
    // A step: runs at least once, its result is journaled exactly once.
    let batch: BatchRef = ctx
        .step("extract")
        .retry(Retry::exponential().max_attempts(8))
        .timeout(Duration::from_secs(600))
        .run(|attempt| extract(&input.source, attempt.idempotency_key()))
        .await?;

    // Concurrent steps: issue order is deterministic, completion order is journaled (section 5.1).
    let (mapped, owners) = ctx.join(
        ctx.step("map").run(|_| map(&batch)),
        ctx.step("owners").run(|_| lookup_owners(&batch)),
    ).await?;

    let resolved = ctx.step("resolve").run(|_| resolve(&mapped, &owners)).await?;

    // Wait for a person, durably, for up to 7 days.
    if resolved.needs_review {
        let review = ctx.awakeable::<ReviewDecision>("review");
        notify_reviewers(&ctx, review.id()).await?;                   // itself a step
        match review.with_timeout(Duration::from_secs(7 * 24 * 3600)).await? {
            Some(decision) => apply(&ctx, decision).await?,
            None => ctx.escalate("review-timeout", &resolved).await?,  // an escalation, not an error
        }
    }

    // Deterministic helpers instead of std:
    let started = ctx.now();          // journaled clock
    let token = ctx.uuid();           // journaled randomness
    ctx.sleep(Duration::from_secs(60)).await?;

    Ok(IngestReport { started, token, /* ... */ })
}
```

The `#[workflow]` macro generates a type (`IngestSource`) that is used both for registration and for
starting runs, so there is one name per workflow:

```rust
let engine = Engine::embedded("./data")
    .register::<IngestSource>()
    .register::<SourceScores>()
    .start()
    .await?;

let handle = engine
    .start::<IngestSource>(RunKey::new("ingest:git-host"), IngestInput { source: "git-host".into() })
    .await?;                                   // idempotent per run key
let report = handle.result().await?;           // or come back later with engine.handle(key)
```

Virtual objects:

```rust
#[object(name = "source_scores")]
impl SourceScores {
    #[handler]
    async fn record(ctx: ObjectContext, score: f64) -> Result<f64, HandlerError> {
        let mut history: Vec<f64> = ctx.get("history").await?.unwrap_or_default();
        history.push(score);
        ctx.set("history", &history);           // committed with the handler result
        Ok(history.iter().sum::<f64>() / history.len() as f64)
    }
}
```

Client of a cluster: the same `register` calls on a `Worker::connect("https://aqueduct.internal")` builder.
Workflow code does not change.

Determinism in code workflows is detected, not enforced: Rust cannot stop a workflow from calling
`SystemTime::now()`, spawning tasks, or iterating a `HashMap` (whose order is randomly seeded per process).
Provide guard rails: documentation, a lint (clippy-style or a custom `cargo aqueduct check`), and the
replay corpus in CI.

### 4.3 Sagas

First-class compensation, because hand-written compensation is where most workflow bugs live:

```rust
let mut saga = ctx.saga();
let booking = saga.step("reserve-room", |_| reserve_room(&req))
                  .compensate_with(|b| cancel_room(b)).await?;
let payment = saga.step("charge-card", |_| charge(&req))
                  .compensate_with(|p| refund(p)).await?;
saga.complete();   // on error or cancellation before this, compensations run in reverse order, durably
```

Procedures express the same with a `compensate` field per step.

### 4.4 Procedures

A procedure is a hermetic **Starlark** script that defines `run(input)` (ADR 0008). Starlark is a
deterministic dialect of Python with a written specification and implementations in Go, Java and Rust;
the engine interprets it as a decider (ADR 0004). The format is frozen only with the owner's approval.

```python
# procedures/claims/water_damage_photo_evidence.star
PROCEDURE = {
    "name": "claims/water-damage-photo-evidence",
    "version": 3,
    "origin": {"explored_by": "agent/claims-explorer", "cases_studied": 37},
}

def run(claim):
    docs = agent("classify_documents", claim["documents"],
                 prompt = "prompts/classify-documents.md",
                 output_schema = {"$ref": "schemas/DocumentClassification.json"},
                 budget_tokens = 20000)                       # bounded judgement, journaled

    coverage = step("coverage.water_damage@v7", claim)        # a registered step (any language)
    if not coverage["covered"]:
        fail("not covered: " + coverage["reason"])

    if docs["missing_photos"]:
        step("notify.customer", {"template": "claims/missing-photos", "to": claim["customer"]})
        photos = wait(signal("photos_received"), timeout = days(14))
        if photos == None:
            escalate("no-photos", claim)

    if claim["amount"] > 5000 or docs["confidence"] < 0.8:
        escalate("manual-review", claim)
    return call("claims/settle@v2", {"claim": claim["id"], "coverage": coverage})
```

The phase 0 spike (`crates/aqueduct-procedure`) implements `step`, `parallel`, `agent` and `escalate`;
`wait`, `signal`, `call`, timers and sagas arrive as the core supports them. Approvals and replay evidence
live in a signed record next to the script, not in it.

Requirements:

- **Deterministic by construction.** The interpreter is hermetic: no `load`, clock, randomness, file system
  or network. The only effects are engine functions, which are journaled. Replay re-executes `run` against
  the journal; changing a procedure under a running run is detected and parks it.
- **Expressive, but bounded.** Full branching, loops over finite collections, functions and computation.
  Every activation runs under a statement budget, a heap limit, a call stack limit and (in production) a
  wall-clock watchdog; exceeding any of them parks the run.
- **Failures are explicit.** A failed step fails the run unless called with `fallible = True`; `fail(...)`
  fails it deliberately; any other script error is a bug and parks the run.
- **Typed at the edges.** Inputs, step outputs and agent step outputs have JSON Schemas, checked at run time
  and, where possible, by a lint before registration.
- **Linted, then proven.** Static checks on the syntax tree (unbounded waits, missing timeouts, unhandled
  escalations, recursion) run before registration; counterfactual replay against past runs (section 5.2)
  carries the rest of the proof.
- **Short and readable.** A process owner must be able to review a version in one sitting. Diffs between
  versions are first-class (`aqueduct procedure diff v2 v3`).
- **Composable.** Procedures call procedures and code workflows; code workflows call procedures.
- **Hashing and signing** use the script's exact bytes plus its metadata record.

### 4.5 Versioning and lifecycle

- **Default: version pinning.** Every run is pinned to the version it started on. Registering new code or a
  new procedure version creates a new version; old runs finish on the old one. Old versions are drained and
  retired.
- **Lifecycle states:** `draft -> proposed -> proven -> approved -> active -> draining -> retired`. Policy
  per namespace decides which transitions need which evidence (for example: "an active version in
  `claims` must have a replay report and an approval by a member of `claims-owners`").
- **Provenance and approval** are stored with the version and recorded as journaled, signed events in a
  registry. Retired versions and their histories are kept according to retention policy.
- **For long-running code workflows that must move to new code:** an explicit patch API
  (`if ctx.patched("add-review-step") { ... }`) recorded in the journal, plus a replay compatibility check.
- **Replay corpus:** the CLI exports real run histories; a test helper replays them against current code or
  a new procedure version in CI.

---

## 5. Execution semantics

### 5.1 Model: journaled replay

A run executes as an ordinary Rust future (code workflows) or as an interpreter over the procedure (which
needs no replay of user code, but uses the same journal). Every interaction with the outside world
corresponds to journal entries:

| Context call | Journal entries | Replay behaviour |
|---|---|---|
| start | `Input { payload, version }` | always first |
| step | `StepStarted { cmd, name }`, `StepCompleted { cmd, result }` or `StepFailed { cmd, error }` | return recorded result, do not run the body |
| agent step | `AgentCallStarted { cmd, model, params }`, `AgentCallCompleted { cmd, response, usage, schema_ok }` | return recorded response |
| sleep | `TimerStarted { cmd, wake_at }`, `TimerFired { cmd }` | resolve if fired |
| `now`, `uuid`, `random` | `SideValue { cmd, value }` | return recorded value |
| awakeable | `AwakeableCreated { cmd, id }`, `AwakeableCompleted { cmd, value }` | resolve if completed |
| signals | `SignalReceived { name, payload }` | deliver in recorded order |
| child run, service call | `CallStarted { cmd }`, `CallCompleted { cmd, result }` | return recorded result |
| `patched(id)` | `Patch { id }` | branch as recorded |
| escalation | `Escalated { cmd, reason, payload }`, `EscalationResolved { cmd, outcome }` | resolve if resolved |
| return | `Output { result }` | run completes |

**Commands and completions are separate.** Each context call that starts something gets a command index
`cmd` in the order the code issues it, which is deterministic. Completions are journaled in the order they
actually happen, which is not. Replay matches *commands* by index (kind and name must agree) and delivers
*completions* in their recorded order. This is what makes `join`, `select` and timeouts racing a step
deterministic on replay. The execution-model ADR in phase 0 specifies this precisely, including how
`select` records which branch won.

- **Mismatch:** a `NonDeterminism` error. The run is **parked** (not failed), an alert is emitted, and an
  operator can fix the code and resume, or reset the run to an earlier journal point.
- **Suspension:** when a run awaits something that is not ready, the SDK reports `Suspended { waiting_on }`
  and the worker drops the future. The engine re-activates the run when an awaited event arrives.
- **Sticky execution:** workers keep recently active runs' futures in memory (bounded LRU) so a new event
  continues the live future instead of replaying. Replay is the fallback, never the only path.

Step bodies run in the worker process by default (Restate style); optional named task queues for heavy or
specialised steps come in phase 3 (Temporal style). Record in an ADR.

### 5.2 Replay modes

Replay is not only crash recovery. The engine supports four modes, sharing one implementation:

1. **Recovery replay:** rebuild a run's state after a restart or failover.
2. **Determinism check:** replay a history against the code that produced it; fail on divergence.
3. **Counterfactual replay:** replay past runs' inputs and recorded external results against a *new*
   procedure or code version, and report where outcomes differ. Steps the old run never called are stubbed
   or marked unresolvable. Divergence is the output, not an error. This produces the replay report that a
   proposed procedure version needs before approval.
4. **Model-drift replay:** re-run agent steps of past runs against a new model or prompt, live, and compare
   the outputs with the recorded ones under the output schema. Run it whenever a model changes, not only
   when a procedure does.

### 5.3 Guarantees (document these verbatim in the user docs)

- A run's journal is linearizable: entries are appended in one order and never rewritten (except by an
  explicit operator reset, which is itself journaled).
- A step body executes **at least once**; its result is recorded **at most once**; the run observes
  **exactly one** result. Each attempt gets a stable idempotency key `run_id/epoch/cmd`, where `epoch`
  increments on operator reset, so a re-executed command after a reset never reuses an old key.
- An agent step's response, once recorded, is never re-requested on replay: a completed model call is never
  billed twice because of a crash or redeploy.
- Timers fire **no earlier** than their wake time, measured by the partition leader's clock anchored to the
  journaled wall-clock time of `TimerStarted`, and eventually after it, subject to availability. Clock
  assumptions per mode are stated in `docs/guarantees.md`.
- Signals to a run are delivered **exactly once, in a total order** per run.
- Virtual object handlers run **one at a time per key**; state changes commit atomically with the handler
  result.
- Starting a run with an existing key follows the configured reuse policy (reject, return existing, allow
  after completion) and is idempotent.

### 5.4 Long-running runs, cancellation and limits

- **Continue-as-new:** a run can complete and start a fresh run with new input and an empty journal, linked
  to its predecessor. Required for agent loops and processes that live for months.
- **Journal size limits** per run, with warnings well before the limit, and **payload size limits** with
  large payloads stored by reference.
- **Cancellation:** propagates to child runs and in-flight steps (steps receive a cancellation signal; their
  results may still arrive and are recorded). Cancelling a saga runs its compensations.
- **Run timeouts** (total and per activation) in addition to step timeouts.

### 5.5 Failure model

Per deployment mode, state precisely which failures are tolerated:

- **Embedded:** process crash, power loss (with fsync on), worker panic. Not disk loss.
- **Single server:** the above plus worker and client disconnects. Not server disk loss.
- **Cluster (3 or 5 nodes):** loss of a minority of nodes per partition, network partitions, slow disks,
  clock skew within bounds. Correctness never depends on synchronised clocks.

---

## 6. Architecture

### 6.1 Components

```text
        clients (SDK, CLI, MCP server, HTTP/JSON, gRPC)
                          |
                  +-------v--------+
                  |    ingress     |  stateless: auth, routing by key hash to partition
                  +-------+--------+
                          |
      +-------------------+-------------------+
      |                   |                   |
+-----v------+      +-----v------+      +-----v------+
| partition 0|      | partition 1|  ... | partition N|   each partition:
|  leader    |      |  leader    |      |  leader    |   - replicated log (Raft group)
+-----+------+      +-----+------+      +-----+------+   - state machine (pure core)
      |                   |                   |          - procedure interpreter (pure)
      |                   |                   |          - state store (runs, journals, timers,
      v                   v                   v            object state, inboxes, idempotency)
   invoker             invoker             invoker       - timer service
      |                   |                   |          - invoker: dispatch to workers
      +---------+---------+---------+---------+
                |                   |
        pull workers          push endpoints
        (long-lived stream)   (HTTP, serverless)

   metadata store (small Raft group): membership, partition map, versions and approvals, namespaces
   object storage (optional, S3-compatible): snapshots, archived histories
```

### 6.2 The partition processor

- Run keys and object keys hash to a fixed number of partitions (configured at cluster creation, default
  64, ADR on resharding later).
- Each partition has one leader at a time. Everything for a key happens on its partition, so no
  cross-partition transactions are needed for a run's own state.
- Cross-partition interactions go through **durable outboxes**: the caller's partition commits "send
  message M" with its own state change; a shipper delivers M to the target partition's inbox exactly once
  (deduplicated by message id).
- The core state machine consumes log entries and produces state changes plus effects (dispatch step, arm
  timer, send message, call model). It is deterministic and fully unit-testable without I/O.

### 6.3 Storage

Two traits, several implementations. Both are synchronous at the core boundary and wrapped by the async
runtime layer (decide in the storage ADR; the point is that the choice is deliberate and consistent):

```rust
trait LogStore {      // append-only, per partition
    fn append(&self, partition: PartitionId, entries: &[LogEntry]) -> Result<Lsn>;
    fn read(&self, partition: PartitionId, from: Lsn) -> Result<LogIter>;
    fn trim(&self, partition: PartitionId, up_to: Lsn) -> Result<()>;
}
trait StateStore {    // materialised state, transactional per partition
    fn transaction(&self, partition: PartitionId) -> Result<StateTxn>;   // get, put, delete, scan, commit
    fn snapshot(&self, partition: PartitionId) -> Result<Snapshot>;
}
```

- **Embedded and single server:** one local storage engine for both log and state. Phase 0 benchmarks
  **redb** (B-tree, ACID, stable format, Apache/MIT) and **fjall** (LSM tree, Apache/MIT) on the journal
  append workload and the state read workload, and records the choice in an ADR. Use group commit (batch
  fsyncs) from day one; durability settings are explicit (`Durability::Fsync` default, `Durability::Os` for
  tests and tools).
- **Cluster:** the log is replicated per partition with Raft (evaluate **openraft**, Apache/MIT). State is
  materialised locally on each replica from the log, with periodic snapshots (optionally to object storage)
  so the log can be trimmed and new replicas can catch up.
- **Optional later:** a PostgreSQL backend for teams that want to reuse an existing database. Behind the
  same traits; never the default.
- Payloads are opaque bytes with a declared codec; the SDK defaults to a compact serde format (ADR: JSON for
  debuggability vs a binary format for speed; consider JSON in development and binary in production, both
  supported).

### 6.4 The history format

Histories are the project's most durable artefact. They are audit records, replay inputs and evidence for
procedure approvals.

- **Specified, not just implemented:** `docs/history-format.md` defines the journal entry encoding, version
  bytes, codecs and canonical forms independently of the Rust types. A second implementation should be able
  to read a history from the spec alone.
- **Tamper-evident from the first format version:** each entry carries the hash of its predecessor (a hash
  chain per run); the head hash is stored with the run and can be anchored externally. This cannot be added
  after the format is frozen.
- **Exportable** to a plain, documented interchange form (for example JSON Lines) for archives, auditors and
  other tools.
- **Erasable without rewriting:** personal data in payloads can be encrypted with per-subject keys through
  the codec layer, so deleting a key makes the data unreadable (crypto-shredding) while the journal stays
  intact and verifiable. Needed for GDPR erasure against immutable histories.
- **Longevity commitment** (decide by 1.0): a published promise of how long a given history format version
  stays readable by new releases, in the spirit of SQLite's file format commitment.

### 6.5 Timers at scale

Persisted timer index per partition (ordered by wake time), plus an in-memory hierarchical timing wheel for
timers due within the next window. Leaders load the window on startup and on leadership change. Target:
tens of millions of pending timers per cluster without memory growth.

### 6.6 Dispatch, flow control and fairness

- Pull workers hold a bidirectional stream to the cluster, declare capacity, and receive activations.
- Push endpoints are called over HTTP/2 with the activation; they stream commands back. This makes
  serverless platforms first-class.
- Per namespace and per key **concurrency limits**, **rate limits**, **priorities**, and **fair scheduling**
  across tenants (weighted fair queueing), so one noisy tenant cannot starve the rest.
- Model calls from agent steps are subject to the same limits plus **token and cost budgets** per run, per
  procedure and per namespace.
- Backpressure all the way to ingress: when partitions are overloaded, starts are rejected with a retry hint
  rather than queued without bound.

---

## 7. Deployment modes

| Mode | What runs | Storage | Availability | Use |
|---|---|---|---|---|
| **Embedded** | engine inside the application process | local file (redb or fjall) | the process | CLIs, desktop, single binary services, tests |
| **Single server** | `aqueduct server` + workers or push endpoints | local file | the server | small teams, departments, staging, edge sites |
| **Cluster** | 3+ `aqueduct server` nodes | local per node, Raft replicated, snapshots to object storage | minority node loss tolerated | production platforms |

Moving from embedded to server: `aqueduct migrate --from ./data --to https://server` copies open runs,
histories, versions and object state. Moving from single server to cluster: add nodes; the partition map
rebalances leadership.

---

## 8. Polyglot: procedures, protocol and SDKs

- **Procedures are the main polyglot path.** A procedure is data; its steps are implemented by **step
  workers** in any language through a thin protocol (receive a step invocation with an idempotency key,
  return a result or an error). Step workers are much simpler to write and maintain than full replay SDKs.
- **Service protocol v1**, defined in protobuf (`proto/`), versioned, documented in `docs/protocol.md`:
  step invocations; and, for code workflows, activations (journal entries to replay, new events) and
  commands (step, timer, call, awakeable, signal, output, suspend). The Rust SDK implements it, even though
  embedded mode can skip serialisation through an in-process channel with identical semantics.
- **Ingress API:** gRPC and HTTP/JSON for starting runs, signalling, querying, updating, completing
  awakeables, resolving escalations, managing versions, listing and inspecting runs. Usable with `curl`.
- **MCP server** (phase 2): exposes the engine to agents: propose a procedure, validate and type-check it,
  run counterfactual replay, start and inspect runs, investigate escalations. Write actions respect the same
  authorisation and approval policies as people.
- **SDKs:** Rust first. Full replay SDKs for other languages (TypeScript first) are optional and come after
  the protocol stabilises; step-worker libraries for TypeScript, Python and Go come earlier because they are
  small.
- **Conformance suite** (`conformance/`): language-neutral scenarios (replay, suspension, non-determinism
  detection, retries, sagas, objects, procedures, step workers) that every SDK and step-worker library must
  pass.

---

## 9. Scalability and performance

Targets are starting points for phase 0 benchmarks and are revised by ADR with measured numbers. Measure on
the owner's reference machine (Apple M4, 10 cores, 16 GB) for embedded and single server, and on a
documented 3-node Linux setup for cluster mode.

**Measure raw fsync latency first.** On macOS, Rust's `File::sync_all` issues `F_FULLFSYNC`, which commonly
costs milliseconds per call; Linux `fdatasync` on NVMe behaves differently. Latency targets with fsync on
must be derived from that measurement, and throughput targets depend on group commit batch size.

| Metric | Initial target |
|---|---|
| Embedded: journal appends per second, fsync on, group commit | 50,000+ |
| Embedded: three-step workflows completed per second | 5,000+ |
| Embedded: start to first step dispatched, p99, fsync on | under 15 ms on macOS (ADR 0003); Linux to be measured |
| Embedded: start to first step dispatched, p99, `Durability::Os` | under 5 ms |
| Procedure interpreter: decisions per second per core | set in phase 2 |
| Single server: run starts per second over gRPC | 10,000+ |
| Cluster: throughput scaling from 3 to 9 nodes | at least 2.5x |
| Recovery: leader failover to first new dispatch, p99 | under 3 s |
| Pending timers per cluster without memory growth | 10 million+ |
| Open runs per partition with lazy loading | millions; only active runs in memory |

The phase 0 measurement on the M4 (about 4 ms per `F_FULLFSYNC`) is in
`docs/measurements/2026-10-31-fsync-m4.md`, and the revised targets are in ADR 0003.

Benchmarks live in `benches/` (criterion for micro benchmarks, a load generator binary for end to end) and
run nightly. Regressions over 10 percent fail the nightly job.

---

## 10. Reliability engineering

### 10.1 Deterministic simulation testing (built in phase 0, extended every phase)

- All sources of non-determinism in the engine go through an environment trait: time, randomness, network,
  disk, task scheduling.
- `aqueduct-sim` runs the whole system (clients, ingress, partitions, workers, storage) in one thread with a
  seeded scheduler and injects faults: process crashes and restarts, torn writes, fsync failures, disk full,
  dropped, delayed, duplicated and reordered messages, network partitions, clock jumps, slow nodes.
- **Runtime decision (phase 0 ADR):** the brief mandates tokio for production, and the simulator needs
  fully deterministic scheduling. Either the engine's internals are runtime-agnostic (an executor trait,
  with tokio in production and a seeded single-threaded executor in simulation), or the simulator relies on
  tokio's current-thread runtime with paused time plus **turmoil** (MIT) for the network. The second is not
  fully deterministic on its own; the ADR must say how the gap is closed. Storage and process faults need
  our own fakes in either case.
- **Invariants checked after every simulated step and at the end of each seed:**
  - no acknowledged start, signal, awakeable completion or step result is lost;
  - no journal has a duplicate step result or a gap; every run's hash chain verifies;
  - every run's observed history equals a valid sequential history (linearizability check per key);
  - procedure runs replay to identical decisions from their journals;
  - timers never fire early; every due timer fires within the bound after faults heal;
  - object state per key equals the sequential application of its committed handlers;
  - compensations of a failed or cancelled saga run exactly in reverse order of completed steps;
  - no agent call with a recorded response is issued again.
- CI runs at least 1,000 seeds per pull request and 100,000 nightly. Any failing seed reproduces exactly with
  `cargo xtask sim --seed <n>` and becomes a regression test.

### 10.2 Other layers

- Property tests (proptest) on the core state machine, the procedure interpreter and the journal codec.
- Fuzzing (cargo-fuzz) on protocol decoding, journal decoding, procedure parsing and storage recovery.
- Crash tests on real disks: a harness that kills the process at random points during load and verifies
  invariants on restart (embedded and single server).
- Before 1.0: an external, Jepsen-style test of the cluster on real machines, published with its findings.

---

## 11. Operations

- **CLI** (`aqueduct`): start, signal, query, update, complete awakeable, resolve escalation, list, describe,
  journal, verify (hash chain), reset to a journal index, cancel, terminate, versions (register, diff,
  propose, approve, drain, retire), replay (determinism, counterfactual, model drift), export history,
  namespaces, sim (developer only), migrate.
- **Web console** (phase 4): runs list with filters, run detail with the journal as a timeline, pending
  timers, awakeables and escalations, procedure versions with provenance, approvals, replay reports and
  per-version metrics, namespaces and quotas, partition health. Served by the server binary itself.
- **Per-procedure metrics:** escalation rate, timeout rate, cost per run (tokens and compute), divergence
  on replay, time to repair, runs per version (to find zombies), all per version.
- **Observability:** OpenTelemetry traces (a span per run activation, step and agent call, linked across
  services, trace context propagated into step bodies), Prometheus metrics (queue depth, step latency,
  retries, non-determinism errors, partition lag, timer lag, model tokens and cost), structured logs with
  run and step ids.
- **Security:** mTLS between nodes and for workers, token authentication on the ingress (OIDC/JWT), per
  namespace authorisation (start, signal, read, propose, approve, admin), an audit log of administrative
  actions and approvals, optional client-side payload encryption through a codec (the server never needs to
  see plaintext payloads).
- **Retention:** per namespace history retention; completed run histories archived to object storage and
  queryable from the CLI.
- **Upgrades:** rolling upgrades of the cluster with a documented compatibility window of one minor version
  between nodes; storage migrations run online.

---

## 12. Agent steps and the agent crate

The split: **semantics in the core, integrations outside it.**

In the core (phase 2): the agent step kind and its journal entries; recorded model identity (provider,
model, version, parameters); budget accounting and enforcement (a run that would exceed its budget
suspends and escalates for approval instead of overspending); output schema validation (a schema failure is
a step failure with retry policy, then an escalation); replay with the recorded response; model-drift
replay.

In `aqueduct-ai` (phase 2 onwards): provider adapters behind one trait (no provider is privileged), prompt
templates, optional response caching keyed by prompt hash, streaming of partial outputs to subscribers
without journaling every token (only the final response is journaled), a ready-made approval UI and chat
webhooks for awakeables, and a documented agent-loop pattern ("plan, call tools, observe, repeat" as a
workflow with each tool call a step, a hard iteration budget, and continue-as-new for long loops).

---

## 13. Repository layout and engineering standards

Start small and split crates when there is a reason. Initial workspace:

```text
aqueduct/
  Cargo.toml                 workspace
  crates/
    aqueduct/                facade crate: `cargo add aqueduct`, re-exports the SDK
    aqueduct-core/           journal model, state machine, commands and events; no I/O, no async
    aqueduct-codec/          payload codecs and versioned journal encoding (implements the history spec)
    aqueduct-storage/        LogStore and StateStore traits; embedded implementations
    aqueduct-sim/            deterministic simulation harness and fault injection
    aqueduct-sdk/            Rust SDK: contexts, workflow and object traits, embedded engine (phase 1)
  docs/
    adr/                     NNNN-title.md
  .github/workflows/
```

Expected later, as phases require them: `aqueduct-macros`, `aqueduct-engine` (partition processor
runtime, invoker, timer service, outboxes), `aqueduct-procedure` (Starlark interpreter; spike in phase 0),
`aqueduct-protocol` (protobuf, prost, tonic), `aqueduct-server`, `aqueduct-cli`, `aqueduct-mcp`,
`aqueduct-otel`, `aqueduct-ai`, `aqueduct-raft`; and top-level `conformance/`, `examples/`, `benches/`,
`proto/`, `console/`, `xtask/`, plus `docs/guide/`, `docs/protocol.md`, `docs/guarantees.md`,
`docs/history-format.md`.

Standards:

- Rust stable, edition 2024, MSRV 1.85 (documented in `Cargo.toml`, checked in CI), `tokio` as the async
  runtime (see the runtime ADR in section 10.1), `tracing` for instrumentation.
- `#![forbid(unsafe_code)]` in every crate. An exception needs an ADR and a safety comment per block.
- `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, `cargo test`, `cargo deny check`, and (from
  phase 0) `cargo xtask sim` in CI.
- License allowlist: Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC, Zlib, Unicode-3.0. No copyleft, no
  BSL. Known pressure points to settle by ADR before phase 3: `webpki-roots` (CDLA-Permissive-2.0) in TLS
  stacks, MPL-2.0 crates, and the OpenSSL-style licenses in some crypto crates.
- Public APIs documented with examples that compile (doc tests). Every example in `examples/` runs in CI.
- Errors: `thiserror` in libraries, typed and actionable; no `unwrap` outside tests.
- All crates have `publish = false` until the owner approves publishing.
- Semantic versioning; `CHANGELOG.md` kept from the first release.
- License: dual **Apache-2.0 OR MIT**. Contributions under the Aqueduct CLA (Apache-derived license
  grant with an open source commitment), signed by pull request comment; trademark policy for the name;
  specifications under CC-BY-4.0 (ADR 0007).

---

## 14. Phased plan and acceptance criteria

### Phase 0: foundations

- ADRs: execution model (commands vs completions, concurrency, `select`), journal and history format
  (including hash chain), storage engine choice (with benchmark numbers), codec, partitioning, simulation
  approach and async runtime, error taxonomy.
- Raw fsync latency measured on the reference machine and on Linux; performance targets revised.
- `aqueduct-core` state machine for a minimal run (input, step, output) with property tests.
- `docs/history-format.md` draft, with a codec implementing it and hash-chain verification.
- Storage benchmark of redb vs fjall on the journal workload; decision recorded.
- `aqueduct-sim` skeleton running the core with simulated storage and crashes, with the first invariants.

**Done when:** core and sim run in CI; a simulated run completes correctly across 10,000 seeds with crash
injection; ADRs merged.

### Phase 1: embedded MVP (code workflows)

- Rust SDK: workflows, steps with retry and timeout, concurrent steps (`join`, `select`), `now`, `uuid`,
  `random`, sleep, signals, awakeables, escalations, sagas, cancellation, continue-as-new.
- `Engine::embedded`: single process, durable storage, group commit, recovery on restart, sticky execution,
  idempotent starts with reuse policies.
- Non-determinism detection with parked runs; CLI basics against an embedded data directory (list,
  describe, journal, verify, signal, complete awakeable, replay locally, export).
- OpenTelemetry traces and metrics.
- Examples: hello world that survives `kill -9`; order saga; human approval with an awakeable; the
  reference ingest pipeline (extract, map, resolve, score, wait for review).

**Done when:** all invariants hold over 1,000 seeds per PR; the crash test harness passes 1,000 random
kills; embedded performance targets measured and recorded; the fifteen-minute README path works on a clean
machine.

### Phase 2: procedures and agents

- Procedures in Starlark (ADR 0008): the full set of engine functions, schema checks, lints, a supported
  termination mechanism (upstream fuel API or a recursion check), the watchdog, and AST caching.
- Procedures and code workflows calling each other; child runs; queries and updates; `patched`.
- Agent steps in the core; `aqueduct-ai` with at least two provider adapters behind one trait.
- Replay modes: counterfactual replay with diff reports; model-drift replay.
- Version lifecycle with provenance, approvals, policies, draining and retirement; per-version metrics.
- MCP server for proposing, validating, replaying, starting, inspecting and escalation handling.
- Examples: a procedure crystallised from recorded agent-handled cases, proven by counterfactual replay and
  approved; a procedure with one bounded agent step and a budget.

**Done when:** a procedure written by an agent through the MCP server can be validated, replayed against a
corpus, approved and run end to end in embedded mode; the interpreter's determinism invariant holds over
1,000 seeds per PR; no recorded agent call is re-issued under crash injection.

### Phase 3: single server and the open protocol

- Service protocol v1 (draft, not yet frozen), step-worker protocol, ingress gRPC and HTTP/JSON, pull
  workers and push endpoints.
- Step-worker libraries for TypeScript and Python.
- Deployments with version pinning and draining across workers; named task queues for steps.
- Virtual objects with keyed state; durable outboxes and inboxes.
- Namespaces, concurrency and rate limits, budgets, priorities, fair scheduling, backpressure.
- Authentication and authorisation (including propose and approve), audit log.
- `aqueduct migrate` from embedded to server.

**Done when:** the conformance suite passes for the Rust SDK and the step-worker libraries over both
in-process and network transports; single server targets measured; simulation covers network faults
between workers and server.

### Phase 4: cluster

- Raft replicated partition logs, snapshots, log trimming, object storage for snapshots and archives.
- Metadata store, membership, partition leadership balancing, rolling upgrades.
- Timer wheel at scale; history archival and retention.
- Web console.

**Done when:** simulation of a 5-node cluster with partitions, crashes and clock skew passes 100,000 seeds
nightly; cluster targets measured; a documented failover drill on real machines.

### Phase 5: more SDKs and format freeze

- A full replay SDK for TypeScript, if demand justifies it, passing the full conformance suite.
- Protocol v1, procedure format v1 and history format v1 frozen with the owner's approval.

### 1.0 release criteria

Frozen protocol, procedure, storage and history formats with a tested upgrade path and a published
longevity commitment; published guarantees document; external Jepsen-style test report; complete user
guide; conformance passing for the Rust SDK and at least two step-worker libraries; security review of the
ingress, the approval path and the cluster transport.

---

## 15. Non-goals

- A BPMN engine or a visual low-code designer. Procedures are text, written by agents and people and
  reviewed like code. A visual view in the console is a rendering, not an editor.
- A general agent framework or an agent orchestrator. Agents explore elsewhere; Aqueduct runs what they
  crystallise and bounds the judgement inside it.
- A model gateway or provider wrapper beyond what agent steps need.
- A message queue, event streaming platform or stream processor.
- Large-scale data processing (batch analytics, dataframes). Steps call such systems; the engine does not
  replace them.
- Hosted cloud service. The project is software people run themselves.

---

## 16. Open questions to settle by ADR

1. Step bodies in the workflow's process by default, or always through task queues?
2. redb or fjall as the embedded storage engine (phase 0 benchmark decides)?
3. Default payload codec: JSON, a binary serde format, or JSON in development and binary in production?
4. Fixed partition count with later resharding, or virtual partitions from the start?
5. openraft or an in-house Raft implementation tuned for the log workload?
6. How much of the push endpoint and procedure formats to align with existing ecosystems (for example the
   CNCF Serverless Workflow specification), so that users can migrate in?
7. Async runtime strategy for deterministic simulation (section 10.1).
8. Procedure language: settled as Starlark (ADR 0008). Open: termination mechanism in starlark-rust, and
   removing its `rustyline` dependency upstream (BSL-1.0 is allowed for it by exception meanwhile).
9. Hash function and signature scheme for the history hash chain and approvals.
10. Longevity commitment for the history format: how many years, how many format versions.
11. License policy for CDLA-Permissive-2.0 and MPL-2.0 dependencies.

---

## 17. References

- *The River and the Canal*, raskell.io: the essay behind the project's direction.
- Temporal documentation: https://docs.temporal.io
- Restate documentation and architecture: https://docs.restate.dev and https://github.com/restatedev/restate
- DBOS: https://www.dbos.dev
- Obelisk: https://github.com/obeli-sk/obelisk
- FoundationDB, testing with simulation: https://apple.github.io/foundationdb/testing.html
- TigerBeetle, deterministic simulation testing: https://tigerbeetle.com
- The Raft consensus algorithm: https://raft.github.io
- openraft: https://crates.io/crates/openraft
- turmoil: https://github.com/tokio-rs/turmoil
- redb: https://github.com/cberner/redb
- fjall: https://github.com/fjall-rs/fjall
- Starlark specification: https://github.com/bazelbuild/starlark/blob/master/spec.md
- starlark-rust: https://github.com/facebook/starlark-rust
- CNCF Serverless Workflow: https://serverlessworkflow.io
- Anthropic, Building effective agents: https://www.anthropic.com/engineering/building-effective-agents
- EU AI Act, Article 12, record-keeping: https://artificialintelligenceact.eu/article/12/
- SQLite long-term support: https://www.sqlite.org/lts.html
- Kyle Kingsbury, Jepsen analyses: https://jepsen.io
