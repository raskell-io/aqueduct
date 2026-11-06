# FAQ

## Can I use Aqueduct as a library, like Tokio?

Yes. The server is an optional layer: the `aqueduct` crate embeds the engine in your program and depends
on neither the server nor Tokio. It is closer to SQLite than to Tokio, though: an engine you embed, not a
runtime your whole program runs on.

```rust
let engine = Engine::builder("./data")
    .workflow("greet", Arc::new(workflow(greet)))
    .step("lookup_name", |input: &Payload| Attempt::Completed(input.clone()))
    .open()?;

engine.start("greet:u-1", "greet", input)?; // durable when it returns
let result = engine.wait("greet:u-1", Duration::from_secs(10))?;
```

[`quickstart.rs`](../crates/aqueduct/examples/quickstart.rs) is a complete program.

**How it differs from Tokio:**

- **It runs on its own threads.** One owns the log and the runs, and a small pool runs steps.
- **The engine's API is blocking.** Inside a Tokio application, use `AsyncEngine` (the `tokio` feature),
  which runs the same calls on Tokio's blocking pool. A long `wait` holds a blocking thread while it waits.
- **Workflows are `async fn`, but they do not run on an async runtime.** The engine polls a workflow once
  per activation, replaying it from its journal. You cannot `.await` Tokio I/O or timers inside one; that
  work belongs in a step, and durable waits use `ctx.sleep` and similar.
- **Registered step functions are synchronous closures.** To call async code from one, block on it
  yourself.

**Current limits as a dependency:**

- **It is not on crates.io.** Every crate has `publish = false`, so depend on it by git or path. The API
  will still change.
- **It is heavy with procedures:** about 200 crates by default, much of that from Starlark. For Rust
  workflows alone, `default-features = false` turns off the `procedures` feature and brings it to about
  75.
- **One process per data directory.** The engine holds the state store open while it runs.

**You can also mix the two.** A Rust program can embed the engine and serve it over the network with
`aqueduct-server`, which does use Tokio, or run its steps and workflows elsewhere with `aqueduct-worker`.

## Is it ready to depend on?

Not yet. It is well tested, but the APIs, the protocol and the history format will still change, and nothing is
published. Those formats freeze in phase 5 of the [build brief](../BRIEF.md). The README lists
[what is not done](../README.md#what-is-not-done).

## Do I need a server?

No. An embedded engine has the same journal, the same guarantees and the same history format. You need a server when
steps should run in other processes or languages, when several clients share one engine, or when you need tokens, an
audit log or namespaces. A server is one process with one disk: it does not survive losing that disk, because the
cluster is not built.

## How do I move from embedded to a server?

Stop the application and run `aqueduct migrate --from ./data --to https://server`, then restart the server to apply
it. The source is frozen first, so no run goes on in two places. Your Rust workflows' code does not move: their open
runs wait until a workflow worker serves them. [ADR 0047](adr/0047-migrate.md) has the limits.

## How fast is it?

On an Apple M4 with a real flush on every commit, a single server starts 12,500 to 16,700 runs per second over gRPC with
256 clients, and 8,300 to 10,300 with tokens and the audit log; on a Linux runner, 17,200 and 14,400. One client gets about 200, because each start waits for
the disk: throughput comes from many clients sharing a flush. Those runs were on a machine that was not idle, with steps
that do nothing. [The measurements](measurements/2026-11-05-server-m4.md) say how far to trust them.

## Do steps run exactly once?

No. A step runs at least once, and its result is recorded exactly once. A worker can crash after a step did its work and
before its result arrived, and nothing can tell that from a step that never ran. So a step with side effects should
be idempotent: every attempt of a step has the same idempotency key, to pass to whatever it changes.

## Can a run be changed while it is running?

Its logic can be, carefully. A run stays on the version it started on, and new runs start on the active one
([ADR 0029](adr/0029-version-pinning.md)), for code in workers too: only a worker of a run's build decides it
([ADR 0048](adr/0048-deployments.md)). A Rust workflow can branch with `ctx.patched("id")` to change code under
runs already going ([ADR 0035](adr/0035-patched.md)). A run whose logic stops matching its journal is parked for an
operator, not corrupted.
