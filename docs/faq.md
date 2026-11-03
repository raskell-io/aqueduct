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
- **The engine's API is blocking.** Inside a Tokio application, call `start`, `wait` and the rest through
  `spawn_blocking`.
- **Workflows are `async fn`, but they do not run on an async runtime.** The engine polls a workflow once
  per activation, replaying it from its journal. You cannot `.await` Tokio I/O or timers inside one; that
  work belongs in a step, and durable waits use `ctx.sleep` and similar.
- **Registered step functions are synchronous closures.** To call async code from one, block on it
  yourself.

**Current limits as a dependency:**

- **It is not on crates.io.** Every crate has `publish = false`, so depend on it by git or path. The API
  will still change.
- **It is heavy for a library:** about 200 crates, much of that from Starlark. The `aqueduct` crate always
  includes procedures and the registry, with no feature flag to leave them out.
- **One process per data directory.** The engine holds the state store open while it runs.
- **Some newer APIs are not exported from `aqueduct`.** `StepCall`, the remote queues and the replay check
  are reachable only through the `aqueduct-engine` crate.

**You can also mix the two.** A Rust program can embed the engine and serve it over the network with
`aqueduct-server`, which does use Tokio, or run its steps and workflows elsewhere with `aqueduct-worker`.
