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
