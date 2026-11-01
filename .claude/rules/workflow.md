# Workflow

## The gates

Every change passes all of these before it is merged. CI runs them, and branch protection on `main` requires
`fmt, clippy, test`, `cargo-deny`, `CLA`, `simulation` and `crash harness`.

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test --workspace --doc
cargo deny check

# The deterministic simulator (CI runs these seed counts)
cargo xtask sim --seeds 10000
cargo xtask sim --seeds 2000 --workload starlark
cargo xtask sim --seeds 2000 --workload timers
cargo xtask sim --seeds 2000 --workload rust

# Real crashes
cargo xtask crash --kills 200
cargo xtask crash-engine --kills 50
cargo xtask crash-engine --kills 30 --os
cargo xtask crash-engine --startup --kills 200

# The examples (CI runs them)
cargo run --release -p aqueduct --example quickstart
cargo run --release -p aqueduct --example approval -- --auto
cargo run --release -p aqueduct --example order_saga
cargo run --release -p aqueduct --example ingest -- --auto
```

A failing seed is reproduced with the command the simulator prints (`cargo xtask sim --seed N --workload W`).
Fix the cause; never change the seed range or loosen an invariant to make it pass.

## How changes reach `main`

1. Work on a branch, one topic per branch.
2. Run the gates locally, including the simulator for anything in core, codec, storage or engine.
3. Open a pull request that says what changed, why, what was checked and what was not.
4. The owner reviews and merges. Agents never merge their own pull requests unless the owner asks for that
   merge, and never push to `main`, force-push, rewrite published history, change branch protection or
   repository settings, or publish crates.

## When to write an ADR

Before a change that disagrees with the brief or an ADR, adds a journal entry or command, changes the
history format, adds a dependency with a new licence, or changes a guarantee. Use
`docs/adr/0000-template.md` and the next number.

## Measurements

Performance claims cite a file in `docs/measurements/` (machine, tool, command, raw tables, two runs where
variance matters). `cargo run --release -p aqueduct-bench-engine` measures the embedded engine.

## Security and secrets

- Never commit or paste secrets, tokens or personal data, and never send them to external services.
- Dependencies come from crates.io only, with licences on the allowlist in `deny.toml`.
- Report vulnerabilities privately (see CONTRIBUTING.md), not in issues or pull requests.
