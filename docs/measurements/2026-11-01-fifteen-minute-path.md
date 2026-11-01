# The fifteen-minute README path

- **Date:** 2026-11-01
- **Goal (BRIEF.md section 2, phase 1 "done when"):** a developer who has never heard of durable execution
  goes from `cargo add aqueduct` to a workflow that survives `kill -9` in under fifteen minutes, following
  the README.
- **Machine:** Apple M4 (10 cores), macOS 26.5.1, Rust 1.99.1 already installed with rustup.
- **Method:** the README's steps, typed literally into a new project outside the repository, with an empty
  `CARGO_HOME` (every dependency downloaded and compiled from scratch) and an empty `HOME` (no global git
  or cargo configuration). The program is `crates/aqueduct/examples/quickstart.rs` copied to `src/main.rs`,
  as the README says. The first run is killed with `kill -9` two seconds into its five second sleep, then
  started again.

## First attempt: one gap

Following the README as it was, the build failed: the complete program uses `serde_json`, which the README's
dependency block did not mention. The compiler's message says what to do (`cargo add serde_json`), so a
newcomer would recover in a minute, but the README should not need that. It now lists both dependencies as
`cargo add` commands, says where to put the program, and how long the sleep is.

## Second attempt, with the fixed README

| step | machine time |
|---|---:|
| `cargo new`, `cargo add aqueduct --git ...`, `cargo add serde_json`, copy the program | 3 s |
| first `cargo build` (cold: all dependencies downloaded and compiled) | 24 s |
| first `cargo run`, killed with `kill -9` after 2 s of the sleep | 2 s |
| second `cargo run`: the run continues and finishes at the sleep's original wake time | 4 s |
| **total** | **33 s** |

The second run printed `Completed: "welcomed acct-ada"`. The first had printed nothing before it was killed.

## What this does and does not show

- The machine part of the path takes well under a minute on this machine; a slower laptop might take a few
  minutes for the cold build. Reading the README and copying the program is the rest of the fifteen
  minutes, and is not measured here.
- Rust was already installed. Installing it with rustup adds a minute or two.
- The person following the path was not a newcomer. The criterion is met for the instructions; it should
  still be confirmed once by someone who has not seen the project, on a machine that has never built it.
- A machine-specific detail that a newcomer would not meet: a global git configuration that rewrites
  GitHub URLs to SSH host aliases breaks Cargo's built-in git fetch. Cargo's
  `net.git-fetch-with-cli = true` (or removing the rewrite) avoids it.
