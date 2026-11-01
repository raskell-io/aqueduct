# Aqueduct

A durable execution engine in Rust, built for the procedures that agents write and people approve. Every
step is journaled before it counts, so a run survives crashes, restarts and week long waits, and replays to
the same result. Procedures are written in Starlark or Rust. The direction comes from the essay
[The River and the Canal](https://raskell.io/articles/the-river-and-the-canal/): agents explore, procedures
crystallise what they find, and the engine runs them deterministically.

This file and `.claude/` are the context for AI-assisted work on Aqueduct (see
[CONTRIBUTING.md](CONTRIBUTING.md), "AI-assisted contributions"). The same rules apply to people.

## Read before changing anything

- [BRIEF.md](BRIEF.md): what Aqueduct is, its principles (section 3, non-negotiable) and its phased plan.
- [docs/adr/](docs/adr/): every non-obvious decision. If a change disagrees with an ADR, write a new ADR
  first.
- [docs/history-format.md](docs/history-format.md): the journal format. It is a specification that other
  implementations rely on; changes need an ADR and the owner's approval.

## Rules

- [.claude/rules/architecture.md](.claude/rules/architecture.md): the crates, how a run flows through them,
  and where new code belongs.
- [.claude/rules/invariants.md](.claude/rules/invariants.md): what must never break (durability,
  determinism, exactly-once recording), and how each is checked.
- [.claude/rules/workflow.md](.claude/rules/workflow.md): commands, the gates every change passes, and how
  changes reach `main`.
- [.claude/rules/writing.md](.claude/rules/writing.md): how code, docs, ADRs and pull requests are written.

## The non-negotiables, in short

1. Never lose acknowledged work: nothing is acknowledged, dispatched or released before the entries it
   depends on are durable.
2. Never record a step twice. Steps may run more than once; their results are recorded once.
3. Determinism: the core is a pure state machine; procedures are deterministic by construction; Rust
   workflows read the outside world only through the context.
4. `#![forbid(unsafe_code)]`, no `unwrap` or `expect` outside tests, permissive licences only.
5. Every change passes `mise run ci` (fmt, clippy, tests, docs, `cargo deny`, the examples, the simulator and
   the crash harness) and the CLA check. A failing check is fixed, never weakened, skipped or deleted.
6. Nothing reaches `main` without a pull request and the owner's approval. Never force-push, never push to
   `main`, never publish crates.
