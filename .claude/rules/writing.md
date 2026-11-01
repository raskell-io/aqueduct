# Writing

## Code

- Rust 2024, MSRV 1.90 (`rust-toolchain.toml`, checked in CI). `#![forbid(unsafe_code)]` in every crate.
- Errors with `thiserror`, typed and actionable. No `unwrap` or `expect` outside tests (`clippy.toml`).
- Public items have doc comments; examples in docs compile (doc tests).
- Comments say why, not what. Prefer a clear name to a comment.
- Tests name the behaviour they check (`a_signal_with_a_known_id_is_recorded_once`).
- Keep dependencies few and boring. A new one needs a reason in the pull request; a new licence needs an
  ADR.

## Docs, ADRs and pull requests

- Plain, precise English. Short sentences. Say what is true now, and say what is not done.
- No em dashes; use a colon, a comma or two sentences. No marketing words.
- Claims are measured or cited: a guarantee names what checks it, a number names its measurement.
- ADRs follow the template: context, options, decision, consequences. Consequences include what the
  decision costs and what it leaves open.
- A pull request description says what changed, why, how it was checked, and what was not done. Mistakes
  made along the way are stated, not hidden.
