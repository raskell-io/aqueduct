# Contributing

Aqueduct is in its design phase. Discussion of the [brief](BRIEF.md) and the
[ADRs](docs/adr/) is the most useful contribution right now.

## Developer Certificate of Origin

Contributions are accepted under the [Developer Certificate of Origin](https://developercertificate.org/)
(DCO). There is no CLA. Sign off every commit:

```sh
git commit -s
```

This adds a `Signed-off-by` line certifying that you wrote the change or have the right to submit it under
the project's licenses (Apache-2.0 OR MIT).

## Checks

Every change must pass:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo deny check
```

## Decisions

Non-obvious choices are recorded as ADRs in `docs/adr/`, numbered `NNNN-title.md`, using
[the template](docs/adr/0000-template.md). If your change disagrees with the brief, write the ADR first.

## Dependencies

Only Apache-2.0, MIT, BSD-2/3-Clause, ISC, Zlib and Unicode-3.0 licensed dependencies are allowed
(enforced by `cargo deny`). Anything else needs an ADR and the owner's approval.
