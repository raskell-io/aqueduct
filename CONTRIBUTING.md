# Contributing

Aqueduct is in its design phase. Discussion of the [brief](BRIEF.md) and the
[ADRs](docs/adr/) is the most useful contribution right now.

## Contributor License Agreement

Before a contribution can be merged, its authors sign the [Aqueduct CLA](CLA.md) once, by commenting on
their pull request with:

```text
I have read the Aqueduct CLA and I hereby sign it.
```

A bot checks every pull request and explains what is missing. The CLA is a license grant adapted from
the Apache Software Foundation's agreement: you keep the copyright to your work. In return, the
maintainer commits that contributions stay available under an open source license. If you contribute
on behalf of an employer, your employer signs the [Corporate CLA](CLA-CORPORATE.md). Signed-off commits
(`git commit -s`) are welcome but not required. The reasoning is in
[ADR 0007](docs/adr/0007-licensing-and-contributions.md).

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
