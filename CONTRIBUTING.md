# Contributing

Thank you for considering a contribution to Aqueduct.

Aqueduct keeps other people's work safe through crashes, restarts and long waits. A bug here loses
someone's order, payment or approval without anyone noticing. The bar for changes is high on purpose, and
the gates below are how the project holds it.

## Where the project is

Aqueduct is in phase 1 of the [build brief](BRIEF.md): an embedded engine you can try, not yet one to
depend on. Read the brief's principles (section 3) and the [decision records](docs/adr/) before proposing a
change. Discussing a change in an issue before implementing it is always welcome, and for anything that
touches the journal, the guarantees or the public API, it is expected.

## AI-assisted contributions

Aqueduct exists to let agents do real work under rules a person can read, approve and check. Its own
development follows the same idea: contributions written with AI assistance are welcome, as long as they
respect the project's rules, directives and gates. The tools change; the standards do not.

To make that practical, the repository carries structured context for AI coding assistants:
[`CLAUDE.md`](CLAUDE.md) and the rules in [`.claude/rules/`](.claude/rules/) (architecture, invariants,
workflow, writing). They are written for people too, and apply to every contribution, however it was made.

What we ask of AI-assisted contributions:

- **A person is responsible.** The person who opens the pull request is its author: they sign the CLA, they
  have read and understood every line, and they can explain and defend it in review. "The assistant wrote
  it" is not an answer to a review comment.
- **Same gates, no shortcuts.** Every check below must pass. An assistant must never weaken, skip or delete
  a test, a simulator invariant, an injected bug or a CI step to make a build pass. If a check is wrong,
  the pull request says so and fixes the check on its own, with the reasoning.
- **The directives still apply.** The brief's principles, the ADRs, the history format specification, the
  licence allowlist and the coding rules bind generated code exactly as they bind handwritten code. A change
  that disagrees with them starts with an ADR.
- **Provenance.** Only submit what you have the right to license under the CLA. Do not submit code copied
  or reproduced from sources under incompatible licences, whoever or whatever wrote it.
- **Say how it was made.** Mention in the pull request description that AI assistance was used, and for
  what. Honest descriptions of what was checked, and what was not, matter more than polish.
- **Agents stay inside the fence.** Automated agents work on branches and through pull requests. They do not
  push to `main`, force-push, rewrite published history, merge their own pull requests without the owner's
  explicit request, change branch protection or repository settings, publish crates, or touch the CLA,
  licences and trademark policy.
- **Security.** Never paste secrets, credentials or personal data into an assistant, and never let one send
  them anywhere. Security issues are reported privately (below), not in issues, pull requests or prompts
  shared with others.

## Setup

Aqueduct uses [mise](https://mise.jdx.dev/) for its tools and tasks, like the other raskell projects. Rust
itself comes from `rust-toolchain.toml` through rustup.

```sh
git clone https://github.com/raskell-io/aqueduct.git
cd aqueduct
mise trust && mise install    # installs cargo-deny
mise tasks                    # lists every task
```

## Gates

Every change must pass all of these. CI runs the same mise tasks on every pull request, and branch
protection on `main` requires `fmt, clippy, test`, `cargo-deny`, `CLA`, `simulation` and `crash harness`.

```sh
mise run ci          # everything CI checks, one after the other
mise run ci:quick    # format, clippy and tests, before pushing
```

`mise run ci` is `fmt:check`, `lint`, `test`, `test:doc`, `doc`, `deny`, `examples`, `sim:ci` (every
simulator workload) and `crash:ci` (real `kill -9` in every mode); `mise.toml` shows the exact commands,
and [`.claude/rules/workflow.md`](.claude/rules/workflow.md) explains them. Changes to the core, codec, storage or engine are
expected to run the simulator locally, and a new failure mode should come with an invariant or an injected
bug that would catch it.

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

## Decisions

Non-obvious choices are recorded as ADRs in `docs/adr/`, numbered `NNNN-title.md`, using
[the template](docs/adr/0000-template.md). If your change disagrees with the brief or an ADR, write the ADR
first. Changes to the [history format](docs/history-format.md) need an ADR and the owner's approval.

## Dependencies

Only Apache-2.0, MIT, BSD-2/3-Clause, ISC, Zlib and Unicode-3.0 licensed dependencies are allowed
(enforced by `cargo deny`). Anything else needs an ADR and the owner's approval.

## Reporting security issues

Please do not open a public issue for a vulnerability. Report it privately through GitHub
([Security, "Report a vulnerability"](https://github.com/raskell-io/aqueduct/security/advisories/new)), or
email hello@raskell.io, with a description and, if you can, a way to reproduce it.
