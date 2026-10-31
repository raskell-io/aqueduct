<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/brand/aqueduct-lockup-light.svg">
    <img alt="aqueduct" src="docs/brand/aqueduct-lockup-dark.svg" width="480">
  </picture>
</h1>

<p align="center">
  <strong>A deterministic engine for procedures that agents write and people approve.</strong><br>
  It starts as a library inside your binary, grows into a replicated cluster without changing your code,
  and every run leaves a history you can replay and audit.
</p>

> **Status: phase 1.** An embedded engine you can try, not yet one to depend on: APIs and the history
> format will still change. It runs [Starlark procedures](docs/adr/0008-procedure-language-starlark.md)
> and [Rust workflows](docs/adr/0014-rust-sdk.md) with durable timers, exactly-once signals, journaled
> retries and checkpoints, survives `kill -9`, and is tested by a deterministic simulator on every change.
> See the [build brief](BRIEF.md) and the [decision records](docs/adr/).

## Try the simulator

```sh
cargo xtask sim --seeds 10000   # every seed injects crashes, torn writes, fsync failures and lossy networks
cargo xtask sim --seed 42       # reproduce one seed exactly
cargo xtask crash --kills 1000  # SIGKILL a log writer at random moments; no acknowledged record may be lost
cargo xtask crash-engine        # SIGKILL the embedded engine mid-run; every acknowledged run must end correctly
cargo xtask fsync               # measure fsync latency and group commit throughput on this machine
```

## Inspect a data directory

With the engine stopped, the `aqueduct` command line reads an embedded engine's data directory:

```sh
cargo run -p aqueduct-cli -- runs ./data                     # every run: workflow, status, journal length
cargo run -p aqueduct-cli -- describe ./data order-1         # status, input, output, each command and its attempts
cargo run -p aqueduct-cli -- journal ./data order-1          # the journal, entry by entry
cargo run -p aqueduct-cli -- verify ./data                   # check every hash chain, print each run's head
cargo run -p aqueduct-cli -- export ./data order-1           # the journal as JSON Lines
cargo run -p aqueduct-cli -- replay ./data order-1 new.star  # would this procedure version diverge from the run?
```

## The idea

Agents are good at exploring: unfamiliar systems, ambiguous documents, cases nobody has seen before.
Much of the work organisations do is the opposite: the same case, handled the same way, every time, with a
record anyone can check afterwards. Aqueduct is built for the hand-over between the two:

1. Agents explore new kinds of work.
2. When work recurs, an agent writes it down as a **procedure**: short, typed, readable.
3. The procedure is **replayed against past cases** before anyone trusts it.
4. A named person **approves** it.
5. Aqueduct **runs it deterministically**, with bounded, budgeted **agent steps** where judgement is
   genuinely needed, and journals everything.
6. Escalations and drift send it back to an agent for **repair**, or it is **retired**.

Engineers who need full expressive power write **code workflows** in Rust instead; both share one engine,
one journal format and one set of guarantees.

## Planned guarantees

- Acknowledged work is never lost; a step's result is recorded exactly once.
- Procedures are deterministic by construction; code workflows are checked by replay.
- Histories are tamper-evident, exportable and specified independently of the code.
- The same semantics embedded, on a single server and in a cluster.
- The whole engine is tested by deterministic simulation with fault injection in CI.

See [BRIEF.md](BRIEF.md) for the full design and the phased plan.

## The name and the mark

An aqueduct carries water from wild sources across terrain nobody can change, on a gentle, constant
gradient, and some have done so for two thousand years. See [ADR 0002](docs/adr/0002-project-name.md).

<img align="right" src="docs/brand/aqueduct-icon.svg" width="96" alt="">

The mark is a fractal arch: one arch carrying two, each carrying two, standing on a line of water. It is
the Pont du Gard reduced to its idea, a structure that carries, repeated at every scale, which is also
how Aqueduct is meant to work: steps inside procedures, procedures inside larger ones, the same loop at
every scale. The water is what the structure exists to carry. The wordmark is set in lowercase
[Syne](https://gitlab.com/bonjour-monde/fonderie/syne-typeface). Every file, the palette and the rules for
using them are in the [brand guide](docs/brand/README.md); `python3 scripts/brand.py` regenerates them.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at
your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this
project by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions. Contributions require signing the [Contributor License Agreement](CLA.md);
see [CONTRIBUTING.md](CONTRIBUTING.md). The name is covered by the [trademark policy](TRADEMARKS.md),
and the [history format specification](docs/history-format.md) is licensed under CC-BY-4.0 so that anyone
can implement it.
