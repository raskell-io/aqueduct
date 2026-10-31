# 0002: Project name: Aqueduct

- **Status:** accepted
- **Date:** 2026-10-31

## Context

The project started under the working name *tsumugi* (紡ぎ, Japanese for spinning fibre into thread). The
metaphor was apt, but the name gave most people outside Japan no image, was hard to pronounce and spell,
and collided with an existing job-management project of the same name. The owner wanted a name that puts a
picture in people's heads.

Candidates considered and rejected:

- **Obelisk:** an existing Rust durable workflow engine (obeli-sk/obelisk).
- **Tapestry:** Apache Tapestry, Tapestry Inc., and several AI projects; crate taken; names a finished
  decoration rather than a process.
- **Sampo** (the mill of the Kalevala): the crate is an active Rust release tool, and Sampo plc is a large
  Nordic insurer, in the project's target market.
- **Canal:** alibaba/canal, a widely used change-data-capture tool; crate taken.
- **Cairn:** crowded with AI agent projects, including one about turning work into reusable knowledge.
- **Watermill, Waterwheel, Millwheel:** an established Go event library, an active Rust workflow scheduler,
  and Google's stream-processing paper.

## Decision

The project is called **Aqueduct**.

An aqueduct carries water from wild sources across terrain that nobody can change, on a gentle and constant
gradient, for centuries. It matches the project's central idea (agents explore the rugged world; procedures
carry what they find, reliably) and its ambition to remain useful long after it is built. The crate name
`aqueduct` was free on crates.io on 2026-10-31; the GitHub organisation repository `raskell-io/aqueduct`
was free.

## Consequences

- Crates are named `aqueduct` (facade) and `aqueduct-*`; the binary is `aqueduct`.
- Known neighbours with similar names: vigimite/aqueducts (Rust, declarative data pipelines), RunLLM/aqueduct
  (dormant since 2023), stablekernel/aqueduct (archived Dart framework).
- A trademark search in software classes (EUIPO, USPTO) is still open and should be done before the first
  release.
- The common misspelling "aquaduct" should be anticipated (documentation search terms, redirects).
