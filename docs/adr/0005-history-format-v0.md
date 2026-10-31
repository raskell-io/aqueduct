# 0005: History format v0 and the hash chain

- **Status:** accepted (format is a draft; freezing it needs the owner's approval)
- **Date:** 2026-10-31

## Context

Histories are audit records, replay inputs and evidence for procedure approvals, and must be readable long
after the code that wrote them (BRIEF.md, section 6.4). Tamper evidence cannot be added after a format is
frozen.

## Options

- **Serde with a general binary format** (bincode, postcard): convenient, but the format is defined by Rust
  types and a library version, not by a document.
- **Protobuf:** well specified and polyglot, but not canonical (field order, default values), which makes
  hashing and signing awkward.
- **A small hand-specified encoding:** a few varints and length-prefixed byte strings, canonical by
  construction, specified in one document.

Hash: SHA-256 or BLAKE3. BLAKE3 is faster; SHA-256 is a FIPS standard that auditors and regulators already
know, is available everywhere, and is fast enough at journal rates.

## Decision

- A hand-specified, canonical encoding, documented in [docs/history-format.md](../history-format.md) and
  implemented in `aqueduct-codec`. Decoding is strict: unknown versions, unknown kinds, non-minimal varints,
  invalid UTF-8 and trailing bytes are errors.
- Every record carries the SHA-256 hash of the previous record of the same run. The first record links to a
  genesis hash derived from the run key, so a chain cannot be moved to another run.
- Payloads stay opaque bytes with a codec chosen by the SDK; the history format does not interpret them.

## Consequences

- A second implementation can read histories from the specification alone; the spec is the contract.
- Changing a run key's bytes changes the whole chain; run keys are immutable.
- The storage layer still frames records with its own length and CRC, so torn writes are detected and
  truncated before the chain is checked. A chain failure after framing succeeded is treated as corruption
  or a bug, never repaired silently.
