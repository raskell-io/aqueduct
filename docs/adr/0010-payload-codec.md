# 0010: Payload codec: JSON by default, declared per run

- **Status:** accepted
- **Date:** 2026-10-31

## Context

The history format treats payloads (inputs, step results, outputs) as opaque bytes (ADR 0005). BRIEF.md
open question 3 asked whether payloads should be JSON, a binary serde format, or JSON in development and
binary in production. Since ADR 0008, procedures exchange JSON with the engine at their boundary.
Histories are meant to be read by auditors and other tools long after the code that wrote them.

## Decision

- **JSON (UTF-8, RFC 8259) is the default and the only codec in history format v0.** Procedures require it;
  code workflows use it unless they declare otherwise.
- **The codec becomes part of the history before the format is frozen.** Version 1 of the history format
  adds a codec identifier to `Started` (applying to every payload of the run) and an envelope for encrypted
  payloads (crypto-shredding, BRIEF.md section 6.4). Candidate binary codecs are CBOR (RFC 8949) and
  protobuf with a declared message type; each needs a specification a reader can follow without our code.
- **No silent per-environment switch.** A run's codec is fixed at start; the same run never mixes codecs.

## Consequences

- Histories are readable with standard tools today, which serves auditors and debugging.
- JSON costs space and CPU for large or binary payloads; large payloads go by reference (BRIEF.md section
  5.4) rather than through a binary codec.
- History format v1 must land before any format freeze, with an upgrade path from v0.
