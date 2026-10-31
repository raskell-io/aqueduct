# History format, version 0 (draft)

> This specification is licensed under [CC-BY-4.0](https://creativecommons.org/licenses/by/4.0/).
> Anyone may implement it, in software under any license, without asking (ADR 0007).

This document specifies how a run's journal is encoded. It is the contract: an implementation in any
language that follows it can read and verify an Aqueduct history without the Rust code. Version 0 is a
draft and may change; freezing it requires the owner's approval (BRIEF.md, section 0). Decisions behind it
are in [ADR 0005](adr/0005-history-format-v0.md).

## 1. Primitives

- **u8:** one byte.
- **varint:** an unsigned integer up to 2^64 - 1 in unsigned LEB128: seven bits per byte, least significant
  group first, high bit set on every byte except the last. Encodings must be minimal: a final byte of
  `0x00` after other bytes is invalid, and so is any encoding longer than ten bytes or exceeding 2^64 - 1.
- **bytes:** a varint length `n` followed by `n` raw bytes.
- **string:** `bytes` whose content is valid UTF-8.
- **hash:** 32 raw bytes, a SHA-256 digest (FIPS 180-4).
- **cmd:** a varint whose value fits in 32 bits (0 to 4,294,967,295).

## 2. Records

A run's history is a sequence of records. Each record encodes one journal entry:

```text
record = version:u8  kind:u8  prev:hash  fields
```

- `version` is `0` for this specification. Readers must reject versions they do not know.
- `kind` selects the entry type and its fields (section 3). Readers must reject unknown kinds.
- `prev` is the hash of the previous record of the same run (section 4).
- A record has no trailing bytes after its fields.

## 3. Entry kinds

| kind | entry | fields, in order |
|---:|---|---|
| 1 | `Started` | `workflow:string`, `input:bytes` |
| 2 | `StepScheduled` | `cmd:cmd`, `name:string`, `input:bytes` |
| 3 | `StepCompleted` | `cmd:cmd`, `result:bytes` |
| 4 | `StepFailed` | `cmd:cmd`, `error:string` |
| 5 | `RunCompleted` | `output:bytes` |
| 6 | `RunFailed` | `error:string` |
| 7 | `Parked` | `reason:string` |

Payload fields (`input`, `result`, `output`) are opaque to this format. Their encoding is declared by the
SDK's codec.

## 4. The hash chain

- `genesis(run_key) = SHA-256("aqueduct-history-v0" || 0x00 || run_key)`, where `run_key` is the run key's
  UTF-8 bytes.
- The first record of a run has `prev = genesis(run_key)`.
- Every later record has `prev = SHA-256(previous record's complete bytes)`.
- The run's **head** is `SHA-256(last record's complete bytes)`. Storing or publishing the head commits to
  the whole history; changing, removing or reordering any record changes it.

A history verifies if every record decodes and every `prev` matches. Verification does not need the payload
codecs.

## 5. Journal rules

A decoded history is a valid journal if:

1. The first entry is `Started`, and it occurs only once.
2. `StepScheduled` command indexes are 0, 1, 2, ... in order, without gaps.
3. `StepCompleted` and `StepFailed` refer to a scheduled command, at most once per command.
4. Nothing follows `RunCompleted`, `RunFailed` or `Parked` (in version 0; operator resume of parked runs is
   specified in a later version).

The order of completions may differ from the order of commands; it is the order in which the outcomes
were recorded.

## 6. Partition log records

On a partition's log, each journal record is wrapped with its run key, because the records of many runs are
interleaved:

```text
log_record = version:u8 (0)  run_key:string  record:bytes
```

The storage layer adds its own framing (length and CRC-32) around each log record. Framing is not part of the
history format: an exported history is the sequence of records of one run, without framing.
