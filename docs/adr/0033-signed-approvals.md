# 0033: Signed approvals

- **Status:** accepted
- **Date:** 2026-11-02

## Context

BRIEF.md sections 2 and 4.5: an approval is "a signed, recorded decision by a named person to activate a
version"; provenance and approvals are recorded as journaled, signed events; hashing and signing use the
script's exact bytes plus its metadata record. BRIEF.md section 16, question 9, leaves the signature
scheme open. The registry (ADR 0030) attributes and hash-chains its events, but a name is not a
signature: anyone who can write to the data directory can append an approval in someone else's name, or
rewrite the whole chain.

## Decision

- **Ed25519**, through `ed25519-dalek` 2.2 (BSD-3-Clause, the long-audited line), with strict
  verification (weak keys and non-canonical signatures are refused). SHA-256 stays the hash, so question 9
  is settled: SHA-256 for chains and version ids, Ed25519 for signatures. Ed25519 is small, fast,
  deterministic (no randomness when signing), standard (RFC 8032) and verifiable by many independent
  implementations, which matters for records that must stay checkable for decades. Keys are written
  `ed25519:` and 64 hex digits, signatures as 128 hex digits.
- **What is signed: an approval statement**, plain text, one field per line:

  ```text
  aqueduct approval v1
  workflow "claims"
  version 3f2a…            (the SHA-256 of the script's exact bytes, ADR 0029)
  approver "grace"
  replay 9c41…             (the SHA-256 of the replay report approved, or none)
  note "ok for the pilot"  (or null)
  ```

  Free text is JSON-quoted, so a note cannot pose as another field. The statement names the version and
  the evidence the person looked at, not the time or the event's place in the log, so it can be signed on
  another machine from what `aqueduct versions statement` prints. Each approval now records the report it
  approved, signed or not, and **only approvals of the version's current report count**: proving a
  version again asks for its approvals again.
- **Identities**: `registry/identities.json`, written by hand, maps names to their public keys
  (`{"grace": ["ed25519:…"]}`); several keys per person allow rotation. `--identities <file>` (and
  `Registry::set_identities`) reads them from elsewhere instead.
- **Checks, in three places:**
  - **when an approval is made**, a signature must verify over the statement and its key must be one of
    the approver's; the policy's new `require_signatures` (default false) refuses unsigned approvals;
  - **when the registry is opened**, every signature in the log is verified again, whatever the
    identities: a forged or altered signed approval, even in a fully rewritten chain, fails to open;
  - **when versions are deployed** (`Registry::deploy`, `deciders`), where the policy requires
    signatures, each deployed version's valid signed approvals (of its current report, by allowed
    approvers, with keys the identities trust now) must still meet the policy. A version approved by a
    forged `approved` event, or by a key since removed, is not run.
- **The CLI:** `aqueduct keys generate <file> --name <name>` writes a key file readable by its owner only
  (never over an existing file) and prints the public key; `keys public`; `keys sign <key> <statement>`.
  `versions approve` takes `--key <file>` to sign there, or `--signature ed25519:<key>:<sig>` for a
  signature made elsewhere. `show` and `log` say for each approval whether it is signed, with whose key,
  and whether it counts.
- **No signing over MCP** (ADR 0032): a server holding a person's key would let its agent approve in
  their name.

## Consequences

- With `require_signatures`, an approval cannot be made in someone's name without their key, and a
  version without enough of them is not deployed, even by someone who can rewrite the registry.
- **The identities file is the trust anchor.** Whoever can change it can add a key in anyone's name. It
  belongs under review, like the policy: in version control, or outside the data directory with
  `--identities`. Per-namespace authorisation (phase 3) takes over this role.
- **Removing an approval, or the log, is still possible** for someone who can write to the directory; a
  signature proves an approval was given, not that every one was kept. An outside anchor for the chain
  head (published, or countersigned) is future work.
- Other registry events (register, propose, prove, activate, retire) stay attributed and hash-chained
  but unsigned. Activation is mechanical once a version is approved; signing provenance and replay
  reports can follow the same pattern.
- Keys are raw Ed25519 seeds in files. SSH (`ssh-keygen -Y sign`) or hardware keys can sign
  statements outside Aqueduct once their signature formats are accepted; the statement does not change.
- Registry logs written before this change have approvals without a recorded report; they open, but
  those approvals no longer count towards new approvals of a proven version.
