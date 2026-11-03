# 0041: Authentication, authorisation and the audit log

- **Status:** accepted
- **Date:** 2026-11-03

## Context

BRIEF.md section 11: token authentication on the ingress (OIDC/JWT), per-namespace authorisation (start,
signal, read, propose, approve, admin), and an audit log of administrative actions and approvals. ADR
0038 kept the server on loopback until this exists. The owner chose static tokens first, because they
work with no outside services; OIDC/JWT follows for teams with an identity provider.

## Decision

- **Tokens.**
  - `aqueduct tokens create <dir> --name <name> --permissions <list>` prints `aqd_` and 64 hexadecimal
    digits of randomness, once.
  - The server stores only its SHA-256, with its name and permissions, in `<dir>/tokens.json`, readable
    by its owner only. Comparison takes constant time.
  - `aqueduct tokens revoke` removes one.
- **Authentication is on when the directory has a tokens file.** The file is checked on every request
  and read again when it changes, so these take effect without a restart: the first token turns
  authentication on, and a revoked one stops working. Once the server has seen tokens, a missing or
  unreadable file refuses everything: losing the file must not open the server.
- **A request carries its token** as `authorization: Bearer <token>`, an HTTP header or gRPC metadata.
- **Permissions:** `read` (wait, describe, list, query, list escalations), `start`, `signal` (signals,
  awakeables, updates), `cancel`, `resolve`, `work` (step and workflow workers), and `admin` (everything).
  Propose and approve stay with `aqueduct versions` on the data directory until version management is
  offered over the network.
- **One middleware guards every route**, gRPC and HTTP alike, and maps it to the permission it needs. A
  route not listed needs `admin`, so a new route is closed until it is given a permission. Refusals are
  `401`/`403` over HTTP and `UNAUTHENTICATED`/`PERMISSION_DENIED` over gRPC.
- **The audit log** is `<dir>/audit.log`: JSON Lines with the time, who (a token's name), the action, the
  target and the outcome, each line carrying the SHA-256 of the previous one.
  - **What it records:** start, signal, completing an awakeable, cancel, update, resolve, token creation
    and revocation, and every refused request. Reads are not recorded unless refused.
  - **Durability:** lines are synced as they are written, under an exclusive file lock, so the server
    and the command line can both write. A line torn by a crash is dropped on opening.
  - **Verification:** `aqueduct audit <dir>` verifies the chain and prints it. A server does not start
    on a log whose chain is broken.
- **Workers and libraries** take a token: `Worker::token` in Rust, a `token` option in Python and
  TypeScript. The conformance suite runs its network transports with authentication on.
- **Beyond loopback still needs `--listen-anywhere`.** Over plain HTTP, tokens and payloads cross the
  network in the clear. (ADR 0042 adds TLS; beyond loopback then needs tokens and a certificate.)

## Consequences

- A server can give each client and worker only what it needs, and show afterwards who did what.
- Tokens do not expire on their own; they last until revoked. OIDC/JWT will bring short-lived ones.
- Syncing an audit line per write adds a disk sync to every start and signal. This is unmeasured so far;
  the single-server measurements later in phase 3 will show what it costs.
- The audit log proves order and integrity against later edits, not against someone who rewrites the
  whole file from the start. Anchoring its head elsewhere would close that, as for run histories.
- Namespaces (phase 3) will scope permissions per tenant.
