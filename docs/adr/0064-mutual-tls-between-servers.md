# 0064: Mutual TLS between servers

- **Status:** accepted
- **Date:** 2026-11-08

## Context

Servers of a cluster authenticate each other with a shared secret (ADR 0056): anyone who learns it, and
can reach a server, can speak Raft to it. BRIEF.md section 11 asks for mutual TLS between nodes.

## Decision

- **A cluster CA.** The cluster section's `"tls": {"ca", "cert", "key"}` names the CA that issued every
  server's certificate, and this server's certificate and key. The server must itself serve TLS
  (`--tls-cert`, `--tls-key`), and every peer's URL be `https://`.
- **The listener asks every client for a certificate and verifies one presented against the cluster's
  CA**, but serves a client without one: ordinary clients and workers connect as before. A certificate
  from another CA fails the handshake.
- **The routes between servers** (`/cluster/v1/raft`, `/cluster/v1/remote`) take only connections that
  presented a certificate the CA verified, besides the secret and a compatible version (ADR 0060);
  others are refused (401). Requests forwarded to a leader (ADR 0056, 0057) travel the same way.
- **Outgoing**, a server trusts only the cluster's CA for its peers and presents its own certificate.

## Consequences

- Tested with three servers on TLS: the cluster forms and serves a client without a certificate; on the
  routes between servers, a connection with the secret but no certificate is refused, one with a
  certificate from the cluster's CA is let through, and one from another CA fails the handshake.
  Dropping the certificate check fails the test.
- Certificates are files read at start; rotating them means a rolling restart (ADR 0060).
- The shared secret stays: it guards a cluster run without mutual TLS, and costs nothing with it.
