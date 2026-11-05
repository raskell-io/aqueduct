# 0042: TLS on the server

- **Status:** accepted
- **Date:** 2026-11-03

## Context

ADR 0041 gave the server tokens, but over plain HTTP they and every payload cross the network in the
clear, so the server stayed on loopback. BRIEF.md section 11 also asks for mTLS between nodes and for
workers.

## Decision

- **`aqueduct server <dir> --tls-cert <file.pem> --tls-key <file.pem>`** serves gRPC and HTTP/JSON over
  TLS on one port, with rustls on ring (ADR 0038), offering HTTP/2 and HTTP/1.1 by ALPN. In Rust:
  `Tls::from_files` and `serve_tls`.
- **Listening beyond loopback needs tokens and a certificate.** Without either, the server refuses to
  start, and says which is missing. `--listen-anywhere` overrides that, for a network that is already
  protected, such as behind a proxy that terminates TLS.
- **Handshakes run per connection, under a 10-second timeout,** so a client that never finishes one
  holds up nobody else.
- **Clients trust the operating system's roots**, plus a private CA or self-signed certificate if given
  one: `Worker::ca_certificate` in Rust, `ca_file` in Python, `NODE_EXTRA_CA_CERTS` in Node.
- **The conformance suite runs its network transports over TLS**, with authentication on.
- **Test certificates** come from `rcgen`, on ring. rustls's own PEM parser reads certificate files, so
  the unmaintained `rustls-pemfile` is not needed. `cargo deny` passes unchanged.

This also corrects the audit log: a refused request now names who was refused (the token's holder, or
`unauthenticated`), rather than `anyone`.

## Consequences

- A server can be reached over a network with its tokens and payloads protected.
- **Replacing a certificate needs a restart.** It is read when the server starts.
- **Push endpoints are as safe as their URLs:** an `http://` endpoint receives its headers and payloads
  in the clear. The configuration does not refuse one.
- **Known limit, since closed by ADR 0045: refused requests were not rate-limited.** Each one wrote and
  synced an audit line, so a flood grew the log and slowed the server, and connections were not limited.
- **mTLS** (client certificates, and between nodes) is not done; workers authenticate with tokens.
