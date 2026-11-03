# 0038: The single server and protocol v1

- **Status:** accepted (the protocol is a draft until phase 5)
- **Date:** 2026-11-03

## Context

Phase 3 (BRIEF.md section 13) turns the embedded engine into a server. It covers:

- the service protocol, the step-worker protocol, and the gRPC and HTTP/JSON ingress;
- pull workers and push endpoints;
- step-worker libraries for other languages, and a conformance suite.

It also asks for decisions the brief left open:

- where step bodies run (section 16, question 1);
- the async runtime's place, given deterministic simulation (question 7);
- the licence policy for TLS stacks: the brief asks for this by ADR "before phase 3" (question 11).

## Decision

### The server

- **`aqueduct server <dir>`** (a new crate, `aqueduct-server`) serves one data directory. A
  partition thread owns the log and the runs, as in the embedded engine, and drains a request
  channel with group commit. Network front ends feed that channel.
- **The core and the partition stay synchronous and free of I/O.** tokio runs only the network edges
  (ingress, workers). So the simulator keeps driving the partition deterministically, as it does
  today (question 7).
- **One partition.** All 4,096 hash slots (ADR 0011) map to it. The cluster (phase 4) splits them.
- **What it runs:** procedures from the data directory's registry, through `Registry::deploy`, so
  only approved versions run. Code workflows in other processes need the code-workflow protocol
  (activations and commands), which gets its own ADR later in phase 3. A Rust program can also embed
  the server and register its code workflows and step functions in process.
- **It binds to loopback by default.** Until authentication exists (a later phase 3 ADR), a
  non-loopback address must be asked for explicitly.

### Where step bodies run (question 1)

- A step runs where it is implemented: in process, if the server's program registers a function for
  it; otherwise on a **step worker** that says it implements the step's name.
- A step nobody implements waits. That is a deployment problem, not a property of the run, so the
  run heals when a worker for the step connects.
- Named task queues (several worker pools for one step name, routed by deployment) come later in
  phase 3. Step names are the queues until then.

### Protocol v1 (draft)

- **Protobuf in `proto/aqueduct/v1`:**
  - `ingress.proto`: start, wait, describe, list, signal, complete awakeable, cancel, query, update,
    escalations;
  - `worker.proto`: step workers.
- **Generated code:** prost and tonic generate it, with **protox** (a protobuf compiler in Rust) in
  `aqueduct-protocol`'s build script. So building never needs `protoc`.
- **The package is `aqueduct.v1`**, a draft that may change until the phase 5 freeze. After the
  freeze, changes within v1 are additive.
- **Payloads are bytes in the run's codec,** JSON by default (ADR 0010).
- **Every call is answered only once what it did is durable**, as in the embedded engine.

### Transports

- **gRPC and HTTP/JSON on one listener.** tonic serves gRPC and axum serves HTTP/JSON, routed by
  content type.
- **HTTP/JSON is the same calls under `/v1/...`,** with payloads embedded as JSON values, usable with
  `curl`. `docs/protocol.md` lists the routes.
- **Step workers connect two ways:**
  - **over gRPC:** one bidirectional stream: a `Hello` with the step names and a capacity, then
    invocations (never more than the worker's credit) one way and results and credit the other;
  - **over HTTP long polling:** `POST /v1/workers/poll` and `POST /v1/workers/results`, for platforms
    without a gRPC stack.

  Push endpoints (the server calls an HTTP endpoint per invocation, for serverless platforms) come
  later in phase 3, on the same messages.
- **Delivery is at least once, recording exactly once:**
  - An invocation is leased to one worker for `lease_ms`. If its result does not arrive in time, or
    the worker's session ends, it is offered again.
  - The first result recorded for a command wins. A late result is dropped by the core, as today.
  - The idempotency key (`<run key>/<epoch>/<command>`) is the same for every attempt.

### Licences (question 11)

- The TLS stack is **rustls on ring**, with the operating system's roots via `rustls-native-certs`.
  It avoids:
  - **`webpki-roots`** (CDLA-Permissive-2.0);
  - **aws-lc-rs and OpenSSL**, with their OpenSSL-style licences.
- **No MPL-2.0 crate is in the stack.**
- **Verified:** tokio, tonic, prost, axum, rustls, tokio-rustls, rustls-native-certs and protox
  together come to 130 crates, and `cargo deny check` passes against the current `deny.toml`. The
  allowlist does not change.
- A dependency that would need CDLA, MPL or an OpenSSL-style licence needs an ADR and the owner's
  approval.

### Order of work in phase 3

Each item is its own PR:

1. this ADR, the proto files, `aqueduct-protocol` and `docs/protocol.md`;
2. `aqueduct-server`: the ingress over gRPC and HTTP/JSON for procedures, and `aqueduct server`;
3. step workers: the gRPC stream and HTTP long polling, leases, a Rust worker library, and simulated
   network faults between workers and the server;
4. step-worker libraries for TypeScript and Python, and the conformance suite (`conformance/`) over
   in-process and network transports;
5. the code-workflow protocol, for the Rust SDK over the network;
6. push endpoints, named task queues, and deployments with pinning and draining across workers;
7. authentication and authorisation (including propose and approve), and an audit log;
8. virtual objects, outboxes and inboxes;
9. namespaces, limits, priorities, fair scheduling and backpressure;
10. `aqueduct migrate`, and the single-server measurements.

## Consequences

- Clients and step workers in any language need only protobuf or JSON over HTTP. A step worker is a
  loop: say which steps it implements, run what arrives, send back the result.
- The embedded engine and the server share the partition and its guarantees. The server adds
  transports, leases and network failure modes, which the simulator must cover (phase 3 "done when").
- tokio is now a dependency of the server and the protocol crate, not of the core, the engine or the
  SDK.
- Until authentication exists, the server is for loopback and trusted networks only.
