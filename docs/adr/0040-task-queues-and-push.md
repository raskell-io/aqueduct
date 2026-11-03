# 0040: Named task queues and push endpoints

- **Status:** accepted (the protocol is a draft until phase 5)
- **Date:** 2026-11-03

## Context

BRIEF.md section 6.6: pull workers hold a stream and receive work; **push endpoints** are called per
invocation, which makes serverless platforms first-class. The phase 3 plan adds **named task queues**
for steps. ADR 0038 made step names the queues until then.

## Decision

- **A step waits in a queue.** By default it is the queue of its own name. The server's configuration
  can route steps to a **named task queue** (`{"queues": {"billing": ["charge", "refund"]}}`;
  `Builder::step_route`). A step is in at most one queue; the configuration is refused otherwise.
- **Workers take from queues:** those of the step names they implement, and the named queues they list
  (`Hello.queues`, the poll's `"queues"`, `Worker::queue`). A worker that names a step but not the
  queue it is routed to does not get it: routing decides.
- **Push endpoints.** The configuration can name an HTTP endpoint for a queue:
  `{"queue", "url", "concurrency" (8), "timeout_ms" (30,000), "headers"}`. A pusher in the server is a
  worker for that queue, with these properties:
  - **Concurrency:** it takes invocations up to its concurrency, under the same leases as any worker.
  - **The request:** it posts each invocation as JSON, in the same shape as a polled one, with an
    `aqueduct-idempotency-key` header.
  - **The answer:** a `200` with `{"completed": <any JSON>}`, `{"failed": "..."}` or `{"retry": "..."}`
    is the outcome. Any other status, a timeout, a connection error or an answer of another shape is a
    retryable failure, so a misconfigured or cold endpoint heals by the step's retry policy.
- **HTTP client:** hyper with rustls on ring and the operating system's roots, as ADR 0038 decided.
  `cargo deny` passes unchanged.
- **Configuration:** `aqueduct server <dir> --config server.json`. In Rust: `Config::apply` routes the
  engine's steps, and `serve_with` starts the pushers.
- **Libraries:**
  - Python: `handle_push(body)` turns a request body into a status and an answer, for a function
    handler; `serve_push(port)` serves it with the standard library's HTTP server.
  - TypeScript: the same as `handlePush(body)` and `servePush(port)`.
- **Conformance:** the suite runs each library as a push endpoint (`--push`), with the steps routed to
  the named queue `conformance`. The Rust worker over gRPC takes them from that queue too.

## Consequences

- Steps can run on serverless platforms without a long-lived worker, and several worker pools can
  share one server, each on its queues.
- A push endpoint's timeout should be below the lease, so a slow request becomes a retry before the
  step is offered to someone else.
- **Not decided here:**
  - deployments with version pinning and draining across workers: workers announcing which version of a
    workflow or step set they run, and the server routing pinned runs to them;
  - push endpoints for workflow activations;
  - a console view of queues.
