# Aqueduct service protocol, version 1 (draft)

Status: **draft** (ADR 0038). `aqueduct server` serves the ingress (section 2) and step workers
(section 3); `aqueduct-worker` is a Rust library for writing a step worker. Messages and routes may change until the format and protocol freeze in
phase 5; after that, changes within v1 are additive. The protobuf definitions in
[`proto/aqueduct/v1`](../proto/aqueduct/v1) are the source of truth; this document explains them and
gives the HTTP/JSON form of each call.

## 1. Conventions

- **Payloads** (inputs, outputs, signal and update payloads, step inputs and results) are bytes in the
  run's codec, JSON by default (ADR 0010). Over gRPC they are `bytes` fields. Over HTTP/JSON they are
  embedded as JSON values.
- **Durability.** A call is answered only once what it did is durable: a start, a signal, a
  cancellation or a resolution survives a crash once it is acknowledged.
- **Run keys** are non-empty strings without NUL. A key can have several runs over time (generations);
  calls by key address the latest. A **run id** is the key for its first run, and
  `<key>\0<generation>` after that (the history format, section 4).
- **Ids for deduplication.** Signals and updates take an optional `id`: sending again with the same id
  delivers once. This makes retries after a timeout safe.
- **Errors** are gRPC status codes, or HTTP status codes with a JSON body `{"error": "..."}`:
  - `NOT_FOUND` / 404: no such run, workflow or version;
  - `ALREADY_EXISTS` / 409: a key conflict;
  - `INVALID_ARGUMENT` / 400: a payload is not JSON, or a key is invalid;
  - `DEADLINE_EXCEEDED` / 504: a wait or an update timed out;
  - `FAILED_PRECONDITION` / 422: an update was rejected, or a resolution does not match its schema;
  - `UNAVAILABLE` / 503: the server is at a limit or stopping; retry later (`retry-after`);
  - `RESOURCE_EXHAUSTED` / 429: the caller is over its rate limit; retry after `retry-after` seconds;
  - `UNAUTHENTICATED` / 401: no token, or one the server does not know;
  - `PERMISSION_DENIED` / 403: the token lacks the permission.
- **Authentication** (ADR 0041). If the server's data directory has tokens, every request carries one:
  `authorization: Bearer <token>`, as an HTTP header or gRPC metadata. The permissions are `read` (wait,
  describe, list, query, list escalations), `start`, `signal` (signals, awakeables, updates), `cancel`,
  `resolve`, `work` (step and workflow workers) and `admin` (everything). Calling an object's handler needs
  `signal`; reading its state, `read`.

## 2. Ingress (`aqueduct.v1.Ingress`)

| call | HTTP/JSON | body | answer |
|---|---|---|---|
| `Start` | `POST /v1/runs` | `{"key", "workflow", "input", "version"?, "budget_tokens"?, "reuse"?}` | `{"run_id", "version"}` |
| `Wait` | `GET /v1/runs/{key}/result?timeout_ms=` | | `{"status", "output", "error", "version"}` |
| `Describe` | `GET /v1/runs/{key}` | | the run: status, version, input, output, commands |
| `ListRuns` | `GET /v1/runs?workflow=&status=&limit=` | | `{"runs": [...]}` |
| `Signal` | `POST /v1/runs/{key}/signals/{name}` | `{"payload", "id"?}` | `{}` |
| `CompleteAwakeable` | `POST /v1/awakeables/{id}` | `{"payload"}` | `{}` |
| `Cancel` | `POST /v1/runs/{key}/cancel` | `{"reason"?}` | `{}` |
| `Query` | `GET /v1/runs/{key}/queries/{name}` | | `{"value"}` |
| `Update` | `POST /v1/runs/{key}/updates/{name}` | `{"payload", "id"?, "timeout_ms"?}` | `{"result"}` or 422 `{"error"}` |
| `ListEscalations` | `GET /v1/escalations` | | `{"escalations": [{"id", "key", "reason", "payload"}]}` |
| `ResolveEscalation` | `POST /v1/escalations/{id}` | `{"outcome"}` | `{}` |
| `CallObject` | `POST /v1/objects/{object}/{key}/{handler}` | `{"args", "id"?, "timeout_ms"?}` | `{"output"}`, or 422 `{"error"}` if the handler failed |
| `GetObjectState` | `GET /v1/objects/{object}/{key}` | | `{"state"}` |

`reuse` is `"return_existing"` (the default), `"reject"` or `"allow_after_completion"`. Statuses are
`"running"`, `"completed"`, `"failed"`, `"parked"` and `"continued"`. Ids containing `#` or a NUL are
percent-encoded in paths.

For example, with `curl`:

```sh
curl -s localhost:7470/v1/runs -d '{"key": "claim-7", "workflow": "claims", "input": {"amount": 400}}'
curl -s 'localhost:7470/v1/runs/claim-7/result?timeout_ms=10000'
```

## 3. Step workers (`aqueduct.v1.Workers`)

A step worker implements steps by name in any language.

### 3.1 Over gRPC

The worker opens the `Work` stream and sends a `Hello` with:

- its name;
- the step names it implements;
- its capacity: how many invocations it runs at once.

Then it receives `StepInvocation`s, never more than its outstanding credit. For each, it answers with
a `StepResult` holding the invocation's `invocation_id` and one of:

- **`completed`:** the step's result (in the run's codec);
- **`failed`:** a terminal failure; the step fails, whatever its retry policy;
- **`retry`:** a retryable failure, retried by the step's retry policy with durable backoff.

Each finished invocation returns one unit of credit implicitly. A `credit` message adds more.

### 3.2 Over HTTP long polling

| call | body | answer |
|---|---|---|
| `POST /v1/workers/poll` | `{"worker", "steps": [...], "max", "wait_ms"}` | `{"invocations": [...]}`, empty after `wait_ms` |
| `POST /v1/workers/results` | `{"results": [{"invocation_id", "completed" \| "failed" \| "retry"}]}` | `{}` |

### 3.3 Named task queues and push endpoints (ADR 0040)

- **Queues.** A step waits in a queue: by default the queue of its own name, or a named queue the
  server's configuration routes it to (`{"queues": {"billing": ["charge", "refund"]}}`). A worker takes
  from the queues of its step names, and from the named queues in its `Hello` (`queues`) or its poll
  (`"queues"`).
- **Push endpoints.** A configured endpoint (`{"push": [{"queue", "url", "concurrency", "timeout_ms",
  "headers"}]}`) receives a queue's steps, one `POST` per invocation, with a JSON body of the same shape
  as a polled invocation, and an `aqueduct-idempotency-key` header. It answers `200` with
  `{"completed": <any JSON>}`, `{"failed": "..."}` or `{"retry": "..."}`. Any other status, a timeout,
  or an answer of another shape is a retryable failure.

### 3.4 Delivery

- **Leases.** An invocation is leased to one worker for `lease_ms`. If its result does not arrive
  within the lease, or the worker's stream ends, the invocation is offered again: to the same worker
  or another, with the next `attempt` number.
- **At least once.** A step body can therefore run more than once. It should use `idempotency_key`
  (`<run key>/<epoch>/<command>`, the same for every attempt) with any external system it changes.
- **Recorded exactly once.** The first result recorded for a command wins. A later one (from a lease
  that expired, or a duplicate) is acknowledged and dropped.

## 4. Workflow workers (`aqueduct.v1.Workflows`, ADR 0039)

A workflow worker runs a code workflow's logic outside the server. It opens the `Serve` stream with a
`WorkflowHello` naming its workflows and its capacity. It then receives `Activation`s:

- the run id and the workflow;
- `at`, how many history entries the decision is for;
- those entries as history-format records, hash-chained to the run id;
- `now_ms` and `seed` for values the logic records.

The worker verifies the chain, replays the history through the logic, and answers with a `Decision`:
every command the logic has issued, in order (`WorkflowCommand`). The server checks it against the
journal as it does for local logic.

- **Stale decisions are dropped.** A decision for a history that is no longer the run's (the run moved
  on, or the decision is a repeat) is dropped, and the run is activated again. Late, lost and repeated
  decisions are therefore harmless.
- **Activations are leased** like step invocations.
- **A worker that cannot decide** returns its credit instead of a decision.
- **Not yet:** queries on remotely decided workflows, and validation of updates on receipt.

## 5. Not in this draft yet
- **Push endpoints:** the server calls an HTTP endpoint per invocation, for serverless platforms.
- **Named task queues** and deployments across workers.
- **OIDC/JWT, mTLS and namespaces.** The server has TLS (ADR 0042), and binds to loopback by default;
  beyond loopback it needs tokens and a certificate.
- **Version management** over the network (register, propose, prove, approve): for now through
  `aqueduct versions` on the server's data directory, and the MCP server.
