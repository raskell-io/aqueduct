# Aqueduct service protocol, version 1 (draft)

Status: **draft** (ADR 0038). `aqueduct server` serves all of it: the ingress (section 2), step workers
(section 3), workflow workers (section 4), imports (section 5) and deployments (section 6). Messages and routes may change until the format and protocol freeze in
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
  - `UNAVAILABLE` / 503: the server is at a limit or stopping, or (in a cluster) does not lead the key's partition;
    retry later (`retry-after`). A server that does not lead names the one that does in `aqueduct-leader` (a header,
    or gRPC metadata), and forwards HTTP/JSON requests and unary `Ingress` calls there itself (ADR 0056, 0057), so a
    client sees this only while a partition has no leader;
  - `RESOURCE_EXHAUSTED` / 429: the caller or its namespace is over its rate limit; retry after `retry-after` seconds;
  - `UNAUTHENTICATED` / 401: no token, or one the server does not know;
  - `PERMISSION_DENIED` / 403: the token lacks the permission, or a namespace is not one it may name.
- **Authentication** (ADR 0041). If the server's data directory has tokens, every request carries one:
  `authorization: Bearer <token>`, as an HTTP header or gRPC metadata. The permissions are `read` (wait,
  describe, list, query, list escalations), `start`, `signal` (signals, awakeables, updates), `cancel`,
  `resolve`, `work` (step and workflow workers) and `admin` (everything, imports, and cluster administration). Calling an object's handler
  needs `signal`; reading its state, `read`. Every refusal, and every change, is in the audit log.
- **Namespaces** (ADR 0046). A token is bound to a namespace, or to `*`. Keys, run ids and escalation ids are
  in the caller's namespace: another's are `NOT_FOUND`. A request may name its namespace in an
  `aqueduct-namespace` header or metadata. A bound token may name only its own (otherwise, or for an invalid
  name, `PERMISSION_DENIED`); a `*` token, or any caller of a server without tokens, chooses, and without the
  header is in `default`. A worker serves its token's namespace, or the one it names; a `*` token that names
  none serves all. Results and decisions for runs outside that are dropped. Workers see a run's stored id
  (`$ns/<namespace>/<key>`, `<key>` in `default`), not the client's key.

## 2. Ingress (`aqueduct.v1.Ingress`)

| call | HTTP/JSON | body | answer |
|---|---|---|---|
| `Start` | `POST /v1/runs` | `{"key", "workflow", "input", "version"?, "budget_tokens"?, "reuse"?, "priority"?}` | `{"run_id", "version"}` |
| `Wait` | `GET /v1/runs/{key}/result?timeout_ms=` | | `{"status", "output", "error", "version"}` |
| `Describe` | `GET /v1/runs/{key}` | | the run: status, version, input, output, commands |
| `ListRuns` | `GET /v1/runs?workflow=&status=&limit=` | | `{"runs": [...]}` |
| `Signal` | `POST /v1/runs/{key}/signals/{name}` | `{"payload", "id"?}` | `{}` |
| `CompleteAwakeable` | `POST /v1/awakeables/{id}` | `{"payload"}` | `{}` |
| `Cancel` | `POST /v1/runs/{key}/cancel` | `{"reason"?}` | `{}` |
| `SetPriority` | `POST /v1/runs/{key}/priority` | `{"priority"}` | `{}` |
| `Query` | `GET /v1/runs/{key}/queries/{name}` | | `{"value"}` |
| `Update` | `POST /v1/runs/{key}/updates/{name}` | `{"payload", "id"?, "timeout_ms"?}` | `{"result"}` or 422 `{"error"}` |
| `ListEscalations` | `GET /v1/escalations` | | `{"escalations": [{"id", "key", "reason", "payload"}]}` |
| `ResolveEscalation` | `POST /v1/escalations/{id}` | `{"outcome"}` | `{}` |
| `CallObject` | `POST /v1/objects/{object}/{key}/{handler}` | `{"args", "id"?, "timeout_ms"?}` | `{"output"}`, or 422 `{"error"}` if the handler failed |
| `GetObjectState` | `GET /v1/objects/{object}/{key}` | | `{"state"}` |

`priority` (ADR 0049) is a number, higher first, 0 by default: workers are given the highest priority among a
namespace's waiting steps and activations, and waiting raises it by one every 10 seconds. Starting a key that has a running
run sets that run's priority. `SetPriority` changes it while the run runs, with the `start` permission; children and
successors inherit it. `Describe` includes it.

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
- optionally, named task queues to take from as well (section 3.3).

Then it receives `StepInvocation`s, never more than its outstanding credit. For each, it answers with
a `StepResult` holding the invocation's `invocation_id` and one of:

- **`completed`:** the step's result (in the run's codec);
- **`failed`:** a terminal failure; the step fails, whatever its retry policy;
- **`retry`:** a retryable failure, retried by the step's retry policy with durable backoff.

Each finished invocation returns one unit of credit implicitly. A `credit` message adds more.

### 3.2 Over HTTP long polling

| call | body | answer |
|---|---|---|
| `POST /v1/workers/poll` | `{"worker", "steps": [...], "queues"?: [...], "max", "wait_ms"}` | `{"invocations": [...]}`, empty after `wait_ms` |
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
- **Namespaces.** An endpoint's `"namespace"` is the one it serves, or `"*"` for all. Without it, it serves
  `default`.

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
- **Deployments** (ADR 0048). A worker may announce a build, `deployment` in its `WorkflowHello`. A run pinned to a
  build is offered only to workers announcing that build; a run not pinned, to any. See section 6.

## 5. Imports (`aqueduct.v1.Imports`, ADR 0047)

`aqueduct migrate` uses this; there is no HTTP/JSON form. `Upload` is one client stream, and needs `admin` in the
default namespace: an `ImportBegin`, then runs (a run's history in the history format, which may be split, the last
part saying so), sources and registry files, then an `ImportCommit` with the counts sent. It is staged, and applied
when the server next starts; a stream without its commit leaves nothing. Refusals: `INVALID_ARGUMENT` for a history
that does not verify, a key outside the default namespace or counts that do not match; `FAILED_PRECONDITION` if a key
has another run, if an import is already waiting for a restart, or if another is being uploaded; `ABORTED` if the
stream ended without its commit. A run the server already has, as it was or gone on from there, is counted in
`already_there`, so an import can be repeated.

## 6. Deployments (`aqueduct.v1.Deployments`, ADR 0048)

A deployment is a build of a remote workflow. New runs start on the active build and stay pinned to it, in `version`.
All of this needs `admin` in the default namespace.

| call | HTTP/JSON | body | answer |
|---|---|---|---|
| `ListDeployments` | `GET /v1/deployments` | | `{"deployments": [{"workflow", "build", "state", "open_runs", "workers"}]}` |
| `ActivateDeployment` | `POST /v1/deployments/{workflow}/{build}/activate` | `{"force"?}` | `{}` |
| `RetireDeployment` | `POST /v1/deployments/{workflow}/{build}/retire` | | `{}` |

`state` is `"active"`, `"draining"`, `"retired"`, or `"unlisted"` for a build that runs or workers name and that was
never activated. Refusals are `FAILED_PRECONDITION` / 422: no connected worker announces the build (and no `force`),
the build was retired, it is active, or it has open runs. `NOT_FOUND` / 404 if the workflow is not remote.

A step worker may announce a build too, `deployment` in its `Hello` or poll. It then takes steps of runs pinned to that
build and of runs not pinned. Without one, it takes any step.

## 7. Cluster administration (ADR 0063)

HTTP/JSON only; needs `admin`. Any server answers, and forwards a change to the partition's leader.

| route | body | answer |
|---|---|---|
| `GET /v1/admin/partitions` | | `{"partitions": [{"partition", "members", "leads_here", "leader"}]}`, as the asked server knows them |
| `POST /v1/admin/partitions/{partition}/members` | `{"add": <node>}` or `{"remove": <node>}` | `{}`; 422 while another change of that group is not committed |

Servers talk to each other on `/cluster/v1/raft` and `/cluster/v1/remote` (ADR 0056, 0057): not for clients, they
need the cluster's secret, a compatible version (ADR 0060) and, with mutual TLS, a certificate from the cluster's CA
(ADR 0064). The web console's page is `GET /console` (ADR 0059).

## 8. Not in this draft yet

- **OIDC/JWT, and client certificates.** The server has static tokens (ADR 0041) and TLS (ADR 0042); servers of a
  cluster authenticate each other with mutual TLS (ADR 0064). It binds to loopback by default; beyond loopback it
  needs tokens and a certificate.
- **Version management** over the network (register, propose, prove, approve): for now through `aqueduct versions` on
  the server's data directory and the MCP server. A change takes effect when the server next starts.
- **For workflow workers:** HTTP long polling, push endpoints, and queries.
