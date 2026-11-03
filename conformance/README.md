# Conformance suite (step workers)

Language-neutral scenarios that every step-worker library must pass (BRIEF.md section 8, ADR 0038). The
runner starts a server with the scenarios' procedures, starts the worker under test, runs every
scenario, and checks its outcome.

```sh
cargo run -p aqueduct-conformance                                  # steps in process, then the Rust worker over gRPC
cargo run -p aqueduct-conformance -- --worker "python3 sdks/python/conformance_worker.py"
cargo run -p aqueduct-conformance -- --worker "node sdks/typescript/conformance_worker.ts"
cargo run -p aqueduct-conformance -- --push "python3 sdks/python/conformance_worker.py"   # as a push endpoint
mise run conformance                                               # all of them, as CI runs them
```

The server has authentication and TLS on (ADR 0041, ADR 0042). The worker under test is started with
`AQUEDUCT_URL` set to the server's address (for example `https://127.0.0.1:50123`), `AQUEDUCT_TOKEN` to a
token with the `work` permission, which it must present, and `AQUEDUCT_CA_FILE` to the server's
self-signed certificate, which it must trust (`NODE_EXTRA_CA_CERTS` is set to it as well, for Node). It must connect, implement the steps below, and keep running until it is
killed. With `--push`, it is started with `AQUEDUCT_PUSH_PORT` instead and must serve the steps as a push
endpoint (ADR 0040) on `127.0.0.1:$AQUEDUCT_PUSH_PORT`: the steps are routed to the named queue
`conformance`, and the server posts each invocation to it.

## The steps a conformance worker implements

Inputs and results are JSON.

| step | does |
|---|---|
| `conformance.echo` | returns its input unchanged |
| `conformance.fail` | fails terminally, with its input (a string) as the error |
| `conformance.flaky` | fails retryably (a retry) until its attempt is `input["succeed_on"]`, then returns `{"attempt": <attempt>}` |
| `conformance.throws` | on its first attempt, raises an exception (or panics, or throws) without classifying it; afterwards returns `"recovered"`. The library must turn an unclassified error into a retry. |
| `conformance.whoami` | returns `{"run_id", "command", "step", "attempt", "idempotency_key"}` from the invocation |
| `conformance.sleep` | waits `input` milliseconds, then returns `input` |

## The scenarios

| scenario | procedure | expected output |
|---|---|---|
| `json` | `step("conformance.echo", <a value with nested lists, objects, numbers, null, booleans and non-ASCII text>)` | the same value |
| `failure` | `step("conformance.fail", "boom", fallible = True)` | `{"ok": false, "error": "boom"}` |
| `retry` | `step("conformance.flaky", {"succeed_on": 3})` | `{"attempt": 3}` |
| `exception` | `step("conformance.throws", None)` | `"recovered"` |
| `identity` | `step("conformance.whoami", None)` | `{"run_id": "identity", "command": 0, "step": "conformance.whoami", "attempt": 1, "idempotency_key": "identity/0/0"}` |
| `parallel` | four `conformance.sleep` steps of 200 ms in `parallel`, then `conformance.echo` of their results | `[200, 200, 200, 200]` |

A scenario passes when its run completes with the expected output within 30 seconds. Retries use a
backoff of 10 ms, so `retry` and `exception` finish quickly.

## Transports

The runner checks the steps implemented in the server's own process (the in-process transport), the
Rust `aqueduct-worker` over gRPC, the same worker also deciding the scenarios' workflows (the
workflow-worker protocol, ADR 0039: the server keeps only the journals), and any worker command given
with `--worker` (the Python and TypeScript libraries use HTTP long polling).
