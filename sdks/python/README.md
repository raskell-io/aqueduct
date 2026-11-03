# aqueduct-worker (Python)

A step worker for [Aqueduct](../../README.md): implement steps in Python and serve them to an Aqueduct
server. It needs Python 3.9 or later and nothing outside the standard library.

```python
from aqueduct_worker import Failure, Worker

worker = Worker("billing", url="http://127.0.0.1:7470", capacity=4)

@worker.step("charge")
def charge(amount, invocation):
    if amount <= 0:
        raise Failure("nothing to charge")   # fails the step for good
    return {"charged": amount}              # the result, as JSON

worker.run()
```

- A step's input and result are JSON values.
- Raising `Failure` fails the step terminally. Raising `Retry`, or any other exception, makes the server
  retry the step by its retry policy, with durable backoff.
- A step may run more than once (its lease runs out, or its result is lost), so make it idempotent:
  `invocation.idempotency_key` is the same for every attempt of a step, and `invocation.attempt` counts
  them from 1.
- `capacity` threads each run one step at a time; `worker.stop()` ends `run()`.

If the server has authentication, give the worker a token with the `work` permission: `Worker(..., token="aqd_...")`. For an `https://` server with a private CA or self-signed certificate, add `ca_file="ca.pem"`.

**As a push endpoint** (for serverless platforms; ADR 0040), the same steps answer the server's
requests instead of polling: `worker.handle_push(body)` turns a request body into the status and answer to send back, for
use in a function handler, and `worker.serve_push(port)` serves it over HTTP. Configure the server to push the steps'
queue to it.

The worker uses the step-worker protocol's HTTP long polling ([docs/protocol.md](../../docs/protocol.md),
section 3). It passes the [conformance suite](../../conformance/README.md):

```sh
cargo run -p aqueduct-conformance -- --worker "python3 sdks/python/conformance_worker.py"
```

Not published yet; the protocol is a draft until phase 5.
