# @aqueduct/worker (TypeScript)

A step worker for [Aqueduct](../../README.md): implement steps in TypeScript and serve them to an
Aqueduct server. It needs Node 22.18 or later, which runs TypeScript directly, and has no dependencies.

```ts
import { Failure, Worker } from "@aqueduct/worker";

const worker = new Worker({ name: "billing", url: "http://127.0.0.1:7470", capacity: 4 });
worker.step("charge", async (amount: number) => {
  if (amount <= 0) throw new Failure("nothing to charge"); // fails the step for good
  return { charged: amount };                              // the result, as JSON
});
await worker.run();
```

- A step's input and result are JSON values.
- Throwing `Failure` fails the step terminally. Throwing `Retry`, or anything else, makes the server
  retry the step by its retry policy, with durable backoff.
- A step may run more than once (its lease runs out, or its result is lost), so make it idempotent:
  `invocation.idempotencyKey` is the same for every attempt of a step, and `invocation.attempt` counts
  them from 1.
- At most `capacity` steps run at once; `worker.stop()` makes `run()` resolve.

The source is written with erasable syntax only (`tsconfig.json` checks it with `erasableSyntaxOnly`), so
there is no build step. The worker uses the step-worker protocol's HTTP long polling
([docs/protocol.md](../../docs/protocol.md), section 3). It passes the
[conformance suite](../../conformance/README.md):

```sh
cargo run -p aqueduct-conformance -- --worker "node sdks/typescript/conformance_worker.ts"
```

Not published yet; the protocol is a draft until phase 5.
