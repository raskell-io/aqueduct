// The conformance worker (conformance/README.md): the suite's steps, served with this library.
//
// Run by the conformance runner with AQUEDUCT_URL set:
//   cargo run -p aqueduct-conformance -- --worker "node sdks/typescript/conformance_worker.ts"

import { Failure, Retry, Worker } from "./src/index.ts";

const url = process.env.AQUEDUCT_URL ?? "";
const pushPort = process.env.AQUEDUCT_PUSH_PORT;

const worker = new Worker({ name: "conformance-typescript", url, capacity: 8, pollMs: 1_000 });

worker
  .step("conformance.echo", (value) => value)
  .step("conformance.fail", (message: string) => {
    throw new Failure(message);
  })
  .step("conformance.flaky", (spec: { succeed_on: number }, invocation) => {
    if (invocation.attempt < spec.succeed_on) {
      throw new Retry(`attempt ${invocation.attempt} of ${spec.succeed_on}`);
    }
    return { attempt: invocation.attempt };
  })
  .step("conformance.throws", (_input, invocation) => {
    if (invocation.attempt === 1) throw new TypeError("conformance.throws: first attempt");
    return "recovered";
  })
  .step("conformance.whoami", (_input, invocation) => ({
    run_id: invocation.runId,
    command: invocation.command,
    step: invocation.step,
    attempt: invocation.attempt,
    idempotency_key: invocation.idempotencyKey,
  }))
  .step("conformance.sleep", async (ms: number) => {
    await new Promise((resolve) => setTimeout(resolve, ms));
    return ms;
  });

if (pushPort !== undefined) {
  // A push endpoint: the server posts each invocation.
  await worker.servePush(Number(pushPort));
} else {
  process.on("SIGTERM", () => worker.stop());
  await worker.run();
}
