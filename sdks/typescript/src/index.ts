/**
 * A step worker for Aqueduct: implement steps in TypeScript and serve them to a server.
 *
 * The worker uses the step-worker protocol's HTTP long polling (docs/protocol.md, section 3; ADR 0038)
 * and only what Node provides (`fetch`). It says which steps it implements, runs what the server sends
 * it, at most `capacity` at once, and sends back each outcome:
 *
 * ```ts
 * import { Failure, Worker } from "@aqueduct/worker";
 *
 * const worker = new Worker({ name: "billing", url: "http://127.0.0.1:7470" });
 * worker.step("charge", async (amount: number) => {
 *   if (amount <= 0) throw new Failure("nothing to charge"); // a terminal failure
 *   return { charged: amount };                              // the result (JSON)
 * });
 * await worker.run();
 * ```
 *
 * The same steps can be served as a **push endpoint** instead (ADR 0040): the server posts each
 * invocation to it, which suits serverless platforms. `worker.handlePush(body)` turns a request body
 * into the answer; `worker.servePush(port)` serves it with `node:http`.
 *
 * A step may run more than once (a lease runs out, a result is lost), so it should be idempotent:
 * `invocation.idempotencyKey` is the same for every attempt of a step. Throwing `Failure` fails the
 * step for good; throwing `Retry`, or anything else, makes the server retry it by its retry policy.
 *
 * Written with erasable syntax only, so Node (22.18 or later) runs it without a build step.
 */

/** A terminal failure: the step fails, whatever its retry policy. */
export class Failure extends Error {
  override name = "Failure";
}

/** A retryable failure: the server retries the step by its retry policy, with durable backoff. */
export class Retry extends Error {
  override name = "Retry";
}

/** One attempt of a step, as the server sent it. */
export interface Invocation {
  readonly runId: string;
  readonly command: number;
  readonly step: string;
  readonly attempt: number;
  readonly idempotencyKey: string;
  readonly leaseMs: number;
}

/** A step's implementation: from its input (JSON) to its result (JSON). */
export type Step = (input: any, invocation: Invocation) => unknown | Promise<unknown>;

export interface WorkerOptions {
  /** A name for the worker, for logs and the console. */
  name: string;
  /** The server, for example `http://127.0.0.1:7470`. */
  url: string;
  /** How many steps it runs at once. Default 4. */
  capacity?: number;
  /** How long one poll waits for work, in milliseconds. Default 10,000. */
  pollMs?: number;
}

interface RawInvocation {
  invocation_id: string;
  run_id: string;
  command: number;
  step: string;
  input: unknown;
  idempotency_key: string;
  attempt: number;
  lease_ms: number;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** A step worker: a set of steps by name, run against a server. */
export class Worker {
  readonly name: string;
  readonly url: string;
  readonly capacity: number;
  readonly pollMs: number;
  #steps = new Map<string, Step>();
  #stopping = false;
  #abort = new AbortController();

  constructor(options: WorkerOptions) {
    this.name = options.name;
    this.url = options.url.replace(/\/+$/, "");
    this.capacity = Math.max(1, options.capacity ?? 4);
    this.pollMs = options.pollMs ?? 10_000;
  }

  /** Implements step `name`. */
  step(name: string, step: Step): this {
    this.#steps.set(name, step);
    return this;
  }

  /** Stops taking work; `run` resolves once the steps in flight have answered. */
  stop(): void {
    this.#stopping = true;
    this.#abort.abort();
  }

  /** Serves the steps until `stop` is called: `capacity` loops, each taking one step at a time. */
  async run(): Promise<void> {
    await Promise.all(Array.from({ length: this.capacity }, () => this.#loop()));
  }

  /** Answers one pushed invocation (a request body): the HTTP status and the answer's body. */
  async handlePush(body: string): Promise<{ status: number; body: string }> {
    let raw: RawInvocation;
    try {
      raw = JSON.parse(body);
      if (typeof raw.invocation_id !== "string" || typeof raw.step !== "string") {
        throw new Error("missing invocation_id or step");
      }
    } catch (error) {
      return { status: 400, body: JSON.stringify({ error: `not an invocation: ${error}` }) };
    }
    return { status: 200, body: JSON.stringify(await this.#execute(raw)) };
  }

  /** Serves the steps as a push endpoint on `host:port` (every path); resolves when it is listening. */
  async servePush(port: number, host = "127.0.0.1"): Promise<import("node:http").Server> {
    const http = await import("node:http");
    const server = http.createServer((request, response) => {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", async () => {
        const answer = await this.handlePush(Buffer.concat(chunks).toString("utf8"));
        response.writeHead(answer.status, { "content-type": "application/json" });
        response.end(answer.body);
      });
    });
    await new Promise<void>((resolve) => server.listen(port, host, resolve));
    return server;
  }

  async #post(path: string, body: unknown): Promise<any> {
    const response = await fetch(this.url + path, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
      signal: this.#abort.signal,
    });
    if (!response.ok) {
      throw new Error(`${path}: ${response.status} ${await response.text()}`);
    }
    return response.json();
  }

  async #loop(): Promise<void> {
    const steps = [...this.#steps.keys()].sort();
    while (!this.#stopping) {
      let polled: { invocations?: RawInvocation[] };
      try {
        polled = await this.#post("/v1/workers/poll", {
          worker: this.name,
          steps,
          max: 1,
          wait_ms: this.pollMs,
        });
      } catch {
        // The server is unreachable or restarting (or the worker is stopping): try again shortly.
        if (!this.#stopping) await sleep(1_000);
        continue;
      }
      for (const raw of polled.invocations ?? []) {
        await this.#answer(await this.#execute(raw));
      }
    }
  }

  async #execute(raw: RawInvocation): Promise<Record<string, unknown>> {
    const invocation: Invocation = {
      runId: raw.run_id,
      command: raw.command,
      step: raw.step,
      attempt: raw.attempt,
      idempotencyKey: raw.idempotency_key,
      leaseMs: raw.lease_ms,
    };
    const answer: Record<string, unknown> = { invocation_id: raw.invocation_id };
    const step = this.#steps.get(raw.step);
    if (step === undefined) {
      answer.retry = `this worker does not implement \`${raw.step}\``;
      return answer;
    }
    try {
      const result = await step(raw.input, invocation);
      answer.completed = result === undefined ? null : result;
    } catch (error) {
      if (error instanceof Failure) {
        answer.failed = error.message;
      } else if (error instanceof Retry) {
        answer.retry = error.message;
      } else {
        // An unclassified error is retryable.
        answer.retry = error instanceof Error ? (error.stack ?? error.message) : String(error);
      }
    }
    return answer;
  }

  async #answer(result: Record<string, unknown>): Promise<void> {
    // A lost result is made up for by the step being offered again; a few tries are enough.
    for (const delay of [0, 200, 1_000]) {
      await sleep(delay);
      try {
        await this.#post("/v1/workers/results", { results: [result] });
        return;
      } catch {
        if (this.#stopping) return;
      }
    }
  }
}
