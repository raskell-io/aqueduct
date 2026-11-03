"""The conformance worker (conformance/README.md): the suite's steps, served with aqueduct_worker.

Run by the conformance runner with AQUEDUCT_URL set:
    cargo run -p aqueduct-conformance -- --worker "python3 sdks/python/conformance_worker.py"
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from aqueduct_worker import Failure, Retry, Worker  # noqa: E402

worker = Worker("conformance-python", url=os.environ["AQUEDUCT_URL"], capacity=8, poll_ms=1_000)


@worker.step("conformance.echo")
def echo(value, invocation):
    return value


@worker.step("conformance.fail")
def fail(message, invocation):
    raise Failure(message)


@worker.step("conformance.flaky")
def flaky(spec, invocation):
    if invocation.attempt < spec["succeed_on"]:
        raise Retry(f"attempt {invocation.attempt} of {spec['succeed_on']}")
    return {"attempt": invocation.attempt}


@worker.step("conformance.throws")
def throws(_, invocation):
    if invocation.attempt == 1:
        raise RuntimeError("conformance.throws: first attempt")
    return "recovered"


@worker.step("conformance.whoami")
def whoami(_, invocation):
    return {
        "run_id": invocation.run_id,
        "command": invocation.command,
        "step": invocation.step,
        "attempt": invocation.attempt,
        "idempotency_key": invocation.idempotency_key,
    }


@worker.step("conformance.sleep")
def sleep(ms, invocation):
    time.sleep(ms / 1000)
    return ms


worker.run()
