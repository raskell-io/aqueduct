"""A step worker for Aqueduct: implement steps in Python and serve them to a server.

The worker uses the step-worker protocol's HTTP long polling (docs/protocol.md, section 3; ADR 0038)
and only the standard library. It says which steps it implements, runs what the server sends it on a
pool of threads, and sends back each outcome::

    from aqueduct_worker import Worker, Failure

    worker = Worker("billing", url="http://127.0.0.1:7470", capacity=4)

    @worker.step("charge")
    def charge(amount, invocation):
        if amount <= 0:
            raise Failure("nothing to charge")   # a terminal failure
        return {"charged": amount}              # the result (JSON)

    worker.run()

The same steps can be served as a **push endpoint** instead (ADR 0040): the server posts each invocation
to it, which suits serverless platforms. ``worker.handle_push(body)`` turns a request body into the
answer; ``worker.serve_push(port)`` serves it with the standard library's HTTP server.

A step may run more than once (a lease runs out, a result is lost), so it should be idempotent:
``invocation.idempotency_key`` is the same for every attempt of a step. Raising ``Failure`` fails the
step for good; raising ``Retry``, or any other exception, makes the server retry it by its retry policy.
"""

import json
import http.server
import threading
import time
import traceback
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

__all__ = ["Failure", "Invocation", "Retry", "Worker"]


class Failure(Exception):
    """A terminal failure: the step fails, whatever its retry policy."""


class Retry(Exception):
    """A retryable failure: the server retries the step by its retry policy, with durable backoff."""


@dataclass(frozen=True)
class Invocation:
    """One attempt of a step, as the server sent it."""

    run_id: str
    command: int
    step: str
    attempt: int
    idempotency_key: str
    lease_ms: int


Step = Callable[[Any, Invocation], Any]


class Worker:
    """A step worker: a set of steps by name, run against a server."""

    def __init__(
        self,
        name: str,
        url: str,
        capacity: int = 4,
        poll_ms: int = 10_000,
        token: Optional[str] = None,
    ):
        self.name = name
        self.token = token
        self.url = url.rstrip("/")
        self.capacity = max(1, capacity)
        self.poll_ms = poll_ms
        self._steps: Dict[str, Step] = {}
        self._stopping = threading.Event()

    def step(self, name: str) -> Callable[[Step], Step]:
        """Implements step ``name``: a decorator for ``f(input, invocation)``, which returns the result."""

        def register(function: Step) -> Step:
            self._steps[name] = function
            return function

        return register

    def stop(self) -> None:
        """Stops taking work; ``run`` returns once the steps in flight have answered."""
        self._stopping.set()

    def run(self) -> None:
        """Serves the steps until ``stop`` is called. Each of ``capacity`` threads takes one step at a time."""
        threads = [
            threading.Thread(target=self._loop, name=f"{self.name}-{i}", daemon=True)
            for i in range(self.capacity)
        ]
        for thread in threads:
            thread.start()
        try:
            for thread in threads:
                while thread.is_alive():
                    thread.join(timeout=0.5)
        except KeyboardInterrupt:
            self.stop()

    def handle_push(self, body: bytes) -> Tuple[int, bytes]:
        """Answers one pushed invocation (a request body): the HTTP status and the answer's body."""
        try:
            raw = json.loads(body)
            answer = self._execute(raw)
        except (ValueError, KeyError, TypeError) as error:
            return 400, json.dumps({"error": f"not an invocation: {error}"}).encode()
        return 200, json.dumps(answer).encode()

    def serve_push(self, port: int, host: str = "127.0.0.1") -> None:
        """Serves the steps as a push endpoint on ``host:port`` (every path) until interrupted."""
        worker = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - the name http.server calls
                length = int(self.headers.get("content-length") or 0)
                status, answer = worker.handle_push(self.rfile.read(length))
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(answer)))
                self.end_headers()
                self.wfile.write(answer)

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                pass

        server = http.server.ThreadingHTTPServer((host, port), Handler)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()

    def _post(self, path: str, body: Dict[str, Any], timeout: float) -> Dict[str, Any]:
        headers = {"content-type": "application/json"}
        if self.token is not None:
            headers["authorization"] = f"Bearer {self.token}"
        request = urllib.request.Request(
            self.url + path,
            data=json.dumps(body).encode(),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read() or b"{}")

    def _loop(self) -> None:
        steps: List[str] = sorted(self._steps)
        while not self._stopping.is_set():
            try:
                polled = self._post(
                    "/v1/workers/poll",
                    {"worker": self.name, "steps": steps, "max": 1, "wait_ms": self.poll_ms},
                    timeout=self.poll_ms / 1000 + 10,
                )
            except (urllib.error.URLError, OSError, ValueError):
                # The server is unreachable or restarting: try again shortly.
                self._stopping.wait(1.0)
                continue
            for raw in polled.get("invocations", []):
                result = self._execute(raw)
                self._answer(result)

    def _execute(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        invocation = Invocation(
            run_id=raw["run_id"],
            command=raw["command"],
            step=raw["step"],
            attempt=raw["attempt"],
            idempotency_key=raw["idempotency_key"],
            lease_ms=raw["lease_ms"],
        )
        answer: Dict[str, Any] = {"invocation_id": raw["invocation_id"]}
        function = self._steps.get(invocation.step)
        if function is None:
            answer["retry"] = f"this worker does not implement `{invocation.step}`"
            return answer
        try:
            answer["completed"] = function(raw.get("input"), invocation)
        except Failure as failure:
            answer["failed"] = str(failure)
        except Retry as retry:
            answer["retry"] = str(retry)
        except Exception:  # noqa: BLE001 - an unclassified error is retryable
            answer["retry"] = traceback.format_exc(limit=5)
        return answer

    def _answer(self, result: Dict[str, Any]) -> None:
        # A lost result is made up for by the step being offered again; a few tries are enough.
        for delay in (0.0, 0.2, 1.0):
            time.sleep(delay)
            try:
                self._post("/v1/workers/results", {"results": [result]}, timeout=10)
                return
            except (urllib.error.URLError, OSError, ValueError):
                continue
