# 0045: Overload protection

- **Status:** accepted
- **Date:** 2026-11-05

## Context

BRIEF.md section 6.6: backpressure all the way to ingress, so that starts are rejected with a retry hint
rather than queued without bound. ADR 0042 recorded a gap: anyone who can reach a server can send refused
requests, each of which wrote and synced an audit line. Namespaces, per-tenant quotas, priorities and fair
scheduling are also in the plan, but are a separate decision that builds on this one.

## Decision

Configured under `"limits"` in the server's configuration:

| limit | default | over it |
|---|---|---|
| `requests_per_second`, `burst` (per caller) | none | `429` / `RESOURCE_EXHAUSTED`, with `retry-after` |
| `max_concurrent_requests` | 256 | `503` / `UNAVAILABLE` at once, with `retry-after` |
| `max_connections` | 1,024 | new connections wait in the operating system's queue |
| `header_timeout_ms` | 10,000 | the connection is closed |
| `audited_refusals_per_second` | 10 | refused anyway, but counted rather than written |
| `max_running_runs` | none | starts of new keys and new object calls get `503`, with `retry-after` |

- **A caller** is a token's name, or `anyone` without authentication. Workers are exempt from the rate limit:
  their capacity limits them already, and limiting them would only delay finished work.
- **Concurrent requests** are shed, not queued. A waiting call holds a blocking thread, so the default stays
  below Tokio's blocking pool of 512. Streams give their place back once they are open.
- **Refused requests are bounded in the audit log.** Beyond the rate they are counted, and the next line
  written says how many were not recorded. So a flood cannot grow the log or slow the server with disk
  syncs, and the log still shows that it happened. A refused request is still answered.
- **Silent connections cannot use up the places.** A client has `header_timeout_ms` to send its first byte,
  and on HTTP/1.1 to finish its headers. A connection cap without this would make locking everyone out
  easier. To apply it, plain and TLS listeners share one accept loop on hyper. HTTP/2 keep-alive pings also
  find dead peers.
- **Backpressure on new work.** `max_running_runs` (`Builder::max_running_runs` in Rust, so embedded programs
  have it too) refuses only what would create a run: a retried start and a retried call are answered, and
  what runs already do (child runs, continue-as-new, messages) is never refused. An object key's own loop
  does not count; its calls do. The count is taken after each flush and kept up by the starts admitted since.

## Consequences

- **Measured:** on the real binary in a debug build, 8,000 refused requests from 16 threads took 1.2 s and
  left 23 audit lines (6 KB), with the chain verified and the skipped count recorded.
- **Limits are per server,** not per tenant, so one noisy caller within its rate can still fill the running
  runs. Namespaces and quotas address that.
- **The rate limit is per token, not per address.** Refused requests are cheap but are not themselves
  limited, and a flood can still use up bandwidth and connection places. Put a network limit in front of a
  server exposed to untrusted clients.
- **A client can keep a place by being slow rather than silent:** sending one byte at a time on HTTP/2, or
  holding an idle authenticated connection. The deadlines address only clients that say nothing.
- **Not limited yet:** signals to one run (kept without bound, until the journal size limits in BRIEF.md
  section 5.4 exist), and a call's inbox depth, other than through `max_running_runs` and the rate limit.
- **`aqueduct_server::serve`** stays without limits, for programs that embed the server and protect it
  themselves. `serve_with` and `serve_tls` apply them.
