# 0059: The web console

- **Status:** accepted
- **Date:** 2026-11-07

## Context

BRIEF.md section 11 asks for a web console served by the server: runs with filters, a run's journal,
escalations, versions and approvals, metrics, namespaces, partition health. ADR 0051 step 8 starts it.

## Decision

- **One page at `/console`**, built into the server binary (`include_str!`), with no build step and no
  dependencies. It calls the server's own HTTP/JSON API (`/v1/...`) with the token a person types in (kept
  in the tab's session storage) and, optionally, a namespace: it can do no more than that token may.
- **The page holds no secrets** and is served without authentication, outside the token check, like the
  routes between servers. It is locked down by its headers: a content security policy allowing only its
  own inline script and style and requests to its own origin, no frames, no referrer, no caching. Every
  value from the server is inserted as text, never as HTML.
- **First version:** runs (filter by key, workflow or status), a run's status, input, output, error and
  commands with their outcomes, cancelling and signalling it; open escalations, resolved with a JSON
  outcome; deployments with their state, open runs and workers.

## Consequences

- Tested: the page is served with its policy while the API still refuses a request without a token
  (`aqueduct-server/tests/console.rs`); looked at in a browser against a running server (listing,
  opening a run, signalling it to completion, the escalations and deployments tabs, no errors).
- Not yet: versions and approvals, per-version metrics, namespaces and quotas, partition health in a
  cluster, the journal entry by entry (the run's commands stand in for it), updates and queries.
- In a cluster, the page's requests are forwarded to each partition's leader like any other (ADR 0057).
