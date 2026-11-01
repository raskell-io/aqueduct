# 0032: The MCP server

- **Status:** accepted
- **Date:** 2026-11-01

## Context

Phase 2 (BRIEF.md section 13) needs an MCP server for proposing, validating, replaying, starting,
inspecting and escalation handling. It is done when a procedure written by an agent through the MCP
server can be validated, replayed against a corpus, approved and run end to end in embedded mode. The
pieces exist: the registry (ADR 0030), counterfactual replay (ADR 0028), per-version metrics
(ADR 0031), and the command line's offline operations on a data directory.

## Decision

- **`aqueduct mcp <dir> [--as <identity>] [--read-only]`** serves the Model Context Protocol over
  stdio: JSON-RPC 2.0, one message per line. It implements `initialize` (protocol versions 2025-11-25,
  2025-06-18, 2025-03-26 and 2024-11-05), `ping`, `tools/list` and `tools/call`. It is written by hand on
  top of `serde_json`: the protocol surface it needs is small, and an SDK would bring an async runtime
  into a synchronous tool. It lives in the CLI crate (`aqueduct_cli::mcp::Server`) and shares the
  CLI's code, so the two cannot drift apart.
- **Tools:**
  - authoring: `validate_procedure`, `replay_draft` (counterfactual replay of an unregistered
    procedure; nothing is recorded), `propose_version` (register and propose, with provenance), and
    `replay_version` (prove: the report is recorded as evidence);
  - inspection: `list_versions`, `show_version`, `diff_versions`, `list_runs`, `describe_run`,
    `run_journal`, `list_escalations` and `metrics`;
  - operations: `start_run` and `resolve_escalation`.

  Tools are annotated read-only or not, and `--read-only` serves only those that change nothing.
- **No approval and no activation over MCP.** Those are a person's decisions, made with the CLI. Until
  approvals are signed, a name given over MCP proves nothing about who decided, and the default policy's
  four eyes would mean nothing if the proposing agent could approve under another name.
- **One identity per server.** Every change is attributed to `--as` (default `agent/mcp`), as author and
  as `by`, never to a name in the request. Each agent gets its own server, or its own identity.
- **Like the CLI, it works on a stopped engine's data directory.** `start_run` (also `aqueduct start`)
  records the start and the run's first decision on the registry's active version, or on another
  approved one. Only deployable versions can be started on, as in an engine. The run continues when the
  engine next opens, and so does a run whose escalation was resolved.

## Consequences

- The phase 2 path works end to end, and a test runs it: an agent validates, proposes and proves a
  version over MCP; a person approves and activates it; the agent starts runs; the embedded engine runs
  them; the agent resolves an escalation, inspects the outcome, and proposes a second version that is
  proven against the first's runs.
- An engine that is running holds the data directory, so the MCP server cannot be used next to it.
  Serving MCP from inside a running engine, or from the single server, comes with the server (phase 3),
  where the same tools map onto its API.
- Tool results are text, the CLI's output; structured results can be added per tool without changing
  the tools.
