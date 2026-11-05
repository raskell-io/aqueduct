# 0046: Namespaces

- **Status:** accepted
- **Date:** 2026-11-05

## Context

BRIEF.md section 6.6 asks for namespaces, per-tenant quotas and fair scheduling. ADR 0045 protects a server
as a whole; one tenant can still use up what it protects, read another's runs, or answer another's steps.

## Decision

**A namespace is a part of a run's stored key.** A tenant's key `k` is stored as `$ns/<namespace>/k`, and
what derives from a key (children, generations, object loops and calls, escalation and awakeable ids) stays
in its namespace. The `default` namespace is stored without a prefix, so logs from before namespaces keep
working, and a server without tokens behaves as it did. There is no separate table to fall out of step with
the journal, and recovery needs nothing new.

**Who is in which.**

| caller | header `aqueduct-namespace` | keys in | serves as a worker |
|---|---|---|---|
| token bound to `acme` | none, or `acme` | `acme` | `acme` |
| token bound to `acme` | another | refused: 403 / `PERMISSION_DENIED` | |
| token for `*`, or no tokens | `acme` | `acme` | `acme` |
| token for `*`, or no tokens | none | `default` | all |

`aqueduct tokens create --namespace <name|*>` binds a token; without it, a token is in `default`.

**Isolation.** Clients and logic give keys in their own namespace, and get them back without the prefix.

- A key that names a namespace or an internal run outside the caller's is answered as what it is to the
  caller: no such run (404 / `NOT_FOUND`). Lists and escalations hold only the caller's own.
- Logic cannot leave its namespace. A key it chooses is scoped to the sender's namespace; a key that names a
  namespace or an internal run fails the command. Only an object's loop may name reserved keys, and only in
  its own namespace.
- Workers receive only what is in their scope, and **answers are checked too**. Invocation and activation
  ids contain the run's key, so they can be guessed: a result or decision from outside the run's namespace
  is dropped as one the queue does not need.
- A push endpoint names the namespace it serves, or `*`. Without one it serves `default`, so no tenant's
  payloads reach an endpoint that did not ask for them.
- A token whose namespace cannot be read is refused rather than treated as unrestricted.

**Quotas.** Under `limits.namespaces.<name>`: `requests_per_second` and `burst` shared by all of the
namespace's callers (workers exempt, as in ADR 0045), and `max_running_runs`. These add to the per-caller
and server-wide limits. Object loops are not counted as running runs; calls are.

**Fairness.** Workers serving several namespaces take steps by stride scheduling, in proportion to
`limits.namespaces.<name>.weight` (default 1). Each namespace has a virtual time that advances by `1/weight`
per step taken, and the lowest backlogged one goes next. A namespace that returns from idle catches up to
the others: idling earns no credit, so it cannot then starve them. A noisy namespace's backlog does not delay
another's steps by more than its share.

## Consequences

- **Workflow definitions are shared.** Every namespace can start every registered workflow, and a
  registered object or workflow has one definition. Namespaces isolate runs and data, not code.
- **Fairness covers steps, not activations.** Workflow workers serving several namespaces take activations
  first come, first served. Activations are short, and a bound worker is unaffected, but one namespace can
  delay another's in a shared `*` pool.
- **Local steps are not fair.** A procedure's steps run on the embedded engine's local pool in order;
  fairness applies to steps taken by remote workers.
- **Disk and memory are not quotas.** Journal size per namespace is not limited, and all namespaces share a
  partition, so one namespace's fsyncs and journal growth affect the others.
- **Internal names reach workers and logic.** A worker sees `$ns/acme/k` as a run id, in its idempotency
  keys, and hash chains cover it; logic sees it as its run key. They stay within the namespace, and are
  stable, but a run's id seen by a worker differs from the key its client gave.
- **Moving a run between namespaces is not supported**: its history is chained to its key.
- **The embedded `Engine` API is not scoped.** A library user holds the whole engine; namespaces apply to
  the server's callers, and `Builder::namespace` sets only quotas and shares.
- **The default namespace can see a stored-looking key of its own** (`$ns/default/k` is a key in `default`,
  distinct from `k`). It reaches nothing else.

## Alternatives considered

- **A partition, or a data directory, per namespace.** Stronger isolation of disk and fsyncs, but it needs
  partitions the engine does not have across a single server yet, and children or messages would cross them.
- **A namespace field beside the key throughout the engine.** Every map, queue, id and journal format would
  change, and each is a place to forget it. A prefix cannot be forgotten by code that only passes keys on.
- **Weighted fair queueing with start and finish times.** The first version; a backlogged namespace could
  starve when the clock moved its start time forward. Stride scheduling has one number to get right.
- **A 403 for another namespace's key.** It would tell a tenant that the key exists.
