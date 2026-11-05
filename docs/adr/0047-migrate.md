# 0047: Migrating a data directory to a server

- **Status:** accepted
- **Date:** 2026-11-05

## Context

BRIEF.md section 7: `aqueduct migrate --from ./data --to https://server` copies open runs, histories, versions
and object state. An embedded engine and a server use the same data directory, so moving a directory to a new,
empty server is a copy. The command is for what a copy cannot do: joining a server that has other runs, over the
network, checked, and without a run ever going on in two places.

## Decision

```text
aqueduct migrate --from ./data --to https://server --token-file admin.token [--ca ca.pem] [--dry-run]
```

1. **The source is frozen first** (`migrated.json`, durably, while holding the log's lock, so an application still
   using the directory stops the command). An engine refuses to open a frozen directory, as do the commands that
   start or change runs; reading commands still work. If a run went on in both places, its steps would run twice,
   so this comes before anything is sent. A server that cannot be reached freezes nothing.
2. **The upload is staged on the server** (`aqueduct.v1.Imports/Upload`, needs `admin`, in the default namespace).
   Each run's hash chain is verified as it arrives. The commit carries the counts it should have sent, and is
   refused if a key has another run here. A stream without its commit leaves nothing.
3. **The server applies it when it next starts.** Before recovery, so all of it arrives before any run acts. The
   operator restarts the server when ready.

**Why at the next start, rather than at once.** A server's logic is fixed when it opens (ADR 0029), and the
configuration an import needs (objects, remote workflows) is too, so a restart is needed in the common case
anyway. It also removes a real hazard: a run acting on a partial import. A parent would start a child whose history
had not arrived, a run that continued as new would start a successor that was on its way, and a sender would create
an object key whose state was still to come. Each would then conflict with what arrived.

**All of it, or none.** Applying is repaired by applying again: a run already there is skipped, and one torn by a
crash gets the rest of its records. A conflict found on applying (a key started after the commit) sets the whole
import aside under `import/rejected-<time>`, with the reason, and the server opens without it: failing to start
would be worse. The outcome is on standard error and in the audit log. The source stays frozen, and sending it again
is safe, because a run the server has, as it was or gone on from there, is counted rather than refused.

**Versions keep their history.** The source's registry is verified, and its events join the server's log with who
did what and when. They are chained again in the new log, so the source's chain is not carried over; it stays in
the source directory. Signed approvals still verify, since they are over the version and its report. A workflow with
other events here refuses the import.

**An import does not get around the server's policy.** Replaying events does not check policy, so the server asks,
of every deployable version arriving, whether its report and its approvals would pass here. If the server has no
`policy.json` or `identities.json`, it takes the source's; otherwise it keeps its own, and says so.

**Object state moves with no special handling:** an object key is a run.

`--abort` unfreezes a source whose upload did not finish, and refuses once it was committed.

## Consequences

- **The server must restart to apply it.** Until then nothing is visible, and a second import is refused.
- **Runs keep their keys, in the default namespace.** A run's chain starts from its key, so moving one into a
  namespace would mean rewriting its history. A tenant's token cannot import. Migrating into a namespace is open.
- **Rust workflows do not move, only their runs.** Their open runs wait, whole, until a workflow worker serves them
  (`remote_workflows`) or an embedded engine opens the directory. `--dry-run` and the command's output list them.
- **Steps that ran in-process go to workers now,** and retry policies set in code do not move. A step that was running
  when the source stopped runs again, under the same idempotency key, as after any crash.
- **A server has no agent executor,** so an open run's pending agent call waits.
- **Tokens, the server's configuration and the source's audit log do not move.** The server's audit log records the
  upload, and the outcome of applying it.
- **Rejection can come late.** A policy refusal or a conflict can appear at restart, with the source frozen. Nothing is
  lost: fix the cause, and send it again.
- **Size:** the source's runs are read into memory to send them, and all of a server's runs are in memory while it
  starts. A large import slows that start, and nothing limits its size beyond 64 MiB per record. This has not been
  measured on a large directory.
- **`--abort` after a lost answer:** if the commit succeeded and its answer was lost, the source looks unfinished. The
  command says so; the operator must check the server before running the source again.

## Alternatives considered

- **Apply at once, on the running server.** It needs deciders that change at runtime, and either a staging state that
  survives a crash or an order of runs that makes partial imports safe. No order does: children, successors and object
  keys depend on each other in both directions.
- **Copying the directory.** It works for an empty server, and still does. It cannot merge, and nothing would stop the
  source running on.
- **Re-chaining histories, to allow any key or namespace.** It would destroy the evidence the chain exists for.
