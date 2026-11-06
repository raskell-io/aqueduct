# 0048: Deployments across workers

- **Status:** accepted
- **Date:** 2026-11-05

## Context

BRIEF.md section 4.5: every run is pinned to the version it started on, and old versions drain and retire. That holds
for procedures: the registry has their code, and every version is deployed with the server (ADR 0030). It does not
hold for code workflows run by workers (ADR 0039): their runs were unpinned, and any worker for the workflow decided
them. Deploying new worker code meant a run that started on the old code could be decided by the new, and be parked.
ADR 0040 left this open: "workers announcing which version of a workflow or step set they run, and the server routing
pinned runs to them".

## Decision

**A deployment is a build of a remote workflow:** a name the operator chooses, such as a release or a commit. The server
does not have its code, only workers do.

**Workers announce their build** (`Worker::deployment("2026.11.5")`, `Hello.deployment`, `WorkflowHello.deployment`, or
`deployment` in an HTTP poll).

**New runs start on the active build, and stay pinned to it,** through the `version` a run already records (ADR 0029),
so the history format does not change. Continue-as-new moves to the active build, as for procedures.

| | activations of pinned runs | activations of unpinned runs | steps of pinned runs | steps of unpinned runs |
|---|---|---|---|---|
| worker announcing that build | yes | yes | yes | yes |
| worker announcing another build | **no** | yes | **no** | yes |
| worker announcing nothing | **no** | yes | yes | yes |

The difference between activations and steps is deliberate. A worker with the wrong code deciding a run would diverge
from its journal, so a pinned run waits for its own build, however long. A step is a request with a contract, and a
pool of step workers that know nothing of deployments is ordinary, so it may take any step. A worker that does announce
a build is saying its steps belong to that build, so it does not take another's.

**The lifecycle, by the operator:**

```text
aqueduct deployments activate order 2026.11.5 --server https://server
aqueduct deployments list --server https://server
aqueduct deployments retire order 2026.11.4 --server https://server
```

- **Activate** takes effect at once, without a restart. The active build starts draining. Activating a draining build is
  a rollback: new runs go back, and what started on the other build stays there.
- **A connected worker must announce the build,** unless `--force`: otherwise new runs would wait for a worker that may
  not exist.
- **The list** shows each build's state, its open runs and its connected workers. A draining build with no open runs is
  drained: its workers can stop.
- **Retire** needs a draining build with no open runs. A retired build cannot be activated again, and no run can start
  on it.

State is in `deployments.json` in the data directory, written and synced before it takes effect, so a run is never
pinned to a build the server forgets. All of this needs `admin` in the default namespace: workflow definitions are
shared across namespaces (ADR 0046), so a tenant's administrator must not change them for others. Changes are in the
audit log.

## Consequences

- A rollout no longer parks runs: bring up the new workers, activate, wait until the old build is drained, stop its
  workers, retire it.
- **Nothing here is the registry's approval.** Procedure versions have evidence, approvals and policy. A build is whatever
  a worker says it is, and activating needs only `admin`. The server cannot tell two workers announcing one build apart,
  or check that they run the same code. That is the operator's to get right, for example with a commit id as the build.
- **A pinned run with no worker waits, without limit,** and is only visible in the list. Retiring refuses, but nothing
  warns about a draining build whose workers have gone.
- **Workflows with no active build go on as before:** unpinned, decided by any worker.
- **Unpinned runs can still diverge.** A run started before the first activation has no build, so any worker may decide it.
- **Child runs start on their own workflow's active build,** not their parent's. They are separate runs, with a contract
  like a step's.
- **Push endpoints do not announce a build.** They take steps as a worker announcing nothing does.
- **A session announces one build,** for all it serves. Serving two builds needs two workers.
- **No canaries yet:** a start can name a draining build, but not one that was never activated, and there is no traffic
  split.
- **`aqueduct migrate` does not move deployments.** Imported runs stay pinned, and go to workers announcing their build.
- **Not simulated:** tests cover this on the real server. The simulator's workloads do not include builds.

## Alternatives considered

- **The registry for code workflows too.** It would give builds approvals and a log, but it holds sources and proofs that
  a build does not have, and its changes take effect at the next start. A rollout should not need a restart.
- **Pin steps strictly.** A step worker that announces nothing would never get a pinned run's step, which would stall every
  existing pool the moment a workflow's first build is activated.
- **Route by a task queue per build.** It needs no new concept, but the operator would repeat in configuration what the
  worker already knows, and nothing would stop the wrong code taking an activation.
