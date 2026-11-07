# 0061: Object storage and history archival

- **Status:** accepted
- **Date:** 2026-11-07

## Context

ADR 0051 step 7 asks for snapshots in object storage and history archival. Both need S3-compatible
storage. The owner chose our own small client over the AWS SDK: a few operations, signed with AWS
Signature Version 4, on the HTTP stack the server already has, rather than a large dependency tree.
Retention (deleting finished runs) needs a new journal entry, a history-format change awaiting the
owner's decision, so this archives without deleting.

## Decision

- **`aqueduct_server::objects::Store`:** a directory, or an S3-compatible bucket addressed path-style
  (`<endpoint>/<bucket>/<prefix><key>`): put, get, list (with continuation) and delete. Credentials come
  from environment variables the configuration names (by default `AWS_ACCESS_KEY_ID` and
  `AWS_SECRET_ACCESS_KEY`), never from the configuration file. Configured as
  `{"kind": "dir", "path": ...}` or
  `{"kind": "s3", "endpoint": ..., "region": ..., "bucket": ..., "prefix": ...}`.
- **Signature Version 4** (`sigv4`) as pure functions: canonical request, string to sign, signing key
  and signature, with HMAC-SHA256 over the `sha2` crate already used for the history format.
- **History archival:** the `"archive"` section (`{"store": ..., "every_ms": 60000}`) makes every server
  copy the histories of finished runs in the partitions it leads to `runs/<run id, percent-encoded>.history`.
  An archived history is the run's head hash followed by its records exactly as its hash chain has them
  (`docs/history-format.md`), each length-prefixed: it verifies on its own, and any changed byte fails
  it. A server lists what is archived when it starts, so a restart does not copy everything again;
  copying twice is harmless.
- **The CLI reads archives:** `aqueduct archive list <store.json> [<prefix>]` and
  `aqueduct archive show <store.json> <run id>`, which verifies before it shows.

## Consequences

- Tested: signing against two cases of AWS's own test suite (aws-c-auth `get-vanilla` and
  `post-header-key-sort`), every stage; the client against an in-memory bucket that recomputes each
  request's signature from what arrived, pages its listings, and refuses wrong credentials; archival
  end to end with a directory store (finished runs archived and equal to the server's history, a
  running run left until it ends); the CLI listing and showing an archive and refusing a damaged one.
  Not tested against a real S3, MinIO or R2 bucket.
- An archive proves itself consistent, not authentic: whoever can write to the bucket can write a
  consistent history. Compare its head with the server's (`aqueduct verify`) to know it is the same.
- Raft snapshots in object storage (for a new replica to load instead of replaying) build on the same
  store, and are next. Deleting archived runs from the server waits for the retention decision.
