# 0007: Licensing, contributions, trademarks and specifications

- **Status:** accepted (the CLA texts are drafts pending legal review)
- **Date:** 2026-10-31

## Context

Aqueduct is a personal and community project with no commercial plan. Its differentiator is that banks,
insurers and governments can adopt it without a license review fight (BRIEF.md, section 1.3), and it is
meant to stay relevant for many years. The repository started with Apache-2.0 OR MIT and the Developer
Certificate of Origin. The owner decided to keep the option of changing the license later, which the
DCO alone does not provide once others contribute: every contributor would have to agree.

Options considered for the code license: Apache-2.0 OR MIT; Apache-2.0 only; MPL-2.0; AGPL-3.0 for the
server with a permissive SDK; source-available licenses (BSL, FSL). Options for contributions: DCO only;
a Contributor License Agreement (CLA).

## Decision

1. **Code:** Apache-2.0 OR MIT, unchanged. Maximum adoption, the Rust ecosystem norm, and an explicit
   patent grant through Apache-2.0.
2. **Contributions:** a CLA, adapted from the Apache Software Foundation's individual (v2.2) and
   corporate agreements: a license grant, not a copyright assignment. The recipient is the maintainer,
   Raffael Schneider, with a succession clause. In return the maintainer commits that every included
   contribution stays available under the project's open source license at the time or another
   OSI-approved license, so the CLA cannot be used to take contributions proprietary.
   - Individuals sign by commenting a fixed sentence on their pull request; the in-repository workflow
     (`.github/workflows/cla.yml`, `.github/cla/check.js`) records signatures on the `cla-signatures`
     branch and sets the `CLA` commit status.
   - The workflow is written in-repository on `actions/github-script` because the common CLA action
     (`contributor-assistant/github-action`) was archived in 2026.
   - Signed-off commits (DCO) are welcome but no longer required.
3. **Trademarks:** a short policy (`TRADEMARKS.md`) reserves the name for the project and its unmodified
   releases. Forks must use another name. This protects the project more than a copyleft license would.
4. **Specifications:** `docs/history-format.md` and later format and protocol specifications are
   licensed under CC-BY-4.0, with explicit permission to implement them in software under any license.
   Histories must be readable without depending on this codebase.

## Consequences

- The license can be changed later for all code, subject to the open source commitment in the CLA.
- Some contributors and companies decline to sign CLAs. The open source commitment and the familiar
  Apache-derived text reduce, but do not remove, that friction.
- The CLA texts must be reviewed by a lawyer before the first external contribution is merged.
- The CLA workflow is not exercised until an external pull request arrives; its logic is tested against a
  mocked GitHub API.
- If the project later moves to a legal entity or foundation, the succession clause lets the maintainer
  assign the agreements to it.
