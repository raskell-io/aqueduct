# 0001: Record architecture decisions

- **Status:** accepted
- **Date:** 2026-10-31

## Context

Aqueduct makes many decisions whose reasons are not visible in the code: storage engine, journal format,
determinism model, licensing. The project is meant to stay relevant for many years, and contributors (people
and agents) need to know why things are the way they are before changing them.

## Decision

Every non-obvious decision is recorded as a short ADR in `docs/adr/`, numbered `NNNN-title.md`, using
`0000-template.md`. ADRs are immutable once accepted, except for their status; a changed decision gets a
new ADR that supersedes the old one. When the brief and an implementer's judgement disagree, the
disagreement is written up as an ADR and the owner decides.

## Consequences

Decisions take a little longer to make and much less time to rediscover. The ADR list doubles as the
project's design history.
