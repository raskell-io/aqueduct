# 0021: Telemetry through an observer hook; one OpenTelemetry trace per run

- **Status:** accepted
- **Date:** 2026-11-01

## Context

BRIEF.md section 3, principle 9: every run, step, timer, signal and agent call is traceable with
OpenTelemetry and inspectable from the CLI without extra setup. Phase 1 asks for OpenTelemetry traces and
metrics. Two properties make durable runs unusual for tracing: a run lives longer than any process (it can
wait for days and continue after restarts), and the engine must not report facts that a crash can take
back.

## Options

1. Instrument the engine with `tracing` spans and bridge them with `tracing-opentelemetry`. A span cannot
   outlive the process, and the bridge offers no supported way to give a run's spans a trace id derived
   from the run, so a restarted run would start a new trace.
2. Depend on OpenTelemetry in the engine. Ties every embedder to one telemetry stack and its versions.
3. **A small observer hook in the engine, and OpenTelemetry in a separate crate.**

## Decision

- `aqueduct_engine::Observer` has three methods with empty defaults: `entry` (a journal entry became
  durable; called after the flush, once, in order per run), `step` (a worker executed a step: start time,
  duration, how it ended) and `flush` (records per durable batch, and how long the write and sync took).
  `Builder::observer` installs one. The engine has no telemetry dependency.
- `aqueduct-otel` implements it with the OpenTelemetry SDK (0.33):
  - **One trace per run.** The trace id is `SHA-256("aqueduct-trace\0" || key)[..16]`. When the run starts,
    a zero-length root span named `run <workflow>` is emitted with span id
    `SHA-256("aqueduct-run\0" || key)[..8]`; every later span of the run is its child, through a parent
    context built from those ids. A run that continues in another process lands in the same trace. The
    SDK takes span ids only from its id generator, so `RunIdGenerator` (install it on the tracer provider)
    supplies the root's ids and random ids otherwise.
  - **Spans:** each step execution, with its real duration (an error status unless it completed); and
    zero-length spans for retries scheduled, sleeps and timers fired, signal waits and signals, recorded
    values, escalations and resolutions, cancellation, and the run's end (failed and parked runs carry an
    error status).
  - **Metrics:** `aqueduct.runs.started`, `aqueduct.runs.ended` (by status), `aqueduct.step.executions`
    (by step and how it ended), `aqueduct.step.duration`, `aqueduct.escalations` (by reason),
    `aqueduct.signals.received`, `aqueduct.log.flush.duration`, `aqueduct.log.flush.records`.
- Exporters are the embedder's choice: `aqueduct-otel` takes the tracer and meter providers, so OTLP (the
  `opentelemetry-otlp` crate), stdout or anything else is configured where the engine is embedded, and
  this repository does not take on an HTTP or gRPC stack.

## Consequences

- Observations are at most once: an entry durable just before a crash, but not yet observed, is not
  observed after recovery. Telemetry is for operations; the journal remains the record.
- A trace has no single span covering the whole run (no span outlives a process); its extent, from the
  root span to `run completed`, is the run's duration in tracing UIs.
- Step spans are emitted for every execution, so duplicate executions (at least once) are visible.
- Per procedure version metrics need versions (phase 2); agent call spans come with agent steps.
- The CLI (`describe`, `journal`, `escalations`) remains the inspection path that needs no setup.
