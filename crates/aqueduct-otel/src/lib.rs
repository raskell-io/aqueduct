//! OpenTelemetry traces and metrics for Aqueduct engines (BRIEF.md section 3, principle 9; ADR 0021).
//!
//! [`OtelObserver`] implements the engine's [`Observer`] hook. Register it with
//! `Builder::observer`, and build the tracer provider with [`RunIdGenerator`] so that every run is one
//! trace, also across restarts:
//!
//! ```
//! use std::sync::Arc;
//! use aqueduct_otel::{OtelObserver, RunIdGenerator};
//! use opentelemetry::metrics::MeterProvider;
//! use opentelemetry_sdk::metrics::SdkMeterProvider;
//! use opentelemetry_sdk::trace::SdkTracerProvider;
//!
//! let tracing = SdkTracerProvider::builder()
//!     .with_id_generator(RunIdGenerator::default())
//!     // .with_batch_exporter(an OTLP exporter, from the opentelemetry-otlp crate)
//!     .build();
//! let metrics = SdkMeterProvider::builder().build();
//! let observer = Arc::new(OtelObserver::new(&tracing, &metrics.meter("aqueduct")));
//! // Engine::builder(dir).observer(observer) ...
//! ```
//!
//! **Traces.** A run's trace id is derived from its run key. When the run starts, a short root span
//! named after the workflow is emitted with a span id also derived from the key; every later span of
//! the run is its child: each step execution (with its real duration), retries, sleeps and their
//! firing, signal waits and signals, recorded values, escalations and their resolution, cancellation,
//! and the run's end. Spans are emitted for durable facts only (after the flush that made them durable).
//!
//! **Metrics.** `aqueduct.runs.started` and `aqueduct.runs.ended` (by workflow, and status),
//! `aqueduct.step.executions` (by step and how it ended) and `aqueduct.step.duration` (seconds),
//! `aqueduct.escalations` (by workflow and reason), `aqueduct.signals.received` (by workflow),
//! `aqueduct.log.flush.duration` (seconds) and `aqueduct.log.flush.records`.

#![forbid(unsafe_code)]

use std::cell::Cell;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aqueduct_core::{CommandIndex, Entry};
use aqueduct_engine::{Observer, StepEnd};
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::trace::{
    Span, SpanContext, SpanId, Status, TraceContextExt, TraceFlags, TraceId, TraceState, Tracer,
    TracerProvider,
};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::trace::{IdGenerator, RandomIdGenerator, SdkTracer, SdkTracerProvider};
use sha2::{Digest, Sha256};

/// The trace id of a run: the first 16 bytes of `SHA-256("aqueduct-trace\0" || run key)`.
pub fn run_trace_id(key: &str) -> TraceId {
    let digest = Sha256::new()
        .chain_update(b"aqueduct-trace\0")
        .chain_update(key.as_bytes())
        .finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    TraceId::from_bytes(bytes)
}

/// The span id of a run's root span: the first 8 bytes of `SHA-256("aqueduct-run\0" || run key)`.
pub fn run_span_id(key: &str) -> SpanId {
    let digest = Sha256::new()
        .chain_update(b"aqueduct-run\0")
        .chain_update(key.as_bytes())
        .finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    SpanId::from_bytes(bytes)
}

thread_local! {
    static NEXT_TRACE: Cell<Option<TraceId>> = const { Cell::new(None) };
    static NEXT_SPAN: Cell<Option<SpanId>> = const { Cell::new(None) };
}

/// An [`IdGenerator`] that gives a run's root span its deterministic ids ([`run_trace_id`],
/// [`run_span_id`]), and random ids to everything else. Install it on the tracer provider that
/// [`OtelObserver`] uses.
#[derive(Clone, Debug, Default)]
pub struct RunIdGenerator {
    random: RandomIdGenerator,
}

impl IdGenerator for RunIdGenerator {
    fn new_trace_id(&self) -> TraceId {
        NEXT_TRACE
            .with(Cell::take)
            .unwrap_or_else(|| self.random.new_trace_id())
    }

    fn new_span_id(&self) -> SpanId {
        NEXT_SPAN
            .with(Cell::take)
            .unwrap_or_else(|| self.random.new_span_id())
    }
}

/// Turns what an engine does into OpenTelemetry spans and metrics.
pub struct OtelObserver {
    tracer: SdkTracer,
    runs_started: Counter<u64>,
    runs_ended: Counter<u64>,
    step_executions: Counter<u64>,
    step_duration: Histogram<f64>,
    escalations: Counter<u64>,
    signals: Counter<u64>,
    flush_duration: Histogram<f64>,
    flush_records: Histogram<u64>,
}

impl OtelObserver {
    /// An observer emitting spans through `tracing` (built with [`RunIdGenerator`]) and metrics through
    /// `meter`.
    pub fn new(tracing: &SdkTracerProvider, meter: &Meter) -> Self {
        OtelObserver {
            tracer: tracing.tracer("aqueduct"),
            runs_started: meter
                .u64_counter("aqueduct.runs.started")
                .with_description("Runs started")
                .build(),
            runs_ended: meter
                .u64_counter("aqueduct.runs.ended")
                .with_description("Runs that completed, failed or were parked")
                .build(),
            step_executions: meter
                .u64_counter("aqueduct.step.executions")
                .with_description("Step executions, including repeated ones")
                .build(),
            step_duration: meter
                .f64_histogram("aqueduct.step.duration")
                .with_unit("s")
                .with_description("How long step executions took")
                .build(),
            escalations: meter
                .u64_counter("aqueduct.escalations")
                .with_description("Escalations, by reason")
                .build(),
            signals: meter
                .u64_counter("aqueduct.signals.received")
                .with_description("Signals recorded")
                .build(),
            flush_duration: meter
                .f64_histogram("aqueduct.log.flush.duration")
                .with_unit("s")
                .with_description("Time to write and sync a batch of log records")
                .build(),
            flush_records: meter
                .u64_histogram("aqueduct.log.flush.records")
                .with_description("Records per durable batch (group commit)")
                .build(),
        }
    }

    /// The context whose span is the run's root span: the parent of every span of the run.
    fn run_context(key: &str) -> Context {
        Context::new().with_remote_span_context(SpanContext::new(
            run_trace_id(key),
            run_span_id(key),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        ))
    }

    /// Emits a span of the run, from `start` to `end`.
    fn span(
        &self,
        key: &str,
        name: String,
        start: SystemTime,
        end: SystemTime,
        mut attributes: Vec<KeyValue>,
        error: Option<String>,
    ) {
        attributes.push(KeyValue::new("aqueduct.run.key", key.to_owned()));
        let mut span = self
            .tracer
            .span_builder(name)
            .with_start_time(start)
            .with_attributes(attributes)
            .start_with_context(&self.tracer, &Self::run_context(key));
        if let Some(error) = error {
            span.set_status(Status::error(error));
        }
        span.end_with_timestamp(end);
    }

    /// Emits the run's root span, with its deterministic ids.
    fn root_span(&self, key: &str, workflow: &str, at: SystemTime) {
        NEXT_TRACE.with(|next| next.set(Some(run_trace_id(key))));
        NEXT_SPAN.with(|next| next.set(Some(run_span_id(key))));
        let mut span = self
            .tracer
            .span_builder(format!("run {workflow}"))
            .with_start_time(at)
            .with_attributes([
                KeyValue::new("aqueduct.run.key", key.to_owned()),
                KeyValue::new("aqueduct.workflow", workflow.to_owned()),
            ])
            .start_with_context(&self.tracer, &Context::new());
        // A provider without RunIdGenerator, or one that did not sample, leaves the ids unused.
        NEXT_TRACE.with(Cell::take);
        NEXT_SPAN.with(Cell::take);
        span.end_with_timestamp(at);
    }
}

fn cmd_attribute(cmd: CommandIndex) -> KeyValue {
    KeyValue::new("aqueduct.cmd", i64::from(cmd.0))
}

fn at_ms(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}

impl Observer for OtelObserver {
    fn entry(&self, key: &str, workflow: &str, entry: &Entry, now_ms: u64) {
        let now = at_ms(now_ms);
        let flow = || KeyValue::new("aqueduct.workflow", workflow.to_owned());
        let point = |name: String, attributes: Vec<KeyValue>, error: Option<String>| {
            self.span(key, name, now, now, attributes, error);
        };
        match entry {
            Entry::Started { .. } => {
                self.runs_started.add(1, &[flow()]);
                self.root_span(key, workflow, now);
            }
            Entry::StepRetryScheduled {
                cmd,
                attempt,
                error,
                wake_at,
            } => point(
                "retry scheduled".into(),
                vec![
                    cmd_attribute(*cmd),
                    KeyValue::new("aqueduct.attempt", i64::from(*attempt)),
                    KeyValue::new("aqueduct.error", error.clone()),
                    KeyValue::new("aqueduct.wake_at_ms", *wake_at as i64),
                ],
                None,
            ),
            Entry::TimerStarted {
                cmd,
                duration_ms,
                wake_at,
            } => point(
                "sleep".into(),
                vec![
                    cmd_attribute(*cmd),
                    KeyValue::new("aqueduct.duration_ms", *duration_ms as i64),
                    KeyValue::new("aqueduct.wake_at_ms", *wake_at as i64),
                ],
                None,
            ),
            Entry::TimerFired { cmd } => {
                point("timer fired".into(), vec![cmd_attribute(*cmd)], None)
            }
            Entry::SignalAwaited { cmd, name, .. } => point(
                format!("wait for signal {name}"),
                vec![cmd_attribute(*cmd)],
                None,
            ),
            Entry::SignalReceived { name, id, .. } => {
                self.signals.add(1, &[flow()]);
                point(
                    format!("signal {name}"),
                    vec![KeyValue::new("aqueduct.signal.id", id.clone())],
                    None,
                );
            }
            Entry::ValueRecorded { cmd, name, .. } => {
                point(format!("recorded {name}"), vec![cmd_attribute(*cmd)], None);
            }
            Entry::Escalated { cmd, reason, .. } => {
                self.escalations.add(
                    1,
                    &[flow(), KeyValue::new("aqueduct.reason", reason.clone())],
                );
                point(
                    format!("escalated {reason}"),
                    vec![cmd_attribute(*cmd)],
                    None,
                );
            }
            Entry::EscalationResolved { cmd, .. } => {
                point(
                    "escalation resolved".into(),
                    vec![cmd_attribute(*cmd)],
                    None,
                );
            }
            Entry::CancelRequested { reason } => point(
                "cancel requested".into(),
                vec![KeyValue::new("aqueduct.reason", reason.clone())],
                None,
            ),
            Entry::RunCompleted { .. } => {
                self.runs_ended
                    .add(1, &[flow(), KeyValue::new("aqueduct.status", "completed")]);
                point("run completed".into(), Vec::new(), None);
            }
            Entry::ContinuedAsNew { .. } => {
                self.runs_ended
                    .add(1, &[flow(), KeyValue::new("aqueduct.status", "continued")]);
                point("continued as new".into(), Vec::new(), None);
            }
            Entry::RunFailed { error } => {
                self.runs_ended
                    .add(1, &[flow(), KeyValue::new("aqueduct.status", "failed")]);
                point("run failed".into(), Vec::new(), Some(error.clone()));
            }
            Entry::Parked { reason } => {
                self.runs_ended
                    .add(1, &[flow(), KeyValue::new("aqueduct.status", "parked")]);
                point("run parked".into(), Vec::new(), Some(reason.clone()));
            }
            Entry::AgentCallCompleted { cmd, response } => point(
                format!("agent answered ({})", response.model),
                vec![
                    cmd_attribute(*cmd),
                    KeyValue::new("gen_ai.request.model", response.model.clone()),
                    KeyValue::new("gen_ai.usage.input_tokens", response.input_tokens as i64),
                    KeyValue::new("gen_ai.usage.output_tokens", response.output_tokens as i64),
                ],
                None,
            ),
            // Covered by the step execution spans.
            Entry::StepScheduled { .. }
            | Entry::StepCompleted { .. }
            | Entry::StepFailed { .. }
            | Entry::AgentCallScheduled { .. } => {}
        }
    }

    fn step(
        &self,
        key: &str,
        cmd: CommandIndex,
        name: &str,
        started: SystemTime,
        elapsed: Duration,
        end: StepEnd,
    ) {
        let ended = match end {
            StepEnd::Completed => "completed",
            StepEnd::Failed => "failed",
            StepEnd::Retry => "retry",
        };
        let step = KeyValue::new("aqueduct.step", name.to_owned());
        self.step_executions.add(
            1,
            &[step.clone(), KeyValue::new("aqueduct.step.end", ended)],
        );
        self.step_duration.record(elapsed.as_secs_f64(), &[step]);
        self.span(
            key,
            format!("step {name}"),
            started,
            started + elapsed,
            vec![
                cmd_attribute(cmd),
                KeyValue::new("aqueduct.step.end", ended),
            ],
            (end != StepEnd::Completed).then(|| format!("step {ended}")),
        );
    }

    fn flush(&self, records: usize, elapsed: Duration) {
        self.flush_duration.record(elapsed.as_secs_f64(), &[]);
        self.flush_records.record(records as u64, &[]);
    }
}
