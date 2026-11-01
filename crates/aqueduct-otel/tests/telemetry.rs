//! A run observed through OpenTelemetry: one trace per run, across a restart, and its metrics.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use aqueduct_core::{Payload, Status};
use aqueduct_engine::RetryPolicy;
use aqueduct_engine::embedded::{Attempt, Engine};
use aqueduct_otel::{OtelObserver, RunIdGenerator, run_span_id, run_trace_id};
use aqueduct_sdk::{WorkflowContext, WorkflowError, workflow};
use opentelemetry::Value;
use opentelemetry::metrics::MeterProvider;
use opentelemetry::trace::SpanId;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};

async fn approve(ctx: WorkflowContext, order: String) -> Result<String, WorkflowError> {
    let quote: u32 = ctx.step("quote", &order).await?;
    let decision: Option<bool> = ctx.wait_signal("approve", None).await?;
    if decision != Some(true) {
        return Err(WorkflowError::failed("not approved"));
    }
    let note: String = ctx.escalate("double-check", &quote).await?;
    Ok(format!("{order}: {quote} ({note})"))
}

struct Telemetry {
    spans: InMemorySpanExporter,
    metrics: InMemoryMetricExporter,
    tracing: SdkTracerProvider,
    meters: SdkMeterProvider,
}

impl Telemetry {
    fn new() -> Self {
        let spans = InMemorySpanExporter::default();
        let metrics = InMemoryMetricExporter::default();
        let tracing = SdkTracerProvider::builder()
            .with_id_generator(RunIdGenerator::default())
            .with_simple_exporter(spans.clone())
            .build();
        let meters = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(metrics.clone()).build())
            .build();
        Telemetry {
            spans,
            metrics,
            tracing,
            meters,
        }
    }

    fn observer(&self) -> Arc<OtelObserver> {
        Arc::new(OtelObserver::new(
            &self.tracing,
            &self.meters.meter("aqueduct"),
        ))
    }
}

fn open(dir: &std::path::Path, telemetry: &Telemetry, calls: Arc<AtomicUsize>) -> Engine {
    Engine::builder(dir)
        .workflow("approve", Arc::new(workflow(approve)))
        .step("quote", move |_: &Payload| {
            // The first attempt fails and is retried.
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Attempt::Retry("busy".into())
            } else {
                Attempt::Completed(Payload::new(*b"42"))
            }
        })
        .retry_policy(
            "quote",
            RetryPolicy {
                max_attempts: 3,
                initial_backoff_ms: 10,
                multiplier: 2,
                max_backoff_ms: 50,
            },
        )
        .observer(telemetry.observer())
        .open()
        .unwrap()
}

fn names(spans: &[SpanData]) -> Vec<String> {
    spans.iter().map(|s| s.name.to_string()).collect()
}

fn counter(metrics: &[ResourceMetrics], name: &str, attribute: Option<(&str, &str)>) -> u64 {
    let mut total = 0;
    for metric in metrics
        .iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(|scope| scope.metrics())
        .filter(|m| m.name() == name)
    {
        if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
            for point in sum.data_points() {
                let matches = attribute.is_none_or(|(k, v)| {
                    point
                        .attributes()
                        .any(|kv| kv.key.as_str() == k && kv.value == Value::from(v.to_owned()))
                });
                if matches {
                    total = total.max(point.value());
                }
            }
        }
    }
    total
}

#[test]
fn a_run_is_one_trace_across_a_restart_and_is_counted() {
    let dir = std::env::temp_dir().join(format!("aqueduct-otel-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let calls = Arc::new(AtomicUsize::new(0));
    let key = "order:7";

    // First process: the run starts, its step is retried and completes, then it waits for a signal.
    let first = Telemetry::new();
    {
        let engine = open(&dir, &first, Arc::clone(&calls));
        engine
            .start(key, "approve", Payload::new(*b"\"o-7\""))
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !names(&first.spans.get_finished_spans().unwrap())
            .contains(&"wait for signal approve".to_owned())
        {
            assert!(std::time::Instant::now() < deadline, "the run never waited");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    // Second process: the signal, the escalation and its resolution, the end.
    let second = Telemetry::new();
    let engine = open(&dir, &second, Arc::clone(&calls));
    engine
        .signal(key, "approve", "a-1", Payload::new(*b"true"))
        .unwrap();
    let id = loop {
        if let Some(open) = engine.escalations().unwrap().into_iter().next() {
            break open.id;
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    engine
        .resolve_escalation(&id, Payload::new(*b"\"checked\""))
        .unwrap();
    let done = engine.wait(key, Duration::from_secs(10)).unwrap();
    assert_eq!(done.status, Status::Completed);
    drop(engine);

    let mut spans = first.spans.get_finished_spans().unwrap();
    spans.extend(second.spans.get_finished_spans().unwrap());
    let trace = run_trace_id(key);
    let root = run_span_id(key);
    assert!(
        spans.iter().all(|s| s.span_context.trace_id() == trace),
        "one trace"
    );
    let roots: Vec<&SpanData> = spans
        .iter()
        .filter(|s| s.span_context.span_id() == root)
        .collect();
    assert_eq!(roots.len(), 1, "one root span: {:?}", names(&spans));
    assert_eq!(roots[0].name, "run approve");
    assert_eq!(roots[0].parent_span_id, SpanId::INVALID);
    assert!(
        spans
            .iter()
            .filter(|s| s.span_context.span_id() != root)
            .all(|s| s.parent_span_id == root),
        "every other span is a child of the root"
    );
    let all = names(&spans);
    for expected in [
        "step quote",
        "retry scheduled",
        "timer fired",
        "wait for signal approve",
        "signal approve",
        "escalated double-check",
        "escalation resolved",
        "run completed",
    ] {
        assert!(
            all.contains(&expected.to_owned()),
            "missing {expected}: {all:?}"
        );
    }
    let step_spans: Vec<&SpanData> = spans.iter().filter(|s| s.name == "step quote").collect();
    assert_eq!(step_spans.len(), 2, "two executions: a retry and a success");

    first.meters.force_flush().unwrap();
    second.meters.force_flush().unwrap();
    let before = first.metrics.get_finished_metrics().unwrap();
    let after = second.metrics.get_finished_metrics().unwrap();
    assert_eq!(counter(&before, "aqueduct.runs.started", None), 1);
    assert_eq!(
        counter(
            &before,
            "aqueduct.step.executions",
            Some(("aqueduct.step.end", "retry"))
        ),
        1
    );
    assert_eq!(
        counter(
            &before,
            "aqueduct.step.executions",
            Some(("aqueduct.step.end", "completed"))
        ),
        1
    );
    assert_eq!(counter(&after, "aqueduct.signals.received", None), 1);
    assert_eq!(
        counter(
            &after,
            "aqueduct.escalations",
            Some(("aqueduct.reason", "double-check"))
        ),
        1
    );
    assert_eq!(
        counter(
            &after,
            "aqueduct.runs.ended",
            Some(("aqueduct.status", "completed"))
        ),
        1
    );
    std::fs::remove_dir_all(&dir).unwrap();
}
