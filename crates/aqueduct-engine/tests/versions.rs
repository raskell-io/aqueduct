//! Version pinning (ADR 0029): runs finish on the version they started on; new runs start on the active
//! version; a run whose version is not registered waits for it.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aqueduct_core::{Payload, Status};
use aqueduct_engine::StartOptions;
use aqueduct_engine::embedded::{Builder, Engine, EngineError};
use aqueduct_procedure::Procedure;

fn version(n: u32) -> Arc<Procedure> {
    let source = format!(
        r#"
def run(request):
    note = wait(signal("go"))
    return {{"version": {n}, "note": note}}
"#
    );
    Arc::new(Procedure::compile("review.star", &source).unwrap())
}

fn temp_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "aqueduct-versions-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// An engine with these versions of `review` registered, the last one active.
fn engine(dir: &PathBuf, versions: &[&Arc<Procedure>]) -> Engine {
    let mut builder: Builder = Engine::builder(dir);
    for procedure in versions {
        builder =
            builder.workflow_version("review", &procedure.version(), Arc::clone(*procedure) as _);
    }
    let active = versions.last().unwrap().version();
    builder.active_version("review", &active).open().unwrap()
}

fn go(engine: &Engine, key: &str) {
    engine
        .signal(key, "go", "", Payload::new(*b"\"ok\""))
        .unwrap();
}

fn output(version: u32) -> Option<Payload> {
    Some(Payload::new(
        format!("{{\"version\":{version},\"note\":\"ok\"}}").into_bytes(),
    ))
}

#[test]
fn runs_finish_on_the_version_they_started_on() {
    let dir = temp_dir();
    let (v1, v2) = (version(1), version(2));
    {
        let engine = engine(&dir, &[&v1]);
        engine.start("old", "review", Payload::new(*b"{}")).unwrap();
    }
    let engine = engine(&dir, &[&v1, &v2]);
    engine.start("new", "review", Payload::new(*b"{}")).unwrap();
    let pinned = StartOptions {
        version: Some(v1.version()),
        ..StartOptions::default()
    };
    engine
        .start_with("canary", "review", Payload::new(*b"{}"), pinned)
        .unwrap();
    let unknown = StartOptions {
        version: Some("0000".into()),
        ..StartOptions::default()
    };
    let refused = engine.start_with("nope", "review", Payload::new(*b"{}"), unknown);
    assert!(
        matches!(&refused, Err(e) if e.to_string().contains("has no version `0000`")),
        "{refused:?}"
    );
    for key in ["old", "new", "canary"] {
        go(&engine, key);
    }
    let wait = |key: &str| engine.wait(key, Duration::from_secs(10)).unwrap();
    let (old, new, canary) = (wait("old"), wait("new"), wait("canary"));
    assert_eq!((old.output, old.version), (output(1), Some(v1.version())));
    assert_eq!((new.output, new.version), (output(2), Some(v2.version())));
    assert_eq!(
        (canary.output, canary.version),
        (output(1), Some(v1.version()))
    );
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_run_waits_while_its_version_is_not_registered() {
    let dir = temp_dir();
    let (v1, v2) = (version(1), version(2));
    {
        let engine = engine(&dir, &[&v1]);
        engine.start("r", "review", Payload::new(*b"{}")).unwrap();
    }
    {
        // Only the new version is deployed: the old run is neither decided by it nor parked.
        let engine = engine(&dir, &[&v2]);
        go(&engine, "r");
        assert!(matches!(
            engine.wait("r", Duration::from_millis(300)),
            Err(EngineError::Timeout(_))
        ));
    }
    // The old version is deployed again: the run finishes on it, with the signal it already received.
    let engine = engine(&dir, &[&v1, &v2]);
    let done = engine.wait("r", Duration::from_secs(10)).unwrap();
    assert_eq!((done.status, done.output), (Status::Completed, output(1)));
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn continue_as_new_moves_a_run_to_the_active_version() {
    let looping = |n: u32| {
        let source = format!(
            r#"
def run(round):
    if round == 0:
        wait(signal("go"))
        continue_as_new(1)
    return "finished on version {n}"
"#
        );
        Arc::new(Procedure::compile("review.star", &source).unwrap())
    };
    let dir = temp_dir();
    let (v1, v2) = (looping(1), looping(2));
    {
        let engine = engine(&dir, &[&v1]);
        engine.start("long", "review", Payload::new(*b"0")).unwrap();
    }
    let engine = engine(&dir, &[&v1, &v2]);
    go(&engine, "long");
    let done = engine.wait("long", Duration::from_secs(10)).unwrap();
    assert_eq!(
        done.output,
        Some(Payload::new(*b"\"finished on version 2\""))
    );
    assert_eq!(done.version, Some(v2.version()));
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn an_unregistered_version_cannot_be_activated() {
    let dir = temp_dir();
    let opened = Engine::builder(&dir)
        .workflow_version("review", &version(1).version(), version(1))
        .active_version("review", "ffff")
        .open();
    assert!(
        matches!(&opened, Err(EngineError::Open(why)) if why.contains("has no version `ffff`")),
        "{:?}",
        opened.err()
    );
    let _ = std::fs::remove_dir_all(&dir);
}
