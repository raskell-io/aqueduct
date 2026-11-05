//! Applying an import when the engine opens (ADR 0047): whole, repaired after a crash, or not at all.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use aqueduct_codec::{ChainWriter, decode_log_record, encode_log_record, verify_chain};
use aqueduct_core::{Command, Decider, Entry, Payload, RunView, Status};
use aqueduct_engine::embedded::Engine;
use aqueduct_engine::import::{Relation, Stage, freeze, frozen, pending, relation};
use aqueduct_storage::{FileLog, FileLogOptions, LogStore};

/// Waits for a signal named "go", and completes with it.
struct Waits;

impl Decider for Waits {
    fn decide(&self, view: &RunView<'_>) -> Vec<Command> {
        let mut commands = vec![Command::AwaitSignal {
            name: "go".into(),
            timeout_ms: 0,
        }];
        if let Some(aqueduct_core::StepOutcome::Completed(value)) =
            view.outcome(aqueduct_core::CommandIndex(0))
        {
            commands.push(Command::CompleteRun {
                output: value.clone(),
            });
        }
        commands
    }
}

fn dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("aqueduct-import-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn open(dir: &Path) -> Engine {
    Engine::builder(dir)
        .workflow("waits", Arc::new(Waits))
        .open()
        .unwrap()
}

fn started(input: &[u8]) -> Entry {
    Entry::Started {
        workflow: "waits".into(),
        input: Payload::new(input.to_vec()),
        budget_tokens: None,
        version: None,
    }
}

fn await_go() -> Entry {
    Entry::SignalAwaited {
        cmd: aqueduct_core::CommandIndex(0),
        name: "go".into(),
        timeout_ms: 0,
        wake_at: 0,
    }
}

fn records(key: &str, entries: &[Entry]) -> Vec<Vec<u8>> {
    let mut chain = ChainWriter::new(key);
    entries.iter().map(|entry| chain.append(entry)).collect()
}

fn stage(dir: &Path, runs: &[(&str, Vec<Vec<u8>>)]) {
    let mut stage = Stage::begin(dir).unwrap();
    for (key, records) in runs {
        stage.add_run(key, records).unwrap();
    }
    stage.commit().unwrap();
}

/// The imported run goes on: it waits for its signal, and finishes with it.
fn finishes(engine: &Engine, key: &str) {
    engine.signal(key, "go", "", Payload::new(*b"7")).unwrap();
    let result = engine.wait(key, Duration::from_secs(5)).unwrap();
    assert_eq!(result.status, Status::Completed, "{result:?}");
}

#[test]
fn an_import_is_applied_when_the_engine_opens() {
    let dir = dir("applied");
    drop(open(&dir));
    stage(&dir, &[("k", records("k", &[started(b"1"), await_go()]))]);
    let engine = open(&dir);
    let report = engine.import_report().unwrap();
    assert_eq!((report.imported, report.already_there), (1, 0));
    assert!(pending(&dir).is_none(), "it is not applied twice");
    finishes(&engine, "k");
    drop(engine);
    // And it is there after a restart, from the log.
    let engine = open(&dir);
    assert!(engine.import_report().is_none());
    assert_eq!(
        engine.wait("k", Duration::from_secs(5)).unwrap().status,
        Status::Completed
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_crash_while_applying_is_repaired_by_applying_again() {
    let dir = dir("repair");
    let whole = [started(b"1"), await_go()];
    // A crash left all of one run, and the start of another, in the log, with the import still pending:
    // no run has acted, since recovery comes after applying.
    {
        let mut log = FileLog::open(dir.join("log"), FileLogOptions::default()).unwrap();
        log.recover_from(0).unwrap();
        let mut batch = Vec::new();
        for record in records("done", &whole) {
            batch.push(encode_log_record("done", &record));
        }
        for record in records("torn", &whole[..1]) {
            batch.push(encode_log_record("torn", &record));
        }
        log.append(&batch).unwrap();
        log.sync().unwrap();
    }
    stage(
        &dir,
        &[
            ("done", records("done", &whole)),
            ("torn", records("torn", &whole)),
            ("other", records("other", &whole)),
        ],
    );
    let engine = open(&dir);
    let report = engine.import_report().unwrap();
    assert_eq!(
        (report.imported, report.already_there),
        (2, 1),
        "the torn one is completed, and the whole one is left alone"
    );
    assert_eq!(report.rejected, None);
    // What a crash now would recover from: the log itself, without a checkpoint to hide anything.
    let copy = dir.join("log-copy");
    std::fs::create_dir_all(&copy).unwrap();
    for file in std::fs::read_dir(dir.join("log")).unwrap() {
        let file = file.unwrap();
        if file.file_name() != "LOCK" {
            std::fs::copy(file.path(), copy.join(file.file_name())).unwrap();
        }
    }
    let mut by_run: BTreeMap<String, Vec<Vec<u8>>> = BTreeMap::new();
    let mut log = FileLog::open(&copy, FileLogOptions::default()).unwrap();
    for bytes in log.recover_from(0).unwrap() {
        let (key, record) = decode_log_record(&bytes).unwrap();
        by_run
            .entry(key.to_owned())
            .or_default()
            .push(record.to_vec());
    }
    for key in ["done", "torn", "other"] {
        let verified = verify_chain(key, by_run[key].iter().map(Vec::as_slice));
        assert!(verified.is_ok(), "{key}: {verified:?}");
    }
    for key in ["done", "torn", "other"] {
        finishes(&engine, key);
    }
    drop(engine);
    // Their chains are whole: the log reopens, which verifies them.
    let engine = open(&dir);
    assert_eq!(
        engine.wait("torn", Duration::from_secs(5)).unwrap().status,
        Status::Completed
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_conflict_found_on_opening_sets_the_whole_import_aside() {
    let dir = dir("conflict");
    let engine = open(&dir);
    engine.start("k", "waits", Payload::new(*b"2")).unwrap();
    drop(engine);
    stage(
        &dir,
        &[
            ("new", records("new", &[started(b"1"), await_go()])),
            ("k", records("k", &[started(b"1"), await_go()])),
        ],
    );
    let engine = open(&dir);
    let report = engine.import_report().unwrap();
    assert!(
        report.rejected.as_ref().unwrap().contains('k'),
        "{report:?}"
    );
    assert_eq!(report.imported, 0);
    assert!(
        engine.describe("new").is_err(),
        "none of it, not part of it"
    );
    finishes(&engine, "k");
    assert!(pending(&dir).is_none());
    let kept = std::fs::read_dir(dir.join("import"))
        .unwrap()
        .filter_map(Result::ok)
        .any(|entry| entry.file_name().to_string_lossy().starts_with("rejected-"));
    assert!(kept, "it is kept, to look at");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_stage_takes_only_what_verifies() {
    let dir = dir("stage");
    let entries = [started(b"1"), await_go()];
    let mut stage = Stage::begin(&dir).unwrap();
    // A chain starts from its key: a history cannot be moved to another key.
    assert!(stage.add_run("other", &records("k", &entries)).is_err());
    // Nor can a changed one come in.
    let mut changed = records("k", &entries);
    let last = changed[0].len() - 1;
    changed[0][last] ^= 1;
    assert!(stage.add_run("k", &changed).is_err());
    // Nor into a tenant's namespace.
    let tenant = "$ns/acme/k";
    assert!(stage.add_run(tenant, &records(tenant, &entries)).is_err());
    // Nor anywhere but where a source or registry file goes.
    assert!(stage.add_source("../../../evil", b"x").is_err());
    assert!(stage.add_registry_file("../tokens.json", b"x").is_err());
    // One upload at a time; abandoning it leaves nothing pending.
    assert!(Stage::begin(&dir).is_err());
    drop(stage);
    assert!(pending(&dir).is_none());
    drop(Stage::begin(&dir).unwrap());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_frozen_directory_does_not_open() {
    let dir = dir("frozen");
    drop(open(&dir));
    freeze(&dir, "https://server", "migrating").unwrap();
    assert!(frozen(&dir).unwrap().contains("--abort"));
    let refused = Engine::builder(&dir)
        .workflow("waits", Arc::new(Waits))
        .open()
        .err()
        .unwrap();
    assert!(refused.to_string().contains("https://server"), "{refused}");
    freeze(&dir, "https://server", "migrated").unwrap();
    assert!(frozen(&dir).unwrap().contains("repeat their steps"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn how_histories_relate() {
    let (a, b) = (started(b"1"), await_go());
    let (one, two) = (vec![a.clone()], vec![a, b]);
    assert_eq!(relation(&two, &one), Relation::AlreadyThere);
    assert_eq!(relation(&one, &one), Relation::AlreadyThere);
    assert_eq!(relation(&one, &two), Relation::Partial);
    assert_eq!(relation(&[started(b"2")], &two), Relation::Conflict);
}
