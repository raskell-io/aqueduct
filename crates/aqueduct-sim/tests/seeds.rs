//! Runs the simulator over many seeds. CI runs 10,000 more through `cargo xtask sim`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use aqueduct_sim::{Bugs, Config, WorkloadKind, run_seed};

#[test]
fn a_thousand_seeds_hold_every_invariant() {
    let config = Config::default();
    let mut crashes = 0;
    let mut duplicates = 0;
    let mut torn = 0;
    let mut sync_failures = 0;
    for seed in 0..1_000 {
        let report = run_seed(seed, &config).unwrap_or_else(|failure| panic!("{failure}"));
        crashes += report.crashes;
        duplicates += report.duplicate_executions;
        torn += report.torn_tails;
        sync_failures += report.sync_failures;
    }
    // The faults actually happen.
    assert!(crashes > 500, "too few crashes: {crashes}");
    assert!(
        duplicates > 500,
        "too few duplicate executions: {duplicates}"
    );
    assert!(torn > 50, "too few torn tails: {torn}");
    assert!(sync_failures > 50, "too few sync failures: {sync_failures}");
}

#[test]
fn seeds_reproduce_exactly() {
    let config = Config::default();
    for seed in [0, 1, 42, 9_999] {
        assert_eq!(run_seed(seed, &config), run_seed(seed, &config));
    }
}

#[test]
fn acknowledging_before_sync_is_caught() {
    // The simulator must find a seed where acknowledging before the sync loses acknowledged work.
    let config = Config {
        bugs: Bugs {
            ack_before_sync: true,
            ..Bugs::default()
        },
        ..Config::default()
    };
    let failure = (0..1_000)
        .find_map(|seed| run_seed(seed, &config).err())
        .expect("the simulator should catch acknowledgements sent before sync");
    assert!(
        failure.message.contains("lost acknowledged"),
        "{}",
        failure.message
    );
}

#[test]
fn skipping_tail_truncation_is_caught() {
    // Appending after a torn tail makes later records unreadable, so acknowledged work is lost.
    let config = Config {
        bugs: Bugs {
            skip_tail_truncation: true,
            ..Bugs::default()
        },
        ..Config::default()
    };
    let failure = (0..1_000)
        .find_map(|seed| run_seed(seed, &config).err())
        .expect("the simulator should catch appends after a torn tail");
    assert!(
        failure.message.contains("lost acknowledged") || failure.message.contains("invalid tail"),
        "{}",
        failure.message
    );
}

#[test]
fn missing_redispatch_is_caught() {
    // Without re-dispatch, a lost dispatch or a forgotten result leaves a run stuck forever.
    let config = Config {
        bugs: Bugs {
            no_redispatch: true,
            ..Bugs::default()
        },
        ..Config::default()
    };
    let failure = (0..1_000)
        .find_map(|seed| run_seed(seed, &config).err())
        .expect("the simulator should catch runs that never finish");
    assert!(failure.message.contains("liveness"), "{}", failure.message);
}

#[test]
fn the_starlark_pipeline_holds_every_invariant() {
    // The same pipeline as a Starlark procedure, replayed by re-execution on every activation.
    let config = Config {
        workload: WorkloadKind::StarlarkPipeline,
        ..Config::default()
    };
    let mut completed = 0;
    let mut failed = 0;
    for seed in 0..300 {
        let report = run_seed(seed, &config).unwrap_or_else(|failure| panic!("{failure}"));
        completed += report.completed;
        failed += report.failed;
    }
    assert!(
        completed > 500 && failed > 200,
        "completed {completed}, failed {failed}"
    );
}

#[test]
fn timers_and_signals_hold_every_invariant() {
    // Durable sleeps, timers that never fire early, and signals retried by the client and duplicated by
    // the network but recorded exactly once.
    let config = Config {
        workload: WorkloadKind::StarlarkTimers,
        ..Config::default()
    };
    for seed in 0..300 {
        run_seed(seed, &config).unwrap_or_else(|failure| panic!("{failure}"));
    }
}

#[test]
fn a_checkpoint_that_skips_recovered_runs_is_caught() {
    // Found by the simulator while checkpoints were written: runs recovered from the log after a
    // checkpoint must be saved by the next one, which moves past their log records.
    let config = Config {
        bugs: Bugs {
            checkpoint_forgets_recovered_runs: true,
            ..Bugs::default()
        },
        ..Config::default()
    };
    let failure = (0..1_000)
        .find_map(|seed| run_seed(seed, &config).err())
        .expect("the simulator should catch checkpoints that skip recovered runs");
    assert!(
        failure.message.contains("lost acknowledged") || failure.message.contains("hash chain"),
        "{}",
        failure.message
    );
}

#[test]
fn the_rust_sdk_pipeline_holds_every_invariant() {
    // The same pipeline as a Rust async fn: replay by re-running the function on every activation.
    let config = Config {
        workload: WorkloadKind::RustPipeline,
        ..Config::default()
    };
    for seed in 0..300 {
        run_seed(seed, &config).unwrap_or_else(|failure| panic!("{failure}"));
    }
}

#[test]
fn a_recorded_value_used_before_it_is_durable_is_caught() {
    // The Rust workload passes a recorded UUID to its first step. Releasing that dispatch before the
    // value is durable lets a crash record a different UUID for the same command.
    let config = Config {
        workload: WorkloadKind::RustPipeline,
        bugs: Bugs {
            ack_before_sync: true,
            ..Bugs::default()
        },
        ..Config::default()
    };
    let failure = (0..2_000)
        .filter_map(|seed| run_seed(seed, &config).err())
        .find(|failure| failure.message.contains("was dispatched as"))
        .expect(
            "the simulator should catch a dispatch that used a value later recorded differently",
        );
    assert!(failure.message.contains("stamp"), "{}", failure.message);
}
