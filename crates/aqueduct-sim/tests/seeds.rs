//! Runs the simulator over many seeds. CI runs 10,000 more through `cargo xtask sim`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use aqueduct_sim::{Bugs, Config, run_seed};

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
