//! A few hundred seeds on every test run; `cargo xtask raft --seeds N` runs more.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use aqueduct_raft::sim::{SimConfig, run};

#[test]
fn five_nodes_stay_safe_and_converge() {
    let config = SimConfig::default();
    for seed in 0..200 {
        if let Err(failure) = run(seed, &config) {
            panic!("seed {seed}: {failure:?}");
        }
    }
}

#[test]
fn three_nodes_stay_safe_and_converge() {
    let config = SimConfig {
        nodes: 3,
        ..SimConfig::default()
    };
    for seed in 0..200 {
        if let Err(failure) = run(seed, &config) {
            panic!("seed {seed}: {failure:?}");
        }
    }
}

#[test]
fn a_seed_runs_the_same_world_every_time() {
    let config = SimConfig::default();
    assert_eq!(run(42, &config), run(42, &config));
}
