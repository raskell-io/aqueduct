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
    let (mut completed, mut failed) = (0, 0);
    for seed in 0..300 {
        let report = run_seed(seed, &config).unwrap_or_else(|failure| panic!("{failure}"));
        completed += report.completed;
        failed += report.failed;
    }
    // Both paths are exercised: a step that rejects its input would fail every run "as intended".
    assert!(
        completed > 500 && failed > 200,
        "completed {completed}, failed {failed}"
    );
}

#[test]
fn a_decision_based_on_something_not_yet_durable_is_caught() {
    // The Rust workload passes a recorded UUID to a step, and the winner of a race to another. Releasing
    // those dispatches before the sync lets a crash take back the value (or the order of the race's
    // results), so the same command is dispatched again with another input.
    let config = Config {
        workload: WorkloadKind::RustPipeline,
        bugs: Bugs {
            ack_before_sync: true,
            ..Bugs::default()
        },
        ..Config::default()
    };
    let caught: std::collections::BTreeSet<&str> = (0..2_000)
        .filter_map(|seed| run_seed(seed, &config).err())
        .filter_map(|failure| {
            ["\"stamp\"", "\"winner\""].into_iter().find(|step| {
                failure.message.contains("was dispatched as") && failure.message.contains(step)
            })
        })
        .collect();
    assert!(
        !caught.is_empty(),
        "the simulator should catch a dispatch built on lost state"
    );
}

#[test]
fn a_successor_torn_off_by_a_crash_is_restarted() {
    // Found by the simulator when continue-as-new was added: a run's `ContinuedAsNew` and its successor's
    // `Started` are separate records of one batch, and a torn write can keep only the first.
    let config = Config {
        workload: WorkloadKind::RustPipeline,
        bugs: Bugs {
            skip_successor_repair: true,
            ..Bugs::default()
        },
        ..Config::default()
    };
    let failure = (0..2_000)
        .find_map(|seed| run_seed(seed, &config).err())
        .expect("the simulator should catch a chain left without its next run");
    assert!(
        failure.message.contains("Continued") || failure.message.contains("liveness"),
        "{}",
        failure.message
    );
}

#[test]
fn a_lost_escalation_resolution_is_caught() {
    // The timers workload escalates a review that the simulated reviewer resolves. Acknowledging the
    // resolution before it is durable lets a crash lose it.
    let config = Config {
        workload: WorkloadKind::StarlarkTimers,
        bugs: Bugs {
            ack_before_sync: true,
            ..Bugs::default()
        },
        ..Config::default()
    };
    let caught = (0..3_000)
        .filter_map(|seed| run_seed(seed, &config).err())
        .find(|failure| failure.message.contains("acknowledged resolution"));
    assert!(
        caught.is_some(),
        "the simulator should catch a lost resolution"
    );
}

#[test]
fn escalations_are_resolved_under_faults() {
    let config = Config {
        workload: WorkloadKind::StarlarkTimers,
        ..Config::default()
    };
    let (mut resolved, mut completed, mut failed) = (0, 0, 0);
    for seed in 0..300 {
        let report = run_seed(seed, &config).unwrap_or_else(|failure| panic!("{failure}"));
        resolved += report.escalations_resolved;
        completed += report.completed;
        failed += report.failed;
    }
    // Reviews happen, and both decisions are taken.
    assert!(resolved > 500, "resolved {resolved}");
    assert!(
        completed > 200 && failed > 200,
        "completed {completed}, failed {failed}"
    );
}

#[test]
fn an_agent_call_released_after_its_answer_is_caught() {
    // Found by the simulator when agent calls were added: a retry's dispatch waits for the flush, a late
    // answer of the first attempt is recorded meanwhile, and releasing the dispatch calls the model again.
    let config = Config {
        workload: WorkloadKind::StarlarkPipeline,
        bugs: Bugs {
            release_stale_dispatches: true,
            ..Bugs::default()
        },
        ..Config::default()
    };
    let caught = (0..3_000)
        .filter_map(|seed| run_seed(seed, &config).err())
        .find(|failure| failure.message.contains("after its response was recorded"));
    assert!(
        caught.is_some(),
        "the simulator should catch an agent call issued again"
    );
}

#[test]
fn agent_calls_hold_every_invariant() {
    let config = Config {
        workload: WorkloadKind::StarlarkPipeline,
        ..Config::default()
    };
    let (mut calls, mut escalated, mut approved, mut answers) = (0, 0, 0, 0);
    for seed in 0..300 {
        let report = run_seed(seed, &config).unwrap_or_else(|failure| panic!("{failure}"));
        calls += report.agent_calls;
        escalated += report.budget_escalations;
        approved += report.budget_approvals;
        answers += report.answer_escalations;
    }
    // Off-schema answers are retried, and some exhaust their retries and are answered by the reviewer.
    assert!(answers > 50, "answers escalated {answers}");
    assert!(calls > 1_000, "agent calls {calls}");
    // Runs with too small a budget escalate; the reviewer approves some and declines others.
    assert!(
        escalated > 100 && approved > 50 && approved < escalated,
        "budget escalations {escalated}, approved {approved}"
    );
}
