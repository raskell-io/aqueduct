//! `cargo xtask sim`: runs the deterministic simulator over seeds.

use std::time::Instant;

use aqueduct_sim::{Config, Report, WorkloadKind, run_seed};

pub fn run(args: &[String]) -> Result<(), String> {
    let mut from = 0u64;
    let mut count = 1_000u64;
    let mut single = None;
    let mut workload = WorkloadKind::Pipeline;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        let mut value = || -> Result<u64, String> {
            iter.next()
                .ok_or_else(|| format!("{arg} needs a value"))?
                .parse()
                .map_err(|e| format!("{arg}: {e}"))
        };
        match arg.as_str() {
            "--seed" => single = Some(value()?),
            "--seeds" => count = value()?,
            "--from" => from = value()?,
            "--workload" => {
                workload = match iter.next().map(String::as_str) {
                    Some("native") => WorkloadKind::Pipeline,
                    Some("starlark") => WorkloadKind::StarlarkPipeline,
                    Some("timers") => WorkloadKind::StarlarkTimers,
                    other => {
                        return Err(format!(
                            "--workload must be native, starlark or timers, not {other:?}"
                        ));
                    }
                }
            }
            other => {
                return Err(format!(
                    "unknown argument {other}; use --seed N, or --seeds N [--from S]"
                ));
            }
        }
    }

    let config = Config {
        workload,
        ..Config::default()
    };
    if let Some(seed) = single {
        let report = run_seed(seed, &config).map_err(|f| f.to_string())?;
        println!("{report:#?}");
        return Ok(());
    }

    let started = Instant::now();
    let mut total = Report::default();
    for seed in from..from + count {
        let report = run_seed(seed, &config).map_err(|f| f.to_string())?;
        total.events += report.events;
        total.crashes += report.crashes;
        total.sync_failures += report.sync_failures;
        total.torn_tails += report.torn_tails;
        total.retries += report.retries;
        total.checkpoints += report.checkpoints;
        total.dropped += report.dropped;
        total.duplicated += report.duplicated;
        total.step_executions += report.step_executions;
        total.duplicate_executions += report.duplicate_executions;
        total.completed += report.completed;
        total.failed += report.failed;
    }
    println!(
        "{count} seeds ({from}..{}, {workload:?}) passed in {:.1} s\n\
         events {}, crashes {} (of which failed syncs {}), torn tails truncated {}, checkpoints {}\n\
         messages dropped {}, duplicated {}\n\
         step executions {} (repeats {}), retries {}, runs completed {}, failed as intended {}",
        from + count,
        started.elapsed().as_secs_f64(),
        total.events,
        total.crashes,
        total.sync_failures,
        total.torn_tails,
        total.checkpoints,
        total.dropped,
        total.duplicated,
        total.step_executions,
        total.duplicate_executions,
        total.retries,
        total.completed,
        total.failed,
    );
    Ok(())
}
