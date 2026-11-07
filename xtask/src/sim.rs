//! `cargo xtask sim`: runs the deterministic simulator over seeds.

use std::time::Instant;

use aqueduct_sim::{Config, Report, WorkloadKind, run_seed};

pub fn run(args: &[String]) -> Result<(), String> {
    let mut from = 0u64;
    let mut count = 1_000u64;
    let mut single = None;
    let mut workload = WorkloadKind::Pipeline;
    let mut replicas = 0usize;
    let mut partitions = 0usize;
    let mut spares = 0usize;
    let mut jobs = 1usize;
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
            "--replicas" => replicas = usize::try_from(value()?).map_err(|e| e.to_string())?,
            "--jobs" => jobs = usize::try_from(value()?).map_err(|e| e.to_string())?,
            "--spares" => spares = usize::try_from(value()?).map_err(|e| e.to_string())?,
            "--partitions" => {
                partitions = usize::try_from(value()?).map_err(|e| e.to_string())?;
            }
            "--workload" => {
                workload = match iter.next().map(String::as_str) {
                    Some("native") => WorkloadKind::Pipeline,
                    Some("starlark") => WorkloadKind::StarlarkPipeline,
                    Some("timers") => WorkloadKind::StarlarkTimers,
                    Some("rust") => WorkloadKind::RustPipeline,
                    Some("composite") => WorkloadKind::Composite,
                    Some("fanout") => WorkloadKind::Fanout,
                    other => {
                        return Err(format!(
                            "--workload must be native, starlark, timers, rust, composite or fanout, not {other:?}"
                        ));
                    }
                }
            }
            other => {
                return Err(format!(
                    "unknown argument {other}; use --seed N, or --seeds N [--from S], with --workload W, --replicas R, --partitions P, --spares S and --jobs J"
                ));
            }
        }
    }

    let config = Config {
        workload,
        replicas,
        partitions,
        spares,
        ..Config::default()
    };
    if let Some(seed) = single {
        let report = run_seed(seed, &config).map_err(|f| f.to_string())?;
        println!("{report:#?}");
        return Ok(());
    }

    let started = Instant::now();
    let mut total = Report::default();
    for report in run_seeds(from, count, jobs, &config)? {
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
        total.cancelled += report.cancelled;
        total.escalations_resolved += report.escalations_resolved;
        total.agent_calls += report.agent_calls;
        total.budget_escalations += report.budget_escalations;
        total.budget_approvals += report.budget_approvals;
        total.answer_escalations += report.answer_escalations;
        total.children += report.children;
        total.updates_answered += report.updates_answered;
        total.messages += report.messages;
        total.failovers += report.failovers;
        total.replica_moves += report.replica_moves;
    }
    println!(
        "{count} seeds ({from}..{}, {workload:?}) passed in {:.1} s\n\
         events {}, crashes {} (of which failed syncs {}), torn tails truncated {}, checkpoints {}\n\
         messages dropped {}, duplicated {}\n\
         step executions {} (repeats {}), retries {}, runs completed {}, failed as intended {}, cancelled {}, escalations resolved {}, agent calls {}, budget escalations {} (approved {}), answers escalated {}\n\
         child runs {}, updates answered {}, messages to objects {}, partitions built by new leaders {}, replica moves {}",
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
        total.cancelled,
        total.escalations_resolved,
        total.agent_calls,
        total.budget_escalations,
        total.budget_approvals,
        total.answer_escalations,
        total.children,
        total.updates_answered,
        total.messages,
        total.failovers,
        total.replica_moves,
    );
    Ok(())
}

/// Runs seeds `from..from + count` on `jobs` threads, and returns their reports in seed order, or the
/// failure of the lowest failing seed run. Seeds are independent, so the reports are those one thread
/// would give; once a seed fails, no new seeds are started.
fn run_seeds(from: u64, count: u64, jobs: usize, config: &Config) -> Result<Vec<Report>, String> {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    let next = AtomicU64::new(from);
    let failed = AtomicBool::new(false);
    let results = Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..jobs.max(1) {
            scope.spawn(|| {
                while !failed.load(Ordering::Relaxed) {
                    let seed = next.fetch_add(1, Ordering::Relaxed);
                    if seed >= from + count {
                        break;
                    }
                    let result = run_seed(seed, config);
                    if result.is_err() {
                        failed.store(true, Ordering::Relaxed);
                    }
                    if let Ok(mut results) = results.lock() {
                        results.push((seed, result));
                    }
                }
            });
        }
    });
    let mut results = results.into_inner().map_err(|e| e.to_string())?;
    results.sort_by_key(|(seed, _)| *seed);
    results
        .into_iter()
        .map(|(_, result)| result.map_err(|f| f.to_string()))
        .collect()
}
