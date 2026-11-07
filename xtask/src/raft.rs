//! `cargo xtask raft`: the Raft simulator (ADR 0051).

use aqueduct_raft::sim::{SimConfig, SimReport, run as simulate};

pub fn run(args: &[String]) -> Result<(), String> {
    let (mut from, mut count, mut nodes) = (0u64, 1000u64, 5u64);
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        let mut number = |what: &str| -> Result<u64, String> {
            iter.next()
                .and_then(|v| v.parse().ok())
                .ok_or_else(|| format!("{what} needs a number"))
        };
        match arg.as_str() {
            "--seed" => {
                from = number("--seed")?;
                count = 1;
            }
            "--seeds" => count = number("--seeds")?,
            "--from" => from = number("--from")?,
            "--nodes" => nodes = number("--nodes")?,
            other => return Err(format!("unknown option `{other}`")),
        }
    }
    let config = SimConfig {
        nodes,
        ..SimConfig::default()
    };
    let started = std::time::Instant::now();
    let mut total = SimReport::default();
    for seed in from..from + count {
        let report = simulate(seed, &config).map_err(|failure| {
            format!(
                "seed {} failed at time {}: {}\nreproduce with: cargo xtask raft --seed {} --nodes {nodes}",
                failure.seed, failure.time, failure.message, failure.seed
            )
        })?;
        total.events += report.events;
        total.crashes += report.crashes;
        total.partitions += report.partitions;
        total.pauses += report.pauses;
        total.leaders += report.leaders;
        total.applied += report.applied;
        total.dropped += report.dropped;
        total.duplicated += report.duplicated;
        total.compactions += report.compactions;
        total.installs += report.installs;
    }
    println!(
        "{count} seeds ({from}..{}, {nodes} nodes) passed in {:.1} s\n\
         events {}, crashes {}, network splits {}, clock pauses {}, terms with a leader {}\n\
         messages dropped {}, duplicated {}, entries applied on every node {}\n\
         logs compacted {}, snapshots installed by followers {}",
        from + count,
        started.elapsed().as_secs_f64(),
        total.events,
        total.crashes,
        total.partitions,
        total.pauses,
        total.leaders,
        total.dropped,
        total.duplicated,
        total.applied,
        total.compactions,
        total.installs,
    );
    Ok(())
}
