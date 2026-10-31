//! Developer tasks for Aqueduct. Run with `cargo xtask <task>`.

#![forbid(unsafe_code)]

mod fsync;
mod sim;

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("fsync") => fsync::run(&args[1..]).map_err(|e| e.to_string()),
        Some("sim") => sim::run(&args[1..]),
        _ => {
            eprintln!(
                "usage: cargo xtask <task>\n\ntasks:\n  fsync [dir]                    measure fsync latency and group commit throughput\n  sim --seed N | --seeds N [--from S]   run the deterministic simulator"
            );
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}
