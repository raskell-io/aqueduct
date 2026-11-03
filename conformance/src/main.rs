//! `aqueduct-conformance [--worker <command>]`: runs the conformance suite. Without a worker command,
//! against the steps in process and against `aqueduct-worker` over gRPC.

use aqueduct_conformance::{Transport, run};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let transports = match args.as_slice() {
        [] => vec![Transport::InProcess, Transport::RustGrpc],
        [flag, command] if flag == "--worker" => vec![Transport::Command(command.clone())],
        _ => {
            eprintln!("usage: aqueduct-conformance [--worker <command>]");
            return std::process::ExitCode::from(2);
        }
    };
    let mut failed = 0;
    for transport in &transports {
        println!("{transport}:");
        match run(transport).await {
            Ok(checks) => {
                for check in checks {
                    if check.passed {
                        println!("  pass  {}", check.scenario);
                    } else {
                        failed += 1;
                        println!("  FAIL  {}: {}", check.scenario, check.detail);
                    }
                }
            }
            Err(error) => {
                failed += 1;
                println!("  FAIL  {error}");
            }
        }
    }
    if failed > 0 {
        println!("{failed} failed");
        std::process::ExitCode::FAILURE
    } else {
        std::process::ExitCode::SUCCESS
    }
}
