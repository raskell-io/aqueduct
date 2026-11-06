//! `aqueduct deployments` (ADR 0048) on the real binary: a build activated, listed, and refused retirement.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

const BINARY: &str = env!("CARGO_BIN_EXE_aqueduct");

fn cli(args: &[&str]) -> Result<String, String> {
    let output = Command::new(BINARY)
        .args(args)
        .env_remove("AQUEDUCT_TOKEN")
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&output.stdout).into_owned()
        + &String::from_utf8_lossy(&output.stderr);
    if output.status.success() {
        Ok(text)
    } else {
        Err(text)
    }
}

/// The server, stopped however the test ends.
struct Stopped(std::process::Child);

impl Drop for Stopped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn builds_are_activated_listed_and_retired_from_the_command_line() {
    let root = std::env::temp_dir().join(format!("aqueduct-cli-deploy-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let dir = root.join("data");
    let config = root.join("server.json");
    std::fs::write(&config, r#"{"remote_workflows": ["order"]}"#).unwrap();
    let mut server = Stopped(
        Command::new(BINARY)
            .args(["server", dir.to_str().unwrap(), "--listen", "127.0.0.1:0"])
            .args(["--config", config.to_str().unwrap()])
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let url = BufReader::new(server.0.stderr.take().unwrap())
        .lines()
        .map_while(Result::ok)
        .find(|line| line.starts_with("serving "))
        .and_then(|line| {
            let at = line.find("http://")?;
            Some(line[at..].split_whitespace().next()?.to_owned())
        })
        .unwrap();
    let run = |args: &[&str]| {
        let mut all = vec!["deployments"];
        all.extend_from_slice(args);
        all.extend_from_slice(&["--server", &url]);
        cli(&all)
    };

    let refused = run(&["activate", "order", "v1"]).unwrap_err();
    assert!(refused.contains("no connected worker"), "{refused}");
    run(&["activate", "order", "v1", "--force"]).unwrap();
    run(&["activate", "order", "v2", "--force"]).unwrap();
    let listed = run(&["list"]).unwrap();
    assert!(listed.contains("active"), "{listed}");
    // With nothing on it, the build it replaced is drained: its workers can stop.
    assert!(listed.contains("drained"), "{listed}");
    assert!(
        run(&["retire", "order", "v2"])
            .unwrap_err()
            .contains("active")
    );
    run(&["retire", "order", "v1"]).unwrap();
    assert!(run(&["list"]).unwrap().contains("retired"));
    assert!(run(&["oops"]).unwrap_err().contains("usage"));

    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}
