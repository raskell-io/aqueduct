//! `aqueduct server` refuses to listen beyond loopback unless asked to (ADR 0038).

fn cli(args: &[&str]) -> Result<String, String> {
    aqueduct_cli::run(&args.iter().map(|a| a.to_string()).collect::<Vec<_>>())
}

#[test]
fn the_server_listens_on_loopback_unless_told_otherwise() {
    let dir = std::env::temp_dir().join(format!("aqueduct-cli-server-{}", std::process::id()));
    let dir = dir.to_str().unwrap_or_default();
    let refused = cli(&["server", dir, "--listen", "0.0.0.0:7470"]).unwrap_err();
    assert!(refused.contains("--listen-anywhere"), "{refused}");
    let refused = cli(&["server", dir, "--listen", "localhost"]).unwrap_err();
    assert!(refused.contains("like 127.0.0.1:7470"), "{refused}");
    let refused = cli(&["server", dir, "--port", "1"]).unwrap_err();
    assert!(refused.contains("unknown option"), "{refused}");
}
