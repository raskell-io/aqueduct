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

#[test]
fn listening_beyond_loopback_needs_tokens_and_tls() {
    let dir = std::env::temp_dir().join(format!("aqueduct-cli-beyond-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap_or_default();
    let path = dir.to_str().unwrap_or_default();
    let anywhere = ["server", path, "--listen", "0.0.0.0:7470"];
    assert!(cli(&anywhere).unwrap_err().contains("no tokens"));
    cli(&[
        "tokens",
        "create",
        path,
        "--name",
        "ada",
        "--permissions",
        "admin",
    ])
    .unwrap_or_default();
    assert!(cli(&anywhere).unwrap_err().contains("no certificate"));
    let half = ["server", path, "--tls-cert", "cert.pem"];
    assert!(cli(&half).unwrap_err().contains("go together"));
    let missing = [
        "server",
        path,
        "--tls-cert",
        "no-cert.pem",
        "--tls-key",
        "no-key.pem",
    ];
    assert!(cli(&missing).unwrap_err().contains("no-cert.pem"));
    let _ = std::fs::remove_dir_all(&dir);
}
