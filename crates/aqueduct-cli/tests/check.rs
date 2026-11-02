//! `aqueduct check` (ADR 0037): lints are reported, recursion does not compile, and registering a
//! version reports its lints too.

#![allow(clippy::unwrap_used, clippy::expect_used)]

fn cli(args: &[&str]) -> Result<String, String> {
    aqueduct_cli::run(&args.iter().map(|a| a.to_string()).collect::<Vec<_>>())
}

#[test]
fn check_reports_lints_and_refuses_recursion() {
    let dir = std::env::temp_dir().join(format!("aqueduct-cli-check-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let write = |name: &str, source: &str| {
        let path = dir.join(name);
        std::fs::write(&path, source).unwrap();
        path.to_str().unwrap().to_owned()
    };
    let waits = write(
        "waits.star",
        "def run(x):\n    return wait(signal(\"approved\"))\n",
    );
    let out = cli(&["check", &waits]).unwrap();
    assert!(out.starts_with("waits.star compiles: version "), "{out}");
    assert!(
        out.contains("warning: line 2: wait(...) has no timeout")
            && out.contains("(unbounded-wait)"),
        "{out}"
    );
    let recursive = write(
        "recursive.star",
        "def f(n):\n    return f(n)\n\ndef run(x):\n    return f(x)\n",
    );
    let error = cli(&["check", &recursive]).unwrap_err();
    assert!(error.contains("recursion is not allowed"), "{error}");

    let path = dir.to_str().unwrap();
    let registered = cli(&["versions", "register", path, "w", &waits, "--by", "ada"]).unwrap();
    assert!(registered.contains("(unbounded-wait)"), "{registered}");
    assert!(
        cli(&["versions", "register", path, "r", &recursive, "--by", "ada"])
            .unwrap_err()
            .contains("recursion is not allowed")
    );
    std::fs::remove_dir_all(&dir).unwrap();
}
