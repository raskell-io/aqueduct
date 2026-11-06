//! `aqueduct migrate` (ADR 0047), end to end on the real binary: runs and versions move to a server, go
//! on there, and do not go on in the source.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

const BINARY: &str = env!("CARGO_BIN_EXE_aqueduct");

const WAITS: &str = "def run(x):\n    return [x, wait(signal(\"go\"))]\n";
const DONE: &str = "def run(x):\n    return x + 1\n";
const STEPS: &str = "def run(x):\n    return step(\"later\", step(\"first\", x))\n";
const PARENT: &str = "def run(x):\n    return call(\"waits\", x, key = \"kid\")\n";
const COUNTDOWN: &str = r#"
def run(n):
    if n > 0:
        continue_as_new(n - 1)
    return wait(signal("go"))
"#;
const ADD: &str = r#"
def run(call):
    total = call["state"].get("n", 0) + call["args"]
    return {"output": total, "writes": {"n": total}}
"#;

static NEXT: AtomicU32 = AtomicU32::new(0);

fn fresh(name: &str) -> PathBuf {
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("aqueduct-mig-{name}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Runs the command line: what it printed, and whether it succeeded.
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

fn ok(args: &[&str]) -> String {
    cli(args).unwrap_or_else(|e| panic!("{args:?}: {e}"))
}

fn activate(dir: &Path, workflow: &str, source: &str) {
    let file = dir.join(format!("{workflow}.star"));
    std::fs::write(&file, source).unwrap();
    let d = dir.to_str().unwrap();
    let out = ok(&[
        "versions",
        "register",
        d,
        workflow,
        file.to_str().unwrap(),
        "--by",
        "ada",
    ]);
    let id = out
        .split_whitespace()
        .nth(3)
        .unwrap()
        .trim_end_matches(':')
        .to_owned();
    ok(&["versions", "propose", d, workflow, &id, "--by", "ada"]);
    ok(&["versions", "prove", d, workflow, &id, "--by", "ada"]);
    ok(&["versions", "approve", d, workflow, &id, "--by", "grace"]);
    ok(&["versions", "activate", d, workflow, &id, "--by", "grace"]);
}

struct Server {
    child: Child,
    addr: String,
    log: Arc<Mutex<String>>,
}

/// A server is stopped however its test ends, so a failing test leaves no process behind.
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Server {
    fn start(dir: &Path, config: &Path) -> Result<Server, String> {
        let mut child = Command::new(BINARY)
            .args(["server", dir.to_str().unwrap(), "--listen", "127.0.0.1:0"])
            .args(["--config", config.to_str().unwrap()])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let log = Arc::new(Mutex::new(String::new()));
        let (found, address) = std::sync::mpsc::channel();
        {
            let (stderr, log) = (child.stderr.take().unwrap(), Arc::clone(&log));
            std::thread::spawn(move || {
                for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                    log.lock().unwrap().push_str(&line);
                    log.lock().unwrap().push('\n');
                    if let Some(at) = line.find("://").filter(|_| line.starts_with("serving ")) {
                        let rest = &line[at + 3..];
                        let addr = rest.split_whitespace().next().unwrap_or("").to_owned();
                        let _ = found.send(addr);
                    }
                }
            });
        }
        let begin = Instant::now();
        loop {
            if let Ok(addr) = address.try_recv() {
                return Ok(Server { child, addr, log });
            }
            if let Some(status) = child.try_wait().unwrap() {
                std::thread::sleep(Duration::from_millis(100));
                let text = log.lock().unwrap().clone();
                return Err(format!("the server exited ({status}): {text}"));
            }
            assert!(begin.elapsed() < Duration::from_secs(30), "no server");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn stop(self) -> String {
        let pid = self.child.id().to_string();
        Command::new("kill").args(["-TERM", &pid]).status().unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let log = self.log.clone();
        drop(self);
        log.lock().unwrap().clone()
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn http(&self, token: Option<&str>, method: &str, path: &str, body: Value) -> (u16, Value) {
        let mut stream = std::net::TcpStream::connect(&self.addr).unwrap();
        let body = if body.is_null() {
            String::new()
        } else {
            body.to_string()
        };
        let authorization =
            token.map_or(String::new(), |t| format!("Authorization: Bearer {t}\r\n"));
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.addr,
            body.len()
        )
        .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        let (_, text) = response.split_once("\r\n\r\n").unwrap();
        (
            response[9..12].parse().unwrap(),
            serde_json::from_str(text).unwrap_or(Value::Null),
        )
    }

    fn start_run(&self, token: Option<&str>, key: &str, workflow: &str, input: Value) {
        let body = json!({"key": key, "workflow": workflow, "input": input});
        let (status, answer) = self.http(token, "POST", "/v1/runs", body);
        assert_eq!(status, 200, "{answer}");
    }

    fn until_running(&self, token: Option<&str>, key: &str) {
        let (status, run) = self.http(token, "GET", &format!("/v1/runs/{key}"), Value::Null);
        assert_eq!(
            (status, run["status"].as_str()),
            (200, Some("running")),
            "{run}"
        );
    }

    fn result(&self, token: Option<&str>, key: &str) -> Value {
        let path = format!("/v1/runs/{key}/result?timeout_ms=10000");
        let (status, result) = self.http(token, "GET", &path, Value::Null);
        assert_eq!(status, 200, "{result}");
        assert_eq!(result["status"], "completed", "{result}");
        result["output"].clone()
    }

    fn signal(&self, token: Option<&str>, key: &str, payload: Value) {
        let path = format!("/v1/runs/{key}/signals/go");
        let (status, answer) = self.http(token, "POST", &path, json!({"payload": payload}));
        assert_eq!(status, 200, "{answer}");
    }

    /// Answers the steps that are waiting, one at a time, until `count` have been.
    fn work(&self, token: Option<&str>, steps: &[&str], count: usize) -> Vec<Value> {
        let mut seen = Vec::new();
        while seen.len() < count {
            let body = json!({"worker": "w", "steps": steps, "max": 1, "wait_ms": 2000});
            let (status, polled) = self.http(token, "POST", "/v1/workers/poll", body);
            assert_eq!(status, 200, "{polled}");
            for invocation in polled["invocations"].as_array().unwrap() {
                let id = &invocation["invocation_id"];
                let answer =
                    json!({"results": [{"invocation_id": id, "completed": invocation["input"]}]});
                let (status, text) = self.http(token, "POST", "/v1/workers/results", answer);
                assert_eq!(status, 200, "{text}");
                seen.push(invocation.clone());
            }
        }
        seen
    }
}

fn config(dir: &Path) -> PathBuf {
    let file = dir.join("server.json");
    std::fs::write(&file, json!({"objects": ["counter"]}).to_string()).unwrap();
    file
}

/// A source directory holding one of everything, left stopped with runs open.
fn source(root: &Path) -> PathBuf {
    let src = root.join("src");
    std::fs::create_dir_all(&src).unwrap();
    for (workflow, text) in [
        ("waits", WAITS),
        ("done", DONE),
        ("steps", STEPS),
        ("parent", PARENT),
        ("countdown", COUNTDOWN),
        ("counter.add", ADD),
    ] {
        activate(&src, workflow, text);
    }
    let server = Server::start(&src, &config(root)).unwrap();
    server.start_run(None, "waiting", "waits", json!(1));
    server.start_run(None, "finished", "done", json!(41));
    assert_eq!(server.result(None, "finished"), 42);
    server.start_run(None, "stepping", "steps", json!("s"));
    // One step done here; the next is what a worker on the server picks up.
    assert_eq!(server.work(None, &["first"], 1).len(), 1);
    server.start_run(None, "parent", "parent", json!(7));
    server.start_run(None, "looping", "countdown", json!(2));
    let (status, called) = server.http(
        None,
        "POST",
        "/v1/objects/counter/c/add",
        json!({"args": 5, "id": "one", "timeout_ms": 10000}),
    );
    assert_eq!(status, 200, "{called}");
    // Let the children and successors start.
    let begin = Instant::now();
    while server.http(None, "GET", "/v1/runs/kid", Value::Null).0 != 200
        || server.http(None, "GET", "/v1/runs/looping", Value::Null).1["status"] != "running"
    {
        assert!(begin.elapsed() < Duration::from_secs(10));
        std::thread::sleep(Duration::from_millis(10));
    }
    server.stop();
    src
}

fn admin_token(dst: &Path, name: &str, namespace: &str, permissions: &str) -> (String, String) {
    let out = ok(&[
        "tokens",
        "create",
        dst.to_str().unwrap(),
        "--name",
        name,
        "--namespace",
        namespace,
        "--permissions",
        permissions,
    ]);
    let secret = out
        .split_whitespace()
        .find(|word| word.starts_with("aqd_"))
        .unwrap()
        .to_owned();
    let file = dst.parent().unwrap().join(format!("{name}.token"));
    std::fs::write(&file, &secret).unwrap();
    (secret, file.to_str().unwrap().to_owned())
}

#[test]
fn runs_and_versions_move_and_go_on_there() {
    let root = fresh("whole");
    let src = source(&root);
    let dst = root.join("dst");
    std::fs::create_dir_all(&dst).unwrap();
    let cfg = config(&root);
    let (admin, admin_file) = admin_token(&dst, "ops", "*", "admin");
    let token = Some(admin.as_str());
    let (from, d) = (src.to_str().unwrap(), dst.to_str().unwrap());

    let server = Server::start(&dst, &cfg).unwrap();
    let url = server.url();

    // A dry run changes nothing.
    let dry = ok(&["migrate", "--from", from, "--to", &url, "--dry-run"]);
    assert!(dry.contains("nothing was changed"), "{dry}");
    assert!(!src.join("migrated.json").exists());

    let out = ok(&[
        "migrate",
        "--from",
        from,
        "--to",
        &url,
        "--token-file",
        &admin_file,
    ]);
    assert!(out.contains("restart the server"), "{out}");

    // The source will not run again.
    let refused = Server::start(&src, &cfg).err().unwrap();
    assert!(refused.contains("was migrated to"), "{refused}");
    // Nor can what it did be undone now.
    assert!(cli(&["migrate", "--from", from, "--abort"]).is_err());

    // Nothing is visible until the server restarts, and only one import waits at a time.
    assert_eq!(
        server.http(token, "GET", "/v1/runs/waiting", Value::Null).0,
        404
    );
    let twice = cli(&[
        "migrate",
        "--from",
        from,
        "--to",
        &url,
        "--token-file",
        &admin_file,
    ]);
    assert!(
        twice.unwrap_err().contains("restart"),
        "a second import while one waits"
    );
    server.stop();

    let server = Server::start(&dst, &cfg).unwrap();
    let log = server.log.lock().unwrap().clone();
    assert!(log.contains("import applied"), "{log}");

    // What ended is there, and what is open goes on.
    assert_eq!(server.result(token, "finished"), 42);
    server.until_running(token, "waiting");
    server.signal(token, "waiting", json!("from the server"));
    assert_eq!(
        server.result(token, "waiting"),
        json!([1, "from the server"])
    );
    // The step done at the source is not done again; the next one comes with the same key.
    let stepped = server.work(token, &["later", "first"], 1);
    assert_eq!(stepped[0]["step"], "later", "{stepped:?}");
    assert_eq!(server.result(token, "stepping"), "s");
    // A parent and the child it waits for: not a second child.
    server.signal(token, "kid", json!("grown"));
    assert_eq!(server.result(token, "parent"), json!([7, "grown"]));
    // A run several generations in follows on.
    server.signal(token, "looping", json!("end"));
    assert_eq!(server.result(token, "looping"), "end");
    // An object keeps its state, and a call repeated by id is not run again.
    let (_, state) = server.http(token, "GET", "/v1/objects/counter/c", Value::Null);
    assert_eq!(state["state"]["n"], 5, "{state}");
    let add = |id: &str, n: i32| {
        server.http(
            token,
            "POST",
            "/v1/objects/counter/c/add",
            json!({"args": n, "id": id, "timeout_ms": 10000}),
        )
    };
    assert_eq!(
        add("one", 5).1["output"],
        5,
        "the same id: the first answer"
    );
    assert_eq!(add("two", 3).1["output"], 8);

    // New runs start on the versions that came with it.
    server.start_run(token, "new", "done", json!(1));
    assert_eq!(server.result(token, "new"), 2);

    // Once applied, sending it again is safe, and adds nothing.
    let again = ok(&[
        "migrate",
        "--from",
        from,
        "--to",
        &server.url(),
        "--token-file",
        &admin_file,
    ]);
    assert!(again.contains("restart"), "{again}");
    server.stop();
    let server = Server::start(&dst, &cfg).unwrap();
    let log = server.log.lock().unwrap().clone();
    assert!(log.contains("import applied: 0 runs added"), "{log}");
    assert_eq!(
        server.result(token, "waiting"),
        json!([1, "from the server"])
    );
    server.stop();

    // Its histories verify, its versions say who approved them, and the audit log has it.
    assert!(ok(&["verify", d]).contains("runs verified"));
    let versions = ok(&["versions", "log", d]);
    assert!(versions.contains("grace"), "{versions}");
    let audit = ok(&["audit", d]);
    assert!(audit.contains("import"), "{audit}");
    assert!(audit.contains("ops"), "{audit}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_key_with_another_run_refuses_the_whole_import() {
    let root = fresh("conflict");
    let src = source(&root);
    let dst = root.join("dst");
    std::fs::create_dir_all(&dst).unwrap();
    activate(&dst, "done", DONE);
    let cfg = config(&root);
    let (admin, admin_file) = admin_token(&dst, "ops", "*", "admin");
    let token = Some(admin.as_str());
    let from = src.to_str().unwrap();
    let server = Server::start(&dst, &cfg).unwrap();
    server.start_run(token, "finished", "done", json!(1000));
    assert_eq!(server.result(token, "finished"), 1001);

    let refused = cli(&[
        "migrate",
        "--from",
        from,
        "--to",
        &server.url(),
        "--token-file",
        &admin_file,
    ]);
    let refused = refused.unwrap_err();
    assert!(refused.contains("finished"), "{refused}");
    assert!(refused.contains("--abort"), "{refused}");
    server.stop();

    // Nothing arrives by restarting, the server's run is untouched, and the source can run again.
    let server = Server::start(&dst, &cfg).unwrap();
    assert_eq!(server.result(token, "finished"), 1001);
    assert_eq!(
        server.http(token, "GET", "/v1/runs/waiting", Value::Null).0,
        404
    );
    server.stop();
    assert!(Server::start(&src, &cfg).is_err(), "still frozen");
    ok(&["migrate", "--from", from, "--abort"]);
    let back = Server::start(&src, &cfg).unwrap();
    back.until_running(None, "waiting");
    back.stop();
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn an_import_cannot_get_around_the_servers_policy() {
    let root = fresh("policy");
    let src = source(&root);
    let dst = root.join("dst");
    std::fs::create_dir_all(dst.join("registry")).unwrap();
    // This server wants two approvals; the source's versions have one.
    let policy = dst.join("registry/policy.json");
    std::fs::write(&policy, r#"{"default": {"approvals": 2}}"#).unwrap();
    let cfg = config(&root);
    let (admin, admin_file) = admin_token(&dst, "ops", "*", "admin");
    let token = Some(admin.as_str());
    let from = src.to_str().unwrap();
    let server = Server::start(&dst, &cfg).unwrap();
    ok(&[
        "migrate",
        "--from",
        from,
        "--to",
        &server.url(),
        "--token-file",
        &admin_file,
    ]);
    server.stop();

    // On restart it is set aside and reported, the server serves on, and none of it arrived.
    let server = Server::start(&dst, &cfg).unwrap();
    let log = server.log.lock().unwrap().clone();
    assert!(log.contains("would not be approved here"), "{log}");
    assert_eq!(
        server.http(token, "GET", "/v1/runs/waiting", Value::Null).0,
        404
    );
    assert!(!dst.join("import/pending").exists());
    server.stop();
    let audit = ok(&["audit", dst.to_str().unwrap()]);
    assert!(audit.contains("would not be approved here"), "{audit}");

    // With the policy changed, sending it again brings it all.
    std::fs::write(&policy, r#"{"default": {"approvals": 1}}"#).unwrap();
    let server = Server::start(&dst, &cfg).unwrap();
    ok(&[
        "migrate",
        "--from",
        from,
        "--to",
        &server.url(),
        "--token-file",
        &admin_file,
    ]);
    server.stop();
    let server = Server::start(&dst, &cfg).unwrap();
    server.until_running(token, "waiting");
    server.stop();
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn an_import_needs_an_admin_of_the_default_namespace() {
    let root = fresh("tokens");
    let src = source(&root);
    let dst = root.join("dst");
    std::fs::create_dir_all(&dst).unwrap();
    let cfg = config(&root);
    let (_, reader) = admin_token(&dst, "reader", "default", "read,start");
    let (_, tenant) = admin_token(&dst, "tenant", "acme", "admin");
    let from = src.to_str().unwrap();
    let server = Server::start(&dst, &cfg).unwrap();
    let url = server.url();

    // No token: the server is not touched, whatever else.
    let none = cli(&["migrate", "--from", from, "--to", &url]).unwrap_err();
    assert!(none.contains("Unauthenticated"), "{none}");
    let weak = cli(&[
        "migrate",
        "--from",
        from,
        "--to",
        &url,
        "--token-file",
        &reader,
    ])
    .unwrap_err();
    assert!(weak.contains("PermissionDenied"), "{weak}");
    let bound = cli(&[
        "migrate",
        "--from",
        from,
        "--to",
        &url,
        "--token-file",
        &tenant,
    ])
    .unwrap_err();
    assert!(bound.contains("default namespace"), "{bound}");
    assert!(!dst.join("import/pending").exists());

    // A server that cannot be reached leaves the source as it was.
    server.stop();
    ok(&["migrate", "--from", from, "--abort"]);
    let unreachable = cli(&["migrate", "--from", from, "--to", &url]).unwrap_err();
    assert!(unreachable.contains("cannot connect"), "{unreachable}");
    assert!(!src.join("migrated.json").exists());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_rust_workflows_open_runs_wait_for_something_to_run_them() {
    use aqueduct_sdk::{WorkflowContext, WorkflowError, workflow};

    async fn charge(ctx: WorkflowContext, input: String) -> Result<String, WorkflowError> {
        let charged: String = ctx.step("charge", &input).await?;
        let confirmed: String = ctx.step("confirm", &charged).await?;
        Ok(confirmed)
    }
    let open = |dir: &Path, gate: Arc<std::sync::Barrier>| {
        aqueduct_engine::embedded::Engine::builder(dir)
            .workflow("charge", Arc::new(workflow(charge)))
            .step("charge", |input: &aqueduct_core::Payload| {
                aqueduct_core::StepOutcome::Completed(input.clone())
            })
            .step("confirm", move |input: &aqueduct_core::Payload| {
                gate.wait();
                aqueduct_core::StepOutcome::Completed(input.clone())
            })
            .open()
            .unwrap()
    };
    let root = fresh("rust");
    let src = root.join("src");
    let dst = root.join("dst");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::create_dir_all(&dst).unwrap();
    let cfg = root.join("empty.json");
    std::fs::write(&cfg, "{}").unwrap();
    {
        // Stopped with its second step under way: as an application is when it is shut down.
        let gate = Arc::new(std::sync::Barrier::new(2));
        let engine = open(&src, Arc::clone(&gate));
        engine
            .start("order", "charge", aqueduct_core::Payload::new(*b"\"o\""))
            .unwrap();
        std::thread::sleep(Duration::from_millis(200));
        gate.wait();
        let finished = engine.wait("order", Duration::from_secs(5)).unwrap();
        assert!(format!("{finished:?}").contains("Completed"));
        engine
            .start("order-2", "charge", aqueduct_core::Payload::new(*b"\"p\""))
            .unwrap();
        std::thread::sleep(Duration::from_millis(200));
        std::mem::forget(gate);
        std::mem::forget(engine);
    }
    // That process has stopped, as far as the directory can tell, only when its lock is free.
    let child_copy = root.join("src-copy");
    copy_dir(&src, &child_copy);
    let from = child_copy.to_str().unwrap();

    let server = Server::start(&dst, &cfg).unwrap();
    let out = ok(&["migrate", "--from", from, "--to", &server.url()]);
    assert!(out.contains("`charge` has 1 open runs"), "{out}");
    server.stop();

    // The server has no code for it: it starts anyway, and the run waits, whole.
    let server = Server::start(&dst, &cfg).unwrap();
    server.until_running(None, "order-2");
    assert_eq!(server.result(None, "order"), "o");
    server.stop();

    // Something that can run it takes over where it was.
    let gate = Arc::new(std::sync::Barrier::new(1));
    let engine = open(&dst, gate);
    let finished = engine.wait("order-2", Duration::from_secs(5)).unwrap();
    assert!(
        format!("{finished:?}").contains("Completed"),
        "{finished:?}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}
