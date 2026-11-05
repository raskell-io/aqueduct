//! Namespaces (ADR 0046): tenants of one server cannot see or reach each other's runs, keys, objects or
//! steps, and have their own quotas.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aqueduct_engine::embedded::Engine;
use aqueduct_procedure::Procedure;
use aqueduct_protocol::v1;
use aqueduct_protocol::v1::ingress_client::IngressClient;
use aqueduct_server::auth::create_token_in;
use aqueduct_server::{Config, Permission, Security};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ECHO: &str = "def run(x):\n    return x\n";
const WAITS: &str = "def run(x):\n    return wait(signal(\"go\"), timeout = 600)\n";
const ESCALATES: &str = "def run(x):\n    return escalate(\"review\", x)\n";
const STEPS: &str = "def run(x):\n    return step(\"work\", x)\n";
const PARENT: &str = "def run(key):\n    return call(\"echo\", key, key = key, fallible = True)\n";
const SENDER: &str = r#"
def run(x):
    send("counter", x["key"], "add", x["n"])
    return "sent"
"#;
const ADD: &str = r#"
def run(call):
    return {"output": call["args"], "writes": {"n": call["args"]}}
"#;

struct Server {
    addr: SocketAddr,
    dir: std::path::PathBuf,
}

/// A request, as a tenant: with its token and, if it is given, the header naming a namespace.
async fn http(
    addr: SocketAddr,
    (token, namespace): (Option<&str>, Option<&str>),
    method: &str,
    path: &str,
    body: Value,
) -> (u16, Value) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = body.to_string();
    let authorization = token.map_or(String::new(), |t| format!("Authorization: Bearer {t}\r\n"));
    let named = namespace.map_or(String::new(), |n| format!("Aqueduct-Namespace: {n}\r\n"));
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{authorization}{named}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let (_, text) = response.split_once("\r\n\r\n").unwrap();
    (
        response[9..12].parse().unwrap(),
        serde_json::from_str(text).unwrap_or(Value::Null),
    )
}

fn everything() -> BTreeSet<Permission> {
    [
        Permission::Start,
        Permission::Signal,
        Permission::Cancel,
        Permission::Resolve,
        Permission::Read,
        Permission::Work,
    ]
    .into_iter()
    .collect()
}

/// A server with one token per entry of `tokens`: its name and namespace.
async fn serve(name: &str, config: Value, tokens: &[(&str, &str)]) -> (Server, Vec<String>) {
    let dir = std::env::temp_dir().join(format!("aqueduct-ns-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config: Config = serde_json::from_value(config).unwrap();
    config.check().unwrap();
    let mut builder = Engine::builder(&dir).remote_steps(Duration::from_secs(30));
    for (workflow, source) in [
        ("echo", ECHO),
        ("waits", WAITS),
        ("escalates", ESCALATES),
        ("steps", STEPS),
        ("parent", PARENT),
        ("sender", SENDER),
    ] {
        builder = builder.workflow(
            workflow,
            Arc::new(Procedure::compile("p.star", source).unwrap()),
        );
    }
    let builder = builder.object_handler(
        "counter",
        "add",
        Arc::new(Procedure::compile("a.star", ADD).unwrap()),
    );
    let engine = Arc::new(config.apply(builder).unwrap().open().unwrap());
    let secrets = tokens
        .iter()
        .map(|(name, namespace)| create_token_in(&dir, name, everything(), namespace).unwrap())
        .collect();
    let security = if tokens.is_empty() {
        Security::none()
    } else {
        Security::for_dir(&dir).unwrap()
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        aqueduct_server::serve_with(engine, listener, &config, security, std::future::pending())
            .await
    });
    (Server { addr, dir }, secrets)
}

fn tenants() -> Value {
    json!({})
}

async fn two_tenants(name: &str) -> (Server, String, String) {
    let (server, mut secrets) =
        serve(name, tenants(), &[("acme", "acme"), ("globex", "globex")]).await;
    let globex = secrets.pop().unwrap();
    let acme = secrets.pop().unwrap();
    (server, acme, globex)
}

fn start(key: &str, workflow: &str, input: Value) -> Value {
    json!({"key": key, "workflow": workflow, "input": input})
}

#[tokio::test(flavor = "multi_thread")]
async fn the_same_key_is_two_runs_in_two_namespaces() {
    let (server, acme, globex) = two_tenants("keys").await;
    let (a, g) = ((Some(acme.as_str()), None), (Some(globex.as_str()), None));
    // If the key were shared, the second start would clash: the input differs.
    let (status, started) = http(
        server.addr,
        a,
        "POST",
        "/v1/runs",
        start("k", "echo", json!(1)),
    )
    .await;
    assert_eq!(status, 200, "{started}");
    assert_eq!(started["run_id"], "k", "a tenant sees its own key");
    let (status, _) = http(
        server.addr,
        g,
        "POST",
        "/v1/runs",
        start("k", "echo", json!(2)),
    )
    .await;
    assert_eq!(status, 200);

    let (_, one) = http(
        server.addr,
        a,
        "GET",
        "/v1/runs/k/result?timeout_ms=5000",
        Value::Null,
    )
    .await;
    let (_, two) = http(
        server.addr,
        g,
        "GET",
        "/v1/runs/k/result?timeout_ms=5000",
        Value::Null,
    )
    .await;
    assert_eq!(one["output"], 1, "{one}");
    assert_eq!(two["output"], 2, "{two}");

    let (_, described) = http(server.addr, a, "GET", "/v1/runs/k", Value::Null).await;
    assert_eq!(described["run_id"], "k", "{described}");
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_cannot_reach_anothers_runs() {
    let (server, acme, globex) = two_tenants("reach").await;
    let (a, g) = ((Some(acme.as_str()), None), (Some(globex.as_str()), None));
    let (status, _) = http(
        server.addr,
        a,
        "POST",
        "/v1/runs",
        start("secret", "waits", json!(1)),
    )
    .await;
    assert_eq!(status, 200);

    // Neither by its key, nor by the name it is stored under.
    for key in ["secret", "%24ns%2Facme%2Fsecret"] {
        for (method, path, body) in [
            ("GET", format!("/v1/runs/{key}"), Value::Null),
            (
                "GET",
                format!("/v1/runs/{key}/result?timeout_ms=1"),
                Value::Null,
            ),
            (
                "POST",
                format!("/v1/runs/{key}/signals/go"),
                json!({"payload": 1}),
            ),
            (
                "POST",
                format!("/v1/runs/{key}/cancel"),
                json!({"reason": "x"}),
            ),
            ("GET", format!("/v1/runs/{key}/queries/q"), Value::Null),
        ] {
            let (status, answer) = http(server.addr, g, method, &path, body).await;
            assert_eq!(status, 404, "{method} {path}: {answer}");
        }
    }
    // Nor start under another tenant's stored name.
    let (status, _) = http(
        server.addr,
        g,
        "POST",
        "/v1/runs",
        start("$ns/acme/stolen", "echo", json!(1)),
    )
    .await;
    assert_eq!(status, 404);
    // A key that is only part of a stored name is refused as well.
    let (status, _) = http(
        server.addr,
        g,
        "POST",
        "/v1/runs",
        start("x/$ns/acme/y", "echo", json!(1)),
    )
    .await;
    assert_eq!(status, 404);

    // Its owner's run was not touched.
    let (_, described) = http(server.addr, a, "GET", "/v1/runs/secret", Value::Null).await;
    assert_eq!(described["status"], "running", "{described}");

    // Lists show only a tenant's own runs, by their own keys.
    let (status, _) = http(
        server.addr,
        g,
        "POST",
        "/v1/runs",
        start("mine", "echo", json!(1)),
    )
    .await;
    assert_eq!(status, 200);
    let (_, listed) = http(server.addr, g, "GET", "/v1/runs", Value::Null).await;
    let ids: Vec<&str> = listed["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|run| run["run_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["mine"], "{listed}");
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bound_token_cannot_name_another_namespace() {
    let (server, mut secrets) = serve("header", tenants(), &[("acme", "acme"), ("ops", "*")]).await;
    let ops = secrets.pop().unwrap();
    let acme = secrets.pop().unwrap();
    let list = |who, namespace| {
        http(
            server.addr,
            (who, namespace),
            "GET",
            "/v1/runs",
            Value::Null,
        )
    };

    let (status, _) = list(Some(acme.as_str()), Some("globex")).await;
    assert_eq!(status, 403, "a token bound to acme names globex");
    let (status, _) = list(Some(acme.as_str()), Some("acme")).await;
    assert_eq!(status, 200, "and may name its own");
    let (status, _) = list(Some(ops.as_str()), Some("not/a/name")).await;
    assert_eq!(status, 403);

    // A token for all of them chooses per request, and its choice is a namespace like any other.
    let (a, g) = (
        (Some(acme.as_str()), None),
        (Some(ops.as_str()), Some("globex")),
    );
    http(
        server.addr,
        a,
        "POST",
        "/v1/runs",
        start("k", "waits", json!(1)),
    )
    .await;
    let (status, _) = http(server.addr, g, "GET", "/v1/runs/k", Value::Null).await;
    assert_eq!(status, 404, "globex has no k");
    let chosen = (Some(ops.as_str()), Some("acme"));
    let (status, _) = http(server.addr, chosen, "GET", "/v1/runs/k", Value::Null).await;
    assert_eq!(status, 200, "acme does");
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_without_tokens_keeps_its_default_namespace() {
    let (server, _) = serve("open", tenants(), &[]).await;
    let (status, started) = http(
        server.addr,
        (None, None),
        "POST",
        "/v1/runs",
        start("k", "waits", json!(1)),
    )
    .await;
    assert_eq!((status, started["run_id"].as_str()), (200, Some("k")));
    // The default namespace's keys are stored as they are: logs from before namespaces stay its own.
    let (_, listed) = http(
        server.addr,
        (None, Some("default")),
        "GET",
        "/v1/runs",
        Value::Null,
    )
    .await;
    assert_eq!(listed["runs"][0]["run_id"], "k");
    let (status, _) = http(
        server.addr,
        (None, Some("acme")),
        "GET",
        "/v1/runs/k",
        Value::Null,
    )
    .await;
    assert_eq!(status, 404);
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn escalations_are_listed_and_resolved_within_a_namespace() {
    let (server, acme, globex) = two_tenants("escalations").await;
    let (a, g) = ((Some(acme.as_str()), None), (Some(globex.as_str()), None));
    http(
        server.addr,
        a,
        "POST",
        "/v1/runs",
        start("claim", "escalates", json!(1)),
    )
    .await;

    let mut listed = Value::Null;
    for _ in 0..100 {
        (_, listed) = http(server.addr, a, "GET", "/v1/escalations", Value::Null).await;
        if !listed["escalations"].as_array().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let escalation = &listed["escalations"][0];
    assert_eq!(escalation["key"], "claim", "{listed}");
    let id = escalation["id"].as_str().unwrap().to_owned();
    assert!(!id.contains("$ns"), "{id}");

    let (_, theirs) = http(server.addr, g, "GET", "/v1/escalations", Value::Null).await;
    assert_eq!(theirs["escalations"], json!([]), "{theirs}");
    let path = format!(
        "/v1/escalations/{}",
        id.replace('/', "%2F").replace('#', "%23")
    );
    let (status, answer) = http(server.addr, g, "POST", &path, json!({"outcome": "stolen"})).await;
    assert_ne!(status, 200, "another tenant resolved it: {answer}");
    let (status, answer) = http(server.addr, a, "POST", &path, json!({"outcome": "ok"})).await;
    assert_eq!(status, 200, "{answer}");
    let (_, result) = http(
        server.addr,
        a,
        "GET",
        "/v1/runs/claim/result?timeout_ms=5000",
        Value::Null,
    )
    .await;
    assert_eq!(result["output"], "ok", "{result}");
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn objects_have_a_state_per_namespace() {
    let (server, acme, globex) = two_tenants("objects").await;
    let (a, g) = ((Some(acme.as_str()), None), (Some(globex.as_str()), None));
    let call = "/v1/objects/counter/c/add";
    let body = |n: i32| json!({"args": n, "id": "1", "timeout_ms": 5000});
    let (status, answer) = http(server.addr, a, "POST", call, body(1)).await;
    assert_eq!(status, 200, "{answer}");
    let (status, answer) = http(server.addr, g, "POST", call, body(2)).await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(
        answer["output"], 2,
        "the same id is not the same call: {answer}"
    );

    let (_, one) = http(server.addr, a, "GET", "/v1/objects/counter/c", Value::Null).await;
    let (_, two) = http(server.addr, g, "GET", "/v1/objects/counter/c", Value::Null).await;
    assert_eq!(one["state"]["n"], 1, "{one}");
    assert_eq!(two["state"]["n"], 2, "{two}");

    // Another tenant's object cannot be named.
    let path = "/v1/objects/counter/%24ns%2Facme%2Fc";
    let (status, _) = http(server.addr, g, "GET", path, Value::Null).await;
    assert_eq!(status, 400);
    let (status, _) = http(server.addr, g, "POST", &format!("{path}/add"), body(9)).await;
    assert_eq!(status, 400);
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn what_logic_chooses_stays_in_its_namespace() {
    let (server, acme, globex) = two_tenants("logic").await;
    let (a, g) = ((Some(acme.as_str()), None), (Some(globex.as_str()), None));

    // A child under a key the logic chose, in both tenants: two runs, each in its own namespace.
    http(
        server.addr,
        a,
        "POST",
        "/v1/runs",
        start("p", "parent", json!("shared")),
    )
    .await;
    http(
        server.addr,
        g,
        "POST",
        "/v1/runs",
        start("p", "parent", json!("shared")),
    )
    .await;
    for who in [a, g] {
        let (_, result) = http(
            server.addr,
            who,
            "GET",
            "/v1/runs/p/result?timeout_ms=5000",
            Value::Null,
        )
        .await;
        assert_eq!(result["output"]["value"], "shared", "{result}");
        let (status, _) = http(server.addr, who, "GET", "/v1/runs/shared", Value::Null).await;
        assert_eq!(status, 200, "the child is its parent's namespace's");
    }

    // Logic that names another tenant's run does not reach it: the command fails.
    http(
        server.addr,
        a,
        "POST",
        "/v1/runs",
        start("target", "waits", json!(1)),
    )
    .await;
    http(
        server.addr,
        g,
        "POST",
        "/v1/runs",
        start("attack", "parent", json!("$ns/acme/target")),
    )
    .await;
    let (_, result) = http(
        server.addr,
        g,
        "GET",
        "/v1/runs/attack/result?timeout_ms=5000",
        Value::Null,
    )
    .await;
    assert_eq!(
        result["output"]["ok"], false,
        "the call should fail: {result}"
    );
    let (_, target) = http(server.addr, a, "GET", "/v1/runs/target", Value::Null).await;
    assert_eq!(target["status"], "running");

    // A message goes to the sender's namespace's object.
    http(
        server.addr,
        g,
        "POST",
        "/v1/runs",
        start("m", "sender", json!({"key": "c", "n": 7})),
    )
    .await;
    let mut state = Value::Null;
    for _ in 0..200 {
        (_, state) = http(server.addr, g, "GET", "/v1/objects/counter/c", Value::Null).await;
        if state["state"]["n"] == 7 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(state["state"]["n"], 7, "{state}");
    let (status, mine) = http(server.addr, a, "GET", "/v1/objects/counter/c", Value::Null).await;
    assert_eq!(status, 404, "acme's object was never called: {mine}");

    // A message to another tenant's object does not arrive.
    http(
        server.addr,
        g,
        "POST",
        "/v1/runs",
        start("m2", "sender", json!({"key": "$ns/acme/c", "n": 9})),
    )
    .await;
    http(
        server.addr,
        g,
        "GET",
        "/v1/runs/m2/result?timeout_ms=5000",
        Value::Null,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (status, mine) = http(server.addr, a, "GET", "/v1/objects/counter/c", Value::Null).await;
    assert_eq!(status, 404, "acme's object was never called: {mine}");
    let _ = std::fs::remove_dir_all(&server.dir);
}

async fn poll(addr: SocketAddr, who: (Option<&str>, Option<&str>), worker: &str) -> Vec<Value> {
    let body = json!({"worker": worker, "steps": ["work"], "max": 10, "wait_ms": 300});
    let (status, polled) = http(addr, who, "POST", "/v1/workers/poll", body).await;
    assert_eq!(status, 200, "{polled}");
    polled["invocations"].as_array().unwrap().clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn workers_get_and_answer_only_their_namespaces_steps() {
    let (server, mut secrets) = serve(
        "workers",
        tenants(),
        &[("acme", "acme"), ("globex", "globex"), ("pool", "*")],
    )
    .await;
    let pool = secrets.pop().unwrap();
    let globex = secrets.pop().unwrap();
    let acme = secrets.pop().unwrap();
    let (a, g) = ((Some(acme.as_str()), None), (Some(globex.as_str()), None));
    http(
        server.addr,
        a,
        "POST",
        "/v1/runs",
        start("k", "steps", json!("from acme")),
    )
    .await;

    assert_eq!(poll(server.addr, g, "theirs").await, Vec::<Value>::new());
    let taken = poll(server.addr, a, "ours").await;
    assert_eq!(taken.len(), 1, "{taken:?}");
    assert_eq!(taken[0]["input"], "from acme");

    // The other tenant's worker cannot answer it, though it can guess the id.
    let id = taken[0]["invocation_id"].as_str().unwrap();
    let answer =
        |completed: &str| json!({"results": [{"invocation_id": id, "completed": completed}]});
    let (status, _) = http(
        server.addr,
        g,
        "POST",
        "/v1/workers/results",
        answer("forged"),
    )
    .await;
    assert_eq!(
        status, 200,
        "a result it may not give is one the queue does not need"
    );
    let (status, _) = http(
        server.addr,
        a,
        "POST",
        "/v1/workers/results",
        answer("real"),
    )
    .await;
    assert_eq!(status, 200);
    let (_, result) = http(
        server.addr,
        a,
        "GET",
        "/v1/runs/k/result?timeout_ms=5000",
        Value::Null,
    )
    .await;
    assert_eq!(result["output"], "real", "{result}");

    // A worker for all of them takes from each; naming one narrows it.
    http(
        server.addr,
        a,
        "POST",
        "/v1/runs",
        start("a2", "steps", json!("a")),
    )
    .await;
    http(
        server.addr,
        g,
        "POST",
        "/v1/runs",
        start("g2", "steps", json!("g")),
    )
    .await;
    let narrowed = poll(server.addr, (Some(pool.as_str()), Some("globex")), "pool").await;
    let inputs: Vec<&Value> = narrowed.iter().map(|i| &i["input"]).collect();
    assert_eq!(inputs, [&json!("g")]);
    // What the narrowed poll holds comes back only after its lease; the other is free for the pool.
    let wide = poll(server.addr, (Some(pool.as_str()), None), "pool").await;
    let inputs: Vec<&Value> = wide.iter().map(|i| &i["input"]).collect();
    assert_eq!(inputs, [&json!("a")]);
    let _ = std::fs::remove_dir_all(&server.dir);
}

/// What an endpoint serving `namespace`, or none, is pushed when each of three tenants starts a run.
async fn pushed(name: &str, namespace: Value) -> Vec<Value> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_addr = target.local_addr().unwrap();
    {
        let seen = Arc::clone(&seen);
        tokio::spawn(async move {
            let app = axum::Router::new().fallback(axum::routing::post(
                move |axum::Json(invocation): axum::Json<Value>| {
                    let seen = Arc::clone(&seen);
                    async move {
                        seen.lock().unwrap().push(invocation["input"].clone());
                        axum::Json(json!({"completed": "pushed"}))
                    }
                },
            ));
            axum::serve(target, app).await.unwrap();
        });
    }
    let mut endpoint = json!({"queue": "functions", "url": format!("http://{target_addr}/")});
    if !namespace.is_null() {
        endpoint["namespace"] = namespace;
    }
    let (server, secrets) = serve(
        name,
        json!({"queues": {"functions": ["work"]}, "push": [endpoint]}),
        &[
            ("acme", "acme"),
            ("globex", "globex"),
            ("default", "default"),
        ],
    )
    .await;
    for (token, input) in secrets.iter().zip(["acme", "globex", "default"]) {
        http(
            server.addr,
            (Some(token.as_str()), None),
            "POST",
            "/v1/runs",
            start("k", "steps", json!(input)),
        )
        .await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = std::fs::remove_dir_all(&server.dir);
    let mut seen = seen.lock().unwrap().clone();
    seen.sort_by_key(Value::to_string);
    seen
}

#[tokio::test(flavor = "multi_thread")]
async fn a_push_endpoint_gets_only_its_namespaces_steps() {
    assert_eq!(pushed("push-one", json!("acme")).await, [json!("acme")]);
    // An endpoint that names none gets no tenant's payloads.
    assert_eq!(pushed("push-none", Value::Null).await, [json!("default")]);
    assert_eq!(
        pushed("push-all", json!("*")).await,
        [json!("acme"), json!("default"), json!("globex")]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_namespace_has_its_own_quotas() {
    let (server, mut secrets) = serve(
        "quotas",
        json!({"limits": {"namespaces": {
            "slow": {"requests_per_second": 1, "burst": 1},
            "small": {"max_running_runs": 1},
        }}}),
        &[
            ("slow", "slow"),
            ("slow-2", "slow"),
            ("small", "small"),
            ("free", "free"),
        ],
    )
    .await;
    let free = secrets.pop().unwrap();
    let small = secrets.pop().unwrap();
    let slow_2 = secrets.pop().unwrap();
    let slow = secrets.pop().unwrap();

    // The rate is the namespace's, whichever of its tokens spends it; others are not affected.
    let list = |who: String| async move {
        http(
            server.addr,
            (Some(who.as_str()), None),
            "GET",
            "/v1/runs",
            Value::Null,
        )
        .await
        .0
    };
    assert_eq!(list(slow.clone()).await, 200);
    assert_eq!(list(slow_2.clone()).await, 429);
    assert_eq!(list(free.clone()).await, 200);
    assert_eq!(list(free.clone()).await, 200);

    // So is the cap on running runs.
    let starting = |who: String, key: &'static str| async move {
        http(
            server.addr,
            (Some(who.as_str()), None),
            "POST",
            "/v1/runs",
            start(key, "waits", json!(1)),
        )
        .await
        .0
    };
    assert_eq!(starting(small.clone(), "a").await, 200);
    let mut refused = 0;
    for _ in 0..100 {
        refused = starting(small.clone(), "b").await;
        if refused == 503 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(refused, 503);
    assert_eq!(starting(free.clone(), "a").await, 200);
    assert_eq!(starting(free.clone(), "b").await, 200);
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_callers_are_scoped_the_same_way_and_the_audit_says_where() {
    let (server, acme, globex) = two_tenants("grpc").await;
    let mut client = IngressClient::connect(format!("http://{}", server.addr))
        .await
        .unwrap();
    let request = |token: &str, namespace: Option<&str>, key: &str| {
        let mut request = tonic::Request::new(v1::StartRequest {
            key: key.to_owned(),
            workflow: "waits".to_owned(),
            input: b"1".to_vec(),
            ..Default::default()
        });
        let metadata = request.metadata_mut();
        metadata.insert("authorization", format!("Bearer {token}").parse().unwrap());
        if let Some(namespace) = namespace {
            metadata.insert("aqueduct-namespace", namespace.parse().unwrap());
        }
        request
    };
    let started = client
        .start(request(&acme, None, "k"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(started.run_id, "k");
    let refused = client
        .start(request(&acme, Some("globex"), "k"))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);
    client.start(request(&globex, None, "k")).await.unwrap();

    // Each action's namespace is in the log; the refusal says who tried.
    let audit = std::fs::read_to_string(server.dir.join("audit.log")).unwrap();
    assert!(audit.contains("acme: k"), "{audit}");
    assert!(audit.contains("globex: k"), "{audit}");
    let _ = std::fs::remove_dir_all(&server.dir);
}

/// A gRPC worker's request: its stream, with a token.
fn as_tenant<T>(token: &str, stream: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(stream);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

#[tokio::test(flavor = "multi_thread")]
async fn step_worker_sessions_are_scoped() {
    use aqueduct_protocol::v1::worker_message::Message;
    use aqueduct_protocol::v1::workers_client::WorkersClient;

    let (server, acme, globex) = two_tenants("step-sessions").await;
    let (a, g) = ((Some(acme.as_str()), None), (Some(globex.as_str()), None));
    http(
        server.addr,
        g,
        "POST",
        "/v1/runs",
        start("k", "steps", json!("g")),
    )
    .await;
    http(
        server.addr,
        a,
        "POST",
        "/v1/runs",
        start("k", "steps", json!("a")),
    )
    .await;

    let session = async |token: &str| {
        let mut client = WorkersClient::connect(format!("http://{}", server.addr))
            .await
            .unwrap();
        let (out, outbound) = tokio::sync::mpsc::channel(8);
        let hello = v1::Hello {
            worker: "w".to_owned(),
            steps: vec!["work".to_owned()],
            capacity: 5,
            queues: Vec::new(),
        };
        out.send(v1::WorkerMessage {
            message: Some(Message::Hello(hello)),
        })
        .await
        .unwrap();
        let stream = tokio_stream::wrappers::ReceiverStream::new(outbound);
        let inbound = client
            .work(as_tenant(token, stream))
            .await
            .unwrap()
            .into_inner();
        (out, inbound)
    };
    let (acme_out, mut acme_in) = session(&acme).await;
    let (globex_out, mut globex_in) = session(&globex).await;
    let next = async |inbound: &mut tonic::Streaming<v1::ServerMessage>| {
        let message = tokio::time::timeout(Duration::from_secs(5), inbound.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Some(v1::server_message::Message::Invocation(invocation)) = message.message else {
            panic!("not an invocation");
        };
        invocation
    };
    let theirs = next(&mut globex_in).await;
    assert_eq!(theirs.input, b"\"g\"");
    let ours = next(&mut acme_in).await;
    assert_eq!(ours.input, b"\"a\"");
    // Each has one step, its own: nothing else comes.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), acme_in.message())
            .await
            .is_err()
    );

    // A worker cannot answer another tenant's step over its stream.
    let answer = |id: &str, result: &str| v1::WorkerMessage {
        message: Some(Message::Result(v1::StepResult {
            invocation_id: id.to_owned(),
            outcome: Some(v1::step_result::Outcome::Completed(
                format!("\"{result}\"").into_bytes(),
            )),
        })),
    };
    acme_out
        .send(answer(&theirs.invocation_id, "forged"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    globex_out
        .send(answer(&theirs.invocation_id, "real"))
        .await
        .unwrap();
    let (_, result) = http(
        server.addr,
        g,
        "GET",
        "/v1/runs/k/result?timeout_ms=5000",
        Value::Null,
    )
    .await;
    assert_eq!(result["output"], "real", "{result}");
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn workflow_worker_sessions_are_scoped() {
    use aqueduct_protocol::v1::workflow_worker_message::Message;
    use aqueduct_protocol::v1::workflows_client::WorkflowsClient;

    let (server, mut secrets) = serve(
        "workflow-sessions",
        json!({"remote_workflows": ["remote"]}),
        &[("acme", "acme"), ("globex", "globex")],
    )
    .await;
    let globex = secrets.pop().unwrap();
    let acme = secrets.pop().unwrap();
    let (a, g) = ((Some(acme.as_str()), None), (Some(globex.as_str()), None));
    http(
        server.addr,
        g,
        "POST",
        "/v1/runs",
        start("k", "remote", json!("g")),
    )
    .await;
    http(
        server.addr,
        a,
        "POST",
        "/v1/runs",
        start("k", "remote", json!("a")),
    )
    .await;

    let session = async |token: &str| {
        let mut client = WorkflowsClient::connect(format!("http://{}", server.addr))
            .await
            .unwrap();
        let (out, outbound) = tokio::sync::mpsc::channel(8);
        let hello = v1::WorkflowHello {
            worker: "w".to_owned(),
            workflows: vec!["remote".to_owned()],
            capacity: 5,
        };
        out.send(v1::WorkflowWorkerMessage {
            message: Some(Message::Hello(hello)),
        })
        .await
        .unwrap();
        let stream = tokio_stream::wrappers::ReceiverStream::new(outbound);
        let inbound = client
            .serve(as_tenant(token, stream))
            .await
            .unwrap()
            .into_inner();
        (out, inbound)
    };
    let (acme_out, mut acme_in) = session(&acme).await;
    let (globex_out, mut globex_in) = session(&globex).await;
    let next = async |inbound: &mut tonic::Streaming<v1::WorkflowServerMessage>| {
        let message = tokio::time::timeout(Duration::from_secs(5), inbound.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Some(v1::workflow_server_message::Message::Activation(activation)) = message.message
        else {
            panic!("not an activation");
        };
        activation
    };
    let theirs = next(&mut globex_in).await;
    assert_eq!(theirs.run_id, "$ns/globex/k");
    let ours = next(&mut acme_in).await;
    assert_eq!(ours.run_id, "$ns/acme/k");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), acme_in.message())
            .await
            .is_err()
    );

    // A worker cannot decide for another tenant's run.
    let decide = |id: &str, output: &str| v1::WorkflowWorkerMessage {
        message: Some(Message::Decision(v1::Decision {
            activation_id: id.to_owned(),
            commands: vec![v1::WorkflowCommand {
                command: Some(v1::workflow_command::Command::CompleteRun(
                    v1::CompleteRun {
                        output: format!("\"{output}\"").into_bytes(),
                    },
                )),
            }],
        })),
    };
    acme_out
        .send(decide(&theirs.activation_id, "forged"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    globex_out
        .send(decide(&theirs.activation_id, "real"))
        .await
        .unwrap();
    let (_, result) = http(
        server.addr,
        g,
        "GET",
        "/v1/runs/k/result?timeout_ms=5000",
        Value::Null,
    )
    .await;
    assert_eq!(result["output"], "real", "{result}");
    let _ = std::fs::remove_dir_all(&server.dir);
}

#[test]
fn names_of_namespaces_are_checked() {
    let dir = std::env::temp_dir().join(format!("aqueduct-ns-names-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    assert!(create_token_in(&dir, "t", everything(), "a/b").is_err());
    assert!(create_token_in(&dir, "t", everything(), "$ns").is_err());
    let config: Config =
        serde_json::from_value(json!({"limits": {"namespaces": {"a/b": {}}}})).unwrap();
    assert!(config.check().is_err());
    let config: Config =
        serde_json::from_value(json!({"limits": {"namespaces": {"a": {"weight": 0}}}})).unwrap();
    assert!(config.check().is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn namespaced_runs_and_their_children_survive_a_restart() {
    const WAITING_PARENT: &str = "def run(x):\n    return call(\"waits\", x, key = \"shared\")\n";
    let dir = std::env::temp_dir().join(format!("aqueduct-ns-restart-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let open = || {
        Engine::builder(&dir)
            .workflow(
                "waits",
                Arc::new(Procedure::compile("w.star", WAITS).unwrap()),
            )
            .workflow(
                "parent",
                Arc::new(Procedure::compile("p.star", WAITING_PARENT).unwrap()),
            )
            .namespace("acme", Some(2), 1.0)
            .open()
            .unwrap()
    };
    let input = aqueduct_core::Payload::new(*b"1");
    {
        let engine = open();
        engine.start("$ns/acme/p", "parent", input.clone()).unwrap();
        // The child is in its parent's namespace, under the key the logic chose.
        for _ in 0..200 {
            if engine.describe("$ns/acme/shared").is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        engine.describe("$ns/acme/shared").unwrap();
    }

    let engine = open();
    // Its quota still counts them: both are running, and the cap is two.
    let refused = engine.start("$ns/acme/third", "waits", input.clone());
    assert!(
        matches!(
            refused,
            Err(aqueduct_engine::embedded::EngineError::Overloaded { .. })
        ),
        "{refused:?}"
    );
    engine.start("$ns/globex/third", "waits", input).unwrap();
    // And the parent still waits for its child in the same namespace.
    engine
        .signal(
            "$ns/acme/shared",
            "go",
            "",
            aqueduct_core::Payload::new(*b"7"),
        )
        .unwrap();
    let result = engine.wait("$ns/acme/p", Duration::from_secs(5)).unwrap();
    assert!(format!("{result:?}").contains("Completed"), "{result:?}");
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}
