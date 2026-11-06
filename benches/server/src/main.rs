//! End-to-end benchmark of a single server against BRIEF.md section 9's target of 10,000 run starts per
//! second over gRPC, and of what authentication, the audit log, limits and namespaces cost.
//!
//! `cargo run --release -p aqueduct-bench-server` (add `-- --quick` for a short run, or `-- <filter>` for
//! the sections whose names contain it). The server listens on a real socket, with the same `serve_with`
//! as `aqueduct server`, and the load generator is in the same process on its own runtime: they share the
//! machine's cores, so these numbers are lower than with clients on other machines. Every number says its
//! durability mode (ADR 0003). Each section checks what it acknowledged afterwards: a run acknowledged
//! but not in the engine fails the benchmark.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aqueduct_engine::Durability;
use aqueduct_engine::embedded::Engine;
use aqueduct_procedure::Procedure;
use aqueduct_protocol::v1;
use aqueduct_protocol::v1::ingress_client::IngressClient;
use aqueduct_protocol::v1::workers_client::WorkersClient;
use aqueduct_sdk::{WorkflowContext, WorkflowError, workflow};
use aqueduct_server::auth::create_token_in;
use aqueduct_server::{Config, Permission, Security};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::transport::Channel;

const WAITS: &str = "def run(x):\n    return wait(signal(\"go\"))\n";

async fn noop(_ctx: WorkflowContext, input: String) -> Result<String, WorkflowError> {
    Ok(input)
}

async fn three_steps(ctx: WorkflowContext, key: String) -> Result<String, WorkflowError> {
    let a: String = ctx.step("first", &key).await?;
    let b: String = ctx.step("second", &a).await?;
    ctx.step("third", &b).await
}

async fn one_step(ctx: WorkflowContext, key: String) -> Result<String, WorkflowError> {
    ctx.step("first", &key).await
}

/// How a server under test is set up.
#[derive(Clone)]
struct Setup {
    durability: Durability,
    /// Tokens (the first is the one requests use), with which the audit log is written.
    tokens: usize,
    /// The token's namespace, if tokens are on.
    token_namespace: &'static str,
    config: Value,
}

impl Setup {
    fn plain(durability: Durability) -> Setup {
        Setup {
            durability,
            tokens: 0,
            token_namespace: "*",
            config: json!({}),
        }
    }
}

struct Server {
    addr: SocketAddr,
    engine: Arc<Engine>,
    dir: PathBuf,
    token: Option<String>,
    runtime: Option<tokio::runtime::Runtime>,
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

static SERVERS: AtomicU64 = AtomicU64::new(0);

/// The server, on its own runtime, as `aqueduct server` runs it.
fn server(setup: &Setup) -> Server {
    let n = SERVERS.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("aqueduct-bench-server-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config: Config = serde_json::from_value(setup.config.clone()).unwrap();
    config.check().unwrap();
    let builder = Engine::builder(&dir)
        .workflow("noop", Arc::new(workflow(noop)))
        .workflow("three_steps", Arc::new(workflow(three_steps)))
        .workflow("one_step", Arc::new(workflow(one_step)))
        .workflow(
            "waits",
            Arc::new(Procedure::compile("waits.star", WAITS).unwrap()),
        )
        .remote_steps(Duration::from_secs(60))
        .durability(setup.durability);
    let engine = Arc::new(config.apply(builder).unwrap().open().unwrap());
    let everything: BTreeSet<Permission> = [
        Permission::Start,
        Permission::Read,
        Permission::Signal,
        Permission::Work,
    ]
    .into_iter()
    .collect();
    let mut token = None;
    for i in 0..setup.tokens {
        let secret = create_token_in(
            &dir,
            &format!("t{i}"),
            everything.clone(),
            setup.token_namespace,
        )
        .unwrap();
        token.get_or_insert(secret);
    }
    let security = if setup.tokens == 0 {
        Security::none()
    } else {
        Security::for_dir(&dir).unwrap()
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("server")
        .build()
        .unwrap();
    let (listener, addr) = runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, addr)
    });
    {
        let engine = Arc::clone(&engine);
        runtime.spawn(async move {
            aqueduct_server::serve_with(engine, listener, &config, security, std::future::pending())
                .await
        });
    }
    Server {
        addr,
        engine,
        dir,
        token,
        runtime: Some(runtime),
    }
}

/// What a load run saw.
#[derive(Default)]
struct Tally {
    latencies: Vec<Duration>,
    shed: u64,
    errors: u64,
    first_error: Option<String>,
}

impl Tally {
    fn merge(&mut self, other: Tally) {
        self.latencies.extend(other.latencies);
        self.shed += other.shed;
        self.errors += other.errors;
        self.first_error = self.first_error.take().or(other.first_error);
    }

    fn fail(&mut self, error: String) {
        self.errors += 1;
        self.first_error.get_or_insert(error);
    }
}

fn percentile(sorted: &[Duration], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let at = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[at].as_secs_f64() * 1000.0
}

fn row(label: &str, clients: usize, tally: &mut Tally, elapsed: Duration) {
    tally.latencies.sort_unstable();
    let l = &tally.latencies;
    #[allow(clippy::cast_precision_loss)]
    let rate = l.len() as f64 / elapsed.as_secs_f64();
    println!(
        "| {label} | {clients} | {rate:.0} | {:.2} | {:.2} | {:.2} | {} | {} |",
        percentile(l, 0.5),
        percentile(l, 0.99),
        percentile(l, 1.0),
        tally.shed,
        tally.errors,
    );
    if let Some(error) = &tally.first_error {
        println!("<!-- first error: {error} -->");
    }
}

const HEADER: &str = "| what | clients | per second | p50 ms | p99 ms | max ms | shed | errors |\n|---|---:|---:|---:|---:|---:|---:|---:|";

fn client_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("load")
        .build()
        .unwrap()
}

async fn channels(addr: SocketAddr, clients: usize) -> Vec<Channel> {
    let mut channels = Vec::new();
    for _ in 0..clients.clamp(1, 8) {
        channels.push(
            Channel::from_shared(format!("http://{addr}"))
                .unwrap()
                .connect()
                .await
                .unwrap(),
        );
    }
    channels
}

#[derive(Clone)]
struct Who {
    token: Option<String>,
    namespace: Option<&'static str>,
}

impl Who {
    fn of(server: &Server, namespace: Option<&'static str>) -> Who {
        Who {
            token: server.token.clone(),
            namespace,
        }
    }

    fn request<T>(&self, message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        if let Some(token) = &self.token {
            request
                .metadata_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
        }
        if let Some(namespace) = self.namespace {
            request
                .metadata_mut()
                .insert("aqueduct-namespace", namespace.parse().unwrap());
        }
        request
    }
}

/// `clients` closed-loop gRPC clients starting new runs of `workflow` for `seconds`.
fn grpc_starts(
    server: &Server,
    who: &Who,
    workflow: &str,
    clients: usize,
    seconds: f64,
) -> (Tally, Duration) {
    let runtime = client_runtime();
    runtime.block_on(async {
        let channels = channels(server.addr, clients).await;
        let stop = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();
        let begin = Instant::now();
        for c in 0..clients {
            let mut client = IngressClient::new(channels[c % channels.len()].clone());
            let (stop, who, workflow) = (Arc::clone(&stop), who.clone(), workflow.to_owned());
            tasks.push(tokio::spawn(async move {
                let mut tally = Tally::default();
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let request = who.request(v1::StartRequest {
                        key: format!("c{c}-{i}"),
                        workflow: workflow.clone(),
                        input: b"\"x\"".to_vec(),
                        ..Default::default()
                    });
                    i += 1;
                    let at = Instant::now();
                    match client.start(request).await {
                        Ok(_) => tally.latencies.push(at.elapsed()),
                        Err(status)
                            if matches!(
                                status.code(),
                                tonic::Code::Unavailable | tonic::Code::ResourceExhausted
                            ) =>
                        {
                            tally.shed += 1;
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        Err(status) => {
                            tally.fail(status.to_string());
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    }
                }
                tally
            }));
        }
        tokio::time::sleep(Duration::from_secs_f64(seconds)).await;
        stop.store(true, Ordering::Relaxed);
        let mut all = Tally::default();
        for task in tasks {
            all.merge(task.await.unwrap());
        }
        (all, begin.elapsed())
    })
}

/// Every acknowledged start must be a run in the engine, or the numbers mean nothing.
fn check_acknowledged(server: &Server, baseline: usize, tally: &Tally) {
    let runs = server.engine.runs().unwrap().len();
    assert!(
        runs >= baseline + tally.latencies.len(),
        "{} starts acknowledged, but the engine has {} new runs",
        tally.latencies.len(),
        runs - baseline
    );
}

/// A keep-alive HTTP/1.1 connection, without a client library's costs.
struct Http {
    stream: tokio::net::TcpStream,
    buffer: Vec<u8>,
}

impl Http {
    async fn connect(addr: SocketAddr) -> Http {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        Http {
            stream,
            buffer: Vec::new(),
        }
    }

    async fn post(
        &mut self,
        addr: SocketAddr,
        who: &Who,
        path: &str,
        body: &str,
    ) -> (u16, Vec<u8>) {
        let authorization = who
            .token
            .as_ref()
            .map_or(String::new(), |t| format!("Authorization: Bearer {t}\r\n"));
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {addr}\r\n{authorization}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        self.stream.write_all(request.as_bytes()).await.unwrap();
        loop {
            if let Some(end) = self.buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&self.buffer[..end]).to_lowercase();
                let status: u16 = head[9..12].parse().unwrap();
                let length: usize = head
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .map_or(0, |value| value.trim().parse().unwrap());
                let total = end + 4 + length;
                while self.buffer.len() < total {
                    assert!(self.stream.read_buf(&mut self.buffer).await.unwrap() > 0);
                }
                let body = self.buffer[end + 4..total].to_vec();
                self.buffer.drain(..total);
                return (status, body);
            }
            assert!(self.stream.read_buf(&mut self.buffer).await.unwrap() > 0);
        }
    }
}

fn http_starts(server: &Server, who: &Who, clients: usize, seconds: f64) -> (Tally, Duration) {
    let runtime = client_runtime();
    let addr = server.addr;
    runtime.block_on(async {
        let stop = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();
        let begin = Instant::now();
        for c in 0..clients {
            let (stop, who) = (Arc::clone(&stop), who.clone());
            tasks.push(tokio::spawn(async move {
                let mut http = Http::connect(addr).await;
                let mut tally = Tally::default();
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let body = format!(r#"{{"key":"c{c}-{i}","workflow":"noop","input":"x"}}"#);
                    i += 1;
                    let at = Instant::now();
                    match http.post(addr, &who, "/v1/runs", &body).await {
                        (200, _) => tally.latencies.push(at.elapsed()),
                        (429 | 503, _) => {
                            tally.shed += 1;
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        (status, text) => {
                            tally.fail(format!("{status}: {}", String::from_utf8_lossy(&text)));
                        }
                    }
                }
                tally
            }));
        }
        tokio::time::sleep(Duration::from_secs_f64(seconds)).await;
        stop.store(true, Ordering::Relaxed);
        let mut all = Tally::default();
        for task in tasks {
            all.merge(task.await.unwrap());
        }
        (all, begin.elapsed())
    })
}

/// `clients` closed-loop readers of one run.
fn grpc_reads(server: &Server, who: &Who, clients: usize, seconds: f64) -> (Tally, Duration) {
    let runtime = client_runtime();
    runtime.block_on(async {
        let channels = channels(server.addr, clients).await;
        let mut first = IngressClient::new(channels[0].clone());
        first
            .start(who.request(v1::StartRequest {
                key: "read-me".into(),
                workflow: "noop".into(),
                input: b"\"x\"".to_vec(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();
        let begin = Instant::now();
        for c in 0..clients {
            let mut client = IngressClient::new(channels[c % channels.len()].clone());
            let (stop, who) = (Arc::clone(&stop), who.clone());
            tasks.push(tokio::spawn(async move {
                let mut tally = Tally::default();
                while !stop.load(Ordering::Relaxed) {
                    let request = who.request(v1::DescribeRequest {
                        key: "read-me".into(),
                    });
                    let at = Instant::now();
                    match client.describe(request).await {
                        Ok(_) => tally.latencies.push(at.elapsed()),
                        Err(status) => tally.fail(status.to_string()),
                    }
                }
                tally
            }));
        }
        tokio::time::sleep(Duration::from_secs_f64(seconds)).await;
        stop.store(true, Ordering::Relaxed);
        let mut all = Tally::default();
        for task in tasks {
            all.merge(task.await.unwrap());
        }
        (all, begin.elapsed())
    })
}

/// Starts `count` runs that wait for a signal, directly on the engine.
fn park_runs(server: &Server, count: usize) {
    let threads = 256;
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let engine = Arc::clone(&server.engine);
            std::thread::spawn(move || {
                for i in 0..count / threads {
                    engine
                        .start(
                            &format!("parked-{t}-{i}"),
                            "waits",
                            aqueduct_core::Payload::new(*b"1"),
                        )
                        .unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
}

type FirstSteps = Arc<Mutex<HashMap<String, Instant>>>;

/// A step worker's gRPC session answering every step at once. Records start-to-first-step for runs in
/// `first`.
async fn step_worker(
    channel: Channel,
    who: Who,
    capacity: u32,
    first: FirstSteps,
    latencies: Arc<Mutex<Vec<Duration>>>,
    done: Arc<AtomicU64>,
) {
    use v1::worker_message::Message;
    let mut client = WorkersClient::new(channel);
    let (out, outbound) = tokio::sync::mpsc::channel(1024);
    out.send(v1::WorkerMessage {
        message: Some(Message::Hello(v1::Hello {
            worker: "bench".into(),
            steps: vec!["first".into(), "second".into(), "third".into()],
            capacity,
            queues: Vec::new(),
            deployment: String::new(),
        })),
    })
    .await
    .unwrap();
    let stream = tokio_stream::wrappers::ReceiverStream::new(outbound);
    let mut inbound = client.work(who.request(stream)).await.unwrap().into_inner();
    while let Ok(Some(message)) = inbound.message().await {
        let Some(v1::server_message::Message::Invocation(invocation)) = message.message else {
            continue;
        };
        if invocation.step == "first"
            && let Some(at) = first.lock().unwrap().remove(&invocation.run_id)
        {
            latencies.lock().unwrap().push(at.elapsed());
        }
        done.fetch_add(1, Ordering::Relaxed);
        let answer = v1::WorkerMessage {
            message: Some(Message::Result(v1::StepResult {
                invocation_id: invocation.invocation_id,
                outcome: Some(v1::step_result::Outcome::Completed(invocation.input)),
            })),
        };
        if out.send(answer).await.is_err() {
            break;
        }
    }
}

/// Closed-loop clients that start a three-step run and wait for its result, with steps on gRPC workers.
fn remote_steps(server: &Server, clients: usize, seconds: f64) {
    let who = Who::of(server, None);
    let runtime = client_runtime();
    let first: FirstSteps = Arc::new(Mutex::new(HashMap::new()));
    let first_latencies = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(AtomicU64::new(0));
    let (mut tally, elapsed) = runtime.block_on(async {
        let channels = channels(server.addr, clients).await;
        for channel in channels.iter().cycle().take(4) {
            tokio::spawn(step_worker(
                channel.clone(),
                who.clone(),
                256,
                Arc::clone(&first),
                Arc::clone(&first_latencies),
                Arc::clone(&done),
            ));
        }
        let stop = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();
        let begin = Instant::now();
        for c in 0..clients {
            let mut client = IngressClient::new(channels[c % channels.len()].clone());
            let (stop, who, first) = (Arc::clone(&stop), who.clone(), Arc::clone(&first));
            tasks.push(tokio::spawn(async move {
                let mut tally = Tally::default();
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let key = format!("c{c}-{i}");
                    i += 1;
                    let at = Instant::now();
                    first.lock().unwrap().insert(key.clone(), at);
                    let started = client
                        .start(who.request(v1::StartRequest {
                            key: key.clone(),
                            workflow: "three_steps".into(),
                            input: b"\"x\"".to_vec(),
                            ..Default::default()
                        }))
                        .await;
                    if let Err(status) = started {
                        tally.fail(status.to_string());
                        continue;
                    }
                    let waited = client
                        .wait(who.request(v1::WaitRequest {
                            key,
                            timeout_ms: 30_000,
                        }))
                        .await;
                    match waited {
                        Ok(_) => tally.latencies.push(at.elapsed()),
                        Err(status) => tally.fail(status.to_string()),
                    }
                }
                tally
            }));
        }
        tokio::time::sleep(Duration::from_secs_f64(seconds)).await;
        stop.store(true, Ordering::Relaxed);
        let mut all = Tally::default();
        for task in tasks {
            all.merge(task.await.unwrap());
        }
        (all, begin.elapsed())
    });
    row("3-step run, start to result", clients, &mut tally, elapsed);
    let mut firsts = first_latencies.lock().unwrap().clone();
    firsts.sort_unstable();
    #[allow(clippy::cast_precision_loss)]
    let rate = firsts.len() as f64 / elapsed.as_secs_f64();
    println!(
        "| start to first step at the worker | {clients} | {rate:.0} | {:.2} | {:.2} | {:.2} | | |",
        percentile(&firsts, 0.5),
        percentile(&firsts, 0.99),
        percentile(&firsts, 1.0),
    );
    runtime.shutdown_background();
}

/// Closed-loop clients of one-step runs, with the steps on four HTTP long-polling workers.
fn http_workers(server: &Server, clients: usize, seconds: f64) {
    let who = Who::of(server, None);
    let addr = server.addr;
    let runtime = client_runtime();
    let (mut tally, elapsed) = runtime.block_on(async {
        let stop = Arc::new(AtomicBool::new(false));
        for w in 0..4 {
            // The pollers keep going until the runtime stops: runs still in flight need them.
            let who = who.clone();
            tokio::spawn(async move {
                let mut http = Http::connect(addr).await;
                let mut answers = Http::connect(addr).await;
                let body = json!({
                    "worker": format!("poller-{w}"), "steps": ["first"], "max": 64, "wait_ms": 500,
                })
                .to_string();
                loop {
                    let (status, text) = http.post(addr, &who, "/v1/workers/poll", &body).await;
                    assert_eq!(status, 200);
                    let polled: Value = serde_json::from_slice(&text).unwrap();
                    let results: Vec<Value> = polled["invocations"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|i| json!({"invocation_id": i["invocation_id"], "completed": i["input"]}))
                        .collect();
                    if !results.is_empty() {
                        let body = json!({"results": results}).to_string();
                        answers.post(addr, &who, "/v1/workers/results", &body).await;
                    }
                }
            });
        }
        let channels = channels(addr, clients).await;
        let mut tasks = Vec::new();
        let begin = Instant::now();
        for c in 0..clients {
            let mut client = IngressClient::new(channels[c % channels.len()].clone());
            let (stop, who) = (Arc::clone(&stop), who.clone());
            tasks.push(tokio::spawn(async move {
                let mut tally = Tally::default();
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let key = format!("c{c}-{i}");
                    i += 1;
                    let at = Instant::now();
                    let started = client
                        .start(who.request(v1::StartRequest {
                            key: key.clone(),
                            workflow: "one_step".into(),
                            input: b"\"x\"".to_vec(),
                            ..Default::default()
                        }))
                        .await;
                    if let Err(status) = started {
                        tally.fail(status.to_string());
                        continue;
                    }
                    let waited = client
                        .wait(who.request(v1::WaitRequest {
                            key,
                            timeout_ms: 30_000,
                        }))
                        .await;
                    match waited {
                        Ok(_) => tally.latencies.push(at.elapsed()),
                        Err(status) => tally.fail(status.to_string()),
                    }
                }
                tally
            }));
        }
        tokio::time::sleep(Duration::from_secs_f64(seconds)).await;
        stop.store(true, Ordering::Relaxed);
        let mut all = Tally::default();
        for task in tasks {
            all.merge(task.await.unwrap());
        }
        (all, begin.elapsed())
    });
    row(
        "1-step run on HTTP pollers, start to result",
        clients,
        &mut tally,
        elapsed,
    );
    runtime.shutdown_background();
}

/// `count` one-step runs are queued, in `namespaces` namespaces, and then workers drain them.
fn drain(count: usize, namespaces: usize, durability: Durability) {
    let server = server(&Setup::plain(durability));
    let threads = 64;
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let engine = Arc::clone(&server.engine);
            std::thread::spawn(move || {
                for i in 0..count / threads {
                    let n = (t * count / threads + i) % namespaces;
                    let key = if namespaces == 1 {
                        format!("q-{t}-{i}")
                    } else {
                        format!("$ns/n{n}/q-{t}-{i}")
                    };
                    engine
                        .start(&key, "one_step", aqueduct_core::Payload::new(*b"\"x\""))
                        .unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    let total = (count / threads * threads) as u64;
    let who = Who::of(&server, None);
    let done = Arc::new(AtomicU64::new(0));
    let runtime = client_runtime();
    let begin = Instant::now();
    let handed_out = runtime.block_on(async {
        let channels = channels(server.addr, 8).await;
        for channel in channels.iter().take(4) {
            tokio::spawn(step_worker(
                channel.clone(),
                who.clone(),
                256,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(Vec::new())),
                Arc::clone(&done),
            ));
        }
        while done.load(Ordering::Relaxed) < total {
            assert!(
                begin.elapsed() < Duration::from_secs(300),
                "only {} of {total} steps after 300 s",
                done.load(Ordering::Relaxed)
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        begin.elapsed()
    });
    // Results are journaled after workers have them: the run is done when that is durable.
    while server
        .engine
        .runs()
        .unwrap()
        .iter()
        .any(|run| run.status == aqueduct_core::Status::Running)
    {
        std::thread::sleep(Duration::from_millis(20));
    }
    let completed = begin.elapsed();
    let finished = server
        .engine
        .runs()
        .unwrap()
        .iter()
        .filter(|run| run.status == aqueduct_core::Status::Completed)
        .count();
    assert_eq!(finished as u64, total, "not every run completed");
    runtime.shutdown_background();
    #[allow(clippy::cast_precision_loss)]
    let (out, done) = (
        total as f64 / handed_out.as_secs_f64(),
        total as f64 / completed.as_secs_f64(),
    );
    println!("| {total} | {namespaces} | {out:.0} | {done:.0} |");
}

fn cell(setup: &Setup, label: &str, clients: usize, seconds: f64, namespace: Option<&'static str>) {
    let server = server(setup);
    let who = Who::of(&server, namespace);
    let (mut tally, elapsed) = grpc_starts(&server, &who, "noop", clients, seconds);
    check_acknowledged(&server, 0, &tally);
    row(label, clients, &mut tally, elapsed);
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let quick = args.iter().any(|a| a == "--quick");
    let filter = args.iter().find(|a| !a.starts_with("--")).cloned();
    let seconds = if quick { 1.0 } else { 5.0 };
    let wanted = |name: &str| filter.as_deref().is_none_or(|f| name.contains(f));
    let fsync = Setup::plain(Durability::Fsync);
    let os = Setup::plain(Durability::Os);
    let secured = Setup {
        tokens: 1,
        ..fsync.clone()
    };

    if wanted("starts") {
        println!("\n## Run starts\n\n{HEADER}");
        for clients in [1, 16, 64, 256] {
            cell(&fsync, "gRPC, `Fsync`, no tokens", clients, seconds, None);
        }
        for clients in [64, 256] {
            cell(&os, "gRPC, `Os`, no tokens", clients, seconds, None);
        }
        for clients in [1, 16, 64, 256] {
            let server = server(&fsync);
            let who = Who::of(&server, None);
            let (mut tally, elapsed) = http_starts(&server, &who, clients, seconds);
            check_acknowledged(&server, 0, &tally);
            row(
                "HTTP/JSON, `Fsync`, no tokens",
                clients,
                &mut tally,
                elapsed,
            );
        }
    }

    if wanted("security") {
        println!("\n## Tokens and the audit log\n\n{HEADER}");
        for clients in [1, 16, 64, 256] {
            cell(
                &secured,
                "starts, tokens and audit, `Fsync`",
                clients,
                seconds,
                None,
            );
        }
        let os_secured = Setup {
            tokens: 1,
            ..os.clone()
        };
        for clients in [64, 256] {
            cell(
                &os_secured,
                "starts, tokens and audit, `Os`",
                clients,
                seconds,
                None,
            );
        }
        for (label, tokens, clients) in [
            ("reads, no tokens", 0, 64),
            ("reads, 1 token", 1, 64),
            ("reads, 1,000 tokens", 1000, 64),
        ] {
            let setup = Setup {
                tokens,
                ..fsync.clone()
            };
            let server = server(&setup);
            let who = Who::of(&server, None);
            let (mut tally, elapsed) = grpc_reads(&server, &who, clients, seconds);
            row(label, clients, &mut tally, elapsed);
        }
    }

    if wanted("namespaces") {
        println!("\n## Namespaces and running-run caps\n\n{HEADER}");
        cell(&fsync, "default namespace", 256, seconds, None);
        cell(
            &fsync,
            "named namespace, by header",
            256,
            seconds,
            Some("acme"),
        );
        let backlog: &[usize] = if quick { &[10_000] } else { &[10_000, 100_000] };
        let capped = Setup {
            config: json!({"limits": {"max_running_runs": 10_000_000}}),
            ..fsync.clone()
        };
        for (label, setup) in [("no cap", &fsync), ("a cap configured", &capped)] {
            cell(
                setup,
                &format!("{label}, nothing parked"),
                256,
                seconds,
                None,
            );
            for parked in backlog {
                let server = server(setup);
                let begin = server.engine.runs().unwrap().len();
                park_runs(&server, *parked);
                assert_eq!(
                    server.engine.runs().unwrap().len(),
                    begin + parked / 256 * 256
                );
                let who = Who::of(&server, None);
                let (mut tally, elapsed) = grpc_starts(&server, &who, "noop", 256, seconds);
                check_acknowledged(&server, begin + parked / 256 * 256, &tally);
                row(
                    &format!("{label}, {parked} parked"),
                    256,
                    &mut tally,
                    elapsed,
                );
            }
        }
    }

    if wanted("overload") {
        println!("\n## Overload (default limits: 256 concurrent requests)\n\n{HEADER}");
        for clients in [256, 1024, 4096] {
            cell(&fsync, "more clients than places", clients, seconds, None);
        }
        let limited = Setup {
            config: json!({"limits": {"max_concurrent_requests": 64}}),
            ..fsync.clone()
        };
        cell(
            &limited,
            "64 concurrent requests allowed",
            1024,
            seconds,
            None,
        );
    }

    if wanted("workers") {
        println!("\n## Steps on remote workers (`Fsync`)\n\n{HEADER}");
        for clients in [1, 16, 64, 256] {
            let server = server(&fsync);
            remote_steps(&server, clients, seconds);
        }
        for clients in [16, 64] {
            let server = server(&fsync);
            http_workers(&server, clients, seconds);
        }
    }

    if wanted("drain") {
        println!("\n## Draining a queued backlog (`Fsync`, 4 gRPC workers)\n");
        println!(
            "| steps | namespaces | handed to workers per second | runs completed per second |\n|---:|---:|---:|---:|"
        );
        let counts: &[usize] = if quick {
            &[2_000]
        } else {
            &[2_000, 20_000, 100_000]
        };
        for count in counts {
            for namespaces in [1, 8, 64] {
                drain(*count, namespaces, Durability::Fsync);
            }
        }
    }
}
