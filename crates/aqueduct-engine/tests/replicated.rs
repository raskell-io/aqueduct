//! A replicated engine (ADR 0055): three engines in one process, each a replica of one partition, their
//! Raft messages carried by a router that can cut a node off. Runs survive the leader's loss, a node that
//! does not lead says who does, and a node that comes back catches up.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use aqueduct_core::{Payload, Status, StepOutcome};
use aqueduct_engine::embedded::{Engine, EngineError, RaftInbox, Replication};
use aqueduct_procedure::Procedure;

const GREET: &str = r#"
def run(user):
    name = step("upper", user)
    return step("greet", name)
"#;

type Router = Arc<RwLock<BTreeMap<u64, RaftInbox>>>;

fn temp_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "aqueduct-replicated-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn text(payload: &Payload) -> String {
    serde_json::from_slice::<String>(payload.as_bytes()).unwrap()
}

fn open(dir: &PathBuf, id: u64, router: &Router) -> Engine {
    let routes = Arc::clone(router);
    let engine = Engine::builder(dir)
        .workflow(
            "greet",
            Arc::new(Procedure::compile("greet.star", GREET).unwrap()),
        )
        .step("upper", |user| {
            StepOutcome::Completed(Payload::new(
                format!("\"{}\"", text(user).to_uppercase()).into_bytes(),
            ))
        })
        .step("greet", |name| {
            StepOutcome::Completed(Payload::new(
                format!("\"hello {}\"", text(name)).into_bytes(),
            ))
        })
        .replicated(Replication {
            id,
            members: vec![1, 2, 3],
            transport: Arc::new(move |envelope| {
                if let Some(inbox) = routes.read().unwrap().get(&envelope.to) {
                    inbox.deliver(envelope);
                }
            }),
            tick: Duration::from_millis(10),
            config: aqueduct_raft::Config::default(),
            preferred_leader: None,
        })
        .open()
        .unwrap();
    router.write().unwrap().insert(id, engine.raft_inbox());
    engine
}

/// The node that leads, once one does.
fn leader(engines: &BTreeMap<u64, Engine>) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Some((id, _)) = engines.iter().find(|(_, engine)| engine.leads()) {
            return *id;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("no leader within 20 s");
}

/// Does something on the leader, as a client does: asked elsewhere, or of a leader that has just lost
/// its leadership, it is asked again of whoever leads now.
fn on_leader<T>(
    engines: &BTreeMap<u64, Engine>,
    what: impl Fn(&Engine) -> Result<T, EngineError>,
) -> T {
    for _ in 0..20 {
        match what(&engines[&leader(engines)]) {
            Err(EngineError::NotLeader(_)) => std::thread::sleep(Duration::from_millis(50)),
            other => return other.unwrap(),
        }
    }
    panic!("no leader kept its leadership long enough");
}

/// Starts a greeting and waits for it, on the leader.
fn greet(engines: &BTreeMap<u64, Engine>, user: &str) -> String {
    let key = format!("greet:{user}");
    let input = Payload::new(format!("\"{user}\"").into_bytes());
    on_leader(engines, |engine| engine.start(&key, "greet", input.clone()));
    finished(engines, &key)
}

/// A run's output, once it has finished, from the leader.
fn finished(engines: &BTreeMap<u64, Engine>, key: &str) -> String {
    let result = on_leader(engines, |engine| engine.wait(key, Duration::from_secs(20)));
    assert_eq!(result.status, Status::Completed);
    text(&result.output.unwrap())
}

#[test]
fn runs_survive_the_leader_and_a_returning_node_catches_up() {
    let router: Router = Arc::default();
    let dirs: BTreeMap<u64, PathBuf> = (1..=3).map(|id| (id, temp_dir())).collect();
    let mut engines: BTreeMap<u64, Engine> = dirs
        .iter()
        .map(|(id, dir)| (*id, open(dir, *id, &router)))
        .collect();

    assert_eq!(greet(&engines, "ada"), "hello ADA");
    let first = leader(&engines);
    // A node that does not lead takes no request, and says who does.
    let follower = *engines.keys().find(|id| **id != first).unwrap();
    match engines[&follower].start("greet:x", "greet", Payload::new(*b"\"x\"")) {
        Err(EngineError::NotLeader(hint)) => assert!(hint.is_none() || hint == Some(first)),
        other => panic!("a follower answered {other:?}"),
    }

    // The leader goes. Another takes over, with the run's end and new work.
    router.write().unwrap().remove(&first);
    drop(engines.remove(&first));
    assert_eq!(finished(&engines, "greet:ada"), "hello ADA");
    assert_eq!(greet(&engines, "grace"), "hello GRACE");
    let second = leader(&engines);

    // The first comes back from its disk; then the second goes. What the first missed, it has.
    engines.insert(first, open(&dirs[&first], first, &router));
    std::thread::sleep(Duration::from_millis(500));
    router.write().unwrap().remove(&second);
    drop(engines.remove(&second));
    assert_eq!(finished(&engines, "greet:grace"), "hello GRACE");
    assert_eq!(greet(&engines, "alan"), "hello ALAN");

    // Every machine stops, and all start again from their disks: nothing acknowledged is lost.
    drop(engines);
    router.write().unwrap().clear();
    let engines: BTreeMap<u64, Engine> = dirs
        .iter()
        .map(|(id, dir)| (*id, open(dir, *id, &router)))
        .collect();
    for (user, name) in [("ada", "ADA"), ("grace", "GRACE"), ("alan", "ALAN")] {
        assert_eq!(
            finished(&engines, &format!("greet:{user}")),
            format!("hello {name}")
        );
    }

    drop(engines);
    for dir in dirs.values() {
        let _ = std::fs::remove_dir_all(dir);
    }
}
