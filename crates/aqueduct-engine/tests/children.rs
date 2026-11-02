//! Child runs (ADR 0034): a run starts another run and waits for its outcome, across restarts, with
//! cancellation passed on to the child.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aqueduct_core::{Payload, Status};
use aqueduct_engine::embedded::{Attempt, Engine};
use aqueduct_procedure::Procedure;

const SECOND: Duration = Duration::from_secs(10);

const ORDER: &str = r#"
def run(order):
    payment = call("payment", {"amount": order["amount"]})
    shipping = call("shipping", order["address"], fallible = True)
    return {"payment": payment, "shipping": shipping}
"#;

const PAYMENT: &str = r#"
def run(request):
    if request["amount"] > 100:
        approval = wait(signal("approve"))
        if not approval:
            fail("payment declined")
    return step("charge", request["amount"])
"#;

const SHIPPING: &str = r#"
def run(address):
    if address == "nowhere":
        fail("no such address")
    return "shipped to " + address
"#;

fn temp_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "aqueduct-children-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn procedure(name: &str, source: &str) -> Arc<Procedure> {
    Arc::new(Procedure::compile(&format!("{name}.star"), source).unwrap())
}

fn engine(dir: &PathBuf, extra: &[(&str, &str)]) -> Engine {
    let mut builder = Engine::builder(dir)
        .workflow("order", procedure("order", ORDER))
        .workflow("payment", procedure("payment", PAYMENT))
        .workflow("shipping", procedure("shipping", SHIPPING))
        .step("charge", |amount: &Payload| {
            Attempt::Completed(Payload::new(
                format!("\"charged {}\"", String::from_utf8_lossy(amount.as_bytes())).into_bytes(),
            ))
        });
    for (name, source) in extra {
        builder = builder.workflow(name, procedure(name, source));
    }
    builder.open().unwrap()
}

fn json(text: &str) -> Payload {
    Payload::new(text.as_bytes().to_vec())
}

fn value(payload: &Option<Payload>) -> serde_json::Value {
    serde_json::from_slice(payload.as_ref().unwrap().as_bytes()).unwrap()
}

#[test]
fn a_run_waits_for_its_children_and_continues_with_their_outcomes() {
    let dir = temp_dir();
    let engine = engine(&dir, &[]);
    engine
        .start(
            "order-1",
            "order",
            json(r#"{"amount": 40, "address": "nowhere"}"#),
        )
        .unwrap();
    let order = engine.wait("order-1", SECOND).unwrap();
    assert_eq!(order.status, Status::Completed, "{:?}", order.error);
    assert_eq!(
        value(&order.output),
        serde_json::json!({
            "payment": "charged 40",
            "shipping": {"ok": false, "error": "no such address"},
        })
    );
    // The children are runs of their own, under keys derived from the parent's: `<key>#<command>`.
    let payment = engine.wait("order-1#0", SECOND).unwrap();
    assert_eq!(value(&payment.output), serde_json::json!("charged 40"));
    let shipping = engine.wait("order-1#1", SECOND).unwrap();
    assert_eq!(shipping.status, Status::Failed);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_child_that_waits_keeps_its_parent_waiting_across_restarts() {
    let dir = temp_dir();
    {
        let engine = engine(&dir, &[]);
        engine
            .start(
                "order-2",
                "order",
                json(r#"{"amount": 500, "address": "Lisbon"}"#),
            )
            .unwrap();
        assert!(engine.wait("order-2", Duration::from_millis(200)).is_err());
    }
    // Restarted, the payment child still waits for its approval, and its parent for it.
    let engine = engine(&dir, &[]);
    engine
        .signal("order-2#0", "approve", "", json("true"))
        .unwrap();
    let order = engine.wait("order-2", SECOND).unwrap();
    assert_eq!(
        value(&order.output),
        serde_json::json!({
            "payment": "charged 500",
            "shipping": {"ok": true, "value": "shipped to Lisbon"},
        })
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_failed_child_fails_its_parent_unless_fallible() {
    let dir = temp_dir();
    let engine = engine(&dir, &[]);
    engine
        .start(
            "order-3",
            "order",
            json(r#"{"amount": 500, "address": "Lisbon"}"#),
        )
        .unwrap();
    engine
        .signal("order-3#0", "approve", "", json("false"))
        .unwrap();
    let order = engine.wait("order-3", SECOND).unwrap();
    assert_eq!(order.status, Status::Failed);
    assert_eq!(order.error.as_deref(), Some("payment declined"));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn cancelling_a_parent_cancels_the_child_it_waits_for() {
    let dir = temp_dir();
    let engine = engine(&dir, &[]);
    engine
        .start(
            "order-4",
            "order",
            json(r#"{"amount": 500, "address": "Lisbon"}"#),
        )
        .unwrap();
    engine.cancel("order-4", "customer left").unwrap();
    let order = engine.wait("order-4", SECOND).unwrap();
    assert_eq!(order.status, Status::Failed);
    assert_eq!(order.error.as_deref(), Some("cancelled: customer left"));
    let payment = engine.wait("order-4#0", SECOND).unwrap();
    assert_eq!(payment.status, Status::Failed);
    assert!(
        payment
            .error
            .as_deref()
            .unwrap()
            .contains("its parent run `order-4` was cancelled"),
        "{payment:?}"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn children_under_a_chosen_key_are_shared() {
    const REPORT: &str = r#"
def run(source):
    return call("ingest", source, key = "ingest:" + source)
"#;
    const INGEST: &str = r#"
def run(source):
    ready = wait(signal("ready"))
    return {"source": source, "records": ready}
"#;
    let dir = temp_dir();
    let engine = engine(&dir, &[("report", REPORT), ("ingest", INGEST)]);
    engine.start("report-a", "report", json("\"git\"")).unwrap();
    engine.start("report-b", "report", json("\"git\"")).unwrap();
    // One ingest run, which both reports wait for.
    engine
        .signal("ingest:git", "ready", "", json("12"))
        .unwrap();
    for key in ["report-a", "report-b"] {
        let report = engine.wait(key, SECOND).unwrap();
        assert_eq!(
            value(&report.output),
            serde_json::json!({"source": "git", "records": 12})
        );
    }
    assert_eq!(
        engine.wait("ingest:git", SECOND).unwrap().status,
        Status::Completed
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_child_that_cannot_start_fails_the_call() {
    const BROKEN: &str = r#"
def run(input):
    missing = call("no-such-workflow", input, fallible = True)
    clash = call("shipping", "Porto", key = "order-5", fallible = True)
    return {"missing": missing["ok"], "error": missing["error"], "clash": clash["ok"]}
"#;
    let dir = temp_dir();
    let engine = engine(&dir, &[("broken", BROKEN)]);
    // `order-5` exists as a run of another workflow: the child cannot have that key.
    engine
        .start(
            "order-5",
            "order",
            json(r#"{"amount": 1, "address": "Faro"}"#),
        )
        .unwrap();
    engine.wait("order-5", SECOND).unwrap();
    engine.start("broken-1", "broken", json("{}")).unwrap();
    let broken = engine.wait("broken-1", SECOND).unwrap();
    let output = value(&broken.output);
    assert_eq!(output["missing"], false);
    assert!(
        output["error"]
            .as_str()
            .unwrap()
            .contains("unknown workflow `no-such-workflow`"),
        "{output}"
    );
    assert_eq!(output["clash"], false);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_procedure_can_call_itself() {
    const COUNTDOWN: &str = r#"
def run(n):
    if n == 0:
        return 0
    return 1 + call("countdown", n - 1)
"#;
    let dir = temp_dir();
    let engine = engine(&dir, &[("countdown", COUNTDOWN)]);
    engine.start("count", "countdown", json("200")).unwrap();
    let count = engine.wait("count", SECOND).unwrap();
    assert_eq!(value(&count.output), serde_json::json!(200));
    std::fs::remove_dir_all(&dir).unwrap();
}
