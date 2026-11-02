//! Queries and updates (ADR 0036): a run publishes state for queries, and answers updates, which are
//! validated before they are journaled, deduplicated by id, and survive crashes.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use aqueduct_core::{CommandIndex, Entry, Payload, Status, StepOutcome};
use aqueduct_engine::{Deciders, Outbound, Partition, UpdateStatus};
use aqueduct_procedure::Procedure;

mod common;
use common::MemLog;

/// An order whose address can be changed until it ships: an empty address is refused on receipt
/// (validation), an address the `check` step does not know is refused after the check.
const ORDER: &str = r#"
def run(order):
    address = order["address"]
    publish("address", address)
    for _ in range(10):
        request = wait(update("change"))
        change = request["value"]
        if change == "ship":
            respond(request, "shipping to " + address)
            break
        if change == "":
            reject(request, "an address cannot be empty")
            continue
        if not step("check", change):
            reject(request, "unknown address " + change)
            continue
        address = change
        publish("address", address)
        respond(request, {"address": address})
    return step("ship", address)
"#;

/// Waits a minute for a change, then goes on without one.
const PATIENT: &str = r#"
def run(input):
    request = wait(update("change"), timeout = 60)
    if request == None:
        return "no change"
    respond(request, "changed")
    return request["value"]
"#;

fn open(log: MemLog) -> Partition<MemLog> {
    let mut deciders = Deciders::new();
    for (name, source) in [("order", ORDER), ("patient", PATIENT)] {
        deciders.insert(
            name,
            Arc::new(Procedure::compile(&format!("{name}.star"), source).unwrap()),
        );
    }
    Partition::recover(log, deciders).unwrap()
}

fn json(text: &str) -> Payload {
    Payload::new(text.as_bytes().to_vec())
}

fn text(payload: &Payload) -> &str {
    std::str::from_utf8(payload.as_bytes()).unwrap()
}

/// Runs every dispatched step: `check` knows every address but Atlantis; `ship` returns its input.
fn work(partition: &mut Partition<MemLog>) {
    loop {
        let mut any = false;
        for message in partition.flush().unwrap() {
            if let Outbound::Dispatch {
                key,
                cmd,
                name,
                input,
            } = message
            {
                any = true;
                let result = match name.as_str() {
                    "check" if text(&input) == "\"Atlantis\"" => json("false"),
                    "check" => json("true"),
                    _ => input,
                };
                partition
                    .step_result(&key, cmd, StepOutcome::Completed(result))
                    .unwrap();
            }
        }
        if !any {
            return;
        }
    }
}

/// Sends an update and runs the work it causes; returns the answer.
fn update(partition: &mut Partition<MemLog>, id: &str, change: &str) -> Result<Payload, String> {
    let status = partition
        .update("order-1", "change", id, json(change))
        .unwrap();
    work(partition);
    match status {
        UpdateStatus::Answered(answer) => answer,
        UpdateStatus::Accepted { run, cmd } => partition
            .run(&run)
            .unwrap()
            .state()
            .response(cmd)
            .cloned()
            .expect("answered once its steps ran"),
    }
}

fn received(journal: &[Entry]) -> usize {
    journal
        .iter()
        .filter(|entry| matches!(entry, Entry::UpdateReceived { .. }))
        .count()
}

#[test]
fn a_run_answers_updates_and_publishes_its_state() {
    let mut partition = open(MemLog::default());
    partition
        .start("order-1", "order", json(r#"{"address": "Lisbon"}"#))
        .unwrap();
    work(&mut partition);
    let query = |partition: &mut Partition<MemLog>| partition.query("order-1", "address").unwrap();
    assert_eq!(query(&mut partition), Some(json("\"Lisbon\"")));
    assert_eq!(partition.query("order-1", "eta").unwrap(), None);

    assert_eq!(
        update(&mut partition, "", "\"Porto\""),
        Ok(json(r#"{"address":"Porto"}"#))
    );
    assert_eq!(query(&mut partition), Some(json("\"Porto\"")));

    // Refused on receipt: validation, which leaves no trace in the journal.
    let before = partition.run("order-1").unwrap().journal().len();
    assert_eq!(
        update(&mut partition, "", "\"\""),
        Err("an address cannot be empty".into())
    );
    assert_eq!(partition.run("order-1").unwrap().journal().len(), before);

    // Refused after a step: the update and its rejection are journaled.
    assert_eq!(
        update(&mut partition, "", "\"Atlantis\""),
        Err("unknown address Atlantis".into())
    );
    let journal = partition.run("order-1").unwrap().journal().to_vec();
    assert!(journal.iter().any(|entry| matches!(
        entry,
        Entry::UpdateRejected { error, .. } if error == "unknown address Atlantis"
    )));
    assert_eq!(query(&mut partition), Some(json("\"Porto\"")));

    // An update of a name the run does not wait for is refused.
    assert_eq!(
        partition
            .update("order-1", "cancel", "", json("null"))
            .unwrap(),
        UpdateStatus::Answered(Err(
            "run `order-1` is not waiting for update `cancel`".into()
        ))
    );

    assert_eq!(
        update(&mut partition, "", "\"ship\""),
        Ok(json("\"shipping to Porto\""))
    );
    let run = partition.run("order-1").unwrap();
    assert_eq!(run.state().status(), Status::Completed);
    assert_eq!(run.state().output(), Some(&json("\"Porto\"")));
    // Queries work on an ended run too: its journal replays to the end.
    assert_eq!(query(&mut partition), Some(json("\"Porto\"")));
    assert_eq!(
        partition
            .update("order-1", "change", "", json("\"Faro\""))
            .unwrap(),
        UpdateStatus::Answered(Err("run `order-1` has ended".into()))
    );

    // The determinism check replays the journal, updates and responses included.
    let procedure = Procedure::compile("order.star", ORDER).unwrap();
    aqueduct_engine::replay::check("order-1", &journal, &procedure).unwrap();
}

#[test]
fn an_update_sent_twice_with_an_id_is_received_once() {
    let mut partition = open(MemLog::default());
    partition
        .start("order-1", "order", json(r#"{"address": "Lisbon"}"#))
        .unwrap();
    work(&mut partition);
    let first = update(&mut partition, "u-1", "\"Porto\"");
    assert_eq!(update(&mut partition, "u-1", "\"Porto\""), first);
    // A retry gets the first answer even if the run has moved on.
    update(&mut partition, "u-2", "\"Faro\"").unwrap();
    assert_eq!(update(&mut partition, "u-1", "\"Porto\""), first);
    let journal = partition.run("order-1").unwrap().journal();
    assert_eq!(received(journal), 2);
}

#[test]
fn a_wait_for_an_update_times_out() {
    let mut partition = open(MemLog::default());
    partition.set_time(1_000);
    partition.start("p", "patient", json("null")).unwrap();
    partition.flush().unwrap();
    partition.set_time(60_999);
    partition.fire_due_timers().unwrap();
    assert_eq!(
        partition.run("p").unwrap().state().status(),
        Status::Running
    );
    partition.set_time(61_000);
    partition.fire_due_timers().unwrap();
    partition.flush().unwrap();
    let state = partition.run("p").unwrap().state();
    assert_eq!(state.status(), Status::Completed);
    assert_eq!(state.output(), Some(&json("\"no change\"")));
}

#[test]
fn logic_that_answers_wrongly_parks_the_run() {
    const TWICE: &str = r#"
def run(input):
    request = wait(update("change"))
    respond(request, 1)
    respond(request, 2)
    return 0
"#;
    const FORGED: &str = r#"
def run(input):
    step("prepare", input)
    respond({"update": 0, "value": None}, 1)
    return 0
"#;
    let mut deciders = Deciders::new();
    for (name, source) in [("twice", TWICE), ("forged", FORGED)] {
        deciders.insert(
            name,
            Arc::new(Procedure::compile(&format!("{name}.star"), source).unwrap()),
        );
    }
    let mut partition = Partition::recover(MemLog::default(), deciders).unwrap();
    partition.start("t", "twice", json("null")).unwrap();
    partition.update("t", "change", "", json("1")).unwrap();
    partition.start("f", "forged", json("null")).unwrap();
    for message in partition.flush().unwrap() {
        if let Outbound::Dispatch { key, cmd, .. } = message {
            partition
                .step_result(&key, cmd, StepOutcome::Completed(json("null")))
                .unwrap();
        }
    }
    partition.flush().unwrap();
    for key in ["t", "f"] {
        let state = partition.run(key).unwrap().state();
        assert_eq!(state.status(), Status::Parked, "{key}");
        assert!(
            state
                .error()
                .unwrap()
                .contains("which is not a received update it has not answered yet"),
            "{key}: {:?}",
            state.error()
        );
    }
}

#[test]
fn a_run_that_ends_while_an_update_waits_never_answers_it() {
    const FORGETFUL: &str = r#"
def run(input):
    wait(update("change"))
    return step("ship", input)
"#;
    let mut deciders = Deciders::new();
    deciders.insert(
        "forgetful",
        Arc::new(Procedure::compile("forgetful.star", FORGETFUL).unwrap()),
    );
    let mut partition = Partition::recover(MemLog::default(), deciders).unwrap();
    partition.start("f", "forgetful", json("1")).unwrap();
    let status = partition.update("f", "change", "", json("2")).unwrap();
    let UpdateStatus::Accepted { run, cmd } = status else {
        panic!("{status:?}");
    };
    assert_eq!(cmd, CommandIndex(0));
    for message in partition.flush().unwrap() {
        if let Outbound::Dispatch { key, cmd, .. } = message {
            partition
                .step_result(&key, cmd, StepOutcome::Completed(json("1")))
                .unwrap();
        }
    }
    partition.flush().unwrap();
    let state = partition.run(&run).unwrap().state();
    assert_eq!(state.status(), Status::Completed);
    assert_eq!(state.response(cmd), None);
}

/// Every prefix of the log of a run that received updates recovers to a valid state, from which the
/// same updates, sent again under their ids, complete the run with the same outcome.
#[test]
fn updates_survive_a_crash_at_every_point() {
    let changes = [
        ("u-1", "\"Porto\""),
        ("u-2", "\"\""),
        ("u-3", "\"Atlantis\""),
        ("u-4", "\"Faro\""),
        ("u-5", "\"ship\""),
    ];
    let mut partition = open(MemLog::default());
    partition
        .start("order-1", "order", json(r#"{"address": "Lisbon"}"#))
        .unwrap();
    work(&mut partition);
    let answers: Vec<_> = changes
        .iter()
        .map(|(id, change)| update(&mut partition, id, change))
        .collect();
    let expected = partition.run("order-1").unwrap().journal().to_vec();
    let log = partition.into_log();
    assert_eq!(
        expected.last(),
        Some(&Entry::RunCompleted {
            output: json("\"Faro\"")
        })
    );

    for cut in 0..=log.records.len() {
        let prefix = MemLog {
            records: log.records[..cut].to_vec(),
            pending: Vec::new(),
        };
        let mut partition = open(prefix);
        work(&mut partition);
        if cut == 0 {
            partition
                .start("order-1", "order", json(r#"{"address": "Lisbon"}"#))
                .unwrap();
            work(&mut partition);
        }
        let again: Vec<_> = changes
            .iter()
            .map(|(id, change)| update(&mut partition, id, change))
            .collect();
        let run = partition.run("order-1").unwrap();
        assert_eq!(run.state().status(), Status::Completed, "cut at {cut}");
        assert_eq!(run.journal(), expected.as_slice(), "cut at {cut}");
        // A retried update gets its first answer: from the journal, or, if the crash lost it, from the
        // logic again. The update refused on receipt left no trace, so a run that has ended by then
        // refuses it for that instead.
        for (i, (answer, first)) in again.iter().zip(&answers).enumerate() {
            if answer != first {
                assert_eq!(i, 1, "cut at {cut}");
                assert_eq!(
                    answer,
                    &Err("run `order-1` has ended".into()),
                    "cut at {cut}"
                );
            }
        }
    }
}
