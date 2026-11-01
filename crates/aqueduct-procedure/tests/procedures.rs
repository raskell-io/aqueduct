//! Tests for Starlark procedures as deciders.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use aqueduct_core::{Decider, Effect, Entry, Event, Payload, RunState, Status, StepOutcome, start};
use aqueduct_procedure::{Limits, Procedure};
use serde_json::{Value, json};

const CLAIMS: &str = include_str!("../examples/water_damage_photo_evidence.star");

/// Drives a run to its end (or until it waits for something only the outside can give, such as an
/// escalation's resolution), executing steps with `execute` in issue order.
fn drive(
    procedure: &Procedure,
    input: Value,
    execute: impl FnMut(&str, &Value) -> Result<Value, String>,
) -> (RunState, Vec<Entry>) {
    let first = start("test", Payload::new(input.to_string().into_bytes()));
    let mut journal = first.entries;
    let mut state = RunState::from_journal(&journal).unwrap();
    run(procedure, &mut state, &mut journal, first.effects, execute);
    (state, journal)
}

/// Handles `event`, then drives the run as [`drive`] does.
fn resume(
    procedure: &Procedure,
    state: &mut RunState,
    journal: &mut Vec<Entry>,
    event: Event,
    execute: impl FnMut(&str, &Value) -> Result<Value, String>,
) {
    let transition = state.handle(event);
    for entry in &transition.entries {
        state.apply(entry).unwrap();
    }
    journal.extend(transition.entries);
    run(procedure, state, journal, transition.effects, execute);
}

fn run(
    procedure: &Procedure,
    state: &mut RunState,
    journal: &mut Vec<Entry>,
    effects: Vec<Effect>,
    mut execute: impl FnMut(&str, &Value) -> Result<Value, String>,
) {
    let mut work = effects;
    work.reverse();
    while let Some(effect) = work.pop() {
        let event = match effect {
            Effect::Activate => {
                let commands = procedure.decide(&state.view());
                // Deterministic: the same journal gives the same commands.
                assert_eq!(commands, procedure.decide(&state.view()));
                Event::Decision(commands)
            }
            Effect::DispatchStep { cmd, name, input } => {
                let input: Value = serde_json::from_slice(input.as_bytes()).unwrap();
                let outcome = match execute(&name, &input) {
                    Ok(result) => {
                        StepOutcome::Completed(Payload::new(result.to_string().into_bytes()))
                    }
                    Err(error) => StepOutcome::Failed(error),
                };
                Event::StepResult { cmd, outcome }
            }
            Effect::Finished(_) | Effect::ContinueAsNew { .. } => continue,
        };
        let transition = state.handle(event);
        for entry in &transition.entries {
            state.apply(entry).unwrap();
        }
        journal.extend(transition.entries);
        work.extend(transition.effects.into_iter().rev());
    }
}

/// Resolves the run's only open escalation, which must have this reason, with `decision`.
fn resolve(state: &RunState, reason: &str, decision: &str) -> Event {
    let open = state.open_escalations();
    assert_eq!(open.len(), 1, "{open:?}");
    assert_eq!(open[0].1, reason);
    Event::ResolveEscalation {
        cmd: open[0].0,
        outcome: Payload::new(json!(decision).to_string().into_bytes()),
    }
}

fn claim(amount: u32) -> Value {
    json!({"id": "c-1", "customer": "cust-9", "amount": amount, "documents": ["d1", "d2"]})
}

fn claims_world(
    missing_photos: bool,
    photos_arrive: bool,
) -> impl FnMut(&str, &Value) -> Result<Value, String> {
    move |name, input| match name {
        "agent:classify_documents" => {
            assert_eq!(input["budget_tokens"], 20000);
            Ok(json!({"missing_photos": missing_photos, "confidence": 0.93}))
        }
        "coverage.water_damage@v7" => Ok(json!({"covered": true, "limit": 10000})),
        "notify.customer" => Ok(json!("sent")),
        "customer.photos" if photos_arrive => Ok(json!(["p1.jpg"])),
        "customer.photos" => Err("timed out".into()),
        "claims.settle@v2" => Ok(json!({"settled": input["claim"]})),
        other => panic!("unexpected step {other}"),
    }
}

fn step_names(journal: &[Entry]) -> Vec<String> {
    journal
        .iter()
        .filter_map(|e| match e {
            Entry::StepScheduled { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn claims_procedure_settles_a_simple_claim() {
    let procedure = Procedure::compile("claims.star", CLAIMS).unwrap();
    let (state, journal) = drive(&procedure, claim(1200), claims_world(false, true));
    assert_eq!(state.status(), Status::Completed);
    assert_eq!(
        state.output(),
        Some(&Payload::new(br#"{"settled":"c-1"}"#.to_vec()))
    );
    assert_eq!(
        step_names(&journal),
        [
            "agent:classify_documents",
            "coverage.water_damage@v7",
            "claims.settle@v2"
        ]
    );
}

#[test]
fn claims_procedure_asks_for_photos_and_escalates_without_them() {
    let procedure = Procedure::compile("claims.star", CLAIMS).unwrap();
    let (mut state, mut journal) = drive(&procedure, claim(1200), claims_world(true, false));
    // An escalation is not an error: the run waits for a person.
    assert_eq!(state.status(), Status::Running);
    assert_eq!(
        step_names(&journal),
        [
            "agent:classify_documents",
            "coverage.water_damage@v7",
            "notify.customer",
            "customer.photos"
        ]
    );
    let open = state.open_escalations();
    assert_eq!(
        serde_json::from_slice::<Value>(open[0].2.as_bytes()).unwrap(),
        claim(1200)
    );
    let event = resolve(&state, "no-photos", "settle");
    resume(
        &procedure,
        &mut state,
        &mut journal,
        event,
        claims_world(true, false),
    );
    assert_eq!(state.status(), Status::Completed);
    assert_eq!(step_names(&journal).last().unwrap(), "claims.settle@v2");
}

#[test]
fn claims_procedure_escalates_large_claims_and_a_person_can_decline() {
    let procedure = Procedure::compile("claims.star", CLAIMS).unwrap();
    let (mut state, mut journal) = drive(&procedure, claim(9000), claims_world(true, true));
    let event = resolve(&state, "manual-review", "suspected fraud");
    resume(
        &procedure,
        &mut state,
        &mut journal,
        event,
        claims_world(true, true),
    );
    assert_eq!(state.status(), Status::Failed);
    assert_eq!(state.error(), Some("declined: suspected fraud"));
    // A second resolution of the same escalation changes nothing.
    let again = state.handle(Event::ResolveEscalation {
        cmd: aqueduct_core::CommandIndex(4),
        outcome: Payload::new(*b"\"settle\""),
    });
    assert!(again.entries.is_empty());
}

#[test]
fn parallel_issues_every_command_before_waiting() {
    let procedure = Procedure::compile(
        "p.star",
        r#"
def run(x):
    a, b = parallel([["left", x], ["right", x]])
    return a + b
"#,
    )
    .unwrap();
    let journal = start("test", Payload::new(br#""x""#.to_vec())).entries;
    let state = RunState::from_journal(&journal).unwrap();
    let commands = procedure.decide(&state.view());
    assert_eq!(
        commands.len(),
        2,
        "both steps are issued at once: {commands:?}"
    );

    // Completion order does not matter: results come back in the order given.
    let mut order = 0;
    let (state, _) = drive(&procedure, json!("x"), |name, input| {
        order += 1;
        Ok(json!(format!("{}{}{order}", input.as_str().unwrap(), name)))
    });
    assert_eq!(state.status(), Status::Completed);
    assert_eq!(
        state.output(),
        Some(&Payload::new(br#""xleft1xright2""#.to_vec()))
    );
}

#[test]
fn fail_fails_the_run_and_bugs_park_it() {
    let failing = Procedure::compile("f.star", "def run(x):\n    fail('bad input', x)\n").unwrap();
    let (state, _) = drive(&failing, json!(1), |_, _| unreachable!());
    assert_eq!(state.status(), Status::Failed);
    assert!(state.error().unwrap().contains("bad input"));

    let buggy = Procedure::compile("b.star", "def run(x):\n    return x + 'text'\n").unwrap();
    let (state, _) = drive(&buggy, json!(1), |_, _| unreachable!());
    assert_eq!(state.status(), Status::Parked);
    assert!(
        state.error().unwrap().contains("b.star"),
        "{:?}",
        state.error()
    );
}

#[test]
fn a_failed_step_fails_the_run_unless_fallible() {
    let strict =
        Procedure::compile("s.star", "def run(x):\n    return step('flaky', x)\n").unwrap();
    let (state, _) = drive(&strict, json!(1), |_, _| Err("down".into()));
    assert_eq!(
        (state.status(), state.error()),
        (Status::Failed, Some("down"))
    );

    let tolerant = Procedure::compile(
        "t.star",
        "def run(x):\n    r = step('flaky', x, fallible = True)\n    return r['ok'] or r['error']\n",
    )
    .unwrap();
    let (state, _) = drive(&tolerant, json!(1), |_, _| Err("down".into()));
    assert_eq!(state.output(), Some(&Payload::new(br#""down""#.to_vec())));
}

#[test]
fn runaway_procedures_are_parked() {
    let limits = Limits {
        max_statements: 10_000,
        ..Limits::default()
    };
    let looping = Procedure::compile_with_limits(
        "loop.star",
        "def run(x):\n    n = 0\n    for i in range(1000000):\n        n += i\n    return n\n",
        limits,
    )
    .unwrap();
    let (state, _) = drive(&looping, json!(0), |_, _| unreachable!());
    assert_eq!(state.status(), Status::Parked);
    assert!(
        state.error().unwrap().contains("budget"),
        "{:?}",
        state.error()
    );

    // starlark-rust allows recursion; the call stack limit stops it.
    let recursive = Procedure::compile(
        "rec.star",
        "def f(n):\n    return f(n + 1)\n\ndef run(x):\n    return f(x)\n",
    )
    .unwrap();
    let (state, _) = drive(&recursive, json!(0), |_, _| unreachable!());
    assert_eq!(state.status(), Status::Parked);
}

#[test]
fn procedures_are_hermetic_and_checked_at_compile_time() {
    for (source, reason) in [
        (
            "load('x.star', 'y')\ndef run(x):\n    return x\n",
            "`load` is not allowed",
        ),
        ("def run(x):\n    return now()\n", "`now` not found"),
        ("def run(x):\n    return random()\n", "`random` not found"),
        ("def setup():\n    pass\n", "does not define `run(input)`"),
        ("run = 1\n", "`run` is not a function"),
        (
            "X = step('top-level', 1)\ndef run(x):\n    return x\n",
            "only be called inside `run`",
        ),
        ("def run(x):\n    return x +\n", "Parse error"),
    ] {
        let error = Procedure::compile("bad.star", source).expect_err(reason);
        assert!(
            error.message.contains(reason),
            "expected `{reason}`, got: {}",
            error.message
        );
    }
}

#[test]
fn changing_a_procedure_under_a_running_run_parks_it() {
    // Version 1 scheduled `a`; version 2 schedules `b` first: replay detects the divergence.
    let v1 = Procedure::compile("v1.star", "def run(x):\n    return step('a', x)\n").unwrap();
    let v2 = Procedure::compile("v2.star", "def run(x):\n    return step('b', x)\n").unwrap();
    let journal = start("test", Payload::new(b"1".to_vec())).entries;
    let mut state = RunState::from_journal(&journal).unwrap();
    for entry in state
        .handle(Event::Decision(v1.decide(&state.view())))
        .entries
    {
        state.apply(&entry).unwrap();
    }
    let transition = state.handle(Event::Decision(v2.decide(&state.view())));
    assert!(matches!(transition.entries[..], [Entry::Parked { .. }]));
}
