//! Static checks (ADR 0037): recursion is refused, lints are reported, and the module is evaluated once.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use aqueduct_core::{Command, Decider, Payload, RunState, start};
use aqueduct_procedure::Procedure;

fn recursion(source: &str) -> String {
    Procedure::compile("p.star", source)
        .map(|_| String::new())
        .expect_err("recursion compiles")
        .message
}

#[test]
fn recursion_does_not_compile() {
    for (source, cycle) in [
        // Directly.
        (
            "def f(n):\n    return f(n - 1)\n\ndef run(x):\n    return f(x)\n",
            "`f` calls `f` (line 2)",
        ),
        // Through another function.
        (
            "def a(n):\n    return b(n)\n\ndef b(n):\n    return a(n)\n\ndef run(x):\n    return a(x)\n",
            "`a` calls `b` (line 2), calls `a` (line 5)",
        ),
        // `run` itself.
        (
            "def run(x):\n    return run(x)\n",
            "`run` calls `run` (line 2)",
        ),
        // A named lambda.
        (
            "f = lambda n: f(n)\n\ndef run(x):\n    return f(x)\n",
            "`f` calls `f` (line 1)",
        ),
    ] {
        let message = recursion(source);
        assert!(message.contains("recursion is not allowed"), "{message}");
        assert!(message.contains(cycle), "expected {cycle}: {message}");
    }
    // Through function values: a function that calls a parameter, given itself.
    for source in [
        "def apply(g, n):\n    return g(g, n)\n\ndef run(x):\n    return apply(apply, x)\n",
        "def f(n):\n    return TABLE['f'](n)\n\nTABLE = {'f': f}\n\ndef run(x):\n    return f(x)\n",
        "def run(x):\n    fs = []\n    fs.append(lambda: fs[0]())\n    return fs[0]()\n",
    ] {
        assert!(
            recursion(source).contains("recursion is not allowed"),
            "{source}"
        );
    }
}

#[test]
fn functions_used_as_values_without_recursion_compile() {
    for source in [
        // A key function that calls methods.
        "def key(name):\n    return name.lower()\n\ndef run(names):\n    return sorted(names, key = key)\n",
        // Nested helpers, and a lambda.
        "def run(x):\n    def double(n):\n        return n * 2\n    inc = lambda n: n + 1\n    return inc(double(x))\n",
        // Helpers called many times, and comprehensions.
        "def sq(n):\n    return n * n\n\ndef run(xs):\n    return [sq(x) for x in xs] + [sq(sq(x)) for x in xs]\n",
    ] {
        Procedure::compile("p.star", source).unwrap();
    }
}

fn lints(source: &str) -> Vec<String> {
    Procedure::compile("p.star", source)
        .unwrap()
        .lints()
        .iter()
        .map(ToString::to_string)
        .collect()
}

#[test]
fn lints_are_reported_with_their_lines() {
    let found = lints(
        r#"
def run(claim):
    approved = wait(signal("approved"))
    escalate("review", claim)
    step("notify", claim, fallible = True)
    wait(update("change"), timeout = 60)
    return approved
"#,
    );
    assert_eq!(found.len(), 5, "{found:#?}");
    for (lint, expected) in found.iter().zip([
        "line 3: wait(...) has no timeout",
        "line 4: the resolution of escalate(...) is not used",
        "line 5: the result of step(..., fallible = True) is not used",
        "line 6: the update wait(update(...)) receives is dropped",
        "line 6: the procedure waits for updates but never answers one",
    ]) {
        assert!(lint.starts_with(expected), "expected {expected}: {lint}");
    }
    assert!(found[0].ends_with("(unbounded-wait)"), "{}", found[0]);
}

#[test]
fn a_clean_procedure_has_no_lints_and_lints_can_be_silenced() {
    assert_eq!(
        lints(
            r#"
def run(order):
    request = wait(update("change"), timeout = days(1))
    if request != None:
        respond(request, "ok")
    r = step("notify", order, fallible = True)
    return escalate("review", r)
"#
        ),
        Vec::<String>::new()
    );
    assert_eq!(
        lints(
            r#"
def run(order):
    # The run waits for its approval as long as it takes.
    # starlark-lint-disable unbounded-wait
    return wait(signal("approved"))
"#
        ),
        Vec::<String>::new()
    );
}

#[test]
fn the_top_level_runs_once_and_is_frozen() {
    let procedure = Procedure::compile(
        "p.star",
        r#"
LIMITS = {"small": 10, "large": 100}
SIZES = sorted(LIMITS.keys())

def run(size):
    return {"limit": LIMITS[size], "sizes": SIZES}
"#,
    )
    .unwrap();
    let decide = |procedure: &Procedure, input: &str| {
        let journal = start("p", Payload::new(input.as_bytes().to_vec())).entries;
        procedure.decide(&RunState::from_journal(&journal).unwrap().view())
    };
    assert_eq!(
        decide(&procedure, "\"large\""),
        vec![Command::CompleteRun {
            output: Payload::new(br#"{"limit":100,"sizes":["large","small"]}"#.to_vec())
        }]
    );
    // Globals cannot be changed from `run`: what one activation does cannot leak into the next.
    let mutating = Procedure::compile(
        "m.star",
        "SEEN = []\n\ndef run(x):\n    SEEN.append(x)\n    return len(SEEN)\n",
    )
    .unwrap();
    let commands = decide(&mutating, "1");
    assert!(
        matches!(&commands[..], [Command::Park { reason }] if reason.contains("Immutable")),
        "{commands:?}"
    );
}
