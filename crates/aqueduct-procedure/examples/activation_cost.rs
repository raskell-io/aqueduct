//! Measures the cost of one activation (parse, evaluate, replay) for procedures of growing length.
//! Run with `cargo run --release -p aqueduct-procedure --example activation_cost`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Instant;

use aqueduct_core::{Decider, Event, Payload, RunState, StepOutcome, start};
use aqueduct_procedure::Procedure;

fn main() {
    let procedure = Procedure::compile(
        "chain.star",
        "def run(n):\n    x = 0\n    for i in range(n):\n        x = step('s', x)\n    return x\n",
    )
    .unwrap();
    println!("{:>8}  {:>14}", "steps", "µs/activation");
    for steps in [1u32, 10, 100, 1000] {
        // Build a journal where every step has completed, so the activation replays all of them.
        let mut journal = start("test", Payload::new(steps.to_string().into_bytes())).entries;
        let mut state = RunState::from_journal(&journal).unwrap();
        for _ in 0..steps {
            let decision = state.handle(Event::Decision(procedure.decide(&state.view())));
            for entry in &decision.entries {
                state.apply(entry).unwrap();
            }
            journal.extend(decision.entries);
            let cmd = aqueduct_core::CommandIndex(state.steps().len() as u32 - 1);
            let value = Payload::new(cmd.0.to_string().into_bytes());
            let result = state.handle(Event::StepResult {
                cmd,
                outcome: StepOutcome::Completed(value),
            });
            for entry in &result.entries {
                state.apply(entry).unwrap();
            }
        }
        let rounds = (20_000 / steps).max(20);
        let started = Instant::now();
        for _ in 0..rounds {
            std::hint::black_box(procedure.decide(&state.view()));
        }
        let micros = started.elapsed().as_secs_f64() * 1e6 / f64::from(rounds);
        println!("{steps:>8}  {micros:>14.1}");
    }
}
