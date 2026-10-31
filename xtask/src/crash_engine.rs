//! `cargo xtask crash-engine`: kills the embedded engine with SIGKILL while runs are in flight, then
//! checks that every acknowledged run exists and ends exactly as a sequential, fault-free execution
//! says it should (BRIEF.md section 14, phase 1: a workflow that survives `kill -9`).
//!
//! The child (`crash-engine-child <dir> <seed> <first>`) opens the engine, which recovers and resumes
//! earlier runs, then starts runs `first, first + 1, ...` and prints `S <n>` after each start is durable.
//! Steps sleep briefly, so kills land in the middle of runs. After the kills, `crash-engine-child <dir>
//! drain <last>` reopens the engine, waits for runs `0..=last` and prints `R <n> <status> <output>`.

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use aqueduct_core::{Decider, Effect, Event, Payload, RunState, Status, StepOutcome, start};
use aqueduct_engine::embedded::{Attempt, Engine};
use aqueduct_procedure::Procedure;
use aqueduct_sim::Rng;

const PIPELINE: &str = r##"
def run(input):
    extracted = step("extract", input)
    a, b = parallel([["map_a", extracted], ["map_b", extracted]])
    merged = step("merge", a + "#" + b)
    return step("validate", merged)
"##;

const STEPS: [&str; 5] = ["extract", "map_a", "map_b", "merge", "validate"];

fn procedure() -> Result<Arc<Procedure>, String> {
    Ok(Arc::new(
        Procedure::compile("pipeline.star", PIPELINE).map_err(|e| e.to_string())?,
    ))
}

/// The input of run `n`: a JSON string of 0 to 5 letters.
fn input(n: u64) -> Payload {
    let mut rng = Rng::new(n ^ 0x5EED);
    let len = rng.below(6);
    let text: String = (0..len)
        .map(|_| char::from(b'a' + rng.below(26) as u8))
        .collect();
    Payload::new(format!("\"{text}\"").into_bytes())
}

/// The step bodies: the same rules as the simulator's workload, on JSON strings.
fn execute(name: &str, input: &Payload) -> StepOutcome {
    let Ok(text) = std::str::from_utf8(input.as_bytes()) else {
        return StepOutcome::Failed("input is not UTF-8".into());
    };
    let text = text.trim_matches('"');
    match name {
        "extract" if text.is_empty() => StepOutcome::Failed("nothing to extract".into()),
        "validate" if text.bytes().map(u32::from).sum::<u32>() % 5 == 0 => {
            StepOutcome::Failed("validation failed".into())
        }
        "validate" => StepOutcome::Completed(input.clone()),
        _ => StepOutcome::Completed(Payload::new(format!("\"{text}|{name}\"").into_bytes())),
    }
}

/// How run `n` must end, by sequential execution without faults.
fn reference(decider: &dyn Decider, n: u64) -> Result<String, String> {
    let first = start("pipeline", input(n));
    let mut state = RunState::from_journal(&first.entries).map_err(|e| e.to_string())?;
    let mut work = first.effects;
    while let Some(effect) = work.pop() {
        let event = match effect {
            Effect::Activate => Event::Decision(decider.decide(&state.view())),
            Effect::DispatchStep { cmd, name, input } => Event::StepResult {
                cmd,
                outcome: execute(&name, &input),
            },
            Effect::Finished(_) => continue,
        };
        let transition = state.handle(event);
        for entry in &transition.entries {
            state.apply(entry).map_err(|e| e.to_string())?;
        }
        work.extend(transition.effects);
    }
    Ok(describe(state.status(), state.output(), state.error()))
}

fn describe(status: Status, output: Option<&Payload>, error: Option<&str>) -> String {
    match status {
        Status::Completed => format!(
            "completed {}",
            String::from_utf8_lossy(output.map_or(&[][..], |p| p.as_bytes()))
        ),
        other => format!("{other:?} {}", error.unwrap_or_default()).to_lowercase(),
    }
}

fn open(dir: &str, seed: u64) -> Result<Engine, String> {
    let mut builder = Engine::builder(PathBuf::from(dir))
        .workflow("pipeline", procedure()?)
        .segment_bytes(16 << 10)
        .checkpoint_every(64);
    for name in STEPS {
        let delay = Rng::new(seed ^ name.len() as u64).below(3);
        builder = builder.step(name, move |input| {
            std::thread::sleep(Duration::from_millis(delay));
            // The workload's failures depend only on the input, so they are terminal.
            match execute(name, input) {
                StepOutcome::Completed(result) => Attempt::Completed(result),
                StepOutcome::Failed(error) => Attempt::Fail(error),
            }
        });
    }
    if std::env::var_os("AQUEDUCT_CRASH_BUG").is_some_and(|v| v == "ack-before-sync") {
        builder = builder.inject_ack_before_sync();
    }
    builder.open().map_err(|e| e.to_string())
}

pub fn run(args: &[String]) -> Result<(), String> {
    let mut kills = 50u64;
    let mut seed = 1u64;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        let mut value = || -> Result<u64, String> {
            iter.next()
                .ok_or(format!("{arg} needs a value"))?
                .parse()
                .map_err(|e| format!("{arg}: {e}"))
        };
        match arg.as_str() {
            "--kills" => kills = value()?,
            "--seed" => seed = value()?,
            other => {
                return Err(format!(
                    "unknown argument {other}; use --kills N [--seed S]"
                ));
            }
        }
    }
    let dir = std::env::temp_dir().join(format!("aqueduct-crash-engine-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let dir_arg = dir.display().to_string();
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let mut rng = Rng::new(seed);
    let started = Instant::now();
    let mut acknowledged: Option<u64> = None;

    for _ in 0..kills {
        let first = acknowledged.map_or(0, |a| a + 1).to_string();
        let mut child = Command::new(&exe)
            .args([
                "crash-engine-child",
                &dir_arg,
                &rng.next_u64().to_string(),
                &first,
            ])
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| format!("cannot start the engine: {e}"))?;
        let stdout = child.stdout.take().ok_or("no stdout")?;
        let (sender, receiver) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        std::thread::sleep(Duration::from_millis(30 + rng.below(150)));
        child.kill().map_err(|e| format!("kill failed: {e}"))?;
        child.wait().map_err(|e| e.to_string())?;
        for line in receiver.try_iter() {
            if let Some(n) = line.strip_prefix("S ") {
                let n: u64 = n
                    .trim()
                    .parse()
                    .map_err(|e| format!("bad line {line:?}: {e}"))?;
                acknowledged = Some(acknowledged.map_or(n, |a| a.max(n)));
            }
        }
    }

    let Some(last) = acknowledged else {
        return Err("no run was ever acknowledged; the engine did not get going".into());
    };
    let output = Command::new(&exe)
        .args(["crash-engine-child", &dir_arg, "drain", &last.to_string()])
        .output()
        .map_err(|e| format!("cannot start the final engine: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "the final engine failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let decider = procedure()?;
    let mut checked = 0u64;
    let mut completed = 0u64;
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Some(rest) = line.strip_prefix("R ") else {
            continue;
        };
        let (n, actual) = rest.split_once(' ').ok_or(format!("bad line {line:?}"))?;
        let n: u64 = n.parse().map_err(|e| format!("bad line {line:?}: {e}"))?;
        let expected = reference(decider.as_ref(), n)?;
        if actual != expected {
            return Err(format!(
                "run {n} ended as `{actual}`, the reference says `{expected}`"
            ));
        }
        checked += 1;
        completed += u64::from(actual.starts_with("completed"));
    }
    if checked != last + 1 {
        return Err(format!("{checked} runs checked, {} acknowledged", last + 1));
    }
    // Evidence that checkpoints trimmed the log: how many segments are left, and the oldest LSN kept.
    let segments = aqueduct_storage::FileLog::open(
        dir.join("log"),
        aqueduct_storage::FileLogOptions::default(),
    )
    .and_then(|log| log.segments())
    .map_err(|e| e.to_string())?;
    println!(
        "{kills} kills passed in {:.1} s: {checked} acknowledged runs all ended as the reference ({completed} completed, {} failed as intended); log trimmed to {} segments from LSN {}",
        started.elapsed().as_secs_f64(),
        checked - completed,
        segments.len(),
        segments.first().copied().unwrap_or(0)
    );
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

/// The engine process.
pub fn child(args: &[String]) -> Result<(), String> {
    match args {
        [dir, mode, last] if mode == "drain" => {
            let engine = open(dir, 0)?;
            let last: u64 = last.parse().map_err(|e| format!("last: {e}"))?;
            let mut out = std::io::stdout().lock();
            for n in 0..=last {
                let result = engine
                    .wait(&format!("run-{n}"), Duration::from_secs(60))
                    .map_err(|e| format!("run {n}: {e}"))?;
                let line = describe(
                    result.status,
                    result.output.as_ref(),
                    result.error.as_deref(),
                );
                writeln!(out, "R {n} {line}").map_err(|e| e.to_string())?;
            }
            Ok(())
        }
        [dir, seed, first] => {
            let seed: u64 = seed.parse().map_err(|e| format!("seed: {e}"))?;
            let engine = open(dir, seed)?;
            let mut n: u64 = first.parse().map_err(|e| format!("first: {e}"))?;
            let stdout = std::io::stdout();
            loop {
                engine
                    .start(&format!("run-{n}"), "pipeline", input(n))
                    .map_err(|e| e.to_string())?;
                let mut out = stdout.lock();
                writeln!(out, "S {n}").map_err(|e| e.to_string())?;
                out.flush().map_err(|e| e.to_string())?;
                n += 1;
            }
        }
        _ => Err("usage: crash-engine-child <dir> <seed> <first> | <dir> drain <last>".into()),
    }
}
