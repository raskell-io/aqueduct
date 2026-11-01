//! The `aqueduct` command line, working on an embedded engine's data directory.
//!
//! The engine must not be running: the log and the state store are locked by their owner, and
//! recovery may truncate a torn tail of the log. Every command returns its output as text, so it can be tested directly.

#![forbid(unsafe_code)]

use std::path::Path;

use aqueduct_core::{CommandKind, Decider, Entry, Event, Payload, RunState, StepOutcome};
use aqueduct_engine::{Deciders, Partition};
use aqueduct_procedure::Procedure;
use aqueduct_storage::{FileLog, FileLogOptions, RedbStateStore};

const USAGE: &str = "usage: aqueduct <command> <data-dir> [arguments]

commands:
  runs <dir>                          list runs: key, workflow, status, journal length
  describe <dir> <key>                a run's status, input, output and commands
  journal <dir> <key>                 a run's journal, one entry per line
  verify <dir>                        verify every run's hash chain and print its head
  export <dir> <key>                  a run's journal as JSON Lines (one object per entry)
  replay <dir> <key> <procedure.star> replay a run against a procedure and report divergence
  signal <dir> <key> <name> <json> [<id>]
                                      send a signal; with an id, sending it again records it once
  complete-awakeable <dir> <id> <json>
                                      complete an awakeable with a payload
  cancel <dir> <key> [<reason>]       ask a run to stop: what it waits for fails as cancelled
  escalations <dir>                   open escalations: id, reason, payload
  resolve <dir> <id> <json>           resolve an escalation; the run continues with the payload

Signals, completions, cancellations and resolutions are durable when the command returns; the run continues when the engine
next opens the directory.
";

/// Runs a command and returns its output.
pub fn run(args: &[String]) -> Result<String, String> {
    let arg = |i: usize, what: &str| {
        args.get(i)
            .ok_or_else(|| format!("missing {what}\n\n{USAGE}"))
    };
    match args.first().map(String::as_str) {
        None | Some("help" | "--help" | "-h") => Ok(USAGE.to_owned()),
        Some("runs") => runs(Path::new(arg(1, "data directory")?)),
        Some("describe") => describe(Path::new(arg(1, "data directory")?), arg(2, "run key")?),
        Some("journal") => journal(Path::new(arg(1, "data directory")?), arg(2, "run key")?),
        Some("verify") => verify(Path::new(arg(1, "data directory")?)),
        Some("export") => export(Path::new(arg(1, "data directory")?), arg(2, "run key")?),
        Some("replay") => replay(
            Path::new(arg(1, "data directory")?),
            arg(2, "run key")?,
            Path::new(arg(3, "procedure file")?),
        ),
        Some("signal") => signal(
            Path::new(arg(1, "data directory")?),
            arg(2, "run key")?,
            arg(3, "signal name")?,
            arg(4, "JSON payload")?,
            args.get(5).map_or("", String::as_str),
            false,
        ),
        Some("complete-awakeable") => {
            let id = arg(2, "awakeable id")?;
            let (run, name) = aqueduct_core::parse_awakeable_id(id)
                .ok_or_else(|| format!("`{id}` is not an awakeable id (<run key>#<command>)"))?;
            let key = aqueduct_core::split_run_id(&run).0.to_owned();
            signal(
                Path::new(arg(1, "data directory")?),
                &run,
                &name,
                arg(3, "JSON payload")?,
                id,
                true,
            )
            .map(|sent| {
                sent.replacen(
                    &format!("signal {name} sent to {key}"),
                    &format!("awakeable {id} completed"),
                    1,
                )
            })
        }
        Some("escalations") => {
            let partition = load(Path::new(arg(1, "data directory")?))?;
            let mut out = String::new();
            for (key, cmd, reason, payload) in partition.open_escalations() {
                out.push_str(&format!(
                    "{}  {reason}  {}\n",
                    aqueduct_core::awakeable_id(&key, cmd),
                    text(&payload)
                ));
            }
            Ok(out)
        }
        Some("resolve") => {
            let id = arg(2, "escalation id")?;
            let json = arg(3, "JSON resolution")?;
            let (key, cmd) = aqueduct_core::parse_command_id(id)
                .ok_or_else(|| format!("`{id}` is not an escalation id (<run key>#<command>)"))?;
            serde_json::from_str::<serde_json::Value>(json)
                .map_err(|e| format!("the resolution is not JSON ({e})"))?;
            let mut partition = load(Path::new(arg(1, "data directory")?))?;
            partition
                .resolve_escalation(&key, cmd, Payload::new(json.as_bytes().to_vec()))
                .map_err(|e| e.to_string())?;
            partition.flush().map_err(|e| e.to_string())?;
            Ok(format!("escalation {id} resolved\n"))
        }
        Some("cancel") => {
            let key = arg(2, "run key")?;
            let reason = args
                .get(3)
                .map_or("cancelled from the command line", String::as_str);
            let mut partition = load(Path::new(arg(1, "data directory")?))?;
            partition.cancel(key, reason).map_err(|e| e.to_string())?;
            partition.flush().map_err(|e| e.to_string())?;
            Ok(format!("cancellation of {key} recorded\n"))
        }
        Some(other) => Err(format!("unknown command `{other}`\n\n{USAGE}")),
    }
}

/// Records a signal durably. The payload must be JSON (ADR 0010).
/// Records a signal durably: to the key's latest run, or, if `exact`, to the run with run id `key`.
fn signal(
    dir: &Path,
    key: &str,
    name: &str,
    json: &str,
    id: &str,
    exact: bool,
) -> Result<String, String> {
    serde_json::from_str::<serde_json::Value>(json)
        .map_err(|e| format!("the payload is not JSON ({e}); quote strings, as in '\"yes\"'"))?;
    let mut partition = load(dir)?;
    let payload = Payload::new(json.as_bytes().to_vec());
    if exact {
        partition.signal_run(key, name, id, payload)
    } else {
        partition.signal(key, name, id, payload)
    }
    .map_err(|e| e.to_string())?;
    partition.flush().map_err(|e| e.to_string())?;
    let run = if exact {
        Some(key.to_owned())
    } else {
        partition.latest(key).map_err(|e| e.to_string())?
    };
    let state = run
        .and_then(|run| partition.runs().get(&run))
        .map(|run| format!("{:?}", run.state().status()).to_lowercase())
        .unwrap_or_else(|| "finished".to_owned());
    let key = aqueduct_core::split_run_id(key).0;
    Ok(format!("signal {name} sent to {key} ({state})\n"))
}

/// Loads every run in a data directory: the last checkpoint and the log after it.
fn load(dir: &Path) -> Result<Partition<FileLog, RedbStateStore>, String> {
    if !dir.join("log").is_dir() {
        return Err(format!(
            "{} is not an aqueduct data directory (no log)",
            dir.display()
        ));
    }
    let log =
        FileLog::open(dir.join("log"), FileLogOptions::default()).map_err(|e| e.to_string())?;
    let state = RedbStateStore::open(&dir.join("state.redb"))
        .map_err(|e| format!("cannot open the state store (is the engine running?): {e}"))?;
    Partition::load_with_state(log, state, Deciders::new()).map_err(|e| e.to_string())
}

fn run_state(dir: &Path, key: &str) -> Result<(RunState, Vec<Entry>), String> {
    let partition = load(dir)?;
    let run = partition
        .run(key)
        .ok_or_else(|| format!("no run `{key}`"))?;
    Ok((run.state().clone(), run.journal().to_vec()))
}

fn runs(dir: &Path) -> Result<String, String> {
    let partition = load(dir)?;
    let mut out = format!(
        "{:<32} {:<24} {:<10} {:>7}\n",
        "KEY", "WORKFLOW", "STATUS", "ENTRIES"
    );
    for (key, run) in partition.runs() {
        let state = run.state();
        out.push_str(&format!(
            "{:<32} {:<24} {:<10} {:>7}\n",
            key,
            state.workflow(),
            format!("{:?}", state.status()).to_lowercase(),
            run.journal().len()
        ));
    }
    Ok(out)
}

fn describe(dir: &Path, key: &str) -> Result<String, String> {
    let (state, _) = run_state(dir, key)?;
    let mut out = format!(
        "run       {key}\nworkflow  {}\nstatus    {}\ninput     {}\n",
        state.workflow(),
        format!("{:?}", state.status()).to_lowercase(),
        text(state.input())
    );
    if let Some(output) = state.output() {
        out.push_str(&format!("output    {}\n", text(output)));
    }
    if let Some(error) = state.error() {
        out.push_str(&format!("error     {error}\n"));
    }
    out.push_str("commands\n");
    for (index, step) in state.steps().iter().enumerate() {
        let kind = match step.kind {
            CommandKind::Step => format!("step {}", step.name),
            CommandKind::Sleep {
                duration_ms,
                wake_at,
            } => format!("sleep {duration_ms} ms (wakes at {wake_at})"),
            CommandKind::Signal { timeout_ms, .. } if timeout_ms > 0 => {
                format!("wait for signal {} (timeout {timeout_ms} ms)", step.name)
            }
            CommandKind::Signal { .. } => format!("wait for signal {}", step.name),
            CommandKind::Recorded => format!("record {}", step.name),
            CommandKind::Escalation => format!("escalation {}", step.name),
        };
        let outcome = match &step.outcome {
            None if step.retry_at.is_some() => {
                format!("retrying at {}", step.retry_at.unwrap_or_default())
            }
            None => "pending".to_owned(),
            Some(StepOutcome::Completed(result)) => format!("completed {}", text(result)),
            Some(StepOutcome::Failed(error)) => format!("failed: {error}"),
        };
        let attempts = if step.failed_attempts > 0 {
            format!(" after {} failed attempts", step.failed_attempts)
        } else {
            String::new()
        };
        out.push_str(&format!("  {index:>3}  {kind}: {outcome}{attempts}\n"));
    }
    Ok(out)
}

fn journal(dir: &Path, key: &str) -> Result<String, String> {
    let (_, journal) = run_state(dir, key)?;
    Ok(journal
        .iter()
        .enumerate()
        .map(|(i, entry)| format!("{i:>4}  {}\n", describe_entry(entry)))
        .collect())
}

fn verify(dir: &Path) -> Result<String, String> {
    // Loading verifies every chain; a broken one fails the load with the run and record.
    let partition = load(dir)?;
    let mut out = String::new();
    for (key, run) in partition.runs() {
        let mut chain = aqueduct_codec::ChainWriter::new(key);
        for entry in run.journal() {
            chain.append(entry);
        }
        out.push_str(&format!(
            "{key}  {}  {} entries\n",
            hex(&chain.head()),
            run.journal().len()
        ));
    }
    out.push_str(&format!("{} runs verified\n", partition.runs().len()));
    Ok(out)
}

fn export(dir: &Path, key: &str) -> Result<String, String> {
    let (_, journal) = run_state(dir, key)?;
    let mut out = String::new();
    for entry in &journal {
        out.push_str(&entry_json(entry).to_string());
        out.push('\n');
    }
    Ok(out)
}

/// Replays a run against a procedure: the procedure's commands at each point of the journal must match
/// the commands the journal recorded (ADR 0004). Reports the first divergence.
fn replay(dir: &Path, key: &str, procedure: &Path) -> Result<String, String> {
    let (_, journal) = run_state(dir, key)?;
    let source =
        std::fs::read_to_string(procedure).map_err(|e| format!("{}: {e}", procedure.display()))?;
    let name = procedure
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("procedure");
    let procedure = Procedure::compile(name, &source).map_err(|e| e.to_string())?;
    let mut state = RunState::from_journal(&journal[..1]).map_err(|e| e.to_string())?;
    let mut activations = 0;
    for (position, entry) in journal.iter().enumerate().skip(1) {
        // Before every entry the logic wrote, ask the procedure what it does now.
        if starts_commands(entry) {
            activations += 1;
            let transition = state.handle(Event::Decision(procedure.decide(&state.view())));
            if let Some(Entry::Parked { reason }) = transition.entries.first() {
                return Ok(format!("diverges at journal entry {position}: {reason}\n"));
            }
        }
        state
            .apply(entry)
            .map_err(|e| format!("journal entry {position}: {e}"))?;
    }
    let transition = state.handle(Event::Decision(procedure.decide(&state.view())));
    if let Some(Entry::Parked { reason }) = transition.entries.first() {
        return Ok(format!("diverges at the end of the journal: {reason}\n"));
    }
    Ok(format!(
        "replays without divergence: {} entries, {activations} activations checked\n",
        journal.len()
    ))
}

/// Entries the run's logic writes (commands and its end), as opposed to completions from outside.
fn starts_commands(entry: &Entry) -> bool {
    matches!(
        entry,
        Entry::StepScheduled { .. }
            | Entry::TimerStarted { .. }
            | Entry::SignalAwaited { .. }
            | Entry::RunCompleted { .. }
            | Entry::RunFailed { .. }
    )
}

fn describe_entry(entry: &Entry) -> String {
    match entry {
        Entry::Started { workflow, input } => format!("started {workflow} with {}", text(input)),
        Entry::StepScheduled { cmd, name, input } => {
            format!("[{cmd}] step {name} scheduled with {}", text(input))
        }
        Entry::StepCompleted { cmd, result } => format!("[{cmd}] completed with {}", text(result)),
        Entry::StepFailed { cmd, error } => format!("[{cmd}] failed: {error}"),
        Entry::StepRetryScheduled {
            cmd,
            attempt,
            error,
            wake_at,
        } => {
            format!("[{cmd}] attempt {attempt} failed ({error}); retry at {wake_at}")
        }
        Entry::TimerStarted {
            cmd,
            duration_ms,
            wake_at,
        } => format!("[{cmd}] sleep {duration_ms} ms until {wake_at}"),
        Entry::SignalAwaited {
            cmd,
            name,
            timeout_ms,
            wake_at,
        } if *timeout_ms > 0 => {
            format!("[{cmd}] waiting for signal {name} until {wake_at}")
        }
        Entry::SignalAwaited { cmd, name, .. } => format!("[{cmd}] waiting for signal {name}"),
        Entry::TimerFired { cmd } => format!("[{cmd}] timer fired"),
        Entry::ValueRecorded { cmd, name, value } => {
            format!("[{cmd}] recorded {name}: {}", text(value))
        }
        Entry::SignalReceived { name, id, payload } => {
            format!("signal {name} ({id}) received: {}", text(payload))
        }
        Entry::RunCompleted { output } => format!("run completed with {}", text(output)),
        Entry::ContinuedAsNew { input } => format!("continued as new with {}", text(input)),
        Entry::RunFailed { error } => format!("run failed: {error}"),
        Entry::Parked { reason } => format!("run parked: {reason}"),
        Entry::CancelRequested { reason } => format!("cancel requested: {reason}"),
        Entry::Escalated {
            cmd,
            reason,
            payload,
        } => format!("[{cmd}] escalated {reason}: {}", text(payload)),
        Entry::EscalationResolved { cmd, outcome } => {
            format!("[{cmd}] escalation resolved: {}", text(outcome))
        }
    }
}

/// A journal entry as a JSON object (BRIEF.md section 6.4: a plain interchange form). Payloads are
/// embedded as JSON when they are JSON (ADR 0010), and as base 16 strings otherwise.
fn entry_json(entry: &Entry) -> serde_json::Value {
    use serde_json::json;
    let cmd = |c: &aqueduct_core::CommandIndex| c.0;
    match entry {
        Entry::Started { workflow, input } => {
            json!({"kind": "Started", "workflow": workflow, "input": payload(input)})
        }
        Entry::StepScheduled {
            cmd: c,
            name,
            input,
        } => {
            json!({"kind": "StepScheduled", "cmd": cmd(c), "name": name, "input": payload(input)})
        }
        Entry::StepCompleted { cmd: c, result } => {
            json!({"kind": "StepCompleted", "cmd": cmd(c), "result": payload(result)})
        }
        Entry::StepFailed { cmd: c, error } => {
            json!({"kind": "StepFailed", "cmd": cmd(c), "error": error})
        }
        Entry::ValueRecorded {
            cmd: c,
            name,
            value,
        } => {
            json!({"kind": "ValueRecorded", "cmd": cmd(c), "name": name, "value": payload(value)})
        }
        Entry::StepRetryScheduled {
            cmd: c,
            attempt,
            error,
            wake_at,
        } => json!({
            "kind": "StepRetryScheduled", "cmd": cmd(c), "attempt": attempt, "error": error, "wake_at": wake_at
        }),
        Entry::TimerStarted {
            cmd: c,
            duration_ms,
            wake_at,
        } => {
            json!({"kind": "TimerStarted", "cmd": cmd(c), "duration_ms": duration_ms, "wake_at": wake_at})
        }
        Entry::SignalAwaited {
            cmd: c,
            name,
            timeout_ms,
            wake_at,
        } => json!({
            "kind": "SignalAwaited", "cmd": cmd(c), "name": name, "timeout_ms": timeout_ms, "wake_at": wake_at
        }),
        Entry::TimerFired { cmd: c } => json!({"kind": "TimerFired", "cmd": cmd(c)}),
        Entry::SignalReceived {
            name,
            id,
            payload: p,
        } => {
            json!({"kind": "SignalReceived", "name": name, "id": id, "payload": payload(p)})
        }
        Entry::RunCompleted { output } => {
            json!({"kind": "RunCompleted", "output": payload(output)})
        }
        Entry::ContinuedAsNew { input } => {
            json!({"kind": "ContinuedAsNew", "input": payload(input)})
        }
        Entry::RunFailed { error } => json!({"kind": "RunFailed", "error": error}),
        Entry::Parked { reason } => json!({"kind": "Parked", "reason": reason}),
        Entry::CancelRequested { reason } => json!({"kind": "CancelRequested", "reason": reason}),
        Entry::Escalated {
            cmd: c,
            reason,
            payload: p,
        } => json!({"kind": "Escalated", "cmd": cmd(c), "reason": reason, "payload": payload(p)}),
        Entry::EscalationResolved { cmd: c, outcome } => {
            json!({"kind": "EscalationResolved", "cmd": cmd(c), "outcome": payload(outcome)})
        }
    }
}

fn payload(payload: &Payload) -> serde_json::Value {
    serde_json::from_slice(payload.as_bytes())
        .unwrap_or_else(|_| serde_json::Value::String(hex(payload.as_bytes())))
}

fn text(payload: &Payload) -> String {
    match std::str::from_utf8(payload.as_bytes()) {
        Ok(text) if !text.is_empty() => text.to_owned(),
        Ok(_) => "(empty)".to_owned(),
        Err(_) => format!("0x{}", hex(payload.as_bytes())),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
