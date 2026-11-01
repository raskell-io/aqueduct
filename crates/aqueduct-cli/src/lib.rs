//! The `aqueduct` command line, working on an embedded engine's data directory.
//!
//! The engine must not be running: the log and the state store are locked by their owner, and
//! recovery may truncate a torn tail of the log. Every command returns its output as text, so it can be tested directly.

#![forbid(unsafe_code)]

use std::path::Path;

use aqueduct_ai::anthropic::Anthropic;
use aqueduct_ai::openai::OpenAiCompatible;
use aqueduct_ai::{AiExecutor, Model};
use aqueduct_core::{CommandKind, Decider, Entry, Event, Payload, RunState, StepOutcome};
use aqueduct_engine::counterfactual::{CounterfactualOptions, Outcome, replay_counterfactual};
use aqueduct_engine::drift::{
    DriftOptions, DriftReport, Recorded, Replayed, Verdict, agent_calls, replay_agent_calls,
};
use aqueduct_engine::{Deciders, Partition};
use aqueduct_procedure::Procedure;
use aqueduct_registry::{Action, Provenance, Registry, RegistryError, ReplayEvidence, State};
use aqueduct_storage::{FileLog, FileLogOptions, RedbStateStore};

const USAGE: &str = "usage: aqueduct <command> <data-dir> [arguments]

commands:
  runs <dir>                          list runs: key, workflow, status, journal length
  describe <dir> <key>                a run's status, input, output and commands
  journal <dir> <key>                 a run's journal, one entry per line
  verify <dir>                        verify every run's hash chain and print its head
  export <dir> <key>                  a run's journal as JSON Lines (one object per entry)
  replay <dir> <key> <procedure.star> replay a run against a procedure and report divergence
  counterfactual <dir> <procedure.star> (<key>... | --workflow <name>) [--stubs <file.json>]
                                      replay past runs against a new procedure version, answering it
                                      with their recorded results, and report where outcomes differ
  versions <subcommand> <dir> ...     procedure versions and their lifecycle (see below)
  drift <dir> --model <provider>/<model> --max-tokens <n> [<key>...] [options]
                                      ask past runs' agent calls again, live, with a model, and
                                      compare the answers with the recorded ones (model-drift replay)
  signal <dir> <key> <name> <json> [<id>]
                                      send a signal; with an id, sending it again records it once
  complete-awakeable <dir> <id> <json>
                                      complete an awakeable with a payload
  cancel <dir> <key> [<reason>]       ask a run to stop: what it waits for fails as cancelled
  escalations <dir>                   open escalations: id, reason, payload
  resolve <dir> <id> <json>           resolve an escalation; the run continues with the payload

versions (ADR 0030; every change names who makes it with --by):
  versions list <dir> [<workflow>]    versions: state, author, approvals, replay, runs pinned to them
  versions show <dir> <workflow> <version>
                                      a version's provenance, evidence, approvals and source
  versions register <dir> <workflow> <file.star> --by <name>
                    [--explored-by <agent>] [--cases <n>] [--note <text>]
  versions propose <dir> <workflow> <version> --by <name>
  versions prove <dir> <workflow> <version> --by <name> [--stubs <file.json>]
                                      replay the workflow's past runs against it and record the report
  versions approve <dir> <workflow> <version> --by <name> [--note <text>]
  versions activate <dir> <workflow> <version> --by <name>
                                      new runs start on it; the active version starts draining
  versions retire <dir> <workflow> <version> --by <name>
                                      only once no unfinished run is pinned to it
  versions diff <dir> <workflow> <version> <version>
  versions log <dir>                  every lifecycle event, verified
  A version can be given by a prefix of its id. Engines deploy the registry with Registry::deploy.

drift options:
  --model anthropic/<model>           the Claude API (ANTHROPIC_API_KEY)
  --model ollama/<model>              Ollama at http://localhost:11434/v1
  --model <provider>/<model>          any OpenAI-compatible endpoint (--base-url; key from <PROVIDER>_API_KEY)
  --base-url <url>                    the provider's base URL
  --max-tokens <n>                    the most tokens the replay may spend (required: calls cost money)
  --max-calls <n>                     the most calls to make
  --prompts <dir>                     read each call's prompt from <dir>/<prompt name> when that file exists
  --system <file>                     a system prompt for every call

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
        Some("counterfactual") => counterfactual(
            Path::new(arg(1, "data directory")?),
            Path::new(arg(2, "procedure file")?),
            &args[3..],
        ),
        Some("versions") => versions(&args[1..]),
        Some("drift") => drift(Path::new(arg(1, "data directory")?), &args[2..]),
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
    let mut out = format!("run       {key}\nworkflow  {}\n", state.workflow());
    if let Some(version) = state.version() {
        out.push_str(&format!("version   {version}\n"));
    }
    out.push_str(&format!(
        "status    {}\ninput     {}\n",
        format!("{:?}", state.status()).to_lowercase(),
        text(state.input())
    ));
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
            CommandKind::Agent { budget_tokens } => {
                format!("agent call {} (budget {budget_tokens} tokens)", step.name)
            }
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
    // A pinned run is decided by its own version only; checking another one is allowed, and said.
    let note = match state.version() {
        Some(pinned) if pinned != procedure.version() => format!(
            "note: the run is pinned to version {}; this procedure is version {}\n",
            short_version(pinned),
            short_version(&procedure.version())
        ),
        _ => String::new(),
    };
    let mut activations = 0;
    for (position, entry) in journal.iter().enumerate().skip(1) {
        // Before every entry the logic wrote, ask the procedure what it does now.
        if starts_commands(entry) {
            activations += 1;
            let transition = state.handle(Event::Decision(procedure.decide(&state.view())));
            if let Some(Entry::Parked { reason }) = transition.entries.first() {
                return Ok(format!(
                    "{note}diverges at journal entry {position}: {reason}\n"
                ));
            }
        }
        state
            .apply(entry)
            .map_err(|e| format!("journal entry {position}: {e}"))?;
    }
    let transition = state.handle(Event::Decision(procedure.decide(&state.view())));
    if let Some(Entry::Parked { reason }) = transition.entries.first() {
        return Ok(format!(
            "{note}diverges at the end of the journal: {reason}\n"
        ));
    }
    Ok(format!(
        "{note}replays without divergence: {} entries, {activations} activations checked\n",
        journal.len()
    ))
}

/// The version registry's commands (ADR 0030).
fn versions(args: &[String]) -> Result<String, String> {
    let sub = args.first().map(String::as_str).unwrap_or("");
    let mut positional: Vec<&str> = Vec::new();
    let mut flags: std::collections::BTreeMap<&str, &str> = std::collections::BTreeMap::new();
    let mut rest = args.iter().skip(1);
    while let Some(arg) = rest.next() {
        if let Some(flag) = arg.strip_prefix("--") {
            let value = rest
                .next()
                .ok_or_else(|| format!("--{flag} needs a value"))?;
            if !["by", "explored-by", "cases", "note", "stubs"].contains(&flag) {
                return Err(format!("unknown option `--{flag}`"));
            }
            flags.insert(flag, value);
        } else {
            positional.push(arg);
        }
    }
    let at = |i: usize, what: &str| {
        positional
            .get(i)
            .copied()
            .ok_or_else(|| format!("versions {sub}: missing {what}\n\n{USAGE}"))
    };
    let by = || {
        flags.get("by").copied().ok_or_else(|| {
            format!("versions {sub} needs --by <name>: every change says who made it")
        })
    };
    let dir = Path::new(at(0, "data directory")?);
    let mut registry = Registry::open(dir).map_err(|e| e.to_string())?;
    let err = |e: RegistryError| e.to_string();
    match sub {
        "list" => {
            let pinned = pinned_runs(dir);
            let mut out = format!(
                "{:<20} {:<12} {:<9} {:<16} {:<9} {:<18} {}\n",
                "WORKFLOW", "VERSION", "STATE", "AUTHOR", "APPROVED", "REPLAY", "RUNS"
            );
            for v in registry
                .versions()
                .filter(|v| positional.get(1).is_none_or(|w| v.workflow == *w))
            {
                let replay = v.replay.as_ref().map_or("-".to_owned(), |r| {
                    format!("{}/{}/{}", r.same, r.different, r.unresolved)
                });
                let runs = pinned.as_ref().map_or("?".to_owned(), |pinned| {
                    let (unfinished, total) = pinned.get(&v.id).copied().unwrap_or((0, 0));
                    format!("{unfinished} running of {total}")
                });
                out.push_str(&format!(
                    "{:<20} {:<12} {:<9} {:<16} {:<9} {:<18} {runs}\n",
                    v.workflow,
                    short_version(&v.id),
                    v.state.as_str(),
                    v.provenance.author,
                    v.approvals.len(),
                    replay,
                ));
            }
            Ok(out)
        }
        "show" => {
            let v = registry
                .version(at(1, "workflow")?, at(2, "version")?)
                .map_err(err)?;
            let mut out = format!(
                "workflow  {}\nversion   {}\nstate     {}\nauthor    {}\n",
                v.workflow,
                v.id,
                v.state.as_str(),
                v.provenance.author
            );
            if let Some(agent) = &v.provenance.explored_by {
                out.push_str(&format!("explored  by {agent}\n"));
            }
            if let Some(cases) = v.provenance.cases_studied {
                out.push_str(&format!("cases     {cases} studied\n"));
            }
            if let Some(note) = &v.provenance.note {
                out.push_str(&format!("note      {note}\n"));
            }
            if let Some(r) = &v.replay {
                out.push_str(&format!(
                    "replay    {} runs: {} same, {} different, {} unresolved (report sha256 {})\n",
                    r.runs, r.same, r.different, r.unresolved, r.report_sha256
                ));
            }
            for a in &v.approvals {
                out.push_str(&format!(
                    "approved  by {}{}\n",
                    a.by,
                    a.note.as_ref().map_or(String::new(), |n| format!(": {n}"))
                ));
            }
            out.push_str("source\n");
            for line in registry.source(v).map_err(err)?.lines() {
                out.push_str(&format!("  {line}\n"));
            }
            Ok(out)
        }
        "register" => {
            let workflow = at(1, "workflow")?;
            let file = Path::new(at(2, "procedure file")?);
            let source =
                std::fs::read_to_string(file).map_err(|e| format!("{}: {e}", file.display()))?;
            let cases = flags
                .get("cases")
                .map(|c| {
                    c.parse::<u64>()
                        .map_err(|_| format!("--cases needs a number, not `{c}`"))
                })
                .transpose()?;
            let provenance = Provenance {
                author: by()?.to_owned(),
                explored_by: flags.get("explored-by").map(|s| (*s).to_owned()),
                cases_studied: cases,
                note: flags.get("note").map(|s| (*s).to_owned()),
            };
            let id = registry
                .register(workflow, &source, provenance)
                .map_err(err)?;
            Ok(format!("registered {workflow} version {id} (draft)\n"))
        }
        "propose" => {
            let (workflow, id) = (at(1, "workflow")?, at(2, "version")?);
            registry.propose(workflow, id, by()?).map_err(err)?;
            Ok(format!("{workflow} version {id} proposed\n"))
        }
        "prove" => {
            let (workflow, id) = (at(1, "workflow")?, at(2, "version")?);
            let by = by()?;
            let version = registry.version(workflow, id).map_err(err)?.clone();
            let source = registry.source(&version).map_err(err)?;
            let procedure = Procedure::compile(&format!("{workflow}.star"), &source)
                .map_err(|e| e.to_string())?;
            let options = CounterfactualOptions {
                stubs: match flags.get("stubs") {
                    Some(path) => load_stubs(Path::new(path))?,
                    None => Default::default(),
                },
            };
            let partition = match load(dir) {
                Ok(partition) => Some(partition),
                // No log yet: nothing has run.
                Err(_) if !dir.join("log").is_dir() => None,
                Err(e) => return Err(e),
            };
            let (report, [same, different, unresolved]) = match &partition {
                Some(partition) => {
                    counterfactual_report(partition, &procedure, &[], Some(workflow), &options)?
                }
                None => (
                    "0 runs: 0 same, 0 different, 0 unresolved\n".to_owned(),
                    [0; 3],
                ),
            };
            // A workflow's first version has no past runs: its report says so (the policy's
            // `min_runs` decides whether that is enough).
            let runs = same + different + unresolved;
            let count = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
            let evidence = ReplayEvidence {
                runs: count(runs),
                same: count(same),
                different: count(different),
                unresolved: count(unresolved),
                report_sha256: aqueduct_procedure::version_of(&report),
            };
            registry.prove(workflow, id, by, evidence).map_err(err)?;
            Ok(format!(
                "{report}{workflow} version {} proven: the report is recorded\n",
                short_version(&version.id)
            ))
        }
        "approve" => {
            let (workflow, id) = (at(1, "workflow")?, at(2, "version")?);
            let note = flags.get("note").map(|s| (*s).to_owned());
            let state = registry.approve(workflow, id, by()?, note).map_err(err)?;
            Ok(match state {
                State::Approved => format!("{workflow} version {id} approved\n"),
                _ => format!(
                    "approval recorded; {workflow} version {id} needs more approvals ({} so far)\n",
                    registry.version(workflow, id).map_err(err)?.approvals.len()
                ),
            })
        }
        "activate" => {
            let (workflow, id) = (at(1, "workflow")?, at(2, "version")?);
            let previous = registry
                .active(workflow)
                .map(|v| short_version(&v.id).to_owned());
            registry.activate(workflow, id, by()?).map_err(err)?;
            let mut out = format!(
                "{workflow} version {id} is active: new runs start on it when the engine next opens\n"
            );
            if let Some(previous) = previous {
                out.push_str(&format!("{workflow} version {previous} is draining\n"));
            }
            Ok(out)
        }
        "retire" => {
            let (workflow, id) = (at(1, "workflow")?, at(2, "version")?);
            let version = registry.version(workflow, id).map_err(err)?.clone();
            let pinned = pinned_runs(dir)
                .ok_or("cannot read the data directory to check that no runs are left (is the engine running?)")?;
            let unfinished = pinned.get(&version.id).map_or(0, |(u, _)| *u);
            registry
                .retire(workflow, id, by()?, unfinished)
                .map_err(err)?;
            Ok(format!("{workflow} version {id} retired\n"))
        }
        "diff" => {
            let workflow = at(1, "workflow")?;
            let a = registry.version(workflow, at(2, "version")?).map_err(err)?;
            let b = registry.version(workflow, at(3, "version")?).map_err(err)?;
            Ok(format!(
                "--- {workflow} {}\n+++ {workflow} {}\n{}",
                short_version(&a.id),
                short_version(&b.id),
                aqueduct_registry::diff(
                    &registry.source(a).map_err(err)?,
                    &registry.source(b).map_err(err)?
                )
            ))
        }
        "log" => {
            let mut out = String::new();
            for e in registry.events() {
                let detail = match &e.action {
                    Action::Registered(p) => {
                        let mut detail = String::new();
                        if let Some(agent) = &p.explored_by {
                            detail.push_str(&format!(" explored by {agent}"));
                        }
                        if let Some(cases) = p.cases_studied {
                            detail.push_str(&format!(" from {cases} cases"));
                        }
                        detail
                    }
                    Action::Proven(r) => format!(
                        " {} runs: {} same, {} different, {} unresolved",
                        r.runs, r.same, r.different, r.unresolved
                    ),
                    Action::Approval { note: Some(note) } => format!(": {note}"),
                    _ => String::new(),
                };
                out.push_str(&format!(
                    "{:>4}  {}  {:<20} {:<12} {:<16} {}{detail}\n",
                    e.seq,
                    e.at_ms,
                    e.workflow,
                    short_version(&e.version),
                    e.by,
                    e.action.kind(),
                ));
            }
            out.push_str(&format!(
                "{} events, chain verified\n",
                registry.events().len()
            ));
            Ok(out)
        }
        "" => Err(format!("versions needs a subcommand\n\n{USAGE}")),
        other => Err(format!("unknown versions subcommand `{other}`\n\n{USAGE}")),
    }
}

/// Runs pinned to each version: `(unfinished, total)`, or `None` if the data directory cannot be read
/// (no log yet, or the engine is running).
fn pinned_runs(dir: &Path) -> Option<std::collections::BTreeMap<String, (usize, usize)>> {
    let partition = load(dir).ok()?;
    let mut pinned = std::collections::BTreeMap::new();
    for run in partition.runs().values() {
        if let Some(version) = run.state().version() {
            let entry: &mut (usize, usize) = pinned.entry(version.to_owned()).or_default();
            entry.1 += 1;
            if !run.state().status().is_terminal() {
                entry.0 += 1;
            }
        }
    }
    Some(pinned)
}

/// Counterfactual replay (ADR 0028): the chosen runs against a new procedure version.
fn counterfactual(dir: &Path, procedure: &Path, args: &[String]) -> Result<String, String> {
    let mut keys: Vec<&str> = Vec::new();
    let (mut workflow, mut stubs_file) = (None, None);
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--workflow" => workflow = Some(rest.next().ok_or("--workflow needs a name")?.as_str()),
            "--stubs" => stubs_file = Some(Path::new(rest.next().ok_or("--stubs needs a file")?)),
            flag if flag.starts_with("--") => return Err(format!("unknown option `{flag}`")),
            key => keys.push(key),
        }
    }
    if keys.is_empty() && workflow.is_none() {
        return Err("counterfactual needs run keys or --workflow <name>".into());
    }
    let options = CounterfactualOptions {
        stubs: match stubs_file {
            Some(path) => load_stubs(path)?,
            None => Default::default(),
        },
    };
    let source =
        std::fs::read_to_string(procedure).map_err(|e| format!("{}: {e}", procedure.display()))?;
    let name = procedure
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("procedure");
    let procedure = Procedure::compile(name, &source).map_err(|e| e.to_string())?;

    let partition = load(dir)?;
    for key in &keys {
        if !partition
            .runs()
            .keys()
            .any(|id| aqueduct_core::split_run_id(id).0 == *key)
        {
            return Err(format!("no run `{key}`"));
        }
    }
    Ok(counterfactual_report(&partition, &procedure, &keys, workflow, &options)?.0)
}

/// Stubs for counterfactual replay: a JSON object of results by step or agent call name.
fn load_stubs(path: &Path) -> Result<std::collections::BTreeMap<String, Payload>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let stubs: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&text).map_err(|e| {
            format!(
                "{}: stubs are a JSON object of results by step or agent call name ({e})",
                path.display()
            )
        })?;
    Ok(stubs
        .into_iter()
        .map(|(name, value)| (name, Payload::new(value.to_string().into_bytes())))
        .collect())
}

/// Replays the chosen runs (by key, or every run of `workflow`) against `procedure`; the report and the
/// counts of same, different and unresolved runs.
fn counterfactual_report(
    partition: &Partition<FileLog, RedbStateStore>,
    procedure: &Procedure,
    keys: &[&str],
    workflow: Option<&str>,
    options: &CounterfactualOptions,
) -> Result<(String, [usize; 3]), String> {
    let mut out = String::new();
    let mut counts = [0usize; 3];
    for (run_id, run) in partition.runs() {
        let key = aqueduct_core::split_run_id(run_id).0;
        let chosen = keys.contains(&key) || workflow.is_some_and(|w| run.state().workflow() == w);
        if !chosen {
            continue;
        }
        let replayed = replay_counterfactual(run_id, run.journal(), procedure, options)?;
        let verdict = replayed.verdict();
        counts[verdict as usize] += 1;
        out.push_str(&format!(
            "{}  {}  old: {}  new: {}\n",
            run_id.replace('\0', "@"),
            verdict.as_str(),
            outcome(&replayed.old),
            outcome(&replayed.new)
        ));
        if let Some(difference) = &replayed.first_difference {
            out.push_str(&format!(
                "  first difference at command {}: {} -> {}\n",
                difference.cmd.0,
                difference.old.as_deref().unwrap_or("(none)"),
                difference.new.as_deref().unwrap_or("(none)")
            ));
        }
        for unresolved in &replayed.unresolved {
            out.push_str(&format!(
                "  unresolved command {}: {}\n",
                unresolved.cmd.0, unresolved.what
            ));
        }
        if !replayed.stubbed.is_empty() {
            out.push_str(&format!("  stubbed: {}\n", replayed.stubbed.join(", ")));
        }
    }
    let [same, different, unresolved] = counts;
    out.push_str(&format!(
        "{} runs: {same} same, {different} different, {unresolved} unresolved\n",
        same + different + unresolved
    ));
    Ok((out, counts))
}

fn outcome(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Completed(output) => format!("completed {}", short(output)),
        Outcome::Failed(error) => format!("failed: {error}"),
        Outcome::Continued(input) => format!("continued as new with {}", short(input)),
        Outcome::Parked(reason) => format!("parked: {reason}"),
        Outcome::Waiting => "waiting".into(),
    }
}

/// Model-drift replay (ADR 0027): every recorded agent call of the chosen runs, asked again.
fn drift(dir: &Path, args: &[String]) -> Result<String, String> {
    let mut keys: Vec<&str> = Vec::new();
    let (mut model, mut base_url, mut prompts, mut system) = (None, None, None, None);
    let mut options = DriftOptions::default();
    let mut max_tokens = None;
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        let mut value = |flag: &str| {
            rest.next()
                .map(String::as_str)
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        let number = |flag: &str, text: &str| {
            text.parse::<u64>()
                .map_err(|_| format!("{flag} needs a number, not `{text}`"))
        };
        match arg.as_str() {
            "--model" => model = Some(value("--model")?),
            "--base-url" => base_url = Some(value("--base-url")?),
            "--prompts" => prompts = Some(Path::new(value("--prompts")?)),
            "--system" => system = Some(Path::new(value("--system")?)),
            "--max-tokens" => max_tokens = Some(number("--max-tokens", value("--max-tokens")?)?),
            "--max-calls" => {
                let calls = number("--max-calls", value("--max-calls")?)?;
                options.max_calls = Some(usize::try_from(calls).unwrap_or(usize::MAX));
            }
            flag if flag.starts_with("--") => return Err(format!("unknown option `{flag}`")),
            key => keys.push(key),
        }
    }
    let model = model.ok_or("drift needs --model <provider>/<model>")?;
    options.max_tokens = Some(max_tokens.ok_or(
        "drift needs --max-tokens: the calls are made live and cost tokens; this caps what the replay may spend",
    )?);

    let partition = load(dir)?;
    let mut calls = Vec::new();
    for (run_id, run) in partition.runs() {
        let key = aqueduct_core::split_run_id(run_id).0;
        if keys.is_empty() || keys.contains(&key) {
            calls.extend(agent_calls(run_id, run.journal()));
        }
    }
    for key in &keys {
        if !partition
            .runs()
            .keys()
            .any(|id| aqueduct_core::split_run_id(id).0 == *key)
        {
            return Err(format!("no run `{key}`"));
        }
    }

    let mut executor = AiExecutor::new(adapter(model, base_url)?);
    if let Some(path) = system {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        executor = executor.system(&text);
    }
    if let Some(dir) = prompts {
        for call in &calls {
            let request: serde_json::Value =
                serde_json::from_slice(call.request.as_bytes()).unwrap_or_default();
            let Some(name) = request.get("prompt").and_then(serde_json::Value::as_str) else {
                continue;
            };
            if let Ok(text) = std::fs::read_to_string(dir.join(name)) {
                executor = executor.prompt(name, &text);
            }
        }
    }
    let report = replay_agent_calls(calls, &executor, &options);
    Ok(drift_report(&report))
}

/// The model adapter for `<provider>/<model>`.
fn adapter(spec: &str, base_url: Option<&str>) -> Result<std::sync::Arc<dyn Model>, String> {
    let (provider, model) = spec
        .split_once('/')
        .filter(|(p, m)| !p.is_empty() && !m.is_empty())
        .ok_or_else(|| format!("`{spec}` is not <provider>/<model>"))?;
    if provider == "anthropic" {
        let mut claude = Anthropic::from_env(model).map_err(|e| e.to_string())?;
        if let Some(url) = base_url {
            claude = claude.base_url(url);
        }
        return Ok(std::sync::Arc::new(claude));
    }
    let url = match (provider, base_url) {
        (_, Some(url)) => url,
        ("ollama", None) => "http://localhost:11434/v1",
        (_, None) => return Err(format!("--model {spec} needs --base-url")),
    };
    let mut endpoint = OpenAiCompatible::new(url, model)
        .map_err(|e| e.to_string())?
        .provider(provider);
    let variable = format!("{}_API_KEY", provider.to_uppercase().replace('-', "_"));
    if let Ok(key) = std::env::var(&variable) {
        endpoint = endpoint.api_key(&key);
    }
    Ok(std::sync::Arc::new(endpoint))
}

fn drift_report(report: &DriftReport) -> String {
    let mut out = String::new();
    for drift in &report.calls {
        let call = &drift.call;
        let recorded_model = match &call.recorded {
            Recorded::Answer(response) => response.model.as_str(),
            Recorded::Supplied(_) => "(supplied)",
            Recorded::Failed(_) => "(failed)",
            Recorded::Pending => "(none)",
        };
        let detail = match &drift.replayed {
            Replayed::Answer {
                schema_error: Some(why),
                response,
            } => format!("{}: {why}", short(&response.response)),
            Replayed::Answer { response, .. } => match call.recorded.answer() {
                Some(recorded) if drift.verdict() == Verdict::Changed => format!(
                    "{} -> {} ({})",
                    short(recorded),
                    short(&response.response),
                    response.model
                ),
                _ => format!("{} ({})", short(&response.response), response.model),
            },
            Replayed::Failed(why) | Replayed::Skipped(why) => why.clone(),
        };
        // One line per call: multi-line errors (schema errors list every violation) are folded.
        let detail = detail
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        out.push_str(&format!(
            "{}  {}  {}  recorded by {recorded_model}  {detail}\n",
            aqueduct_core::awakeable_id(&call.run, call.cmd),
            call.name,
            drift.verdict().as_str(),
        ));
    }
    let counts: Vec<String> = [
        Verdict::Same,
        Verdict::Changed,
        Verdict::New,
        Verdict::Invalid,
        Verdict::Failed,
        Verdict::Skipped,
    ]
    .into_iter()
    .map(|v| format!("{} {}", report.count(v), v.as_str()))
    .collect();
    out.push_str(&format!(
        "{} agent calls: {}; {} tokens used\n",
        report.calls.len(),
        counts.join(", "),
        report.tokens_used()
    ));
    out
}

/// A version id as reports print it: the first twelve characters of a hash.
fn short_version(version: &str) -> &str {
    version.get(..12).unwrap_or(version)
}

/// A payload for a one-line report: its text, cut at 60 characters.
fn short(payload: &Payload) -> String {
    let text = text(payload);
    match text.char_indices().nth(60) {
        Some((end, _)) => format!("{}...", &text[..end]),
        None => text,
    }
}

/// Entries the run's logic writes (commands and its end), as opposed to completions from outside.
fn starts_commands(entry: &Entry) -> bool {
    matches!(
        entry,
        Entry::StepScheduled { .. }
            | Entry::AgentCallScheduled { .. }
            | Entry::AgentCallEscalated { .. }
            | Entry::TimerStarted { .. }
            | Entry::SignalAwaited { .. }
            | Entry::RunCompleted { .. }
            | Entry::RunFailed { .. }
    )
}

fn describe_entry(entry: &Entry) -> String {
    match entry {
        Entry::Started {
            workflow,
            input,
            budget_tokens,
            version,
        } => {
            let mut line = format!("started {workflow}");
            if let Some(version) = version {
                line.push_str(&format!(" version {}", short_version(version)));
            }
            line.push_str(&format!(" with {}", text(input)));
            if let Some(tokens) = budget_tokens {
                line.push_str(&format!(" (budget {tokens} tokens)"));
            }
            line
        }
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
        Entry::AgentCallEscalated {
            cmd,
            name,
            budget_tokens,
            available,
            ..
        } => format!(
            "[{cmd}] agent call {name} over budget: needs {budget_tokens} tokens, {available} available; escalated"
        ),
        Entry::AgentAnswerEscalated { cmd, error, answer } => {
            format!(
                "[{cmd}] answer rejected ({error}), escalated: {}",
                text(answer)
            )
        }
        Entry::BudgetApproved { cmd, tokens } => {
            format!("[{cmd}] budget raised to {tokens} tokens")
        }
        Entry::AgentCallScheduled {
            cmd,
            name,
            request,
            budget_tokens,
        } => format!(
            "[{cmd}] agent call {name} (budget {budget_tokens} tokens) with {}",
            text(request)
        ),
        Entry::AgentCallCompleted { cmd, response } => format!(
            "[{cmd}] {} answered with {} ({} + {} tokens)",
            response.model,
            text(&response.response),
            response.input_tokens,
            response.output_tokens
        ),
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
        Entry::Started {
            workflow,
            input,
            budget_tokens,
            version,
        } => {
            json!({"kind": "Started", "workflow": workflow, "version": version, "input": payload(input), "budget_tokens": budget_tokens})
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
        Entry::AgentCallEscalated {
            cmd: c,
            name,
            request,
            budget_tokens,
            available,
        } => json!({
            "kind": "AgentCallEscalated", "cmd": cmd(c), "name": name, "request": payload(request),
            "budget_tokens": budget_tokens, "available": available
        }),
        Entry::AgentAnswerEscalated {
            cmd: c,
            error,
            answer,
        } => {
            json!({"kind": "AgentAnswerEscalated", "cmd": cmd(c), "error": error, "answer": payload(answer)})
        }
        Entry::BudgetApproved { cmd: c, tokens } => {
            json!({"kind": "BudgetApproved", "cmd": cmd(c), "tokens": tokens})
        }
        Entry::AgentCallScheduled {
            cmd: c,
            name,
            request,
            budget_tokens,
        } => json!({
            "kind": "AgentCallScheduled", "cmd": cmd(c), "name": name,
            "request": payload(request), "budget_tokens": budget_tokens
        }),
        Entry::AgentCallCompleted { cmd: c, response } => json!({
            "kind": "AgentCallCompleted", "cmd": cmd(c), "model": response.model,
            "params": payload(&response.params), "response": payload(&response.response),
            "input_tokens": response.input_tokens, "output_tokens": response.output_tokens
        }),
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
