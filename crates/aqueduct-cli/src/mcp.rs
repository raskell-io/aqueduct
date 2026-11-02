//! An MCP server over a data directory (BRIEF.md section 13, phase 2; ADR 0032): agents propose,
//! validate and replay procedure versions, start and inspect runs, and handle escalations.
//!
//! It speaks the Model Context Protocol over stdio: JSON-RPC 2.0, one message per line. Like the rest of
//! the command line it works on a data directory while the engine is stopped; runs it starts or
//! escalations it resolves continue when the engine next opens.
//!
//! What it does not do: approve or activate versions. Those are a person's decisions, made with
//! `aqueduct versions approve` and `activate`; until approvals are signed, a name given over MCP is not
//! proof of who decided. Every change it makes is attributed to the identity the server was started as
//! (`--as`), never to a name the client supplies.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use aqueduct_engine::counterfactual::CounterfactualOptions;
use aqueduct_procedure::Procedure;
use aqueduct_registry::{Provenance, Registry};

use crate::{Payload, prove_version, replay_workflow, start_run};

/// Protocol versions this server speaks, newest first.
const PROTOCOL_VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

const INSTRUCTIONS: &str = "Aqueduct runs procedures: hermetic Starlark scripts that define run(input) and call \
step(), agent(), wait(), escalate() and other engine functions, each journaled. Write a procedure, check it with \
validate_procedure, see how it would have decided past runs with replay_draft, then propose_version and \
replay_version to record the evidence. A person approves and activates versions; you cannot. Start runs with \
start_run (they run when the engine next opens), inspect them with describe_run and run_journal, and resolve \
escalations with resolve_escalation.";

/// The server: a data directory, who it acts as, and whether it may change anything.
pub struct Server {
    dir: PathBuf,
    identity: String,
    read_only: bool,
}

struct Tool {
    name: &'static str,
    description: &'static str,
    schema: fn() -> Value,
    writes: bool,
}

fn string(description: &str) -> Value {
    json!({"type": "string", "description": description})
}

fn object(properties: Value, required: &[&str]) -> Value {
    json!({"type": "object", "properties": properties, "required": required, "additionalProperties": false})
}

const TOOLS: &[Tool] = &[
    Tool {
        name: "validate_procedure",
        description: "Compile a procedure (Starlark defining run(input)) and return its version id, or why it does not compile.",
        schema: || {
            object(
                json!({"source": string("The procedure's source.")}),
                &["source"],
            )
        },
        writes: false,
    },
    Tool {
        name: "replay_draft",
        description: "Replay every past run of a workflow against a procedure that is not registered, and report which runs it would decide differently. Nothing is recorded.",
        schema: || {
            object(
                json!({
                    "workflow": string("The workflow whose past runs to replay."),
                    "source": string("The procedure's source."),
                    "stubs": {"type": "object", "description": "Results for steps or agent calls the past runs never made, by name."}
                }),
                &["workflow", "source"],
            )
        },
        writes: false,
    },
    Tool {
        name: "propose_version",
        description: "Register a procedure as a new version of a workflow, with its provenance, and propose it for review. Returns the version id.",
        schema: || {
            object(
                json!({
                    "workflow": string("The workflow, for example claims/water-damage."),
                    "source": string("The procedure's source."),
                    "cases_studied": {"type": "integer", "minimum": 0, "description": "How many cases were studied to write it."},
                    "note": string("What changed and why.")
                }),
                &["workflow", "source"],
            )
        },
        writes: true,
    },
    Tool {
        name: "replay_version",
        description: "Replay every past run of the workflow against a proposed version and record the report as its evidence (proven). Returns the report.",
        schema: || {
            object(
                json!({
                    "workflow": string("The workflow."),
                    "version": string("The version id, or a prefix of it."),
                    "stubs": {"type": "object", "description": "Results for steps or agent calls the past runs never made, by name."}
                }),
                &["workflow", "version"],
            )
        },
        writes: true,
    },
    Tool {
        name: "list_versions",
        description: "List versions: state, author, approvals, replay evidence, and runs pinned to each.",
        schema: || {
            object(
                json!({"workflow": string("Only this workflow's versions.")}),
                &[],
            )
        },
        writes: false,
    },
    Tool {
        name: "show_version",
        description: "A version's provenance, replay evidence, approvals and source.",
        schema: || {
            object(
                json!({"workflow": string("The workflow."), "version": string("The version id, or a prefix.")}),
                &["workflow", "version"],
            )
        },
        writes: false,
    },
    Tool {
        name: "diff_versions",
        description: "A line diff of two versions' sources.",
        schema: || {
            object(
                json!({"workflow": string("The workflow."), "from": string("The older version."), "to": string("The newer version.")}),
                &["workflow", "from", "to"],
            )
        },
        writes: false,
    },
    Tool {
        name: "start_run",
        description: "Start a run of a workflow on its active version (or a given approved one). It runs when the engine next opens.",
        schema: || {
            object(
                json!({
                    "workflow": string("The workflow."),
                    "key": string("The run key, unique per run, for example claim-7."),
                    "input": {"description": "The run's input (any JSON)."},
                    "version": string("Start on this approved version instead of the active one."),
                    "budget_tokens": {"type": "integer", "minimum": 1, "description": "The run's token budget for agent calls."}
                }),
                &["workflow", "key", "input"],
            )
        },
        writes: true,
    },
    Tool {
        name: "list_runs",
        description: "Every run: key, workflow, status, journal length.",
        schema: || object(json!({}), &[]),
        writes: false,
    },
    Tool {
        name: "describe_run",
        description: "A run's status, version, input, output and commands.",
        schema: || object(json!({"key": string("The run key.")}), &["key"]),
        writes: false,
    },
    Tool {
        name: "run_journal",
        description: "A run's journal as JSON Lines: everything it did, in order.",
        schema: || object(json!({"key": string("The run key.")}), &["key"]),
        writes: false,
    },
    Tool {
        name: "list_escalations",
        description: "Open escalations: id, reason, and what the resolver needs to decide.",
        schema: || object(json!({}), &[]),
        writes: false,
    },
    Tool {
        name: "resolve_escalation",
        description: "Resolve an escalation; the run continues with the resolution when the engine next opens.",
        schema: || {
            object(
                json!({"id": string("The escalation id (<run key>#<command>)."), "resolution": {"description": "The resolution (any JSON)."}}),
                &["id", "resolution"],
            )
        },
        writes: true,
    },
    Tool {
        name: "metrics",
        description: "Per version: runs, escalation and timeout rates, cost, and replay divergence.",
        schema: || object(json!({"workflow": string("Only this workflow.")}), &[]),
        writes: false,
    },
];

impl Server {
    /// A server for a data directory, acting as `identity` (for example `agent/claims-explorer`).
    pub fn new(dir: &Path, identity: &str, read_only: bool) -> Server {
        Server {
            dir: dir.to_owned(),
            identity: identity.to_owned(),
            read_only,
        }
    }

    /// Serves requests from `input` until it ends, writing responses to `output`.
    pub fn serve(&self, input: impl BufRead, mut output: impl Write) -> std::io::Result<()> {
        for line in input.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let response = match serde_json::from_str::<Value>(&line) {
                Ok(message) => self.handle(&message),
                Err(e) => Some(error(Value::Null, -32700, &format!("parse error: {e}"))),
            };
            if let Some(response) = response {
                writeln!(output, "{response}")?;
                output.flush()?;
            }
        }
        Ok(())
    }

    /// Handles one JSON-RPC message; notifications get no response.
    pub fn handle(&self, message: &Value) -> Option<Value> {
        let id = message.get("id").cloned()?;
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            return Some(error(id, -32600, "invalid request: no method"));
        };
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        let result = match method {
            "initialize" => {
                let asked = params["protocolVersion"].as_str().unwrap_or("");
                let version = PROTOCOL_VERSIONS
                    .iter()
                    .find(|v| **v == asked)
                    .unwrap_or(&PROTOCOL_VERSIONS[0]);
                json!({
                    "protocolVersion": version,
                    "capabilities": {"tools": {"listChanged": false}},
                    "serverInfo": {"name": "aqueduct", "version": env!("CARGO_PKG_VERSION")},
                    "instructions": INSTRUCTIONS,
                })
            }
            "ping" => json!({}),
            "tools/list" => json!({"tools": self.tools().map(|t| json!({
                "name": t.name,
                "description": t.description,
                "inputSchema": (t.schema)(),
                "annotations": {"readOnlyHint": !t.writes, "destructiveHint": false},
            })).collect::<Vec<_>>()}),
            "tools/call" => {
                let name = params["name"].as_str().unwrap_or("");
                let Some(tool) = self.tools().find(|t| t.name == name) else {
                    return Some(error(id, -32602, &format!("unknown tool `{name}`")));
                };
                let arguments = params.get("arguments").cloned().unwrap_or(json!({}));
                match self.call(tool.name, &arguments) {
                    Ok(text) => {
                        json!({"content": [{"type": "text", "text": text}], "isError": false})
                    }
                    Err(why) => {
                        json!({"content": [{"type": "text", "text": why}], "isError": true})
                    }
                }
            }
            other => return Some(error(id, -32601, &format!("method not found: {other}"))),
        };
        Some(json!({"jsonrpc": "2.0", "id": id, "result": result}))
    }

    fn tools(&self) -> impl Iterator<Item = &'static Tool> + '_ {
        TOOLS.iter().filter(|t| !(self.read_only && t.writes))
    }

    fn call(&self, tool: &str, args: &Value) -> Result<String, String> {
        let text = |key: &str| {
            args[key]
                .as_str()
                .ok_or_else(|| format!("`{key}` is required (a string)"))
        };
        let dir = self.dir.to_str().ok_or("the data directory is not UTF-8")?;
        let cli =
            |args: &[&str]| crate::run(&args.iter().map(|a| (*a).to_owned()).collect::<Vec<_>>());
        let stubs = || -> Result<_, String> {
            match &args["stubs"] {
                Value::Null => Ok(Default::default()),
                Value::Object(map) => Ok(map
                    .iter()
                    .map(|(name, value)| {
                        (name.clone(), Payload::new(value.to_string().into_bytes()))
                    })
                    .collect()),
                _ => Err("`stubs` is an object of results by name".into()),
            }
        };
        match tool {
            "validate_procedure" => {
                let procedure = Procedure::compile("procedure.star", text("source")?)
                    .map_err(|e| e.to_string())?;
                Ok(format!("compiles: version {}\n", procedure.version()))
            }
            "replay_draft" => {
                let workflow = text("workflow")?;
                let procedure = Procedure::compile(&format!("{workflow}.star"), text("source")?)
                    .map_err(|e| e.to_string())?;
                let options = CounterfactualOptions { stubs: stubs()? };
                Ok(replay_workflow(&self.dir, &procedure, workflow, &options)?.0)
            }
            "propose_version" => {
                let workflow = text("workflow")?;
                let mut registry = Registry::open(&self.dir).map_err(|e| e.to_string())?;
                let provenance = Provenance {
                    author: self.identity.clone(),
                    explored_by: Some(self.identity.clone()),
                    cases_studied: args["cases_studied"].as_u64(),
                    note: args["note"].as_str().map(str::to_owned),
                };
                let id = registry
                    .register(workflow, text("source")?, provenance)
                    .map_err(|e| e.to_string())?;
                registry
                    .propose(workflow, &id, &self.identity)
                    .map_err(|e| e.to_string())?;
                Ok(format!(
                    "{workflow} version {id} registered and proposed by {}. Next: replay_version, then a person approves it.\n",
                    self.identity
                ))
            }
            "replay_version" => {
                let mut registry = Registry::open(&self.dir).map_err(|e| e.to_string())?;
                prove_version(
                    &self.dir,
                    &mut registry,
                    text("workflow")?,
                    text("version")?,
                    &self.identity,
                    stubs()?,
                )
            }
            "list_versions" => match args["workflow"].as_str() {
                Some(workflow) => cli(&["versions", "list", dir, workflow]),
                None => cli(&["versions", "list", dir]),
            },
            "show_version" => cli(&["versions", "show", dir, text("workflow")?, text("version")?]),
            "diff_versions" => cli(&[
                "versions",
                "diff",
                dir,
                text("workflow")?,
                text("from")?,
                text("to")?,
            ]),
            "start_run" => {
                let input = args.get("input").ok_or("`input` is required")?.to_string();
                start_run(
                    &self.dir,
                    text("workflow")?,
                    text("key")?,
                    &input,
                    args["version"].as_str().map(str::to_owned),
                    args["budget_tokens"].as_u64(),
                )
            }
            "list_runs" => cli(&["runs", dir]),
            "describe_run" => cli(&["describe", dir, text("key")?]),
            "run_journal" => cli(&["export", dir, text("key")?]),
            "list_escalations" => {
                let open = cli(&["escalations", dir])?;
                Ok(if open.is_empty() {
                    "no open escalations\n".into()
                } else {
                    open
                })
            }
            "resolve_escalation" => {
                let resolution = args
                    .get("resolution")
                    .ok_or("`resolution` is required")?
                    .to_string();
                cli(&["resolve", dir, text("id")?, &resolution])
            }
            "metrics" => match args["workflow"].as_str() {
                Some(workflow) => cli(&["metrics", dir, workflow]),
                None => cli(&["metrics", dir]),
            },
            other => Err(format!("unknown tool `{other}`")),
        }
    }
}

fn error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}
