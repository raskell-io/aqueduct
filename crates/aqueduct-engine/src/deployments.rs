//! Deployments (ADR 0048): the builds of each remote workflow, which one new runs start on, and which
//! have been retired. Kept in `deployments.json` in the data directory, written before it takes effect.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use serde_json::{Map, Value, json};

use crate::partition::Deciders;

const FILE: &str = "deployments.json";

/// Where a build is in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildState {
    /// New runs start on it. At most one per workflow.
    Active,
    /// It was replaced: no new runs start on it, and its runs finish on it. It can be activated again.
    Draining,
    /// No runs were left on it, and none will start.
    Retired,
}

impl BuildState {
    /// The state's name.
    pub fn as_str(self) -> &'static str {
        match self {
            BuildState::Active => "active",
            BuildState::Draining => "draining",
            BuildState::Retired => "retired",
        }
    }

    fn parse(text: &str) -> Option<BuildState> {
        match text {
            "active" => Some(BuildState::Active),
            "draining" => Some(BuildState::Draining),
            "retired" => Some(BuildState::Retired),
            _ => None,
        }
    }
}

/// One build, as the server sees it now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeploymentInfo {
    /// The workflow.
    pub workflow: String,
    /// The build.
    pub build: String,
    /// Its state; `None` if runs or workers name it, and it was never activated.
    pub state: Option<BuildState>,
    /// Runs pinned to it that have not ended.
    pub open_runs: usize,
    /// Workers connected now that announce it.
    pub workers: usize,
}

/// The builds of every remote workflow.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Deployments {
    pub(crate) builds: BTreeMap<String, BTreeMap<String, BuildState>>,
}

impl Deployments {
    /// Reads a data directory's deployments. A file that cannot be understood stops the engine opening:
    /// starting new runs on the wrong build would be worse.
    pub(crate) fn load(dir: &Path) -> Result<Deployments, String> {
        let path = dir.join(FILE);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Deployments::default()),
            Err(e) => return Err(format!("{}: {e}", path.display())),
        };
        let invalid = |why: &str| format!("{}: {why}", path.display());
        let value: Value = serde_json::from_str(&text).map_err(|e| invalid(&e.to_string()))?;
        let workflows = value.as_object().ok_or_else(|| invalid("not an object"))?;
        let mut builds = BTreeMap::new();
        for (workflow, states) in workflows {
            let states = states.as_object().ok_or_else(|| invalid("not an object"))?;
            let mut parsed = BTreeMap::new();
            for (build, state) in states {
                let state = state
                    .as_str()
                    .and_then(BuildState::parse)
                    .ok_or_else(|| invalid("an unknown state"))?;
                parsed.insert(build.clone(), state);
            }
            let active = parsed
                .values()
                .filter(|s| **s == BuildState::Active)
                .count();
            if active > 1 {
                return Err(invalid("a workflow has several active builds"));
            }
            builds.insert(workflow.clone(), parsed);
        }
        Ok(Deployments { builds })
    }

    /// Writes them durably: a crash leaves the old file or the new one.
    fn save(&self, dir: &Path) -> Result<(), String> {
        let io = |e: std::io::Error| format!("{}: {e}", dir.display());
        let mut workflows = Map::new();
        for (workflow, builds) in &self.builds {
            let states: Map<String, Value> = builds
                .iter()
                .map(|(build, state)| (build.clone(), json!(state.as_str())))
                .collect();
            workflows.insert(workflow.clone(), Value::Object(states));
        }
        let temporary = dir.join("deployments.json.new");
        let mut file = std::fs::File::create(&temporary).map_err(io)?;
        file.write_all(Value::Object(workflows).to_string().as_bytes())
            .map_err(io)?;
        file.sync_all().map_err(io)?;
        std::fs::rename(&temporary, dir.join(FILE)).map_err(io)?;
        std::fs::File::open(dir)
            .and_then(|d| d.sync_all())
            .map_err(io)
    }

    /// Gives the workflows' logic these builds: the active one for new runs.
    pub(crate) fn apply(&self, deciders: &mut Deciders) -> Result<(), String> {
        for (workflow, builds) in &self.builds {
            if !deciders.is_remote(workflow) {
                continue;
            }
            for (build, state) in builds {
                match state {
                    BuildState::Active => deciders
                        .activate_build(workflow, build)
                        .map_err(|e| e.to_string())?,
                    BuildState::Draining => deciders.allow_build(workflow, build),
                    BuildState::Retired => {}
                }
            }
        }
        Ok(())
    }

    /// Makes `build` active, durably; the build before it starts draining.
    pub(crate) fn activate(
        &mut self,
        dir: &Path,
        workflow: &str,
        build: &str,
    ) -> Result<(), String> {
        let mut next = self.clone();
        let builds = next.builds.entry(workflow.to_owned()).or_default();
        if builds.get(build) == Some(&BuildState::Retired) {
            return Err(format!("build `{build}` of `{workflow}` was retired"));
        }
        for state in builds.values_mut() {
            if *state == BuildState::Active {
                *state = BuildState::Draining;
            }
        }
        builds.insert(build.to_owned(), BuildState::Active);
        next.save(dir)?;
        *self = next;
        Ok(())
    }

    /// Retires a draining build with no open runs, durably.
    pub(crate) fn retire(
        &mut self,
        dir: &Path,
        workflow: &str,
        build: &str,
        open_runs: usize,
    ) -> Result<(), String> {
        match self.builds.get(workflow).and_then(|b| b.get(build)) {
            None => return Err(format!("`{workflow}` has no build `{build}`")),
            Some(BuildState::Active) => {
                return Err(format!(
                    "build `{build}` of `{workflow}` is active: activate another first"
                ));
            }
            Some(BuildState::Retired) => return Ok(()),
            Some(BuildState::Draining) => {}
        }
        if open_runs > 0 {
            return Err(format!(
                "build `{build}` of `{workflow}` has {open_runs} open runs"
            ));
        }
        let mut next = self.clone();
        if let Some(builds) = next.builds.get_mut(workflow) {
            builds.insert(build.to_owned(), BuildState::Retired);
        }
        next.save(dir)?;
        *self = next;
        Ok(())
    }
}
