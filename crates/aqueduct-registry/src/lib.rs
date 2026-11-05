//! The version registry (BRIEF.md section 4.5; ADR 0030): procedure versions with their provenance,
//! replay evidence, approvals and lifecycle.
//!
//! ```text
//! draft -> proposed -> proven -> approved -> active -> draining -> retired
//! ```
//!
//! A registry lives in a data directory's `registry/` directory:
//!
//! - `sources/<version>.star`: each version's source, named by its version id (the SHA-256 of the
//!   source, ADR 0029), so a version cannot change after it is registered;
//! - `events.log`: the lifecycle, one event per line, each naming who did it and when, and each chained
//!   to the one before by a SHA-256 hash, so an edited or removed event is detected when the registry is
//!   opened;
//! - `policy.json` (optional, written by hand): which evidence each workflow's transitions need;
//! - `identities.json` (optional, written by hand): which Ed25519 public keys belong to whom, so that
//!   approvals can be signed ([`identity`], ADR 0033).
//!
//! Every transition is checked when it is made: its starting state, and the policy. The engine is given
//! the deployable versions (approved, active and draining) with [`Registry::deploy`]; the active one is
//! where new runs start.

#![forbid(unsafe_code)]

pub mod identity;

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use aqueduct_engine::Deciders;
use aqueduct_engine::embedded::Builder;
use aqueduct_procedure::{Procedure, version_of};
use serde_json::{Value, json};

pub use identity::{Identities, PublicKey, SecretKey, Signature, approval_statement};

/// A version's place in the lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum State {
    /// Registered; not yet put forward.
    Draft,
    /// Put forward for review.
    Proposed,
    /// Replayed against past runs, with the report recorded.
    Proven,
    /// Approved as the policy requires: deployable, and ready to activate.
    Approved,
    /// The version new runs start on. At most one per workflow.
    Active,
    /// Replaced by another version: no new runs start on it; its runs finish on it.
    Draining,
    /// No runs left on it, and none will start: it is no longer deployed.
    Retired,
}

impl State {
    /// The state's name.
    pub fn as_str(self) -> &'static str {
        match self {
            State::Draft => "draft",
            State::Proposed => "proposed",
            State::Proven => "proven",
            State::Approved => "approved",
            State::Active => "active",
            State::Draining => "draining",
            State::Retired => "retired",
        }
    }

    /// Whether the engine is given versions in this state: approved (for canaries), active, and
    /// draining (its runs must finish).
    pub fn is_deployed(self) -> bool {
        matches!(self, State::Approved | State::Active | State::Draining)
    }
}

/// Where a version came from.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Provenance {
    /// Who wrote or submitted it: a person, or an agent such as `agent/claims-explorer`.
    pub author: String,
    /// The agent that explored the cases it was crystallised from, if any.
    pub explored_by: Option<String>,
    /// How many cases were studied to write it, if known.
    pub cases_studied: Option<u64>,
    /// Anything else worth knowing.
    pub note: Option<String>,
}

/// A counterfactual replay's results (ADR 0028), the evidence that proves a version.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplayEvidence {
    /// The past runs replayed.
    pub runs: u64,
    /// Runs the version decides the same way.
    pub same: u64,
    /// Runs it decides differently.
    pub different: u64,
    /// Runs it asked something of that nothing could answer.
    pub unresolved: u64,
    /// The SHA-256 of the full report, so the report the evidence came from can be checked.
    pub report_sha256: String,
}

/// One approval of a version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Approval {
    /// Who approved it.
    pub by: String,
    /// When, in milliseconds since the Unix epoch.
    pub at_ms: u64,
    /// Why, or on what conditions.
    pub note: Option<String>,
    /// The SHA-256 of the replay report approved, if the version had one. Only approvals of the
    /// version's current report count towards the policy.
    pub replay_sha256: Option<String>,
    /// The approver's signature of the [approval statement](approval_statement), if signed. It was
    /// checked when the registry was opened; whether its key is the approver's is
    /// [`Registry::trusted`].
    pub signature: Option<Signature>,
}

/// A registered version and what is known about it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version {
    /// The workflow it is a version of.
    pub workflow: String,
    /// Its id: the SHA-256 of its source.
    pub id: String,
    /// Where it is in the lifecycle.
    pub state: State,
    /// Where it came from.
    pub provenance: Provenance,
    /// The latest replay evidence, if it was proven.
    pub replay: Option<ReplayEvidence>,
    /// Its approvals, in order.
    pub approvals: Vec<Approval>,
    /// When it was registered.
    pub registered_at_ms: u64,
}

/// What an event did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// The version was registered (state: draft).
    Registered(Provenance),
    /// It was proposed.
    Proposed,
    /// It was proven by a replay.
    Proven(ReplayEvidence),
    /// Someone approved it. Approvals accumulate; the state changes with [`Action::Approved`].
    Approval {
        /// Why, or on what conditions.
        note: Option<String>,
        /// The SHA-256 of the replay report approved.
        replay_sha256: Option<String>,
        /// The approver's signature of the approval statement.
        signature: Option<Signature>,
    },
    /// Its approvals met the policy (state: approved).
    Approved,
    /// It became the active version (state: active).
    Activated,
    /// Another version became active (state: draining).
    Draining,
    /// It was retired (state: retired).
    Retired,
}

impl Action {
    /// The action's name, as the log records it.
    pub fn kind(&self) -> &'static str {
        match self {
            Action::Registered(_) => "registered",
            Action::Proposed => "proposed",
            Action::Proven(_) => "proven",
            Action::Approval { .. } => "approval",
            Action::Approved => "approved",
            Action::Activated => "activated",
            Action::Draining => "draining",
            Action::Retired => "retired",
        }
    }
}

/// Whether a version's replay report satisfies a policy.
fn replay_meets(policy: &Policy, version: &Version) -> Result<(), RegistryError> {
    if policy.require_replay {
        let replay = version
            .replay
            .as_ref()
            .ok_or_else(|| RegistryError::Denied("the version has no replay report".into()))?;
        if replay.runs < policy.min_runs {
            return Err(RegistryError::Denied(format!(
                "its replay covers {} past runs (at least {})",
                replay.runs, policy.min_runs
            )));
        }
        if replay.unresolved > policy.max_unresolved {
            return Err(RegistryError::Denied(format!(
                "its replay left {} runs unresolved (at most {})",
                replay.unresolved, policy.max_unresolved
            )));
        }
        if let Some(max) = policy.max_different
            && replay.different > max
        {
            return Err(RegistryError::Denied(format!(
                "its replay decides {} runs differently (at most {max})",
                replay.different
            )));
        }
    }
    Ok(())
}

/// One line of the registry's log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    /// The event's position, from 0.
    pub seq: u64,
    /// When, in milliseconds since the Unix epoch.
    pub at_ms: u64,
    /// Who did it.
    pub by: String,
    /// The workflow.
    pub workflow: String,
    /// The version.
    pub version: String,
    /// What was done.
    pub action: Action,
    /// The event's hash: SHA-256 of the previous event's hash, `:`, and the event's JSON.
    pub hash: String,
}

/// Which evidence a workflow's transitions need.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    /// Approval needs a replay report (the version is proven). Default: true.
    pub require_replay: bool,
    /// The fewest past runs the replay must cover. Default: 0 (a workflow's first version has none).
    pub min_runs: u64,
    /// The most unresolved runs the replay may have. Default: 0.
    pub max_unresolved: u64,
    /// The most runs the replay may decide differently, if limited. Default: unlimited (a new version
    /// usually means to decide some cases differently; the report shows which).
    pub max_different: Option<u64>,
    /// Approvals needed. Default: 1.
    pub approvals: usize,
    /// Who may approve; empty: anyone. Default: empty.
    pub approvers: Vec<String>,
    /// Whether the version's author may approve it. Default: false (four eyes).
    pub author_may_approve: bool,
    /// Whether approvals must be signed with a key `identities.json` gives the approver. Deployment
    /// checks it again, so a version whose signed approvals no longer meet the policy is not run.
    /// Default: false.
    pub require_signatures: bool,
}

impl Default for Policy {
    fn default() -> Self {
        Policy {
            require_replay: true,
            min_runs: 0,
            max_unresolved: 0,
            max_different: None,
            approvals: 1,
            approvers: Vec::new(),
            author_may_approve: false,
            require_signatures: false,
        }
    }
}

impl Policy {
    fn from_json(value: &Value, base: &Policy) -> Result<Policy, String> {
        let Some(map) = value.as_object() else {
            return Err("a policy is a JSON object".into());
        };
        let mut policy = base.clone();
        for (key, value) in map {
            let wrong = || format!("policy field `{key}` has the wrong type");
            match key.as_str() {
                "require_replay" => policy.require_replay = value.as_bool().ok_or_else(wrong)?,
                "min_runs" => policy.min_runs = value.as_u64().ok_or_else(wrong)?,
                "max_unresolved" => policy.max_unresolved = value.as_u64().ok_or_else(wrong)?,
                "max_different" => {
                    policy.max_different = match value {
                        Value::Null => None,
                        value => Some(value.as_u64().ok_or_else(wrong)?),
                    }
                }
                "approvals" => {
                    policy.approvals =
                        usize::try_from(value.as_u64().ok_or_else(wrong)?).map_err(|_| wrong())?;
                }
                "approvers" => {
                    policy.approvers = value
                        .as_array()
                        .ok_or_else(wrong)?
                        .iter()
                        .map(|v| v.as_str().map(str::to_owned).ok_or_else(wrong))
                        .collect::<Result<_, _>>()?;
                }
                "author_may_approve" => {
                    policy.author_may_approve = value.as_bool().ok_or_else(wrong)?;
                }
                "require_signatures" => {
                    policy.require_signatures = value.as_bool().ok_or_else(wrong)?;
                }
                other => return Err(format!("unknown policy field `{other}`")),
            }
        }
        Ok(policy)
    }
}

/// Registry errors.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RegistryError {
    /// Reading or writing the registry failed.
    #[error("registry: {0}")]
    Io(String),
    /// The log is not a valid chain of events: edited, truncated in the middle, or not a registry log.
    #[error("the registry log is invalid at line {line}: {why}")]
    Tampered {
        /// The line, from 1.
        line: usize,
        /// What is wrong.
        why: String,
    },
    /// `policy.json` is invalid.
    #[error("registry policy: {0}")]
    Policy(String),
    /// No version matches.
    #[error("workflow `{workflow}` has no version `{version}`")]
    UnknownVersion {
        /// The workflow.
        workflow: String,
        /// The version (or prefix) asked for.
        version: String,
    },
    /// A version prefix matches more than one version.
    #[error("`{0}` matches more than one version; give more of it")]
    Ambiguous(String),
    /// The source is already registered.
    #[error("this source is already registered as version {0}")]
    AlreadyRegistered(String),
    /// The source is not a valid procedure.
    #[error("{0}")]
    Invalid(String),
    /// The transition is not allowed from the version's state.
    #[error("version {version} is {state}; it cannot be {action}")]
    WrongState {
        /// The version.
        version: String,
        /// Its state.
        state: &'static str,
        /// What was attempted.
        action: &'static str,
    },
    /// The policy does not allow it.
    #[error("the policy does not allow this: {0}")]
    Denied(String),
}

fn io(error: impl std::fmt::Display) -> RegistryError {
    RegistryError::Io(error.to_string())
}

/// The hash before the first event.
const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// A data directory's version registry.
#[derive(Clone, Debug)]
pub struct Registry {
    dir: PathBuf,
    events: Vec<Event>,
    versions: BTreeMap<(String, String), Version>,
    default_policy: Policy,
    policies: BTreeMap<String, Policy>,
    identities: Identities,
}

impl Registry {
    /// Opens the registry of a data directory (creating it if there is none) and verifies its log.
    pub fn open(data_dir: &Path) -> Result<Registry, RegistryError> {
        let dir = data_dir.join("registry");
        std::fs::create_dir_all(dir.join("sources")).map_err(io)?;
        let mut registry = Registry {
            dir,
            events: Vec::new(),
            versions: BTreeMap::new(),
            default_policy: Policy::default(),
            policies: BTreeMap::new(),
            identities: Identities::default(),
        };
        registry.load_policy()?;
        registry.identities = Identities::read(&registry.dir.join("identities.json"))?;
        let log = match std::fs::read_to_string(registry.dir.join("events.log")) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => return Err(io(e)),
        };
        let mut prev = GENESIS.to_owned();
        for (i, line) in log.lines().enumerate() {
            let tampered = |why: String| RegistryError::Tampered { line: i + 1, why };
            let (hash, body) = line
                .split_once(' ')
                .ok_or_else(|| tampered("not `<hash> <event>`".into()))?;
            if hash != chain_hash(&prev, body) {
                return Err(tampered(
                    "the hash does not match the event and its predecessor".into(),
                ));
            }
            let event = parse_event(body, hash).map_err(tampered)?;
            if event.seq != u64::try_from(i).unwrap_or(u64::MAX) {
                return Err(tampered(format!("event {} is out of sequence", event.seq)));
            }
            registry
                .apply(&event)
                .map_err(|e| tampered(e.to_string()))?;
            registry.events.push(event);
            prev = hash.to_owned();
        }
        Ok(registry)
    }

    fn load_policy(&mut self) -> Result<(), RegistryError> {
        let text = match std::fs::read_to_string(self.dir.join("policy.json")) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(io(e)),
        };
        let value: Value =
            serde_json::from_str(&text).map_err(|e| RegistryError::Policy(e.to_string()))?;
        if let Some(default) = value.get("default") {
            self.default_policy =
                Policy::from_json(default, &Policy::default()).map_err(RegistryError::Policy)?;
        }
        if let Some(workflows) = value.get("workflows").and_then(Value::as_object) {
            for (name, policy) in workflows {
                let policy = Policy::from_json(policy, &self.default_policy)
                    .map_err(|e| RegistryError::Policy(format!("{name}: {e}")))?;
                self.policies.insert(name.clone(), policy);
            }
        }
        Ok(())
    }

    /// The policy for a workflow.
    pub fn policy(&self, workflow: &str) -> &Policy {
        self.policies.get(workflow).unwrap_or(&self.default_policy)
    }

    /// Who holds which keys.
    pub fn identities(&self) -> &Identities {
        &self.identities
    }

    /// Uses `identities` instead of the registry's `identities.json`: for keeping the identities
    /// somewhere the registry's writers cannot change them.
    pub fn set_identities(&mut self, identities: Identities) {
        self.identities = identities;
    }

    /// Whether an approval is signed with one of its approver's keys.
    pub fn trusted(&self, approval: &Approval) -> bool {
        approval
            .signature
            .as_ref()
            .is_some_and(|s| self.identities.trusts(&approval.by, &s.key))
    }

    /// The approvals of a version that count towards its policy now: of its current replay report, by
    /// someone the policy lets approve it, and signed with one of their keys if the policy says so.
    pub fn valid_approvals<'a>(&self, version: &'a Version) -> Vec<&'a Approval> {
        let policy = self.policy(&version.workflow);
        let current = version.replay.as_ref().map(|r| r.report_sha256.as_str());
        version
            .approvals
            .iter()
            .filter(|a| a.replay_sha256.as_deref() == current)
            .filter(|a| policy.author_may_approve || a.by != version.provenance.author)
            .filter(|a| policy.approvers.is_empty() || policy.approvers.contains(&a.by))
            .filter(|a| !policy.require_signatures || self.trusted(a))
            .collect()
    }

    /// The statement `by` signs to approve a version as it is now: see [`approval_statement`].
    pub fn statement(
        &self,
        workflow: &str,
        id: &str,
        by: &str,
        note: Option<&str>,
    ) -> Result<String, RegistryError> {
        let version = self.version(workflow, id)?;
        Ok(approval_statement(
            workflow,
            &version.id,
            by,
            version.replay.as_ref().map(|r| r.report_sha256.as_str()),
            note,
        ))
    }

    /// Every event, in order.
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// Every version, by workflow, then id.
    pub fn versions(&self) -> impl Iterator<Item = &Version> {
        self.versions.values()
    }

    /// The version of `workflow` whose id is or starts with `id`.
    pub fn version(&self, workflow: &str, id: &str) -> Result<&Version, RegistryError> {
        let mut matches = self
            .versions
            .range((workflow.to_owned(), id.to_owned())..)
            .take_while(|((w, v), _)| w == workflow && v.starts_with(id))
            .map(|(_, version)| version);
        let first = matches
            .next()
            .ok_or_else(|| RegistryError::UnknownVersion {
                workflow: workflow.to_owned(),
                version: id.to_owned(),
            })?;
        if matches.next().is_some() {
            return Err(RegistryError::Ambiguous(id.to_owned()));
        }
        Ok(first)
    }

    /// The active version of a workflow, if it has one.
    pub fn active(&self, workflow: &str) -> Option<&Version> {
        self.versions
            .values()
            .find(|v| v.workflow == workflow && v.state == State::Active)
    }

    /// A version's source.
    pub fn source(&self, version: &Version) -> Result<String, RegistryError> {
        let text = std::fs::read_to_string(self.source_path(&version.id)).map_err(io)?;
        if version_of(&text) != version.id {
            return Err(RegistryError::Tampered {
                line: 0,
                why: format!("the source of version {} was changed", version.id),
            });
        }
        Ok(text)
    }

    fn source_path(&self, id: &str) -> PathBuf {
        self.dir.join("sources").join(format!("{id}.star"))
    }

    /// Registers a procedure version (state: draft) and returns its id. The source must compile.
    pub fn register(
        &mut self,
        workflow: &str,
        source: &str,
        provenance: Provenance,
    ) -> Result<String, RegistryError> {
        let id = version_of(source);
        if self
            .versions
            .contains_key(&(workflow.to_owned(), id.clone()))
        {
            return Err(RegistryError::AlreadyRegistered(id));
        }
        Procedure::compile(&format!("{workflow}.star"), source)
            .map_err(|e| RegistryError::Invalid(e.to_string()))?;
        if provenance.author.is_empty() {
            return Err(RegistryError::Invalid(
                "a version needs an author in its provenance".into(),
            ));
        }
        let path = self.source_path(&id);
        std::fs::write(&path, source).map_err(io)?;
        std::fs::File::open(&path)
            .and_then(|f| f.sync_all())
            .map_err(io)?;
        let by = provenance.author.clone();
        self.append(&by, workflow, &id, Action::Registered(provenance))?;
        Ok(id)
    }

    /// Proposes a draft.
    pub fn propose(&mut self, workflow: &str, id: &str, by: &str) -> Result<(), RegistryError> {
        let version = self.version(workflow, id)?.clone();
        require(&version, &[State::Draft], "proposed")?;
        self.append(by, workflow, &version.id, Action::Proposed)
    }

    /// Records a replay report for a proposed version (or a newer one for a proven version).
    pub fn prove(
        &mut self,
        workflow: &str,
        id: &str,
        by: &str,
        evidence: ReplayEvidence,
    ) -> Result<(), RegistryError> {
        let version = self.version(workflow, id)?.clone();
        require(&version, &[State::Proposed, State::Proven], "proven")?;
        self.append(by, workflow, &version.id, Action::Proven(evidence))
    }

    /// Records `by`'s unsigned approval: see [`Registry::approve_signed`].
    pub fn approve(
        &mut self,
        workflow: &str,
        id: &str,
        by: &str,
        note: Option<String>,
    ) -> Result<State, RegistryError> {
        self.approve_signed(workflow, id, by, note, None)
    }

    /// Records `by`'s approval of the version and its current replay report, with `by`'s signature of
    /// the [statement](Registry::statement) if given: it must verify, with a key `identities.json`
    /// gives `by`. The policy may require one. Once the valid approvals meet the policy, the version
    /// is approved. Returns the version's state afterwards.
    pub fn approve_signed(
        &mut self,
        workflow: &str,
        id: &str,
        by: &str,
        note: Option<String>,
        signature: Option<Signature>,
    ) -> Result<State, RegistryError> {
        let version = self.version(workflow, id)?.clone();
        let policy = self.policy(workflow).clone();
        let allowed: &[State] = if policy.require_replay {
            &[State::Proven]
        } else {
            &[State::Proposed, State::Proven]
        };
        require(&version, allowed, "approved")?;
        if !policy.author_may_approve && by == version.provenance.author {
            return Err(RegistryError::Denied(format!(
                "{by} wrote this version and may not approve it"
            )));
        }
        if !policy.approvers.is_empty() && !policy.approvers.iter().any(|a| a == by) {
            return Err(RegistryError::Denied(format!(
                "{by} is not an approver of `{workflow}` ({})",
                policy.approvers.join(", ")
            )));
        }
        let replay_sha256 = version.replay.as_ref().map(|r| r.report_sha256.clone());
        if version
            .approvals
            .iter()
            .any(|a| a.by == by && a.replay_sha256 == replay_sha256)
        {
            return Err(RegistryError::Denied(format!(
                "{by} has already approved it"
            )));
        }
        match &signature {
            Some(signature) => {
                let statement = self.statement(workflow, &version.id, by, note.as_deref())?;
                if !signature
                    .key
                    .verifies(statement.as_bytes(), &signature.signature)
                {
                    return Err(RegistryError::Denied(
                        "the signature does not verify: it was not made over this approval's statement with this key".into(),
                    ));
                }
                if !self.identities.trusts(by, &signature.key) {
                    return Err(RegistryError::Denied(format!(
                        "{} is not one of {by}'s keys in identities.json",
                        signature.key
                    )));
                }
            }
            None if policy.require_signatures => {
                return Err(RegistryError::Denied(format!(
                    "approvals of `{workflow}` must be signed"
                )));
            }
            None => {}
        }
        replay_meets(&policy, &version)?;
        self.append(
            by,
            workflow,
            &version.id,
            Action::Approval {
                note,
                replay_sha256,
                signature,
            },
        )?;
        let valid = self
            .valid_approvals(self.version(workflow, &version.id)?)
            .len();
        if valid >= policy.approvals {
            self.append(by, workflow, &version.id, Action::Approved)?;
        }
        Ok(self.version(workflow, &version.id)?.state)
    }

    /// Makes an approved version active; the active one, if any, starts draining. A draining version
    /// can be made active again (a rollback).
    pub fn activate(&mut self, workflow: &str, id: &str, by: &str) -> Result<(), RegistryError> {
        let version = self.version(workflow, id)?.clone();
        require(&version, &[State::Approved, State::Draining], "activated")?;
        if let Some(active) = self.active(workflow).map(|v| v.id.clone()) {
            self.append(by, workflow, &active, Action::Draining)?;
        }
        self.append(by, workflow, &version.id, Action::Activated)
    }

    /// Retires a version that is not active and has no unfinished runs: `unfinished` is how many runs
    /// pinned to it have not ended.
    pub fn retire(
        &mut self,
        workflow: &str,
        id: &str,
        by: &str,
        unfinished: usize,
    ) -> Result<(), RegistryError> {
        let version = self.version(workflow, id)?.clone();
        require(
            &version,
            &[
                State::Draft,
                State::Proposed,
                State::Proven,
                State::Approved,
                State::Draining,
            ],
            "retired",
        )?;
        if unfinished > 0 {
            return Err(RegistryError::Denied(format!(
                "{unfinished} runs on it have not finished; it is still draining"
            )));
        }
        self.append(by, workflow, &version.id, Action::Retired)
    }

    /// Registers the deployable versions (approved, active, draining) with an engine, and activates the
    /// active ones.
    pub fn deploy(&self, mut builder: Builder) -> Result<Builder, RegistryError> {
        for version in self.versions.values().filter(|v| v.state.is_deployed()) {
            self.check_signed(version)?;
            let source = self.source(version)?;
            let procedure = Procedure::compile(&format!("{}.star", version.workflow), &source)
                .map_err(|e| RegistryError::Invalid(e.to_string()))?;
            builder = builder.workflow_version(&version.workflow, &version.id, Arc::new(procedure));
            if version.state == State::Active {
                builder = builder.active_version(&version.workflow, &version.id);
            }
        }
        Ok(builder)
    }

    /// The deployable versions (approved, active, draining) as deciders, with the active ones
    /// activated: for working on a data directory without an engine (the CLI's `start`).
    pub fn deciders(&self) -> Result<Deciders, RegistryError> {
        let mut deciders = Deciders::new();
        for version in self.versions.values().filter(|v| v.state.is_deployed()) {
            self.check_signed(version)?;
            let source = self.source(version)?;
            let procedure = Procedure::compile(&format!("{}.star", version.workflow), &source)
                .map_err(|e| RegistryError::Invalid(e.to_string()))?;
            deciders.insert_version(&version.workflow, &version.id, Arc::new(procedure));
            if version.state == State::Active {
                deciders
                    .activate(&version.workflow, &version.id)
                    .map_err(|e| RegistryError::Invalid(e.to_string()))?;
            }
        }
        Ok(deciders)
    }

    /// Where the policy requires signatures, whether a deployed version's signed approvals still meet
    /// it, with the identities as they are now.
    fn check_signed(&self, version: &Version) -> Result<(), RegistryError> {
        let policy = self.policy(&version.workflow);
        if !policy.require_signatures {
            return Ok(());
        }
        let valid = self.valid_approvals(version).len();
        if valid < policy.approvals {
            return Err(RegistryError::Denied(format!(
                "{} version {} is {} but has {valid} valid signed approvals of {} needed; it will not be deployed",
                version.workflow,
                version.id,
                version.state.as_str(),
                policy.approvals
            )));
        }
        Ok(())
    }

    fn append(
        &mut self,
        by: &str,
        workflow: &str,
        version: &str,
        action: Action,
    ) -> Result<(), RegistryError> {
        if by.trim().is_empty() {
            return Err(RegistryError::Invalid(
                "every change needs a name (by)".into(),
            ));
        }
        let event = Event {
            seq: u64::try_from(self.events.len()).unwrap_or(u64::MAX),
            at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            by: by.to_owned(),
            workflow: workflow.to_owned(),
            version: version.to_owned(),
            action,
            hash: String::new(),
        };
        self.append_event(event)
    }

    /// Chains an event to the log, writes it durably, and folds it into the versions.
    fn append_event(&mut self, event: Event) -> Result<(), RegistryError> {
        let body = event_json(&event).to_string();
        let prev = self.events.last().map_or(GENESIS, |e| e.hash.as_str());
        let hash = chain_hash(prev, &body);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("events.log"))
            .map_err(io)?;
        writeln!(file, "{hash} {body}").map_err(io)?;
        file.sync_all().map_err(io)?;
        let event = Event { hash, ..event };
        self.apply(&event)?;
        self.events.push(event);
        Ok(())
    }

    /// Takes on another registry's workflows, with their whole history: who did what, and when (ADR 0047).
    /// The events are chained again in this log, so a source's own chain is not kept; approvals' signatures
    /// are over the version and its report, and still verify. Nothing changes in the log if a workflow has
    /// other events here, or if a deployed version would not pass this registry's policy, which applies
    /// whatever policy approved it. Taking on the same registry again does nothing. Returns notes for the
    /// operator.
    pub fn merge(&mut self, from: &Registry) -> Result<Vec<String>, RegistryError> {
        let mut notes = Vec::new();
        // What each workflow still needs.
        let same = |a: &Event, b: &Event| {
            a.at_ms == b.at_ms && a.by == b.by && a.version == b.version && a.action == b.action
        };
        let mut have: BTreeMap<&str, usize> = BTreeMap::new();
        for event in &from.events {
            if have.contains_key(event.workflow.as_str()) {
                continue;
            }
            let mine: Vec<&Event> = self
                .events
                .iter()
                .filter(|e| e.workflow == event.workflow)
                .collect();
            let theirs: Vec<&Event> = from
                .events
                .iter()
                .filter(|e| e.workflow == event.workflow)
                .collect();
            let common = mine.len().min(theirs.len());
            if mine.len() > theirs.len()
                || !mine[..common]
                    .iter()
                    .zip(&theirs[..common])
                    .all(|(a, b)| same(a, b))
            {
                return Err(RegistryError::Invalid(format!(
                    "workflow `{}` already has other versions or events here",
                    event.workflow
                )));
            }
            have.insert(&event.workflow, mine.len());
        }
        let mut seen: BTreeMap<&str, usize> = BTreeMap::new();
        let mut needed = Vec::new();
        for event in &from.events {
            let count = seen.entry(&event.workflow).or_default();
            *count += 1;
            if *count > have.get(event.workflow.as_str()).copied().unwrap_or(0) {
                needed.push(event);
            }
        }
        // This directory's policy and identities stay, if it has them.
        for (name, what) in [("policy.json", "policy"), ("identities.json", "identities")] {
            let (ours, theirs) = (self.dir.join(name), from.dir.join(name));
            if !theirs.exists() {
                continue;
            }
            if ours.exists() {
                if std::fs::read(&ours).map_err(io)? != std::fs::read(&theirs).map_err(io)? {
                    notes.push(format!("kept this server's {what}: the source's differs"));
                }
            } else {
                std::fs::copy(&theirs, &ours).map_err(io)?;
                std::fs::File::open(&ours)
                    .and_then(|f| f.sync_all())
                    .map_err(io)?;
                notes.push(format!("took the source's {what}"));
            }
        }
        self.load_policy()?;
        self.identities = Identities::read(&self.dir.join("identities.json"))?;
        // Whether it all holds, before anything is written.
        let mut trial = self.clone();
        for event in &needed {
            trial.apply(event)?;
        }
        let incoming = trial
            .versions
            .values()
            .filter(|v| v.state.is_deployed() && have.contains_key(v.workflow.as_str()));
        for version in incoming {
            trial.check_signed(version)?;
            trial.passes_policy(version)?;
        }
        for version in from.versions.values() {
            let text = from.source(version)?;
            let path = self.source_path(&version.id);
            if !path.exists() {
                let mut file = std::fs::File::create(&path).map_err(io)?;
                file.write_all(text.as_bytes()).map_err(io)?;
                file.sync_all().map_err(io)?;
            }
        }
        for event in needed {
            self.append_event(Event {
                seq: u64::try_from(self.events.len()).unwrap_or(u64::MAX),
                hash: String::new(),
                ..event.clone()
            })?;
        }
        Ok(notes)
    }

    /// Whether a version approved elsewhere would have been approved here: replaying events does not ask,
    /// and an import must not be a way around this directory's policy.
    fn passes_policy(&self, version: &Version) -> Result<(), RegistryError> {
        let policy = self.policy(&version.workflow);
        let denied = |why: String| {
            RegistryError::Denied(format!(
                "{} version {} would not be approved here: {why}",
                version.workflow, version.id
            ))
        };
        if policy.require_replay {
            replay_meets(policy, version).map_err(|e| denied(e.to_string()))?;
        }
        let valid = self
            .valid_approvals(version)
            .iter()
            .filter(|a| policy.author_may_approve || a.by != version.provenance.author)
            .filter(|a| policy.approvers.is_empty() || policy.approvers.contains(&a.by))
            .count();
        if valid < policy.approvals {
            return Err(denied(format!(
                "it has {valid} approvals that count, of {} needed",
                policy.approvals
            )));
        }
        Ok(())
    }

    /// Folds an event into the versions.
    fn apply(&mut self, event: &Event) -> Result<(), RegistryError> {
        let key = (event.workflow.clone(), event.version.clone());
        if let Action::Registered(provenance) = &event.action {
            if self.versions.contains_key(&key) {
                return Err(RegistryError::AlreadyRegistered(event.version.clone()));
            }
            self.versions.insert(
                key,
                Version {
                    workflow: event.workflow.clone(),
                    id: event.version.clone(),
                    state: State::Draft,
                    provenance: provenance.clone(),
                    replay: None,
                    approvals: Vec::new(),
                    registered_at_ms: event.at_ms,
                },
            );
            return Ok(());
        }
        let version = self
            .versions
            .get_mut(&key)
            .ok_or_else(|| RegistryError::UnknownVersion {
                workflow: event.workflow.clone(),
                version: event.version.clone(),
            })?;
        let (from, to): (&[State], Option<State>) = match &event.action {
            Action::Registered(_) => unreachable!("handled above"),
            Action::Proposed => (&[State::Draft], Some(State::Proposed)),
            Action::Proven(evidence) => {
                version.replay = Some(evidence.clone());
                (&[State::Proposed, State::Proven], Some(State::Proven))
            }
            Action::Approval {
                note,
                replay_sha256,
                signature,
            } => {
                // An approval names the report it approved; one without (written before reports
                // were recorded) opens, but no longer counts.
                let current = version.replay.as_ref().map(|r| &r.report_sha256);
                if replay_sha256.is_some() && replay_sha256.as_ref() != current {
                    return Err(RegistryError::Invalid(format!(
                        "an approval of version {} names a replay report it does not have",
                        version.id
                    )));
                }
                if let Some(signature) = signature {
                    let statement = approval_statement(
                        &event.workflow,
                        &event.version,
                        &event.by,
                        replay_sha256.as_deref(),
                        note.as_deref(),
                    );
                    if !signature
                        .key
                        .verifies(statement.as_bytes(), &signature.signature)
                    {
                        return Err(RegistryError::Invalid(format!(
                            "the signature on {}'s approval of version {} does not verify",
                            event.by, version.id
                        )));
                    }
                }
                version.approvals.push(Approval {
                    by: event.by.clone(),
                    at_ms: event.at_ms,
                    note: note.clone(),
                    replay_sha256: replay_sha256.clone(),
                    signature: signature.clone(),
                });
                (&[State::Proposed, State::Proven], None)
            }
            Action::Approved => (&[State::Proposed, State::Proven], Some(State::Approved)),
            Action::Activated => (&[State::Approved, State::Draining], Some(State::Active)),
            Action::Draining => (&[State::Active], Some(State::Draining)),
            Action::Retired => (
                &[
                    State::Draft,
                    State::Proposed,
                    State::Proven,
                    State::Approved,
                    State::Draining,
                ],
                Some(State::Retired),
            ),
        };
        if !from.contains(&version.state) {
            return Err(RegistryError::WrongState {
                version: version.id.clone(),
                state: version.state.as_str(),
                action: event.action.kind(),
            });
        }
        if let Some(to) = to {
            version.state = to;
        }
        Ok(())
    }
}

fn require(version: &Version, from: &[State], action: &'static str) -> Result<(), RegistryError> {
    if from.contains(&version.state) {
        Ok(())
    } else {
        Err(RegistryError::WrongState {
            version: version.id.clone(),
            state: version.state.as_str(),
            action,
        })
    }
}

fn chain_hash(prev: &str, body: &str) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(prev.as_bytes());
    hasher.update(b":");
    hasher.update(body.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn event_json(event: &Event) -> Value {
    let mut value = json!({
        "seq": event.seq,
        "at_ms": event.at_ms,
        "by": event.by,
        "workflow": event.workflow,
        "version": event.version,
        "action": event.action.kind(),
    });
    match &event.action {
        Action::Registered(p) => {
            value["author"] = json!(p.author);
            value["explored_by"] = json!(p.explored_by);
            value["cases_studied"] = json!(p.cases_studied);
            value["note"] = json!(p.note);
        }
        Action::Proven(e) => {
            value["runs"] = json!(e.runs);
            value["same"] = json!(e.same);
            value["different"] = json!(e.different);
            value["unresolved"] = json!(e.unresolved);
            value["report_sha256"] = json!(e.report_sha256);
        }
        Action::Approval {
            note,
            replay_sha256,
            signature,
        } => {
            value["note"] = json!(note);
            value["replay_sha256"] = json!(replay_sha256);
            if let Some(signature) = signature {
                value["key"] = json!(signature.key.to_string());
                value["signature"] = json!(signature.signature);
            }
        }
        _ => {}
    }
    value
}

fn parse_event(body: &str, hash: &str) -> Result<Event, String> {
    let value: Value = serde_json::from_str(body).map_err(|e| e.to_string())?;
    let text = |key: &str| {
        value[key]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| format!("`{key}` is missing"))
    };
    let opt_text = |key: &str| value[key].as_str().map(str::to_owned);
    let number = |key: &str| {
        value[key]
            .as_u64()
            .ok_or_else(|| format!("`{key}` is missing"))
    };
    let action = match text("action")?.as_str() {
        "registered" => Action::Registered(Provenance {
            author: text("author")?,
            explored_by: opt_text("explored_by"),
            cases_studied: value["cases_studied"].as_u64(),
            note: opt_text("note"),
        }),
        "proposed" => Action::Proposed,
        "proven" => Action::Proven(ReplayEvidence {
            runs: number("runs")?,
            same: number("same")?,
            different: number("different")?,
            unresolved: number("unresolved")?,
            report_sha256: text("report_sha256")?,
        }),
        "approval" => Action::Approval {
            note: opt_text("note"),
            replay_sha256: opt_text("replay_sha256"),
            signature: match (opt_text("key"), opt_text("signature")) {
                (Some(key), Some(signature)) => Some(Signature {
                    key: PublicKey::parse(&key).map_err(|e| e.to_string())?,
                    signature,
                }),
                (None, None) => None,
                _ => return Err("an approval has a key without a signature, or the reverse".into()),
            },
        },
        "approved" => Action::Approved,
        "activated" => Action::Activated,
        "draining" => Action::Draining,
        "retired" => Action::Retired,
        other => return Err(format!("unknown action `{other}`")),
    };
    Ok(Event {
        seq: number("seq")?,
        at_ms: number("at_ms")?,
        by: text("by")?,
        workflow: text("workflow")?,
        version: text("version")?,
        action,
        hash: hash.to_owned(),
    })
}

/// A line diff of two sources, in unified style without context limits: lines only in `old` start with
/// `-`, lines only in `new` with `+`, shared lines with a space.
pub fn diff(old: &str, new: &str) -> String {
    let (a, b): (Vec<&str>, Vec<&str>) = (old.lines().collect(), new.lines().collect());
    // Longest common subsequence, by dynamic programming: procedures are short.
    let mut lcs = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for i in (0..a.len()).rev() {
        for j in (0..b.len()).rev() {
            lcs[i][j] = if a[i] == b[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    let (mut i, mut j, mut out) = (0, 0, String::new());
    while i < a.len() || j < b.len() {
        if i < a.len() && j < b.len() && a[i] == b[j] {
            out.push_str(&format!("  {}\n", a[i]));
            i += 1;
            j += 1;
        } else if i < a.len() && (j == b.len() || lcs[i + 1][j] >= lcs[i][j + 1]) {
            // Removed lines before added ones, as diffs are usually read.
            out.push_str(&format!("- {}\n", a[i]));
            i += 1;
        } else {
            out.push_str(&format!("+ {}\n", b[j]));
            j += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merge_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("aqueduct-merge-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn activated(dir: &Path, workflow: &str, source: &str) -> String {
        let mut registry = Registry::open(dir).unwrap();
        let id = registry
            .register(
                workflow,
                source,
                Provenance {
                    author: "ada".into(),
                    ..Provenance::default()
                },
            )
            .unwrap();
        registry.propose(workflow, &id, "ada").unwrap();
        registry
            .prove(workflow, &id, "ada", ReplayEvidence::default())
            .unwrap();
        registry.approve(workflow, &id, "grace", None).unwrap();
        registry.activate(workflow, &id, "grace").unwrap();
        id
    }

    #[test]
    fn merging_takes_on_workflows_with_their_history() {
        let (from, to) = (merge_dir("from"), merge_dir("to"));
        let id = activated(&from, "a", "def run(x):\n    return x\n");
        activated(&to, "b", "def run(x):\n    return 1\n");
        let source = Registry::open(&from).unwrap();
        let mut target = Registry::open(&to).unwrap();
        target.merge(&source).unwrap();
        // It survives being read back: the log's chain holds, and so does the source's hash.
        let reopened = Registry::open(&to).unwrap();
        let version = reopened.version("a", &id).unwrap();
        assert_eq!(version.state, State::Active);
        assert_eq!(version.approvals[0].by, "grace");
        reopened.source(version).unwrap();
        assert!(reopened.active("b").is_some(), "what it had stays");
        let event = reopened
            .events()
            .iter()
            .find(|e| e.workflow == "a")
            .unwrap();
        let original = source.events().iter().find(|e| e.workflow == "a").unwrap();
        assert_eq!(event.at_ms, original.at_ms, "when, as it happened");
        // Merging again changes nothing.
        let before = reopened.events().len();
        Registry::open(&to).unwrap().merge(&source).unwrap();
        assert_eq!(Registry::open(&to).unwrap().events().len(), before);
        let _ = (std::fs::remove_dir_all(&from), std::fs::remove_dir_all(&to));
    }

    #[test]
    fn merging_refuses_a_workflow_with_another_history_and_changes_nothing() {
        let (from, to) = (merge_dir("from2"), merge_dir("to2"));
        activated(&from, "a", "def run(x):\n    return x\n");
        activated(&from, "c", "def run(x):\n    return 3\n");
        activated(&to, "a", "def run(x):\n    return 2\n");
        let before = Registry::open(&to).unwrap().events().len();
        let source = Registry::open(&from).unwrap();
        let refused = Registry::open(&to).unwrap().merge(&source).unwrap_err();
        assert!(refused.to_string().contains("`a`"), "{refused}");
        assert_eq!(Registry::open(&to).unwrap().events().len(), before);
        let _ = (std::fs::remove_dir_all(&from), std::fs::remove_dir_all(&to));
    }

    #[test]
    fn merging_refuses_what_this_directorys_policy_would_not_pass() {
        let (from, to) = (merge_dir("from3"), merge_dir("to3"));
        activated(&from, "a", "def run(x):\n    return x\n");
        std::fs::create_dir_all(to.join("registry")).unwrap();
        std::fs::write(
            to.join("registry/policy.json"),
            r#"{"default": {"approvals": 2}}"#,
        )
        .unwrap();
        let source = Registry::open(&from).unwrap();
        assert!(Registry::open(&to).unwrap().merge(&source).is_err());
        assert_eq!(Registry::open(&to).unwrap().events().len(), 0);
        let _ = (std::fs::remove_dir_all(&from), std::fs::remove_dir_all(&to));
    }

    const V1: &str = "def run(claim):\n    return step(\"score\", claim)\n";
    const V2: &str = "def run(claim):\n    score = step(\"score\", claim)\n    return score\n";

    fn temp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "aqueduct-registry-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn by(author: &str) -> Provenance {
        Provenance {
            author: author.into(),
            ..Provenance::default()
        }
    }

    fn clean(replay_runs: u64) -> ReplayEvidence {
        ReplayEvidence {
            runs: replay_runs,
            same: replay_runs,
            report_sha256: "ab".repeat(32),
            ..ReplayEvidence::default()
        }
    }

    #[test]
    fn a_version_goes_through_its_lifecycle() {
        let dir = temp_dir();
        let mut registry = Registry::open(&dir).unwrap();
        let v1 = registry.register("claims", V1, by("ada")).unwrap();
        assert_eq!(v1, version_of(V1));
        assert!(matches!(
            registry.register("claims", V1, by("ada")),
            Err(RegistryError::AlreadyRegistered(_))
        ));
        // Out of order: a draft cannot be approved or activated.
        assert!(matches!(
            registry.approve("claims", &v1, "grace", None),
            Err(RegistryError::WrongState { .. })
        ));
        registry.propose("claims", &v1[..8], "ada").unwrap();
        // Approval needs a replay report.
        assert!(matches!(
            registry.approve("claims", &v1, "grace", None),
            Err(RegistryError::WrongState { .. })
        ));
        registry.prove("claims", &v1, "ada", clean(12)).unwrap();
        // Four eyes: the author may not approve.
        assert!(matches!(
            registry.approve("claims", &v1, "ada", None),
            Err(RegistryError::Denied(_))
        ));
        assert_eq!(
            registry
                .approve("claims", &v1, "grace", Some("ok".into()))
                .unwrap(),
            State::Approved
        );
        registry.activate("claims", &v1, "grace").unwrap();
        assert_eq!(registry.active("claims").unwrap().id, v1);

        // A second version replaces it; the first drains, then retires once its runs are done.
        let v2 = registry.register("claims", V2, by("ada")).unwrap();
        registry.propose("claims", &v2, "ada").unwrap();
        registry.prove("claims", &v2, "ada", clean(12)).unwrap();
        registry.approve("claims", &v2, "grace", None).unwrap();
        registry.activate("claims", &v2, "grace").unwrap();
        assert_eq!(
            registry.version("claims", &v1).unwrap().state,
            State::Draining
        );
        assert!(matches!(
            registry.retire("claims", &v1, "grace", 3),
            Err(RegistryError::Denied(_))
        ));
        assert!(matches!(
            registry.retire("claims", &v2, "grace", 0),
            Err(RegistryError::WrongState { .. })
        ));
        registry.retire("claims", &v1, "grace", 0).unwrap();

        // Reopened, the log replays to the same state.
        let reopened = Registry::open(&dir).unwrap();
        assert_eq!(reopened.events().len(), registry.events().len());
        assert_eq!(
            reopened.versions().collect::<Vec<_>>(),
            registry.versions().collect::<Vec<_>>()
        );
        let v1 = reopened.version("claims", &v1).unwrap();
        assert_eq!(v1.state, State::Retired);
        assert_eq!(v1.approvals[0].by, "grace");
        assert_eq!(v1.approvals[0].note.as_deref(), Some("ok"));
        assert_eq!(reopened.source(v1).unwrap(), V1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_policy_decides_what_evidence_is_needed() {
        let dir = temp_dir();
        std::fs::create_dir_all(dir.join("registry")).unwrap();
        std::fs::write(
            dir.join("registry/policy.json"),
            r#"{"default": {"approvals": 2},
                "workflows": {"claims": {"approvers": ["grace", "linus"], "max_different": 1, "min_runs": 10},
                              "notes": {"require_replay": false, "approvals": 1, "author_may_approve": true}}}"#,
        )
        .unwrap();
        let mut registry = Registry::open(&dir).unwrap();
        let v1 = registry.register("claims", V1, by("ada")).unwrap();
        registry.propose("claims", &v1, "ada").unwrap();
        let mut evidence = clean(10);
        (evidence.same, evidence.different) = (8, 2);
        registry.prove("claims", &v1, "ada", evidence).unwrap();
        let denied = registry.approve("claims", &v1, "grace", None).unwrap_err();
        assert!(
            denied.to_string().contains("decides 2 runs differently"),
            "{denied}"
        );
        registry.prove("claims", &v1, "ada", clean(10)).unwrap();
        assert!(matches!(
            registry.approve("claims", &v1, "margaret", None),
            Err(RegistryError::Denied(_))
        ));
        // Two approvals are needed (the default), from the named approvers, once each.
        assert_eq!(
            registry.approve("claims", &v1, "grace", None).unwrap(),
            State::Proven
        );
        assert!(registry.approve("claims", &v1, "grace", None).is_err());
        assert_eq!(
            registry.approve("claims", &v1, "linus", None).unwrap(),
            State::Approved
        );

        // Another workflow's policy: no replay needed, one approval, the author may give it.
        let notes = registry.register("notes", V1, by("ada")).unwrap();
        registry.propose("notes", &notes, "ada").unwrap();
        assert_eq!(
            registry.approve("notes", &notes, "ada", None).unwrap(),
            State::Approved
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn an_edited_log_or_source_is_detected() {
        let dir = temp_dir();
        let mut registry = Registry::open(&dir).unwrap();
        let v1 = registry.register("claims", V1, by("ada")).unwrap();
        registry.propose("claims", &v1, "ada").unwrap();
        let log = dir.join("registry/events.log");
        let text = std::fs::read_to_string(&log).unwrap();
        std::fs::write(&log, text.replace("\"by\":\"ada\"", "\"by\":\"eve\"")).unwrap();
        assert!(matches!(
            Registry::open(&dir),
            Err(RegistryError::Tampered { line: 1, .. })
        ));
        std::fs::write(&log, text.lines().nth(1).unwrap()).unwrap();
        assert!(matches!(
            Registry::open(&dir),
            Err(RegistryError::Tampered { .. })
        ));
        std::fs::write(&log, &text).unwrap();
        let registry = Registry::open(&dir).unwrap();
        std::fs::write(dir.join(format!("registry/sources/{v1}.star")), V2).unwrap();
        let version = registry.version("claims", &v1).unwrap();
        assert!(matches!(
            registry.source(version),
            Err(RegistryError::Tampered { .. })
        ));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Rewrites the log with `edit` applied to every event's JSON, recomputing the whole chain: what
    /// someone who can write to the directory can do, and the chain alone cannot detect.
    fn rewrite_chain(dir: &Path, edit: impl Fn(&str) -> String) {
        let log = dir.join("registry/events.log");
        let text = std::fs::read_to_string(&log).unwrap();
        let mut prev = GENESIS.to_owned();
        let mut out = String::new();
        for line in text.lines() {
            let body = edit(line.split_once(' ').unwrap().1);
            prev = chain_hash(&prev, &body);
            out.push_str(&format!("{prev} {body}\n"));
        }
        std::fs::write(&log, out).unwrap();
    }

    fn sign(registry: &Registry, key: &SecretKey, workflow: &str, id: &str, by: &str) -> Signature {
        let statement = registry.statement(workflow, id, by, None).unwrap();
        Signature {
            key: key.public(),
            signature: key.sign(statement.as_bytes()),
        }
    }

    #[test]
    fn signed_approvals_are_checked_when_made_read_and_deployed() {
        let dir = temp_dir();
        std::fs::create_dir_all(dir.join("registry")).unwrap();
        std::fs::write(
            dir.join("registry/policy.json"),
            r#"{"default": {"require_signatures": true}}"#,
        )
        .unwrap();
        let grace = SecretKey::from_seed([1; 32]);
        let eve = SecretKey::from_seed([2; 32]);
        std::fs::write(
            dir.join("registry/identities.json"),
            format!(r#"{{"grace": ["{}"]}}"#, grace.public()),
        )
        .unwrap();
        let mut registry = Registry::open(&dir).unwrap();
        let v1 = registry.register("claims", V1, by("ada")).unwrap();
        registry.propose("claims", &v1, "ada").unwrap();
        registry.prove("claims", &v1, "ada", clean(3)).unwrap();

        // Unsigned, signed by someone else's key, or signed over something else: refused.
        let denied = |r: Result<State, RegistryError>| matches!(r, Err(RegistryError::Denied(_)));
        assert!(denied(registry.approve("claims", &v1, "grace", None)));
        let forged = sign(&registry, &eve, "claims", &v1, "grace");
        assert!(denied(registry.approve_signed(
            "claims",
            &v1,
            "grace",
            None,
            Some(forged)
        )));
        let elsewhere = sign(&registry, &grace, "claims", &v1, "grace");
        assert!(denied(registry.approve_signed(
            "claims",
            &v1,
            "grace",
            Some("with a note it did not sign".into()),
            Some(elsewhere.clone())
        )));
        assert_eq!(
            registry
                .approve_signed("claims", &v1, "grace", None, Some(elsewhere))
                .unwrap(),
            State::Approved
        );
        registry.activate("claims", &v1, "grace").unwrap();
        assert!(registry.deciders().is_ok());
        let reopened = Registry::open(&dir).unwrap();
        let approval = &reopened.version("claims", &v1).unwrap().approvals[0];
        assert!(reopened.trusted(approval));
        assert_eq!(
            approval.replay_sha256.as_deref(),
            Some("ab".repeat(32).as_str())
        );

        // A rewritten log keeps its chain, but not its signatures.
        let log = std::fs::read_to_string(dir.join("registry/events.log")).unwrap();
        rewrite_chain(&dir, |body| {
            body.replace("\"by\":\"grace\",", "\"by\":\"linus\",")
        });
        let err = Registry::open(&dir).unwrap_err();
        assert!(err.to_string().contains("does not verify"), "{err}");
        std::fs::write(dir.join("registry/events.log"), &log).unwrap();

        // If grace's key is no longer trusted, the active version is not deployed.
        let mut registry = Registry::open(&dir).unwrap();
        registry.set_identities(Identities::default().with("grace", eve.public()));
        let err = registry.deciders().unwrap_err();
        assert!(
            err.to_string().contains("0 valid signed approvals"),
            "{err}"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn approvals_count_only_for_the_report_they_approved() {
        let dir = temp_dir();
        std::fs::create_dir_all(dir.join("registry")).unwrap();
        std::fs::write(
            dir.join("registry/policy.json"),
            r#"{"default": {"approvals": 2}}"#,
        )
        .unwrap();
        let mut registry = Registry::open(&dir).unwrap();
        let v1 = registry.register("claims", V1, by("ada")).unwrap();
        registry.propose("claims", &v1, "ada").unwrap();
        registry.prove("claims", &v1, "ada", clean(3)).unwrap();
        registry.approve("claims", &v1, "grace", None).unwrap();
        // A new report: grace's approval was of the old one.
        let newer = ReplayEvidence {
            report_sha256: "cd".repeat(32),
            ..clean(4)
        };
        registry.prove("claims", &v1, "ada", newer).unwrap();
        assert_eq!(
            registry.approve("claims", &v1, "linus", None).unwrap(),
            State::Proven
        );
        assert_eq!(
            registry.approve("claims", &v1, "grace", None).unwrap(),
            State::Approved
        );
        assert!(Registry::open(&dir).is_ok());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn sources_are_diffed_by_line() {
        assert_eq!(
            diff(V1, V2),
            "  def run(claim):\n- \
             \x20   return step(\"score\", claim)\n+ \
             \x20   score = step(\"score\", claim)\n+ \
             \x20   return score\n"
        );
    }
}
