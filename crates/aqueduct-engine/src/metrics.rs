//! Per-version metrics (BRIEF.md sections 3 and 11; ADR 0031): what each version of each workflow did,
//! folded from its runs' journals. Runs pinned to a version (ADR 0029) are counted under it; runs
//! without one, under the workflow alone.
//!
//! The same numbers are live, as OpenTelemetry metrics with an `aqueduct.version` attribute, from
//! `aqueduct-otel`; these are computed from the record, so they cover every run the data directory
//! holds, from before any collector was attached.

use std::collections::BTreeMap;

use aqueduct_core::{
    BUDGET_ESCALATION, CommandKind, Entry, RunState, SCHEMA_ESCALATION, Status, StepOutcome,
    TIMED_OUT,
};

/// What one version of a workflow did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VersionMetrics {
    /// The workflow.
    pub workflow: String,
    /// The version, if its runs are pinned to one.
    pub version: Option<String>,
    /// Runs on it.
    pub runs: u64,
    /// Runs that have not ended (a draining version with runs that never end is a zombie).
    pub running: u64,
    /// Runs that completed.
    pub completed: u64,
    /// Runs that failed (cancelled ones included).
    pub failed: u64,
    /// Runs that were parked: their logic diverged from their journal.
    pub parked: u64,
    /// Runs that continued as new.
    pub continued: u64,
    /// Runs that were asked to stop.
    pub cancelled: u64,
    /// Runs that escalated at least once.
    pub escalated_runs: u64,
    /// Escalations, by reason (`budget` and `schema` for agent calls).
    pub escalations: BTreeMap<String, u64>,
    /// Runs in which a signal wait timed out.
    pub timed_out_runs: u64,
    /// Steps scheduled.
    pub steps: u64,
    /// Attempts retried (steps and agent calls).
    pub retries: u64,
    /// Agent calls made.
    pub agent_calls: u64,
    /// Tokens read by answered agent calls.
    pub input_tokens: u64,
    /// Tokens written by answered agent calls.
    pub output_tokens: u64,
}

impl VersionMetrics {
    /// The share of runs that escalated, from 0 to 1.
    pub fn escalation_rate(&self) -> f64 {
        ratio(self.escalated_runs, self.runs)
    }

    /// The share of runs in which a signal wait timed out, from 0 to 1.
    pub fn timeout_rate(&self) -> f64 {
        ratio(self.timed_out_runs, self.runs)
    }

    /// Model tokens per run, on average.
    pub fn tokens_per_run(&self) -> f64 {
        ratio(self.input_tokens + self.output_tokens, self.runs)
    }

    fn add(&mut self, state: &RunState, journal: &[Entry]) {
        self.runs += 1;
        match state.status() {
            Status::Running => self.running += 1,
            Status::Completed => self.completed += 1,
            Status::Failed => self.failed += 1,
            Status::Parked => self.parked += 1,
            Status::Continued => self.continued += 1,
        }
        let mut escalated = false;
        for entry in journal {
            let reason = match entry {
                Entry::Escalated { reason, .. } => Some(reason.as_str()),
                Entry::AgentCallEscalated { .. } => Some(BUDGET_ESCALATION),
                Entry::AgentAnswerEscalated { .. } => Some(SCHEMA_ESCALATION),
                _ => None,
            };
            if let Some(reason) = reason {
                escalated = true;
                *self.escalations.entry(reason.to_owned()).or_default() += 1;
            }
            match entry {
                Entry::StepScheduled { .. } => self.steps += 1,
                Entry::StepRetryScheduled { .. } => self.retries += 1,
                Entry::AgentCallScheduled { .. } | Entry::AgentCallEscalated { .. } => {
                    self.agent_calls += 1;
                }
                Entry::AgentCallCompleted { response, .. } => {
                    self.input_tokens += response.input_tokens;
                    self.output_tokens += response.output_tokens;
                }
                Entry::CancelRequested { .. } => self.cancelled += 1,
                _ => {}
            }
        }
        if escalated {
            self.escalated_runs += 1;
        }
        let timed_out = state.steps().iter().any(|step| {
            matches!(step.kind, CommandKind::Signal { .. })
                && matches!(&step.outcome, Some(StepOutcome::Failed(e)) if e == TIMED_OUT)
        });
        if timed_out {
            self.timed_out_runs += 1;
        }
    }
}

fn ratio(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        // Counts stay far below 2^52, so the conversion is exact.
        #[allow(clippy::cast_precision_loss)]
        let (part, whole) = (part as f64, whole as f64);
        part / whole
    }
}

/// Metrics per workflow and version, from runs and their journals, ordered by workflow, then version
/// (unversioned runs first).
pub fn version_metrics<'a>(
    runs: impl IntoIterator<Item = (&'a RunState, &'a [Entry])>,
) -> Vec<VersionMetrics> {
    let mut by_version: BTreeMap<(String, Option<String>), VersionMetrics> = BTreeMap::new();
    for (state, journal) in runs {
        let key = (
            state.workflow().to_owned(),
            state.version().map(str::to_owned),
        );
        by_version
            .entry(key.clone())
            .or_insert_with(|| VersionMetrics {
                workflow: key.0,
                version: key.1,
                ..VersionMetrics::default()
            })
            .add(state, journal);
    }
    by_version.into_values().collect()
}

#[cfg(test)]
mod tests {
    use aqueduct_core::{AgentResponse, CommandIndex, Payload};

    use super::*;

    fn payload(text: &str) -> Payload {
        Payload::new(text.as_bytes().to_vec())
    }

    fn started(version: Option<&str>) -> Entry {
        Entry::Started {
            workflow: "claims".into(),
            input: payload("{}"),
            budget_tokens: None,
            version: version.map(str::to_owned),
        }
    }

    #[test]
    fn runs_are_counted_under_their_version() {
        let escalated = vec![
            started(Some("v1")),
            Entry::Escalated {
                cmd: CommandIndex(0),
                reason: "manual-review".into(),
                payload: payload("{}"),
            },
            Entry::EscalationResolved {
                cmd: CommandIndex(0),
                outcome: payload("true"),
            },
            Entry::AgentCallScheduled {
                cmd: CommandIndex(1),
                name: "classify".into(),
                request: payload("{}"),
                budget_tokens: 100,
            },
            Entry::StepRetryScheduled {
                cmd: CommandIndex(1),
                attempt: 1,
                error: "429".into(),
                wake_at: 5,
            },
            Entry::AgentCallCompleted {
                cmd: CommandIndex(1),
                response: AgentResponse {
                    model: "m".into(),
                    params: payload("{}"),
                    response: payload("1"),
                    input_tokens: 30,
                    output_tokens: 10,
                },
            },
            Entry::RunCompleted {
                output: payload("1"),
            },
        ];
        let timed_out = vec![
            started(Some("v1")),
            Entry::SignalAwaited {
                cmd: CommandIndex(0),
                name: "photos".into(),
                timeout_ms: 10,
                wake_at: 10,
            },
            Entry::TimerFired {
                cmd: CommandIndex(0),
            },
            Entry::RunFailed {
                error: "no photos".into(),
            },
        ];
        let running = vec![
            started(Some("v2")),
            Entry::StepScheduled {
                cmd: CommandIndex(0),
                name: "score".into(),
                input: payload("{}"),
            },
        ];
        let unversioned = vec![started(None)];
        let journals = [escalated, timed_out, running, unversioned];
        let states: Vec<RunState> = journals
            .iter()
            .map(|j| RunState::from_journal(j).unwrap())
            .collect();
        let metrics = version_metrics(states.iter().zip(journals.iter().map(Vec::as_slice)));
        let versions: Vec<_> = metrics.iter().map(|m| m.version.as_deref()).collect();
        assert_eq!(versions, [None, Some("v1"), Some("v2")]);
        let v1 = &metrics[1];
        assert_eq!((v1.runs, v1.completed, v1.failed), (2, 1, 1));
        assert_eq!(
            v1.escalations,
            BTreeMap::from([("manual-review".to_owned(), 1)])
        );
        assert_eq!((v1.escalated_runs, v1.timed_out_runs), (1, 1));
        assert!((v1.escalation_rate() - 0.5).abs() < f64::EPSILON);
        assert!((v1.timeout_rate() - 0.5).abs() < f64::EPSILON);
        assert_eq!((v1.agent_calls, v1.retries), (1, 1));
        assert!((v1.tokens_per_run() - 20.0).abs() < f64::EPSILON);
        let v2 = &metrics[2];
        assert_eq!((v2.runs, v2.running, v2.steps), (1, 1, 1));
        assert!(v2.escalation_rate().abs() < f64::EPSILON);
    }
}
