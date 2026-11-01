//! Agent calls: the hook through which an engine calls models (BRIEF.md section 12). The engine knows
//! nothing about providers; `aqueduct-ai` implements [`AgentExecutor`] for them.

use aqueduct_core::{AgentResponse, Payload};

/// An agent call to execute.
#[derive(Clone, Copy, Debug)]
pub struct AgentCall<'a> {
    /// The run's run id (stable across retries; useful for idempotency and tracing).
    pub run: &'a str,
    /// The agent call's name, as the workflow or procedure gave it.
    pub name: &'a str,
    /// The request (JSON: input, prompt, output schema).
    pub request: &'a Payload,
    /// The most tokens the call may use. A response over it is recorded as a failure.
    pub budget_tokens: u64,
}

/// What an agent call came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentAttempt {
    /// The model answered: the response with its model identity and usage, journaled as is.
    Completed(AgentResponse),
    /// A retryable failure (rate limited, timeout, provider unavailable): retried by the retry policy.
    Retry(String),
    /// A terminal failure: the agent call fails.
    Fail(String),
}

/// Executes agent calls (calls models). Called on worker threads; may block.
pub trait AgentExecutor: Send + Sync {
    /// Executes one call.
    fn call(&self, call: &AgentCall<'_>) -> AgentAttempt;
}
