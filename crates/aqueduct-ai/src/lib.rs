//! Model providers for Aqueduct's agent steps (BRIEF.md section 12; ADR 0026).
//!
//! The engine knows nothing about models: an agent call reaches an [`AgentExecutor`]. This crate provides
//! one, [`AiExecutor`], which turns a call into a request to a [`Model`] and its answer into the
//! journaled response, and two adapters behind the same trait, so no provider is privileged:
//!
//! - [`anthropic::Anthropic`]: the Claude Messages API.
//! - [`openai::OpenAiCompatible`]: any OpenAI-compatible Chat Completions endpoint (hosted providers,
//!   and local servers such as Ollama or vLLM).
//!
//! ```no_run
//! use std::sync::Arc;
//! use aqueduct_ai::{AiExecutor, anthropic::Anthropic};
//!
//! let claude = Anthropic::from_env("claude-opus-5-5")?; // reads ANTHROPIC_API_KEY
//! let agents = AiExecutor::new(Arc::new(claude))
//!     .prompt("prompts/classify.md", "Classify the document: invoice, receipt or other.");
//! // Engine::builder(dir).agent_executor(Arc::new(agents)) ...
//! # Ok::<(), aqueduct_ai::AiError>(())
//! ```
//!
//! What an agent call asks for, as the SDK's `ctx.agent` and procedures' `agent()` send it: the request
//! is JSON `{"input": ..., "prompt": ..., "output_schema": ...}`. The prompt is looked up among the
//! registered prompts by name, or used as written. The call's token budget bounds the model's output
//! (`max_tokens`), after a conservative estimate of the input; the engine records a failure for any
//! answer that cost more than the budget, and checks the answer against the output schema (ADR 0025).

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::sync::Arc;

use aqueduct_core::{AgentResponse, Payload};
use aqueduct_engine::{AgentAttempt, AgentCall, AgentExecutor};
use serde_json::Value;

pub mod anthropic;
mod http;
pub mod openai;

/// A request to a model.
#[derive(Clone, Debug, PartialEq)]
pub struct CompletionRequest {
    /// Instructions for the model, if any.
    pub system: Option<String>,
    /// The user message: the prompt, the input, and what to answer with.
    pub prompt: String,
    /// The most tokens the answer may use (thinking included, where the model thinks).
    pub max_tokens: u64,
    /// The JSON Schema the answer must follow, if any. Adapters that support structured output pass it
    /// on; the engine checks the answer against it either way.
    pub output_schema: Option<Value>,
}

/// A model's answer.
#[derive(Clone, Debug, PartialEq)]
pub struct Completion {
    /// The answer's text.
    pub text: String,
    /// The model that answered, as `<provider>/<model>` (the model the provider reports, which can
    /// differ from the one requested, for example after a server-side fallback).
    pub model: String,
    /// The parameters the request was made with, journaled with the answer.
    pub params: Value,
    /// Tokens read.
    pub input_tokens: u64,
    /// Tokens written (thinking included, where the model thinks).
    pub output_tokens: u64,
}

/// Why a model call failed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ModelError {
    /// Worth retrying: rate limited, overloaded, a server or network error.
    #[error("{0}")]
    Retry(String),
    /// Not worth retrying: a bad request, an authentication error, a refusal, an answer cut off.
    #[error("{0}")]
    Fail(String),
}

/// A model behind a provider's API. Implementations block; they run on the engine's worker threads.
pub trait Model: Send + Sync {
    /// Asks the model once.
    fn complete(&self, request: &CompletionRequest) -> Result<Completion, ModelError>;
}

/// Errors setting up an adapter.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AiError {
    /// A required environment variable is not set.
    #[error("{0} is not set")]
    MissingEnv(&'static str),
    /// No certificate could be loaded from the operating system's store, so TLS cannot verify servers.
    #[error("no root certificates could be loaded from the operating system: {0}")]
    NoRootCertificates(String),
}

/// Answers the engine's agent calls with models: one by default, others by agent call name.
pub struct AiExecutor {
    default: Arc<dyn Model>,
    by_name: BTreeMap<String, Arc<dyn Model>>,
    prompts: BTreeMap<String, String>,
    system: Option<String>,
}

/// Characters per token assumed when estimating a request's input, on the safe side (most tokenizers
/// average about four characters per token for English text).
const CHARS_PER_TOKEN: usize = 3;

/// The fewest output tokens worth asking for.
const MIN_OUTPUT_TOKENS: u64 = 16;

impl AiExecutor {
    /// An executor that answers every agent call with `model`.
    pub fn new(model: Arc<dyn Model>) -> Self {
        AiExecutor {
            default: model,
            by_name: BTreeMap::new(),
            prompts: BTreeMap::new(),
            system: None,
        }
    }

    /// Answers agent calls named `name` with `model` instead of the default.
    #[must_use]
    pub fn model_for(mut self, name: &str, model: Arc<dyn Model>) -> Self {
        self.by_name.insert(name.to_owned(), model);
        self
    }

    /// Registers a prompt under a name (for example `prompts/classify.md`): a call whose prompt is
    /// that name gets this text.
    #[must_use]
    pub fn prompt(mut self, name: &str, text: &str) -> Self {
        self.prompts.insert(name.to_owned(), text.to_owned());
        self
    }

    /// Instructions sent as the system prompt with every call.
    #[must_use]
    pub fn system(mut self, text: &str) -> Self {
        self.system = Some(text.to_owned());
        self
    }

    /// The model request for an agent call, or why it cannot be made.
    pub fn request(&self, call: &AgentCall<'_>) -> Result<CompletionRequest, String> {
        let request: Value = serde_json::from_slice(call.request.as_bytes())
            .map_err(|e| format!("the agent call's request is not JSON: {e}"))?;
        let prompt_name = request.get("prompt").and_then(Value::as_str).unwrap_or("");
        let prompt = self
            .prompts
            .get(prompt_name)
            .map_or(prompt_name, String::as_str);
        let input = request.get("input").cloned().unwrap_or(Value::Null);
        let output_schema = request
            .get("output_schema")
            .filter(|schema| !schema.is_null())
            .cloned();
        let mut text = prompt.to_owned();
        text.push_str("\n\n<input>\n");
        text.push_str(&serde_json::to_string_pretty(&input).unwrap_or_default());
        text.push_str("\n</input>");
        if let Some(schema) = &output_schema {
            text.push_str(
                "\n\nAnswer with JSON that matches this JSON Schema, and nothing else:\n",
            );
            text.push_str(&serde_json::to_string(schema).unwrap_or_default());
        }
        let characters = text.len() + self.system.as_ref().map_or(0, String::len);
        let estimate = u64::try_from(characters / CHARS_PER_TOKEN + 16).unwrap_or(u64::MAX);
        let max_tokens = call.budget_tokens.saturating_sub(estimate);
        if max_tokens < MIN_OUTPUT_TOKENS {
            return Err(format!(
                "the budget of {} tokens leaves no room for an answer after about {estimate} input tokens",
                call.budget_tokens
            ));
        }
        Ok(CompletionRequest {
            system: self.system.clone(),
            prompt: text,
            max_tokens,
            output_schema,
        })
    }
}

/// The answer as JSON: the text itself when it is JSON (also inside a Markdown code fence), otherwise
/// the text as a JSON string.
pub fn answer_json(text: &str) -> Payload {
    let trimmed = text.trim();
    let unfenced = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```"))
        .and_then(|rest| rest.strip_suffix("```"))
        .map_or(trimmed, str::trim);
    match serde_json::from_str::<Value>(unfenced) {
        Ok(value) => Payload::new(value.to_string().into_bytes()),
        Err(_) => Payload::new(Value::String(text.to_owned()).to_string().into_bytes()),
    }
}

impl AgentExecutor for AiExecutor {
    fn call(&self, call: &AgentCall<'_>) -> AgentAttempt {
        let request = match self.request(call) {
            Ok(request) => request,
            Err(why) => return AgentAttempt::Fail(why),
        };
        let model = self.by_name.get(call.name).unwrap_or(&self.default);
        match model.complete(&request) {
            Ok(completion) => AgentAttempt::Completed(AgentResponse {
                model: completion.model,
                params: Payload::new(completion.params.to_string().into_bytes()),
                response: answer_json(&completion.text),
                input_tokens: completion.input_tokens,
                output_tokens: completion.output_tokens,
            }),
            Err(ModelError::Retry(why)) => AgentAttempt::Retry(why),
            Err(ModelError::Fail(why)) => AgentAttempt::Fail(why),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(request: &Payload, budget_tokens: u64) -> AgentCall<'_> {
        AgentCall {
            run: "r",
            name: "classify",
            request,
            budget_tokens,
        }
    }

    struct Echo;
    impl Model for Echo {
        fn complete(&self, request: &CompletionRequest) -> Result<Completion, ModelError> {
            Ok(Completion {
                text: format!("```json\n{{\"max\": {}}}\n```", request.max_tokens),
                model: "test/echo".into(),
                params: serde_json::json!({"max_tokens": request.max_tokens}),
                input_tokens: 10,
                output_tokens: 5,
            })
        }
    }

    #[test]
    fn a_call_becomes_a_request_within_its_budget() {
        let executor = AiExecutor::new(Arc::new(Echo))
            .prompt("prompts/classify.md", "Classify it.")
            .system("Be brief.");
        let request = Payload::new(
            br#"{"input":{"doc":"d1"},"prompt":"prompts/classify.md","output_schema":{"type":"string"}}"#
                .to_vec(),
        );
        let built = executor.request(&call(&request, 1_000)).unwrap();
        assert!(built.prompt.starts_with("Classify it.\n\n<input>\n"));
        assert!(built.prompt.contains("\"doc\": \"d1\""));
        assert!(built.prompt.contains("JSON Schema"));
        assert_eq!(built.system.as_deref(), Some("Be brief."));
        assert_eq!(
            built.output_schema,
            Some(serde_json::json!({"type": "string"}))
        );
        assert!(built.max_tokens < 1_000 && built.max_tokens > 900);
        // Too small a budget fails the call before any model is asked.
        assert!(executor.request(&call(&request, 50)).is_err());
        let AgentAttempt::Completed(response) = executor.call(&call(&request, 1_000)) else {
            panic!("the call completes");
        };
        assert_eq!(response.model, "test/echo");
        assert_eq!(
            response.response,
            Payload::new(format!("{{\"max\":{}}}", built.max_tokens).into_bytes())
        );
    }

    #[test]
    fn answers_become_json() {
        assert_eq!(answer_json(" {\"a\": 1} ").as_bytes(), b"{\"a\":1}");
        assert_eq!(answer_json("```\n[1,2]\n```").as_bytes(), b"[1,2]");
        assert_eq!(answer_json("plain words").as_bytes(), b"\"plain words\"");
    }
}
