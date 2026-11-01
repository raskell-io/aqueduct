//! The Claude Messages API (`POST /v1/messages`, API version `2026-10-21`).
//!
//! - Sampling parameters (`temperature`, `top_p`, `top_k`) are not sent: version `2026-10-21` rejects
//!   them.
//! - An output schema is sent as structured output (`output_config.format`, type `json_schema`), with
//!   the keywords structured output does not support removed; the engine still checks the answer
//!   against the full schema (ADR 0025).
//! - On the models that support it, a declined request is re-run server-side on the model Anthropic
//!   recommends for the refusal category (`fallbacks: "default"`); the answer records the model that
//!   actually answered. A refusal that still stands fails the call.
//! - Current models think before answering: the answer is the response's `text` blocks, and the output
//!   tokens (thinking included) count against the call's budget.

use std::time::Duration;

use serde_json::{Value, json};

use crate::http::{Reply, agent, post_json, retryable};
use crate::{AiError, Completion, CompletionRequest, Model, ModelError};

/// The API version sent with every request.
pub const API_VERSION: &str = "2026-10-21";

/// The beta that enables `fallbacks: "default"`.
const FALLBACK_BETA: &str = "server-side-fallback-2026-07-01";

/// Models that accept `fallbacks: "default"` on the Claude API.
const FALLBACK_MODELS: &[&str] = &[
    "claude-fable-5-1",
    "claude-opus-5-5",
    "claude-opus-5",
    "claude-sonnet-5-5",
];

/// The most output tokens asked for in one non-streaming request, to stay clear of HTTP timeouts.
const MAX_OUTPUT_TOKENS: u64 = 16_000;

/// A Claude model through the Messages API.
pub struct Anthropic {
    agent: ureq::Agent,
    base_url: String,
    api_key: String,
    model: String,
    fallbacks: bool,
    effort: Option<String>,
}

impl Anthropic {
    /// An adapter for `model` (for example `claude-opus-5-5`) with this API key.
    pub fn new(api_key: &str, model: &str) -> Result<Self, AiError> {
        Ok(Anthropic {
            agent: agent(Duration::from_secs(600))?,
            base_url: "https://api.anthropic.com".into(),
            api_key: api_key.to_owned(),
            model: model.to_owned(),
            fallbacks: true,
            effort: None,
        })
    }

    /// An adapter with the API key from `ANTHROPIC_API_KEY`.
    pub fn from_env(model: &str) -> Result<Self, AiError> {
        let key = std::env::var("ANTHROPIC_API_KEY")
            .map_err(|_| AiError::MissingEnv("ANTHROPIC_API_KEY"))?;
        Anthropic::new(&key, model)
    }

    /// Sends requests to another base URL (a gateway, or a test server).
    #[must_use]
    pub fn base_url(mut self, url: &str) -> Self {
        self.base_url = url.trim_end_matches('/').to_owned();
        self
    }

    /// Turns server-side refusal fallbacks off (on by default where the model supports them).
    #[must_use]
    pub fn without_fallbacks(mut self) -> Self {
        self.fallbacks = false;
        self
    }

    /// Sets the effort (`low`, `medium`, `high`, `xhigh`, `max`): how much the model thinks, and so how
    /// many of the call's tokens it spends. Unset, the model's default applies.
    #[must_use]
    pub fn effort(mut self, effort: &str) -> Self {
        self.effort = Some(effort.to_owned());
        self
    }

    /// The request body for a completion request.
    pub fn body(&self, request: &CompletionRequest) -> Value {
        let mut body = json!({
            "model": self.model,
            "max_tokens": request.max_tokens.min(MAX_OUTPUT_TOKENS),
            "messages": [{"role": "user", "content": request.prompt}],
        });
        if let Some(system) = &request.system {
            body["system"] = json!(system);
        }
        let mut output_config = serde_json::Map::new();
        if let Some(schema) = &request.output_schema {
            output_config.insert(
                "format".into(),
                json!({"type": "json_schema", "schema": structured_schema(schema)}),
            );
        }
        if let Some(effort) = &self.effort {
            output_config.insert("effort".into(), json!(effort));
        }
        if !output_config.is_empty() {
            body["output_config"] = Value::Object(output_config);
        }
        if self.uses_fallbacks() {
            body["fallbacks"] = json!("default");
        }
        body
    }

    fn uses_fallbacks(&self) -> bool {
        self.fallbacks && FALLBACK_MODELS.contains(&self.model.as_str())
    }

    fn read(&self, request: &CompletionRequest, reply: Reply) -> Result<Completion, ModelError> {
        let Reply { status, body } = reply;
        if status != 200 {
            let kind = body["error"]["type"].as_str().unwrap_or("error");
            let message = body["error"]["message"].as_str().unwrap_or("no message");
            let why = format!("anthropic: HTTP {status} {kind}: {message}");
            return Err(if retryable(status) {
                ModelError::Retry(why)
            } else {
                ModelError::Fail(why)
            });
        }
        match body["stop_reason"].as_str() {
            Some("refusal") => {
                let category = body["stop_details"]["category"].as_str().unwrap_or("none");
                return Err(ModelError::Fail(format!(
                    "anthropic: the model declined (category {category})"
                )));
            }
            Some("max_tokens") => {
                return Err(ModelError::Fail(format!(
                    "anthropic: the answer was cut off at {} output tokens; raise the call's budget_tokens",
                    request.max_tokens.min(MAX_OUTPUT_TOKENS)
                )));
            }
            _ => {}
        }
        let text: String = body["content"]
            .as_array()
            .map(|blocks| {
                blocks
                    .iter()
                    .filter(|block| block["type"] == "text")
                    .filter_map(|block| block["text"].as_str())
                    .collect()
            })
            .unwrap_or_default();
        let answered_by = body["model"].as_str().unwrap_or(&self.model);
        let mut params = json!({
            "max_tokens": request.max_tokens.min(MAX_OUTPUT_TOKENS),
            "structured_output": request.output_schema.is_some(),
        });
        if self.uses_fallbacks() {
            params["fallbacks"] = json!("default");
        }
        if let Some(effort) = &self.effort {
            params["effort"] = json!(effort);
        }
        Ok(Completion {
            text,
            model: format!("anthropic/{answered_by}"),
            params,
            input_tokens: body["usage"]["input_tokens"].as_u64().unwrap_or(0),
            output_tokens: body["usage"]["output_tokens"].as_u64().unwrap_or(0),
        })
    }
}

impl Model for Anthropic {
    fn complete(&self, request: &CompletionRequest) -> Result<Completion, ModelError> {
        let mut headers = vec![
            ("x-api-key", self.api_key.clone()),
            ("anthropic-version", API_VERSION.to_owned()),
        ];
        if self.uses_fallbacks() {
            headers.push(("anthropic-beta", FALLBACK_BETA.to_owned()));
        }
        let url = format!("{}/v1/messages", self.base_url);
        let reply = post_json(&self.agent, &url, &headers, &self.body(request))?;
        self.read(request, reply)
    }
}

/// Keywords structured output does not support, removed from the schema sent to the model (the engine
/// checks the full schema).
const UNSUPPORTED: &[&str] = &[
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "minLength",
    "maxLength",
    "minItems",
    "maxItems",
    "uniqueItems",
    "contains",
    "minContains",
    "maxContains",
    "minProperties",
    "maxProperties",
];

/// The schema as structured output accepts it: unsupported keywords removed, and every object closed
/// (`additionalProperties: false`, which structured output requires).
pub fn structured_schema(schema: &Value) -> Value {
    match schema {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (key, value) in map {
                if UNSUPPORTED.contains(&key.as_str()) || key == "additionalProperties" {
                    continue;
                }
                out.insert(key.clone(), structured_schema(value));
            }
            let is_object =
                map.get("type") == Some(&json!("object")) || map.contains_key("properties");
            if is_object {
                out.insert("additionalProperties".into(), json!(false));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(structured_schema).collect()),
        other => other.clone(),
    }
}
