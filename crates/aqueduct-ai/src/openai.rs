//! Any OpenAI-compatible Chat Completions endpoint (`POST {base}/chat/completions`): hosted providers
//! that offer it, and local servers such as Ollama (`http://localhost:11434/v1`) or vLLM. For Claude,
//! use [`crate::anthropic::Anthropic`].

use std::time::Duration;

use serde_json::{Value, json};

use crate::http::{Reply, agent, post_json, retryable};
use crate::{AiError, Completion, CompletionRequest, Model, ModelError};

/// A model behind an OpenAI-compatible Chat Completions endpoint.
pub struct OpenAiCompatible {
    agent: ureq::Agent,
    base_url: String,
    api_key: Option<String>,
    model: String,
    provider: String,
    json_schema: bool,
    max_tokens_field: String,
}

impl OpenAiCompatible {
    /// An adapter for `model` at `base_url` (for example `http://localhost:11434/v1`).
    pub fn new(base_url: &str, model: &str) -> Result<Self, AiError> {
        Ok(OpenAiCompatible {
            agent: agent(Duration::from_secs(600))?,
            base_url: base_url.trim_end_matches('/').to_owned(),
            api_key: None,
            model: model.to_owned(),
            provider: "openai-compatible".into(),
            json_schema: true,
            max_tokens_field: "max_tokens".into(),
        })
    }

    /// Sends this API key as a bearer token.
    #[must_use]
    pub fn api_key(mut self, key: &str) -> Self {
        self.api_key = Some(key.to_owned());
        self
    }

    /// The provider name recorded with every answer (`<provider>/<model>`; default
    /// `openai-compatible`), for example `ollama`.
    #[must_use]
    pub fn provider(mut self, name: &str) -> Self {
        self.provider = name.to_owned();
        self
    }

    /// Does not send `response_format` (for servers that do not support JSON Schema output); the
    /// schema is still in the prompt, and the engine still checks the answer.
    #[must_use]
    pub fn without_json_schema(mut self) -> Self {
        self.json_schema = false;
        self
    }

    /// The request field for the output limit (`max_tokens` by default; some providers want
    /// `max_completion_tokens`).
    #[must_use]
    pub fn max_tokens_field(mut self, field: &str) -> Self {
        self.max_tokens_field = field.to_owned();
        self
    }

    /// The request body for a completion request.
    pub fn body(&self, request: &CompletionRequest) -> Value {
        let mut messages = Vec::new();
        if let Some(system) = &request.system {
            messages.push(json!({"role": "system", "content": system}));
        }
        messages.push(json!({"role": "user", "content": request.prompt}));
        let mut body = json!({"model": self.model, "messages": messages});
        body[&self.max_tokens_field] = json!(request.max_tokens);
        if let (true, Some(schema)) = (self.json_schema, &request.output_schema) {
            body["response_format"] = json!({
                "type": "json_schema",
                "json_schema": {"name": "answer", "schema": schema},
            });
        }
        body
    }

    fn read(&self, request: &CompletionRequest, reply: Reply) -> Result<Completion, ModelError> {
        let Reply { status, body } = reply;
        if status != 200 {
            let message = body["error"]["message"]
                .as_str()
                .or_else(|| body["error"].as_str())
                .unwrap_or("no message");
            let why = format!("{}: HTTP {status}: {message}", self.provider);
            return Err(if retryable(status) {
                ModelError::Retry(why)
            } else {
                ModelError::Fail(why)
            });
        }
        let choice = &body["choices"][0];
        match choice["finish_reason"].as_str() {
            Some("length") => {
                return Err(ModelError::Fail(format!(
                    "{}: the answer was cut off at {} output tokens; raise the call's budget_tokens",
                    self.provider, request.max_tokens
                )));
            }
            Some("content_filter") => {
                return Err(ModelError::Fail(format!(
                    "{}: the answer was withheld by the provider's content filter",
                    self.provider
                )));
            }
            _ => {}
        }
        let Some(text) = choice["message"]["content"].as_str() else {
            return Err(ModelError::Fail(format!(
                "{}: the response has no answer text",
                self.provider
            )));
        };
        let answered_by = body["model"].as_str().unwrap_or(&self.model);
        Ok(Completion {
            text: text.to_owned(),
            model: format!("{}/{answered_by}", self.provider),
            params: json!({
                &self.max_tokens_field: request.max_tokens,
                "json_schema": self.json_schema && request.output_schema.is_some(),
            }),
            input_tokens: body["usage"]["prompt_tokens"].as_u64().unwrap_or(0),
            output_tokens: body["usage"]["completion_tokens"].as_u64().unwrap_or(0),
        })
    }
}

impl Model for OpenAiCompatible {
    fn complete(&self, request: &CompletionRequest) -> Result<Completion, ModelError> {
        let headers: Vec<(&str, String)> = self
            .api_key
            .iter()
            .map(|key| ("authorization", format!("Bearer {key}")))
            .collect();
        let url = format!("{}/chat/completions", self.base_url);
        let reply = post_json(&self.agent, &url, &headers, &self.body(request))?;
        self.read(request, reply)
    }
}
