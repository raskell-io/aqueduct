# 0026: Provider adapters in `aqueduct-ai`

- **Status:** accepted
- **Date:** 2026-11-01

## Context

Agent steps are in the core (ADR 0023), and the engine runs them through an `AgentExecutor` hook with no
provider code. BRIEF.md section 12 puts provider adapters, prompts and the like in `aqueduct-ai`, a crate
the engine does not depend on. An application needs one executor that turns an agent call's request
(`{"input", "prompt", "output_schema"}`) into a model request, and turns the answer into a journaled
`AgentResponse`. No provider should be privileged by the engine, and the crate should not pull a large
async stack into an engine that is otherwise synchronous.

## Decision

- **One trait, two adapters.** `Model::complete(&CompletionRequest) -> Result<Completion, ModelError>`;
  `ModelError` is `Retry` or `Fail`, which map onto the executor's `AgentAttempt`. `anthropic::Anthropic`
  speaks the Claude Messages API; `openai::OpenAiCompatible` speaks Chat Completions, which covers hosted
  providers that offer it and local servers (Ollama, vLLM). Other providers implement `Model`.
- **`AiExecutor` is the `AgentExecutor`.** It has a default model, other models by agent call name, prompts
  registered by name (so a call's `prompt: "prompts/classify.md"` resolves to text), and an optional system
  prompt. The user message is the prompt, the input in an `<input>` block, and the output schema when
  there is one.
- **The budget caps the output.** `max_tokens` is the call's `budget_tokens` minus a conservative estimate
  of the input (three characters a token). A budget too small for any answer fails the call before a
  model is asked. The core still rejects any answer that cost more than its budget (ADR 0023).
- **The recorded model is the one that answered**, as `<provider>/<model>` from the response, which can
  differ from the one requested (for example after a server-side fallback). The request parameters are
  recorded with it.
- **Claude specifics.** API version `2026-10-21`. No sampling parameters (that version rejects them). An
  output schema is sent as structured output, with the keywords it does not support removed and objects
  closed; the engine checks the full schema. On the models that support it, refusals fall back
  server-side (`fallbacks: "default"`). A refusal that stands, or an answer cut off at `max_tokens`, fails
  the call. Only `text` blocks make the answer.
- **Retry or fail.** HTTP 408, 429 and 5xx (529 included) and network errors are retried under the call's
  retry policy; other errors, refusals and cut-off answers are final.
- **HTTP is blocking**: `ureq` on the engine's worker threads, with `rustls`, the `ring` provider, and the
  operating system's root certificates (`rustls-native-certs`). A bundled root store is avoided: the usual
  ones (`webpki-roots`, `webpki-root-certs`) are under CDLA-Permissive-2.0, which is not on the licence
  allowlist.
- **Tests never call a provider.** The adapters are tested against a local HTTP server for the request
  each one sends and the way it reads every kind of reply, and end to end through the engine.

## Consequences

- An application wires a provider in a few lines (`Engine::builder(dir).agent_executor(Arc::new(
  AiExecutor::new(Arc::new(Anthropic::from_env("claude-opus-5-5")?))))`), and can mix providers by agent
  call name.
- Answers are not streamed. Output is capped at 16,000 tokens a request to stay clear of HTTP timeouts.
- Prompt caching, tool use inside an agent call, and streaming are not here yet; each can be added inside
  an adapter without changing the core.
- A host with no root certificates in its store cannot build an adapter (`AiError::NoRootCertificates`)
  rather than silently skipping verification.
