//! HTTP for the adapters: blocking, rustls with the `ring` provider, and the operating system's root
//! certificates (no bundled root store).

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use ureq::tls::{Certificate, RootCerts, TlsConfig, TlsProvider};

use crate::{AiError, ModelError};

/// An HTTP agent that verifies servers against the operating system's root certificates.
pub(crate) fn agent(timeout: Duration) -> Result<ureq::Agent, AiError> {
    let loaded = rustls_native_certs::load_native_certs();
    if loaded.certs.is_empty() {
        let why = loaded
            .errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        return Err(AiError::NoRootCertificates(why));
    }
    let roots: Vec<Certificate<'static>> = loaded
        .certs
        .iter()
        .map(|der| Certificate::from_der(der.as_ref()).to_owned())
        .collect();
    let tls = TlsConfig::builder()
        .provider(TlsProvider::Rustls)
        .unversioned_rustls_crypto_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .root_certs(RootCerts::new_with_certs(&roots))
        .build();
    Ok(ureq::Agent::config_builder()
        .tls_config(tls)
        .http_status_as_error(false)
        .timeout_global(Some(timeout))
        .build()
        .new_agent())
}

/// What came back from a POST: the status and the body parsed as JSON (`Null` if it was not JSON).
pub(crate) struct Reply {
    pub status: u16,
    pub body: Value,
}

/// POSTs `body` as JSON. Network errors and timeouts are worth retrying.
pub(crate) fn post_json(
    agent: &ureq::Agent,
    url: &str,
    headers: &[(&str, String)],
    body: &Value,
) -> Result<Reply, ModelError> {
    let mut request = agent.post(url).header("content-type", "application/json");
    for (name, value) in headers {
        request = request.header(*name, value);
    }
    let mut response = request
        .send(body.to_string())
        .map_err(|e| ModelError::Retry(format!("request to {url} failed: {e}")))?;
    let status = response.status().as_u16();
    let text = response
        .body_mut()
        .read_to_string()
        .map_err(|e| ModelError::Retry(format!("reading the response from {url} failed: {e}")))?;
    Ok(Reply {
        status,
        body: serde_json::from_str(&text).unwrap_or(Value::Null),
    })
}

/// Whether an HTTP status is worth retrying: timeouts, rate limits, overload and server errors.
pub(crate) fn retryable(status: u16) -> bool {
    status == 408 || status == 429 || status >= 500
}
