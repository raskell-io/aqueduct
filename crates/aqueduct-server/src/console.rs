//! The web console (ADR 0059): one page, served by the server at `/console`, that shows runs, their
//! commands, open escalations and deployments, and cancels, signals and resolves. It calls the server's
//! HTTP/JSON API with the token the person enters, so it can do no more than that token may. The page
//! holds no secrets and is served without authentication.

use axum::Router;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;

/// The page, built into the binary.
const PAGE: &str = include_str!("console.html");

/// What the page may load and run: only itself, no frames, nothing elsewhere.
const POLICY: &str = "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; \
                      connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'";

pub(crate) fn routes() -> Router {
    Router::new().route("/console", get(page))
}

async fn page() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CONTENT_SECURITY_POLICY, POLICY),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::REFERRER_POLICY, "no-referrer"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        PAGE,
    )
}
