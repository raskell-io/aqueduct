//! TLS on the server's listener (ADR 0042): rustls on ring (ADR 0038), offering HTTP/2 (for gRPC) and
//! HTTP/1.1. A certificate and key are read when the server starts; replacing them needs a restart.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// How long a client has to finish its TLS handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long connections have to finish when the server stops.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// A certificate and key to serve with.
#[derive(Clone)]
pub struct Tls {
    config: Arc<rustls::ServerConfig>,
}

impl std::fmt::Debug for Tls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never prints the key.
        f.write_str("Tls")
    }
}

impl Tls {
    /// Reads a certificate chain and its private key from PEM files.
    pub fn from_files(certificate: &Path, key: &Path) -> Result<Tls, String> {
        let certificates: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(certificate)
            .and_then(Iterator::collect)
            .map_err(|e| format!("{}: {e}", certificate.display()))?;
        let key =
            PrivateKeyDer::from_pem_file(key).map_err(|e| format!("{}: {e}", key.display()))?;
        Tls::new(certificates, key)
    }

    /// From PEM text (for tests, and certificates that do not live in files).
    pub fn from_pem(certificate: &str, key: &str) -> Result<Tls, String> {
        let certificates: Vec<CertificateDer<'static>> =
            CertificateDer::pem_slice_iter(certificate.as_bytes())
                .collect::<Result<_, _>>()
                .map_err(|e| format!("the certificate: {e}"))?;
        let key = PrivateKeyDer::from_pem_slice(key.as_bytes())
            .map_err(|e| format!("the private key: {e}"))?;
        Tls::new(certificates, key)
    }

    fn new(
        certificates: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<Tls, String> {
        if certificates.is_empty() {
            return Err("no certificate found".into());
        }
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(|e| format!("the certificate and key: {e}"))?;
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Tls {
            config: Arc::new(config),
        })
    }
}

/// Serves `router` over TLS until `shutdown` completes, then lets connections finish for a while.
pub(crate) async fn serve(
    router: Router,
    listener: tokio::net::TcpListener,
    tls: Tls,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let acceptor = tokio_rustls::TlsAcceptor::from(tls.config);
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let (stream, _) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                // Out of file descriptors, or a connection reset while queued: not fatal.
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            () = &mut shutdown => break,
        };
        let (acceptor, router) = (acceptor.clone(), router.clone());
        let watcher = graceful.watcher();
        // A task per connection: a slow handshake must not hold up the others.
        tokio::spawn(async move {
            let Ok(Ok(stream)) =
                tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await
            else {
                return;
            };
            let builder =
                hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
            let connection = builder.serve_connection_with_upgrades(
                hyper_util::rt::TokioIo::new(stream),
                hyper_util::service::TowerToHyperService::new(router),
            );
            let _ = watcher.watch(connection.into_owned()).await;
        });
    }
    let _ = tokio::time::timeout(DRAIN_TIMEOUT, graceful.shutdown()).await;
    Ok(())
}
