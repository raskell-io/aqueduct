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
    certificates: Vec<CertificateDer<'static>>,
    key: Arc<PrivateKeyDer<'static>>,
}

/// Whether the connection a request came on presented a client certificate that the configured
/// client CA verified (ADR 0064): servers of the cluster do, clients need not.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PeerCertificate(pub(crate) bool);

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
        Tls::build(certificates, key, None)
    }

    fn build(
        certificates: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        clients: Option<Arc<dyn rustls::server::danger::ClientCertVerifier>>,
    ) -> Result<Tls, String> {
        if certificates.is_empty() {
            return Err("no certificate found".into());
        }
        let builder = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?;
        let builder = match clients {
            Some(verifier) => builder.with_client_cert_verifier(verifier),
            None => builder.with_no_client_auth(),
        };
        let mut config = builder
            .with_single_cert(certificates.clone(), key.clone_key())
            .map_err(|e| format!("the certificate and key: {e}"))?;
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Tls {
            config: Arc::new(config),
            certificates,
            key: Arc::new(key),
        })
    }

    /// Asks every client for a certificate, and verifies one that is presented against the CA in
    /// `ca_pem` (ADR 0064). A client without one is still served: only the routes between servers
    /// require it.
    pub fn with_client_ca(self, ca_pem: &str) -> Result<Tls, String> {
        let mut roots = rustls::RootCertStore::empty();
        for certificate in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
            roots
                .add(certificate.map_err(|e| format!("the client CA: {e}"))?)
                .map_err(|e| format!("the client CA: {e}"))?;
        }
        if roots.is_empty() {
            return Err("the client CA holds no certificate".into());
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .allow_unauthenticated()
        .build()
        .map_err(|e| format!("the client CA: {e}"))?;
        Tls::build(self.certificates, self.key.clone_key(), Some(verifier))
    }
}

/// What a connection may hold on to, while it is doing nothing useful.
#[derive(Clone)]
pub(crate) struct Connections {
    /// Places: over them, new connections wait in the operating system's queue.
    pub(crate) places: Arc<tokio::sync::Semaphore>,
    /// How long a client has for a request's headers: from connecting to its first byte, and from there
    /// to the end of the headers on HTTP/1.1.
    pub(crate) header_timeout: Duration,
}

/// Serves `router` until `shutdown` completes, over TLS if given, then lets connections finish for a
/// while. A place is held from accepting a connection until it closes, so without deadlines a silent
/// client could keep one for ever, and enough of them would lock everyone else out.
pub(crate) async fn serve(
    router: Router,
    listener: tokio::net::TcpListener,
    tls: Option<Tls>,
    connections: Connections,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let acceptor = tls.map(|tls| tokio_rustls::TlsAcceptor::from(tls.config));
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let permit = tokio::select! {
            permit = Arc::clone(&connections.places).acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
            () = &mut shutdown => break,
        };
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
        let timeout = connections.header_timeout;
        // A task per connection: a slow one must not hold up the others.
        tokio::spawn(async move {
            let _permit = permit;
            match acceptor {
                Some(acceptor) => {
                    let Ok(Ok(stream)) =
                        tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await
                    else {
                        return;
                    };
                    // A certificate here was verified by the client CA during the handshake.
                    let verified = stream
                        .get_ref()
                        .1
                        .peer_certificates()
                        .is_some_and(|certificates| !certificates.is_empty());
                    let router = router.layer(axum::Extension(PeerCertificate(verified)));
                    connection(stream, router, watcher, timeout).await;
                }
                None => {
                    let router = router.layer(axum::Extension(PeerCertificate(false)));
                    connection(stream, router, watcher, timeout).await;
                }
            }
        });
    }
    let _ = tokio::time::timeout(DRAIN_TIMEOUT, graceful.shutdown()).await;
    Ok(())
}

/// Serves one connection, HTTP/1.1 or HTTP/2, once its client has said something in time.
async fn connection<S>(
    stream: S,
    router: Router,
    watcher: hyper_util::server::graceful::Watcher,
    header_timeout: Duration,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use tokio::io::AsyncBufReadExt;
    let mut stream = tokio::io::BufReader::new(stream);
    match tokio::time::timeout(header_timeout, stream.fill_buf()).await {
        Ok(Ok(bytes)) if !bytes.is_empty() => {}
        _ => return,
    }
    let timer = hyper_util::rt::TokioTimer::new;
    let mut builder =
        hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
    builder
        .http1()
        .timer(timer())
        .header_read_timeout(header_timeout);
    // Finds dead peers, such as workers on a machine that went away.
    builder
        .http2()
        .timer(timer())
        .keep_alive_interval(Some(Duration::from_secs(30)))
        .keep_alive_timeout(Duration::from_secs(20));
    let connection = builder.serve_connection_with_upgrades(
        hyper_util::rt::TokioIo::new(stream),
        hyper_util::service::TowerToHyperService::new(router),
    );
    let _ = watcher.watch(connection.into_owned()).await;
}
