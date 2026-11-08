//! Mutual TLS between servers (ADR 0064): three servers on TLS whose routes between servers take only
//! connections with a certificate the cluster's CA issued. The cluster forms and serves; a connection
//! without a certificate, or with one from another CA, is refused.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aqueduct_engine::embedded::Engine;
use aqueduct_procedure::Procedure;
use aqueduct_server::{Config, Security, Tls};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A CA, and a function that issues a certificate and key (PEM) for 127.0.0.1 under it.
struct Authority {
    issuer: CertifiedIssuer<'static, KeyPair>,
}

impl Authority {
    fn new() -> Authority {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        Authority {
            issuer: CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap(),
        }
    }

    fn pem(&self) -> String {
        self.issuer.pem()
    }

    fn issue(&self) -> (String, String) {
        let mut params = CertificateParams::new(vec!["127.0.0.1".to_owned()]).unwrap();
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = KeyPair::generate().unwrap();
        let certificate = params.signed_by(&key, &self.issuer).unwrap();
        (certificate.pem(), key.serialize_pem())
    }
}

/// An HTTP/1.1 request over TLS that trusts `ca`, presenting `identity` if given. `None` if the
/// connection could not be made (refused in the handshake, for one).
async fn https(
    addr: SocketAddr,
    ca: &str,
    identity: Option<&(String, String)>,
    method: &str,
    path: &str,
    headers: &str,
    body: &str,
) -> Option<(u16, String)> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(ca.as_bytes()) {
        roots.add(certificate.unwrap()).unwrap();
    }
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots);
    let config = match identity {
        Some((certificate, key)) => builder
            .with_client_auth_cert(
                CertificateDer::pem_slice_iter(certificate.as_bytes())
                    .collect::<Result<_, _>>()
                    .unwrap(),
                PrivateKeyDer::from_pem_slice(key.as_bytes()).unwrap(),
            )
            .unwrap(),
        None => builder.with_no_client_auth(),
    };
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let tcp = tokio::net::TcpStream::connect(addr).await.ok()?;
    let mut stream = connector
        .connect(ServerName::try_from("127.0.0.1").unwrap(), tcp)
        .await
        .ok()?;
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut response = Vec::new();
    // A refused client certificate surfaces here, once the server has looked at it.
    stream.read_to_end(&mut response).await.ok()?;
    let response = String::from_utf8_lossy(&response).into_owned();
    if response.len() < 12 {
        return None;
    }
    let text = response
        .split_once("\r\n\r\n")
        .map_or("", |(_, t)| t)
        .to_owned();
    Some((response[9..12].parse().unwrap(), text))
}

fn write(dir: &Path, name: &str, text: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    path
}

#[tokio::test(flavor = "multi_thread")]
async fn servers_take_only_certificates_from_their_own_ca() {
    let base = std::env::temp_dir().join(format!("aqueduct-mtls-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let ca = Authority::new();
    let ca_file = write(&base, "ca.pem", &ca.pem());
    let mut listeners = BTreeMap::new();
    for id in 1..=3u64 {
        listeners.insert(
            id,
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        );
    }
    let addrs: BTreeMap<u64, SocketAddr> = listeners
        .iter()
        .map(|(id, l)| (*id, l.local_addr().unwrap()))
        .collect();
    let peers: BTreeMap<String, String> = addrs
        .iter()
        .map(|(id, addr)| (id.to_string(), format!("https://{addr}")))
        .collect();
    for (id, listener) in listeners {
        let dir = base.join(format!("node-{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let (certificate, key) = ca.issue();
        let cert_file = write(&dir, "cert.pem", &certificate);
        let key_file = write(&dir, "key.pem", &key);
        let config: Config = serde_json::from_value(json!({
            "cluster": {
                "id": id, "peers": peers, "secret": "a secret for the test", "tick_ms": 20,
                "tls": {"ca": ca_file, "cert": cert_file, "key": key_file}
            }
        }))
        .unwrap();
        config.check().unwrap();
        let cluster = config.cluster.clone().unwrap();
        let shards = cluster
            .open(&dir.join("data"), 1, |path, _| {
                Ok(Engine::builder(path).workflow(
                    "double",
                    Arc::new(
                        Procedure::compile("d.star", "def run(x):\n    return x * 2\n").unwrap(),
                    ),
                ))
            })
            .unwrap();
        let tls = Tls::from_pem(&certificate, &key)
            .unwrap()
            .with_client_ca(&ca.pem())
            .unwrap();
        let security = Security::for_dir(&dir).unwrap();
        tokio::spawn(async move {
            aqueduct_server::serve_shards(
                shards,
                listener,
                &config,
                security,
                Some(tls),
                std::future::pending(),
            )
            .await
        });
    }
    // The cluster forms over mutual TLS: a client, without a certificate of its own, starts a run on any
    // server and reads its result.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut started = false;
    while !started {
        for addr in addrs.values() {
            let answer = https(
                *addr,
                &ca.pem(),
                None,
                "POST",
                "/v1/runs",
                "",
                &json!({"key": "a", "workflow": "double", "input": 21}).to_string(),
            )
            .await;
            if answer
                .as_ref()
                .is_some_and(|(status, _)| (200..300).contains(status))
            {
                started = true;
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "the cluster never formed over mutual TLS"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut done = false;
        for addr in addrs.values() {
            if let Some((200, text)) =
                https(*addr, &ca.pem(), None, "GET", "/v1/runs/a/result", "", "").await
            {
                let answer: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(answer["output"], json!(42), "{answer}");
                done = true;
                break;
            }
        }
        if done {
            break;
        }
        assert!(Instant::now() < deadline, "no result");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let peer = format!(
        "aqueduct-cluster-secret: a secret for the test\r\naqueduct-version: {}\r\n",
        env!("CARGO_PKG_VERSION")
    );
    let addr = addrs[&1];
    // With the secret but no certificate: refused.
    let (status, _) = https(addr, &ca.pem(), None, "POST", "/cluster/v1/raft", &peer, "")
        .await
        .unwrap();
    assert_eq!(status, 401);
    // With a certificate from the cluster's CA, the same request passes the check (and is a bad body).
    let ours = ca.issue();
    let (status, _) = https(
        addr,
        &ca.pem(),
        Some(&ours),
        "POST",
        "/cluster/v1/raft",
        &peer,
        "",
    )
    .await
    .unwrap();
    assert_eq!(status, 400);
    // A certificate from another CA does not get through the handshake.
    let stranger = Authority::new().issue();
    let refused = https(
        addr,
        &ca.pem(),
        Some(&stranger),
        "POST",
        "/cluster/v1/raft",
        &peer,
        "",
    )
    .await;
    assert!(refused.is_none(), "{refused:?}");
    let _ = std::fs::remove_dir_all(&base);
}
