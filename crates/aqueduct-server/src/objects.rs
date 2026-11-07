//! Object storage (ADR 0061): a directory, or an S3-compatible bucket (AWS S3, MinIO, Cloudflare R2, ...)
//! reached with our own small client: four operations, signed with AWS Signature Version 4
//! ([`crate::sigv4`]), over the server's HTTP stack.

use std::path::PathBuf;
use std::time::Duration;

use axum::body::Bytes;
use http_body_util::{BodyExt, Full};
use serde::Deserialize;

use crate::sigv4;

/// Where objects are kept: the `"store"` of a configuration section.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum StoreConfig {
    /// A directory on this machine.
    Dir {
        /// The directory.
        path: PathBuf,
    },
    /// An S3-compatible bucket, addressed path-style: `<endpoint>/<bucket>/<prefix><key>`.
    S3 {
        /// The service's base URL, for example `https://s3.eu-central-1.amazonaws.com`.
        endpoint: String,
        /// The region the requests are signed for.
        region: String,
        /// The bucket.
        bucket: String,
        /// Put before every key, for example `aqueduct/`.
        #[serde(default)]
        prefix: String,
        /// The environment variable holding the access key id. Default `AWS_ACCESS_KEY_ID`.
        #[serde(default = "default_access_env")]
        access_key_env: String,
        /// The environment variable holding the secret key. Default `AWS_SECRET_ACCESS_KEY`.
        #[serde(default = "default_secret_env")]
        secret_key_env: String,
    },
}

fn default_access_env() -> String {
    "AWS_ACCESS_KEY_ID".into()
}

fn default_secret_env() -> String {
    "AWS_SECRET_ACCESS_KEY".into()
}

/// An object store.
pub struct Store {
    kind: Kind,
}

/// One per store, built once: the size of its variants does not matter.
#[allow(clippy::large_enum_variant)]
enum Kind {
    Dir(PathBuf),
    S3 {
        endpoint: String,
        host: String,
        region: String,
        bucket: String,
        prefix: String,
        access_key: String,
        secret_key: String,
        client: crate::push::Client,
    },
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            Kind::Dir(path) => write!(f, "Store::Dir({})", path.display()),
            Kind::S3 {
                endpoint, bucket, ..
            } => write!(f, "Store::S3({endpoint}/{bucket})"),
        }
    }
}

/// How long one request to the bucket may take.
const TIMEOUT: Duration = Duration::from_secs(60);

/// A key as a path under the directory: `/` separates directories, and any byte a file name cannot
/// safely hold becomes `%XX`. Callers that put arbitrary text in a key encode it first (as the archive
/// does run ids), so keys are the same in every store.
fn file_name(key: &str) -> PathBuf {
    key.split('/')
        .filter(|part| !part.is_empty() && *part != "." && *part != "..")
        .map(|part| {
            part.bytes()
                .map(|b| match b {
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'%' => {
                        (b as char).to_string()
                    }
                    _ => format!("%{b:02X}"),
                })
                .collect::<String>()
        })
        .collect()
}

impl Store {
    /// Opens the store a configuration names; for S3, its credentials come from the environment.
    pub fn open(config: &StoreConfig) -> Result<Store, String> {
        let kind = match config {
            StoreConfig::Dir { path } => Kind::Dir(path.clone()),
            StoreConfig::S3 {
                endpoint,
                region,
                bucket,
                prefix,
                access_key_env,
                secret_key_env,
            } => {
                let read = |name: &str| {
                    std::env::var(name)
                        .map_err(|_| format!("object storage: the environment has no {name}"))
                };
                return Store::s3(
                    endpoint,
                    region,
                    bucket,
                    prefix,
                    &read(access_key_env)?,
                    &read(secret_key_env)?,
                );
            }
        };
        Ok(Store { kind })
    }

    /// An S3-compatible bucket, with its credentials.
    pub fn s3(
        endpoint: &str,
        region: &str,
        bucket: &str,
        prefix: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<Store, String> {
        let endpoint = endpoint.trim_end_matches('/').to_owned();
        let host = endpoint
            .split_once("://")
            .map(|(_, rest)| rest)
            .ok_or_else(|| format!("object storage: `{endpoint}` is not a URL"))?
            .split('/')
            .next()
            .unwrap_or_default()
            .to_owned();
        Ok(Store {
            kind: Kind::S3 {
                host,
                region: region.to_owned(),
                bucket: bucket.to_owned(),
                prefix: prefix.to_owned(),
                access_key: access_key.to_owned(),
                secret_key: secret_key.to_owned(),
                client: crate::push::client()?,
                endpoint,
            },
        })
    }

    /// Stores an object, replacing one with the same key.
    pub async fn put(&self, key: &str, data: Vec<u8>) -> Result<(), String> {
        match &self.kind {
            Kind::Dir(root) => {
                let path = root.join(file_name(key));
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
                }
                // Whole or not at all: written aside, then renamed.
                let writing = path.with_extension("writing");
                std::fs::write(&writing, data).map_err(|e| e.to_string())?;
                std::fs::rename(&writing, &path).map_err(|e| e.to_string())
            }
            Kind::S3 { .. } => {
                let (status, _) = self.object_request("PUT", key, &[], data).await?;
                ok(status, "put", key)
            }
        }
    }

    /// An object, if there is one with this key.
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        match &self.kind {
            Kind::Dir(root) => match std::fs::read(root.join(file_name(key))) {
                Ok(data) => Ok(Some(data)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e.to_string()),
            },
            Kind::S3 { .. } => {
                let (status, body) = self.object_request("GET", key, &[], Vec::new()).await?;
                if status == 404 {
                    return Ok(None);
                }
                ok(status, "get", key)?;
                Ok(Some(body))
            }
        }
    }

    /// Removes an object; removing one that is not there is not an error.
    pub async fn delete(&self, key: &str) -> Result<(), String> {
        match &self.kind {
            Kind::Dir(root) => match std::fs::remove_file(root.join(file_name(key))) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.to_string()),
                _ => Ok(()),
            },
            Kind::S3 { .. } => {
                let (status, _) = self.object_request("DELETE", key, &[], Vec::new()).await?;
                if status == 404 {
                    return Ok(());
                }
                ok(status, "delete", key)
            }
        }
    }

    /// The keys that start with `prefix`, sorted.
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        match &self.kind {
            Kind::Dir(root) => {
                let mut keys = Vec::new();
                list_dir(root, root, &mut keys)?;
                keys.retain(|key| key.starts_with(prefix));
                keys.sort();
                Ok(keys)
            }
            Kind::S3 { prefix: base, .. } => {
                let mut keys = Vec::new();
                let mut token: Option<String> = None;
                loop {
                    let mut query = vec![
                        ("list-type".to_owned(), "2".to_owned()),
                        ("prefix".to_owned(), format!("{base}{prefix}")),
                    ];
                    if let Some(token) = &token {
                        query.push(("continuation-token".to_owned(), token.clone()));
                    }
                    let (status, body) = self.s3_bucket("GET", &query).await?;
                    ok(status, "list", prefix)?;
                    let text = String::from_utf8_lossy(&body);
                    for key in tags(&text, "Key") {
                        keys.push(unescape(&key).trim_start_matches(base.as_str()).to_owned());
                    }
                    token = tags(&text, "NextContinuationToken")
                        .into_iter()
                        .next()
                        .map(|t| unescape(&t));
                    if token.is_none() {
                        break;
                    }
                }
                keys.sort();
                Ok(keys)
            }
        }
    }

    /// A request for one object.
    async fn object_request(
        &self,
        method: &str,
        key: &str,
        query: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<(u16, Vec<u8>), String> {
        let Kind::S3 { bucket, prefix, .. } = &self.kind else {
            return Err("not an S3 store".into());
        };
        let path = format!(
            "/{}/{}",
            sigv4::uri_encode(bucket, false),
            sigv4::uri_encode(&format!("{prefix}{key}"), true)
        );
        self.send(method, &path, query, body).await
    }

    /// A request for the bucket itself (a listing).
    async fn s3_bucket(
        &self,
        method: &str,
        query: &[(String, String)],
    ) -> Result<(u16, Vec<u8>), String> {
        let Kind::S3 { bucket, .. } = &self.kind else {
            return Err("not an S3 store".into());
        };
        let path = format!("/{}", sigv4::uri_encode(bucket, false));
        self.send(method, &path, query, Vec::new()).await
    }

    async fn send(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<(u16, Vec<u8>), String> {
        let Kind::S3 {
            endpoint,
            host,
            region,
            access_key,
            secret_key,
            client,
            ..
        } = &self.kind
        else {
            return Err("not an S3 store".into());
        };
        let seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_secs();
        let timestamp = sigv4::timestamp(seconds);
        let payload_hash = sigv4::sha256_hex(&body);
        let headers = vec![
            ("host".to_owned(), host.clone()),
            ("x-amz-content-sha256".to_owned(), payload_hash.clone()),
            ("x-amz-date".to_owned(), timestamp.clone()),
        ];
        let scope = sigv4::Scope {
            timestamp: &timestamp,
            region,
            service: "s3",
        };
        let request = sigv4::Request {
            method,
            path,
            query,
            headers: &headers,
            payload_hash: &payload_hash,
        };
        let authorization = sigv4::authorization(access_key, secret_key, &scope, &request);
        let mut query_string = query
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    sigv4::uri_encode(k, false),
                    sigv4::uri_encode(v, false)
                )
            })
            .collect::<Vec<_>>()
            .join("&");
        if !query_string.is_empty() {
            query_string.insert(0, '?');
        }
        let http = axum::http::Request::builder()
            .method(method)
            .uri(format!("{endpoint}{path}{query_string}"))
            .header("x-amz-content-sha256", &payload_hash)
            .header("x-amz-date", &timestamp)
            .header("authorization", authorization)
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| e.to_string())?;
        let response = tokio::time::timeout(TIMEOUT, client.request(http))
            .await
            .map_err(|_| format!("object storage did not answer within {TIMEOUT:?}"))?
            .map_err(|e| format!("object storage: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|e| e.to_string())?
            .to_bytes()
            .to_vec();
        Ok((status, body))
    }
}

fn ok(status: u16, what: &str, key: &str) -> Result<(), String> {
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(format!(
            "object storage refused to {what} `{key}`: HTTP {status}"
        ))
    }
}

fn list_dir(
    root: &std::path::Path,
    dir: &std::path::Path,
    keys: &mut Vec<String>,
) -> Result<(), String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Ok(());
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            list_dir(root, &path, keys)?;
        } else if path.extension().is_none_or(|e| e != "writing")
            && let Ok(relative) = path.strip_prefix(root)
        {
            let key = relative
                .iter()
                .map(|part| part.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");
            keys.push(key);
        }
    }
    Ok(())
}

/// The texts of every `<tag>...</tag>` in an XML answer: enough for a listing.
fn tags(xml: &str, tag: &str) -> Vec<String> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut found = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else {
            break;
        };
        found.push(after[..end].to_owned());
        rest = &after[end + close.len()..];
    }
    found
}

fn unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_make_safe_file_names_and_come_back() {
        assert_eq!(
            file_name("runs/%24ns%2Facme/x y/../."),
            PathBuf::from("runs").join("%24ns%2Facme").join("x%20y")
        );
        assert_eq!(
            tags("<a><Key>x&amp;y</Key><Key>z</Key></a>", "Key"),
            vec!["x&amp;y".to_owned(), "z".to_owned()]
        );
        assert_eq!(unescape("x&amp;y"), "x&y");
    }

    /// A bucket in memory that checks every request's signature as S3 would, by recomputing it from
    /// what arrived; it lists two keys a page, so that continuation is used.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_client_works_against_a_bucket_that_checks_signatures() {
        use axum::extract::{Request, State};
        use axum::http::StatusCode;
        use std::collections::BTreeMap;
        use std::sync::{Arc, Mutex};
        type Objects = Arc<Mutex<BTreeMap<String, Vec<u8>>>>;

        fn decode(text: &str) -> String {
            let bytes = text.as_bytes();
            let mut out = Vec::new();
            let mut i = 0;
            while i < bytes.len() {
                if bytes[i] == b'%' && i + 3 <= bytes.len() {
                    out.push(u8::from_str_radix(&text[i + 1..i + 3], 16).unwrap());
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            String::from_utf8(out).unwrap()
        }

        async fn bucket(State(objects): State<Objects>, request: Request) -> (StatusCode, Vec<u8>) {
            let (parts, body) = request.into_parts();
            let body = axum::body::to_bytes(body, 1 << 20).await.unwrap().to_vec();
            let header = |name: &str| {
                parts
                    .headers
                    .get(name)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned()
            };
            // The signature, as S3 recomputes it from the request it received.
            let query: Vec<(String, String)> = parts
                .uri
                .query()
                .unwrap_or_default()
                .split('&')
                .filter(|p| !p.is_empty())
                .map(|p| {
                    let (k, v) = p.split_once('=').unwrap();
                    (decode(k), decode(v))
                })
                .collect();
            let headers = vec![
                ("host".to_owned(), header("host")),
                (
                    "x-amz-content-sha256".to_owned(),
                    header("x-amz-content-sha256"),
                ),
                ("x-amz-date".to_owned(), header("x-amz-date")),
            ];
            assert_eq!(header("x-amz-content-sha256"), sigv4::sha256_hex(&body));
            let timestamp = header("x-amz-date");
            let scope = sigv4::Scope {
                timestamp: &timestamp,
                region: "test-1",
                service: "s3",
            };
            let expected = sigv4::authorization(
                "AKTEST",
                "secret",
                &scope,
                &sigv4::Request {
                    method: parts.method.as_str(),
                    path: parts.uri.path(),
                    query: &query,
                    headers: &headers,
                    payload_hash: &header("x-amz-content-sha256"),
                },
            );
            if header("authorization") != expected {
                return (StatusCode::FORBIDDEN, Vec::new());
            }
            let path = decode(parts.uri.path());
            let mut objects = objects.lock().unwrap();
            let Some(key) = path.strip_prefix("/bucket/") else {
                // A listing, two keys a page.
                let prefix = query
                    .iter()
                    .find(|(k, _)| k == "prefix")
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default();
                let after = query
                    .iter()
                    .find(|(k, _)| k == "continuation-token")
                    .map(|(_, v)| v.clone());
                let keys: Vec<&String> = objects
                    .keys()
                    .filter(|k| k.starts_with(&prefix) && after.as_ref().is_none_or(|a| *k > a))
                    .collect();
                let mut xml = String::from("<ListBucketResult>");
                for key in keys.iter().take(2) {
                    xml.push_str(&format!(
                        "<Contents><Key>{}</Key></Contents>",
                        key.replace('&', "&amp;")
                    ));
                }
                if keys.len() > 2 {
                    xml.push_str(&format!(
                        "<NextContinuationToken>{}</NextContinuationToken>",
                        keys[1]
                    ));
                }
                xml.push_str("</ListBucketResult>");
                return (StatusCode::OK, xml.into_bytes());
            };
            match parts.method.as_str() {
                "PUT" => {
                    objects.insert(key.to_owned(), body);
                    (StatusCode::OK, Vec::new())
                }
                "GET" => match objects.get(key) {
                    Some(data) => (StatusCode::OK, data.clone()),
                    None => (StatusCode::NOT_FOUND, Vec::new()),
                },
                "DELETE" => {
                    objects.remove(key);
                    (StatusCode::NO_CONTENT, Vec::new())
                }
                _ => (StatusCode::METHOD_NOT_ALLOWED, Vec::new()),
            }
        }

        let objects: Objects = Arc::default();
        let app = axum::Router::new()
            .fallback(bucket)
            .with_state(Arc::clone(&objects));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });

        let store = Store::s3(
            &format!("http://{addr}"),
            "test-1",
            "bucket",
            "pre/",
            "AKTEST",
            "secret",
        )
        .unwrap();
        for key in [
            "runs/a%20b.history",
            "runs/b.history",
            "runs/c&d.history",
            "other/x",
        ] {
            store.put(key, key.as_bytes().to_vec()).await.unwrap();
        }
        assert_eq!(
            store.get("runs/b.history").await.unwrap(),
            Some(b"runs/b.history".to_vec())
        );
        assert_eq!(store.get("runs/none").await.unwrap(), None);
        assert_eq!(
            store.list("runs/").await.unwrap(),
            vec!["runs/a%20b.history", "runs/b.history", "runs/c&d.history"]
        );
        store.delete("runs/b.history").await.unwrap();
        store.delete("runs/b.history").await.unwrap();
        assert_eq!(store.list("runs/").await.unwrap().len(), 2);
        assert!(objects.lock().unwrap().contains_key("pre/other/x"));
        // Wrong credentials are refused.
        let wrong = Store::s3(
            &format!("http://{addr}"),
            "test-1",
            "bucket",
            "pre/",
            "AKTEST",
            "nope",
        )
        .unwrap();
        assert!(wrong.put("x", Vec::new()).await.is_err());
    }
}
