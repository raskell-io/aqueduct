//! Authentication and authorisation (ADR 0041): bearer tokens with permissions.
//!
//! A token is `aqd_` and 64 hexadecimal digits of randomness. The server stores only its SHA-256, with
//! a name and permissions, in `<dir>/tokens.json` (readable by its owner only). A request carries it as
//! `authorization: Bearer <token>`, in an HTTP header or gRPC metadata. With no tokens file, the server
//! has no authentication.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// What a token may do.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Permission {
    /// Read runs: wait, describe, list, query, list escalations.
    Read,
    /// Start runs.
    Start,
    /// Signal runs, complete awakeables, send updates.
    Signal,
    /// Cancel runs.
    Cancel,
    /// Resolve escalations.
    Resolve,
    /// Serve as a step or workflow worker.
    Work,
    /// Everything.
    Admin,
}

impl Permission {
    /// Every permission, as written.
    pub const ALL: [Permission; 7] = [
        Permission::Read,
        Permission::Start,
        Permission::Signal,
        Permission::Cancel,
        Permission::Resolve,
        Permission::Work,
        Permission::Admin,
    ];

    /// The permission's name.
    pub fn as_str(self) -> &'static str {
        match self {
            Permission::Read => "read",
            Permission::Start => "start",
            Permission::Signal => "signal",
            Permission::Cancel => "cancel",
            Permission::Resolve => "resolve",
            Permission::Work => "work",
            Permission::Admin => "admin",
        }
    }

    /// Parses a permission's name.
    pub fn parse(name: &str) -> Result<Permission, String> {
        Permission::ALL
            .into_iter()
            .find(|p| p.as_str() == name)
            .ok_or_else(|| {
                format!(
                    "unknown permission `{name}`: one of {}",
                    Permission::ALL.map(Permission::as_str).join(", ")
                )
            })
    }
}

/// A token as stored: its name, the SHA-256 of the secret, and what it may do.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Token {
    /// Who or what holds it (for example `billing-worker` or `ada`); it names the caller in the audit
    /// log.
    pub name: String,
    /// The SHA-256 of the secret, in hex.
    pub sha256: String,
    /// What it may do.
    pub permissions: BTreeSet<Permission>,
    /// When it was created (milliseconds since the Unix epoch).
    pub created_ms: u64,
    /// The namespace it works in (ADR 0046), or `*` for all of them: its requests then name one, with
    /// the `aqueduct-namespace` header, and as a worker it serves all.
    #[serde(default = "default_namespace")]
    pub namespace: String,
}

fn default_namespace() -> String {
    aqueduct_engine::namespaces::DEFAULT.to_owned()
}

impl Token {
    /// Whether the token may do what `needed` stands for.
    pub fn allows(&self, needed: Permission) -> bool {
        self.permissions.contains(&Permission::Admin) || self.permissions.contains(&needed)
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct TokensFile {
    tokens: Vec<Token>,
}

/// The tokens file of a data directory.
pub fn tokens_path(dir: &Path) -> PathBuf {
    dir.join("tokens.json")
}

fn sha256_hex(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn read_file(path: &Path) -> Result<TokensFile, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
}

/// Writes the tokens file atomically, readable by its owner only.
fn write_file(path: &Path, file: &TokensFile) -> Result<(), String> {
    use std::io::Write;
    let io = |e: std::io::Error| format!("{}: {e}", path.display());
    let temporary = path.with_extension("json.new");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut out = options.open(&temporary).map_err(io)?;
    let text = serde_json::to_string_pretty(file).map_err(|e| e.to_string())?;
    out.write_all(text.as_bytes()).map_err(io)?;
    out.sync_all().map_err(io)?;
    std::fs::rename(&temporary, path).map_err(io)
}

/// Creates a token in a data directory's tokens file (creating the file, which turns authentication
/// on), and returns its secret: shown once, never stored.
pub fn create_token(
    dir: &Path,
    name: &str,
    permissions: BTreeSet<Permission>,
) -> Result<String, String> {
    create_token_in(dir, name, permissions, aqueduct_engine::namespaces::DEFAULT)
}

/// [`create_token`], for a token that works in `namespace`, or in all of them (`*`).
pub fn create_token_in(
    dir: &Path,
    name: &str,
    permissions: BTreeSet<Permission>,
    namespace: &str,
) -> Result<String, String> {
    if namespace != "*" && !aqueduct_engine::namespaces::valid_namespace(namespace) {
        return Err(format!(
            "`{namespace}` is not a namespace: letters, digits, - and _, or * for all"
        ));
    }
    if name.is_empty() {
        return Err("a token needs a name".into());
    }
    if permissions.is_empty() {
        return Err("a token needs at least one permission".into());
    }
    let path = tokens_path(dir);
    let mut file = if path.exists() {
        read_file(&path)?
    } else {
        TokensFile::default()
    };
    if file.tokens.iter().any(|t| t.name == name) {
        return Err(format!("a token named `{name}` exists; revoke it first"));
    }
    let mut secret = [0u8; 32];
    getrandom::fill(&mut secret).map_err(|e| format!("no randomness for a token: {e}"))?;
    let token = format!(
        "aqd_{}",
        secret
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    file.tokens.push(Token {
        name: name.to_owned(),
        sha256: sha256_hex(&token),
        permissions,
        created_ms: now_ms(),
        namespace: namespace.to_owned(),
    });
    std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    write_file(&path, &file)?;
    Ok(token)
}

/// Revokes a token by name.
pub fn revoke_token(dir: &Path, name: &str) -> Result<(), String> {
    let path = tokens_path(dir);
    let mut file = read_file(&path)?;
    let before = file.tokens.len();
    file.tokens.retain(|t| t.name != name);
    if file.tokens.len() == before {
        return Err(format!("no token named `{name}`"));
    }
    write_file(&path, &file)
}

/// The tokens of a data directory (without secrets, which are never stored).
pub fn list_tokens(dir: &Path) -> Result<Vec<Token>, String> {
    let path = tokens_path(dir);
    if !path.exists() {
        return Ok(Vec::new());
    }
    Ok(read_file(&path)?.tokens)
}

/// Who made a request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Principal {
    /// The server has no authentication: anyone who can reach it.
    Anyone,
    /// The holder of the named token.
    Token(String),
}

impl Principal {
    /// The name the audit log gives it.
    pub fn name(&self) -> &str {
        match self {
            Principal::Anyone => "anyone",
            Principal::Token(name) => name,
        }
    }
}

/// Why a request was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// No token, or one the server does not know.
    Unauthenticated(String),
    /// A known token without the permission: the token's name, and why.
    Forbidden(String, String),
}

/// Checks requests against a data directory's tokens file, which it reads again when it changes, so
/// a token created or revoked takes effect without a restart: the first token created turns
/// authentication on at once. Once it has seen tokens, a missing file refuses every request: losing the
/// file must not open the server.
#[derive(Debug)]
pub struct Authenticator {
    path: Option<PathBuf>,
    /// Whether the tokens file has been seen, in this process.
    seen: std::sync::atomic::AtomicBool,
    cached: Mutex<Option<(SystemTime, u64, Vec<Token>)>>,
}

impl Authenticator {
    /// Authentication by the tokens file of `dir`, if it has one; none otherwise.
    pub fn for_dir(dir: &Path) -> Authenticator {
        let path = tokens_path(dir);
        Authenticator {
            seen: std::sync::atomic::AtomicBool::new(path.exists()),
            path: Some(path),
            cached: Mutex::new(None),
        }
    }

    /// No authentication: every request is allowed.
    pub fn none() -> Authenticator {
        Authenticator {
            path: None,
            seen: std::sync::atomic::AtomicBool::new(false),
            cached: Mutex::new(None),
        }
    }

    /// Whether requests must carry a token.
    pub fn is_enabled(&self) -> bool {
        use std::sync::atomic::Ordering;
        let Some(path) = &self.path else {
            return false;
        };
        if self.seen.load(Ordering::SeqCst) {
            return true;
        }
        let exists = path.exists();
        if exists {
            self.seen.store(true, Ordering::SeqCst);
        }
        exists
    }

    fn tokens(&self, path: &Path) -> Result<Vec<Token>, String> {
        let metadata = std::fs::metadata(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let modified = metadata
            .modified()
            .map_err(|e| format!("{}: {e}", path.display()))?;
        // A change within the clock's resolution can keep the same time: the cache is trusted only for
        // a file left alone for a second, of the same size.
        let settled = modified
            .elapsed()
            .is_ok_and(|age| age > std::time::Duration::from_secs(1));
        let mut cached = self.cached.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((at, size, tokens)) = cached.as_ref()
            && settled
            && *at == modified
            && *size == metadata.len()
        {
            return Ok(tokens.clone());
        }
        let tokens = read_file(path)?.tokens;
        *cached = Some((modified, metadata.len(), tokens.clone()));
        Ok(tokens)
    }

    /// The namespace of the named token: `*` for all.
    pub fn token_namespace(&self, name: &str) -> Option<String> {
        let path = self.path.as_ref()?;
        let tokens = self.tokens(path).ok()?;
        tokens
            .into_iter()
            .find(|token| token.name == name)
            .map(|token| token.namespace)
    }

    /// Checks a request's `authorization` header for `needed`.
    pub fn check(
        &self,
        authorization: Option<&str>,
        needed: Permission,
    ) -> Result<Principal, Refusal> {
        if !self.is_enabled() {
            return Ok(Principal::Anyone);
        }
        let Some(path) = &self.path else {
            return Ok(Principal::Anyone);
        };
        let secret = authorization
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::trim)
            .ok_or_else(|| {
                Refusal::Unauthenticated(
                    "a token is required: authorization: Bearer <token>".into(),
                )
            })?;
        let digest = sha256_hex(secret);
        // A tokens file that cannot be read refuses everything rather than letting anything through.
        let tokens = self.tokens(path).map_err(Refusal::Unauthenticated)?;
        let token = tokens
            .iter()
            .find(|t| constant_time_eq(t.sha256.as_bytes(), digest.as_bytes()))
            .ok_or_else(|| Refusal::Unauthenticated("unknown or revoked token".into()))?;
        if !token.allows(needed) {
            return Err(Refusal::Forbidden(
                token.name.clone(),
                format!(
                    "token `{}` lacks the `{}` permission",
                    token.name,
                    needed.as_str()
                ),
            ));
        }
        Ok(Principal::Token(token.name.clone()))
    }
}

/// Compares two byte strings in time that depends only on their lengths.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_checked_for_their_permissions() {
        let dir = std::env::temp_dir().join(format!("aqueduct-auth-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let early = Authenticator::for_dir(&dir);
        assert!(!early.is_enabled());
        assert_eq!(early.check(None, Permission::Admin), Ok(Principal::Anyone));
        let reader = create_token(&dir, "dashboard", [Permission::Read].into()).unwrap();
        let admin = create_token(&dir, "ada", [Permission::Admin].into()).unwrap();
        assert!(reader.starts_with("aqd_") && reader.len() == 68);
        assert!(create_token(&dir, "ada", [Permission::Read].into()).is_err());
        // The first token turns authentication on, without a restart.
        assert!(matches!(
            early.check(None, Permission::Read),
            Err(Refusal::Unauthenticated(_))
        ));
        let auth = Authenticator::for_dir(&dir);
        let bearer = |t: &str| format!("Bearer {t}");
        assert_eq!(
            auth.check(Some(&bearer(&reader)), Permission::Read),
            Ok(Principal::Token("dashboard".into()))
        );
        assert!(matches!(
            auth.check(Some(&bearer(&reader)), Permission::Start),
            Err(Refusal::Forbidden(..))
        ));
        assert!(auth.check(Some(&bearer(&admin)), Permission::Work).is_ok());
        assert!(matches!(
            auth.check(None, Permission::Read),
            Err(Refusal::Unauthenticated(_))
        ));
        assert!(matches!(
            auth.check(Some("Bearer aqd_nope"), Permission::Read),
            Err(Refusal::Unauthenticated(_))
        ));
        // Revoked: refused at once, without a restart.
        revoke_token(&dir, "dashboard").unwrap();
        assert!(matches!(
            auth.check(Some(&bearer(&reader)), Permission::Read),
            Err(Refusal::Unauthenticated(_))
        ));
        // Only digests are stored.
        let stored = std::fs::read_to_string(tokens_path(&dir)).unwrap();
        assert!(!stored.contains(&admin));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(tokens_path(&dir))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        // Losing the file does not open the server: everything is refused.
        std::fs::remove_file(tokens_path(&dir)).unwrap();
        assert!(matches!(
            auth.check(None, Permission::Read),
            Err(Refusal::Unauthenticated(_))
        ));
        assert!(matches!(
            auth.check(Some(&bearer(&admin)), Permission::Read),
            Err(Refusal::Unauthenticated(_))
        ));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
