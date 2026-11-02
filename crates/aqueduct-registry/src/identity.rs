//! Signing keys, identities and approval statements (BRIEF.md section 4.5; ADR 0033).
//!
//! An approval is signed with the approver's Ed25519 key over an [approval statement]: the workflow,
//! the version (the SHA-256 of the script's exact bytes), the approver, the replay report approved and
//! the note. The registry's `identities.json` says which public keys belong to which person:
//!
//! ```json
//! { "grace": ["ed25519:3b6a27bc…"], "linus": ["ed25519:…", "ed25519:…"] }
//! ```
//!
//! Keys are written as `ed25519:` and 64 hex digits; signatures as 128 hex digits.
//!
//! [approval statement]: approval_statement

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use serde_json::Value;

use crate::RegistryError;

const KEY_PREFIX: &str = "ed25519:";
const SECRET_LINE: &str = "secret ed25519:";

/// An Ed25519 public key: who a signature claims to be from.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublicKey([u8; 32]);

impl PublicKey {
    /// Parses `ed25519:<64 hex digits>`.
    pub fn parse(text: &str) -> Result<PublicKey, RegistryError> {
        let invalid = || RegistryError::Invalid(format!("`{text}` is not an ed25519:<hex> key"));
        let hex = text.trim().strip_prefix(KEY_PREFIX).ok_or_else(invalid)?;
        let bytes: [u8; 32] = from_hex(hex).ok_or_else(invalid)?;
        VerifyingKey::from_bytes(&bytes).map_err(|_| invalid())?;
        Ok(PublicKey(bytes))
    }

    /// Whether `signature` (128 hex digits) is this key's signature of `message`. Verification is
    /// strict: weak keys and non-canonical signatures are refused.
    pub fn verifies(&self, message: &[u8], signature: &str) -> bool {
        let (Ok(key), Some(bytes)) = (
            VerifyingKey::from_bytes(&self.0),
            from_hex::<64>(signature.trim()),
        ) else {
            return false;
        };
        key.verify_strict(message, &ed25519_dalek::Signature::from_bytes(&bytes))
            .is_ok()
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{KEY_PREFIX}{}", to_hex(&self.0))
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PublicKey({self})")
    }
}

/// An Ed25519 signing key. Its file holds the secret, so it is created readable by its owner only.
pub struct SecretKey(SigningKey);

impl SecretKey {
    /// A new key, from the operating system's random number generator.
    pub fn generate() -> Result<SecretKey, RegistryError> {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed)
            .map_err(|e| RegistryError::Io(format!("no randomness for a key: {e}")))?;
        Ok(SecretKey(SigningKey::from_bytes(&seed)))
    }

    /// The key from its 32-byte seed.
    pub fn from_seed(seed: [u8; 32]) -> SecretKey {
        SecretKey(SigningKey::from_bytes(&seed))
    }

    /// Its public key.
    pub fn public(&self) -> PublicKey {
        PublicKey(self.0.verifying_key().to_bytes())
    }

    /// Signs `message`; the signature in hex.
    pub fn sign(&self, message: &[u8]) -> String {
        to_hex(&self.0.sign(message).to_bytes())
    }

    /// Reads a key file written by [`SecretKey::write`].
    pub fn read(path: &Path) -> Result<SecretKey, RegistryError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| RegistryError::Io(format!("{}: {e}", path.display())))?;
        let seed = text
            .lines()
            .find_map(|line| line.trim().strip_prefix(SECRET_LINE))
            .and_then(|hex| from_hex::<32>(hex.trim()))
            .ok_or_else(|| {
                RegistryError::Invalid(format!("{} is not an Aqueduct key file", path.display()))
            })?;
        Ok(SecretKey::from_seed(seed))
    }

    /// Writes the key to a new file (never over an existing one), readable by its owner only.
    pub fn write(&self, path: &Path, name: &str) -> Result<(), RegistryError> {
        use std::io::Write;
        let io = |e: std::io::Error| RegistryError::Io(format!("{}: {e}", path.display()));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path).map_err(io)?;
        write!(
            file,
            "# Aqueduct signing key of {name}. Keep it secret; share only the public key.\n\
             # public {}\n\
             {SECRET_LINE}{}\n",
            self.public(),
            to_hex(&self.0.to_bytes())
        )
        .map_err(io)?;
        file.sync_all().map_err(io)
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SecretKey({})", self.public())
    }
}

/// A signature on an approval, and the key that made it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Signature {
    /// The signer's public key.
    pub key: PublicKey,
    /// The signature of the approval statement, in hex.
    pub signature: String,
}

impl Signature {
    /// Parses `ed25519:<key hex>:<signature hex>`, the form `aqueduct versions sign` prints.
    pub fn parse(text: &str) -> Result<Signature, RegistryError> {
        let (key, signature) = text.trim().rsplit_once(':').ok_or_else(|| {
            RegistryError::Invalid("a signature is ed25519:<key hex>:<signature hex>".into())
        })?;
        Ok(Signature {
            key: PublicKey::parse(key)?,
            signature: signature.to_owned(),
        })
    }
}

impl fmt::Display for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.key, self.signature)
    }
}

/// Who holds which keys: the registry's `identities.json`, written by hand and kept under review,
/// since whoever can change it can approve in anyone's name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Identities {
    keys: BTreeMap<String, Vec<PublicKey>>,
}

impl Identities {
    /// Reads an identities file; a missing file means no one has a key.
    pub fn read(path: &Path) -> Result<Identities, RegistryError> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Identities::default()),
            Err(e) => return Err(RegistryError::Io(format!("{}: {e}", path.display()))),
        };
        Identities::parse(&text)
            .map_err(|e| RegistryError::Invalid(format!("{}: {e}", path.display())))
    }

    /// Parses `{"<name>": ["ed25519:<hex>", ...], ...}`.
    pub fn parse(text: &str) -> Result<Identities, String> {
        let value: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let map = value
            .as_object()
            .ok_or("identities are a JSON object of names to lists of keys")?;
        let mut keys = BTreeMap::new();
        for (name, list) in map {
            let list = list
                .as_array()
                .ok_or_else(|| format!("`{name}`: a list of keys"))?
                .iter()
                .map(|key| {
                    key.as_str()
                        .ok_or_else(|| format!("`{name}`: keys are strings"))
                        .and_then(|key| PublicKey::parse(key).map_err(|e| format!("`{name}`: {e}")))
                })
                .collect::<Result<Vec<_>, _>>()?;
            keys.insert(name.clone(), list);
        }
        Ok(Identities { keys })
    }

    /// Adds a key to a person (for building identities in code).
    pub fn with(mut self, name: &str, key: PublicKey) -> Identities {
        self.keys.entry(name.to_owned()).or_default().push(key);
        self
    }

    /// Whether `key` is one of `name`'s keys.
    pub fn trusts(&self, name: &str, key: &PublicKey) -> bool {
        self.keys.get(name).is_some_and(|keys| keys.contains(key))
    }

    /// The keys of `name`.
    pub fn keys(&self, name: &str) -> &[PublicKey] {
        self.keys.get(name).map_or(&[], Vec::as_slice)
    }

    /// Whether no one has a key.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// The text an approval's signature covers. It names, one per line, the workflow, the version (the
/// SHA-256 of its exact source), the approver, the SHA-256 of the replay report approved (or `none`)
/// and the note; free text is JSON-quoted, so no field can run into the next. It does not name the
/// time or the position in the log, so it can be signed away from the registry.
pub fn approval_statement(
    workflow: &str,
    version: &str,
    by: &str,
    replay_sha256: Option<&str>,
    note: Option<&str>,
) -> String {
    let quote = |text: &str| Value::String(text.to_owned()).to_string();
    format!(
        "aqueduct approval v1\nworkflow {}\nversion {version}\napprover {}\nreplay {}\nnote {}\n",
        quote(workflow),
        quote(by),
        replay_sha256.unwrap_or("none"),
        note.map_or("null".to_owned(), quote),
    )
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn from_hex<const N: usize>(hex: &str) -> Option<[u8; N]> {
    if hex.len() != 2 * N || !hex.is_ascii() {
        return None;
    }
    let mut out = [0u8; N];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_signature_verifies_only_its_statement_and_key() {
        let key = SecretKey::from_seed([7; 32]);
        let statement = approval_statement("claims", "ab", "grace", Some("cd"), Some("ok"));
        let signature = key.sign(statement.as_bytes());
        assert!(key.public().verifies(statement.as_bytes(), &signature));
        let other = approval_statement("claims", "ab", "grace", Some("ce"), Some("ok"));
        assert!(!key.public().verifies(other.as_bytes(), &signature));
        let stranger = SecretKey::from_seed([8; 32]);
        assert!(!stranger.public().verifies(statement.as_bytes(), &signature));
        assert!(!key.public().verifies(statement.as_bytes(), "zz"));

        let text = Signature {
            key: key.public(),
            signature: signature.clone(),
        }
        .to_string();
        let parsed = Signature::parse(&text).unwrap();
        assert_eq!((parsed.key, parsed.signature), (key.public(), signature));
        assert_eq!(
            PublicKey::parse(&key.public().to_string()).unwrap(),
            key.public()
        );
    }

    #[test]
    fn free_text_cannot_forge_another_field() {
        // A note with a newline is quoted, so it cannot pose as the replay line.
        let sneaky = approval_statement("claims", "ab", "grace", None, Some("x\nreplay cd"));
        assert_eq!(sneaky.lines().count(), 6);
        assert!(sneaky.contains("replay none\n"));
    }

    #[test]
    fn identities_name_keys() {
        let key = SecretKey::from_seed([7; 32]).public();
        let identities = Identities::parse(&format!(r#"{{"grace": ["{key}"]}}"#)).unwrap();
        assert!(identities.trusts("grace", &key));
        assert!(!identities.trusts("linus", &key));
        assert!(Identities::parse(r#"{"grace": ["ed25519:00"]}"#).is_err());
    }

    #[test]
    fn a_key_file_round_trips() {
        let path = std::env::temp_dir().join(format!("aqueduct-key-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let key = SecretKey::generate().unwrap();
        key.write(&path, "grace").unwrap();
        assert!(key.write(&path, "grace").is_err(), "never overwrites a key");
        assert_eq!(SecretKey::read(&path).unwrap().public(), key.public());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        std::fs::remove_file(&path).unwrap();
    }
}
