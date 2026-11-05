//! Namespaces (ADR 0046): tenants' runs, kept apart by their keys.
//!
//! A tenant's run key `k` is stored as `$ns/<namespace>/k`; the default namespace has no prefix, so a
//! data directory from before namespaces is the default namespace's. What a run derives from its key
//! (children, continued runs) stays in its namespace by construction, and keys that logic chooses are
//! scoped here, so a procedure cannot reach another tenant's runs.

use crate::objects::is_reserved_key;

/// The namespace of keys without a prefix.
pub const DEFAULT: &str = "default";

const MARK: &str = "$ns/";

/// Whether a name can be a namespace: letters, digits, `-` and `_`.
pub fn valid_namespace(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// A key in a namespace, as stored.
pub fn scoped(namespace: &str, key: &str) -> String {
    if namespace == DEFAULT {
        key.to_owned()
    } else {
        format!("{MARK}{namespace}/{key}")
    }
}

/// A stored key as its namespace's clients know it.
pub fn unscoped<'a>(namespace: &str, key: &'a str) -> &'a str {
    key.strip_prefix(MARK)
        .and_then(|rest| rest.strip_prefix(namespace))
        .and_then(|rest| rest.strip_prefix('/'))
        .unwrap_or(key)
}

/// The namespace a stored key is in: of a tenant's run, or of an object key's loop or call.
pub fn namespace_of(key: &str) -> &str {
    let rest = if let Some(rest) = key.strip_prefix(MARK) {
        rest
    } else if is_reserved_key(key) {
        match key.find("/$ns/") {
            Some(at) => &key[at + 1 + MARK.len()..],
            None => return DEFAULT,
        }
    } else {
        return DEFAULT;
    };
    rest.split_once('/').map_or(DEFAULT, |(name, _)| name)
}

/// Whether text from outside could name a namespace, which only the engine may write.
pub fn names_namespace(text: &str) -> bool {
    text.contains(MARK)
}

/// The run key of a child or message target that logic chose, for a sender with this run key: in the
/// sender's namespace. `None` if the key is one logic may not name: it could leave the namespace, or
/// reach an object's internal runs.
pub fn chosen_key(sender: &str, key: &str) -> Option<String> {
    // An object key's loop starts its calls, which are under reserved keys, in its own namespace. Only
    // the engine runs loops, and it derives these keys (ADR 0043).
    if sender.starts_with("$object/")
        && key.starts_with("$call/")
        && namespace_of(key) == namespace_of(sender)
    {
        return Some(key.to_owned());
    }
    if names_namespace(key) || is_reserved_key(key) {
        return None;
    }
    Some(scoped(namespace_of(sender), key))
}

/// Which namespaces a worker takes work from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scope {
    /// Every namespace: a shared worker pool.
    All,
    /// One namespace.
    One(String),
}

impl Scope {
    /// Whether a stored run key is in scope.
    pub fn contains(&self, run: &str) -> bool {
        self.includes(namespace_of(run))
    }

    /// Whether a namespace is in scope.
    pub fn includes(&self, namespace: &str) -> bool {
        match self {
            Scope::All => true,
            Scope::One(one) => one == namespace,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_know_their_namespace() {
        assert_eq!(namespace_of("k"), DEFAULT);
        assert_eq!(namespace_of("$ns/acme/k"), "acme");
        // What a run derives from its key stays in its namespace.
        assert_eq!(namespace_of("$ns/acme/k#3"), "acme");
        assert_eq!(namespace_of("$ns/acme/k\x002"), "acme");
        // An object key's loop and calls.
        assert_eq!(namespace_of("$object/o/k"), DEFAULT);
        assert_eq!(namespace_of("$call/o/k/id"), DEFAULT);
        assert_eq!(
            namespace_of(&format!("$object/o/{}", scoped("acme", "k"))),
            "acme"
        );
        assert_eq!(
            namespace_of(&format!("$call/o/{}/id", scoped("acme", "k"))),
            "acme"
        );
        assert_eq!(unscoped("acme", "$ns/acme/k"), "k");
        assert_eq!(unscoped("acme", "$ns/globex/k"), "$ns/globex/k");
        assert_eq!(unscoped("default", "k"), "k");
    }

    #[test]
    fn logic_cannot_choose_a_key_outside_its_namespace() {
        assert_eq!(chosen_key("r", "x").unwrap(), "x");
        assert_eq!(chosen_key("$ns/acme/r", "x").unwrap(), "$ns/acme/x");
        assert_eq!(
            chosen_key("$call/o/$ns/acme/k/id", "x").unwrap(),
            "$ns/acme/x"
        );
        // A loop starts its calls, in its namespace only.
        assert_eq!(
            chosen_key("$object/o/$ns/acme/k", "$call/o/$ns/acme/k/i").unwrap(),
            "$call/o/$ns/acme/k/i"
        );
        assert_eq!(
            chosen_key("$object/o/$ns/acme/k", "$call/o/$ns/globex/k/i"),
            None
        );
        assert_eq!(chosen_key("$object/o/$ns/acme/k", "$call/o/k/i"), None);
        // Anything else may not name a call, even its own handler's.
        assert_eq!(chosen_key("$call/o/k/i", "$call/o/k/j"), None);
        for key in [
            "$ns/globex/x",
            "a/$ns/globex/x",
            "$object/o/k",
            "$call/o/k/i",
        ] {
            assert_eq!(chosen_key("$ns/acme/r", key), None, "{key}");
            assert_eq!(chosen_key("r", key), None, "{key}");
        }
    }

    #[test]
    fn names_and_scopes() {
        assert!(valid_namespace("acme-1_x"));
        for bad in ["", "a/b", "*", "$ns", "a b"] {
            assert!(!valid_namespace(bad), "{bad}");
        }
        assert!(Scope::All.contains("$ns/acme/k"));
        assert!(Scope::One("acme".into()).contains("$ns/acme/k"));
        assert!(!Scope::One("acme".into()).contains("$ns/globex/k"));
        assert!(!Scope::One("acme".into()).contains("k"));
        assert!(Scope::One(DEFAULT.into()).contains("k"));
    }
}
