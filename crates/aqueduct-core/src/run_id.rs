//! Run ids: which run of a key (ADR 0022).
//!
//! A run key can have several runs over time, one after another (continue-as-new, or a start allowed after
//! the previous run ended). Each is a **generation**: 0 for the first. A run's id is its key for generation
//! 0, so histories written before generations existed keep their ids, and `<key>\0<generation>` after
//! that. Keys must not contain NUL (see [`valid_key`]), so ids are unambiguous.

/// Whether a run key is valid: not empty, and without NUL (which separates the generation in run ids).
pub fn valid_key(key: &str) -> bool {
    !key.is_empty() && !key.contains('\0')
}

/// The id of a key's run in a generation.
///
/// ```
/// use aqueduct_core::{run_id, split_run_id};
/// assert_eq!(run_id("orders:7", 0), "orders:7");
/// assert_eq!(run_id("orders:7", 2), "orders:7\u{0}2");
/// assert_eq!(split_run_id("orders:7\u{0}2"), ("orders:7", 2));
/// assert_eq!(split_run_id("orders:7"), ("orders:7", 0));
/// ```
pub fn run_id(key: &str, generation: u32) -> String {
    if generation == 0 {
        key.to_owned()
    } else {
        format!("{key}\0{generation}")
    }
}

/// Splits a run id into its key and generation.
pub fn split_run_id(id: &str) -> (&str, u32) {
    match id.split_once('\0') {
        Some((key, generation)) => (key, generation.parse().unwrap_or(0)),
        None => (id, 0),
    }
}
