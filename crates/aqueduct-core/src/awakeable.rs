//! Awakeables: waits completed from outside by an id that names the run and the command.
//!
//! An awakeable is a signal wait under a reserved name, `$awakeable/<command index>`. Its id,
//! `<run key>#<command index>` (`<run key>#<generation>.<command index>` for a later generation of the
//! key, ADR 0022), carries everything needed to complete it, so whoever holds the id (a
//! person, a webhook, another system) can complete it without knowing the run's signal names. The id is
//! also the signal's deduplication id: completing an awakeable twice records it once.

use crate::journal::CommandIndex;

/// The prefix of the signal names awakeables wait for. Signal names starting with it are reserved.
pub const AWAKEABLE_PREFIX: &str = "$awakeable/";

/// The signal name the awakeable at this command waits for.
pub fn awakeable_signal(cmd: CommandIndex) -> String {
    format!("{AWAKEABLE_PREFIX}{}", cmd.0)
}

/// The id of the awakeable (or escalation) at this command of the run with this run id.
pub fn awakeable_id(run: &str, cmd: CommandIndex) -> String {
    match crate::split_run_id(run) {
        (key, 0) => format!("{key}#{}", cmd.0),
        (key, generation) => format!("{key}#{generation}.{}", cmd.0),
    }
}

/// Splits an awakeable id into the run id and the signal name to send, or `None` if it is not one.
///
/// ```
/// use aqueduct_core::{CommandIndex, awakeable_id, parse_awakeable_id, run_id};
/// let id = awakeable_id("orders#7", CommandIndex(3));
/// assert_eq!(id, "orders#7#3");
/// assert_eq!(parse_awakeable_id(&id), Some(("orders#7".to_owned(), "$awakeable/3".to_owned())));
/// assert_eq!(awakeable_id(&run_id("orders", 2), CommandIndex(3)), "orders#2.3");
/// assert_eq!(parse_awakeable_id("no command"), None);
/// ```
pub fn parse_awakeable_id(id: &str) -> Option<(String, String)> {
    let (run, cmd) = parse_command_id(id)?;
    Some((run, awakeable_signal(cmd)))
}

/// Splits an id of the form `<run key>#<command index>` or `<run key>#<generation>.<command index>`
/// (awakeables, escalations) into the run id and the command.
///
/// ```
/// use aqueduct_core::{CommandIndex, parse_command_id, run_id};
/// assert_eq!(parse_command_id("claims:7#4"), Some(("claims:7".to_owned(), CommandIndex(4))));
/// assert_eq!(parse_command_id("claims:7#2.4"), Some((run_id("claims:7", 2), CommandIndex(4))));
/// assert_eq!(parse_command_id("#4"), None);
/// ```
pub fn parse_command_id(id: &str) -> Option<(String, CommandIndex)> {
    let (key, tail) = id.rsplit_once('#')?;
    let (generation, cmd) = match tail.split_once('.') {
        Some((generation, cmd)) => (generation.parse::<u32>().ok()?, cmd),
        None => (0, tail),
    };
    let cmd = cmd.parse::<u32>().ok()?;
    if !crate::valid_key(key) {
        return None;
    }
    Some((crate::run_id(key, generation), CommandIndex(cmd)))
}
