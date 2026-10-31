//! Awakeables: waits completed from outside by an id that names the run and the command.
//!
//! An awakeable is a signal wait under a reserved name, `$awakeable/<command index>`. Its id,
//! `<run key>#<command index>`, carries everything needed to complete it, so whoever holds the id (a
//! person, a webhook, another system) can complete it without knowing the run's signal names. The id is
//! also the signal's deduplication id: completing an awakeable twice records it once.

use crate::journal::CommandIndex;

/// The prefix of the signal names awakeables wait for. Signal names starting with it are reserved.
pub const AWAKEABLE_PREFIX: &str = "$awakeable/";

/// The signal name the awakeable at this command waits for.
pub fn awakeable_signal(cmd: CommandIndex) -> String {
    format!("{AWAKEABLE_PREFIX}{}", cmd.0)
}

/// The id of the awakeable at this command of this run.
pub fn awakeable_id(key: &str, cmd: CommandIndex) -> String {
    format!("{key}#{}", cmd.0)
}

/// Splits an awakeable id into the run key and the signal name to send, or `None` if it is not one.
///
/// ```
/// use aqueduct_core::{CommandIndex, awakeable_id, parse_awakeable_id};
/// let id = awakeable_id("orders#7", CommandIndex(3));
/// assert_eq!(id, "orders#7#3");
/// assert_eq!(parse_awakeable_id(&id), Some(("orders#7", "$awakeable/3".to_owned())));
/// assert_eq!(parse_awakeable_id("no command"), None);
/// ```
pub fn parse_awakeable_id(id: &str) -> Option<(&str, String)> {
    let (key, cmd) = parse_command_id(id)?;
    Some((key, awakeable_signal(cmd)))
}

/// Splits an id of the form `<run key>#<command index>` (awakeables, escalations).
///
/// ```
/// use aqueduct_core::{CommandIndex, parse_command_id};
/// assert_eq!(parse_command_id("claims:7#4"), Some(("claims:7", CommandIndex(4))));
/// assert_eq!(parse_command_id("#4"), None);
/// ```
pub fn parse_command_id(id: &str) -> Option<(&str, CommandIndex)> {
    let (key, cmd) = id.rsplit_once('#')?;
    let cmd = cmd.parse::<u32>().ok()?;
    if key.is_empty() {
        return None;
    }
    Some((key, CommandIndex(cmd)))
}
