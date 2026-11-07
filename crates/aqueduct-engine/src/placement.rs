//! Where runs live (ADR 0052): a key's home, its slot (ADR 0011), and the partition the slot table gives.

use std::sync::Arc;

use aqueduct_core::split_run_id;

/// Tells a partition whether a home is its own.
pub type Placement = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// The home of a run key or run id: where it lives. A key up to its first `#`, so a run's derived children
/// and awakeables live with it. An object key's loop and its calls live at `<object>/<key>`: object names
/// and keys hold no `/` ([`crate::objects::valid_object_key`]), so a call's run key says whose it is.
///
/// ```
/// use aqueduct_engine::placement::home;
/// assert_eq!(home("orders:7"), "orders:7");
/// assert_eq!(home("orders:7#3#1"), "orders:7");
/// assert_eq!(home("orders:7\u{0}2"), "orders:7");
/// assert_eq!(home("$object/cart/a#b"), "cart/a#b");
/// assert_eq!(home("$call/cart/a#b/run-1#4"), "cart/a#b");
/// assert_eq!(home("$object/cart/$ns/acme/a"), "cart/$ns/acme/a");
/// assert_eq!(home("$call/cart/$ns/acme/a/$ns/acme/r#2\u{0}1"), "cart/$ns/acme/a");
/// ```
pub fn home(key: &str) -> &str {
    let key = split_run_id(key).0;
    if let Some(object) = key.strip_prefix("$object/") {
        return object;
    }
    if let Some(call) = key.strip_prefix("$call/") {
        return loop_of_call(call).unwrap_or(call);
    }
    key.split_once('#').map_or(key, |(home, _)| home)
}

/// `<object>/<key>` of a call's `<object>/<key>/<id>`, where the key may be `$ns/<namespace>/<key>`.
fn loop_of_call(call: &str) -> Option<&str> {
    let object = call.find('/')?;
    let rest = &call[object + 1..];
    let skip = if let Some(scoped) = rest.strip_prefix("$ns/") {
        "$ns/".len() + scoped.find('/')? + 1
    } else {
        0
    };
    let key = skip + rest[skip..].find('/')?;
    Some(&call[..object + 1 + key])
}

/// Which partition each slot belongs to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotTable {
    owners: Vec<u32>,
}

impl SlotTable {
    /// Slots split into `partitions` contiguous ranges of nearly equal size.
    pub fn even(partitions: u32) -> SlotTable {
        let partitions = partitions.max(1);
        let slots = u32::from(aqueduct_codec::SLOTS);
        SlotTable {
            owners: (0..slots).map(|slot| slot * partitions / slots).collect(),
        }
    }

    /// The partition a slot belongs to.
    pub fn owner(&self, slot: u16) -> u32 {
        self.owners[usize::from(slot)]
    }

    /// The partition a key or run id lives on.
    pub fn partition_of(&self, key: &str) -> u32 {
        self.owner(aqueduct_codec::slot(home(key)))
    }

    /// How many partitions own slots.
    pub fn partitions(&self) -> u32 {
        self.owners.iter().max().map_or(0, |max| max + 1)
    }

    /// The placement of partition `partition`: whether a home is its own.
    pub fn placement(&self, partition: u32) -> Placement {
        let table = self.clone();
        Arc::new(move |home: &str| table.owner(aqueduct_codec::slot(home)) == partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_even_table_gives_every_partition_its_share() {
        let table = SlotTable::even(64);
        assert_eq!(table.partitions(), 64);
        for p in 0..64 {
            assert_eq!(table.owners.iter().filter(|o| **o == p).count(), 64);
        }
        // A run, its children, successors and awakeables are on one partition.
        let p = table.partition_of("order-17");
        assert_eq!(table.partition_of("order-17#3"), p);
        assert_eq!(table.partition_of("order-17#3#0\u{0}4"), p);
        assert!(table.placement(p)(home("order-17#9")));
    }
}
