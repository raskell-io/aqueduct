//! Where runs live (ADR 0052): a key's home, its slot (ADR 0011), and the partition the slot table gives.

use std::sync::Arc;

use aqueduct_core::split_run_id;

/// Tells a partition whether a home is its own.
pub type Placement = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// The home of a run key or run id: where it lives. A key up to its first `#`, so a run's derived children
/// and awakeables live with it; an object's loop and its calls at the loop's key.
///
/// ```
/// use aqueduct_engine::placement::home;
/// assert_eq!(home("orders:7"), "orders:7");
/// assert_eq!(home("orders:7#3#1"), "orders:7");
/// assert_eq!(home("orders:7\u{0}2"), "orders:7");
/// assert_eq!(home("$object/cart/a#b"), "$object/cart/a#b");
/// ```
pub fn home(key: &str) -> &str {
    let key = split_run_id(key).0;
    if key.starts_with("$object/") || key.starts_with("$call/") {
        return key;
    }
    key.split_once('#').map_or(key, |(home, _)| home)
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
