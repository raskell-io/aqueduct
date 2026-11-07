//! Hash slots (ADR 0011): every run key and object key maps to one of [`SLOTS`] slots, and slots map to
//! partitions. The function is part of the format: it never changes for a given cluster.

use sha2::{Digest, Sha256};

/// How many slots there are.
pub const SLOTS: u16 = 4096;

/// The slot of a key: `u64_be(SHA-256("aqueduct-slot-v1" || 0x00 || key)[0..8]) mod 4096`.
///
/// ```
/// let slot = aqueduct_codec::slot("orders:7");
/// assert!(slot < aqueduct_codec::SLOTS);
/// assert_eq!(slot, aqueduct_codec::slot("orders:7"));
/// ```
pub fn slot(key: &str) -> u16 {
    let mut hasher = Sha256::new();
    hasher.update(b"aqueduct-slot-v1");
    hasher.update([0u8]);
    hasher.update(key.as_bytes());
    let digest = hasher.finalize();
    let mut first = [0u8; 8];
    first.copy_from_slice(&digest[..8]);
    // The remainder is below 4,096, so it fits.
    (u64::from_be_bytes(first) % u64::from(SLOTS)) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Computed independently (Python's hashlib): the function must never change.
    #[test]
    fn the_function_is_pinned() {
        assert_eq!(slot("orders:7"), 195);
        assert_eq!(slot(""), 1381);
        assert_eq!(slot("run-0"), 374);
    }

    #[test]
    fn slots_are_spread_and_fixed() {
        let mut counts = vec![0u32; usize::from(SLOTS)];
        for i in 0..100_000 {
            counts[usize::from(slot(&format!("run-{i}")))] += 1;
        }
        // About 24 keys per slot; none empty, none crowded.
        assert!(
            counts.iter().all(|c| (3..=60).contains(c)),
            "{:?}",
            counts.iter().max()
        );
    }
}
