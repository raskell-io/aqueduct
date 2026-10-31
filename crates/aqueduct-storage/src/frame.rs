//! Record framing: `len:u32 LE | crc32(payload):u32 LE | payload`.

use crate::StorageError;

/// The largest payload a frame may carry (64 MiB). Larger lengths are treated as corruption.
pub const MAX_PAYLOAD: usize = 64 << 20;

const HEADER: usize = 8;

/// Frames a payload.
pub fn encode(payload: &[u8]) -> Result<Vec<u8>, StorageError> {
    if payload.len() > MAX_PAYLOAD {
        return Err(StorageError::RecordTooLarge(payload.len()));
    }
    let len =
        u32::try_from(payload.len()).map_err(|_| StorageError::RecordTooLarge(payload.len()))?;
    let mut out = Vec::with_capacity(HEADER + payload.len());
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

/// The result of scanning framed bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decoded {
    /// Payloads of the valid frames, in order.
    pub payloads: Vec<Vec<u8>>,
    /// The length of the valid prefix. Everything after it is torn or corrupt.
    pub valid_len: usize,
}

/// Scans framed bytes and returns the longest valid prefix of frames.
pub fn decode_all(bytes: &[u8]) -> Decoded {
    let mut payloads = Vec::new();
    let mut pos = 0;
    while let Some((payload, next)) = decode_at(bytes, pos) {
        payloads.push(payload.to_vec());
        pos = next;
    }
    Decoded {
        payloads,
        valid_len: pos,
    }
}

fn decode_at(bytes: &[u8], pos: usize) -> Option<(&[u8], usize)> {
    let len = usize::try_from(read_u32(bytes, pos)?).ok()?;
    if len > MAX_PAYLOAD {
        return None;
    }
    let crc = read_u32(bytes, pos + 4)?;
    let start = pos + HEADER;
    let payload = bytes.get(start..start.checked_add(len)?)?;
    (crc32fast::hash(payload) == crc).then_some((payload, start + len))
}

fn read_u32(bytes: &[u8], pos: usize) -> Option<u32> {
    let slice = bytes.get(pos..pos.checked_add(4)?)?;
    let mut word = [0u8; 4];
    word.copy_from_slice(slice);
    Some(u32::from_le_bytes(word))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn framed(payloads: &[Vec<u8>]) -> Vec<u8> {
        payloads.iter().flat_map(|p| encode(p).unwrap()).collect()
    }

    proptest! {
        #[test]
        fn round_trip(payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..50), 0..10)) {
            let bytes = framed(&payloads);
            let decoded = decode_all(&bytes);
            prop_assert_eq!(decoded.valid_len, bytes.len());
            prop_assert_eq!(decoded.payloads, payloads);
        }

        #[test]
        fn torn_tail_yields_a_prefix(
            payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..50), 1..10),
            cut in any::<prop::sample::Index>(),
        ) {
            let bytes = framed(&payloads);
            let cut = cut.index(bytes.len());
            let decoded = decode_all(&bytes[..cut]);
            prop_assert!(decoded.valid_len <= cut);
            prop_assert_eq!(&decoded.payloads[..], &payloads[..decoded.payloads.len()]);
        }

        #[test]
        fn corruption_yields_a_prefix(
            payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..50), 1..10),
            at in any::<prop::sample::Index>(),
            flip in 1u8..=255,
        ) {
            let mut bytes = framed(&payloads);
            let at = at.index(bytes.len());
            bytes[at] ^= flip;
            // The frame containing the flipped byte, and everything after it, is dropped.
            let mut frame_start = 0;
            let mut damaged = 0;
            for (i, p) in payloads.iter().enumerate() {
                let frame_end = frame_start + HEADER + p.len();
                if at < frame_end {
                    damaged = i;
                    break;
                }
                frame_start = frame_end;
            }
            let decoded = decode_all(&bytes);
            prop_assert_eq!(decoded.valid_len, frame_start);
            prop_assert_eq!(&decoded.payloads[..], &payloads[..damaged]);
        }
    }
}
