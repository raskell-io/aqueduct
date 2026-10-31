//! Primitive encodings from `docs/history-format.md`, section 1.

use crate::{DecodeError, Hash};

pub(crate) fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value & 0x7f) as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub(crate) fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Reader { bytes, pos: 0 }
    }

    pub(crate) fn u8(&mut self) -> Result<u8, DecodeError> {
        let byte = *self.bytes.get(self.pos).ok_or(DecodeError::UnexpectedEnd)?;
        self.pos += 1;
        Ok(byte)
    }

    pub(crate) fn varint(&mut self) -> Result<u64, DecodeError> {
        let mut value = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = self.u8()?;
            let group = u64::from(byte & 0x7f);
            if shift == 63 && byte > 1 {
                return Err(DecodeError::Overflow);
            }
            value |= group << shift;
            if byte & 0x80 == 0 {
                if byte == 0 && shift > 0 {
                    return Err(DecodeError::NonCanonicalVarint);
                }
                return Ok(value);
            }
            shift += 7;
        }
    }

    pub(crate) fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = usize::try_from(self.varint()?).map_err(|_| DecodeError::Overflow)?;
        let end = self.pos.checked_add(len).ok_or(DecodeError::Overflow)?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or(DecodeError::UnexpectedEnd)?;
        self.pos = end;
        Ok(slice)
    }

    pub(crate) fn str(&mut self) -> Result<&'a str, DecodeError> {
        std::str::from_utf8(self.bytes()?).map_err(|_| DecodeError::InvalidUtf8)
    }

    pub(crate) fn hash(&mut self) -> Result<Hash, DecodeError> {
        let end = self.pos.checked_add(32).ok_or(DecodeError::Overflow)?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or(DecodeError::UnexpectedEnd)?;
        let mut hash = [0u8; 32];
        hash.copy_from_slice(slice);
        self.pos = end;
        Ok(hash)
    }

    pub(crate) fn finish(&self) -> Result<(), DecodeError> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(DecodeError::TrailingBytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(bytes: &[u8]) -> Result<u64, DecodeError> {
        let mut reader = Reader::new(bytes);
        let value = reader.varint()?;
        reader.finish()?;
        Ok(value)
    }

    #[test]
    fn varint_round_trips_at_boundaries() {
        for value in [
            0,
            1,
            127,
            128,
            16_383,
            16_384,
            u64::from(u32::MAX),
            u64::MAX - 1,
            u64::MAX,
        ] {
            let mut out = Vec::new();
            put_varint(&mut out, value);
            assert_eq!(decode(&out), Ok(value), "value {value}");
        }
    }

    #[test]
    fn varint_rejects_non_canonical_and_overflow() {
        assert_eq!(decode(&[0x80, 0x00]), Err(DecodeError::NonCanonicalVarint));
        assert_eq!(
            decode(&[0xff, 0x80, 0x00]),
            Err(DecodeError::NonCanonicalVarint)
        );
        let mut too_big = vec![0xff; 9];
        too_big.push(0x02);
        assert_eq!(decode(&too_big), Err(DecodeError::Overflow));
        assert_eq!(decode(&[0xff; 11]), Err(DecodeError::Overflow));
        assert_eq!(decode(&[0x80]), Err(DecodeError::UnexpectedEnd));
    }
}
