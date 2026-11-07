//! Bytes for Raft's types: what a driver persists ([`Entry`], [`HardState`], [`Snapshot`]) and sends
//! ([`Envelope`]). Integers are LEB128 varints, byte strings and lists are length-prefixed, and every
//! top-level value starts with a version byte, so a later encoding can be told apart.

use crate::{Entry, Envelope, HardState, Message, NodeId, Snapshot};

/// The version byte of this encoding.
const VERSION: u8 = 1;

/// Why bytes did not decode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodeError(pub String);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "raft bytes do not decode: {}", self.0)
    }
}

impl std::error::Error for DecodeError {}

struct Writer(Vec<u8>);

impl Writer {
    fn new() -> Writer {
        Writer(vec![VERSION])
    }

    fn u64(&mut self, mut value: u64) {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                self.0.push(byte);
                return;
            }
            self.0.push(byte | 0x80);
        }
    }

    fn bool(&mut self, value: bool) {
        self.0.push(u8::from(value));
    }

    fn bytes(&mut self, value: &[u8]) {
        self.u64(value.len() as u64);
        self.0.extend_from_slice(value);
    }

    fn ids(&mut self, ids: &[NodeId]) {
        self.u64(ids.len() as u64);
        for id in ids {
            self.u64(*id);
        }
    }

    fn entry(&mut self, entry: &Entry) {
        self.u64(entry.term);
        self.u64(entry.index);
        self.bytes(&entry.data);
        match &entry.members {
            None => self.bool(false),
            Some(members) => {
                self.bool(true);
                self.ids(members);
            }
        }
    }

    fn snapshot(&mut self, snapshot: &Snapshot) {
        self.u64(snapshot.index);
        self.u64(snapshot.term);
        self.bytes(&snapshot.data);
        self.ids(&snapshot.members);
    }
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Result<Reader<'a>, DecodeError> {
        match data.split_first() {
            Some((&VERSION, rest)) => Ok(Reader(rest)),
            Some((version, _)) => Err(DecodeError(format!("unknown version {version}"))),
            None => Err(DecodeError("empty".into())),
        }
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let (&byte, rest) = self
                .0
                .split_first()
                .ok_or_else(|| DecodeError("ends inside a number".into()))?;
            self.0 = rest;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(DecodeError("a number longer than 64 bits".into()))
    }

    fn bool(&mut self) -> Result<bool, DecodeError> {
        match self.u64()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(DecodeError(format!("{other} is not a boolean"))),
        }
    }

    fn len(&mut self) -> Result<usize, DecodeError> {
        let len = usize::try_from(self.u64()?).map_err(|e| DecodeError(e.to_string()))?;
        if len > self.0.len() {
            return Err(DecodeError("a length beyond the end".into()));
        }
        Ok(len)
    }

    fn bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let len = self.len()?;
        let (value, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(value.to_vec())
    }

    fn ids(&mut self) -> Result<Vec<NodeId>, DecodeError> {
        let len = self.len()?;
        (0..len).map(|_| self.u64()).collect()
    }

    fn entry(&mut self) -> Result<Entry, DecodeError> {
        Ok(Entry {
            term: self.u64()?,
            index: self.u64()?,
            data: self.bytes()?,
            members: if self.bool()? {
                Some(self.ids()?)
            } else {
                None
            },
        })
    }

    fn snapshot(&mut self) -> Result<Snapshot, DecodeError> {
        Ok(Snapshot {
            index: self.u64()?,
            term: self.u64()?,
            data: self.bytes()?,
            members: self.ids()?,
        })
    }

    fn end(self) -> Result<(), DecodeError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(DecodeError(format!("{} bytes after the end", self.0.len())))
        }
    }
}

/// An entry's bytes.
pub fn encode_entry(entry: &Entry) -> Vec<u8> {
    let mut w = Writer::new();
    w.entry(entry);
    w.0
}

/// An entry from [`encode_entry`].
pub fn decode_entry(data: &[u8]) -> Result<Entry, DecodeError> {
    let mut r = Reader::new(data)?;
    let entry = r.entry()?;
    r.end()?;
    Ok(entry)
}

/// A hard state's bytes.
pub fn encode_hard_state(state: &HardState) -> Vec<u8> {
    let mut w = Writer::new();
    w.u64(state.term);
    match state.vote {
        None => w.bool(false),
        Some(vote) => {
            w.bool(true);
            w.u64(vote);
        }
    }
    w.u64(state.commit);
    w.0
}

/// A hard state from [`encode_hard_state`].
pub fn decode_hard_state(data: &[u8]) -> Result<HardState, DecodeError> {
    let mut r = Reader::new(data)?;
    let term = r.u64()?;
    let vote = if r.bool()? { Some(r.u64()?) } else { None };
    let commit = r.u64()?;
    r.end()?;
    Ok(HardState { term, vote, commit })
}

/// A snapshot's bytes.
pub fn encode_snapshot(snapshot: &Snapshot) -> Vec<u8> {
    let mut w = Writer::new();
    w.snapshot(snapshot);
    w.0
}

/// A snapshot from [`encode_snapshot`].
pub fn decode_snapshot(data: &[u8]) -> Result<Snapshot, DecodeError> {
    let mut r = Reader::new(data)?;
    let snapshot = r.snapshot()?;
    r.end()?;
    Ok(snapshot)
}

/// A message and its addresses, for the network.
pub fn encode_envelope(envelope: &Envelope) -> Vec<u8> {
    let mut w = Writer::new();
    w.u64(envelope.from);
    w.u64(envelope.to);
    match &envelope.message {
        Message::RequestVote {
            term,
            last_index,
            last_term,
            pre_vote,
        } => {
            w.u64(0);
            w.u64(*term);
            w.u64(*last_index);
            w.u64(*last_term);
            w.bool(*pre_vote);
        }
        Message::Vote {
            term,
            granted,
            pre_vote,
        } => {
            w.u64(1);
            w.u64(*term);
            w.bool(*granted);
            w.bool(*pre_vote);
        }
        Message::Append {
            term,
            prev_index,
            prev_term,
            entries,
            commit,
        } => {
            w.u64(2);
            w.u64(*term);
            w.u64(*prev_index);
            w.u64(*prev_term);
            w.u64(entries.len() as u64);
            for entry in entries {
                w.entry(entry);
            }
            w.u64(*commit);
        }
        Message::InstallSnapshot { term, snapshot } => {
            w.u64(3);
            w.u64(*term);
            w.snapshot(snapshot);
        }
        Message::AppendReply {
            term,
            success,
            index,
        } => {
            w.u64(4);
            w.u64(*term);
            w.bool(*success);
            w.u64(*index);
        }
    }
    w.0
}

/// An envelope from [`encode_envelope`].
pub fn decode_envelope(data: &[u8]) -> Result<Envelope, DecodeError> {
    let mut r = Reader::new(data)?;
    let from = r.u64()?;
    let to = r.u64()?;
    let message = match r.u64()? {
        0 => Message::RequestVote {
            term: r.u64()?,
            last_index: r.u64()?,
            last_term: r.u64()?,
            pre_vote: r.bool()?,
        },
        1 => Message::Vote {
            term: r.u64()?,
            granted: r.bool()?,
            pre_vote: r.bool()?,
        },
        2 => {
            let term = r.u64()?;
            let prev_index = r.u64()?;
            let prev_term = r.u64()?;
            let count = r.len()?;
            let entries = (0..count)
                .map(|_| r.entry())
                .collect::<Result<Vec<_>, _>>()?;
            Message::Append {
                term,
                prev_index,
                prev_term,
                entries,
                commit: r.u64()?,
            }
        }
        3 => Message::InstallSnapshot {
            term: r.u64()?,
            snapshot: r.snapshot()?,
        },
        4 => Message::AppendReply {
            term: r.u64()?,
            success: r.bool()?,
            index: r.u64()?,
        },
        other => return Err(DecodeError(format!("unknown message kind {other}"))),
    };
    r.end()?;
    Ok(Envelope { from, to, message })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64) -> Entry {
        Entry {
            term: 3,
            index,
            data: vec![0, 1, 2, 255],
            members: index.is_multiple_of(2).then(|| vec![1, 2, 300]),
        }
    }

    #[test]
    fn everything_survives_the_round_trip_and_damage_is_refused() {
        let snapshot = Snapshot {
            index: 9,
            term: 2,
            data: b"state".to_vec(),
            members: vec![1, 2, 3],
        };
        let messages = [
            Message::RequestVote {
                term: u64::MAX,
                last_index: 7,
                last_term: 2,
                pre_vote: true,
            },
            Message::Vote {
                term: 4,
                granted: true,
                pre_vote: false,
            },
            Message::Append {
                term: 4,
                prev_index: 9,
                prev_term: 2,
                entries: vec![entry(10), entry(11)],
                commit: 9,
            },
            Message::InstallSnapshot {
                term: 4,
                snapshot: snapshot.clone(),
            },
            Message::AppendReply {
                term: 4,
                success: false,
                index: 3,
            },
        ];
        for message in messages {
            let envelope = Envelope {
                from: 1,
                to: 2,
                message,
            };
            let bytes = encode_envelope(&envelope);
            assert_eq!(decode_envelope(&bytes), Ok(envelope));
            for cut in 0..bytes.len() {
                assert!(decode_envelope(&bytes[..cut]).is_err(), "cut at {cut}");
            }
        }
        assert_eq!(decode_entry(&encode_entry(&entry(4))), Ok(entry(4)));
        assert_eq!(decode_snapshot(&encode_snapshot(&snapshot)), Ok(snapshot));
        for state in [
            HardState::default(),
            HardState {
                term: 5,
                vote: Some(2),
                commit: 70,
            },
        ] {
            assert_eq!(decode_hard_state(&encode_hard_state(&state)), Ok(state));
        }
        assert!(decode_entry(&[2, 0]).is_err());
    }
}
