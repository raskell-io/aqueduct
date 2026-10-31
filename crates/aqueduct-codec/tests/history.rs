//! Property tests for the history format.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use aqueduct_codec::{
    ChainError, ChainWriter, DecodeError, decode_log_record, decode_record, encode_log_record,
    encode_record, genesis, verify_chain,
};
use aqueduct_core::{CommandIndex, Entry, Payload};
use proptest::prelude::*;

fn payload() -> impl Strategy<Value = Payload> {
    prop::collection::vec(any::<u8>(), 0..40).prop_map(Payload::new)
}

fn entry() -> impl Strategy<Value = Entry> {
    let cmd = any::<u32>().prop_map(CommandIndex);
    prop_oneof![
        (".{0,12}", payload()).prop_map(|(workflow, input)| Entry::Started { workflow, input }),
        (cmd.clone(), ".{0,12}", payload()).prop_map(|(cmd, name, input)| Entry::StepScheduled {
            cmd,
            name,
            input
        }),
        (cmd.clone(), payload()).prop_map(|(cmd, result)| Entry::StepCompleted { cmd, result }),
        (cmd, ".{0,12}").prop_map(|(cmd, error)| Entry::StepFailed { cmd, error }),
        payload().prop_map(|output| Entry::RunCompleted { output }),
        ".{0,12}".prop_map(|error| Entry::RunFailed { error }),
        ".{0,12}".prop_map(|reason| Entry::Parked { reason }),
        (any::<u32>(), any::<u64>(), any::<u64>()).prop_map(|(cmd, duration_ms, wake_at)| {
            Entry::TimerStarted {
                cmd: CommandIndex(cmd),
                duration_ms,
                wake_at,
            }
        }),
        (any::<u32>(), ".{0,12}", any::<u64>(), any::<u64>()).prop_map(
            |(cmd, name, timeout_ms, wake_at)| {
                Entry::SignalAwaited {
                    cmd: CommandIndex(cmd),
                    name,
                    timeout_ms,
                    wake_at,
                }
            }
        ),
        any::<u32>().prop_map(|cmd| Entry::TimerFired {
            cmd: CommandIndex(cmd)
        }),
        (any::<u32>(), any::<u32>(), ".{0,12}", any::<u64>()).prop_map(
            |(cmd, attempt, error, wake_at)| {
                Entry::StepRetryScheduled {
                    cmd: CommandIndex(cmd),
                    attempt,
                    error,
                    wake_at,
                }
            }
        ),
        (".{0,12}", ".{0,12}", payload()).prop_map(|(name, id, payload)| Entry::SignalReceived {
            name,
            id,
            payload
        }),
    ]
}

proptest! {
    #[test]
    fn records_round_trip(entry in entry(), prev in any::<[u8; 32]>()) {
        let bytes = encode_record(&entry, &prev);
        let record = decode_record(&bytes).unwrap();
        prop_assert_eq!(record.entry, entry);
        prop_assert_eq!(record.prev, prev);
    }

    #[test]
    fn encoding_is_canonical(entry in entry(), prev in any::<[u8; 32]>()) {
        // Decoding then re-encoding gives the same bytes, so hashes are well defined.
        let bytes = encode_record(&entry, &prev);
        let record = decode_record(&bytes).unwrap();
        prop_assert_eq!(encode_record(&record.entry, &record.prev), bytes);
    }

    #[test]
    fn decoding_arbitrary_bytes_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..80)) {
        let _ = decode_record(&bytes);
        let _ = decode_log_record(&bytes);
    }

    #[test]
    fn any_change_is_detected(
        key in "[a-z]{1,8}",
        entries in prop::collection::vec(entry(), 1..8),
        which in any::<prop::sample::Index>(),
        byte in any::<prop::sample::Index>(),
        flip in 1u8..=255,
    ) {
        let mut chain = ChainWriter::new(&key);
        let mut records: Vec<Vec<u8>> = entries.iter().map(|e| chain.append(e)).collect();
        let original_head = chain.head();

        let record = which.index(records.len());
        let at = byte.index(records[record].len());
        records[record][at] ^= flip;

        // Either verification fails, or (for the last record) the head no longer matches.
        match verify_chain(&key, records.iter().map(Vec::as_slice)) {
            Err(_) => {}
            Ok(verified) => prop_assert_ne!(verified.head, original_head),
        }
    }

    #[test]
    fn removing_or_reordering_records_is_detected(
        key in "[a-z]{1,8}",
        entries in prop::collection::vec(entry(), 2..8),
        which in any::<prop::sample::Index>(),
    ) {
        let mut chain = ChainWriter::new(&key);
        let records: Vec<Vec<u8>> = entries.iter().map(|e| chain.append(e)).collect();
        let i = which.index(records.len() - 1);

        let mut removed = records.clone();
        removed.remove(i);
        prop_assert!(verify_chain(&key, removed.iter().map(Vec::as_slice)).is_err());

        let mut swapped = records.clone();
        swapped.swap(i, i + 1);
        if swapped != records {
            prop_assert!(verify_chain(&key, swapped.iter().map(Vec::as_slice)).is_err());
        }
    }

    #[test]
    fn log_records_round_trip(key in ".{0,16}", record in prop::collection::vec(any::<u8>(), 0..64)) {
        let bytes = encode_log_record(&key, &record);
        prop_assert_eq!(decode_log_record(&bytes).unwrap(), (key.as_str(), record.as_slice()));
    }
}

#[test]
fn golden_record() {
    // A fixed vector pins the format: if this changes, the history format changed.
    let entry = Entry::StepScheduled {
        cmd: CommandIndex(300),
        name: "map".into(),
        input: Payload::new(*b"ab"),
    };
    let prev = genesis("run-1");
    let bytes = encode_record(&entry, &prev);
    let mut expected = vec![0x00, 0x02];
    expected.extend_from_slice(&prev);
    expected.extend_from_slice(&[0xac, 0x02, 0x03, b'm', b'a', b'p', 0x02, b'a', b'b']);
    assert_eq!(bytes, expected);
    assert_eq!(
        hex(&prev),
        hex(&sha256(
            &[b"aqueduct-history-v0\0".as_slice(), b"run-1"].concat()
        ))
    );
}

#[test]
fn strict_decoding() {
    let prev = [0u8; 32];
    let mut record = encode_record(&Entry::RunFailed { error: "e".into() }, &prev);
    record.push(0);
    assert_eq!(decode_record(&record), Err(DecodeError::TrailingBytes));

    let mut bad_version = encode_record(&Entry::RunFailed { error: "e".into() }, &prev);
    bad_version[0] = 1;
    assert_eq!(
        decode_record(&bad_version),
        Err(DecodeError::UnsupportedVersion(1))
    );

    let mut bad_kind = encode_record(&Entry::RunFailed { error: "e".into() }, &prev);
    bad_kind[1] = 99;
    assert_eq!(decode_record(&bad_kind), Err(DecodeError::UnknownKind(99)));

    let mut bad_utf8 = encode_record(&Entry::RunFailed { error: "e".into() }, &prev);
    let last = bad_utf8.len() - 1;
    bad_utf8[last] = 0xff;
    assert_eq!(decode_record(&bad_utf8), Err(DecodeError::InvalidUtf8));

    assert!(matches!(
        verify_chain("k", [bad_utf8.as_slice()]),
        Err(ChainError::Decode { index: 0, .. })
    ));
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
