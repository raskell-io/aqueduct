//! History archival (ADR 0061): finished runs' histories copied to object storage, where they can be read
//! after the server no longer holds them. Each server archives the runs of the partitions it leads; an
//! archived history is the run's records exactly as its hash chain has them (`docs/history-format.md`),
//! so it verifies on its own. Nothing is deleted here: retention is a separate decision.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use aqueduct_core::Entry;
use aqueduct_engine::shards::Shards;
use serde::Deserialize;

use crate::objects::{Store, StoreConfig};

/// The `"archive"` section of the server configuration.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArchiveConfig {
    /// Where histories go.
    pub store: StoreConfig,
    /// How often finished runs are looked for, in milliseconds. Default 60,000.
    #[serde(default = "default_every")]
    pub every_ms: u64,
}

fn default_every() -> u64 {
    60_000
}

/// Where a run's history is kept: `runs/<run id, encoded>.history`.
pub fn key(run: &str) -> String {
    format!("runs/{}.history", crate::sigv4::uri_encode(run, false))
}

/// A history as stored: the head hash of its chain (32 bytes), then each record of the chain, prefixed
/// by its length (`u32`, little-endian). The chain links every record to the one before; the head
/// covers the last.
pub fn encode(run: &str, entries: &[Entry]) -> Vec<u8> {
    let mut chain = aqueduct_codec::ChainWriter::new(run);
    let mut records = Vec::new();
    for entry in entries {
        let record = chain.append(entry);
        let length = u32::try_from(record.len()).unwrap_or(u32::MAX);
        records.extend_from_slice(&length.to_le_bytes());
        records.extend_from_slice(&record);
    }
    let mut out = chain.head().to_vec();
    out.extend_from_slice(&records);
    out
}

/// A stored history, verified against its run id's hash chain and its head: whatever byte changed, it
/// does not verify. (That it is the history the server had, an archive cannot show by itself: compare
/// the head with the server's, as `aqueduct verify` prints them.)
pub fn decode(run: &str, data: &[u8]) -> Result<Vec<Entry>, String> {
    let (head, mut data) = data
        .split_first_chunk::<32>()
        .ok_or("an archived history without its head")?;
    let mut records = Vec::new();
    while !data.is_empty() {
        let (length, rest) = data
            .split_first_chunk::<4>()
            .ok_or("an archived history ends inside a length")?;
        let length = u32::from_le_bytes(*length) as usize;
        let record = rest
            .get(..length)
            .ok_or("an archived history ends inside a record")?;
        records.push(record);
        data = &rest[length..];
    }
    let history = aqueduct_codec::verify_chain(run, records.iter().copied())
        .map_err(|e| format!("an archived history does not verify: {e}"))?;
    if history.head[..] != head[..] {
        return Err("an archived history does not match its head".into());
    }
    Ok(history.entries)
}

/// Starts archiving: a task that looks for finished runs every so often. Must be called inside a tokio
/// runtime.
pub(crate) fn start(shards: Arc<Shards>, config: &ArchiveConfig) -> Result<(), String> {
    let store = Arc::new(Store::open(&config.store)?);
    let every = Duration::from_millis(config.every_ms.max(100));
    tokio::spawn(async move {
        // What is there already, so that a restart does not copy everything again.
        let mut archived: BTreeSet<String> = match store.list("runs/").await {
            Ok(keys) => keys.into_iter().collect(),
            Err(e) => {
                eprintln!("archive: {e}");
                BTreeSet::new()
            }
        };
        loop {
            if let Err(e) = pass(&shards, &store, &mut archived).await {
                eprintln!("archive: {e}");
            }
            tokio::time::sleep(every).await;
        }
    });
    Ok(())
}

/// One pass: every finished run of a partition led here that is not archived yet.
async fn pass(
    shards: &Arc<Shards>,
    store: &Store,
    archived: &mut BTreeSet<String>,
) -> Result<(), String> {
    for engine in shards.engines() {
        if !engine.leads() {
            continue;
        }
        let engine = Arc::clone(engine);
        let runs = tokio::task::spawn_blocking({
            let engine = Arc::clone(&engine);
            move || engine.runs()
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        for run in runs {
            let key = key(&run.run_id);
            if !run.status.is_terminal() || archived.contains(&key) {
                continue;
            }
            let id = run.run_id.clone();
            let history = tokio::task::spawn_blocking({
                let engine = Arc::clone(&engine);
                move || engine.history(&id)
            })
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
            let Some(entries) = history else {
                continue;
            };
            store.put(&key, encode(&run.run_id, &entries)).await?;
            archived.insert(key);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aqueduct_core::Payload;

    #[test]
    fn a_history_round_trips_and_tampering_is_caught() {
        let entries = vec![
            Entry::Started {
                workflow: "w".into(),
                input: Payload::new(*b"1"),
                budget_tokens: None,
                version: None,
            },
            Entry::RunCompleted {
                output: Payload::new(*b"2"),
            },
        ];
        let run = "$ns/acme/order#7";
        let data = encode(run, &entries);
        assert_eq!(decode(run, &data), Ok(entries));
        // Another run's id, or any changed byte, does not verify.
        assert!(decode("other", &data).is_err());
        for at in 0..data.len() {
            let mut changed = data.clone();
            changed[at] ^= 1;
            assert!(
                decode(run, &changed).is_err(),
                "a change at byte {at} went unseen"
            );
        }
        assert!(decode(run, &data[..data.len() - 1]).is_err());
        assert_eq!(key(run), "runs/%24ns%2Facme%2Forder%237.history");
    }
}
