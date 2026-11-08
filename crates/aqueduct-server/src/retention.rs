//! Retention (ADR 0066): runs that ended long enough ago are forgotten, so that a server's storage does
//! not only grow. Each server forgets runs of the partitions it leads; by default only once their
//! history is in the archive (ADR 0061).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aqueduct_core::Status;
use aqueduct_engine::shards::Shards;
use serde::Deserialize;

use crate::objects::Store;

/// The `"retention"` section of the server configuration.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RetentionConfig {
    /// How long after it ended a run is forgotten, in milliseconds. Counted from when this server first
    /// saw it ended, so never sooner than that after its end.
    pub after_ms: u64,
    /// Forget a run only once its history is archived (the `"archive"` section). Default true.
    #[serde(default = "yes")]
    pub archive_first: bool,
    /// How often ended runs are looked for, in milliseconds. Default 60,000.
    #[serde(default = "default_every")]
    pub every_ms: u64,
}

fn yes() -> bool {
    true
}

fn default_every() -> u64 {
    60_000
}

/// Starts retention: a task that forgets runs ended long enough ago. Must be called inside a tokio
/// runtime.
pub(crate) fn start(
    shards: Arc<Shards>,
    config: &RetentionConfig,
    archive: Option<&crate::archive::ArchiveConfig>,
) -> Result<(), String> {
    let store = match (config.archive_first, archive) {
        (true, Some(archive)) => Some(Arc::new(Store::open(&archive.store)?)),
        (true, None) => {
            return Err(
                "retention forgets only archived runs unless archive_first is false: configure \"archive\""
                    .into(),
            );
        }
        (false, _) => None,
    };
    let after = Duration::from_millis(config.after_ms);
    let every = Duration::from_millis(config.every_ms.max(100));
    let reason = format!("retention: ended more than {} ms ago", config.after_ms);
    tokio::spawn(async move {
        let mut ended: BTreeMap<String, Instant> = BTreeMap::new();
        loop {
            if let Err(e) = pass(&shards, store.as_deref(), after, &reason, &mut ended).await {
                eprintln!("retention: {e}");
            }
            tokio::time::sleep(every).await;
        }
    });
    Ok(())
}

/// One pass over the partitions led here.
async fn pass(
    shards: &Arc<Shards>,
    store: Option<&Store>,
    after: Duration,
    reason: &str,
    ended: &mut BTreeMap<String, Instant>,
) -> Result<(), String> {
    for engine in shards.engines() {
        if !engine.leads() {
            continue;
        }
        let runs = tokio::task::spawn_blocking({
            let engine = Arc::clone(engine);
            move || engine.runs()
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        for run in runs {
            if !matches!(
                run.status,
                Status::Completed | Status::Failed | Status::Continued
            ) {
                continue;
            }
            let seen = *ended.entry(run.run_id.clone()).or_insert_with(Instant::now);
            if seen.elapsed() < after {
                continue;
            }
            if let Some(store) = store
                && store
                    .get(&crate::archive::key(&run.run_id))
                    .await?
                    .is_none()
            {
                // Not archived yet: the archive's next pass will, and this one waits.
                continue;
            }
            let (engine, id, reason) = (Arc::clone(engine), run.run_id.clone(), reason.to_owned());
            tokio::task::spawn_blocking(move || engine.forget(&id, &reason))
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
            ended.remove(&run.run_id);
        }
    }
    Ok(())
}
