//! The import service (ADR 0047): what `aqueduct migrate` uploads is verified, staged and committed here,
//! and applied when the engine next opens.

use std::sync::Arc;

use aqueduct_engine::embedded::Engine;
use aqueduct_engine::import::{Relation, Stage, relation};
use aqueduct_engine::namespaces;
use aqueduct_protocol::v1;
use tonic::{Request, Response, Status, Streaming};

use crate::Api;

/// The most one message may hold. A run's history is split, so this bounds a single record.
pub(crate) const MOST_IN_A_MESSAGE: usize = 64 * 1024 * 1024;

/// What the stage holds so far.
#[derive(Debug, Default)]
struct Progress {
    runs: u64,
    already_there: u64,
    sources: u64,
    conflicts: Vec<String>,
}

fn add_run(
    engine: &Engine,
    stage: &mut Stage,
    progress: &mut Progress,
    key: &str,
    records: &[Vec<u8>],
) -> Result<(), Status> {
    let entries = stage
        .add_run(key, records)
        .map_err(Status::invalid_argument)?;
    progress.runs += 1;
    let existing = engine
        .history(key)
        .map_err(|e| Status::internal(e.to_string()))?;
    if let Some(existing) = existing {
        // A run that has only the start of this history is another run: nothing is applying now.
        match relation(&existing, &entries) {
            Relation::AlreadyThere => progress.already_there += 1,
            Relation::Partial | Relation::Conflict => progress.conflicts.push(key.to_owned()),
        }
    }
    Ok(())
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
}

async fn receive(
    engine: Arc<Engine>,
    mut inbound: Streaming<v1::ImportMessage>,
) -> Result<v1::ImportReceipt, Status> {
    use v1::import_message::Message;
    let starting = Arc::clone(&engine);
    let mut stage =
        blocking(move || starting.begin_import().map_err(Status::failed_precondition)).await?;
    let mut progress = Progress::default();
    let mut current: Option<(String, Vec<Vec<u8>>)> = None;
    loop {
        let Some(message) = inbound.message().await? else {
            return Err(Status::aborted("the upload ended without its commit"));
        };
        let Some(message) = message.message else {
            continue;
        };
        match message {
            Message::Begin(_) => {}
            Message::Run(part) => {
                let (key, mut records) = match current.take() {
                    Some((key, records)) if key == part.run_id => (key, records),
                    Some((key, _)) => {
                        return Err(Status::invalid_argument(format!(
                            "run `{key}` ended without its last part"
                        )));
                    }
                    None => (part.run_id, Vec::new()),
                };
                records.extend(part.records);
                if !part.last {
                    current = Some((key, records));
                    continue;
                }
                let engine = Arc::clone(&engine);
                (stage, progress) = blocking(move || {
                    add_run(&engine, &mut stage, &mut progress, &key, &records)?;
                    Ok((stage, progress))
                })
                .await?;
            }
            Message::Source(source) => {
                stage
                    .add_source(&source.version, &source.text)
                    .map_err(Status::invalid_argument)?;
                progress.sources += 1;
            }
            Message::File(file) => {
                let name = match v1::ImportFileKind::try_from(file.kind) {
                    Ok(v1::ImportFileKind::Events) => "events.log",
                    Ok(v1::ImportFileKind::Policy) => "policy.json",
                    Ok(v1::ImportFileKind::Identities) => "identities.json",
                    _ => return Err(Status::invalid_argument("an unknown kind of file")),
                };
                stage
                    .add_registry_file(name, &file.data)
                    .map_err(Status::invalid_argument)?;
            }
            Message::Commit(commit) => {
                if current.is_some() {
                    return Err(Status::invalid_argument(
                        "the commit came before a run's last part",
                    ));
                }
                if commit.runs != progress.runs || commit.sources != progress.sources {
                    return Err(Status::invalid_argument(format!(
                        "the commit says {} runs and {} sources, and {} and {} arrived",
                        commit.runs, commit.sources, progress.runs, progress.sources
                    )));
                }
                if !progress.conflicts.is_empty() {
                    let count = progress.conflicts.len();
                    progress.conflicts.truncate(10);
                    return Err(Status::failed_precondition(format!(
                        "{count} keys have other runs on this server, among them: {}",
                        progress.conflicts.join(", ")
                    )));
                }
                blocking(move || stage.commit().map_err(Status::internal)).await?;
                return Ok(v1::ImportReceipt {
                    runs: progress.runs,
                    already_there: progress.already_there,
                    sources: progress.sources,
                });
            }
        }
    }
}

#[tonic::async_trait]
impl v1::imports_server::Imports for Api {
    async fn upload(
        &self,
        request: Request<Streaming<v1::ImportMessage>>,
    ) -> Result<Response<v1::ImportReceipt>, Status> {
        // A run's hash chain starts from its key, so it cannot move into a namespace (ADR 0047).
        if crate::namespace() != namespaces::DEFAULT {
            return Err(Status::permission_denied(
                "imports go into the default namespace",
            ));
        }
        let answer = receive(Arc::clone(&self.engine), request.into_inner()).await;
        let outcome = match &answer {
            Ok(receipt) => format!(
                "staged: {} runs, {} already here, {} sources",
                receipt.runs, receipt.already_there, receipt.sources
            ),
            Err(status) => format!("failed: {}", status.message()),
        };
        self.audit("import", "upload".to_owned(), outcome).await;
        answer.map(Response::new)
    }
}
