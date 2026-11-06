//! `aqueduct migrate` (ADR 0047): moves a data directory's runs and procedure versions to a server.

use std::collections::BTreeMap;
use std::path::Path;

use aqueduct_codec::ChainWriter;
use aqueduct_engine::import;
use aqueduct_protocol::v1;
use aqueduct_protocol::v1::import_message::Message;
use aqueduct_protocol::v1::imports_client::ImportsClient;
use aqueduct_registry::Registry;
use tokio::sync::mpsc;

/// The most of a run's history in one message; one record larger than this goes alone.
const CHUNK: usize = 1024 * 1024;

/// What the options asked for.
struct Options {
    from: String,
    to: Option<String>,
    token_file: Option<String>,
    ca: Option<String>,
    dry_run: bool,
    abort: bool,
}

fn options(args: &[String]) -> Result<Options, String> {
    let mut options = Options {
        from: String::new(),
        to: None,
        token_file: None,
        ca: None,
        dry_run: false,
        abort: false,
    };
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        let mut value = || {
            rest.next()
                .cloned()
                .ok_or_else(|| format!("{arg} needs a value"))
        };
        match arg.as_str() {
            "--from" => options.from = value()?,
            "--to" => options.to = Some(value()?),
            "--token-file" => options.token_file = Some(value()?),
            "--ca" => options.ca = Some(value()?),
            "--dry-run" => options.dry_run = true,
            "--abort" => options.abort = true,
            other => return Err(format!("unknown option `{other}`")),
        }
    }
    if options.from.is_empty() {
        return Err("migrate needs --from <dir>".to_owned());
    }
    Ok(options)
}

/// Runs `aqueduct migrate`.
pub(crate) fn migrate(args: &[String]) -> Result<String, String> {
    let options = options(args)?;
    let dir = Path::new(&options.from);
    if options.abort {
        return abort(dir);
    }
    let to = options
        .to
        .clone()
        .ok_or("migrate needs --to <url>, or --abort")?;
    if !dir.join("log").is_dir() {
        return Err(format!(
            "{} is not an aqueduct data directory (no log)",
            dir.display()
        ));
    }
    // Holds the log's lock throughout: an application still using the directory stops this here.
    let partition = crate::load(dir)?;
    let registry = Registry::open(dir).map_err(|e| e.to_string())?;
    let report = report(&partition, &registry);
    if options.dry_run {
        return Ok(format!("nothing was changed (--dry-run)\n{report}"));
    }
    if let Some(state) = import::frozen_state(dir)
        && state == "migrated"
    {
        // Repeating it is safe: a server that has the runs answers that it has them.
        eprintln!("{} was migrated before: sending it again", dir.display());
    }
    let token = match &options.token_file {
        Some(file) => Some(
            std::fs::read_to_string(file)
                .map_err(|e| format!("{file}: {e}"))?
                .trim()
                .to_owned(),
        ),
        None => std::env::var("AQUEDUCT_TOKEN").ok(),
    };
    let ca = options
        .ca
        .as_ref()
        .map(|file| std::fs::read_to_string(file).map_err(|e| format!("{file}: {e}")))
        .transpose()?;
    let runtime = runtime()?;
    // Before anything changes: a server that cannot be reached leaves the source as it was.
    let channel = runtime.block_on(connect(&to, ca.as_deref()))?;

    // From here on, this directory's runs must not go on here: the server may have them.
    import::freeze(dir, &to, "migrating")?;
    let receipt = runtime.block_on(upload(channel, token, dir, &partition, &registry))?;
    import::freeze(dir, &to, "migrated")?;
    Ok(format!(
        "{} runs and {} procedure sources staged on {to} ({} of those runs it has already)\n\
         restart the server to apply them; {} is frozen, and will not run again\n{report}",
        receipt.runs,
        receipt.sources,
        receipt.already_there,
        dir.display()
    ))
}

/// Unfreezes a source that was never committed on the server.
fn abort(dir: &Path) -> Result<String, String> {
    match import::frozen_state(dir).as_deref() {
        Some("migrating") => {
            import::thaw(dir)?;
            Ok(format!(
                "{} can run again. If the server did get the import (its answer may have been lost), \
                 it holds these runs too: check its import directory before restarting it\n",
                dir.display()
            ))
        }
        Some(_) => Err(format!(
            "{} was migrated: the server has its runs. Running them here too would repeat their steps",
            dir.display()
        )),
        None => Err(format!("{} is not frozen", dir.display())),
    }
}

/// What the operator needs to know before the server restarts.
fn report(
    partition: &aqueduct_engine::Partition<
        aqueduct_storage::FileLog,
        aqueduct_storage::RedbStateStore,
    >,
    registry: &Registry,
) -> String {
    let (mut open, mut ended) = (0, 0);
    let mut unserved: BTreeMap<&str, usize> = BTreeMap::new();
    let mut objects = std::collections::BTreeSet::new();
    for run in partition.runs().values() {
        if run.state().status().is_terminal() {
            ended += 1;
            continue;
        }
        open += 1;
        let workflow = run.state().workflow();
        if let Some(object) = workflow.strip_prefix("$object/") {
            objects.insert(object);
            continue;
        }
        let known = registry.versions().any(|version| {
            version.workflow == workflow
                && run
                    .state()
                    .version()
                    .is_none_or(|pinned| pinned == version.id)
                && version.state.is_deployed()
        });
        if !known {
            *unserved.entry(workflow).or_default() += 1;
        }
    }
    let mut out = format!(
        "{open} open runs, {ended} ended, {} procedure versions\n",
        registry.versions().count()
    );
    if open > 0 {
        out.push_str(
            "open runs' steps will go to step workers or push endpoints, and retry policies set in \
             code do not move: configure them on the server\n",
        );
    }
    for object in objects {
        out.push_str(&format!(
            "object `{object}` has keys: list it under objects in the server's configuration\n"
        ));
    }
    for (workflow, runs) in unserved {
        out.push_str(&format!(
            "`{workflow}` has {runs} open runs and no deployable procedure version: until a workflow \
             worker serves it (remote_workflows in the server's configuration), they wait\n"
        ));
    }
    out
}

pub(crate) fn runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())
}

pub(crate) async fn connect(
    url: &str,
    ca: Option<&str>,
) -> Result<tonic::transport::Channel, String> {
    let error = |e: tonic::transport::Error| {
        // What actually went wrong is further down: `transport error` says little.
        let mut text = format!("cannot connect to {url}: {e}");
        let mut source = std::error::Error::source(&e);
        while let Some(cause) = source {
            text.push_str(&format!(": {cause}"));
            source = cause.source();
        }
        text
    };
    let mut endpoint = tonic::transport::Endpoint::from_shared(url.to_owned()).map_err(error)?;
    if url.starts_with("https://") {
        let mut tls = tonic::transport::ClientTlsConfig::new().with_native_roots();
        if let Some(ca) = ca {
            tls = tls.ca_certificate(tonic::transport::Certificate::from_pem(ca));
        }
        endpoint = endpoint.tls_config(tls).map_err(error)?;
    }
    endpoint.connect().await.map_err(error)
}

async fn upload(
    channel: tonic::transport::Channel,
    token: Option<String>,
    dir: &Path,
    partition: &aqueduct_engine::Partition<
        aqueduct_storage::FileLog,
        aqueduct_storage::RedbStateStore,
    >,
    registry: &Registry,
) -> Result<v1::ImportReceipt, String> {
    let header: Option<tonic::metadata::MetadataValue<_>> = token
        .map(|token| format!("Bearer {token}").parse())
        .transpose()
        .map_err(|_| "the token is not a valid header value".to_owned())?;
    let registry_dir = dir.join("registry");
    let (out, outbound) = mpsc::channel::<v1::ImportMessage>(16);
    let mut request = tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(outbound));
    if let Some(header) = header {
        request.metadata_mut().insert("authorization", header);
    }
    let mut client = ImportsClient::new(channel);
    let answer = tokio::spawn(async move { client.upload(request).await });
    let sent = send_all(&out, dir, partition, registry, &registry_dir).await;
    drop(out);
    match answer.await.map_err(|e| e.to_string())? {
        Ok(receipt) => {
            sent?;
            Ok(receipt.into_inner())
        }
        // The server's refusal says more than that it stopped reading.
        Err(status) => Err(format!(
            "the server refused the import ({:?}): {}\nnothing was applied. {} stays frozen: \
             try again, or undo with aqueduct migrate --from {} --abort",
            status.code(),
            status.message(),
            dir.display(),
            dir.display()
        )),
    }
}

async fn send_all(
    out: &mpsc::Sender<v1::ImportMessage>,
    dir: &Path,
    partition: &aqueduct_engine::Partition<
        aqueduct_storage::FileLog,
        aqueduct_storage::RedbStateStore,
    >,
    registry: &Registry,
    registry_dir: &Path,
) -> Result<(), String> {
    let send = async |message: Message| {
        out.send(v1::ImportMessage {
            message: Some(message),
        })
        .await
        .map_err(|_| "the server stopped reading".to_owned())
    };
    send(Message::Begin(v1::ImportBegin {
        source: dir.display().to_string(),
    }))
    .await?;
    let mut runs = 0u64;
    for (key, run) in partition.runs() {
        let mut chain = ChainWriter::new(key);
        let mut part = Vec::new();
        let mut size = 0;
        for entry in run.journal() {
            let record = chain.append(entry);
            if size + record.len() > CHUNK && !part.is_empty() {
                send(Message::Run(v1::ImportRun {
                    run_id: key.clone(),
                    records: std::mem::take(&mut part),
                    last: false,
                }))
                .await?;
                size = 0;
            }
            size += record.len();
            part.push(record);
        }
        send(Message::Run(v1::ImportRun {
            run_id: key.clone(),
            records: part,
            last: true,
        }))
        .await?;
        runs += 1;
    }
    let mut sources = 0u64;
    for version in registry.versions() {
        let text = registry.source(version).map_err(|e| e.to_string())?;
        send(Message::Source(v1::ImportSource {
            version: version.id.clone(),
            text: text.into_bytes(),
        }))
        .await?;
        sources += 1;
    }
    for (name, kind) in [
        ("events.log", v1::ImportFileKind::Events),
        ("policy.json", v1::ImportFileKind::Policy),
        ("identities.json", v1::ImportFileKind::Identities),
    ] {
        let path = registry_dir.join(name);
        if path.exists() {
            let data = std::fs::read(&path).map_err(|e| format!("{}: {e}", path.display()))?;
            send(Message::File(v1::ImportFile {
                kind: kind.into(),
                data,
            }))
            .await?;
        }
    }
    send(Message::Commit(v1::ImportCommit { runs, sources })).await
}

/// Before an engine opens on a directory with a pending import: its versions join the registry, so that
/// the runs that use them can recover. A registry that cannot take them rejects the import.
pub(crate) fn merge_pending_registry(dir: &Path) -> Result<Vec<String>, String> {
    let Some(stage) = import::pending(dir) else {
        return Ok(Vec::new());
    };
    let merged = Registry::open(&stage).and_then(|from| {
        let mut registry = Registry::open(dir)?;
        registry.merge(&from)
    });
    match merged {
        Ok(notes) => Ok(notes),
        Err(e) => {
            let why = format!("its procedure versions: {e}");
            import::reject(dir, &why)?;
            Ok(vec![format!(
                "rejected, and set aside under {}: {why}",
                dir.join("import").display()
            )])
        }
    }
}
