//! `aqueduct deployments` (ADR 0048): which build of a remote workflow new runs start on.

use aqueduct_protocol::v1;
use aqueduct_protocol::v1::deployments_client::DeploymentsClient;

use crate::migrate::{connect, runtime};

/// Runs `aqueduct deployments`.
pub(crate) fn deployments(args: &[String]) -> Result<String, String> {
    let (mut words, mut server, mut token_file, mut ca, mut force) =
        (Vec::new(), None, None, None, false);
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        let mut value = || {
            rest.next()
                .cloned()
                .ok_or_else(|| format!("{arg} needs a value"))
        };
        match arg.as_str() {
            "--server" => server = Some(value()?),
            "--token-file" => token_file = Some(value()?),
            "--ca" => ca = Some(value()?),
            "--force" => force = true,
            other if other.starts_with("--") => return Err(format!("unknown option `{other}`")),
            word => words.push(word.to_owned()),
        }
    }
    let server = server.ok_or("deployments needs --server <url>")?;
    let token = match &token_file {
        Some(file) => Some(
            std::fs::read_to_string(file)
                .map_err(|e| format!("{file}: {e}"))?
                .trim()
                .to_owned(),
        ),
        None => std::env::var("AQUEDUCT_TOKEN").ok(),
    };
    let ca = ca
        .as_ref()
        .map(|file: &String| std::fs::read_to_string(file).map_err(|e| format!("{file}: {e}")))
        .transpose()?;
    let header: Option<tonic::metadata::MetadataValue<_>> = token
        .map(|token| format!("Bearer {token}").parse())
        .transpose()
        .map_err(|_| "the token is not a valid header value".to_owned())?;
    let refused = |status: tonic::Status| format!("{:?}: {}", status.code(), status.message());
    runtime()?.block_on(async {
        let mut client = DeploymentsClient::new(connect(&server, ca.as_deref()).await?);
        let names: Vec<&str> = words.iter().map(String::as_str).collect();
        match names.as_slice() {
            ["list"] => {
                let listed = client
                    .list_deployments(with_token(&header, v1::ListDeploymentsRequest {}))
                    .await
                    .map_err(refused)?
                    .into_inner();
                let mut out = format!(
                    "{:<20} {:<20} {:<9} {:>9} {:>7}\n",
                    "WORKFLOW", "BUILD", "STATE", "OPEN RUNS", "WORKERS"
                );
                for d in listed.deployments {
                    // Safe to stop its workers, and to retire.
                    let state = if d.state == "draining" && d.open_runs == 0 {
                        "drained"
                    } else {
                        d.state.as_str()
                    };
                    out.push_str(&format!(
                        "{:<20} {:<20} {:<9} {:>9} {:>7}\n",
                        d.workflow, d.build, state, d.open_runs, d.workers
                    ));
                }
                Ok(out)
            }
            ["activate", workflow, build] => {
                client
                    .activate_deployment(with_token(&header, v1::ActivateDeploymentRequest {
                        workflow: (*workflow).to_owned(),
                        build: (*build).to_owned(),
                        force,
                    }))
                    .await
                    .map_err(refused)?;
                Ok(format!(
                    "new runs of {workflow} start on {build}; the build before it drains\n"
                ))
            }
            ["retire", workflow, build] => {
                client
                    .retire_deployment(with_token(&header, v1::RetireDeploymentRequest {
                        workflow: (*workflow).to_owned(),
                        build: (*build).to_owned(),
                    }))
                    .await
                    .map_err(refused)?;
                Ok(format!("build {build} of {workflow} is retired\n"))
            }
            _ => Err(
                "usage: aqueduct deployments list | activate <workflow> <build> | retire <workflow> <build>, with --server <url>"
                    .to_owned(),
            ),
        }
    })
}

fn with_token<T>(
    header: &Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    message: T,
) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    if let Some(header) = header {
        request
            .metadata_mut()
            .insert("authorization", header.clone());
    }
    request
}
