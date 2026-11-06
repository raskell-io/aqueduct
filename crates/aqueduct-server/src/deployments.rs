//! The deployments API (ADR 0048): which build of a remote workflow new runs start on. Builds are not a
//! tenant's: workflow definitions are shared across namespaces (ADR 0046), so this is for the default
//! namespace's administrators.

use aqueduct_engine::embedded::Engine;
use aqueduct_engine::namespaces;
use aqueduct_protocol::v1;
use axum::Router;
use axum::extract::{Path, State};
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{Api, Body, Failure, Json, Kind};

fn in_default_namespace() -> Result<(), Failure> {
    if crate::namespace() == namespaces::DEFAULT {
        return Ok(());
    }
    Err(Failure {
        kind: Kind::Forbidden,
        message: "deployments are changed from the default namespace".to_owned(),
    })
}

impl Api {
    async fn list_deployments(&self) -> Result<Vec<v1::Deployment>, Failure> {
        in_default_namespace()?;
        let listed = self.call(Engine::deployments).await?;
        Ok(listed
            .into_iter()
            .map(|info| v1::Deployment {
                workflow: info.workflow,
                build: info.build,
                state: info.state.map_or("unlisted", |s| s.as_str()).to_owned(),
                open_runs: info.open_runs as u64,
                workers: info.workers as u64,
            })
            .collect())
    }

    async fn activate_deployment(
        &self,
        workflow: String,
        build: String,
        force: bool,
    ) -> Result<(), Failure> {
        in_default_namespace()?;
        let target = format!("{workflow} {build}");
        let result = self
            .call(move |engine| engine.activate_deployment(&workflow, &build, force))
            .await;
        self.audited("activate deployment", target, result).await
    }

    async fn retire_deployment(&self, workflow: String, build: String) -> Result<(), Failure> {
        in_default_namespace()?;
        let target = format!("{workflow} {build}");
        let result = self
            .call(move |engine| engine.retire_deployment(&workflow, &build))
            .await;
        self.audited("retire deployment", target, result).await
    }
}

#[tonic::async_trait]
impl v1::deployments_server::Deployments for Api {
    async fn list_deployments(
        &self,
        _request: tonic::Request<v1::ListDeploymentsRequest>,
    ) -> Result<tonic::Response<v1::ListDeploymentsResponse>, tonic::Status> {
        let deployments = Api::list_deployments(self).await?;
        Ok(tonic::Response::new(v1::ListDeploymentsResponse {
            deployments,
        }))
    }

    async fn activate_deployment(
        &self,
        request: tonic::Request<v1::ActivateDeploymentRequest>,
    ) -> Result<tonic::Response<v1::DeploymentAck>, tonic::Status> {
        let request = request.into_inner();
        Api::activate_deployment(self, request.workflow, request.build, request.force).await?;
        Ok(tonic::Response::new(v1::DeploymentAck {}))
    }

    async fn retire_deployment(
        &self,
        request: tonic::Request<v1::RetireDeploymentRequest>,
    ) -> Result<tonic::Response<v1::DeploymentAck>, tonic::Status> {
        let request = request.into_inner();
        Api::retire_deployment(self, request.workflow, request.build).await?;
        Ok(tonic::Response::new(v1::DeploymentAck {}))
    }
}

#[derive(Default, Deserialize)]
struct Activate {
    #[serde(default)]
    force: bool,
}

async fn http_list(State(api): State<Api>) -> Result<Json<Value>, Failure> {
    let listed = api.list_deployments().await?;
    let deployments: Vec<Value> = listed
        .into_iter()
        .map(|d| {
            json!({
                "workflow": d.workflow,
                "build": d.build,
                "state": d.state,
                "open_runs": d.open_runs,
                "workers": d.workers,
            })
        })
        .collect();
    Ok(Json(json!({"deployments": deployments})))
}

async fn http_activate(
    State(api): State<Api>,
    Path((workflow, build)): Path<(String, String)>,
    Body(activate): Body<Activate>,
) -> Result<Json<Value>, Failure> {
    api.activate_deployment(workflow, build, activate.force)
        .await?;
    Ok(Json(json!({})))
}

async fn http_retire(
    State(api): State<Api>,
    Path((workflow, build)): Path<(String, String)>,
) -> Result<Json<Value>, Failure> {
    api.retire_deployment(workflow, build).await?;
    Ok(Json(json!({})))
}

/// The deployments' HTTP routes.
pub(crate) fn routes(api: Api) -> Router {
    Router::new()
        .route("/v1/deployments", get(http_list))
        .route(
            "/v1/deployments/{workflow}/{build}/activate",
            post(http_activate),
        )
        .route(
            "/v1/deployments/{workflow}/{build}/retire",
            post(http_retire),
        )
        .with_state(api)
}
