//! Aqueduct is a deterministic engine for procedures that agents write and people approve. It starts
//! as a library inside your binary, grows into a cluster, and every run leaves a history you can replay
//! and audit.
//!
//! This facade re-exports what an application needs: Rust workflows ([`workflow`],
//! [`WorkflowContext`]), Starlark procedures (`Procedure`) and the embedded engine ([`embedded`]).
//!
//! # Features
//!
//! - `procedures` (on by default): Starlark procedures and the version registry. A program with only
//!   Rust workflows can turn it off (`default-features = false`) and depend on far fewer crates.
//! - `tokio`: `embedded::AsyncEngine`, the embedded engine for async code on Tokio.
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use aqueduct::{Payload, StepOutcome, WorkflowContext, WorkflowError, embedded::Engine, workflow};
//!
//! async fn greet(ctx: WorkflowContext, user: String) -> Result<String, WorkflowError> {
//!     let name: String = ctx.step("lookup_name", &user).await?;
//!     ctx.sleep(Duration::from_secs(60)).await; // durable: survives restarts
//!     Ok(format!("hello {name}"))
//! }
//!
//! let engine = Engine::builder("./data")
//!     .workflow("greet", Arc::new(workflow(greet)))
//!     .step("lookup_name", |user: &Payload| StepOutcome::Completed(user.clone()))
//!     .open()?;
//! engine.start("greet:ada", "greet", Payload::new(br#""ada""#.to_vec()))?;
//! let result = engine.wait("greet:ada", Duration::from_secs(120))?;
//! # Ok::<(), aqueduct::embedded::EngineError>(())
//! ```
//!
//! Procedure versions are pinned (ADR 0029) and go through a lifecycle in the `registry` (ADR 0030):
//! `Registry::open(dir)?.deploy(Engine::builder(dir))` registers the deployable ones.
//!
//! The project is in phase 3 (BRIEF.md): the API will change.

#![forbid(unsafe_code)]

#[cfg(feature = "tokio")]
mod asynchronous;

pub use aqueduct_core::{CommandIndex, Decider, Payload, RunState, Status, StepOutcome};
pub use aqueduct_engine::replay;
pub use aqueduct_engine::{RetryPolicy, ReusePolicy, StartOptions};
#[cfg(feature = "procedures")]
pub use aqueduct_procedure::Procedure;
pub use aqueduct_sdk::{
    AgentOptions, Either, ObjectContext, Saga, Update, WorkflowContext, WorkflowError, join,
    join_all, object_handler, select, select_all, workflow,
};

/// The version registry: procedure versions with provenance, replay evidence, approvals and a
/// lifecycle.
#[cfg(feature = "procedures")]
pub mod registry {
    pub use aqueduct_registry::{
        Action, Approval, Event, Identities, Policy, Provenance, PublicKey, Registry,
        RegistryError, ReplayEvidence, SecretKey, Signature, State, Version, approval_statement,
        diff,
    };
}

/// The embedded engine: a durable engine inside your process.
pub mod embedded {
    #[cfg(feature = "tokio")]
    pub use crate::asynchronous::AsyncEngine;
    pub use aqueduct_core::AgentResponse;
    pub use aqueduct_engine::embedded::{
        Attempt, Builder, Engine, EngineError, EngineStats, Escalation, RunResult, RunSummary,
        StepCall,
    };
    pub use aqueduct_engine::{
        AgentAttempt, AgentCall, AgentExecutor, Durability, Observer, StepEnd,
    };
    pub use aqueduct_engine::{activations, queue};
}
