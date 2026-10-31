//! Aqueduct is a deterministic engine for procedures that agents write and people approve. It starts
//! as a library inside your binary, grows into a cluster, and every run leaves a history you can replay
//! and audit.
//!
//! This facade re-exports what an application needs: Rust workflows ([`workflow`],
//! [`WorkflowContext`]), Starlark procedures ([`Procedure`]) and the embedded engine ([`embedded`]).
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
//! The project is in phase 1 (BRIEF.md): the API will change.

#![forbid(unsafe_code)]

pub use aqueduct_core::{Payload, Status, StepOutcome};
pub use aqueduct_engine::RetryPolicy;
pub use aqueduct_procedure::Procedure;
pub use aqueduct_sdk::{
    Either, WorkflowContext, WorkflowError, join, join_all, select, select_all, workflow,
};

/// The embedded engine: a durable engine inside your process.
pub mod embedded {
    pub use aqueduct_engine::embedded::{
        Attempt, Builder, Engine, EngineError, EngineStats, RunResult,
    };
}
