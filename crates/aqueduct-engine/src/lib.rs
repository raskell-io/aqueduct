//! The Aqueduct engine.
//!
//! - [`Partition`] is the sans-I/O partition processor (ADR 0004, ADR 0006). The deterministic
//!   simulator drives it with a simulated disk; the embedded engine drives it with a file log.
//! - [`embedded`] runs a partition, step workers and clients inside one process, on a durable file log
//!   (ADR 0009).

#![forbid(unsafe_code)]

mod agent;
pub mod counterfactual;
pub mod drift;
pub mod embedded;
pub mod metrics;
mod observe;
mod partition;
mod schema;

pub use agent::{AgentAttempt, AgentCall, AgentExecutor};
pub use aqueduct_storage::Durability;
pub use observe::{Observer, StepEnd};
pub use partition::{
    Deciders, Outbound, Partition, PartitionError, PartitionRun, RetryPolicy, ReusePolicy,
    StartOptions,
};
