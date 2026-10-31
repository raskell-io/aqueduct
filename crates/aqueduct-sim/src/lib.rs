//! Deterministic simulation testing for Aqueduct.
//!
//! Runs the whole system in one thread with a seeded scheduler, injects faults (crashes, torn
//! writes, fsync failures, message loss and reordering, partitions, clock jumps) and checks the
//! invariants in `BRIEF.md`, section 10.1, after every simulated step.
//!
//! Status: phase 0, not yet implemented.

#![forbid(unsafe_code)]
