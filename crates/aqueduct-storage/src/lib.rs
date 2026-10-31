//! The `LogStore` and `StateStore` traits and their embedded implementations for Aqueduct.
//!
//! The embedded engine (redb or fjall) is chosen by a phase 0 benchmark and recorded in an ADR.
//! See `BRIEF.md`, section 6.3.
//!
//! Status: phase 0, not yet implemented.

#![forbid(unsafe_code)]
