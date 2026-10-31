//! Payload codecs and the versioned journal encoding for Aqueduct.
//!
//! This crate implements the history format specified in `docs/history-format.md` (phase 0),
//! including version bytes, canonical forms and the per-run hash chain. See `BRIEF.md`, section 6.4.
//!
//! Status: phase 0, not yet implemented.

#![forbid(unsafe_code)]
