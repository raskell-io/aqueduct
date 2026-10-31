//! The pure core of Aqueduct: the journal model, commands and events, and the state machine
//! `(state, event) -> (state, effects)` that decides what happens to a run.
//!
//! This crate has no I/O, no clock, no randomness and no async. Everything with side effects lives
//! behind traits in other crates so it can be simulated. See `BRIEF.md`, sections 4 and 5.
//!
//! Status: phase 0, not yet implemented.

#![forbid(unsafe_code)]
