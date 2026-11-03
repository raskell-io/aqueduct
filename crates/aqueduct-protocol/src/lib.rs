//! Aqueduct's service protocol, version 1 (draft, ADR 0038): the messages and services defined in
//! `proto/aqueduct/v1`, generated with prost and tonic.
//!
//! - [`v1::ingress_server`] and [`v1::ingress_client`]: starting runs and talking to them (start, wait,
//!   describe, signal, awakeables, cancel, queries, updates, escalations).
//! - [`v1::workers_server`] and [`v1::workers_client`]: step workers, which implement steps by name in
//!   any language and receive invocations over one stream.
//!
//! The protocol is a draft until phase 5: messages may still change. `docs/protocol.md` describes it,
//! with the HTTP/JSON form of the same calls.

#![forbid(unsafe_code)]

/// Version 1 of the protocol (package `aqueduct.v1`).
#[allow(missing_docs, clippy::all, clippy::pedantic)]
pub mod v1 {
    tonic::include_proto!("aqueduct.v1");
}
