//! Communication module for Chordify
//!
//! Phase 2: Basic Node Infrastructure
//! - Socket Setup: TCP server/client with async multithreading (Tokio)
//! - ID Generation: SHA-1 hash of ip:port for unique node IDs
//! - Message Protocol: Custom JSON-based protocol for node communication
//!
//! This module re-exports all communication primitives for easy access.
//!

pub mod protocol;

pub use protocol::{Request, Response, Message, NodeInfo, MessagePayload};
