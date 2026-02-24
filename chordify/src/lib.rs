//! Chordify - P2P Song Sharing Application Library
//!
//! This library provides the core functionality for a Chord DHT implementation.
//!
//! # Modules
//! - `communication`: Low-level P2P messaging (connect, message, response).
//! - `nodes`: Node identity (NodeId) and higher-level node logic.

pub mod communication;
pub mod nodes;

// Re-export commonly used types
pub use communication::{Peer, Connection, connect, connect_with_timeout};
pub use nodes::NodeId;
