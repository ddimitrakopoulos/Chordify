//! Chordify - P2P Chord DHT Library
//!
//! A Chord distributed hash table implementation using a P2P communication layer.
//!
//! # Architecture
//!
//! - `communication`: Low-level P2P messaging (connect, message, response)
//! - `nodes`: Chord DHT nodes (NodeId, Node, Request/Response protocol)

pub mod communication;
pub mod nodes;

// Re-export commonly used types
pub use communication::{Peer, Connection, connect, connect_with_timeout};
pub use nodes::{Node, Protocol};
