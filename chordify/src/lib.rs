//! Chordify - P2P Chord DHT Library
//!
//! A Chord distributed hash table implementation using a P2P communication layer.
//!
//! # Architecture
//!
//! - `communication`: Low-level P2P messaging (connect, message, response)
//! - `nodes`: Chord DHT nodes (NodeId, Node, Request/Response protocol)

pub mod tcp;
pub mod nodes;

// Re-export commonly used types
pub use tcp::{Server, Client, connect, connect_with_timeout};
pub use nodes::{Node, NodeInfo, Request, Response};
