//! Chordify - P2P Song Sharing Application Library
//!
//! This library provides the core functionality for a Chord DHT implementation.

pub mod communication;
pub mod nodes;

// Re-export commonly used types
pub use nodes::{Client, Server, NodeId};
pub use communication::{Message, Request, Response};
pub use communication::protocol::{NodeInfo, MessagePayload};
