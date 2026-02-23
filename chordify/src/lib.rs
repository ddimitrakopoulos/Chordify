//! Chordify - P2P Song Sharing Application Library
//!
//! This library provides the core functionality for a Chord DHT implementation.

pub mod communication;

// Re-export commonly used types
pub use communication::{Client, Server, NodeId, Message, Request, Response};
pub use communication::protocol::{NodeInfo, MessagePayload};
