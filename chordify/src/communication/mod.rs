//! Communication module for Chordify
//! 
//! Phase 2: Basic Node Infrastructure
//! - Socket Setup: TCP server/client with async multithreading (Tokio)
//! - ID Generation: SHA-1 hash of ip:port for unique node IDs
//! - Message Protocol: Custom JSON-based protocol for node communication

pub mod protocol;
pub mod server;
pub mod client;
pub mod node_id;

pub use protocol::{Request, Response, Message};
pub use server::Server;
pub use client::Client;
pub use node_id::NodeId;
