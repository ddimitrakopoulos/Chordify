//! Protocol - Message types for node-to-node communication
//! These are serialized to bytes and sent over the TCP communication layer.

use serde::{Serialize, Deserialize};
use std::net::SocketAddr;

/// Information about a node (address only)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeInfo {
    pub addr: SocketAddr,
}

impl NodeInfo {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}

/// Request messages sent between nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Ping to check if node is alive
    Ping,
    /// Join the ring via this node
    Join { node: NodeInfo },
    /// Find the successor of a given address
    FindSuccessor { addr: SocketAddr },
    /// Get this node's predecessor
    GetPredecessor,
    /// Notify node that we might be its predecessor
    Notify { node: NodeInfo },
    /// Store a key-value pair
    Put { key: String, value: String },
    /// Retrieve a value by key
    Get { key: String },
    /// Delete a key
    Delete { key: String },
}

/// Response messages sent between nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// Pong response to Ping
    Pong,
    /// The successor node for a given address
    Successor(NodeInfo),
    /// The predecessor node (if any)
    Predecessor(Option<NodeInfo>),
    /// Value for a key (None if not found)
    Value(Option<String>),
    /// Acknowledgment (for Notify, Join)
    Ok,
    /// Error response
    Error(String),
}

impl Request {
    /// Serialize request to bytes
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Deserialize request from bytes
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

impl Response {
    /// Serialize response to bytes
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Deserialize response from bytes
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}
