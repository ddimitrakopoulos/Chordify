//! Protocol - Message types for node-to-node communication
//!
//! These are serialized to bytes and sent over the P2P communication layer.
//! All join/depart operations are coordinated by the bootstrap node.

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
    /// Find the successor of a given address
    FindSuccessor { addr: SocketAddr },
    /// Get this node's predecessor
    GetPredecessor,
    /// Get this node's successor
    GetSuccessor,
    /// Notify node that we might be its predecessor
    Notify { node: NodeInfo },
    /// Set this node's predecessor directly (sent by bootstrap during coordination)
    SetPredecessor { node: NodeInfo },
    /// Set this node's successor directly (sent by bootstrap during coordination)
    SetSuccessor { node: NodeInfo },
    /// Request keys to transfer (returns all keys)
    TransferKeys { to_addr: SocketAddr },
    /// Store a key-value pair
    Insert { key: String, value: String },
    /// Retrieve a value by key
    Query { key: String, source: SocketAddr },
    /// Query response for a key
    QueryResponse { source: SocketAddr, value: Option<String> },
    /// Delete a key
    Delete { key: String },
    
    // === Bootstrap-coordinated operations ===
    
    /// Request to join the ring (sent to bootstrap node)
    /// Bootstrap will coordinate all pointer updates and key transfers
    JoinRequest { joining_node: NodeInfo },
    
    /// Request to depart from the ring (sent to bootstrap node)
    /// Bootstrap will coordinate all pointer updates and key transfers
    DepartRequest { departing_node: NodeInfo },
    
    /// Command from bootstrap to transfer keys directly to another node
    TransferKeysTo { target_addr: SocketAddr },
    
    /// Receive keys from another node (sent during key transfer)
    ReceiveKeys { keys: Vec<(String, String)> },
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
    /// Acknowledgment
    Ok,
    /// Value for a key (None if not found)
    Value(Option<String>),
    /// Transferred keys (for TransferKeys)
    Keys(Vec<(String, String)>),
    /// Error response
    Error(String),
    
    // === Bootstrap-coordinated operation responses ===
    
    /// Join successful - includes the assigned successor and predecessor
    JoinSuccess { 
        successor: NodeInfo, 
        predecessor: Option<NodeInfo>,
    },
    
    /// Depart acknowledged - node can now shut down
    DepartSuccess,
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
