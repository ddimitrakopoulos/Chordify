//! Protocol - Message types for node-to-node communication
//!
//! These are serialized to bytes and sent over the P2P communication layer.
//! All join/depart operations are coordinated by the bootstrap node.

use serde::{Serialize, Deserialize};
use std::net::SocketAddr;
use std::collections::HashMap;
use super::node::NodeInfo;

/// Request messages sent between nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Ping to check if node is alive
    Ping,
    /// Set this node's predecessor and transfer keys later
    SetPredecessorWithKeys { node: NodeInfo },
    /// Set this node's predecessor directly (sent by bootstrap during coordination)
    SetPredecessor { node: NodeInfo },
    /// Set this node's successor directly (sent by bootstrap during coordination)
    SetSuccessor { node: NodeInfo },
    /// Request keys to transfer (returns all keys)
    TransferData { data: HashMap<String, String> },
    /// Store a key-value pair
    Insert { key: String, value: String },
    /// Retrieve a value by key
    Query { key: String, source: SocketAddr },
    // /// Query response for a key
    // QueryResponse { source: SocketAddr, value: Option<String> },
    /// Query all the key values
    QueryAll { source: SocketAddr, data: Vec<(u64, HashMap<String, String>)> },
    /// Delete a key
    Delete { key: String },
    
    // === Bootstrap-coordinated operations ===
    
    /// Request to join the ring (sent to bootstrap node)
    /// Bootstrap will coordinate all pointer updates and key transfers
    JoinRequest { joining_node: NodeInfo },
    
    /// Request to depart from the ring (sent to bootstrap node)
    /// Bootstrap will coordinate all pointer updates and key transfers
    DepartRequest { departing_node: NodeInfo },
    
    /// Request to transfer replicas to new node
    TransferReplicas { new_replicated_data: HashMap<u64, (u64, HashMap<String, String>)>, node_addr: SocketAddr },

    /// Request to update replicas to new node
    UpdateReplicas { data: HashMap<String, String>, k_left: u64 },
}

/// Response messages sent between nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// Pong response to Ping
    Pong,
    /// The successor node for a given address
    Successor(NodeInfo),
    /// The predecessor node (if any)
    Predecessor(NodeInfo),
    /// Acknowledgment
    Ok,
    /// Query response for a key
    QueryResponse { source: SocketAddr, value: Option<String> },
    // Resporse to QueryAll - includes all key-value pairs from the queried node
    QueryAll{ source: SocketAddr, data: Vec<(u64, HashMap<String, String>)> },
    /// Value for a key (None if not found)
    Value(Option<String>),
    /// Transferred keys (for TransferKeys)
    Keys(Vec<(String, String)>),
    /// Error response
    Error(String),

    // === Bootstrap-coordinated operation responses ===
    
    /// Join successful - includes the assigned successor, predecessor, and replication parameters
    JoinSuccess { 
        successor: NodeInfo, 
        predecessor: NodeInfo,
        k: u64,
        t: u8,
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
