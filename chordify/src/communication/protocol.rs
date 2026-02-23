//! Message Protocol for Chordify
//! 
//! Phase 2: Message Protocol
//! Defines the message format for inter-node communication

use serde::{Serialize, Deserialize};
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::communication::node_id::NodeId;

/// Unique message identifier
pub type MessageId = u64;

/// Generate a unique message ID using timestamp
fn generate_id() -> MessageId {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// Information about a node in the ring
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeInfo {
    pub id: NodeId,
    pub addr: SocketAddr,
}

impl NodeInfo {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            id: NodeId::from_address(&addr),
            addr,
        }
    }
}

/// Wrapper message for all communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: MessageId,
    pub sender: SocketAddr,
    pub payload: MessagePayload,
}

impl Message {
    pub fn new(sender: SocketAddr, payload: MessagePayload) -> Self {
        Self {
            id: generate_id(),
            sender,
            payload,
        }
    }

    pub fn response(request_id: MessageId, sender: SocketAddr, payload: MessagePayload) -> Self {
        Self {
            id: request_id,
            sender,
            payload,
        }
    }

    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// Message payload - either request or response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessagePayload {
    Request(Request),
    Response(Response),
}

/// All possible request types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Simple ping to check connectivity
    Ping,
    
    /// Get node's ID
    GetId,
    
    /// Get node's successor
    GetSuccessor,
    
    /// Get node's predecessor  
    GetPredecessor,
    
    /// Find the successor of a given ID
    FindSuccessor { id: NodeId },
    
    /// Notify a node that we think we're its predecessor
    Notify { node_info: NodeInfo },
    
    /// Join the ring via this node
    Join { node_info: NodeInfo },
    
    /// Node is departing the ring
    Depart { node_info: NodeInfo },
    
    /// Insert a key-value pair
    Insert { key: String, value: String },
    
    /// Query for a key
    Query { key: String },
    
    /// Delete a key
    Delete { key: String },
    
    /// Get the ring overlay/topology
    GetOverlay,
}

/// All possible response types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// Response to Ping
    Pong,
    
    /// Response with node ID
    Id(NodeId),
    
    /// Response with node info (successor/predecessor)
    NodeInfo(Option<NodeInfo>),
    
    /// Response to FindSuccessor
    FoundSuccessor(NodeInfo),
    
    /// Acknowledgment responses
    Ok,
    
    /// Join acknowledgment with successor and predecessor
    JoinAck { 
        successor: NodeInfo, 
        predecessor: Option<NodeInfo> 
    },
    
    /// Insert result
    InsertAck { success: bool },
    
    /// Query result
    QueryResult { 
        key: String, 
        value: Option<String> 
    },
    
    /// Delete result
    DeleteAck { success: bool },
    
    /// Overlay/topology response
    Overlay { nodes: Vec<NodeInfo> },
    
    /// Error response
    Error { message: String },
}
