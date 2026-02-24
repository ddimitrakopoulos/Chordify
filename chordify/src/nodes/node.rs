//! Node - A Chord DHT node implementation
//!
//! Uses the P2P communication layer for all network operations.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug, warn};

use crate::communication::{Peer, connect};
use super::{NodeId, Request, Response};
use super::protocol::NodeInfo;

/// A Chord DHT node
pub struct Node {
    /// This node's info (ID + address)
    info: NodeInfo,
    
    /// Shared mutable state
    state: Arc<RwLock<NodeState>>,
}

/// Mutable state for a node
struct NodeState {
    /// Our successor in the ring
    successor: Option<NodeInfo>,
    
    /// Our predecessor in the ring
    predecessor: Option<NodeInfo>,
    
    /// Local key-value storage
    data: HashMap<String, String>,
}

impl Node {
    /// Create a new node bound to the given address
    pub fn new(addr: SocketAddr) -> Self {
        let info = NodeInfo::new(addr);
        let state = Arc::new(RwLock::new(NodeState {
            successor: None,
            predecessor: None,
            data: HashMap::new(),
        }));
        
        info!("Node created: {} at {}", info.id, info.addr);
        Self { info, state }
    }

    /// Get this node's ID
    pub fn id(&self) -> NodeId {
        self.info.id
    }

    /// Get this node's address
    pub fn addr(&self) -> SocketAddr {
        self.info.addr
    }

    /// Get this node's info
    pub fn info(&self) -> NodeInfo {
        self.info.clone()
    }

    /// Create the first node in a new ring (no existing nodes)
    pub async fn create_ring(&self) {
        let mut state = self.state.write().await;
        state.successor = Some(self.info.clone());
        state.predecessor = Some(self.info.clone());
        info!("Created new ring, node {} is alone", self.info.id);
    }

    /// Join an existing ring via a known node
    pub async fn join(&self, known_addr: SocketAddr) -> anyhow::Result<()> {
        info!("Joining ring via {}", known_addr);
        
        // Ask known node to find our successor
        let request = Request::FindSuccessor { id: self.info.id };
        let response = self.send_request(known_addr, request).await?;
        
        match response {
            Response::Successor(successor) => {
                let mut state = self.state.write().await;
                state.successor = Some(successor.clone());
                info!("Joined ring, successor is {} at {}", successor.id, successor.addr);
                Ok(())
            }
            Response::Error(e) => Err(anyhow::anyhow!("Join failed: {}", e)),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    /// Start listening and handling requests
    pub async fn run(&self) -> anyhow::Result<()> {
        let peer = Peer::bind(self.info.addr).await?;
        let state = Arc::clone(&self.state);
        let info = self.info.clone();

        peer.listen(move |request_bytes, from| {
            let state = Arc::clone(&state);
            let info = info.clone();
            async move {
                handle_request(request_bytes, from, info, state).await
            }
        }).await
    }

    /// Send a request to another node and get the response
    async fn send_request(&self, addr: SocketAddr, request: Request) -> anyhow::Result<Response> {
        let request_bytes = request.to_bytes()?;
        let response_bytes = connect(addr).await?.message(&request_bytes).await?;
        Response::from_bytes(&response_bytes)
    }

    /// Find the successor of a given ID
    pub async fn find_successor(&self, id: NodeId) -> anyhow::Result<NodeInfo> {
        let state = self.state.read().await;
        
        if let Some(ref successor) = state.successor {
            // Check if id is between us and our successor
            if id.is_between(&self.info.id, &successor.id) || self.info.id == successor.id {
                return Ok(successor.clone());
            }
            
            // Otherwise, forward the query to our successor
            drop(state);
            let request = Request::FindSuccessor { id };
            let response = self.send_request(successor.addr, request).await?;
            
            match response {
                Response::Successor(node) => Ok(node),
                Response::Error(e) => Err(anyhow::anyhow!("{}", e)),
                _ => Err(anyhow::anyhow!("Unexpected response")),
            }
        } else {
            // We are the only node
            Ok(self.info.clone())
        }
    }

    /// Get the successor
    pub async fn get_successor(&self) -> Option<NodeInfo> {
        self.state.read().await.successor.clone()
    }

    /// Get the predecessor
    pub async fn get_predecessor(&self) -> Option<NodeInfo> {
        self.state.read().await.predecessor.clone()
    }

    /// Store a key-value pair (stores locally if we're responsible)
    pub async fn put(&self, key: String, value: String) -> anyhow::Result<()> {
        let key_id = NodeId::from_key(&key);
        let successor = self.find_successor(key_id).await?;
        
        if successor.id == self.info.id {
            // We're responsible for this key
            let mut state = self.state.write().await;
            state.data.insert(key.clone(), value);
            debug!("Stored key '{}' locally", key);
            Ok(())
        } else {
            // Forward to the responsible node
            let request = Request::Put { key, value };
            let response = self.send_request(successor.addr, request).await?;
            match response {
                Response::Ok => Ok(()),
                Response::Error(e) => Err(anyhow::anyhow!("{}", e)),
                _ => Err(anyhow::anyhow!("Unexpected response")),
            }
        }
    }

    /// Get a value by key
    pub async fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        let key_id = NodeId::from_key(key);
        let successor = self.find_successor(key_id).await?;
        
        if successor.id == self.info.id {
            // We're responsible for this key
            let state = self.state.read().await;
            Ok(state.data.get(key).cloned())
        } else {
            // Forward to the responsible node
            let request = Request::Get { key: key.to_string() };
            let response = self.send_request(successor.addr, request).await?;
            match response {
                Response::Value(v) => Ok(v),
                Response::Error(e) => Err(anyhow::anyhow!("{}", e)),
                _ => Err(anyhow::anyhow!("Unexpected response")),
            }
        }
    }
}

/// Handle an incoming request
async fn handle_request(
    request_bytes: Vec<u8>,
    from: SocketAddr,
    info: NodeInfo,
    state: Arc<RwLock<NodeState>>,
) -> anyhow::Result<Vec<u8>> {
    let request = Request::from_bytes(&request_bytes)?;
    debug!("Received {:?} from {}", request, from);

    let response = match request {
        Request::Ping => Response::Pong,
        
        Request::FindSuccessor { id } => {
            let state_guard = state.read().await;
            if let Some(ref successor) = state_guard.successor {
                if id.is_between(&info.id, &successor.id) || info.id == successor.id {
                    Response::Successor(successor.clone())
                } else {
                    // Would need to forward, but for simplicity return our successor
                    Response::Successor(successor.clone())
                }
            } else {
                Response::Successor(info.clone())
            }
        }
        
        Request::GetPredecessor => {
            let state_guard = state.read().await;
            Response::Predecessor(state_guard.predecessor.clone())
        }
        
        Request::Notify { node } => {
            let mut state_guard = state.write().await;
            // Update predecessor if needed
            if state_guard.predecessor.is_none() {
                state_guard.predecessor = Some(node.clone());
                info!("Set predecessor to {}", node.id);
            } else if let Some(ref pred) = state_guard.predecessor {
                if node.id.is_between(&pred.id, &info.id) {
                    state_guard.predecessor = Some(node.clone());
                    info!("Updated predecessor to {}", node.id);
                }
            }
            Response::Ok
        }
        
        Request::Join { node } => {
            // Find successor for the joining node
            let state_guard = state.read().await;
            if let Some(ref successor) = state_guard.successor {
                if node.id.is_between(&info.id, &successor.id) || info.id == successor.id {
                    Response::Successor(successor.clone())
                } else {
                    Response::Successor(successor.clone())
                }
            } else {
                Response::Successor(info.clone())
            }
        }
        
        Request::Put { key, value } => {
            let mut state_guard = state.write().await;
            state_guard.data.insert(key.clone(), value);
            debug!("Stored key '{}'", key);
            Response::Ok
        }
        
        Request::Get { key } => {
            let state_guard = state.read().await;
            Response::Value(state_guard.data.get(&key).cloned())
        }
        
        Request::Delete { key } => {
            let mut state_guard = state.write().await;
            state_guard.data.remove(&key);
            debug!("Deleted key '{}'", key);
            Response::Ok
        }
    };

    response.to_bytes()
}
