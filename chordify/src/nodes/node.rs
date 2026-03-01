//! Node - A Chord DHT node implementation
//!
//! Uses the P2P communication layer for all network operations.
//! All join/depart operations are coordinated by the bootstrap node.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug, warn};

use crate::tcp::{Server, connect};
use super::protocol::{Request, Response, NodeInfo};

/// A Chord DHT node
pub struct Node {
    /// This node's info (address only)
    info: NodeInfo,
    /// Shared mutable state
    pub(crate) state: Arc<RwLock<NodeState>>,
    /// Bootstrap node address (required for join/depart)
    bootstrap_addr: Option<SocketAddr>,
}

/// Mutable state for a node
pub struct NodeState {
    /// Our successor in the ring
    pub(crate) successor: Option<NodeInfo>,
    /// Our predecessor in the ring
    pub(crate) predecessor: Option<NodeInfo>,
    /// Local key-value storage
    pub(crate) data: HashMap<String, String>,
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
        info!("Node created at {}", info.addr);
        Self { info, state, bootstrap_addr: None }
    }

    /// Set bootstrap node address (required for join/depart operations)
    pub fn with_bootstrap(mut self, bootstrap_addr: SocketAddr) -> Self {
        self.bootstrap_addr = Some(bootstrap_addr);
        self
    }

    /// Get this node's address
    pub fn addr(&self) -> SocketAddr {
        self.info.addr
    }

    /// Get this node's info
    pub fn info(&self) -> NodeInfo {
        self.info.clone()
    }

    /// Get a clone of the state Arc (for use by BootstrapNode)
    pub(crate) fn state_clone(&self) -> Arc<RwLock<NodeState>> {
        Arc::clone(&self.state)
    }

    /// Create the first node in a new ring (internal use only).
    /// 
    /// **Do not call this method directly.** Use `BootstrapNode::create_ring()` instead.
    /// Only `BootstrapNode` should create rings - regular nodes must join via bootstrap.
    pub(crate) async fn create_ring(&self) {
        let mut state = self.state.write().await;
        state.successor = Some(self.info.clone());
        state.predecessor = Some(self.info.clone());
        info!("Created new ring, node is alone at {}", self.info.addr);
    }

    /// Join an existing ring via bootstrap node
    /// 
    /// The node only contacts the bootstrap, which coordinates all pointer 
    /// updates and key transfers.
    pub async fn join(&self, bootstrap_addr: SocketAddr) -> anyhow::Result<()> {
        info!("Joining ring via bootstrap at {}", bootstrap_addr);
        
        // Send JoinRequest to bootstrap
        let request = Request::JoinRequest { joining_node: self.info.clone() };
        let response = self.send_request(bootstrap_addr, request).await?;
        
        match response {
            Response::JoinSuccess { successor, predecessor } => {
                // Update our state with the assigned successor and predecessor
                let mut state = self.state.write().await;
                state.successor = Some(successor.clone());
                state.predecessor = predecessor.clone();
                
                info!("Joined ring via bootstrap: successor={}, predecessor={:?}", 
                      successor.addr, predecessor.as_ref().map(|p| p.addr));
                Ok(())
            }
            Response::Error(e) => Err(anyhow::anyhow!("Join failed: {}", e)),
            _ => Err(anyhow::anyhow!("Unexpected response from bootstrap")),
        }
    }

    /// Depart from the ring via bootstrap
    /// 
    /// The node only contacts the bootstrap, which coordinates all pointer 
    /// updates and key transfers.
    pub async fn depart(&self, bootstrap_addr: SocketAddr) -> anyhow::Result<()> {
        info!("Departing ring via bootstrap at {}", bootstrap_addr);
        
        // Send DepartRequest to bootstrap
        let request = Request::DepartRequest { departing_node: self.info.clone() };
        let response = self.send_request(bootstrap_addr, request).await?;
        
        match response {
            Response::DepartSuccess => {
                // Clear local state
                let mut state = self.state.write().await;
                state.successor = None;
                state.predecessor = None;
                state.data.clear();
                
                info!("Departed ring via bootstrap");
                Ok(())
            }
            Response::Error(e) => Err(anyhow::anyhow!("Depart failed: {}", e)),
            _ => Err(anyhow::anyhow!("Unexpected response from bootstrap")),
        }
    }

    /// Start listening and handling requests
    pub async fn run(&self) -> anyhow::Result<()> {
        let server = Server::bind(self.info.addr).await?;
        let state = Arc::clone(&self.state);
        let info = self.info.clone();
        server.listen(move |request_bytes, from| {
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

    /// Find the successor of a given address
    pub async fn find_successor(&self, addr: SocketAddr) -> anyhow::Result<NodeInfo> {
        let state = self.state.read().await;
        if let Some(ref successor) = state.successor {
            // If we're the only node or successor is us, return successor
            if successor.addr == self.info.addr {
                return Ok(successor.clone());
            }
            // Copy successor address before dropping state
            let successor_addr = successor.addr;
            drop(state);
            let request = Request::FindSuccessor { addr };
            let response = self.send_request(successor_addr, request).await?;
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
        let successor = self.find_successor(self.info.addr).await?;
        if successor.addr == self.info.addr {
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
        let successor = self.find_successor(self.info.addr).await?;
        if successor.addr == self.info.addr {
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
        
        Request::FindSuccessor { addr: _ } => {
            let state_guard = state.read().await;
            if let Some(ref successor) = state_guard.successor {
                Response::Successor(successor.clone())
            } else {
                Response::Successor(info.clone())
            }
        }
        
        Request::GetPredecessor => {
            let state_guard = state.read().await;
            Response::Predecessor(state_guard.predecessor.clone())
        }
        
        Request::GetSuccessor => {
            let state_guard = state.read().await;
            Response::Successor(state_guard.successor.clone().unwrap_or(info.clone()))
        }
        
        Request::SetPredecessor { node } => {
            let mut state_guard = state.write().await;
            state_guard.predecessor = Some(node.clone());
            info!("Set predecessor to {}", node.addr);
            Response::Ok
        }
        
        Request::SetSuccessor { node } => {
            let mut state_guard = state.write().await;
            state_guard.successor = Some(node.clone());
            info!("Set successor to {}", node.addr);
            Response::Ok
        }
        
        Request::Notify { node } => {
            let mut state_guard = state.write().await;
            // Update predecessor if needed
            if state_guard.predecessor.is_none() {
                state_guard.predecessor = Some(node.clone());
                info!("Set predecessor to {}", node.addr);
            } else if let Some(ref pred) = state_guard.predecessor {
                if node.addr != pred.addr {
                    state_guard.predecessor = Some(node.clone());
                    info!("Updated predecessor to {}", node.addr);
                }
            }
            Response::Ok
        }
        
        Request::JoinRequest { joining_node: _ } => {
            // Regular nodes don't handle JoinRequest - only bootstrap does
            warn!("Regular node received JoinRequest - should be sent to bootstrap");
            Response::Error("JoinRequest should be sent to bootstrap node".to_string())
        }
        
        Request::DepartRequest { departing_node: _ } => {
            // Regular nodes don't handle DepartRequest - only bootstrap does
            warn!("Regular node received DepartRequest - should be sent to bootstrap");
            Response::Error("DepartRequest should be sent to bootstrap node".to_string())
        }
        
        Request::TransferKeys { to_addr: _ } => {
            // Transfer all keys (bootstrap-coordinated transfer)
            let mut state_guard = state.write().await;
            let keys: Vec<(String, String)> = state_guard.data.drain().collect();
            info!("Transferring {} keys", keys.len());
            Response::Keys(keys)
        }
        
        Request::TransferKeysTo { target_addr } => {
            // Bootstrap told us to transfer our keys to target
            let keys = {
                let mut state_guard = state.write().await;
                state_guard.data.drain().collect::<Vec<_>>()
            };
            info!("TransferKeysTo {}: {} keys", target_addr, keys.len());
            
            if !keys.is_empty() {
                // Send keys to target node
                let request = Request::ReceiveKeys { keys: keys.clone() };
                let request_bytes = request.to_bytes()?;
                match connect(target_addr).await {
                    Ok(client) => {
                        match client.message(&request_bytes).await {
                            Ok(_) => info!("Transferred {} keys to {}", keys.len(), target_addr),
                            Err(e) => warn!("Failed to transfer keys to {}: {}", target_addr, e),
                        }
                    }
                    Err(e) => warn!("Failed to connect to {} for key transfer: {}", target_addr, e),
                }
            }
            Response::Ok
        }
        
        Request::ReceiveKeys { keys } => {
            let mut state_guard = state.write().await;
            let count = keys.len();
            for (key, value) in keys {
                state_guard.data.insert(key, value);
            }
            info!("Received {} keys", count);
            Response::Ok
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
