//! Node - A Chord DHT node implementation
//!
//! Uses the P2P communication layer for all network operations.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug};

use crate::communication::{Peer, connect};
use super::protocol::{Request, Response, NodeInfo};

/// A Chord DHT node
pub struct Node {
    /// This node's info (address only)
    info: NodeInfo,
    /// Shared mutable state
    pub(crate) state: Arc<RwLock<NodeState>>,
    /// Bootstrap node address (optional)
    bootstrap_addr: Option<SocketAddr>,
}

/// Mutable state for a node
pub(crate) struct NodeState {
    /// Our successor in the ring
    pub(crate) successor: Option<NodeInfo>,
    /// Our predecessor in the ring
    pub(crate) predecessor: Option<NodeInfo>,
    /// Local key-value storage
    pub(crate) data: HashMap<String, String>,
}

impl Node {
    /// Create a new node bound to the given address, optionally with a bootstrap node
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

    /// Set bootstrap node address
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

    /// Create the first node in a new ring (no existing nodes)
    pub async fn create_ring(&self) {
        // Enforce only one bootstrap node per ring (per address)
        match connect(self.info.addr).await {
            Ok(_) => {
                panic!("Cannot create ring: another node is already listening at {}. Use join() instead.", self.info.addr);
            }
            Err(_) => {
                let mut state = self.state.write().await;
                state.successor = Some(self.info.clone());
                state.predecessor = Some(self.info.clone());
                info!("Created new ring, node is alone at {}", self.info.addr);
            }
        }
    }

    /// Join an existing ring via a known node (or bootstrap node if set)
    pub async fn join(&self, known_addr: SocketAddr) -> anyhow::Result<()> {
        let bootstrap = self.bootstrap_addr.unwrap_or(known_addr);
        info!("Joining ring via {}", bootstrap);
        
        // Step 1: Ask known node to find our successor
        let request = Request::FindSuccessor { addr: self.info.addr };
        let response = self.send_request(bootstrap, request).await?;
        let successor = match response {
            Response::Successor(s) => s,
            Response::Error(e) => return Err(anyhow::anyhow!("Join failed: {}", e)),
            _ => return Err(anyhow::anyhow!("Unexpected response")),
        };
        
        // Step 2: Get predecessor from our new successor
        let pred_response = self.send_request(successor.addr, Request::GetPredecessor).await?;
        let predecessor = match pred_response {
            Response::Predecessor(p) => p,
            _ => None,
        };
        
        // Step 3: Update our state
        {
            let mut state = self.state.write().await;
            state.successor = Some(successor.clone());
            state.predecessor = predecessor.clone();
        }
        info!("Joined ring, successor is at {}", successor.addr);
        
        // Step 4: Tell successor we are its new predecessor
        let _ = self.send_request(
            successor.addr,
            Request::SetPredecessor { node: self.info.clone() }
        ).await;
        
        // Step 5: Tell predecessor (if any) we are its new successor
        if let Some(ref pred) = predecessor {
            if pred.addr != successor.addr {
                let _ = self.send_request(
                    pred.addr,
                    Request::SetSuccessor { node: self.info.clone() }
                ).await;
            }
        }
        
        // Step 6: Request key transfer from successor (keys we're now responsible for)
        let keys_response = self.send_request(
            successor.addr,
            Request::TransferKeys { to_addr: self.info.addr }
        ).await?;
        if let Response::Keys(keys) = keys_response {
            let mut state = self.state.write().await;
            for (key, value) in keys {
                state.data.insert(key, value);
            }
            info!("Received {} keys from successor", state.data.len());
        }
        
        Ok(())
    }

    /// Graceful departure: notify neighbors and transfer keys before leaving
    pub async fn depart(&self) -> anyhow::Result<()> {
        let (successor, predecessor, keys) = {
            let state = self.state.read().await;
            (state.successor.clone(), state.predecessor.clone(), state.data.clone())
        };
        
        info!("Node at {} departing, transferring {} keys", self.info.addr, keys.len());
        
        // Step 1: Transfer all keys to successor
        if let Some(ref succ) = successor {
            if succ.addr != self.info.addr {
                for (key, value) in &keys {
                    let request = Request::Put { key: key.clone(), value: value.clone() };
                    let _ = self.send_request(succ.addr, request).await;
                }
                info!("Transferred {} keys to successor {}", keys.len(), succ.addr);
            }
        }
        
        // Step 2: Update successor's predecessor to our predecessor
        if let Some(ref succ) = successor {
            if succ.addr != self.info.addr {
                let new_pred = predecessor.clone().unwrap_or(succ.clone());
                let _ = self.send_request(
                    succ.addr,
                    Request::SetPredecessor { node: new_pred }
                ).await;
            }
        }
        
        // Step 3: Update predecessor's successor to our successor
        if let Some(ref pred) = predecessor {
            if pred.addr != self.info.addr {
                let new_succ = successor.clone().unwrap_or(pred.clone());
                let _ = self.send_request(
                    pred.addr,
                    Request::SetSuccessor { node: new_succ }
                ).await;
            }
        }
        
        // Clear local state
        {
            let mut state = self.state.write().await;
            state.successor = None;
            state.predecessor = None;
            state.data.clear();
        }
        
        info!("Node at {} departed gracefully", self.info.addr);
        Ok(())
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
        Request::Join { node: _ } => {
            // Find successor for the joining node
            let state_guard = state.read().await;
            if let Some(ref successor) = state_guard.successor {
                Response::Successor(successor.clone())
            } else {
                Response::Successor(info.clone())
            }
        }
        Request::TransferKeys { to_addr: _ } => {
            // Transfer keys that the new node is now responsible for
            // For simplicity, transfer all keys (proper implementation would check key ranges)
            let mut state_guard = state.write().await;
            let keys: Vec<(String, String)> = state_guard.data.drain().collect();
            info!("Transferring {} keys to new node", keys.len());
            Response::Keys(keys)
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
