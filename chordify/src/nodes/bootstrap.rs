//! Bootstrap Node - The stable entry point for the Chord ring
//!
//! The bootstrap node is always connected, known to all nodes, and handles all join requests.
//! It is the first node to enter the system and never departs.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug, warn};

use crate::communication::Peer;
use super::protocol::{Request, Response, NodeInfo};
use super::node::{Node, NodeState};

/// A Bootstrap node for the Chord DHT ring.
/// 
/// The bootstrap node:
/// - Is the first node in the ring
/// - Is always connected (never departs)
/// - Has a known IP address
/// - Handles all join requests from new nodes
/// - Coordinates successor/predecessor updates and key transfers
pub struct BootstrapNode {
    /// The underlying node
    node: Node,
    /// Track all nodes in the ring (for coordination)
    ring_members: Arc<RwLock<Vec<NodeInfo>>>,
}

impl BootstrapNode {
    /// Create a new bootstrap node at the given address.
    /// This is the first node in the ring.
    pub fn new(addr: SocketAddr) -> Self {
        let node = Node::new(addr);
        Self {
            node,
            ring_members: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Get this node's address
    pub fn addr(&self) -> SocketAddr {
        self.node.addr()
    }

    /// Get this node's info
    pub fn info(&self) -> NodeInfo {
        self.node.info()
    }

    /// Initialize the ring with this bootstrap node as the only member
    pub async fn create_ring(&self) {
        self.node.create_ring().await;
        let mut members = self.ring_members.write().await;
        members.push(self.node.info());
        info!("Bootstrap node initialized ring at {}", self.addr());
    }

    /// Get the underlying node (for access to Node methods)
    pub fn inner(&self) -> &Node {
        &self.node
    }

    /// Get the successor
    pub async fn get_successor(&self) -> Option<NodeInfo> {
        self.node.get_successor().await
    }

    /// Get the predecessor
    pub async fn get_predecessor(&self) -> Option<NodeInfo> {
        self.node.get_predecessor().await
    }

    /// Find the successor for a given address
    pub async fn find_successor(&self, addr: SocketAddr) -> anyhow::Result<NodeInfo> {
        self.node.find_successor(addr).await
    }

    /// Store a key-value pair
    pub async fn put(&self, key: String, value: String) -> anyhow::Result<()> {
        self.node.put(key, value).await
    }

    /// Get a value by key
    pub async fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.node.get(key).await
    }

    /// Get all ring members (bootstrap-specific)
    pub async fn get_ring_members(&self) -> Vec<NodeInfo> {
        self.ring_members.read().await.clone()
    }

    /// Register a new node in the ring (called when a node joins)
    pub async fn register_node(&self, node_info: NodeInfo) {
        let mut members = self.ring_members.write().await;
        if !members.iter().any(|n| n.addr == node_info.addr) {
            members.push(node_info.clone());
            info!("Registered new node {} in ring ({} total members)", node_info.addr, members.len());
        }
    }

    /// Unregister a node from the ring (called when a node departs)
    pub async fn unregister_node(&self, addr: SocketAddr) {
        let mut members = self.ring_members.write().await;
        let before_len = members.len();
        members.retain(|n| n.addr != addr);
        if members.len() < before_len {
            info!("Unregistered node {} from ring ({} remaining members)", addr, members.len());
        }
    }

    /// Handle a join request from a new node
    /// Returns the successor for the joining node
    pub async fn handle_join(&self, joining_addr: SocketAddr) -> anyhow::Result<NodeInfo> {
        info!("Handling join request from {}", joining_addr);
        
        // Find the successor for the joining node
        let successor = self.find_successor(joining_addr).await?;
        
        // Register the new node
        self.register_node(NodeInfo::new(joining_addr)).await;
        
        info!("Join handled: {} will have successor {}", joining_addr, successor.addr);
        Ok(successor)
    }

    /// Handle a depart notification from a node
    pub async fn handle_depart(&self, departing_addr: SocketAddr) -> anyhow::Result<()> {
        info!("Handling depart notification from {}", departing_addr);
        
        // Unregister the node
        self.unregister_node(departing_addr).await;
        
        info!("Depart handled: {} removed from ring", departing_addr);
        Ok(())
    }

    /// Start listening and handling requests (bootstrap-aware)
    pub async fn run(&self) -> anyhow::Result<()> {
        let peer = Peer::bind(self.addr()).await?;
        let node_state = self.node.state_clone();
        let node_info = self.node.info();
        let ring_members = Arc::clone(&self.ring_members);

        info!("Bootstrap node listening on {}", self.addr());

        peer.listen(move |request_bytes, from| {
            let state = Arc::clone(&node_state);
            let info = node_info.clone();
            let members = Arc::clone(&ring_members);
            async move {
                handle_bootstrap_request(request_bytes, from, info, state, members).await
            }
        }).await
    }

    /// Prevent bootstrap from departing (bootstrap never departs)
    pub async fn depart(&self) -> anyhow::Result<()> {
        warn!("Bootstrap node cannot depart - it must always stay connected");
        Err(anyhow::anyhow!("Bootstrap node cannot depart"))
    }
}

/// Handle an incoming request (bootstrap-aware version)
async fn handle_bootstrap_request(
    request_bytes: Vec<u8>,
    from: SocketAddr,
    info: NodeInfo,
    state: Arc<RwLock<NodeState>>,
    ring_members: Arc<RwLock<Vec<NodeInfo>>>,
) -> anyhow::Result<Vec<u8>> {
    let request = Request::from_bytes(&request_bytes)?;
    debug!("Bootstrap received {:?} from {}", request, from);
    
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
            info!("Bootstrap: Set predecessor to {}", node.addr);
            Response::Ok
        }
        
        Request::SetSuccessor { node } => {
            let mut state_guard = state.write().await;
            state_guard.successor = Some(node.clone());
            info!("Bootstrap: Set successor to {}", node.addr);
            Response::Ok
        }
        
        Request::Notify { node } => {
            let mut state_guard = state.write().await;
            if state_guard.predecessor.is_none() {
                state_guard.predecessor = Some(node.clone());
                info!("Bootstrap: Set predecessor to {}", node.addr);
            } else if let Some(ref pred) = state_guard.predecessor {
                if node.addr != pred.addr {
                    state_guard.predecessor = Some(node.clone());
                    info!("Bootstrap: Updated predecessor to {}", node.addr);
                }
            }
            Response::Ok
        }
        
        Request::Join { node } => {
            // Bootstrap handles join: register node and return successor
            let mut members = ring_members.write().await;
            if !members.iter().any(|n| n.addr == node.addr) {
                members.push(node.clone());
                info!("Bootstrap: Registered joining node {} ({} total)", node.addr, members.len());
            }
            drop(members);
            
            let state_guard = state.read().await;
            if let Some(ref successor) = state_guard.successor {
                Response::Successor(successor.clone())
            } else {
                Response::Successor(info.clone())
            }
        }
        
        Request::TransferKeys { to_addr: _ } => {
            let mut state_guard = state.write().await;
            let keys: Vec<(String, String)> = state_guard.data.drain().collect();
            info!("Bootstrap: Transferring {} keys", keys.len());
            Response::Keys(keys)
        }
        
        Request::Put { key, value } => {
            let mut state_guard = state.write().await;
            state_guard.data.insert(key.clone(), value);
            debug!("Bootstrap: Stored key '{}'", key);
            Response::Ok
        }
        
        Request::Get { key } => {
            let state_guard = state.read().await;
            Response::Value(state_guard.data.get(&key).cloned())
        }
        
        Request::Delete { key } => {
            let mut state_guard = state.write().await;
            state_guard.data.remove(&key);
            debug!("Bootstrap: Deleted key '{}'", key);
            Response::Ok
        }
    };
    
    response.to_bytes()
}
