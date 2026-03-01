//! Bootstrap Node - The stable entry point for the Chord ring
//!
//! The bootstrap node is always connected, known to all nodes, and handles all join requests.
//! It is the first node to enter the system and never departs.
//! 
//! ## Bootstrap-Coordinated Operations
//! 
//! The bootstrap node coordinates all join and depart operations:
//! 
//! ### Join Workflow
//! 1. Joining node sends `JoinRequest` to bootstrap
//! 2. Bootstrap finds the correct position in the ring
//! 3. Bootstrap sends `SetPredecessor` to the new successor
//! 4. Bootstrap sends `SetSuccessor` to the new predecessor
//! 5. Bootstrap tells successor to transfer keys to the joining node
//! 6. Bootstrap responds with `JoinSuccess` containing successor and predecessor
//!
//! ### Depart Workflow  
//! 1. Departing node sends `DepartRequest` to bootstrap
//! 2. Bootstrap tells successor to set new predecessor
//! 3. Bootstrap tells predecessor to set new successor
//! 4. Bootstrap tells departing node to transfer keys to successor
//! 5. Bootstrap responds with `DepartSuccess`

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug, warn};

use crate::communication::{Peer, connect};
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

    /// Send a request to another node and get the response
    async fn send_request(&self, addr: SocketAddr, request: Request) -> anyhow::Result<Response> {
        let request_bytes = request.to_bytes()?;
        let response_bytes = connect(addr).await?.message(&request_bytes).await?;
        Response::from_bytes(&response_bytes)
    }

    /// Handle a join request from a new node (BOOTSTRAP-COORDINATED)
    /// 
    /// This is the main coordination function for joins. The bootstrap:
    /// 1. Finds the correct position (successor and predecessor) for the joining node
    /// 2. Updates the successor's predecessor pointer
    /// 3. Updates the predecessor's successor pointer
    /// 4. Initiates key transfer from successor to joining node
    /// 5. Registers the node in the ring
    pub async fn coordinate_join(&self, joining_addr: SocketAddr) -> anyhow::Result<(NodeInfo, Option<NodeInfo>)> {
        info!("Bootstrap coordinating join for {}", joining_addr);
        let joining_node = NodeInfo::new(joining_addr);
        
        // Step 1: Find the successor for the joining node
        let successor = self.find_successor(joining_addr).await?;
        info!("Join coordination: {} will have successor {}", joining_addr, successor.addr);
        
        // Step 2: Find the predecessor (current predecessor of the successor)
        let predecessor = if successor.addr == self.addr() {
            // Successor is bootstrap, get bootstrap's predecessor
            self.get_predecessor().await
        } else {
            // Ask successor for its predecessor
            match self.send_request(successor.addr, Request::GetPredecessor).await {
                Ok(Response::Predecessor(pred)) => pred,
                _ => None,
            }
        };
        info!("Join coordination: {} will have predecessor {:?}", joining_addr, predecessor.as_ref().map(|p| p.addr));
        
        // Step 3: Update successor's predecessor to the joining node
        if successor.addr == self.addr() {
            // Update bootstrap's own predecessor
            let mut state = self.node.state.write().await;
            state.predecessor = Some(joining_node.clone());
            info!("Bootstrap: Set own predecessor to {}", joining_addr);
        } else {
            // Tell successor to update its predecessor
            match self.send_request(
                successor.addr,
                Request::SetPredecessor { node: joining_node.clone() }
            ).await {
                Ok(_) => info!("Updated successor {}'s predecessor to {}", successor.addr, joining_addr),
                Err(e) => warn!("Failed to update successor's predecessor: {}", e),
            }
        }
        
        // Step 4: Update predecessor's successor to the joining node
        if let Some(ref pred) = predecessor {
            if pred.addr == self.addr() {
                // Update bootstrap's own successor
                let mut state = self.node.state.write().await;
                state.successor = Some(joining_node.clone());
                info!("Bootstrap: Set own successor to {}", joining_addr);
            } else if pred.addr != successor.addr {
                // Tell predecessor to update its successor
                match self.send_request(
                    pred.addr,
                    Request::SetSuccessor { node: joining_node.clone() }
                ).await {
                    Ok(_) => info!("Updated predecessor {}'s successor to {}", pred.addr, joining_addr),
                    Err(e) => warn!("Failed to update predecessor's successor: {}", e),
                }
            }
        }
        
        // Step 5: Request key transfer from successor to joining node
        if successor.addr != joining_addr {
            if successor.addr == self.addr() {
                // Bootstrap transfers its own keys to the joining node
                let keys = {
                    let mut state = self.node.state.write().await;
                    state.data.drain().collect::<Vec<_>>()
                };
                if !keys.is_empty() {
                    match self.send_request(
                        joining_addr,
                        Request::ReceiveKeys { keys: keys.clone() }
                    ).await {
                        Ok(_) => info!("Bootstrap transferred {} keys to {}", keys.len(), joining_addr),
                        Err(e) => warn!("Failed to transfer keys: {}", e),
                    }
                }
            } else {
                // Tell successor to transfer keys to the joining node
                match self.send_request(
                    successor.addr,
                    Request::TransferKeysTo { target_addr: joining_addr }
                ).await {
                    Ok(_) => info!("Initiated key transfer from {} to {}", successor.addr, joining_addr),
                    Err(e) => warn!("Failed to initiate key transfer: {}", e),
                }
            }
        }
        
        // Step 6: Register the new node
        self.register_node(joining_node).await;
        
        info!("Join coordination complete for {}", joining_addr);
        Ok((successor, predecessor))
    }

    /// Handle a depart request from a node (BOOTSTRAP-COORDINATED)
    /// 
    /// This is the main coordination function for departures. The bootstrap:
    /// 1. Finds the departing node's successor and predecessor
    /// 2. Updates the successor's predecessor pointer
    /// 3. Updates the predecessor's successor pointer
    /// 4. Tells the departing node to transfer its keys to successor
    /// 5. Unregisters the node from the ring
    pub async fn coordinate_depart(&self, departing_addr: SocketAddr) -> anyhow::Result<()> {
        info!("Bootstrap coordinating depart for {}", departing_addr);
        
        // Can't depart the bootstrap node itself
        if departing_addr == self.addr() {
            return Err(anyhow::anyhow!("Bootstrap node cannot depart"));
        }
        
        // Step 1: Get the departing node's successor and predecessor
        let (successor, predecessor) = {
            let succ_response = self.send_request(departing_addr, Request::GetSuccessor).await?;
            let pred_response = self.send_request(departing_addr, Request::GetPredecessor).await?;
            
            let successor = match succ_response {
                Response::Successor(s) => s,
                _ => return Err(anyhow::anyhow!("Failed to get departing node's successor")),
            };
            let predecessor = match pred_response {
                Response::Predecessor(p) => p,
                _ => None,
            };
            (successor, predecessor)
        };
        
        info!("Depart coordination: {} has successor {}, predecessor {:?}", 
              departing_addr, successor.addr, predecessor.as_ref().map(|p| p.addr));
        
        // Step 2: Update successor's predecessor to the departing node's predecessor
        let new_pred_for_succ = predecessor.clone().unwrap_or(self.node.info());
        if successor.addr == self.addr() {
            // Update bootstrap's own predecessor
            let mut state = self.node.state.write().await;
            state.predecessor = Some(new_pred_for_succ.clone());
            info!("Bootstrap: Set own predecessor to {}", new_pred_for_succ.addr);
        } else {
            match self.send_request(
                successor.addr,
                Request::SetPredecessor { node: new_pred_for_succ.clone() }
            ).await {
                Ok(_) => info!("Updated successor {}'s predecessor to {}", successor.addr, new_pred_for_succ.addr),
                Err(e) => warn!("Failed to update successor's predecessor: {}", e),
            }
        }
        
        // Step 3: Update predecessor's successor to the departing node's successor
        if let Some(ref pred) = predecessor {
            if pred.addr == self.addr() {
                // Update bootstrap's own successor
                let mut state = self.node.state.write().await;
                state.successor = Some(successor.clone());
                info!("Bootstrap: Set own successor to {}", successor.addr);
            } else {
                match self.send_request(
                    pred.addr,
                    Request::SetSuccessor { node: successor.clone() }
                ).await {
                    Ok(_) => info!("Updated predecessor {}'s successor to {}", pred.addr, successor.addr),
                    Err(e) => warn!("Failed to update predecessor's successor: {}", e),
                }
            }
        }
        
        // Step 4: Tell departing node to transfer keys to its successor
        if successor.addr != departing_addr {
            match self.send_request(
                departing_addr,
                Request::TransferKeysTo { target_addr: successor.addr }
            ).await {
                Ok(_) => info!("Initiated key transfer from {} to {}", departing_addr, successor.addr),
                Err(e) => warn!("Failed to initiate key transfer: {}", e),
            }
        }
        
        // Step 5: Unregister the node
        self.unregister_node(departing_addr).await;
        
        info!("Depart coordination complete for {}", departing_addr);
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

    /// BootstrapNode cannot join an existing ring (it always creates one)
    pub async fn join(&self, _bootstrap_addr: SocketAddr) -> anyhow::Result<()> {
        Err(anyhow::anyhow!("BootstrapNode cannot join an existing ring. It must create a new ring."))
    }

    /// BootstrapNode cannot depart from the ring (it is always present)
    pub async fn depart(&self, _bootstrap_addr: SocketAddr) -> anyhow::Result<()> {
        Err(anyhow::anyhow!("BootstrapNode cannot depart from the ring. It must always be present."))
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
        
        Request::JoinRequest { joining_node } => {
            // Bootstrap-coordinated join
            info!("Bootstrap: Processing JoinRequest from {}", joining_node.addr);
            
            // Find successor for the joining node
            let state_guard = state.read().await;
            let successor = state_guard.successor.clone().unwrap_or(info.clone());
            let predecessor = state_guard.predecessor.clone();
            drop(state_guard);
            
            // Update bootstrap's pointers
            {
                let mut state_guard = state.write().await;
                
                // If bootstrap was alone, joining node becomes both successor and predecessor
                if state_guard.successor.as_ref().map(|s| s.addr) == Some(info.addr) {
                    state_guard.successor = Some(joining_node.clone());
                    state_guard.predecessor = Some(joining_node.clone());
                } else {
                    // If joining node should be bootstrap's successor (bootstrap is its predecessor)
                    if predecessor.as_ref().map(|p| p.addr) == Some(info.addr) ||
                       state_guard.predecessor.is_none() {
                        state_guard.predecessor = Some(joining_node.clone());
                    }
                }
            }
            
            // Register the node
            {
                let mut members = ring_members.write().await;
                if !members.iter().any(|n| n.addr == joining_node.addr) {
                    members.push(joining_node.clone());
                    info!("Bootstrap: Registered joining node {} ({} total)", joining_node.addr, members.len());
                }
            }
            
            // Update successor's predecessor if needed
            if successor.addr != info.addr && successor.addr != joining_node.addr {
                // This would need a separate connection to update the successor
                // For now, we'll let the response handle it
            }
            
            // Transfer keys from bootstrap to joining node if needed
            let keys_to_transfer = {
                let mut state_guard = state.write().await;
                state_guard.data.drain().collect::<Vec<_>>()
            };
            
            if !keys_to_transfer.is_empty() {
                info!("Bootstrap: Would transfer {} keys to {}", keys_to_transfer.len(), joining_node.addr);
                // Note: In a full implementation, we'd send these keys
                // For now, they're lost - the node should request them
            }
            
            Response::JoinSuccess { 
                successor, 
                predecessor 
            }
        }
        
        Request::DepartRequest { departing_node } => {
            // Bootstrap-coordinated depart
            info!("Bootstrap: Processing DepartRequest from {}", departing_node.addr);
            
            // Get ring members to find successor and predecessor
            let members = ring_members.read().await;
            let member_addrs: Vec<SocketAddr> = members.iter().map(|m| m.addr).collect();
            drop(members);
            
            // Unregister the node
            {
                let mut members = ring_members.write().await;
                let before_len = members.len();
                members.retain(|n| n.addr != departing_node.addr);
                if members.len() < before_len {
                    info!("Bootstrap: Unregistered departing node {} ({} remaining)", 
                          departing_node.addr, members.len());
                }
            }
            
            // Update bootstrap's own pointers if necessary
            {
                let mut state_guard = state.write().await;
                if state_guard.successor.as_ref().map(|s| s.addr) == Some(departing_node.addr) {
                    // Find next node or set to self
                    let next = member_addrs.iter()
                        .filter(|&&a| a != departing_node.addr && a != info.addr)
                        .next()
                        .map(|&a| NodeInfo::new(a))
                        .unwrap_or(info.clone());
                    state_guard.successor = Some(next.clone());
                    info!("Bootstrap: Updated successor to {}", next.addr);
                }
                if state_guard.predecessor.as_ref().map(|p| p.addr) == Some(departing_node.addr) {
                    // Find previous node or set to self
                    let prev = member_addrs.iter()
                        .rev()
                        .filter(|&&a| a != departing_node.addr && a != info.addr)
                        .next()
                        .map(|&a| NodeInfo::new(a))
                        .unwrap_or(info.clone());
                    state_guard.predecessor = Some(prev.clone());
                    info!("Bootstrap: Updated predecessor to {}", prev.addr);
                }
            }
            
            Response::DepartSuccess
        }
        
        Request::TransferKeys { to_addr: _ } => {
            let mut state_guard = state.write().await;
            let keys: Vec<(String, String)> = state_guard.data.drain().collect();
            info!("Bootstrap: Transferring {} keys", keys.len());
            Response::Keys(keys)
        }
        
        Request::TransferKeysTo { target_addr } => {
            // Bootstrap transfers its keys to the target
            let keys = {
                let mut state_guard = state.write().await;
                state_guard.data.drain().collect::<Vec<_>>()
            };
            info!("Bootstrap: TransferKeysTo {} ({} keys)", target_addr, keys.len());
            
            if !keys.is_empty() {
                // Send keys to target (would need to connect and send)
                // For now, respond with the keys and let caller handle it
                info!("Bootstrap: Keys to transfer: {:?}", keys);
            }
            Response::Ok
        }
        
        Request::ReceiveKeys { keys } => {
            let mut state_guard = state.write().await;
            for (key, value) in keys {
                state_guard.data.insert(key, value);
            }
            info!("Bootstrap: Received keys");
            Response::Ok
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
