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

use sha1::{Sha1, Digest};
const N: u64 = 10; // Number of bits in the identifier space (SHA-1 hash size)

/// A Chord DHT node
#[derive(Debug)]
pub struct Node {
    /// This node's info (address only)
    info: NodeInfo,
    /// Shared mutable state
    pub(crate) state: Arc<RwLock<NodeState>>,
    /// Bootstrap node address (required for join/depart)
    bootstrap_addr: Option<SocketAddr>,
}

// Node is cheap to clone: the fields are `Clone`/`Arc`.
impl Clone for Node {
    fn clone(&self) -> Self {
        Self {
            info: self.info.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

/// Mutable state for a node
#[derive(Debug)]
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
        // wrap `self` in an Arc so the handler can keep a reference long-term
        let node = Arc::new(self.clone());

        server.listen(move |request_bytes, from| {
            let node = Arc::clone(&node);
            async move { node.handle_request(request_bytes, from).await }
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

    /// Send a request and ignore the response; propagate errors if desired.
    async fn send_request_no_response(&self, addr: SocketAddr, request: Request) -> anyhow::Result<()> {
        let request_bytes = request.to_bytes()?;
        connect(addr).await?.send(&request_bytes).await?;
        Ok(())
    }


    // Forward message to closest node

    // Auxuliary functions for hashing and key responsibility
    fn hash_value(val: &str) -> u64 {
        let mut hasher = Sha1::new();
        hasher.update(val.as_bytes());
        let hash = hasher.finalize();

        // Calculate the hash value modulo 2^N
        let hash_value = u64::from_be_bytes(hash[0..8].try_into().unwrap());
        hash_value % (1 << N) // Assuming an N-bit identifier space
    }

    async fn belongs_to_current (&self, key_hash: u64, node_hash: u64) -> bool {
        let prev = self.state.read().await.predecessor.clone();
        let prev_hash = Self::hash_value(&prev.unwrap().addr.to_string());

        (prev_hash < node_hash && key_hash > prev_hash && key_hash <= node_hash) ||
        (prev_hash > node_hash && (key_hash > prev_hash || key_hash <= node_hash))
    }


    /// Insert a key-value pair
    pub async fn insert(&self, key: String, value: String) -> anyhow::Result<()> {
        // Hash the key to find its identifier
        let key_hash = Self::hash_value(&key);
        let node_hash = Self::hash_value(&self.info.addr.to_string());

        debug!("Inserting key '{}' with hash {} (node hash {})", key, key_hash, node_hash);

        // If responsible node for the key is this node, store it locally
        // FIX ME: CHECK BEHAVIOUR ON SAME KEY INSERTS
        if self.belongs_to_current(key_hash, node_hash).await {
            let mut state = self.state.write().await;
            state.data.insert(key.clone(), value);
            debug!("Stored key '{}' locally", key);
            Ok(())
        }
        // Otherwise, forward to the appropriate node (successor or predecessor)
        else {
            let forward_dist = if node_hash >= key_hash {(1 << N) - node_hash + key_hash } 
            else {key_hash - node_hash };
            let request = Request::Insert { key, value };

            // Forward to successor if it's closer, otherwise forward to predecessor
            if (forward_dist < (1 << (N - 1))) && self.state.read().await.successor.is_some() {
                let successor = self.state.read().await.successor.clone().unwrap();
                self.send_request_no_response(successor.addr, request).await?;
            } else {
                let predecessor = self.state.read().await.predecessor.clone().unwrap();
                self.send_request_no_response(predecessor.addr, request).await?;
            }
            Ok(())
        }
    }


    /// Query function to retrieve a value by key
    pub async fn query(&self, key: String) -> anyhow::Result<()> {
        // Hash the key to find its identifier
        let key_hash = Self::hash_value(&key);
        let node_hash = Self::hash_value(&self.info.addr.to_string());

        if key!="*" {
            // If responsible node for the key is this node, retrieve it locally
            if self.belongs_to_current(key_hash, node_hash).await {
                let state = self.state.read().await;
                let value = state.data.get(&key).cloned();
                println!("{:?}", value);
                Ok(())
            }
            // Otherwise, forward to the appropriate node (successor or predecessor)
            else {
                let forward_dist = if node_hash >= key_hash {(1 << N) - node_hash + key_hash } 
                else {key_hash - node_hash };
                let request = Request::Query { key, source: self.info.addr };

                // Forward to successor if it's closer, otherwise forward to predecessor
                if (forward_dist < (1 << (N - 1))) && self.state.read().await.successor.is_some() {
                    let successor = self.state.read().await.successor.clone().unwrap();
                    self.send_request_no_response(successor.addr, request).await?;
                } else {
                    let predecessor = self.state.read().await.predecessor.clone().unwrap();
                    self.send_request_no_response(predecessor.addr, request).await?;
                }
                Ok(())
            }
        }
        else {
            // Handle wildcard query: retrieve all key-value pairs from this node and forward to successor/predecessor
            Ok(())
        }
    }


    /// Delete a key-value pair
    pub async fn delete(&self, key: String) -> anyhow::Result<()> {
        // Hash the key to find its identifier
        let key_hash = Self::hash_value(&key);
        let node_hash = Self::hash_value(&self.info.addr.to_string());

        debug!("Deleting key '{}' with hash {} (node hash {})", key, key_hash, node_hash);

        // If responsible node for the key is this node, store it locally
        // FIX ME: CHECK BEHAVIOUR ON SAME KEY INSERTS
        if self.belongs_to_current(key_hash, node_hash).await {
            let mut state = self.state.write().await;
            state.data.remove(&key);
            debug!("Deleted key '{}' locally", key);
            Ok(())
        }
        // Otherwise, forward to the appropriate node (successor or predecessor)
        else {
            let forward_dist = if node_hash >= key_hash {(1 << N) - node_hash + key_hash } 
            else {key_hash - node_hash };
            let request = Request::Delete { key };

            // Forward to successor if it's closer, otherwise forward to predecessor
            if (forward_dist < (1 << (N - 1))) && self.state.read().await.successor.is_some() {
                let successor = self.state.read().await.successor.clone().unwrap();
                self.send_request_no_response(successor.addr, request).await?;
            } else {
                let predecessor = self.state.read().await.predecessor.clone().unwrap();
                self.send_request_no_response(predecessor.addr, request).await?;
            }
            Ok(())
        }
    
    }


    /// Handle an incoming request
    async fn handle_request(&self, request_bytes: Vec<u8>, from: SocketAddr)
    -> anyhow::Result<Vec<u8>> {
        let request = Request::from_bytes(&request_bytes)?;
        debug!("Received {:?} from {}", request, from);
        let response = match request {
            Request::Ping => Response::Pong,
            Request::FindSuccessor { addr: _ } => {
                let state_guard = self.state.read().await;
                if let Some(ref successor) = state_guard.successor {
                    if successor.addr == self.info.addr {
                        Response::Successor(successor.clone())
                    } else {
                        Response::Successor(successor.clone())
                    }
                } else {
                    Response::Successor(self.info.clone())
                }
            }
            Request::GetPredecessor => {
                let state_guard = self.state.read().await;
                Response::Predecessor(state_guard.predecessor.clone())
            }
        Request::GetSuccessor => {
            let state_guard = self.state.read().await;
            Response::Successor(state_guard.successor.clone().unwrap_or(self.info.clone()))
        }
        
        Request::SetPredecessor { node } => {
            let mut state_guard = self.state.write().await;
            state_guard.predecessor = Some(node.clone());
            info!("Set predecessor to {}", node.addr);
            Response::Ok
        }
        
        Request::SetSuccessor { node } => {
            let mut state_guard = self.state.write().await;
            state_guard.successor = Some(node.clone());
            info!("Set successor to {}", node.addr);
            Response::Ok
        }
            Request::Notify { node } => {
                let mut state_guard = self.state.write().await;
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
            let mut state_guard = self.state.write().await;
            let keys: Vec<(String, String)> = state_guard.data.drain().collect();
            info!("Transferring {} keys", keys.len());
            Response::Keys(keys)
        }

        Request::TransferKeysTo { target_addr } => {
            // Bootstrap told us to transfer our keys to target
            let keys = {
                let mut state_guard = self.state.write().await;
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
            let mut state_guard = self.state.write().await;
            let count = keys.len();
            for (key, value) in keys {
                state_guard.data.insert(key, value);
            }
            info!("Received {} keys", count);
            Response::Ok
        }

            Request::Insert { key, value } => {
                match self.insert(key, value).await {
                    Ok(()) => Response::Ok,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            Request::Query { key, source } => {
                // Hash the key to find its identifier
                let key_hash = Self::hash_value(&key);
                let node_hash = Self::hash_value(&self.info.addr.to_string());

                
                // If responsible node for the key is this node, retrieve it locally
                if self.belongs_to_current(key_hash, node_hash).await {
                    let state = self.state.read().await;
                    let value = state.data.get(&key).cloned();
                    let predecessor = self.state.read().await.predecessor.clone().unwrap();
                    let request = Request::QueryResponse { source, value };
                    self.send_request_no_response(predecessor.addr, request).await?;
                }
                // Otherwise, forward to the appropriate node (successor or predecessor)
                else {
                    let forward_dist = if node_hash >= key_hash {(1 << N) - node_hash + key_hash } 
                    else {key_hash - node_hash };
                    let request = Request::Query { key, source };

                    // Forward to successor if it's closer, otherwise forward to predecessor
                    if (forward_dist < (1 << (N - 1))) && self.state.read().await.successor.is_some() {
                        let successor = self.state.read().await.successor.clone().unwrap();
                        self.send_request_no_response(successor.addr, request).await?;
                    } else {
                        let predecessor = self.state.read().await.predecessor.clone().unwrap();
                        self.send_request_no_response(predecessor.addr, request).await?;
                    }
                }
                Response::Ok
                
            }
            Request::QueryResponse { source, value } => {
                if source == self.info.addr {
                    println!("{:?}", value);
                    Response::Ok
                } else {
                    // Forward the response back to the original requester
                    let request = Request::QueryResponse { source, value };
                    let source_hash = Self::hash_value(&source.to_string());
                    let node_hash = Self::hash_value(&self.info.addr.to_string());
                    let forward_dist = if node_hash >= source_hash {(1 << N) - node_hash + source_hash } 
                    else {source_hash - node_hash };

                    // Forward to successor if it's closer, otherwise forward to predecessor
                    if (forward_dist < (1 << (N - 1))) && self.state.read().await.successor.is_some() {
                        let successor = self.state.read().await.successor.clone().unwrap();
                        self.send_request_no_response(successor.addr, request).await?;
                    } else {
                        let predecessor = self.state.read().await.predecessor.clone().unwrap();
                        self.send_request_no_response(predecessor.addr, request).await?;
                    }
                    Response::Ok
                }
            }
            Request::Delete { key } => {
                match self.delete(key).await {
                    Ok(()) => Response::Ok,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            _ => Response::Error("Unsupported request type".to_string()),
            // Request::Delete { key } => {
            //     let mut state_guard = state.write().await;
            //     state_guard.data.remove(&key);
            //     debug!("Deleted key '{}'", key);
            //     Response::Ok
            // }
        };
        response.to_bytes()

    }
}
