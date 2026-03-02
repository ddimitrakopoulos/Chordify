//! Node - A Chord DHT node implementation
//! DONE: Broke NodeInfo into addr and id
//! DONE: Merged Node and NodeState into a single struct for simplicity
//! DONE: Made id into a BigUint (160-bit hash) instead of u64 to comply with the Chord specification
//! DONE: Removed is_bootstrap fn as the bootstrap addr should be set at node creation and not change dynamically
//! DONE: Removed state_clone and state_arc - we can just use self with async locks for state management
//! DONE: Made all getters start with get_ for consistency
//! DONE: Added the is_responsible() function to check if this node is responsible for a given key ID
//! 
//! 
//! 
//! 
//! DONE: Added detailed comments and logging for clarity
//! TO-DO: Why is the bootstrap_addr optional?

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug};

use crate::tcp::{Server, connect};
use super::protocol::{Request, Response};

use sha1::{Sha1, Digest}; // SHA-1 is used for hashing the IP:port to get the node ID (160-bit hash)
use serde::{Serialize, Deserialize};
const N: u64 = 10;

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    // IP:Port address of the node (this is only 4+2 bytes and lives in the stack Copy)
    pub addr: SocketAddr,
    // The ID of the node in the Chord ring (this lives in the heap and needs to use clone())
    pub id: u64,
}

/// A Chord DHT node
#[derive(Debug)]
pub struct Node {
    // IP:Port address and ID of this node
    info: NodeInfo,
    //
    state: Arc<RwLock<NodeState>>,
    // Bootstrap node address (required for join/depart)
    bootstrap_addr: SocketAddr,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct NodeState {
    // The ID of the successor in the ring
    successor: NodeInfo,
    // The ID of the predecessor in the ring
    predecessor: NodeInfo,
    // Local key-value storage
    data: HashMap<String, String>,
}

// Helper function that takes an IP:port address and returns its SHA-1 hash as a BigUint (for node ID and key hashing)
pub fn hash_value(data: &str) -> u64 {
    let mut hasher = Sha1::new();
    
    // We pass the data string that we the break to bytes.
    hasher.update(data.as_bytes());
    
    // Get the hash result as a byte array (20 bytes for SHA-1)
    let result = hasher.finalize();
    
    // Convert the byte array to a u64
    let hash_value = u64::from_be_bytes(result[0..8].try_into().unwrap());
    hash_value % (1 << N) // Assuming an N-bit identifier space
}

impl Node {
    /// Given an IP:port address and bootstrap address we create a new node instance.
    pub fn new(addr: SocketAddr, bootstrap_addr: SocketAddr) -> Self {
        info!("Node with addr: {addr} created with ID: {}", hash_value(&addr.to_string()));
        
        Self { 
            info: NodeInfo {
                addr,
                id: hash_value(&addr.to_string()),
            },
            state: Arc::new(RwLock::new(NodeState {
                successor: NodeInfo { addr: SocketAddr::new("0.0.0.0".parse().unwrap(), 0), id: 0 },
                predecessor: NodeInfo { addr: SocketAddr::new("0.0.0.0".parse().unwrap(), 0), id: 0 }, 
                data: HashMap::new(), 
            })),
            bootstrap_addr   
        }
    }

    /// GETTERS
    pub fn get_addr(&self) -> SocketAddr {
        self.info.addr
    }

    pub fn get_id(&self) -> u64 {
        self.info.id
    }

        /// Get the successor
    pub async fn get_successor(&self) -> NodeInfo {
        self.state.read().await.successor.clone()
    }

    /// Get the predecessor
    pub async fn get_predecessor(&self) -> NodeInfo {
        self.state.read().await.predecessor.clone()
    }

    /// Check if this node is responsible for a given key ID
    pub async fn is_responsible(&self, key_id: &String, predecessor_id: u64) -> bool {
        let key_id_hash = hash_value(key_id);
        // We read the predecessor under a lock to ensure we have a consistent view of the ring
        // state while checking responsibility. We await until we can acquire the lock.

        // If there is no predecessor, then this node is the only node in the ring and is
        // responsible for all the keys

        // If the ID of the predecessor is the same as this node's ID, then again this node is
        // responsible for all keys
        if predecessor_id == self.info.id {
            return true;
        }

        // Check if the key ID falls between the predecessor's ID and this node's ID
        // Normal case:
        if predecessor_id < self.info.id {
            key_id_hash > predecessor_id && key_id_hash <= self.info.id
        } 
        // Wrap-around case:
        else { 
            key_id_hash > predecessor_id || key_id_hash <= self.info.id
        }
    }

    // 1. Change `&self` to `self: Arc<Self>`
    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        let server = Server::bind(self.info.addr).await?;
        
        // 2. Now `self` IS an Arc<Node>, so we can clone it!
        // This creates our owned Arc pointer for the outer closure.
        let node_owned = Arc::clone(&self); 

        server.listen(move |request_bytes, from| {
            
            // 3. Clone it again for the inner async block
            let node_inner = Arc::clone(&node_owned); 
            
            async move { 
                node_inner.handle_request(request_bytes, from).await 
            }
        }).await
    }


    /// Join an existing ring via bootstrap node
    /// 
    /// The node only contacts the bootstrap, which coordinates all pointer 
    /// updates and key transfers.
    pub async fn join(&self) -> anyhow::Result<()> {
        info!("Node with ID {} joining ring via bootstrap at {}", self.info.id, self.bootstrap_addr);
        
        // Send JoinRequest to bootstrap
        let request = Request::JoinRequest { joining_node: self.info.clone() };
        let response = self.send_request(self.bootstrap_addr, request).await?;
        
        match response {
            Response::JoinSuccess { successor, predecessor } => {
                // Update our state with the assigned successor and predecessor
                let mut state = self.state.write().await;
                state.successor = successor.clone();
                state.predecessor = predecessor.clone();
                
                info!("Joined ring via bootstrap: successor={}, predecessor={:?}", 
                      successor.addr, predecessor.addr);
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

        // Send all the data to the successor before departing (only if successor is not itself)
        let successor = self.get_successor().await;
        if successor.addr != self.info.addr && successor.id != 0 {
            let state = self.state.read().await;
            let data_clone = state.data.clone();
            drop(state); // Release the lock before sending
            
            let request = Request::TransferData { data: data_clone };
            if let Err(e) = self.send_request_no_response(successor.addr, request).await {
                //warn!("Failed to transfer data to successor during depart: {}", e);
            }
        }

        
        // Send DepartRequest to bootstrap
        let request = Request::DepartRequest { departing_node: self.info.clone() };
        let response = self.send_request(bootstrap_addr, request).await?;
        
        match response {
            Response::DepartSuccess => {
                // Clear local state
                let mut state = self.state.write().await;
                state.successor = NodeInfo { addr: SocketAddr::new("0.0.0.0".parse().unwrap(), 0), id: 0 };
                state.predecessor = NodeInfo { addr: SocketAddr::new("0.0.0.0".parse().unwrap(), 0), id: 0 };
                state.data.clear();
                
                info!("Departed ring via bootstrap");
                Ok(())
            }
            Response::Error(e) => Err(anyhow::anyhow!("Depart failed: {}", e)),
            _ => Err(anyhow::anyhow!("Unexpected response from bootstrap")),
        }
    }

    /// Send a request to another node and get the response
    async fn send_request(&self, addr: SocketAddr, request: Request) -> anyhow::Result<Response> {
        let request_bytes = request.to_bytes()?;
        let response_bytes = connect(addr).await?.message(&request_bytes).await?;
        Response::from_bytes(&response_bytes)
    }



    /// Send a request and ignore the response; propagate errors if desired.
    async fn send_request_no_response(&self, addr: SocketAddr, request: Request) -> anyhow::Result<()> {
        let request_bytes = request.to_bytes()?;
        connect(addr).await?.send(&request_bytes).await?;
        Ok(())
    }





    async fn belongs_to_current (&self, key_hash: u64, node_hash: u64) -> bool {
        let prev = self.state.read().await.predecessor.clone();
        let prev_hash = hash_value(&prev.addr.to_string());

        (prev_hash < node_hash && key_hash > prev_hash && key_hash <= node_hash) ||
        (prev_hash > node_hash && (key_hash > prev_hash || key_hash <= node_hash)) ||
        (prev_hash == node_hash) // Only node in the ring
    }


    /// Insert a key-value pair
    pub async fn insert(&self, key: String, value: String) -> anyhow::Result<()> {
        // Hash the key to find its identifier
        let key_hash = hash_value(&key);
        let node_hash = hash_value(&self.info.addr.to_string());

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
            if forward_dist < (1 << (N - 1)){
                let successor = self.state.read().await.successor.clone();
                self.send_request_no_response(successor.addr, request).await?;
            } else {
                let predecessor = self.state.read().await.predecessor.clone();
                self.send_request_no_response(predecessor.addr, request).await?;
            }
            Ok(())
        }
    }


    /// Query function to retrieve a value by key
    pub async fn query(&self, key: String) -> anyhow::Result<()> {
        // Hash the key to find its identifier
        let key_hash = hash_value(&key);
        let node_hash = hash_value(&self.info.addr.to_string());

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
                if forward_dist < (1 << (N - 1)) {
                    let successor = self.state.read().await.successor.clone();
                    self.send_request_no_response(successor.addr, request).await?;
                } else {
                    let predecessor = self.state.read().await.predecessor.clone();
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
        let key_hash = hash_value(&key);
        let node_hash = hash_value(&self.info.addr.to_string());

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
            if forward_dist < (1 << (N - 1))  {
                let successor = self.state.read().await.successor.clone();
                self.send_request_no_response(successor.addr, request).await?;
            } else {
                let predecessor = self.state.read().await.predecessor.clone();
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

            Request::SetPredecessor { node } => {
                let mut state_guard = self.state.write().await;
                state_guard.predecessor = node.clone();
                info!("Set predecessor to {}", node.addr);
                Response::Ok
            }
            
            Request::SetSuccessor { node } => {
                let mut state_guard = self.state.write().await;
                state_guard.successor = node.clone();
                info!("Set successor to {}", node.addr);
                Response::Ok
            }

            Request::SetPredecessorWithKeys { node } => {
                let mut state_guard = self.state.write().await;
                state_guard.predecessor = node.clone();
                let predecessor_id = state_guard.predecessor.clone().id;
                info!("Set predecessor to {} with keys", node.addr);

                // Find the data that should be transferred to the new predecessor (keys for which the new predecessor is now responsible)
                let mut new_data = HashMap::new();
                for (key, value) in state_guard.data.iter() {
                    if !self.is_responsible(key, predecessor_id).await {
                        new_data.insert(key.clone(), value.clone());
                    }

                }

                // Send the keys to the new predecessor
                let request = Request::TransferData { data: new_data };
                self.send_request_no_response(node.addr, request).await?;
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
                let key_hash = hash_value(&key);
                let node_hash = hash_value(&self.info.addr.to_string());

                
                // If responsible node for the key is this node, retrieve it locally
                if self.belongs_to_current(key_hash, node_hash).await {
                    let state = self.state.read().await;
                    let value = state.data.get(&key).cloned();
                    let predecessor = self.state.read().await.predecessor.clone();
                    let request = Request::QueryResponse { source, value };
                    self.send_request_no_response(predecessor.addr, request).await?;
                }
                // Otherwise, forward to the appropriate node (successor or predecessor)
                else {
                    let forward_dist = if node_hash >= key_hash {(1 << N) - node_hash + key_hash } 
                    else {key_hash - node_hash };
                    let request = Request::Query { key, source };

                    // Forward to successor if it's closer, otherwise forward to predecessor
                    if forward_dist < (1 << (N - 1)) {
                        let successor = self.state.read().await.successor.clone();
                        self.send_request_no_response(successor.addr, request).await?;
                    } else {
                        let predecessor = self.state.read().await.predecessor.clone();
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
                    let source_hash = hash_value(&source.to_string());
                    let node_hash = hash_value(&self.info.addr.to_string());
                    let forward_dist = if node_hash >= source_hash {(1 << N) - node_hash + source_hash } 
                    else {source_hash - node_hash };

                    // Forward to successor if it's closer, otherwise forward to predecessor
                    if forward_dist < (1 << (N - 1)) {
                        let successor = self.state.read().await.successor.clone();
                        self.send_request_no_response(successor.addr, request).await?;
                    } else {
                        let predecessor = self.state.read().await.predecessor.clone();
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
            },
            Request::TransferData { data } => {
                let mut state = self.state.write().await;
                for (key, value) in data {
                    state.data.insert(key, value);
                }
                Response::Ok
            },
            _ => Response::Error("Unsupported request".to_string()),
        };
        response.to_bytes()

    }
}
