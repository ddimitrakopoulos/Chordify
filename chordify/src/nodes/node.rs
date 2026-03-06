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
    // Replicated data (song name, song value, replication position, origin node info)
    replicated_data: HashMap<String, (String, u64, NodeInfo)>,
    /// Replication factor
    k: u64,
    /// Replication type (0 for linearizability, 1 for eventual consistency)
    t: u8,
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
                replicated_data: HashMap::new(),
                k: 1,
                t: 0,
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

    /// Check if this node is responsible for a given key hash
    /// Uses the current predecessor from the node's state
    async fn is_responsible_for_hash(&self, key_hash: u64) -> bool {
        let predecessor = self.state.read().await.predecessor.clone();
        let predecessor_id = predecessor.id;
        
        // Single node in ring (predecessor is itself or uninitialized)
        if predecessor_id == self.info.id || predecessor_id == 0 {
            return true;
        }

        // Check if the key hash falls between predecessor and this node in the Chord ring
        // Normal case: predecessor_id < node_id
        if predecessor_id < self.info.id {
            key_hash > predecessor_id && key_hash <= self.info.id
        } 
        // Wrap-around case: node_id < predecessor_id (ring wraps around)
        else { 
            key_hash > predecessor_id || key_hash <= self.info.id
        }
    }

    /// Check if a key belongs to this node (taking a String key)
    async fn is_responsible_for_key(&self, key: &String) -> bool {
        let key_hash = hash_value(key);
        self.is_responsible_for_hash(key_hash).await
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
            Response::JoinSuccess { successor, predecessor, k, t } => {
                // Update our state with the assigned successor, predecessor, and replication params
                let mut state = self.state.write().await;
                state.successor = successor.clone();
                state.predecessor = predecessor.clone();
                state.k = k;
                state.t = t;
                
                info!("Joined ring via bootstrap: successor={}, predecessor={:?}, k={}, t={}",
                      successor.addr, predecessor.addr, k, t);
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
            if let Err(_) = self.send_request_no_response(successor.addr, request).await {
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

                if state.k > 1 {
                    // If we have replicas, we need to transfer them to the successor as well
                    // Send UpdateReplicasOnDepart to successor to update replicas and transfer any new keys that should be replicated to the successor
                    let request = Request::UpdateReplicasOnDepart {
                        data: state.data.clone(),
                        k_left: state.k-1,
                        startingNodeInfo: state.successor.clone(),
                        replicated_data: state.replicated_data.clone(),
                    };
                    self.send_request_no_response(state.successor.addr, request).await?;
                }

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

    /// Insert a key-value pair
    pub async fn insert(&self, key: String, value: String) -> anyhow::Result<()> {
        let state = self.state.read().await;
        let k = state.k;
        let t = state.t;
        drop(state);

        if k <= 1 {
            debug!("Inserting key '{}' with no replication", key);
            self.insert_no_replication(key, value).await
        } else if t == 0 {
            debug!("Inserting key '{}' with replication factor Linearizability", key);
            self.insert_linearizability(key, value).await
        } else {
            debug!("Inserting key '{}' with replication factor Eventual Consistency", key);
            self.insert_eventual_consistency(key, value).await
        }
    }

    pub async fn insert_no_replication(&self, key: String, value: String) -> anyhow::Result<()> {
        // Hash the key to find its identifier
        let key_hash = hash_value(&key);

        debug!("Inserting key '{}' with hash {}", key, key_hash);

        // If responsible node for the key is this node, store it locally
        if self.is_responsible_for_hash(key_hash).await {
            let mut state = self.state.write().await;

            // Check if the key already exists and concat the values 
            if let Some(existing_value) = state.data.get(&key) {
                let new_value = format!("{}{}", existing_value, value);
                state.data.insert(key.clone(), new_value);
            } 
            else { 
                state.data.insert(key.clone(), value);
                debug!("Stored key '{}' locally", key);
            }

            Ok(())
        }
        // Otherwise, forward to the appropriate node (successor or predecessor)
        else {
            let node_hash = self.info.id;
            let forward_dist = if node_hash >= key_hash {(1 << N) - node_hash + key_hash } 
            else {key_hash - node_hash };
            let request = Request::Insert { key, value };

            // Forward to successor if it's closer, otherwise forward to predecessor
            if forward_dist < (1 << (N - 1)){
                let successor = self.state.read().await.successor.clone();
                let _ = self.send_request(successor.addr, request).await?;
            } else {
                let predecessor = self.state.read().await.predecessor.clone();
                let _ = self.send_request(predecessor.addr, request).await?;
            }
            Ok(())
        }
    }

    pub async fn insert_linearizability(&self, key: String, value: String) -> anyhow::Result<()> {
        // Hash the key to find its identifier
        let key_hash = hash_value(&key);

        debug!("Inserting key '{}' with hash {}", key, key_hash);

        // If responsible node for the key is this node, store it locally
        if self.is_responsible_for_hash(key_hash).await {
            let mut state = self.state.write().await;
            // Start with the incoming value and update if the key already exists.
            let mut new_value = value.clone();

            if let Some(existing_value) = state.data.get(&key) {
                // concatenate on existing data
                new_value = format!("{}{}", existing_value, value);
            }

            // insert/update using a clone so `new_value` remains available for
            // the replica request below
            state.data.insert(key.clone(), new_value.clone());
            debug!("Stored key '{}' locally", key);
            let successor = state.successor.clone();
            let request = Request::InsertReplica {
                key: key.clone(),
                value: new_value,
                node_info: self.info.clone(),
                k_left: state.k - 1,
            };
            drop(state);
            let _ = self.send_request(successor.addr, request).await?;

            Ok(())
        }
        // Otherwise, forward to the appropriate node (successor or predecessor)
        else {
            let node_hash = self.info.id;
            let forward_dist = if node_hash >= key_hash {(1 << N) - node_hash + key_hash } 
            else {key_hash - node_hash };
            let request = Request::Insert { key, value };

            // Forward to successor if it's closer, otherwise forward to predecessor
            if forward_dist < (1 << (N - 1)){
                let successor = self.state.read().await.successor.clone();
                let _ = self.send_request(successor.addr, request).await?;
            } else {
                let predecessor = self.state.read().await.predecessor.clone();
                let _ = self.send_request(predecessor.addr, request).await?;
            }
            Ok(())
        }
    }

    /// Stub for eventual consistency insert.
    pub async fn insert_eventual_consistency(&self, key: String, value: String) -> anyhow::Result<()> {
        // Hash the key to find its identifier
        let key_hash = hash_value(&key);

        debug!("Inserting key '{}' with hash {}", key, key_hash);

        // If responsible node for the key is this node, store it locally
        if self.is_responsible_for_hash(key_hash).await {
            let mut state = self.state.write().await;
            // Start with the incoming value and update if the key already exists.
            let mut new_value = value.clone();

            if let Some(existing_value) = state.data.get(&key) {
                // concatenate on existing data
                new_value = format!("{}{}", existing_value, value);
            }

            // insert/update using a clone so `new_value` remains available for
            // the replica request below
            state.data.insert(key.clone(), new_value.clone());
            debug!("Stored key '{}' locally", key);

            // IMPORTANT: do not hold the write-lock while awaiting network I/O.
            let successor = state.successor.clone();
            let k_left = state.k - 1;
            drop(state);

            let request = Request::InsertReplica {
                key: key.clone(),
                value: new_value,
                node_info: self.info.clone(),
                k_left,
            };
            let _ = self.send_request(successor.addr, request).await?;

            Ok(())
        }
        // Otherwise, forward to the appropriate node (successor or predecessor)
        else {
            let node_hash = self.info.id;
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
    pub async fn query(&self, key: String) -> Vec<(u64, Vec<String>)> {
        let state = self.state.read().await;
        let k = state.k;
        let t = state.t;
        drop(state);

        if k <= 1 {
            debug!("Querying key '{}' with no replication", key);
            self.query_no_replication(key).await
        } else if t == 0 {
            debug!("Querying key '{}' with replication factor Linearizability", key);
            self.query_linearizability(key).await
        } else {
            debug!("Querying key '{}' with replication factor Eventual Consistency", key);
            self.query_eventual_consistency(key).await
        }
    }

    pub async fn query_no_replication(&self, key: String) -> Vec<(u64, Vec<String>)> {
        // Hash the key to find its identifier
        let key_hash = hash_value(&key);

        if key!="*" {
            // If responsible node for the key is this node, retrieve it locally
            if self.is_responsible_for_hash(key_hash).await {
                let state = self.state.read().await;
                let value = state.data.get(&key).cloned();
                let result = vec![(key_hash, value.map(|v| vec![v]).unwrap_or_else(Vec::new))];
                return result;
            }
            // Otherwise, forward to the appropriate node (successor or predecessor)
            else {
                let node_hash = self.info.id;
                let forward_dist = if node_hash >= key_hash {(1 << N) - node_hash + key_hash } 
                else {key_hash - node_hash };
                let request = Request::Query { key, source: self.info.addr };
                // we'll store the result of the forwarded request here; start with a successful placeholder
                let mut response: anyhow::Result<Response> = Ok(Response::Ok);

                // Forward to successor if it's closer, otherwise forward to predecessor
                if forward_dist < (1 << (N - 1)) {
                    let successor = self.state.read().await.successor.clone();
                    response = self.send_request(successor.addr, request).await;
                }
                else {
                    let predecessor = self.state.read().await.predecessor.clone();
                    response = self.send_request(predecessor.addr, request).await;
                }
                
                match response {
                    Ok(Response::QueryResponse { source: _, value }) => {
                        let result = vec![(key_hash, value.map(|v| vec![v]).unwrap_or_else(Vec::new))];
                        return result;
                    }
                    _ => {
                        debug!("Failed to get response from predecessor during query");
                        return vec![];
                    }
                }
            
            }
        
        }
        else {
            // Handle wildcard query: retrieve all key-value pairs from this node and forward to successor/predecessor
            let state = self.state.read().await;
            let data_clone = state.data.clone();
            drop(state); // Release the lock before sending

            let node_hash = self.info.id;
            let request = Request::QueryAll { source: self.info.addr, data: vec![(node_hash, data_clone)] };

            // Forward to successor and predecessor
            let successor = self.state.read().await.successor.clone();
            let response = self.send_request(successor.addr, request.clone()).await;
            match response {
                Ok(Response::QueryAll { source: _, data }) => {
                    let mut newvec = vec![];
                    for (node_hash, kv_pairs) in data {
                        let values = kv_pairs.into_iter().map(|(k,v)| format!("{}:{}", k, v)).collect();
                        newvec.push((node_hash, values));
                    }
                    return newvec;
                }
                _ => {
                    debug!("Failed to get response from successor during wildcard query");
                    return vec![];
                }
            }
        }
    }


    pub async fn query_linearizability(&self, key: String) -> Vec<(u64, Vec<String>)> {
        // Hash the key to find its identifier
        let key_hash = hash_value(&key);

        if key != "*" {
            if self.is_responsible_for_hash(key_hash).await {
                // In linearizability we get the result from the last replica in the chain.
                let (successor_addr, k) = {
                    let state = self.state.read().await;
                    (state.successor.addr, state.k)
                };

                let request = Request::QueryLastReplica { key, k_left: k - 1 };
                let response = self.send_request(successor_addr, request).await;

                match response {
                    Ok(Response::QueryResponse { source: _, value }) => {
                        vec![(key_hash, value.map(|v| vec![v]).unwrap_or_else(Vec::new))]
                    }
                    _ => {
                        debug!("Failed to get response from successor during query");
                        vec![]
                    }
                }
            }
            // Otherwise, forward to the appropriate node (successor or predecessor)
            else {
                let node_hash = self.info.id;
                let forward_dist = if node_hash >= key_hash {(1 << N) - node_hash + key_hash } 
                else {key_hash - node_hash };
                let request = Request::Query { key, source: self.info.addr };
                // we'll store the result of the forwarded request here; start with a successful placeholder
                let mut response: anyhow::Result<Response> = Ok(Response::Ok);

                // Forward to successor if it's closer, otherwise forward to predecessor
                if forward_dist < (1 << (N - 1)) {
                    let successor = self.state.read().await.successor.clone();
                    response = self.send_request(successor.addr, request).await;
                }
                else {
                    let predecessor = self.state.read().await.predecessor.clone();
                    response = self.send_request(predecessor.addr, request).await;
                }
                
                match response {
                    Ok(Response::QueryResponse { source: _, value }) => {
                        let result = vec![(key_hash, value.map(|v| vec![v]).unwrap_or_else(Vec::new))];
                        return result;
                    }
                    _ => {
                        debug!("Failed to get response from predecessor during query");
                        return vec![];
                    }
                }
            }
        
        }
        else {
            // Handle wildcard query: retrieve all key-value pairs from this node and forward to successor/predecessor
            let state = self.state.read().await;
            let data_clone = state.replicated_data.clone();
            drop(state); // Release the lock before sending

            let mut map = HashMap::new();
            let mut origin_node_hash = self.info.id;
            
            for (k, (v, pos, node_info )) in data_clone {
                if pos == 1 {
                    map.insert(k, v);
                    origin_node_hash = node_info.id;
                }
            }

            // Only send origin node data if we found replicas, otherwise send our own empty entry
            let initial_data = vec![(origin_node_hash, map),(self.info.id, HashMap::new())];
            let request = Request::QueryAll { source: self.info.addr, data: initial_data };

            // Forward to successor and predecessor
            let successor = self.state.read().await.successor.clone();
            let response = self.send_request(successor.addr, request.clone()).await;
            match response {
                Ok(Response::QueryAll { source: _, data }) => {
                    // Debug: print raw data
                    debug!("Raw QueryAll data received (linearizability):");
                    for (node_hash, kv_pairs) in &data {
                        debug!("  node_hash={}, num_keys={}", node_hash, kv_pairs.len());
                        for (k, v) in kv_pairs {
                            debug!("    {}:{}", k, v);
                        }
                    }

                    // Count occurrences of each node hash
                    let mut node_counts: HashMap<u64, usize> = HashMap::new();
                    for (node_hash, _) in &data {
                        *node_counts.entry(*node_hash).or_insert(0) += 1;
                    }

                    // For node hashes that appear multiple times, keep the entry with the most keys
                    let mut best_by_node: HashMap<u64, Vec<String>> = HashMap::new();

                    for (node_hash, kv_pairs) in data {
                        let values: Vec<String> = kv_pairs
                            .into_iter()
                            .map(|(k, v)| format!("{}:{}", k, v))
                            .collect();

                        // If this node hash appears only once, add it directly
                        if node_counts.get(&node_hash).copied().unwrap_or(0) == 1 {
                            best_by_node.insert(node_hash, values);
                        } else {
                            // Multiple entries for this node hash - keep the one with most keys
                            let should_replace = match best_by_node.get(&node_hash) {
                                Some(existing) => values.len() > existing.len(),
                                None => true,
                            };

                            if should_replace {
                                best_by_node.insert(node_hash, values);
                            }
                        }
                    }

                    debug!("After aggregation (linearizability): {} unique nodes", best_by_node.len());

                    return best_by_node.into_iter().collect();
                }
                _ => {
                    debug!("Failed to get response from successor during wildcard query");
                    return vec![];
                }
            }
        }
    }

    pub async fn query_eventual_consistency(&self, key: String) -> Vec<(u64, Vec<String>)> {
        // For eventual consistency we can return a best-effort value from the responsible node.
        // Hash the key to find its identifier
        let key_hash = hash_value(&key);
        let state = self.state.read().await;

        if key!="*" {
            // If responsible node for the key is this node, retrieve it locally
            if self.is_responsible_for_hash(key_hash).await {
                let value = state.data.get(&key).cloned();
                let result = vec![(key_hash, value.map(|v| vec![v]).unwrap_or_else(Vec::new))];
                return result;
            }
            else if state.replicated_data.contains_key(&key) {
                // If we have a replica for the key, return it
                let (value, _, _) = state.replicated_data.get(&key).unwrap().clone();
                let result = vec![(key_hash, vec![value])];
                return result;
            }
            // Otherwise, forward to the appropriate node (successor or predecessor)
            else {
                let node_hash = self.info.id;
                let forward_dist = if node_hash >= key_hash {(1 << N) - node_hash + key_hash } 
                else {key_hash - node_hash };
                let request = Request::Query { key, source: self.info.addr };
                // we'll store the result of the forwarded request here; start with a successful placeholder
                let mut response: anyhow::Result<Response> = Ok(Response::Ok);

                // Forward to successor if it's closer, otherwise forward to predecessor
                if forward_dist < (1 << (N - 1)) {
                    let successor = state.successor.clone();
                    response = self.send_request(successor.addr, request).await;
                }
                else {
                    let predecessor = state.predecessor.clone();
                    response = self.send_request(predecessor.addr, request).await;
                }
                
                match response {
                    Ok(Response::QueryResponse { source: _, value }) => {
                        let result = vec![(key_hash, value.map(|v| vec![v]).unwrap_or_else(Vec::new))];
                        return result;
                    }
                    _ => {
                        debug!("Failed to get response from predecessor during query");
                        return vec![];
                    }
                }
            
            }
        
        }
        else {
            // Handle wildcard query: retrieve all key-value pairs from this node and forward to successor/predecessor
            let state = self.state.read().await;
            let data_clone = state.data.clone();
            drop(state); // Release the lock before sending

            let node_hash = self.info.id;
            let request = Request::QueryAll { source: self.info.addr, data: vec![(node_hash, data_clone)] };

            // Forward to successor and predecessor
            let successor = self.state.read().await.successor.clone();
            let response = self.send_request(successor.addr, request.clone()).await;
            match response {
                Ok(Response::QueryAll { source: _, data }) => {
                    // Count occurrences of each node hash
                    let mut node_counts: HashMap<u64, usize> = HashMap::new();
                    for (node_hash, _) in &data {
                        *node_counts.entry(*node_hash).or_insert(0) += 1;
                    }

                    // For node hashes that appear multiple times, keep the entry with the most keys
                    let mut best_by_node: HashMap<u64, Vec<String>> = HashMap::new();

                    for (node_hash, kv_pairs) in data {
                        let values: Vec<String> = kv_pairs
                            .into_iter()
                            .map(|(k, v)| format!("{}:{}", k, v))
                            .collect();

                        // If this node hash appears only once, add it directly
                        if node_counts.get(&node_hash).copied().unwrap_or(0) == 1 {
                            best_by_node.insert(node_hash, values);
                        } else {
                            // Multiple entries for this node hash - keep the one with most keys
                            let should_replace = match best_by_node.get(&node_hash) {
                                Some(existing) => values.len() > existing.len(),
                                None => true,
                            };

                            if should_replace {
                                best_by_node.insert(node_hash, values);
                            }
                        }
                    }

                    return best_by_node.into_iter().collect();
                }
                _ => {
                    debug!("Failed to get response from successor during wildcard query");
                    return vec![];
                }
            }
        }
    }

    /// Delete a key-value pair
    pub async fn delete(&self, key: String) -> anyhow::Result<()> {
        let state = self.state.read().await;
        let k = state.k;
        let t = state.t;
        drop(state);

        if k <= 1 {
            debug!("Deleting key '{}' with no replication", key);
            self.delete_no_replication(key).await
        } else if t == 0 {
            debug!("Deleting key '{}' with replication factor Linearizability", key);
            self.delete_linearizability(key).await
        } else {
            debug!("Deleting key '{}' with replication factor Eventual Consistency", key);
            self.delete_eventual_consistency(key).await
        }
    }

    pub async fn delete_no_replication(&self, key: String) -> anyhow::Result<()> {
        // Hash the key to find its identifier
        let key_hash = hash_value(&key);

        debug!("Deleting key '{}' with hash {}", key, key_hash);

        // If responsible node for the key is this node, delete it locally
        if self.is_responsible_for_hash(key_hash).await {
            let mut state = self.state.write().await;
            state.data.remove(&key);
            debug!("Deleted key '{}' locally", key);
            Ok(())
        }
        // Otherwise, forward to the appropriate node (successor or predecessor)
        else {
            let node_hash = self.info.id;
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

    pub async fn delete_linearizability(&self, key: String) -> anyhow::Result<()> {
        // For linearizability, we can implement delete similarly to insert with replication. 
        // The responsible node deletes the key locally and then sends a DeleteReplica request to the successor to delete the replica, which forwards it along the replication chain until it reaches the end.
        // This way, the delete will only return after all replicas have been deleted, ensuring linearizability.
        // banana

        // Hash the key to find its identifier
        let key_hash = hash_value(&key);

        debug!("Deleting key '{}' with hash {}", key, key_hash);

        // If responsible node for the key is this node, delete it locally
        if self.is_responsible_for_hash(key_hash).await {
            let mut state = self.state.write().await;
            state.data.remove(&key);
            debug!("Deleted key '{}' locally", key);

            let request = Request::DeleteReplica { key: key.clone(), node_info: self.info.clone(), k_left: state.k - 1 };
            self.send_request_no_response(state.successor.addr, request).await?;

            Ok(())
        }
        // Otherwise, forward to the appropriate node (successor or predecessor)
        else {
            let node_hash = self.info.id;
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

    pub async fn delete_eventual_consistency(&self, key: String) -> anyhow::Result<()> {
        // For eventual consistency, delete at the responsible node and rely on background
        // mechanisms to propagate the delete.
        // Hash the key to find its identifier
        let key_hash = hash_value(&key);

        debug!("Deleting key '{}' with hash {}", key, key_hash);

        // If responsible node for the key is this node, delete it locally
        if self.is_responsible_for_hash(key_hash).await {
            let mut state = self.state.write().await;
            state.data.remove(&key);
            debug!("Deleted key '{}' locally", key);

            let request = Request::DeleteReplica { key: key.clone(), node_info: self.info.clone(), k_left: state.k - 1 };
            self.send_request_no_response(state.successor.addr, request).await?;

            Ok(())
        }
        // Otherwise, forward to the appropriate node (successor or predecessor)
        else {
            let node_hash = self.info.id;
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


    pub async fn overlay(&self) -> Vec<(u64, SocketAddr)> {
        let request = Request::Overlay;
        let response = self.send_request(self.bootstrap_addr, request).await;

        match response {
            Ok(Response::OverlayResponse { topology }) => topology,
            _ => {
                debug!("Failed to get overlay response from bootstrap");
                vec![]
            }
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
                drop(state_guard); // Release lock before checking responsibility

                Response::Ok
            }

            Request::SetPredecessorWithKeys { node } => {
                let mut state_guard = self.state.write().await;
                state_guard.predecessor = node.clone();
                info!("Set predecessor to {} with keys", node.addr);
                drop(state_guard); // Release lock before checking responsibility

                // Find the data that should be transferred to the new predecessor
                // (keys for which this node is no longer responsible)
                let state = self.state.read().await;
                let mut new_data = HashMap::new();
                let mut keys_to_remove = Vec::new();
                for (key, value) in state.data.iter() {
                    if !self.is_responsible_for_key(key).await {
                        new_data.insert(key.clone(), value.clone());
                        keys_to_remove.push(key.clone());
                    }
                }
                drop(state);

                // Delete the collected keys under a write lock
                if !keys_to_remove.is_empty() {
                    let mut state = self.state.write().await;
                    for key in keys_to_remove {
                        state.data.remove(&key);
                    }
                    drop(state);
                }

                // Send the keys to the new predecessor
                if !new_data.is_empty() {
                    let request = Request::TransferData { data: new_data };
                    self.send_request(node.addr, request).await?;
                }

                // Send replicated data that should be transferred to the new predecessor
                let state = self.state.read().await;
                if state.k > 1 {
                    let request = Request::TransferReplicas { new_replicated_data: state.replicated_data.clone() , node_addr: self.info.addr, k: state.k };
                    self.send_request_no_response(node.addr, request).await?;
                }
                drop(state);

                Response::Ok
            }

            Request::Insert { key, value } => {
                match self.insert(key, value).await {
                    Ok(()) => Response::Ok,
                    Err(e) => Response::Error(e.to_string()),
                }
            }

            Request::InsertReplica { key, value, node_info, k_left } => {
                // IMPORTANT: don't hold the write-lock across an `.await` (can deadlock)
                // because the forwarded request may come back to this node and also need
                // the same lock.
                let successor_addr = {
                    let mut state = self.state.write().await;
                    state
                        .replicated_data
                        .insert(key.clone(), (value.clone(), k_left, node_info.clone()));
                    state.successor.addr
                };

                // Forward the replica to the successor if we still have replication hops left
                if k_left > 1 {
                    let request = Request::InsertReplica {
                        key,
                        value,
                        node_info,
                        k_left: k_left - 1,
                    };
                    self.send_request_no_response(successor_addr, request).await?;
                } else if k_left == 1 {
                    debug!(
                        "Inserted replica for key '{}' at final replica node {}",
                        key, self.info.addr
                    );
                }

                Response::Ok
            }

            Request::Query { key, source } => {
                let state = self.state.read().await;
                let k = state.k;
                let t = state.t;
                drop(state);

                if k <= 1 {
                    let key_hash = hash_value(&key);
                    
                    // If responsible node for the key is this node, retrieve it locally
                    if self.is_responsible_for_hash(key_hash).await {
                        let state = self.state.read().await;
                        let value = state.data.get(&key).cloned();
                        //let predecessor = state.predecessor.clone();
                        drop(state);
                        
                        Response::QueryResponse { source, value }
                    }
                    // Otherwise, forward to the appropriate node (successor or predecessor)
                    else {
                        let node_hash = self.info.id;
                        let forward_dist = if node_hash >= key_hash {(1 << N) - node_hash + key_hash } 
                        else {key_hash - node_hash };
                        let request = Request::Query { key, source };
                        let mut response = Response::Ok; // Placeholder response in case forwarding fails

                        // Forward to successor if it's closer, otherwise forward to predecessor
                        if forward_dist < (1 << (N - 1)) {
                            let successor = self.state.read().await.successor.clone();
                            response = self.send_request(successor.addr, request).await?;

                        } else {
                            let predecessor = self.state.read().await.predecessor.clone();
                            response = self.send_request(predecessor.addr, request).await?;
                        }

                        match response {
                            Response::QueryResponse { source: _, value } => {
                                Response::QueryResponse { source, value }
                            }
                            _ => {
                                debug!("Failed to get response from successor during query");
                                Response::QueryResponse { source, value: None }
                            }
                        }
                    }
                } else if t == 0 {
                    let key_hash = hash_value(&key);
                    
                    // If responsible node for the key is this node, retrieve it locally
                    if self.is_responsible_for_hash(key_hash).await {
                        let (successor_addr, k) = {
                            let state = self.state.read().await;
                            (state.successor.addr, state.k)
                        };

                        let request = Request::QueryLastReplica { key, k_left: k - 1 };
                        let response = self.send_request(successor_addr, request).await;

                        match response {
                            Ok(Response::QueryResponse { source: _, value }) => {
                                Response::QueryResponse { source , value }
                            }
                            _ => {
                                debug!("Failed to get response from successor during query");
                                Response::QueryResponse { source , value: None }
                            }
                        }
                    }
                    // Otherwise, forward to the appropriate node (successor or predecessor)
                    else {
                        let node_hash = self.info.id;
                        let forward_dist = if node_hash >= key_hash {(1 << N) - node_hash + key_hash } 
                        else {key_hash - node_hash };
                        let request = Request::Query { key, source };
                        let mut response = Response::Ok; // Placeholder response in case forwarding fails

                        // Forward to successor if it's closer, otherwise forward to predecessor
                        if forward_dist < (1 << (N - 1)) {
                            let successor = self.state.read().await.successor.clone();
                            response = self.send_request(successor.addr, request).await?;

                        } else {
                            let predecessor = self.state.read().await.predecessor.clone();
                            response = self.send_request(predecessor.addr, request).await?;
                        }

                        match response {
                            Response::QueryResponse { source: _, value } => {
                                Response::QueryResponse { source, value }
                            }
                            _ => {
                                debug!("Failed to get response from successor during query");
                                Response::QueryResponse { source, value: None }
                            }
                        }
                    }
                } else {

                    let key_hash = hash_value(&key);
                    let state = self.state.read().await;
                    
                    // If responsible node for the key is this node, retrieve it locally
                    if self.is_responsible_for_hash(key_hash).await {
                        let value = state.data.get(&key).cloned();
                        //let predecessor = state.predecessor.clone();
                        drop(state);
                        
                        Response::QueryResponse { source, value }
                    }
                    else if state.replicated_data.contains_key(&key) {
                        // If we have a replica for the key, return it
                        let (value, _, _) = state.replicated_data.get(&key).unwrap().clone();
                        Response::QueryResponse { source, value: Some(value) }
                    }
                    // Otherwise, forward to the appropriate node (successor or predecessor)
                    else {
                        let node_hash = self.info.id;
                        let forward_dist = if node_hash >= key_hash {(1 << N) - node_hash + key_hash } 
                        else {key_hash - node_hash };
                        let request = Request::Query { key, source };
                        let mut response = Response::Ok; // Placeholder response in case forwarding fails

                        // Forward to successor if it's closer, otherwise forward to predecessor
                        if forward_dist < (1 << (N - 1)) {
                            let successor = state.successor.clone();
                            response = self.send_request(successor.addr, request).await?;

                        } else {
                            let predecessor = state.predecessor.clone();
                            response = self.send_request(predecessor.addr, request).await?;
                        }

                        match response {
                            Response::QueryResponse { source: _, value } => {
                                Response::QueryResponse { source, value }
                            }
                            _ => {
                                debug!("Failed to get response from successor during query");
                                Response::QueryResponse { source, value: None }
                            }
                        }
                    }
                }
                
            }

            Request::QueryLastReplica { key, k_left } => {
                if k_left != 1 {
                    let request = Request::QueryLastReplica { key, k_left: k_left - 1 };
                    let successor = self.state.read().await.successor.clone();
                    let response = self.send_request(successor.addr, request).await?;

                    match response {
                        Response::QueryResponse { source: _, value } => {
                            debug!("Received response from successor during query: {:?}", value);
                            Response::QueryResponse { source: self.info.addr, value }
                        }
                        _ => {
                            debug!("Failed to get response from successor during query");
                            Response::QueryResponse { source: self.info.addr, value: None }
                        }
                    }
                } else {
                    // This is the last replica in the chain, so return the replicated value locally
                    let state = self.state.read().await;
                    let value = state
                        .replicated_data
                        .get(&key)
                        .map(|(v, _, _)| v.clone());
                    drop(state);

                    Response::QueryResponse { source: self.info.addr, value }
                }
            }

            Request::QueryAll { source, data } => {
                let state = self.state.read().await;
                let k = state.k;
                let t = state.t;
                drop(state);

                if k <= 1 || (k > 1 && t == 1) {

                    // Add own data to the response before forwarding
                    let state = self.state.read().await;
                    let own_data_clone = state.data.clone();
                    let node_hash = hash_value(&self.info.addr.to_string());
                    let mut current_data = data.clone();
                    current_data.push((node_hash, own_data_clone));
            
                    if source == state.successor.addr {
                        Response::QueryAll { source, data: current_data }
                    }
                    else {
                        // Forward the response back to the original requester
                        let request = Request::QueryAll { source, data: current_data };

                        // Forward to successor
                        let successor = state.successor.clone();
                        let response = self.send_request(successor.addr, request).await?;

                        match response {
                            Response::QueryAll { source: _, data } => {
                                Response::QueryAll { source, data }
                            }
                            _ => {
                                debug!("Failed to get response from successor during wildcard query");
                                Response::QueryAll { source, data: vec![] }
                            }
                        }
                    }
                } else if t == 0 {
                    // Add own data to the response before forwarding
                    let state = self.state.read().await;
                    let data_clone = state.replicated_data.clone();

                    let mut map = HashMap::new();
                    let mut node_hash = self.info.id;
                    for (k, (v, pos, node_info )) in data_clone {
                        if pos == 1 {
                            map.insert(k, v);
                            node_hash = node_info.id;
                        }
                    }

                    let mut current_data = data.clone();
                    current_data.push((node_hash, map));
                    current_data.push((self.info.id, HashMap::new()));

                    if source == state.successor.addr {
                        Response::QueryAll { source, data: current_data }
                    }
                    else {
                        // Forward the response back to the original requester
                        let request = Request::QueryAll { source, data: current_data };

                        // Forward to successor
                        let successor = state.successor.clone();
                        let response = self.send_request(successor.addr, request).await?;

                        match response {
                            Response::QueryAll { source: _, data } => {
                                Response::QueryAll { source, data }
                            }
                            _ => {
                                debug!("Failed to get response from successor during wildcard query");
                                Response::QueryAll { source, data: vec![] }
                            }
                        }
                    }
                } else {
                    Response::Ok
                }
            
            },

            Request::Delete { key } => {
                match self.delete(key).await {
                    Ok(()) => Response::Ok,
                    Err(e) => Response::Error(e.to_string()),
                }
            },

            Request::DeleteReplica { key, node_info, k_left } => {
                let mut state = self.state.write().await;
                state.replicated_data.remove(&key);

                // Forward the delete to the successor if we still have replication hops left
                if k_left > 1 {
                    let request = Request::DeleteReplica { key, node_info, k_left: k_left - 1 };
                    let successor = state.successor.clone();
                    self.send_request_no_response(successor.addr, request).await?;
                }

                Response::Ok
            },

            Request::TransferData { data } => {
                // Only accept keys that this node is responsible for
                let mut state = self.state.write().await;
                for (key, value) in data {
                    state.data.insert(key, value);
                }
                drop(state);
                info!("Received and stored transferred data");
                Response::Ok
            },

            Request::TransferReplicas { new_replicated_data, node_addr, k} => {
                // Move new replicated data into our state
                let mut state = self.state.write().await;
                state.replicated_data = new_replicated_data;
                info!("Received and stored transferred replicas");

                debug!("Value of k is {}", state.k);
                // Send message to successors to update their replicas
                let request = Request::UpdateReplicas { 
                    data: state.data.clone(), 
                    k_left: k-1,
                };
                drop(state);

                // Send the UpdateReplicas request to the successor
                //let successor = self.state.read().await.successor.clone();
                self.send_request_no_response(node_addr, request).await?;

                Response::Ok
            },

            Request::UpdateReplicas { data, mut k_left } => {
                let mut state = self.state.write().await;

                // new hashmap<String, (String, u64, NodeInfo)>
                let mut new_replicated_data: HashMap<String, (String, u64, NodeInfo)> = HashMap::new();

                let predecessor_info = state.predecessor.clone();
                let successor_addr = state.successor.addr;

                // print all replicated data before update
                debug!("Current replicated data before update:");
                for (key, (value, replication_pos, node_info)) in state.replicated_data.iter() {
                    debug!("Key: {}, Value: {}, Replication Pos: {}, Node Info: {:?}", key, value, replication_pos, node_info);
                }


                for (key, (value, replication_pos, node_info)) in state.replicated_data.iter() {
                    if data.get(key).is_some_and(|v| v == value) {
                        // If it exists with the same value, keep as-is
                        new_replicated_data.insert(key.clone(), (value.clone(), k_left, node_info.clone()));
                    } else {
                        // Otherwise decrement position and keep only if still > 0
                        if *replication_pos > 1 && *replication_pos <= k_left {
                            new_replicated_data.insert(
                                key.clone(),
                                (value.clone(), replication_pos - 1, node_info.clone()),
                            );
                        }
                        else {
                            new_replicated_data.insert(
                                key.clone(),
                                (value.clone(), *replication_pos, node_info.clone()),
                            );
                        }
                    }
                }

                // First hop: add incoming data keys as fresh replicas at position k_left
                if k_left == state.k - 1 {
                    for (k, v) in data.iter() {
                        new_replicated_data.insert(k.clone(), (v.clone(), k_left, predecessor_info.clone()));
                    }
                }

                state.replicated_data = new_replicated_data;

                // Forward once (not per-item)
                k_left = k_left - 1;
                if k_left > 0 {
                    let request = Request::UpdateReplicas {
                        data,
                        k_left,
                    };
                    drop(state);
                    self.send_request_no_response(successor_addr, request).await?;
                }

                Response::Ok
            }


            Request::UpdateReplicasOnDepart { data, k_left, startingNodeInfo, replicated_data } => {
                let mut state = self.state.write().await;
                let mut new_replicated_data: HashMap<String, (String, u64, NodeInfo)> = HashMap::new();
                let successor_addr = state.successor.addr;

                // First, process all keys in current replicated_data
                for (key, (value, replication_pos, node_info)) in state.replicated_data.iter() {
                    if *replication_pos > k_left {
                        // This key is no longer replicated at the departing node, so we need to add it to the new replicated data with position 1 (fresh replica)
                        new_replicated_data.insert(
                            key.clone(),
                            (value.clone(), *replication_pos, node_info.clone()),
                        );
                    }
                    else if *replication_pos <= k_left {
                        // This key is at the departing node's replication position, so we need to add it to the new replicated data with position 1 (fresh replica)
                        new_replicated_data.insert(
                            key.clone(),
                            (value.clone(), replication_pos + 1, node_info.clone()),
                        );
                    }
                }

                // Now, add any new keys from incoming data that are not already in replicated_data
                for (key, (value, replication_pos, node_info)) in replicated_data.iter() {
                    if *replication_pos == state.k - k_left {
                        new_replicated_data.insert(key.clone(), (value.clone(), 1, node_info.clone()));
                    }
                }

                
                if k_left == state.k - 1 {
                    for (key, value) in data.iter() {
                        // Remove from replicated data and add to data
                        new_replicated_data.remove(&key.clone());
                    }
                }

                if k_left > 0 {
                    state.replicated_data = new_replicated_data;

                    let request = Request::UpdateReplicasOnDepart {
                        data,
                        k_left: k_left - 1,
                        startingNodeInfo,
                        replicated_data: state.replicated_data.clone(),
                    };
                    drop(state);
                    self.send_request_no_response(successor_addr, request).await?;
                }
                else {
                    for (key, value) in data.iter() {
                        new_replicated_data.insert(key.clone(), (value.clone(), 1, startingNodeInfo.clone()));
                    }

                    state.replicated_data = new_replicated_data;
                }
                Response::Ok
            }


            _ => Response::Error("Unsupported request".to_string()),
        };
        response.to_bytes()

    }
}
