//! Bootstrap Node - The stable entry point for the Chord ring
//!
//! The bootstrap node is always connected, known to all nodes, and handles all join requests.
//! It is the first node to enter the system and never departs.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug, warn};

use crate::tcp::{Server, connect};
use super::protocol::{Request, Response};
use super::node::{NodeInfo};

use sha1::{Sha1, Digest};
const N: u64 = 10; // Number of bits in the identifier space (SHA-1 hash size)

pub struct BootstrapNode {
    /// IP:Port of bootstrap
    addr: SocketAddr,
    /// Track all nodes in the ring (for coordination)
    ring_members: Arc<RwLock<Vec<NodeInfo>>>,
    /// Replication factor
    k: u64,
    /// Replication type
    t: u8,
}

impl Clone for BootstrapNode {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr,
            ring_members: Arc::clone(&self.ring_members),
            k: self.k,
            t: self.t,
        }
    }
}

// Helper function that takes an IP:port address and returns its SHA-1 hash as a BigUint (for node ID and key hashing)
pub fn hash_string_to_u64(data: &str) -> u64 {
    let mut hasher = Sha1::new();
    
    // We pass the data string that we the break to bytes.
    hasher.update(data.as_bytes());
    
    // Get the hash result as a byte array (20 bytes for SHA-1)
    let result = hasher.finalize();
    
    // Convert the byte array to a u64
    let hash_value = u64::from_be_bytes(result[0..8].try_into().unwrap());
    hash_value % (1 << N) // Assuming an N-bit identifier space
}

impl BootstrapNode {
    /// Create a new bootstrap node at the given address.
    /// This is the first node in the ring.
    pub fn new(addr: SocketAddr, k: u64, t: u8) -> Self {
        Self {
            addr,
            ring_members: Arc::new(RwLock::new(Vec::new())),
            k,
            t,
        }
    }

    /// Get the bootstrap's address
    pub fn get_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Get all ring members (bootstrap-specific)
    pub async fn get_ring_members(&self) -> Vec<NodeInfo> {
        self.ring_members.read().await.clone()
    }

    /// Start listening and handling requests
    pub async fn run(&self) -> anyhow::Result<()> {
        let server = Server::bind(self.addr).await?;
        // wrap `self` in an Arc so the handler can keep a reference long-term
        let bootstrap = Arc::new(self.clone());

        server.listen(move |request_bytes, from| {
            let bootstrap = Arc::clone(&bootstrap);
            async move { bootstrap.handle_bootstrap_request(request_bytes, from).await }
        }).await
    }


    /// Handle an incoming request (bootstrap-aware version)
    pub async fn handle_bootstrap_request(
        &self,
        request_bytes: Vec<u8>,
        from: SocketAddr,
    ) -> anyhow::Result<Vec<u8>> {
        let request = Request::from_bytes(&request_bytes)?;
        debug!("Bootstrap received {:?} from {}", request, from);
        
        let response = match request {
            Request::Ping => Response::Pong,
            
            Request::JoinRequest { joining_node } => {
                // Bootstrap-coordinated join
                match self.coordinate_join(joining_node.addr).await {
                    Ok((successor, predecessor)) => {
                        Response::JoinSuccess { successor, predecessor, k: self.k, t: self.t } 
                    },
                    Err(e) => {
                        warn!("Failed to coordinate join: {}", e);
                        Response::Error(format!("Join failed: {}", e))
                    }
                }

            }
            
            Request::DepartRequest { departing_node } => {
                // Bootstrap-coordinated depart
                info!("Bootstrap: Processing DepartRequest from {}", departing_node.addr);
                
                match self.coordinate_depart(departing_node.addr).await {
                    Ok(_) => Response::DepartSuccess,
                    Err(e) => {
                        warn!("Failed to coordinate depart: {}", e);
                        Response::Error(format!("Depart failed: {}", e))
                    }
                }
            }
            
            // default case for unrecognized requests
            _ => {
                warn!("Bootstrap received unrecognized request: {:?}", request);
                Response::Error("Unrecognized request".to_string())
            }
        };
        
        response.to_bytes()
    }

    /// Register a new node in the ring (called when a node joins) sorted by ID for easier successor/predecessor lookups
    pub async fn register_node(&self, node_info: NodeInfo) {
        let mut members = self.ring_members.write().await;
        if !members.iter().any(|n| n.addr == node_info.addr) {
            members.push(node_info.clone());
            members.sort_by_key(|n| hash_string_to_u64(&n.addr.to_string()));
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
    pub async fn coordinate_join(&self, joining_addr: SocketAddr) -> anyhow::Result<(NodeInfo, NodeInfo)> {
        info!("Bootstrap coordinating join for {}", joining_addr);
        let joining_node = NodeInfo { addr: joining_addr, id: hash_string_to_u64(&joining_addr.to_string()) };

        let mut successor = NodeInfo { addr: SocketAddr::new("0.0.0.0".parse().unwrap(), 0), id: 0 };
        let mut predecessor = NodeInfo { addr: SocketAddr::new("0.0.0.0".parse().unwrap(), 0), id: 0 };

        let id = hash_string_to_u64(&joining_addr.to_string());
        if self.ring_members.read().await.len() == 0 {
            // If no members, the joining node becomes the only member
            successor = joining_node.clone();
            predecessor = joining_node.clone();
        }
        else {
            // Traverse the ring members to find the correct successor and predecessor
            let members = self.ring_members.read().await;
            

            for member in members.iter() {
                let member_id = &member.id;
                if *member_id >= id {
                    successor = member.clone();
                    break;
                }
                predecessor = member.clone();
            }

            // If we reached the end of the list, the successor is the first member (ring wrap-around)
            if successor == (NodeInfo { addr: SocketAddr::new("0.0.0.0".parse().unwrap(), 0), id: 0 }) {
                successor = members.first().cloned().expect("Ring members should not be empty here");
            }

            // If predecessor is still None, it means the predecessor is the last member in the list (ring wrap-around)
            if predecessor == (NodeInfo { addr: SocketAddr::new("0.0.0.0".parse().unwrap(), 0), id: 0 }) {
                predecessor = members.last().cloned().expect("Ring members should not be empty here");
            }
        }

        self.register_node(joining_node.clone()).await;
        info!("Join coordination: {} with successor {} and predecessor {}", joining_addr, successor.addr, predecessor.addr);

        // Notify the successor and predecessor about the new node (if they exist and are not the joining node itself)
        if successor.addr != joining_addr {
            match self.send_request(
                successor.addr,
                Request::SetPredecessorWithKeys { node: joining_node.clone() }
            ).await {
                Ok(_) => info!("Notified successor {} about joining node {}", successor.addr, joining_addr),
                Err(e) => warn!("Failed to notify successor: {}", e),
            }
        }


        if predecessor.addr != joining_addr {
            match self.send_request(
                predecessor.addr,
                Request::SetSuccessor { node: joining_node.clone() }
            ).await {
                Ok(_) => info!("Notified predecessor {} about joining node {}", predecessor.addr, joining_addr),
                Err(e) => warn!("Failed to notify predecessor: {}", e),
            }
        }
        

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
        
        // Find the departing node's successor and predecessor from the ring members
        let mut members = self.ring_members.write().await;
        let len = members.len();
        let mut successor = NodeInfo { addr: SocketAddr::new("0.0.0.0".parse().unwrap(), 0), id: 0 };
        let mut predecessor = NodeInfo { addr: SocketAddr::new("0.0.0.0".parse().unwrap(), 0), id: 0 };
        for (i, member) in members.iter().enumerate() {
            if member.addr == departing_addr {
                // Successor is the next element, wrapping around to the first element if at the end
                successor = members[(i + 1) % len].clone();
                // Predecessor is the previous element, wrapping around to the last element if at the start
                predecessor = members[(i + len - 1) % len].clone();

                // Remove the departing node
                members.remove(i);
                break;
            }
        }
        
        // Inform the successor and predecessor of departing node to change their pointers
        if successor.addr != departing_addr {
            match self.send_request(
                successor.addr,
                Request::SetPredecessor { node: predecessor.clone() }
            ).await {
                Ok(_) => info!("Notified successor {} about departing node {}", successor.addr, departing_addr),
                Err(e) => warn!("Failed to notify successor: {}", e),
            }
        }

        if predecessor.addr != departing_addr {
            match self.send_request(
                predecessor.addr,
                Request::SetSuccessor { node: successor.clone() }
            ).await {
                Ok(_) => info!("Notified predecessor {} about departing node {}", predecessor.addr, departing_addr),
                Err(e) => warn!("Failed to notify predecessor: {}", e),
            }
        }
        
        info!("Depart coordination complete for {}", departing_addr);
        Ok(())
    }
}

