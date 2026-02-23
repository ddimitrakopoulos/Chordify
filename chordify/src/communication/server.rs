//! TCP Server for Chordify
//! 
//! Phase 2: Socket Setup
//! Async TCP server using Tokio for concurrent request handling

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tracing::{info, error, debug};

use crate::communication::protocol::{Message, MessagePayload, Request, Response, NodeInfo};
use crate::communication::node_id::NodeId;

/// Maximum message size (16 MB)
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Shared node state for the server
pub struct NodeState {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub successor: Option<NodeInfo>,
    pub predecessor: Option<NodeInfo>,
    pub storage: std::collections::HashMap<String, String>,
}

impl NodeState {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            id: NodeId::from_address(&addr),
            addr,
            successor: None,
            predecessor: None,
            storage: std::collections::HashMap::new(),
        }
    }

    pub fn info(&self) -> NodeInfo {
        NodeInfo {
            id: self.id,
            addr: self.addr,
        }
    }
}

/// TCP Server for handling incoming connections
pub struct Server {
    addr: SocketAddr,
    state: Arc<RwLock<NodeState>>,
}

impl Server {
    pub fn new(addr: SocketAddr) -> Self {
        let state = Arc::new(RwLock::new(NodeState::new(addr)));
        Self { addr, state }
    }

    pub fn state(&self) -> Arc<RwLock<NodeState>> {
        Arc::clone(&self.state)
    }

    /// Start the server and listen for connections
    pub async fn start(&self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        info!("Server listening on {}", self.addr);

        loop {
            match listener.accept().await {
                Ok((socket, peer)) => {
                    debug!("Accepted connection from {}", peer);
                    let state = Arc::clone(&self.state);
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(socket, peer, state).await {
                            error!("Error handling connection from {}: {}", peer, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}

/// Handle a single connection
async fn handle_connection(
    mut socket: TcpStream,
    peer: SocketAddr,
    state: Arc<RwLock<NodeState>>,
) -> anyhow::Result<()> {
    loop {
        // Read length prefix (4 bytes, big-endian u32)
        let mut len_buf = [0u8; 4];
        match socket.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("Connection closed by {}", peer);
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }

        let msg_len = u32::from_be_bytes(len_buf) as usize;
        if msg_len > MAX_MESSAGE_SIZE {
            error!("Message too large: {} bytes", msg_len);
            return Err(anyhow::anyhow!("Message too large"));
        }

        // Read message body
        let mut msg_buf = vec![0u8; msg_len];
        socket.read_exact(&mut msg_buf).await?;

        // Parse message
        let message = Message::from_bytes(&msg_buf)?;
        debug!("Received message from {}: {:?}", peer, message.payload);

        // Handle request and generate response
        let response = match message.payload {
            MessagePayload::Request(req) => {
                let resp = handle_request(req, &state).await;
                let our_addr = state.read().await.addr;
                Message::response(message.id, our_addr, MessagePayload::Response(resp))
            }
            MessagePayload::Response(_) => {
                // Unexpected response, ignore
                continue;
            }
        };

        // Send response with length prefix
        let resp_bytes = response.to_bytes()?;
        let len_bytes = (resp_bytes.len() as u32).to_be_bytes();
        socket.write_all(&len_bytes).await?;
        socket.write_all(&resp_bytes).await?;
        socket.flush().await?;
    }
}

/// Handle a single request
async fn handle_request(request: Request, state: &Arc<RwLock<NodeState>>) -> Response {
    match request {
        Request::Ping => Response::Pong,

        Request::GetId => {
            let state = state.read().await;
            Response::Id(state.id)
        }

        Request::GetSuccessor => {
            let state = state.read().await;
            Response::NodeInfo(state.successor.clone())
        }

        Request::GetPredecessor => {
            let state = state.read().await;
            Response::NodeInfo(state.predecessor.clone())
        }

        Request::FindSuccessor { id } => {
            let state = state.read().await;
            // Simple case: return our successor if we're responsible
            // TODO: Implement proper routing logic
            if let Some(ref successor) = state.successor {
                Response::FoundSuccessor(successor.clone())
            } else {
                // Single node ring - we are our own successor
                Response::FoundSuccessor(state.info())
            }
        }

        Request::Notify { node_info } => {
            let mut state = state.write().await;
            // Update predecessor if needed
            match &state.predecessor {
                None => {
                    state.predecessor = Some(node_info);
                }
                Some(pred) => {
                    // Check if notifying node should be our new predecessor
                    if node_info.id.is_between(&pred.id, &state.id) {
                        state.predecessor = Some(node_info);
                    }
                }
            }
            Response::Ok
        }

        Request::Join { node_info } => {
            let state = state.read().await;
            // Return our successor and predecessor for the joining node
            Response::JoinAck {
                successor: state.successor.clone().unwrap_or_else(|| state.info()),
                predecessor: state.predecessor.clone(),
            }
        }

        Request::Depart { node_info } => {
            // TODO: Handle key transfer and pointer updates
            Response::Ok
        }

        Request::Insert { key, value } => {
            let mut state = state.write().await;
            // TODO: Route to responsible node
            // For now, store locally
            state.storage.insert(key, value);
            Response::InsertAck { success: true }
        }

        Request::Query { key } => {
            let state = state.read().await;
            if key == "*" {
                // Return all keys (simplified - should aggregate from all nodes)
                let all_keys: Vec<String> = state.storage.keys().cloned().collect();
                // For wildcard, return first key-value or None
                let first = state.storage.iter().next();
                if let Some((k, v)) = first {
                    Response::QueryResult { key: k.clone(), value: Some(v.clone()) }
                } else {
                    Response::QueryResult { key, value: None }
                }
            } else {
                let value = state.storage.get(&key).cloned();
                Response::QueryResult { key, value }
            }
        }

        Request::Delete { key } => {
            let mut state = state.write().await;
            let existed = state.storage.remove(&key).is_some();
            Response::DeleteAck { success: existed }
        }

        Request::GetOverlay => {
            let state = state.read().await;
            // Return known nodes (self, successor, predecessor)
            let mut nodes = vec![state.info()];
            if let Some(ref succ) = state.successor {
                if succ.id != state.id {
                    nodes.push(succ.clone());
                }
            }
            if let Some(ref pred) = state.predecessor {
                if pred.id != state.id && !nodes.iter().any(|n| n.id == pred.id) {
                    nodes.push(pred.clone());
                }
            }
            Response::Overlay { nodes }
        }
    }
}
