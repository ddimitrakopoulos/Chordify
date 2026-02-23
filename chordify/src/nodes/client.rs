//! TCP Client for Chordify
//!
//! Phase 2: Socket Setup
//! Async TCP client using Tokio for sending requests to other nodes
//!
//! This module implements the client-side communication for Chordify nodes.
//! Each node communicates via its IP address and port (SocketAddr).
//! All protocol operations are performed over TCP using async methods.

use std::net::SocketAddr; // Used for node identity and communication
use std::time::Duration; // Used for request timeouts
use tokio::net::TcpStream; // Tokio's async TCP stream
use tokio::io::{AsyncReadExt, AsyncWriteExt}; // Async read/write traits
use tokio::time::timeout; // Timeout utility for async operations
use tracing::{debug, error}; // Logging macros

use crate::communication::protocol::{Message, MessagePayload, Request, Response, NodeInfo}; // Protocol types
use crate::nodes::NodeId; // NodeId type

/// Maximum message size (16 MB)
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // Prevents oversized messages

/// Default request timeout
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5); // 5 seconds timeout for requests

/// TCP Client for sending requests to other nodes
/// Node communication is always via ip address and port (SocketAddr)
pub struct Client {
    source_addr: SocketAddr, // The client's own address (ip:port)
    timeout: Duration,       // Timeout for requests
}

impl Client {
    /// Create a new client with default timeout
    pub fn new(source_addr: SocketAddr) -> Self {
        Self {
            source_addr, // Store the client's address
            timeout: DEFAULT_TIMEOUT, // Use default timeout
        }
    }

    /// Create a new client with custom timeout
    pub fn with_timeout(source_addr: SocketAddr, timeout: Duration) -> Self {
        Self {
            source_addr, // Store the client's address
            timeout,     // Use custom timeout
        }
    }

    /// Send a request and wait for response
    /// - target: address of the node to contact
    /// - request: protocol request to send
    /// Returns: protocol response
    async fn send_request(&self, target: SocketAddr, request: Request) -> anyhow::Result<Response> {
        // Wrap request in a Message, including sender address
        let message = Message::new(self.source_addr, MessagePayload::Request(request));
        // Send the message and get the response
        let response = self.send_message(target, message).await?;

        // Match the response payload
        match response.payload {
            MessagePayload::Response(resp) => Ok(resp), // Expected response
            MessagePayload::Request(_) => Err(anyhow::anyhow!("Unexpected request in response")), // Error if wrong type
        }
    }

    /// Send a message and receive response
    /// Handles TCP connection, serialization, framing, and parsing
    async fn send_message(&self, target: SocketAddr, message: Message) -> anyhow::Result<Message> {
        // Connect to target node with timeout
        let mut stream = timeout(self.timeout, TcpStream::connect(target))
            .await
            .map_err(|_| anyhow::anyhow!("Connection timeout"))??;

        // Serialize message to bytes
        let msg_bytes = message.to_bytes()?;
        // Prefix message with its length (u32, big-endian)
        let len_bytes = (msg_bytes.len() as u32).to_be_bytes();

        // Send length prefix
        stream.write_all(&len_bytes).await?;
        // Send message bytes
        stream.write_all(&msg_bytes).await?;
        // Flush stream to ensure all data is sent
        stream.flush().await?;

        debug!("Sent message to {}: {:?}", target, message.payload);

        // Read response length prefix (4 bytes)
        let mut len_buf = [0u8; 4];
        timeout(self.timeout, stream.read_exact(&mut len_buf))
            .await
            .map_err(|_| anyhow::anyhow!("Read timeout"))??;

        let resp_len = u32::from_be_bytes(len_buf) as usize; // Parse length
        if resp_len > MAX_MESSAGE_SIZE {
            return Err(anyhow::anyhow!("Response too large: {} bytes", resp_len));
        }

        // Read response body
        let mut resp_buf = vec![0u8; resp_len];
        timeout(self.timeout, stream.read_exact(&mut resp_buf))
            .await
            .map_err(|_| anyhow::anyhow!("Read timeout"))??;

        // Parse response from bytes
        let response = Message::from_bytes(&resp_buf)?;
        debug!("Received response from {}: {:?}", target, response.payload);

        Ok(response)
    }

    // ========== Convenience Methods ===========

    /// Ping a node to check connectivity
    /// Returns true if Pong received
    pub async fn ping(&self, target: SocketAddr) -> anyhow::Result<bool> {
        match self.send_request(target, Request::Ping).await? {
            Response::Pong => Ok(true), // Success
            _ => Ok(false),             // Any other response
        }
    }

    /// Get the ID of a node
    pub async fn get_id(&self, target: SocketAddr) -> anyhow::Result<NodeId> {
        match self.send_request(target, Request::GetId).await? {
            Response::Id(id) => Ok(id), // Success
            Response::Error { message } => Err(anyhow::anyhow!(message)), // Error response
            _ => Err(anyhow::anyhow!("Unexpected response")), // Wrong type
        }
    }

    /// Get the successor of a node
    pub async fn get_successor(&self, target: SocketAddr) -> anyhow::Result<Option<NodeInfo>> {
        match self.send_request(target, Request::GetSuccessor).await? {
            Response::NodeInfo(info) => Ok(info), // Success
            Response::Error { message } => Err(anyhow::anyhow!(message)), // Error response
            _ => Err(anyhow::anyhow!("Unexpected response")), // Wrong type
        }
    }

    /// Get the predecessor of a node
    pub async fn get_predecessor(&self, target: SocketAddr) -> anyhow::Result<Option<NodeInfo>> {
        match self.send_request(target, Request::GetPredecessor).await? {
            Response::NodeInfo(info) => Ok(info), // Success
            Response::Error { message } => Err(anyhow::anyhow!(message)), // Error response
            _ => Err(anyhow::anyhow!("Unexpected response")), // Wrong type
        }
    }

    /// Find the successor of a given ID
    pub async fn find_successor(&self, target: SocketAddr, id: NodeId) -> anyhow::Result<NodeInfo> {
        match self.send_request(target, Request::FindSuccessor { id }).await? {
            Response::FoundSuccessor(info) => Ok(info), // Success
            Response::Error { message } => Err(anyhow::anyhow!(message)), // Error response
            _ => Err(anyhow::anyhow!("Unexpected response")), // Wrong type
        }
    }

    /// Notify a node that we think we're its predecessor
    pub async fn notify(&self, target: SocketAddr, node_info: NodeInfo) -> anyhow::Result<()> {
        match self.send_request(target, Request::Notify { node_info }).await? {
            Response::Ok => Ok(()), // Success
            Response::Error { message } => Err(anyhow::anyhow!(message)), // Error response
            _ => Err(anyhow::anyhow!("Unexpected response")), // Wrong type
        }
    }

    /// Join the ring via a bootstrap node
    pub async fn join(&self, bootstrap: SocketAddr, node_info: NodeInfo) -> anyhow::Result<(NodeInfo, Option<NodeInfo>)> {
        match self.send_request(bootstrap, Request::Join { node_info }).await? {
            Response::JoinAck { successor, predecessor } => Ok((successor, predecessor)), // Success
            Response::Error { message } => Err(anyhow::anyhow!(message)), // Error response
            _ => Err(anyhow::anyhow!("Unexpected response")), // Wrong type
        }
    }

    /// Insert a key-value pair
    pub async fn insert(&self, target: SocketAddr, key: String, value: String) -> anyhow::Result<bool> {
        match self.send_request(target, Request::Insert { key, value }).await? {
            Response::InsertAck { success } => Ok(success), // Success
            Response::Error { message } => Err(anyhow::anyhow!(message)), // Error response
            _ => Err(anyhow::anyhow!("Unexpected response")), // Wrong type
        }
    }

    /// Query for a key
    pub async fn query(&self, target: SocketAddr, key: String) -> anyhow::Result<Option<String>> {
        match self.send_request(target, Request::Query { key }).await? {
            Response::QueryResult { value, .. } => Ok(value), // Success
            Response::Error { message } => Err(anyhow::anyhow!(message)), // Error response
            _ => Err(anyhow::anyhow!("Unexpected response")), // Wrong type
        }
    }

    /// Delete a key
    pub async fn delete(&self, target: SocketAddr, key: String) -> anyhow::Result<bool> {
        match self.send_request(target, Request::Delete { key }).await? {
            Response::DeleteAck { success } => Ok(success), // Success
            Response::Error { message } => Err(anyhow::anyhow!(message)), // Error response
            _ => Err(anyhow::anyhow!("Unexpected response")), // Wrong type
        }
    }

    /// Get the ring overlay/topology
    pub async fn get_overlay(&self, target: SocketAddr) -> anyhow::Result<Vec<NodeInfo>> {
        match self.send_request(target, Request::GetOverlay).await? {
            Response::Overlay { nodes } => Ok(nodes), // Success
            Response::Error { message } => Err(anyhow::anyhow!(message)), // Error response
            _ => Err(anyhow::anyhow!("Unexpected response")), // Wrong type
        }
    }
}
