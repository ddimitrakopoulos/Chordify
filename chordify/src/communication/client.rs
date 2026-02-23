//! TCP Client for Chordify
//! 
//! Phase 2: Socket Setup
//! Async TCP client using Tokio for sending requests to other nodes

use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use tracing::{debug, error};

use crate::communication::protocol::{Message, MessagePayload, Request, Response, NodeInfo};
use crate::communication::node_id::NodeId;

/// Maximum message size (16 MB)
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Default request timeout
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// TCP Client for sending requests to other nodes
pub struct Client {
    source_addr: SocketAddr,
    timeout: Duration,
}

impl Client {
    pub fn new(source_addr: SocketAddr) -> Self {
        Self {
            source_addr,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    pub fn with_timeout(source_addr: SocketAddr, timeout: Duration) -> Self {
        Self {
            source_addr,
            timeout,
        }
    }

    /// Send a request and wait for response
    async fn send_request(&self, target: SocketAddr, request: Request) -> anyhow::Result<Response> {
        let message = Message::new(self.source_addr, MessagePayload::Request(request));
        let response = self.send_message(target, message).await?;

        match response.payload {
            MessagePayload::Response(resp) => Ok(resp),
            MessagePayload::Request(_) => Err(anyhow::anyhow!("Unexpected request in response")),
        }
    }

    /// Send a message and receive response
    async fn send_message(&self, target: SocketAddr, message: Message) -> anyhow::Result<Message> {
        // Connect with timeout
        let mut stream = timeout(self.timeout, TcpStream::connect(target))
            .await
            .map_err(|_| anyhow::anyhow!("Connection timeout"))??;

        // Serialize message
        let msg_bytes = message.to_bytes()?;
        let len_bytes = (msg_bytes.len() as u32).to_be_bytes();

        // Send length prefix + message
        stream.write_all(&len_bytes).await?;
        stream.write_all(&msg_bytes).await?;
        stream.flush().await?;

        debug!("Sent message to {}: {:?}", target, message.payload);

        // Read response length prefix
        let mut len_buf = [0u8; 4];
        timeout(self.timeout, stream.read_exact(&mut len_buf))
            .await
            .map_err(|_| anyhow::anyhow!("Read timeout"))??;

        let resp_len = u32::from_be_bytes(len_buf) as usize;
        if resp_len > MAX_MESSAGE_SIZE {
            return Err(anyhow::anyhow!("Response too large: {} bytes", resp_len));
        }

        // Read response body
        let mut resp_buf = vec![0u8; resp_len];
        timeout(self.timeout, stream.read_exact(&mut resp_buf))
            .await
            .map_err(|_| anyhow::anyhow!("Read timeout"))??;

        // Parse response
        let response = Message::from_bytes(&resp_buf)?;
        debug!("Received response from {}: {:?}", target, response.payload);

        Ok(response)
    }

    // ========== Convenience Methods ==========

    /// Ping a node to check connectivity
    pub async fn ping(&self, target: SocketAddr) -> anyhow::Result<bool> {
        match self.send_request(target, Request::Ping).await? {
            Response::Pong => Ok(true),
            _ => Ok(false),
        }
    }

    /// Get the ID of a node
    pub async fn get_id(&self, target: SocketAddr) -> anyhow::Result<NodeId> {
        match self.send_request(target, Request::GetId).await? {
            Response::Id(id) => Ok(id),
            Response::Error { message } => Err(anyhow::anyhow!(message)),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    /// Get the successor of a node
    pub async fn get_successor(&self, target: SocketAddr) -> anyhow::Result<Option<NodeInfo>> {
        match self.send_request(target, Request::GetSuccessor).await? {
            Response::NodeInfo(info) => Ok(info),
            Response::Error { message } => Err(anyhow::anyhow!(message)),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    /// Get the predecessor of a node
    pub async fn get_predecessor(&self, target: SocketAddr) -> anyhow::Result<Option<NodeInfo>> {
        match self.send_request(target, Request::GetPredecessor).await? {
            Response::NodeInfo(info) => Ok(info),
            Response::Error { message } => Err(anyhow::anyhow!(message)),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    /// Find the successor of a given ID
    pub async fn find_successor(&self, target: SocketAddr, id: NodeId) -> anyhow::Result<NodeInfo> {
        match self.send_request(target, Request::FindSuccessor { id }).await? {
            Response::FoundSuccessor(info) => Ok(info),
            Response::Error { message } => Err(anyhow::anyhow!(message)),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    /// Notify a node that we think we're its predecessor
    pub async fn notify(&self, target: SocketAddr, node_info: NodeInfo) -> anyhow::Result<()> {
        match self.send_request(target, Request::Notify { node_info }).await? {
            Response::Ok => Ok(()),
            Response::Error { message } => Err(anyhow::anyhow!(message)),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    /// Join the ring via a bootstrap node
    pub async fn join(&self, bootstrap: SocketAddr, node_info: NodeInfo) -> anyhow::Result<(NodeInfo, Option<NodeInfo>)> {
        match self.send_request(bootstrap, Request::Join { node_info }).await? {
            Response::JoinAck { successor, predecessor } => Ok((successor, predecessor)),
            Response::Error { message } => Err(anyhow::anyhow!(message)),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    /// Insert a key-value pair
    pub async fn insert(&self, target: SocketAddr, key: String, value: String) -> anyhow::Result<bool> {
        match self.send_request(target, Request::Insert { key, value }).await? {
            Response::InsertAck { success } => Ok(success),
            Response::Error { message } => Err(anyhow::anyhow!(message)),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    /// Query for a key
    pub async fn query(&self, target: SocketAddr, key: String) -> anyhow::Result<Option<String>> {
        match self.send_request(target, Request::Query { key }).await? {
            Response::QueryResult { value, .. } => Ok(value),
            Response::Error { message } => Err(anyhow::anyhow!(message)),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    /// Delete a key
    pub async fn delete(&self, target: SocketAddr, key: String) -> anyhow::Result<bool> {
        match self.send_request(target, Request::Delete { key }).await? {
            Response::DeleteAck { success } => Ok(success),
            Response::Error { message } => Err(anyhow::anyhow!(message)),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    /// Get the ring overlay/topology
    pub async fn get_overlay(&self, target: SocketAddr) -> anyhow::Result<Vec<NodeInfo>> {
        match self.send_request(target, Request::GetOverlay).await? {
            Response::Overlay { nodes } => Ok(nodes),
            Response::Error { message } => Err(anyhow::anyhow!(message)),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }
}
