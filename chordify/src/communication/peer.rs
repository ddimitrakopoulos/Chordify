//! P2P Peer Communication
//!
//! This module implements a general-purpose peer-to-peer communication layer.
//! Peers are identified by their IP address and port (SocketAddr).
//! All nodes are equal; there is no client/server distinction.
//!
//! # Primitives
//! - `Peer`: Represents this node, listens for incoming connections.
//! - `Connection`: A bidirectional connection to another peer.
//! - `connect`: Establish a connection to a remote peer.
//! - `send`: Send a message (raw bytes) to a connected peer.
//! - `receive`: Receive a message (raw bytes) from a connected peer.
//! - `answer`: Send a response message back to the sender.
//!
//! # Message Framing
//! Messages are length-prefixed (4 bytes, big-endian u32) followed by raw bytes.
//! Serialization/deserialization is the caller's responsibility.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tracing::{info, error, debug};

/// Maximum message size (16 MB)
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Default connection timeout
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Represents this node as a peer in the P2P network.
/// Listens for incoming connections and manages local state.
pub struct Peer {
    /// This peer's address (IP:port)
    addr: SocketAddr,
    /// Shared state for custom data (optional, for higher layers)
    state: Arc<RwLock<PeerState>>,
}

/// Shared state for a peer (extensible for higher layers)
pub struct PeerState {
    pub addr: SocketAddr,
    // Higher layers can add more fields as needed
}

impl Peer {
    /// Create a new peer bound to the given address.
    pub fn new(addr: SocketAddr) -> Self {
        let state = Arc::new(RwLock::new(PeerState { addr }));
        Self { addr, state }
    }

    /// Get this peer's address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Get a clone of the shared state for external use.
    pub fn state(&self) -> Arc<RwLock<PeerState>> {
        Arc::clone(&self.state)
    }

    /// Start listening for incoming connections.
    /// For each incoming connection, calls the provided handler with a Connection.
    pub async fn listen<F, Fut>(&self, handler: F) -> anyhow::Result<()>
    where
        F: Fn(Connection) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind(self.addr).await?;
        info!("Peer listening on {}", self.addr);

        loop {
            match listener.accept().await {
                Ok((socket, peer_addr)) => {
                    debug!("Accepted connection from {}", peer_addr);
                    let conn = Connection::from_stream(socket, peer_addr);
                    tokio::spawn(handler(conn));
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}

/// A bidirectional connection to another peer.
/// Provides send, receive, and answer primitives.
pub struct Connection {
    stream: TcpStream,
    peer_addr: SocketAddr,
}

impl Connection {
    /// Create a Connection from an existing TcpStream.
    pub fn from_stream(stream: TcpStream, peer_addr: SocketAddr) -> Self {
        Self { stream, peer_addr }
    }

    /// Connect to a remote peer by address.
    pub async fn connect(addr: SocketAddr) -> anyhow::Result<Self> {
        Self::connect_with_timeout(addr, DEFAULT_TIMEOUT).await
    }

    /// Connect to a remote peer with a custom timeout.
    pub async fn connect_with_timeout(addr: SocketAddr, timeout: Duration) -> anyhow::Result<Self> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| anyhow::anyhow!("Connection timeout"))??;
        Ok(Self { stream, peer_addr: addr })
    }

    /// Get the remote peer's address.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Send a message (raw bytes) to the connected peer.
    /// Message is length-prefixed (4 bytes, big-endian).
    pub async fn send(&mut self, data: &[u8]) -> anyhow::Result<()> {
        let len = data.len() as u32;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(data).await?;
        self.stream.flush().await?;
        debug!("Sent {} bytes to {}", data.len(), self.peer_addr);
        Ok(())
    }

    /// Receive a message (raw bytes) from the connected peer.
    /// Returns None if the connection is closed.
    pub async fn receive(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        let mut len_buf = [0u8; 4];
        match self.stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("Connection closed by {}", self.peer_addr);
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        }
        let msg_len = u32::from_be_bytes(len_buf) as usize;
        if msg_len > MAX_MESSAGE_SIZE {
            return Err(anyhow::anyhow!("Message too large: {} bytes", msg_len));
        }
        let mut buf = vec![0u8; msg_len];
        self.stream.read_exact(&mut buf).await?;
        debug!("Received {} bytes from {}", msg_len, self.peer_addr);
        Ok(Some(buf))
    }

    /// Answer (send a response) to the connected peer.
    /// This is semantically the same as send, but named for clarity.
    pub async fn answer(&mut self, data: &[u8]) -> anyhow::Result<()> {
        self.send(data).await
    }

    /// Close the connection gracefully.
    pub async fn close(mut self) -> anyhow::Result<()> {
        self.stream.shutdown().await?;
        debug!("Closed connection to {}", self.peer_addr);
        Ok(())
    }
}
