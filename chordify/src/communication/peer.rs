//! P2P Peer Communication
//!
//! A general-purpose peer-to-peer communication layer.
//! Peers are identified only by their IP address and port (SocketAddr).
//! All nodes are equal; there is no client/server distinction.
//!
//! # Architecture: Connect → Message → Response
//!
//! ```text
//! Peer A                          Peer B
//!   |                               |
//!   |-------- connect() ----------->|  (establish connection)
//!   |                               |
//!   |-------- message() ----------->|  (send request bytes)
//!   |                               |
//!   |<------- response() -----------|  (receive response bytes)
//!   |                               |
//! ```
//!
//! # Usage
//!
//! **Listening for connections (responder side):**
//! ```rust,no_run
//! # tokio::main
//! async fn main() -> anyhow::Result<()> {
//!     let peer = Peer::bind("127.0.0.1:8000".parse().unwrap()).await?;
//!     peer.listen(|request, from| async move {
//!         // process request bytes, return response bytes
//!         Ok(request) // echo example
//!     }).await?;
//!     Ok(())
//! }
//! ```
//!
//! **Sending a message and getting response (initiator side):**
//! ```rust,no_run
//! # tokio::main
//! async fn main() -> anyhow::Result<()> {
//!     let response = connect("127.0.0.1:8000".parse().unwrap())
//!         .await?
//!         .message(b"hello")
//!         .await?;
//!     Ok(())
//! }
//! ```
//!
//! # Message Framing
//! Messages are length-prefixed (4 bytes, big-endian u32) followed by raw bytes.
//! Serialization/deserialization is the caller's responsibility.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, error, debug};

/// Maximum message size (16 MB)
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Default connection timeout
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

// ============================================================================
// Peer - Listens for incoming connections and handles requests
// ============================================================================

/// A peer in the P2P network that listens for incoming connections.
pub struct Peer {
    listener: TcpListener,
    addr: SocketAddr,
}

impl Peer {
    /// Bind to an address and create a new peer ready to accept connections.
    pub async fn bind(addr: SocketAddr) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        info!("Peer bound to {}", local_addr);
        Ok(Self { listener, addr: local_addr })
    }

    /// Get this peer's bound address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Listen for incoming connections and handle each request.
    /// 
    /// The handler receives:
    /// - `request`: The raw bytes of the incoming message
    /// - `from`: The SocketAddr of the sender
    /// 
    /// The handler returns the response bytes to send back.
    pub async fn listen<F, Fut>(&self, handler: F) -> anyhow::Result<()>
    where
        F: Fn(Vec<u8>, SocketAddr) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = anyhow::Result<Vec<u8>>> + Send + 'static,
    {
        info!("Listening for connections on {}", self.addr);

        let handler = Arc::new(handler);
        loop {
            match self.listener.accept().await {
                Ok((stream, peer_addr)) => {
                    debug!("Connection from {}", peer_addr);
                    let handler = Arc::clone(&handler);
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, peer_addr, handler).await {
                            error!("Error handling connection from {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                    // Optionally, break or return Err if fatal error
                }
            }
        }
    }
}

/// Handle a single incoming connection: receive message, call handler, send response.
async fn handle_connection<F, Fut>(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    handler: Arc<F>,
) -> anyhow::Result<()>
where
    F: Fn(Vec<u8>, SocketAddr) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = anyhow::Result<Vec<u8>>> + Send + 'static,
{
    // Receive the request
    let request = receive_bytes(&mut stream).await?;
    debug!("Received {} bytes from {}", request.len(), peer_addr);

    // Process and get response
    let response = (handler)(request, peer_addr).await?;

    // Send the response back
    send_bytes(&mut stream, &response).await?;
    debug!("Sent {} bytes response to {}", response.len(), peer_addr);

    Ok(())
}

// ============================================================================
// Connection - Initiates connection and sends messages
// ============================================================================

/// An outgoing connection to another peer.
/// Used to send a message and receive a response.
pub struct Connection {
    stream: TcpStream,
    peer_addr: SocketAddr,
}

/// Connect to a remote peer by address.
pub async fn connect(addr: SocketAddr) -> anyhow::Result<Connection> {
    connect_with_timeout(addr, DEFAULT_TIMEOUT).await
}

/// Connect to a remote peer with a custom timeout.
pub async fn connect_with_timeout(addr: SocketAddr, timeout: Duration) -> anyhow::Result<Connection> {
    let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| anyhow::anyhow!("Connection timeout to {}", addr))??;
    debug!("Connected to {}", addr);
    Ok(Connection { stream, peer_addr: addr })
}

impl Connection {
    /// Get the remote peer's address.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Send a message and wait for the response.
    /// This is the primary way to communicate: message in, response out.
    pub async fn message(mut self, data: &[u8]) -> anyhow::Result<Vec<u8>> {
        // Send the message
        send_bytes(&mut self.stream, data).await?;
        debug!("Sent {} bytes to {}", data.len(), self.peer_addr);

        // Wait for response
        let response = receive_bytes(&mut self.stream).await?;
        debug!("Received {} bytes response from {}", response.len(), self.peer_addr);

        Ok(response)
    }

    /// Send a message without waiting for a response (fire-and-forget).
    pub async fn send(mut self, data: &[u8]) -> anyhow::Result<()> {
        send_bytes(&mut self.stream, data).await?;
        debug!("Sent {} bytes to {} (no response expected)", data.len(), self.peer_addr);
        Ok(())
    }
}

// ============================================================================
// Wire Protocol - Length-prefixed message framing
// ============================================================================

/// Send length-prefixed bytes over a stream.
async fn send_bytes(stream: &mut TcpStream, data: &[u8]) -> anyhow::Result<()> {
    let len = data.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(data).await?;
    stream.flush().await?;
    Ok(())
}

/// Receive length-prefixed bytes from a stream.
async fn receive_bytes(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    
    let msg_len = u32::from_be_bytes(len_buf) as usize;
    if msg_len > MAX_MESSAGE_SIZE {
        return Err(anyhow::anyhow!("Message too large: {} bytes", msg_len));
    }
    
    let mut buf = vec![0u8; msg_len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}
