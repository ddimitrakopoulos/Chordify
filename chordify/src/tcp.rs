//! A TCP communication module with a Server/Client API.
//!
//! The code provides a simple API for sending and receiving messages between nodes,
//! abstracting away the underlying TCP connections and message framing.
//! It doesn't make any assumptions about the application-level protocol; it simply sends and receives raw bytes.
//! The main components are: 
//!     Server: listens for incoming connections and handles requests
//!     Client: opens a connection, sends a message, waits for a response and then closes the connection
//! 
//!
//! Basic usage example:
//!                           Node A                          Node B
//!     
//!                            |                               |  (start server, listen for connections)
//!                            |                               |
//!    (establish connection)  |-------- connect() ----------->|  
//!                            |                               |
//!      (send request bytes)  |-------- message() ----------->|  (recieve request bytes) 
//!                            |                               |
//!  (receive response bytes)  |<------- response() -----------|  (send response bytes)
//!                            |                               |
//!        (close connection)  |                               | 
//!

use std::net::SocketAddr; // IP address + port
use std::sync::Arc; // Atomic Reference Counted: A smart pointer allowing thread-safe (read only!) reference-counted access to the handler across connections 
use std::time::Duration; 
use tokio::net::{
    TcpListener, // TCP socket server, listening for connections
    TcpStream // TCP socket client, used to connect to a server and send/receive messages
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, error, debug};

/// Maximum message size (16 MB)
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Default connection timeout
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);


// ============================================================================
// Server - Listens for incoming connections and handles requests
// ============================================================================

/// A Server that listens for incoming TCP connections and handles requests using a provided handler function.
pub struct Server {
    listener: TcpListener,
    addr: SocketAddr,
}

impl Server {
    /// Bind to an address and create a new Server ready to accept connections.
    /// If the port in the address is 0, the OS will choose an available port. The actual bound address (with the chosen port) is returned in the Server instance.
    pub async fn bind(addr: SocketAddr) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(addr).await?; // Create a TCP listener bound to the specified address
        let bound_addr = listener.local_addr()?; // Get the actual bound address (useful if addr had port 0, which tells the OS to choose an available port)
        info!("Server bound to {}", bound_addr);

        Ok(Self { listener, addr: bound_addr }) // Return the successfully initialized Server instance with the listener and its bound address
    }

    /// Get this server's bound address.
    pub fn get_addr(&self) -> SocketAddr {
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

        // Accept incoming connections in a loop. For each connection, spawn a new task to handle it concurrently.
        loop {
            match self.listener.accept().await {
                Ok((stream, client_addr)) => {
                    debug!("Connection from {}", client_addr);

                    let handler = Arc::clone(&handler);

                    // Spawn a new asynchronous task to handle the connection so that we can continue accepting other connections concurrently.
                    // We use move to transfer ownership of the stream, client_addr, and handler into the async block. That is because tokio::spawn
                    // requires the future to be 'static, meaning it must own all its data or only reference 'static data.
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, client_addr, handler).await {
                            error!("Error handling connection from {}: {}", client_addr, e);
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
    client_addr: SocketAddr,
    handler: Arc<F>,
) -> anyhow::Result<()>
where
    F: Fn(Vec<u8>, SocketAddr) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = anyhow::Result<Vec<u8>>> + Send + 'static,
{
    // Receive the request bytes from the stream. This will read the length prefix and then the message bytes.
    let request = receive_bytes(&mut stream).await?;
    debug!("Received {} bytes from {}", request.len(), client_addr);

    // Process the request and compute the response by calling the provided handler function. The handler is asynchronous, so we await its result.
    // The handler is expected to return a Result containing the response bytes or an error. If the handler returns an error, we propagate it up.
    let response = (handler)(request, client_addr).await?;

    // Send the response back to the client using the same stream. This will write the length prefix and then the response bytes.
    send_bytes(&mut stream, &response).await?;
    debug!("Sent {} bytes response to {}", response.len(), client_addr);

    Ok(())
}

// ============================================================================
// Client - Initiates connection and sends messages
// ============================================================================

/// An outgoing connection to another node.
/// Used to send a message and receive a response.
pub struct Client {
    stream: TcpStream,
    target_addr: SocketAddr,
}

/// Connect to a remote node using its SocketAddr. 
pub async fn connect(target_addr: SocketAddr) -> anyhow::Result<Client> {
    connect_with_timeout(target_addr, DEFAULT_TIMEOUT).await
}

/// Connect to a remote node with a custom timeout.
pub async fn connect_with_timeout(target_addr: SocketAddr, timeout: Duration) -> anyhow::Result<Client> {
    // TcpStream::connect is a future that resolves when the connection is established. We wrap it with a timeout to avoid hanging indefinitely if the node is unreachable.
    let stream = tokio::time::timeout(timeout, TcpStream::connect(target_addr))
        .await
        .map_err(|_| anyhow::anyhow!("Connection timeout to {}", target_addr))??;
    debug!("Connected to {}", target_addr);

    Ok(Client { stream, target_addr })
}

impl Client {
    /// Get the remote node's address.
    pub fn get_target_addr(&self) -> SocketAddr {
        self.target_addr
    }

    /// Send a message and wait for the response.
    /// This is the primary way to communicate: message in, response out.
    /// We use mut self, instead of &mut self, because we want to consume the connection after
    /// sending a message, i.e. noone can reuse it. This is by design, you open a new connection
    /// send a message, get a response and close the connection. 
    pub async fn message(mut self, data: &[u8]) -> anyhow::Result<Vec<u8>> {
        // Send the message
        send_bytes(&mut self.stream, data).await?;
        debug!("Sent {} bytes to {}", data.len(), self.target_addr);

        // Wait for response
        let response = receive_bytes(&mut self.stream).await?;
        debug!("Received {} bytes response from {}", response.len(), self.target_addr);

        Ok(response)
    }

    /// Send a message without waiting for a response (fire-and-forget).
    pub async fn send(mut self, data: &[u8]) -> anyhow::Result<()> {
        send_bytes(&mut self.stream, data).await?;
        debug!("Sent {} bytes to {} (no response expected)", data.len(), self.target_addr);
        Ok(())
    }
}

// ============================================================================
// Wire Protocol - Length-prefixed message framing
// When you consume a stream, you don't know how many bytes the sender will send. 
// To solve this, we use a simple framing protocol: the sender first sends a 4-byte
// big-endian unsigned integer indicating the length of the message, followed by the message bytes.
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
