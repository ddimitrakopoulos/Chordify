//! Communication module for Chordify
//!
//! A general-purpose P2P messaging layer for peer-to-peer communication.
//! 
//! # Architecture: Connect → Message → Response
//! 
//! All peers are equal. Any peer can:
//! - **Listen** for incoming connections and respond to messages
//! - **Connect** to another peer, send a message, and receive a response
//!
//! # Design
//! - Peers are identified purely by their IP address and port (SocketAddr).
//! - No client/server distinction: all nodes are equal peers.
//! - Messages are raw bytes; serialization is the caller's responsibility.
//!
//! # Example
//! ```ignore
//! // Responder side
//! let peer = Peer::bind("127.0.0.1:8000".parse()?).await?;
//! peer.listen(|request, from| async move {
//!     Ok(request) // echo
//! }).await?;
//!
//! // Initiator side  
//! let response = connect("127.0.0.1:8000".parse()?).await?.message(b"hello").await?;
//! ```

mod peer;

pub use peer::{Peer, Connection, connect, connect_with_timeout};
