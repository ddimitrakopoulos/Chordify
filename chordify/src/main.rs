//! Chordify - P2P Song Sharing Application
//! 
//! Phase 1: Architecture Choices
//! - Language: Rust (safe, fast, great async support)
//! - Libraries: Tokio (async runtime), Serde (serialization), SHA-1 (hashing)
//!
//! Phase 2: Basic Node Infrastructure
//! - Socket Setup: Async TCP server/client with Tokio
//! - ID Generation: SHA-1 hash of ip:port
//! - Message Protocol: JSON-based custom protocol

mod communication;

use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use communication::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Default address
    let addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8000".to_string())
        .parse()?;

    info!("Starting Chordify node at {}", addr);
    info!("Node ID: {}", communication::NodeId::from_address(&addr));

    // Create and start the server
    let server = Server::new(addr);
    
    // Initialize as single-node ring (self is successor and predecessor)
    {
        let state = server.state();
        let mut state = state.write().await;
        let self_info = state.info();
        state.successor = Some(self_info.clone());
        state.predecessor = Some(self_info);
        info!("Initialized single-node ring");
    }

    // Start listening for connections
    server.start().await?;

    Ok(())
}

