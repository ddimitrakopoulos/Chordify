//! Chordify - P2P Song Sharing Application
//! 
//! A peer-to-peer song sharing application using the Chord DHT protocol.

mod communication;
mod nodes;

use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use communication::Peer;

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

    info!("Starting Chordify peer at {}", addr);

    // Bind and start listening
    let peer = Peer::bind(addr).await?;
    
    // Handle incoming messages with echo response
    peer.listen(|request, from| async move {
        info!("Received {} bytes from {}", request.len(), from);
        // Echo back the request as the response
        Ok(request)
    }).await?;

    Ok(())
}

