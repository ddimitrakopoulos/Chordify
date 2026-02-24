//! Chordify - P2P Song Sharing Application
//! 
//! A peer-to-peer song sharing application using the Chord DHT protocol.

mod communication;
mod nodes;

use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use communication::Peer;
use nodes::NodeId;

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
    info!("Node ID: {}", NodeId::from_address(&addr));

    // Create peer and start listening
    let peer = Peer::new(addr);
    
    // Handle incoming connections
    peer.listen(|mut conn| async move {
        info!("New connection from {}", conn.peer_addr());
        
        // Simple echo handler for demonstration
        loop {
            match conn.receive().await {
                Ok(Some(data)) => {
                    info!("Received {} bytes from {}", data.len(), conn.peer_addr());
                    // Echo back
                    if let Err(e) = conn.answer(&data).await {
                        info!("Error sending response: {}", e);
                        break;
                    }
                }
                Ok(None) => {
                    info!("Connection closed by {}", conn.peer_addr());
                    break;
                }
                Err(e) => {
                    info!("Error receiving: {}", e);
                    break;
                }
            }
        }
    }).await?;

    Ok(())
}

