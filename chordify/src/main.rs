//! Chordify - P2P Song Sharing Application
//! 
//! A peer-to-peer song sharing application using the Chord DHT protocol.

use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use chordify::nodes::Node;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    
    let addr: SocketAddr = args.get(1)
        .unwrap_or(&"127.0.0.1:8000".to_string())
        .parse()?;

    let join_addr: Option<SocketAddr> = args.get(2)
        .map(|s| s.parse())
        .transpose()?;

    // Create the node
    let node = Node::new(addr);
    info!("Node at {}", node.addr());

    // Join existing ring or create new one
    if let Some(known_addr) = join_addr {
        info!("Joining ring via {}", known_addr);
        node.join(known_addr).await?;
    } else {
        info!("Creating new ring");
        node.create_ring().await;
    }

    // Run the node (listens for requests)
    node.run().await
}

