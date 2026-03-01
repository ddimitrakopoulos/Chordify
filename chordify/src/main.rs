//! Chordify - P2P Song Sharing Application
//! 
//! A peer-to-peer song sharing application using the Chord DHT protocol.

use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use chordify::nodes::Node;
use chordify::BootstrapNode;

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

    // Join existing ring or create new one as bootstrap
    if let Some(bootstrap_addr) = join_addr {
        // Join as regular node
        info!("Joining ring via bootstrap at {}", bootstrap_addr);
        let node = Node::new(addr);
        node.join(bootstrap_addr).await?;
        node.run().await
    } else {
        // Create new ring as bootstrap node
        info!("Creating new ring as bootstrap node at {}", addr);
        let bootstrap = BootstrapNode::new(addr);
        bootstrap.create_ring().await;
        bootstrap.run().await
    }
}

