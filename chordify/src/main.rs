//! Chordify binary: activate a node with concurrent network
//! listener and an interactive command loop.
//!
//! Usage:
//! ```text
//! chordify <my_ip:port> [bootstrap_ip:port]
//! ```
//!
//! If the second argument is supplied the node will join an existing ring via
//! the bootstrap address.  Otherwise this process becomes the bootstrap node
//! (first node in a new ring).

use std::io::{self, BufRead};
use std::net::SocketAddr;
use std::thread;
use std::sync::Arc;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use chordify::nodes::Node;
use chordify::BootstrapNode;

fn main() -> anyhow::Result<()> {
    // logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args: Vec<String> = std::env::args().collect();
    let addr: SocketAddr = args
        .get(1)
        .unwrap_or(&"127.0.0.1:8000".to_string())
        .parse()?;
    let bootstrap_arg: Option<SocketAddr> = args.get(2).map(|s| s.parse()).transpose()?;

    // we will keep an `Arc<Node>` instance for the command thread
    let command_node: std::sync::Arc<Node>;
    let network_handle;

    if let Some(bs_addr) = bootstrap_arg {
        // regular node
        let node = Arc::new(Node::new(addr, bs_addr));
        command_node = Arc::clone(&node);

        // network thread: start listener FIRST, then join
        network_handle = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("unable to make runtime");
            rt.block_on(async move {
                let node_clone = Arc::clone(&node);
                
                // Start the listener in a background task
                tokio::spawn(async move {
                    let _ = node_clone.run().await;
                });
                
                // Wait a bit for the listener to start
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                
                // Now join the ring - the node can receive TransferData requests
                node.join().await.expect("join failed");
                
                // Keep the runtime alive
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            });
        });
    } else {
        // bootstrap node: create both a BootstrapNode (for network) and an
        // `Arc<Node>` for the command loop.  We don't need the bootstrap
        // instance itself for commands.
        let bootstrap = BootstrapNode::new(addr);
        let node_inst = Arc::new(Node::new(addr, addr));
        command_node = Arc::clone(&node_inst);

        network_handle = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("unable to make runtime");
            rt.block_on(async move {
                let _ = bootstrap.run().await;
            });
        });
    }

    // command thread
    let bs_for_cmd = bootstrap_arg.clone();
    let cmds = thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let line = match line { Ok(l) => l, Err(_) => break };
            let mut parts = line.trim().split_whitespace();
            if let Some(cmd) = parts.next() {
                match cmd {
                    "insert" => {
                        if let (Some(k), Some(v)) = (parts.next(), parts.next()) {
                            rt.block_on(command_node.insert(k.to_string(), v.to_string())).ok();
                        } else {
                            println!("usage: insert <key> <value>");
                        }
                    }
                    "query" => {
                        if let Some(k) = parts.next() {
                            rt.block_on(command_node.query(k.to_string())).ok();
                        } else {
                            println!("usage: query <key>");
                        }
                    }
                    "delete" => {
                        if let Some(k) = parts.next() {
                            rt.block_on(command_node.delete(k.to_string())).ok();
                        } else {
                            println!("usage: delete <key>");
                        }
                    }
                    "depart" => {
                        if let Some(bs) = bs_for_cmd {
                            rt.block_on(command_node.depart(bs)).ok();
                        } else {
                            println!("bootstrap node cannot depart");
                        }
                    }
                    "exit" | "quit" => break,
                    _ => println!("unknown command '{}'", cmd),
                }
            }
        }
    });

    cmds.join().expect("command thread panicked");
    network_handle.join().expect("network thread panicked");

    Ok(())
}
