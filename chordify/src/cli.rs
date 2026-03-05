use clap::Parser;
use clap::Subcommand;
use std::net::SocketAddr;
use std::thread;
use std::sync::Arc;

use chordify::nodes::Node;
use chordify::BootstrapNode;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Join {
        addr: String,
        bootstrap_arg: String, //optional, if not provided will start a bootstrap node
    },
    Insert {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    Query {
        key: String,
    },
    Overlay,
    Depart,
}

pub fn init() ->  anyhow::Result<std::sync::Arc<Node>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Join { addr, bootstrap_arg } => {
            // we will keep an `Arc<Node>` instance for the command thread
            let command_node: std::sync::Arc<Node>;
            let network_handle;
            let addr: std::net::SocketAddr = addr.parse()?;
            let bootstrap_arg: Option<std::net::SocketAddr> = if bootstrap_arg.is_empty() {
                None
            } else {
                Some(bootstrap_arg.parse()?)
            };

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
            }
            return Ok(command_node);
        }

        _ => {
            println!("Please join the network first using the 'join' command");
            std::process::exit(1);
        }

    }

}

pub fn run(node: std::sync::Arc<Node>) {
    let cli = Cli::parse();
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    match cli.command {
        Commands::Insert { key, value } => {
            node.insert(key, value);
        }
        Commands::Delete { key } => {
            node.delete(key);
        }
        Commands::Query { key } => {
            let results = rt.block_on(node.query(key));
            if results.is_empty() {
                println!("no entries found");
            } else {
                for (hash, vals) in results {
                    if vals.is_empty() {
                        println!("key hash {}: <none>", hash);
                    } else {
                        println!("key hash {}: {}", hash, vals.join(", "));
                    }
                }
            }
        }
        Commands::Overlay => {
            let topo = rt.block_on(node.overlay());
            if topo.is_empty() {
                println!("overlay request failed or ring is empty");
            } else {
                println!("ring topology (id -> addr):");
                for (id, addr) in topo {
                    println!("  {} -> {}", id, addr);
                }
            }
        }
        
        Commands::Depart => {
            println!("Departing from the network");
            //node.depart();
        }

        Commands::Join { addr, bootstrap_arg } => {
            println!("Already joined"); 
        }
    }
}