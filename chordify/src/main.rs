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
    // Parse -k and -m only for bootstrap node
    let mut k: Option<u64> = None;
    let mut t: Option<u64> = None;
    let mut filtered_args: Vec<String> = Vec::new();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-k" => {
                if i + 1 < args.len() {
                    let k_val = &args[i + 1];
                    // Only treat as missing if the next token is another known flag
                    if k_val == "-k" || k_val == "-m" || k_val == "-t" {
                        println!("Error: Missing value for -k (replication factor)");
                        std::process::exit(1);
                    }
                    match k_val.parse::<i64>() {
                        Ok(val) if val >= 1 => {
                            k = Some(val as u64);
                        },
                        Ok(val) => {
                            println!("Error: Replication factor (-k) must be at least 1, got {}.", val);
                            std::process::exit(1);
                        },
                        Err(_) => {
                            println!("Error: Invalid value for -k (replication factor): '{}'. Must be an integer >= 1.", k_val);
                            std::process::exit(1);
                        }
                    }
                    i += 2;
                } else {
                    println!("Error: Missing value for -k (replication factor)");
                    std::process::exit(1);
                }
            },
            "-m" | "-t" => {
                if i + 1 < args.len() {
                    let t_val = &args[i + 1];
                    // Only treat as missing if the next token is another known flag
                    if t_val == "-k" || t_val == "-m" || t_val == "-t" {
                        println!("Error: Missing value for {} (replication mode)", args[i]);
                        std::process::exit(1);
                    }
                    match t_val.parse::<i64>() {
                        Ok(val) if val == 0 || val == 1 => {
                            t = Some(val as u64);
                        },
                        Ok(val) => {
                            println!("Error: Replication mode ({}) must be 0 (linearizability) or 1 (eventual consistency), got {}.", args[i], val);
                            std::process::exit(1);
                        },
                        Err(_) => {
                            println!("Error: Invalid value for {} (replication mode): '{}'. Must be 0 or 1.", args[i], t_val);
                            std::process::exit(1);
                        }
                    }
                    i += 2;
                } else {
                    println!("Error: Missing value for {} (replication mode)", args[i]);
                    std::process::exit(1);
                }
            },
            _ => {
                filtered_args.push(args[i].clone());
                i += 1;
            }
        }
    }
    let addr: SocketAddr = match filtered_args.get(0) {
        Some(a) => match a.parse() {
            Ok(addr) => addr,
            Err(_) => {
                println!("Error: Invalid address '{}'.", a);
                println!("Usage: chordify <my_ip:port> [bootstrap_ip:port] ...");
                std::process::exit(1);
            }
        },
        None => {
            println!("Error: Missing address argument.");
            println!("Usage: chordify <my_ip:port> [bootstrap_ip:port] ...");
            std::process::exit(1);
        }
    };
    let bootstrap_arg: Option<SocketAddr> = filtered_args.get(1).and_then(|s| {
        if s.starts_with('-') {
            None
        } else {
            match s.parse() {
                Ok(addr) => Some(addr),
                Err(_) => {
                    println!("Error: Invalid bootstrap address '{}'.", s);
                    println!("Usage: chordify <my_ip:port> [bootstrap_ip:port] ...");
                    std::process::exit(1);
                }
            }
        }
    });

    println!("Chordify starting at {}", addr);
    if let Some(bs_addr) = bootstrap_arg {
        println!("Joining ring via bootstrap at {}", bs_addr);
        // regular node
        let node = Arc::new(Node::new(addr, bs_addr));
        let command_node = Arc::clone(&node);

        // network thread: start listener FIRST, then join
        let network_handle = thread::spawn(move || {
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
                match node.join().await {
                    Ok(_) => println!("Successfully joined the ring via bootstrap at {}", bs_addr),
                    Err(e) => {
                        println!("Failed to join the ring: {}", e);
                        std::process::exit(1);
                    }
                }
                // Keep the runtime alive
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            });
        });

        // command thread
        let bs_for_cmd = bootstrap_arg.clone();
        let cmds = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime");
            let stdin = io::stdin();
            println!("Chordify CLI ready. Type 'help' for commands.");
            for line in stdin.lock().lines() {
                let line = match line { Ok(l) => l, Err(_) => break };
                let mut parts = line.trim().split_whitespace();
                if let Some(cmd) = parts.next() {
                    match cmd {
                        "insert" => {
                            if let (Some(k), Some(v)) = (parts.next(), parts.next()) {
                                println!("Inserting key '{}' with value '{}'...", k, v);
                                match rt.block_on(command_node.insert(k.to_string(), v.to_string())) {
                                    Ok(_) => println!("Insert successful."),
                                    Err(e) => println!("Insert failed: {}", e),
                                }
                            } else {
                                println!("usage: insert <key> <value>");
                            }
                        }
                        "query" => {
                            if let Some(k) = parts.next() {
                                // Allow wildcard query "*" to fetch all keys
                                println!("Querying key '{}'...", k);
                                let results = rt.block_on(command_node.query(k.to_string()));
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
                            } else {
                                println!("usage: query <key|*>");
                            }
                        }
                        "delete" => {
                            if let Some(k) = parts.next() {
                                println!("Deleting key '{}'...", k);
                                match rt.block_on(command_node.delete(k.to_string())) {
                                    Ok(_) => println!("Delete successful."),
                                    Err(e) => println!("Delete failed: {}", e),
                                }
                            } else {
                                println!("usage: delete <key>");
                            }
                        }
                        "overlay" => {
                            println!("Requesting ring topology...");
                            let topo = rt.block_on(command_node.overlay());
                            if topo.is_empty() {
                                println!("overlay request failed or ring is empty");
                            } else {
                                println!("ring topology (id -> addr):");
                                for (id, addr) in topo {
                                    println!("  {} -> {}", id, addr);
                                }
                            }
                        }
                        "depart" => {
                            if let Some(bs) = bs_for_cmd {
                                println!("Departing from ring via bootstrap at {}...", bs);
                                match rt.block_on(command_node.depart(bs)) {
                                    Ok(_) => println!("Departed from ring."),
                                    Err(e) => println!("Depart failed: {}", e),
                                }
                            } else {
                                println!("bootstrap node cannot depart");
                            }
                            std::process::exit(1);
                        }
                        "help" => {
                            println!("\nChordify CLI Commands:");
                            println!("  help                 - Show this help message");
                            println!("  insert <key> <value> - Insert a key-value pair");
                            println!("  delete <key>         - Delete a key");
                            println!("  query <key|*>        - Query a key or all keys (use *)");
                            println!("  depart               - Depart from the ring");
                            println!("  overlay              - Print ring topology");
                        }
                        _ => println!("unknown command '{}'", cmd),
                    }
                }
            }
        });

        cmds.join().expect("command thread panicked");
        network_handle.join().expect("network thread panicked");
    } else {
        // bootstrap node: require k and t
        if k.is_none() || t.is_none() {
            println!("Error: Both -k <replication factor> and -m <replication mode> must be provided for bootstrap node.");
            println!("Usage: chordify <my_ip:port> -k <replication factor> -m <replication mode (t = 0 for linearizability, 1 for eventual consistency)>");
            std::process::exit(1);
        }
        let k = k.unwrap();
        let t = t.unwrap();
        if t != 0 && t != 1 {
            println!("Error: Replication mode (-m) must be 0 (linearizability) or 1 (eventual consistency).");
            std::process::exit(1);
        }
        if k < 1 {
            println!("Error: Replication factor (-k) must be at least 1.");
            std::process::exit(1);
        }
        let t = t as u8;
        println!("Starting as bootstrap node at {}", addr);
        println!("Bootstrap is running ... Open a node to use the command line interface in a separate terminal with:\n  {} <my_ip:port> <bootstrap_ip:port>", args.get(0).unwrap_or(&"chordify".to_string()));
        let bootstrap = BootstrapNode::new(addr, k, t);
        
        let network_handle = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("unable to make runtime");
            rt.block_on(async move {
                let _ = bootstrap.run().await;
            });
        });
        network_handle.join().expect("network thread panicked");
        // Exit immediately after printing the message (or block forever if you want the process to stay alive)
        // std::thread::park();
        return Ok(());
    }

    Ok(())
}
