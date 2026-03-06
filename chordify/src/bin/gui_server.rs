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

use std::net::SocketAddr;
use std::thread;
use std::sync::Arc;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use chordify::nodes::Node;
use chordify::BootstrapNode;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};

// App State to share the Node across HTTP requests
#[derive(Clone)]
struct AppState {
    node: Arc<Node>,
    bootstrap_addr: Option<SocketAddr>,
}

// Request / Response structures
#[derive(Deserialize)]
struct InsertReq {
    key: String,
    value: String,
}

// Axum Handlers
async fn handle_insert(
    State(state): State<AppState>,
    Json(payload): Json<InsertReq>,
) -> Result<String, StatusCode> {
    println!("API: Inserting key '{}' with value '{}'...", payload.key, payload.value);
    match state.node.insert(payload.key, payload.value).await {
        Ok(_) => Ok("Insert successful".to_string()),
        Err(e) => {
            eprintln!("Insert failed: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn handle_query(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<Vec<(u64, Vec<String>)>>, StatusCode> { // Assuming hash is u64 based on your original prints
    println!("API: Querying key '{}'...", key);
    let results = state.node.query(key).await;
    Ok(Json(results))
}

async fn handle_delete(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<String, StatusCode> {
    println!("API: Deleting key '{}'...", key);
    match state.node.delete(key).await {
        Ok(_) => Ok("Delete successful".to_string()),
        Err(e) => {
            eprintln!("Delete failed: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn handle_overlay(
    State(state): State<AppState>,
) -> Result<Json<Vec<(u64, SocketAddr)>>, StatusCode> { // Assuming ID is u64
    println!("API: Requesting ring topology...");
    let topo = state.node.overlay().await;
    Ok(Json(topo))
}

async fn handle_depart(State(state): State<AppState>) -> Result<String, StatusCode> {
    if let Some(bs) = state.bootstrap_addr {
        println!("API: Departing from ring via bootstrap at {}...", bs);
        match state.node.depart(bs).await {
            Ok(_) => {
                // Exit the process after a successful departure
                tokio::spawn(async {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    std::process::exit(0);
                });
                Ok("Departed from ring".to_string())
            }
            Err(e) => {
                eprintln!("Depart failed: {}", e);
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    } else {
        Err(StatusCode::BAD_REQUEST)
    }
}

/// Split a CLI line into whitespace-delimited tokens, but keep quoted strings intact.
///
/// Examples:
/// - `insert "hello there" "oh hi"` -> ["insert", "hello there", "oh hi"]
/// - `query "hello there"` -> ["query", "hello there"]
///
/// Notes:
/// - Only double quotes are supported.
/// - Backslash escaping is not supported.
fn split_cli_args(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;

    for ch in line.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            c if c.is_whitespace() && !in_quotes => {
                if !cur.is_empty() {
                    out.push(cur.clone());
                    cur.clear();
                }
            }
            _ => cur.push(ch),
        }
    }

    if !cur.is_empty() {
        out.push(cur);
    }

    out
}

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

        // command thread -> Now an HTTP API thread
        let bs_for_cmd = bootstrap_arg.clone();
        
        // We will assign the API port to be exactly 10,000 higher than the node's internal port.
        // E.g., if the node is 127.0.0.1:8000, the React API connects to 127.0.0.1:18000.
        let api_port = addr.port() + 10000;
        let api_addr = SocketAddr::from(([127, 0, 0, 1], api_port));

        let cmds = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime");
            rt.block_on(async {
                let state = AppState {
                    node: command_node,
                    bootstrap_addr: bs_for_cmd,
                };

                // CORS is completely open here so your React dev server (e.g., localhost:5173) can access it
                let cors = CorsLayer::new()
                    .allow_origin(Any)
                    .allow_methods(Any)
                    .allow_headers(Any);

                let app = Router::new()
                    .route("/insert", post(handle_insert))
                    // Note: Since React might pass "*", we use a query param or careful path routing. 
                    // Axum handles wildcards in paths fine if URL-encoded.
                    .route("/query/:key", get(handle_query)) 
                    .route("/delete/:key", delete(handle_delete))
                    .route("/overlay", get(handle_overlay))
                    .route("/depart", post(handle_depart))
                    .layer(cors)
                    .with_state(state);

                println!("Chordify HTTP API ready at http://{}", api_addr);
                
                let listener = tokio::net::TcpListener::bind(api_addr).await.expect("Failed to bind API port");
                axum::serve(listener, app).await.expect("API server crashed");
            });
        });

        cmds.join().expect("command thread panicked");
        network_handle.join().expect("network thread panicked");
    } else {
        // bootstrap node: require k and t
        if k.is_none() || t.is_none() {
            println!("Error: Both -k <replication factor> and -m <replication mode> must be provided for bootstrap node.");
            std::process::exit(1);
        }
        let k = k.unwrap();
        let t = t.unwrap() as u8;
        
        println!("Starting as bootstrap node at {}", addr);
        println!("Bootstrap is running ... Open a node to use the command line interface in a separate terminal with:\n  {} <my_ip:port> <bootstrap_ip:port>", args.get(0).unwrap_or(&"chordify".to_string()));
        
        let t_val = t as u8;
        
        println!("Starting as bootstrap node at {}", addr);
        let bootstrap = Arc::new(BootstrapNode::new(addr, k, t_val));
        let api_port = addr.port() + 10000;
        let api_addr = SocketAddr::from(([127, 0, 0, 1], api_port));
        
        // 1. Wrap the bootstrap node in an Arc to guarantee it stays in memory
        // let bootstrap = Arc::new(BootstrapNode::new(addr, k, t));
        
        let network_handle = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("unable to make runtime");
            rt.block_on(async move {
                let bs_for_run = Arc::clone(&bootstrap);
                let bs_for_api = Arc::clone(&bootstrap);
                
                // 1. Run the Chord Listener
                tokio::spawn(async move {
                    if let Err(e) = bs_for_run.run().await {
                        eprintln!("CRITICAL ERROR: Bootstrap node crashed: {}", e);
                    }
                });


                // 2. Start the HTTP API for the Bootstrap Node
                // Note: If BootstrapNode doesn't support .overlay() or .query(), 
                // you may need to implement a dummy handler or add those methods to BootstrapNode.
                let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any);
                let app = Router::new()
                    .route("/overlay", get(move || {
                        // Clone again inside the closure to move into the async block
                        let bs = Arc::clone(&bs_for_api);
                        async move {
                            let members = bs.ring_members.read().await;
                            
                            // Map NodeInfo to (u64, SocketAddr) tuples so the JSON matches the regular node
                            let topology: Vec<(u64, SocketAddr)> = members
                                .iter()
                                .map(|m| (m.id, m.addr))
                                .collect();
                                
                            // Add the bootstrap node itself to the topology 
                            let bs_info = (0, addr);
                            let mut full_topology = vec![bs_info];
                            full_topology.extend(topology);

                            Json(full_topology)
                        }
                    })) // You may need to implement this for BootstrapNode
                    .route("/ping", get(|| async { "pong" })) // Simple health check endpoint
                    .layer(cors);

                println!("Chordify HTTP API (Bootstrap) ready at http://{}", api_addr);
                let listener = tokio::net::TcpListener::bind(api_addr).await.unwrap();
                axum::serve(listener, app).await.unwrap();
            });
        });
        
        network_handle.join().expect("network thread panicked");
        return Ok(());
    }

    Ok(())
}