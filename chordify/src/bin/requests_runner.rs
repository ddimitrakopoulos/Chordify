//! Batch runner for `data/requests.txt`.
//!
//! This binary is meant to be used as a *client node* that joins an already
//! running Chord ring and then replays a workload file consisting of `insert`
//! and `query` operations.
//!
//! It records the answers of all queries for both:
//! - **Linearizability** (`t = 0`)
//! - **Eventual consistency** (`t = 1`)
//!
//! The ring itself must already be running with 10 nodes and `k = 3`.
//!
//! File format (CSV-ish):
//! - `insert, <key>, <value>`
//! - `query, <key>`
//!
//! Example:
//! - `insert, Hey Jude, 1001`
//! - `query, Hey Jude`

use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use clap::Parser;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use chordify::nodes::Node;

#[derive(Debug, Clone, Parser)]
#[command(name = "requests_runner")]
struct Args {
    /// Address for THIS node (must be a free ip:port).
    #[arg(long)]
    addr: SocketAddr,

    /// Bootstrap address of the already running ring.
    #[arg(long)]
    bootstrap: SocketAddr,

    /// Path to requests file.
    #[arg(long, default_value = "data/requests.txt")]
    requests: PathBuf,

    /// Output file for query results.
    #[arg(long, default_value = "output.txt")]
    out: PathBuf,

    /// Delay between requests, in milliseconds.
    #[arg(long, default_value_t = 0)]
    delay_ms: u64,

    /// Optional: extra delay after joining (lets ring stabilize).
    #[arg(long, default_value_t = 500)]
    join_grace_ms: u64,
}

#[derive(Debug, Clone)]
enum Op {
    Insert { key: String, value: String },
    Query { key: String },
}

fn parse_requests_file(contents: &str) -> anyhow::Result<Vec<Op>> {
    let mut ops = Vec::new();

    for (i, raw_line) in contents.lines().enumerate() {
        let line_no = i + 1;
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        // Split by comma, trim each field.
        let parts: Vec<String> = line.split(',').map(|p| p.trim().to_string()).collect();
        let op = parts.first().map(|s| s.as_str()).unwrap_or("");

        match op {
            "insert" => {
                if parts.len() < 3 {
                    return Err(anyhow!(
                        "Invalid insert at line {}: expected 'insert, <key>, <value>', got: {}",
                        line_no,
                        raw_line
                    ));
                }
                ops.push(Op::Insert {
                    key: parts[1].clone(),
                    value: parts[2].clone(),
                });
            }
            "query" => {
                if parts.len() < 2 {
                    return Err(anyhow!(
                        "Invalid query at line {}: expected 'query, <key>', got: {}",
                        line_no,
                        raw_line
                    ));
                }
                ops.push(Op::Query { key: parts[1].clone() });
            }
            other => {
                return Err(anyhow!(
                    "Unknown operation '{}' at line {}: {}",
                    other,
                    line_no,
                    raw_line
                ));
            }
        }
    }

    Ok(ops)
}

fn format_query_result(results: Vec<(u64, Vec<String>)>) -> String {
    // Make output deterministic.
    let mut map: BTreeMap<u64, Vec<String>> = BTreeMap::new();
    for (node_id, mut vals) in results {
        vals.sort();
        map.insert(node_id, vals);
    }

    let mut out = String::new();
    for (node_id, vals) in map {
        if vals.is_empty() {
            out.push_str(&format!("{}: <none>\n", node_id));
        } else {
            out.push_str(&format!("{}: {}\n", node_id, vals.join(" | ")));
        }
    }
    out
}

async fn start_client_node(addr: SocketAddr, bootstrap: SocketAddr) -> anyhow::Result<Arc<Node>> {
    let node = Arc::new(Node::new(addr, bootstrap));

    // Start listener in background.
    let node_for_task = Arc::clone(&node);
    tokio::spawn(async move {
        if let Err(e) = node_for_task.run().await {
            warn!("node listener stopped: {e:?}");
        }
    });

    // Give listener time to bind.
    tokio::time::sleep(Duration::from_millis(300)).await;

    node.join().await?;
    Ok(node)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .context("failed to init tracing subscriber")?;

    let args = Args::parse();

    let contents = fs::read_to_string(&args.requests)
        .with_context(|| format!("failed to read requests file: {}", args.requests.display()))?;
    let ops = parse_requests_file(&contents)?;

    info!(
        "Starting batch runner node at {} joining ring via {}",
        args.addr, args.bootstrap
    );

    // Start a single client node.
    let node = start_client_node(args.addr, args.bootstrap).await?;
    tokio::time::sleep(Duration::from_millis(args.join_grace_ms)).await;

    // Infer (best-effort) replication mode from wildcard query output.
    // - In eventual consistency (t=1) or no-replication, `query('*')` returns actual key/value pairs per node.
    // - In linearizability (t=0) implementation in this project, `query('*')` often includes per-node empty placeholders.
    // This is heuristic, but it lets us avoid touching the node implementation.
    let wildcard = node.query("*".to_string()).await;

    // Infer t:
    // if we see any entry that is an empty placeholder, treat as linearizability.
    let inferred_t = if wildcard.iter().any(|(_, vals)| vals.is_empty()) {
        0u8
    } else {
        1u8
    };

    let (mode_name, out_path) = match inferred_t {
        0 => ("linearizability (t=0)", args.out.clone()),
        1 => ("eventual consistency (t=1)", args.out.clone()),
        _ => unreachable!(),
    };

    info!("Detected mode: {mode_name}; writing query answers to {}", out_path.display());

    let mut out = String::new();
    out.push_str(&format!("mode: {mode_name}\n"));
    out.push('\n');

    for (idx, op) in ops.iter().enumerate() {
        match op {
            Op::Insert { key, value } => {
                node.insert(key.clone(), value.clone())
                    .await
                    .with_context(|| format!("{mode_name}: insert failed at op {}", idx + 1))?;
            }
            Op::Query { key } => {
                let res = node.query(key.clone()).await;
                out.push_str(&format!("op {}: query '{}'\n", idx + 1, key));
                out.push_str(&format_query_result(res));
                out.push('\n');
            }
        }

        if args.delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.delay_ms)).await;
        }
    }

    // Best-effort depart.
    if let Err(e) = node.depart(args.bootstrap).await {
        warn!("node depart failed (ignored): {e:?}");
    }

    fs::write(&out_path, out)
        .with_context(|| format!("failed writing {}", out_path.display()))?;

    info!("Done. Results written to {}.", out_path.display());

    Ok(())
}
