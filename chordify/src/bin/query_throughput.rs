//! Query throughput benchmark runner.
//!
//! This binary is meant to be started on a node after you have already
//! brought up a ring with the desired parameters (k, t).
//!
//! It will:
//! 1) Join the ring as a client node.
//! 2) Load all keys from a specific insert file and perform inserts.
//! 3) Wait 5 seconds (to let the system stabilize).
//! 4) Load all keys from a specific query file and issue all queries sequentially.
//! 5) Measure elapsed time and report read throughput.
//!
//! Notes:
//! - This runner prints ONLY query stats (not insert stats).
//! - No mode/ring-size inference.
//! - No extra queries (like `query "*"`).

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use clap::Parser;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use chordify::nodes::Node;

#[derive(Debug, Clone, Parser)]
#[command(name = "query_throughput")]
struct Args {
    /// Address for THIS node (must be a free ip:port).
    #[arg(long)]
    addr: SocketAddr,

    /// Bootstrap address of the already running ring.
    #[arg(long)]
    bootstrap: SocketAddr,

    /// Path to the insert file to replay before queries (one key per line).
    #[arg(long, default_value = "data/insert/insert_00_part.txt")]
    insert_file: PathBuf,

    /// Value to insert for each key.
    #[arg(long, default_value = "jsd12312")]
    insert_value: String,

    /// Path to the query file to replay after inserts (one key per line).
    #[arg(long, default_value = "data/queries/query_00.txt")]
    query_file: PathBuf,

    /// Optional: extra delay after joining (lets ring stabilize).
    #[arg(long, default_value_t = 500)]
    join_grace_ms: u64,

    /// Fixed delay (ms) before starting queries (default 5000ms).
    #[arg(long, default_value_t = 5000)]
    pre_query_wait_ms: u64,

    /// Optional: add a delay between queries in milliseconds.
    #[arg(long, default_value_t = 0)]
    query_delay_ms: u64,
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

fn load_lines_file(path: &Path) -> anyhow::Result<Vec<String>> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed reading {}", path.display()))?;

    let mut out = Vec::new();
    for raw in contents.lines() {
        let k = raw.trim();
        if k.is_empty() {
            continue;
        }
        out.push(k.to_string());
    }
    Ok(out)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .context("failed to init tracing subscriber")?;

    let args = Args::parse();

    if !args.insert_file.is_file() {
        return Err(anyhow!(
            "insert file not found: {}",
            args.insert_file.display()
        ));
    }
    if !args.query_file.is_file() {
        return Err(anyhow!(
            "query file not found: {}",
            args.query_file.display()
        ));
    }

    info!(
        "Starting query_throughput runner at {} joining ring via {}",
        args.addr, args.bootstrap
    );
    tokio::time::sleep(Duration::from_millis(args.join_grace_ms)).await;
    let node = start_client_node(args.addr, args.bootstrap).await?;

    tokio::time::sleep(Duration::from_millis(args.pre_query_wait_ms)).await;
    // 1) Inserts phase
    let insert_keys = load_lines_file(&args.insert_file)?;
    info!(
        "Loaded {} inserts from {}",
        insert_keys.len(),
        args.insert_file.display()
    );

    for key in insert_keys {
        node.insert(key, args.insert_value.clone()).await?;
    }

    // 2) Wait before queries
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // 3) Queries phase (stats only here)
    let query_keys = load_lines_file(&args.query_file)?;
    let query_ops = query_keys.len();

    info!(
        "Loaded {} queries from {}",
        query_ops,
        args.query_file.display()
    );

    let start = Instant::now();

    for key in query_keys {
        let _ = node.query(key).await;

    }

    let elapsed = start.elapsed();

    let results = node.query("*".to_string()).await;
    let mut i = 0;
    if results.is_empty() {
        println!("no entries found");
    } else {
        for (hash, vals) in results {
            if vals.is_empty() {
                //println!("key hash {}: <none>", hash);
            } else {
                //println!("key hash {}: {}", hash, vals.join(", "));
            }
            i += vals.len();
        }
    }
    println!("TOTAL QUERIED KEYS: {}", i);
    tokio::time::sleep(Duration::from_millis(args.pre_query_wait_ms)).await;

    // Best-effort depart.
    if let Err(e) = node.depart(args.bootstrap).await {
        warn!("node depart failed (ignored): {e:?}");
    }

    let secs = elapsed.as_secs_f64();
    let throughput = if secs > 0.0 {
        (query_ops as f64) / secs
    } else {
        f64::INFINITY
    };

    println!("query_ops={}", query_ops);
    println!("elapsed_ms={}", elapsed.as_millis());
    println!("throughput_queries_per_sec={:.3}", throughput);

    Ok(())
}