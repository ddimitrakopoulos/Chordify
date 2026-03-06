//! Insert throughput benchmark runner.
//!
//! This binary is meant to be started on the *last node* after you have already
//! brought up a 10-node ring with the desired parameters (k, t).
//!
//! It will:
//! 1) Join the ring as a client node.
//! 2) Load all song names from `data/insert/*.txt`.
//! 3) Insert them with value `"1"`.
//! 4) Measure elapsed time and report write throughput.
//!
//! Important constraints:
//! - No mode/ring-size inference.
//! - No extra queries (like `query "*"`).
//!
//! Typical usage (example):
//! - ensure ring (10 nodes) is already running with desired k/t
//! - run: insert_throughput --addr 127.0.0.1:5009 --bootstrap 127.0.0.1:4000

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
#[command(name = "insert_throughput")]
struct Args {
    /// Address for THIS node (must be a free ip:port).
    #[arg(long)]
    addr: SocketAddr,

    /// Bootstrap address of the already running ring.
    #[arg(long)]
    bootstrap: SocketAddr,

    /// Path to the insert file to replay (one key per line).
    #[arg(long, default_value = "data/insert/insert_00_part.txt")]
    insert_file: PathBuf,

    /// Value to insert for each key.
    #[arg(long, default_value = "1skkapmt")]
    value: String,

    /// Optional: extra delay after joining (lets ring stabilize).
    #[arg(long, default_value_t = 500)]
    join_grace_ms: u64,

    /// Optional: add a delay between inserts in milliseconds.
    #[arg(long, default_value_t = 0)]
    delay_ms: u64,
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

fn load_keys_from_file(path: &Path) -> anyhow::Result<Vec<String>> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed reading {}", path.display()))?;

    let mut keys = Vec::new();
    for (i, raw) in contents.lines().enumerate() {
        let k = raw.trim();
        if k.is_empty() {
            continue;
        }
        // Disallow commas/newlines in keys (workload uses one key per line).
        if k.contains('\n') {
            return Err(anyhow!(
                "invalid key at {}:{} contains newline",
                path.display(),
                i + 1
            ));
        }
        keys.push(k.to_string());
    }
    Ok(keys)
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

    info!(
        "Starting insert throughput runner at {} joining ring via {}",
        args.addr, args.bootstrap
    );

    let node = start_client_node(args.addr, args.bootstrap).await?;
    tokio::time::sleep(Duration::from_millis(args.join_grace_ms)).await;

    let keys = load_keys_from_file(&args.insert_file)?;
    let insert_ops = keys.len();

    info!(
        "Loaded {} insert ops from {}",
        insert_ops,
        args.insert_file.display()
    );

    //sleep for 5 seconds to let the ring stabilize after join before starting insert
    tokio::time::sleep(Duration::from_millis(5000)).await;

    let start = Instant::now();

    for (i, key) in keys.into_iter().enumerate() {
        node.insert(key.clone(), args.value.clone())
            .await
            .with_context(|| format!(
                "insert failed at {} / {} (key='{}')",
                i + 1,
                insert_ops,
                key
            ))?;

        if args.delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.delay_ms)).await;
        }
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
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Best-effort depart.
    if let Err(e) = node.depart(args.bootstrap).await {
        warn!("node depart failed (ignored): {e:?}");
    }

    let secs = elapsed.as_secs_f64();
    let throughput = if secs > 0.0 {
        (insert_ops as f64) / secs
    } else {
        f64::INFINITY
    };

    println!("insert_ops={}", insert_ops);
    println!("elapsed_ms={}", elapsed.as_millis());
    println!("throughput_inserts_per_sec={:.3}", throughput);

    Ok(())
}