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

    /// Directory containing `insert_*.txt` files.
    #[arg(long, default_value = "data/insert")]
    insert_dir: PathBuf,

    /// Value to insert for each key.
    #[arg(long, default_value = "1")]
    value: String,

    /// If true, skip duplicate keys across files (benchmark unique keys).
    /// If false, insert everything as encountered.
    #[arg(long, default_value_t = true)]
    dedup: bool,

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

fn list_insert_files(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .with_context(|| format!("failed to read insert dir {}", dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|name| name.starts_with("insert_") && name.ends_with(".txt"))
        })
        .collect();

    files.sort();
    Ok(files)
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

    let files = list_insert_files(&args.insert_dir)?;
    if files.is_empty() {
        return Err(anyhow!(
            "no insert_*.txt files found under {}",
            args.insert_dir.display()
        ));
    }

    info!(
        "Starting insert throughput runner at {} joining ring via {}",
        args.addr, args.bootstrap
    );

    let node = start_client_node(args.addr, args.bootstrap).await?;
    tokio::time::sleep(Duration::from_millis(args.join_grace_ms)).await;

    // Load all keys.
    let mut keys: Vec<String> = Vec::new();
    for p in &files {
        keys.extend(load_keys_from_file(p)?);
    }

    let total_loaded = keys.len();

    if args.dedup {
        keys.sort();
        keys.dedup();
    }

    let total_to_insert = keys.len();

    info!(
        "Loaded {} keys ({} after dedup={}) from {} files",
        total_loaded,
        total_to_insert,
        args.dedup,
        files.len()
    );

    let start = Instant::now();

    for (i, key) in keys.iter().enumerate() {
        node.insert(key.clone(), args.value.clone())
            .await
            .with_context(|| format!("insert failed at {} / {} (key='{}')", i + 1, total_to_insert, key))?;

        if args.delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.delay_ms)).await;
        }
    }

    let elapsed = start.elapsed();

    // Best-effort depart.
    if let Err(e) = node.depart(args.bootstrap).await {
        warn!("node depart failed (ignored): {e:?}");
    }

    let secs = elapsed.as_secs_f64();
    let throughput = if secs > 0.0 {
        (total_to_insert as f64) / secs
    } else {
        f64::INFINITY
    };

    println!("inserted_keys={}", total_to_insert);
    println!("elapsed_ms={}", elapsed.as_millis());
    println!("throughput_inserts_per_sec={:.3}", throughput);

    Ok(())
}
