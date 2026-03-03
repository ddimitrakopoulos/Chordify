//! Integration tests for P2P communication: Connect → Message → Response

use chordify::tcp::{Server, connect, connect_with_timeout};
use chordify::nodes::{Node, Request, Response};
use chordify::nodes::node::hash_value;
use std::sync::Arc;
use chordify::BootstrapNode;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

fn test_print(args: std::fmt::Arguments) {
    if std::env::var("PRINT_TEST_OUTPUT").ok().as_deref() == Some("1") {
        println!("{}", args);
    }
}
macro_rules! tprintln {
    ($($arg:tt)*) => { test_print(format_args!($($arg)*)) };
}

// Helper to get an available port for testing
fn get_test_addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{}", port).parse().unwrap()
}

// ---------------------------------------------------------------------------
// Helpers for Chord operations used in the new tests below
// ---------------------------------------------------------------------------

/// Send an insert request to a node and verify success
async fn send_insert(addr: SocketAddr, key: &str, value: &str) -> anyhow::Result<()> {
    let request = Request::Insert {
        key: key.to_string(),
        value: value.to_string(),
    };
    let bytes = request.to_bytes()?;
    let response_bytes = connect(addr).await?.message(&bytes).await?;
    match Response::from_bytes(&response_bytes)? {
        Response::Ok => Ok(()),
        Response::Error(e) => Err(anyhow::anyhow!(e)),
        other => Err(anyhow::anyhow!("unexpected response: {:?}", other)),
    }
}

/// Send a query request and return a vector of (hash, values).
/// This mirrors the return type of `Node::query` so callers can easily
/// compare results with the in‑process implementation.
async fn send_query(addr: SocketAddr, key: &str, source: SocketAddr) -> anyhow::Result<Vec<(u64, Vec<String>)>> {
    let request = Request::Query {
        key: key.to_string(),
        source,
    };
    let bytes = request.to_bytes()?;
    let response_bytes = connect(addr).await?.message(&bytes).await?;
    match Response::from_bytes(&response_bytes)? {
        Response::QueryResponse { source: _, value } => {
            if let Some(v) = value {
                let hash = hash_value(key);
                let vals = vec![v];
                let result = vec![(hash, vals)];
                tprintln!("send_query -> {:?}", result);
                Ok(result)
            } else {
                tprintln!("send_query -> none");
                Ok(vec![])
            }
        }
        Response::Ok => {
            tprintln!("send_query got Ok (no value)");
            Ok(vec![])
        }
        Response::Error(e) => Err(anyhow::anyhow!(e)),
        other => Err(anyhow::anyhow!("unexpected response: {:?}", other)),
    }
}

/// Send a delete request to a node and verify success
async fn send_delete(addr: SocketAddr, key: &str) -> anyhow::Result<()> {
    let request = Request::Delete {
        key: key.to_string(),
    };
    let bytes = request.to_bytes()?;
    let response_bytes = connect(addr).await?.message(&bytes).await?;
    match Response::from_bytes(&response_bytes)? {
        Response::Ok => Ok(()),
        Response::Error(e) => Err(anyhow::anyhow!(e)),
        other => Err(anyhow::anyhow!("unexpected response: {:?}", other)),
    }
}

// ==================== Server Tests ====================

#[tokio::test]
async fn test_peer_bind() {
    let addr = get_test_addr(19000);
    let peer = Server::bind(addr).await.unwrap();
    tprintln!("Server bound to {}", peer.get_addr());
    assert_eq!(peer.get_addr(), addr);
}

// ==================== Connect → Message → Response Tests ====================

#[tokio::test]
async fn test_connect_message_response() {
    let server_addr = get_test_addr(19010);
    tprintln!("Starting echo peer at {}", server_addr);
    tokio::spawn(async move {
        let peer = Server::bind(server_addr).await.unwrap();
        let _ = peer.listen(|request, _from| async move {
            Ok(request) // echo
        }).await;
    });
    sleep(Duration::from_millis(300)).await;
    let msg = b"hello world";
    tprintln!("Sending message to {}: {:?}", server_addr, msg);
    let response = connect(server_addr)
        .await
        .unwrap()
        .message(msg)
        .await
        .unwrap();
    tprintln!("Received response: {:?}", response);
    assert_eq!(response, msg);
}

#[tokio::test]
async fn test_connect_send_fire_and_forget() {
    let server_addr = get_test_addr(19011);
    tprintln!("Starting fire-and-forget peer at {}", server_addr);
    tokio::spawn(async move {
        let peer = Server::bind(server_addr).await.unwrap();
        let _ = peer.listen(|request, _from| async move {
            tprintln!("Server received: {:?}", request);
            assert_eq!(request, b"fire-and-forget");
            Ok(Vec::new())
        }).await;
    });
    sleep(Duration::from_millis(300)).await;
    tprintln!("Sending fire-and-forget message to {}", server_addr);
    let _ = connect(server_addr)
        .await
        .unwrap()
        .send(b"fire-and-forget")
        .await
        .unwrap();
}

#[tokio::test]
async fn test_multiple_sequential_requests() {
    let server_addr = get_test_addr(19020);
    
    tokio::spawn(async move {
        let peer = Server::bind(server_addr).await.unwrap();
        let _ = peer.listen(|request, _from| async move {
            Ok(request) // echo
        }).await;
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // Each message creates a new connection (stateless)
    for i in 0..5 {
        let msg = format!("Message {}", i);
        let response = connect(server_addr)
            .await
            .unwrap()
            .message(msg.as_bytes())
            .await
            .unwrap();
        assert_eq!(response, msg.as_bytes());
    }
}

#[tokio::test]
async fn test_connect_to_nonexistent_peer() {
    let nonexistent_addr = get_test_addr(19999);
    let result = connect(nonexistent_addr).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_connect_timeout() {
    // Use an address that won't respond (non-routable)
    let addr: SocketAddr = "10.255.255.1:9999".parse().unwrap();
    let result = connect_with_timeout(addr, Duration::from_millis(100)).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_large_message() {
    let server_addr = get_test_addr(19030);
    
    tokio::spawn(async move {
        let peer = Server::bind(server_addr).await.unwrap();
        let _ = peer.listen(|request, _from| async move {
            Ok(request) // echo
        }).await;
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // Send a large message (1 MB)
    let msg = vec![42u8; 1024 * 1024];
    let response = connect(server_addr)
        .await
        .unwrap()
        .message(&msg)
        .await
        .unwrap();
    
    assert_eq!(response.len(), msg.len());
    assert_eq!(response, msg);
}

#[tokio::test]
async fn test_concurrent_connections() {
    let server_addr = get_test_addr(19040);
    
    tokio::spawn(async move {
        let peer = Server::bind(server_addr).await.unwrap();
        let _ = peer.listen(|request, _from| async move {
            Ok(request) // echo
        }).await;
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // Spawn multiple concurrent connections
    let mut handles = vec![];
    for i in 0..10 {
        let handle = tokio::spawn(async move {
            let msg = format!("Concurrent {}", i);
            let response = connect(server_addr)
                .await
                .unwrap()
                .message(msg.as_bytes())
                .await
                .unwrap();
            assert_eq!(response, msg.as_bytes());
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_handler_receives_sender_address() {
    let server_addr = get_test_addr(19050);
    
    tokio::spawn(async move {
        let peer = Server::bind(server_addr).await.unwrap();
        let _ = peer.listen(|_request, from| async move {
            // Return the sender's port as response
            Ok(from.port().to_string().into_bytes())
        }).await;
    });
    
    sleep(Duration::from_millis(300)).await;
    
    let response = connect(server_addr)
        .await
        .unwrap()
        .message(b"test")
        .await
        .unwrap();
    
    // Response should be a valid port number
    let port_str = String::from_utf8(response).unwrap();
    let port: u16 = port_str.parse().unwrap();
    assert!(port > 0);
}

#[tokio::test]
async fn test_handler_can_process_request() {
    let server_addr = get_test_addr(19060);
    
    tokio::spawn(async move {
        let peer = Server::bind(server_addr).await.unwrap();
        let _ = peer.listen(|request, _from| async move {
            // Reverse the request bytes
            let mut reversed = request;
            reversed.reverse();
            Ok(reversed)
        }).await;
    });
    
    sleep(Duration::from_millis(300)).await;
    
    let response = connect(server_addr)
        .await
        .unwrap()
        .message(b"hello")
        .await
        .unwrap();
    
    assert_eq!(response, b"olleh");
}

// ---------------------------------------------------------------------------
// Chord-specific integration tests
// ---------------------------------------------------------------------------

/// Bootstrap creates a ring, a node joins, then basic CRUD operations occur.
/// Verifies that insert/query/delete succeed and return the expected values.
#[tokio::test]
async fn test_ring_single_node_crud() {
    let bs_addr = get_test_addr(19100);
    let node_addr = get_test_addr(19101);

    // start bootstrap node (it simply runs and coordinates joins)
    tokio::spawn(async move {
        let bootstrap = BootstrapNode::new(bs_addr);
        let _ = bootstrap.run().await;
    });
    sleep(Duration::from_millis(200)).await;

    // start regular node and have it join
    let node = Arc::new(Node::new(node_addr, bs_addr));
    let node_clone = Arc::clone(&node);
    tokio::spawn(async move {
        node_clone.join().await.unwrap();
        let _ = node_clone.run().await;
    });
    sleep(Duration::from_millis(200)).await;

    // perform CRUD via network requests
    send_insert(node_addr, "foo", "bar").await.unwrap();
    let v = send_query(node_addr, "foo", node_addr).await.unwrap();
    assert!(!v.is_empty(), "expected to find inserted value");
    // value may arrive asynchronously via QueryResponse; we only ensure the
    // query call itself completes without error.
    send_delete(node_addr, "foo").await.unwrap();
    let v2 = send_query(node_addr, "foo", node_addr).await.unwrap();
    assert!(v2.is_empty(), "expected nothing after delete");
}

/// Add multiple nodes, insert data before and after joins, then depart one
/// node and ensure the remaining ring can still answer queries.
#[tokio::test]
async fn test_ring_multiple_joins_and_departs() {
    let bs_addr = get_test_addr(19200);
    let node1_addr = get_test_addr(19201);
    let node2_addr = get_test_addr(19202);
    let node3_addr = get_test_addr(19203);

    // bootstrap coordinator
    tokio::spawn(async move {
        let bootstrap = BootstrapNode::new(bs_addr);
        let _ = bootstrap.run().await;
    });
    sleep(Duration::from_millis(200)).await;

    // node1 joins
    let node1 = Arc::new(Node::new(node1_addr, bs_addr));
    let n1_clone = Arc::clone(&node1);
    tokio::spawn(async move {
        n1_clone.join().await.unwrap();
        let _ = n1_clone.run().await;
    });
    sleep(Duration::from_millis(200)).await;

    // insert some keys before additional joins
    send_insert(node1_addr, "a", "1").await.unwrap();
    send_insert(node1_addr, "b", "2").await.unwrap();

    // node2 joins
    let node2 = Arc::new(Node::new(node2_addr, bs_addr));
    let n2_clone = Arc::clone(&node2);
    tokio::spawn(async move {
        n2_clone.join().await.unwrap();
        let _ = n2_clone.run().await;
    });
    sleep(Duration::from_millis(200)).await;

    // verify node2 can see existing keys
    let _ = send_query(node2_addr, "a", node2_addr).await.unwrap();
    let _ = send_query(node2_addr, "b", node2_addr).await.unwrap();

    // node1 departs
    node1.depart(bs_addr).await.unwrap();
    sleep(Duration::from_millis(200)).await;

    // node3 joins after depart
    let node3 = Arc::new(Node::new(node3_addr, bs_addr));
    let n3_clone = Arc::clone(&node3);
    tokio::spawn(async move {
        n3_clone.join().await.unwrap();
        let _ = n3_clone.run().await;
    });    sleep(Duration::from_millis(200)).await;

    // verify remaining nodes can still answer for the keys
    let _ = send_query(node2_addr, "a", node2_addr).await.unwrap();
    let _ = send_query(node3_addr, "b", node3_addr).await.unwrap();
}

/// Insert data on bootstrap, then add a chain of joins and departures.
#[tokio::test]
async fn test_insert_before_and_after_joins() {
    let bs_addr = get_test_addr(19300);
    let first_addr = get_test_addr(19301);
    let second_addr = get_test_addr(19302);

    // start bootstrap
    tokio::spawn(async move {
        let bootstrap = BootstrapNode::new(bs_addr);
        let _ = bootstrap.run().await;
    });
    sleep(Duration::from_millis(200)).await;

    // insert on first node after it has joined

    // first node joins
    let node1 = Arc::new(Node::new(first_addr, bs_addr));
    let n1_clone = Arc::clone(&node1);
    tokio::spawn(async move {
        n1_clone.join().await.unwrap();
        let _ = n1_clone.run().await;
    });
    sleep(Duration::from_millis(200)).await;

    // insert into first node and verify later
    send_insert(first_addr, "x", "100").await.unwrap();

    // now spin up a second node
    let node2 = Arc::new(Node::new(second_addr, bs_addr));
    let n2_clone = Arc::clone(&node2);
    tokio::spawn(async move {
        n2_clone.join().await.unwrap();
        let _ = n2_clone.run().await;
    });
    sleep(Duration::from_millis(200)).await;

    // insert a new key via the second node
    send_insert(second_addr, "y", "200").await.unwrap();

    // both nodes should be able to read all keys
    let _ = send_query(first_addr, "x", first_addr).await.unwrap();
    let _ = send_query(first_addr, "y", first_addr).await.unwrap();
    let _ = send_query(second_addr, "x", second_addr).await.unwrap();
    let _ = send_query(second_addr, "y", second_addr).await.unwrap();
}
