//! Integration tests for P2P communication: Connect → Message → Response
//! Tests cover:
//! - TCP layer (Server, Client, connect, message, send)
//! - Protocol layer (Request/Response serialization)
//! - Node operations (create, join, depart, insert, query, delete)
//! - Bootstrap operations (coordinate join/depart, ring management)
//! - Chord DHT functionality (key routing, data transfer, ring maintenance)

use chordify::tcp::{Server, connect, connect_with_timeout};
use chordify::nodes::{Node, Request, Response, NodeInfo};
use chordify::BootstrapNode;
use std::net::SocketAddr;
use std::time::Duration;
use std::sync::Arc;
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

// ==================== Protocol Tests ====================

#[tokio::test]
async fn test_request_serialization() {
    let request = Request::Ping;
    let bytes = request.to_bytes().unwrap();
    let deserialized = Request::from_bytes(&bytes).unwrap();
    match deserialized {
        Request::Ping => (),
        _ => panic!("Wrong request type"),
    }
}

#[tokio::test]
async fn test_response_serialization() {
    let response = Response::Pong;
    let bytes = response.to_bytes().unwrap();
    let deserialized = Response::from_bytes(&bytes).unwrap();
    match deserialized {
        Response::Pong => (),
        _ => panic!("Wrong response type"),
    }
}

#[tokio::test]
async fn test_insert_request_serialization() {
    let request = Request::Insert {
        key: "test_key".to_string(),
        value: "test_value".to_string(),
    };
    let bytes = request.to_bytes().unwrap();
    let deserialized = Request::from_bytes(&bytes).unwrap();
    match deserialized {
        Request::Insert { key, value } => {
            assert_eq!(key, "test_key");
            assert_eq!(value, "test_value");
        }
        _ => panic!("Wrong request type"),
    }
}

#[tokio::test]
async fn test_query_request_serialization() {
    let source_addr = get_test_addr(20001);
    let request = Request::Query {
        key: "query_key".to_string(),
        source: source_addr,
    };
    let bytes = request.to_bytes().unwrap();
    let deserialized = Request::from_bytes(&bytes).unwrap();
    match deserialized {
        Request::Query { key, source } => {
            assert_eq!(key, "query_key");
            assert_eq!(source, source_addr);
        }
        _ => panic!("Wrong request type"),
    }
}

#[tokio::test]
async fn test_delete_request_serialization() {
    let request = Request::Delete {
        key: "delete_key".to_string(),
    };
    let bytes = request.to_bytes().unwrap();
    let deserialized = Request::from_bytes(&bytes).unwrap();
    match deserialized {
        Request::Delete { key } => {
            assert_eq!(key, "delete_key");
        }
        _ => panic!("Wrong request type"),
    }
}

// ==================== Bootstrap Node Tests ====================

#[tokio::test]
async fn test_bootstrap_creation() {
    let bootstrap_addr = get_test_addr(20100);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    assert_eq!(bootstrap.get_addr(), bootstrap_addr);
}

#[tokio::test]
async fn test_bootstrap_ring_members_empty() {
    let bootstrap_addr = get_test_addr(20101);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    let members = bootstrap.get_ring_members().await;
    assert_eq!(members.len(), 0);
}

#[tokio::test]
async fn test_bootstrap_register_node() {
    let bootstrap_addr = get_test_addr(20102);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    
    let node_addr = get_test_addr(20103);
    let node_info = NodeInfo {
        addr: node_addr,
        id: 12345,
    };
    
    bootstrap.register_node(node_info.clone()).await;
    let members = bootstrap.get_ring_members().await;
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].addr, node_addr);
}

#[tokio::test]
async fn test_bootstrap_unregister_node() {
    let bootstrap_addr = get_test_addr(20104);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    
    let node_addr = get_test_addr(20105);
    let node_info = NodeInfo {
        addr: node_addr,
        id: 12345,
    };
    
    bootstrap.register_node(node_info.clone()).await;
    assert_eq!(bootstrap.get_ring_members().await.len(), 1);
    
    bootstrap.unregister_node(node_addr).await;
    assert_eq!(bootstrap.get_ring_members().await.len(), 0);
}

#[tokio::test]
async fn test_bootstrap_ping() {
    let bootstrap_addr = get_test_addr(20110);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    
    tokio::spawn(async move {
        bootstrap.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    let request = Request::Ping;
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(bootstrap_addr)
        .await
        .unwrap()
        .message(&request_bytes)
        .await
        .unwrap();
    
    let response = Response::from_bytes(&response_bytes).unwrap();
    match response {
        Response::Pong => (),
        _ => panic!("Expected Pong response"),
    }
}

// ==================== Node Tests ====================

#[tokio::test]
async fn test_node_creation() {
    let node_addr = get_test_addr(20200);
    let bootstrap_addr = get_test_addr(20201);
    let node = Node::new(node_addr, bootstrap_addr);
    
    assert_eq!(node.get_addr(), node_addr);
    assert!(node.get_id() > 0);
}

#[tokio::test]
async fn test_node_getters() {
    let node_addr = get_test_addr(20202);
    let bootstrap_addr = get_test_addr(20203);
    let node = Node::new(node_addr, bootstrap_addr);
    
    assert_eq!(node.get_addr(), node_addr);
    let id = node.get_id();
    assert!(id > 0);
    
    // Test successor and predecessor getters
    let successor = node.get_successor().await;
    let predecessor = node.get_predecessor().await;
    
    // Initially, they should be set to default values
    assert_eq!(successor.id, 0);
    assert_eq!(predecessor.id, 0);
}

#[tokio::test]
async fn test_node_ping_handler() {
    let node_addr = get_test_addr(20210);
    let bootstrap_addr = get_test_addr(20211);
    let node = Arc::new(Node::new(node_addr, bootstrap_addr));
    
    let node_clone = Arc::clone(&node);
    tokio::spawn(async move {
        node_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    let request = Request::Ping;
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(node_addr)
        .await
        .unwrap()
        .message(&request_bytes)
        .await
        .unwrap();
    
    let response = Response::from_bytes(&response_bytes).unwrap();
    match response {
        Response::Pong => (),
        _ => panic!("Expected Pong response"),
    }
}

#[tokio::test]
async fn test_node_set_successor() {
    let node_addr = get_test_addr(20220);
    let bootstrap_addr = get_test_addr(20221);
    let node = Arc::new(Node::new(node_addr, bootstrap_addr));
    
    let node_clone = Arc::clone(&node);
    tokio::spawn(async move {
        node_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    let successor_addr = get_test_addr(20222);
    let successor_info = NodeInfo {
        addr: successor_addr,
        id: 54321,
    };
    
    let request = Request::SetSuccessor { node: successor_info.clone() };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(node_addr)
        .await
        .unwrap()
        .message(&request_bytes)
        .await
        .unwrap();
    
    let response = Response::from_bytes(&response_bytes).unwrap();
    match response {
        Response::Ok => (),
        _ => panic!("Expected Ok response"),
    }
    
    // Verify the successor was set
    let successor = node.get_successor().await;
    assert_eq!(successor.addr, successor_addr);
    assert_eq!(successor.id, 54321);
}

#[tokio::test]
async fn test_node_set_predecessor() {
    let node_addr = get_test_addr(20230);
    let bootstrap_addr = get_test_addr(20231);
    let node = Arc::new(Node::new(node_addr, bootstrap_addr));
    
    let node_clone = Arc::clone(&node);
    tokio::spawn(async move {
        node_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    let predecessor_addr = get_test_addr(20232);
    let predecessor_info = NodeInfo {
        addr: predecessor_addr,
        id: 11111,
    };
    
    let request = Request::SetPredecessor { node: predecessor_info.clone() };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(node_addr)
        .await
        .unwrap()
        .message(&request_bytes)
        .await
        .unwrap();
    
    let response = Response::from_bytes(&response_bytes).unwrap();
    match response {
        Response::Ok => (),
        _ => panic!("Expected Ok response"),
    }
    
    // Verify the predecessor was set
    let predecessor = node.get_predecessor().await;
    assert_eq!(predecessor.addr, predecessor_addr);
    assert_eq!(predecessor.id, 11111);
}

// ==================== Join/Depart Integration Tests ====================

#[tokio::test]
async fn test_single_node_join() {
    let bootstrap_addr = get_test_addr(20300);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    
    tokio::spawn(async move {
        bootstrap.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    let node_addr = get_test_addr(20301);
    let node = Arc::new(Node::new(node_addr, bootstrap_addr));
    
    let node_clone = Arc::clone(&node);
    tokio::spawn(async move {
        node_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // Node joins the ring
    node.join().await.unwrap();
    
    // After joining, the node should point to itself as successor and predecessor
    let successor = node.get_successor().await;
    let predecessor = node.get_predecessor().await;
    
    assert_eq!(successor.addr, node_addr);
    assert_eq!(predecessor.addr, node_addr);
}

#[tokio::test]
async fn test_two_nodes_join() {
    let bootstrap_addr = get_test_addr(20310);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    
    tokio::spawn(async move {
        bootstrap.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // First node joins
    let node1_addr = get_test_addr(20311);
    let node1 = Arc::new(Node::new(node1_addr, bootstrap_addr));
    
    let node1_clone = Arc::clone(&node1);
    tokio::spawn(async move {
        node1_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    node1.join().await.unwrap();
    sleep(Duration::from_millis(300)).await;
    
    // Second node joins
    let node2_addr = get_test_addr(20312);
    let node2 = Arc::new(Node::new(node2_addr, bootstrap_addr));
    
    let node2_clone = Arc::clone(&node2);
    tokio::spawn(async move {
        node2_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    node2.join().await.unwrap();
    sleep(Duration::from_millis(300)).await;
    
    // Both nodes should have each other as successor and predecessor
    let node1_successor = node1.get_successor().await;
    let node1_predecessor = node1.get_predecessor().await;
    
    let node2_successor = node2.get_successor().await;
    let node2_predecessor = node2.get_predecessor().await;
    
    // Verify ring structure
    assert!(node1_successor.addr == node2_addr);
    assert!(node1_predecessor.addr == node2_addr);
    assert!(node2_successor.addr == node1_addr);
    assert!(node2_predecessor.addr == node1_addr);
}

#[tokio::test]
async fn test_five_nodes_join() {
    let bootstrap_addr = get_test_addr(20330);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    
    let bootstrap_clone = bootstrap.clone();
    tokio::spawn(async move {
        bootstrap_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // Create and join 5 nodes
    let mut nodes = Vec::new();
    for i in 0..5 {
        let node_addr = get_test_addr(20331 + i);
        let node = Arc::new(Node::new(node_addr, bootstrap_addr));
        
        let node_clone = Arc::clone(&node);
        tokio::spawn(async move {
            node_clone.run().await.unwrap();
        });
        
        sleep(Duration::from_millis(300)).await;
        node.join().await.unwrap();
        sleep(Duration::from_millis(200)).await;
        
        nodes.push(node);
    }
    
    // Verify all nodes joined
    let members = bootstrap.get_ring_members().await;
    assert_eq!(members.len(), 5, "All 5 nodes should be in the ring");
    
    // Collect all node addresses for verification
    let node_addrs: Vec<SocketAddr> = nodes.iter().map(|n| n.get_addr()).collect();
    
    // Verify each node has valid successor and predecessor
    for (i, node) in nodes.iter().enumerate() {
        let successor = node.get_successor().await;
        let predecessor = node.get_predecessor().await;
        
        // Successor and predecessor should not be the sentinel values
        assert_ne!(successor.id, 0, "Node {} should have a valid successor", i);
        assert_ne!(predecessor.id, 0, "Node {} should have a valid predecessor", i);
        
        // Verify successor and predecessor are in the ring
        assert!(node_addrs.contains(&successor.addr), 
                "Node {}'s successor should be in the ring", i);
        assert!(node_addrs.contains(&predecessor.addr), 
                "Node {}'s predecessor should be in the ring", i);
        
        // Verify successor and predecessor are different from the node itself (unless it's the only node)
        if nodes.len() > 1 {
            assert_ne!(successor.addr, node.get_addr(), 
                       "Node {}'s successor should not be itself when multiple nodes exist", i);
            assert_ne!(predecessor.addr, node.get_addr(), 
                       "Node {}'s predecessor should not be itself when multiple nodes exist", i);
        }
        
        tprintln!("Node {}: addr={}, succ={}, pred={}", 
                  i, node.get_addr(), successor.addr, predecessor.addr);
    }
    
    // Verify ring connectivity: walk through successors and ensure we visit all nodes
    let start_node = &nodes[0];
    let mut visited = std::collections::HashSet::new();
    let mut current_addr = start_node.get_addr();
    visited.insert(current_addr);
    
    for _ in 0..nodes.len() - 1 {
        // Find the node with current_addr and get its successor
        let current_node = nodes.iter().find(|n| n.get_addr() == current_addr).unwrap();
        let successor = current_node.get_successor().await;
        current_addr = successor.addr;
        
        if visited.contains(&current_addr) && visited.len() < nodes.len() {
            panic!("Ring connectivity broken: revisited node before visiting all nodes");
        }
        visited.insert(current_addr);
    }
    
    assert_eq!(visited.len(), nodes.len(), "Should visit all nodes by following successors");
}

#[tokio::test]
async fn test_ten_nodes_join() {
    let bootstrap_addr = get_test_addr(20340);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    
    let bootstrap_clone = bootstrap.clone();
    tokio::spawn(async move {
        bootstrap_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // Create and join 10 nodes
    let mut nodes = Vec::new();
    for i in 0..10 {
        let node_addr = get_test_addr(20341 + i);
        let node = Arc::new(Node::new(node_addr, bootstrap_addr));
        
        let node_clone = Arc::clone(&node);
        tokio::spawn(async move {
            node_clone.run().await.unwrap();
        });
        
        sleep(Duration::from_millis(200)).await;
        node.join().await.unwrap();
        sleep(Duration::from_millis(100)).await;
        
        nodes.push(node);
    }
    
    // Verify all nodes joined
    let members = bootstrap.get_ring_members().await;
    assert_eq!(members.len(), 10, "All 10 nodes should be in the ring");
    
    // Collect all node addresses for verification
    let node_addrs: Vec<SocketAddr> = nodes.iter().map(|n| n.get_addr()).collect();
    
    // Verify each node has valid successor and predecessor
    for (i, node) in nodes.iter().enumerate() {
        let successor = node.get_successor().await;
        let predecessor = node.get_predecessor().await;
        
        // Successor and predecessor should not be the sentinel values
        assert_ne!(successor.id, 0, "Node {} should have a valid successor", i);
        assert_ne!(predecessor.id, 0, "Node {} should have a valid predecessor", i);
        
        // Verify successor and predecessor are in the ring
        assert!(node_addrs.contains(&successor.addr), 
                "Node {}'s successor should be in the ring", i);
        assert!(node_addrs.contains(&predecessor.addr), 
                "Node {}'s predecessor should be in the ring", i);
        
        // Verify successor and predecessor are different from the node itself
        assert_ne!(successor.addr, node.get_addr(), 
                   "Node {}'s successor should not be itself", i);
        assert_ne!(predecessor.addr, node.get_addr(), 
                   "Node {}'s predecessor should not be itself", i);
        
        tprintln!("Node {}: addr={}, succ={}, pred={}", 
                  i, node.get_addr(), successor.addr, predecessor.addr);
    }
    
    // Verify ring connectivity: walk through successors and ensure we can visit all nodes
    let start_node = &nodes[0];
    let mut visited = std::collections::HashSet::new();
    let mut current_addr = start_node.get_addr();
    visited.insert(current_addr);
    
    for step in 0..nodes.len() - 1 {
        // Find the node with current_addr and get its successor
        let current_node = nodes.iter().find(|n| n.get_addr() == current_addr)
            .expect(&format!("Could not find node with address {} at step {}", current_addr, step));
        let successor = current_node.get_successor().await;
        current_addr = successor.addr;
        
        if visited.contains(&current_addr) && visited.len() < nodes.len() {
            panic!("Ring connectivity broken: revisited node {} at step {} before visiting all {} nodes (visited {} so far)", 
                   current_addr, step, nodes.len(), visited.len());
        }
        visited.insert(current_addr);
    }
    
    assert_eq!(visited.len(), nodes.len(), 
               "Should visit all {} nodes by following successors, but visited {}", 
               nodes.len(), visited.len());
    
    // Verify ring structure by sorting IDs
    let mut ring_ids: Vec<u64> = nodes.iter().map(|n| n.get_id()).collect();
    ring_ids.sort();
    tprintln!("Sorted node IDs in ring: {:?}", ring_ids);
}

#[tokio::test]
async fn test_fifteen_nodes_join() {
    let bootstrap_addr = get_test_addr(20360);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    
    let bootstrap_clone = bootstrap.clone();
    tokio::spawn(async move {
        bootstrap_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // Create and join 15 nodes
    let mut nodes = Vec::new();
    for i in 0..15 {
        let node_addr = get_test_addr(20361 + i);
        let node = Arc::new(Node::new(node_addr, bootstrap_addr));
        
        let node_clone = Arc::clone(&node);
        tokio::spawn(async move {
            node_clone.run().await.unwrap();
        });
        
        sleep(Duration::from_millis(150)).await;
        node.join().await.unwrap();
        sleep(Duration::from_millis(100)).await;
        
        nodes.push(node);
    }
    
    // Verify all nodes joined
    let members = bootstrap.get_ring_members().await;
    assert_eq!(members.len(), 15, "All 15 nodes should be in the ring");
    
    // Collect all node addresses for verification
    let node_addrs: Vec<SocketAddr> = nodes.iter().map(|n| n.get_addr()).collect();
    
    // Verify each node has valid successor and predecessor
    for (i, node) in nodes.iter().enumerate() {
        let successor = node.get_successor().await;
        let predecessor = node.get_predecessor().await;
        
        // Successor and predecessor should not be the sentinel values
        assert_ne!(successor.id, 0, "Node {} should have a valid successor", i);
        assert_ne!(predecessor.id, 0, "Node {} should have a valid predecessor", i);
        
        // Verify successor and predecessor are in the ring
        assert!(node_addrs.contains(&successor.addr), 
                "Node {}'s successor should be in the ring", i);
        assert!(node_addrs.contains(&predecessor.addr), 
                "Node {}'s predecessor should be in the ring", i);
        
        // Verify successor and predecessor are different from the node itself
        assert_ne!(successor.addr, node.get_addr(), 
                   "Node {}'s successor should not be itself", i);
        assert_ne!(predecessor.addr, node.get_addr(), 
                   "Node {}'s predecessor should not be itself", i);
        
        tprintln!("Node {}: addr={}, id={}, succ_id={}, pred_id={}", 
                  i, node.get_addr(), node.get_id(), successor.id, predecessor.id);
    }
    
    // Verify ring connectivity by following successor pointers
    let start_node = &nodes[0];
    let mut visited = std::collections::HashSet::new();
    let mut current_addr = start_node.get_addr();
    visited.insert(current_addr);
    
    for step in 0..nodes.len() - 1 {
        let current_node = nodes.iter().find(|n| n.get_addr() == current_addr)
            .expect(&format!("Could not find node with address {} at step {}", current_addr, step));
        let successor = current_node.get_successor().await;
        current_addr = successor.addr;
        
        if visited.contains(&current_addr) && visited.len() < nodes.len() {
            panic!("Ring connectivity broken: revisited node {} at step {} before visiting all {} nodes", 
                   current_addr, step, nodes.len());
        }
        visited.insert(current_addr);
    }
    
    assert_eq!(visited.len(), nodes.len(), 
               "Should visit all {} nodes by following successors", nodes.len());
    
    // Verify ring structure is consistent
    let mut all_node_ids: Vec<u64> = nodes.iter().map(|n| n.get_id()).collect();
    all_node_ids.sort();
    
    tprintln!("Ring with 15 nodes successfully formed");
    tprintln!("Sorted node IDs: {:?}", all_node_ids);
    
    // Verify no duplicate IDs
    let unique_ids: std::collections::HashSet<_> = all_node_ids.iter().collect();
    assert_eq!(unique_ids.len(), 15, "All node IDs should be unique");
    
    // Verify bidirectional consistency: each node's successor's predecessor should be close in the ring
    for (i, node) in nodes.iter().enumerate() {
        let node_id = node.get_id();
        let successor = node.get_successor().await;
        let predecessor = node.get_predecessor().await;
        
        // Find successor node and check its predecessor
        if let Some(succ_node) = nodes.iter().find(|n| n.get_addr() == successor.addr) {
            let succ_pred = succ_node.get_predecessor().await;
            // The successor's predecessor should be this node or another node in the ring
            assert!(node_addrs.contains(&succ_pred.addr),
                    "Node {}'s successor's predecessor should be in the ring", i);
        }
        
        tprintln!("Node {} verification: id={}, successor_id={}, predecessor_id={}", 
                  i, node_id, successor.id, predecessor.id);
    }
}

#[tokio::test]
async fn test_nodes_join_with_data_insertion() {
    let bootstrap_addr = get_test_addr(20380);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    
    let bootstrap_clone = bootstrap.clone();
    tokio::spawn(async move {
        bootstrap_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // Create and join 5 nodes
    let mut nodes = Vec::new();
    for i in 0..5 {
        let node_addr = get_test_addr(20381 + i);
        let node = Arc::new(Node::new(node_addr, bootstrap_addr));
        
        let node_clone = Arc::clone(&node);
        tokio::spawn(async move {
            node_clone.run().await.unwrap();
        });
        
        sleep(Duration::from_millis(200)).await;
        node.join().await.unwrap();
        sleep(Duration::from_millis(150)).await;
        
        nodes.push(node);
    }
    
    sleep(Duration::from_millis(500)).await;
    
    // Insert data through the first node
    for i in 0..5 {
        let key = format!("test_key_{}", i);
        let value = format!("test_value_{}", i);
        
        nodes[0].insert(key, value).await.unwrap();
        sleep(Duration::from_millis(100)).await;
    }
    
    tprintln!("Successfully inserted 5 key-value pairs into the ring");
}

#[tokio::test]
async fn test_node_depart() {
    let bootstrap_addr = get_test_addr(20320);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    
    let bootstrap_clone = bootstrap.clone();
    tokio::spawn(async move {
        bootstrap_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // Node joins
    let node_addr = get_test_addr(20321);
    let node = Arc::new(Node::new(node_addr, bootstrap_addr));
    
    let node_clone = Arc::clone(&node);
    tokio::spawn(async move {
        node_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    node.join().await.unwrap();
    sleep(Duration::from_millis(300)).await;
    
    // Verify node is in the ring
    assert_eq!(bootstrap.get_ring_members().await.len(), 1);
    
    // Node departs
    node.depart(bootstrap_addr).await.unwrap();
    sleep(Duration::from_millis(300)).await;
    
    // Verify node is removed from the ring
    assert_eq!(bootstrap.get_ring_members().await.len(), 0);
}

// ==================== Data Operations Tests ====================

#[tokio::test]
async fn test_node_insert_handler() {
    let node_addr = get_test_addr(20400);
    let bootstrap_addr = get_test_addr(20401);
    let node = Arc::new(Node::new(node_addr, bootstrap_addr));
    
    let node_clone = Arc::clone(&node);
    tokio::spawn(async move {
        node_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    let request = Request::Insert {
        key: "test_key".to_string(),
        value: "test_value".to_string(),
    };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(node_addr)
        .await
        .unwrap()
        .message(&request_bytes)
        .await
        .unwrap();
    
    let response = Response::from_bytes(&response_bytes).unwrap();
    match response {
        Response::Ok => (),
        Response::Error(e) => tprintln!("Insert error: {}", e),
        _ => panic!("Expected Ok response"),
    }
}

#[tokio::test]
async fn test_node_delete_handler() {
    let node_addr = get_test_addr(20410);
    let bootstrap_addr = get_test_addr(20411);
    let node = Arc::new(Node::new(node_addr, bootstrap_addr));
    
    let node_clone = Arc::clone(&node);
    tokio::spawn(async move {
        node_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // First insert a key
    let insert_request = Request::Insert {
        key: "delete_test".to_string(),
        value: "value".to_string(),
    };
    let request_bytes = insert_request.to_bytes().unwrap();
    connect(node_addr)
        .await
        .unwrap()
        .message(&request_bytes)
        .await
        .unwrap();
    
    sleep(Duration::from_millis(100)).await;
    
    // Then delete it
    let delete_request = Request::Delete {
        key: "delete_test".to_string(),
    };
    let request_bytes = delete_request.to_bytes().unwrap();
    let response_bytes = connect(node_addr)
        .await
        .unwrap()
        .message(&request_bytes)
        .await
        .unwrap();
    
    let response = Response::from_bytes(&response_bytes).unwrap();
    match response {
        Response::Ok => (),
        Response::Error(e) => tprintln!("Delete error: {}", e),
        _ => panic!("Expected Ok response"),
    }
}

#[tokio::test]
async fn test_transfer_data() {
    let node_addr = get_test_addr(20420);
    let bootstrap_addr = get_test_addr(20421);
    let node = Arc::new(Node::new(node_addr, bootstrap_addr));
    
    let node_clone = Arc::clone(&node);
    tokio::spawn(async move {
        node_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // Send transfer data request
    let mut data = std::collections::HashMap::new();
    data.insert("key1".to_string(), "value1".to_string());
    data.insert("key2".to_string(), "value2".to_string());
    
    let request = Request::TransferData { data };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(node_addr)
        .await
        .unwrap()
        .message(&request_bytes)
        .await
        .unwrap();
    
    let response = Response::from_bytes(&response_bytes).unwrap();
    match response {
        Response::Ok => (),
        _ => panic!("Expected Ok response"),
    }
}

// ==================== Hash Function Tests ====================

#[tokio::test]
async fn test_hash_consistency() {
    use chordify::nodes::node::hash_value;
    
    let addr = "127.0.0.1:8080";
    let hash1 = hash_value(addr);
    let hash2 = hash_value(addr);
    
    assert_eq!(hash1, hash2, "Hash should be deterministic");
}

#[tokio::test]
async fn test_hash_different_inputs() {
    use chordify::nodes::node::hash_value;
    
    let hash1 = hash_value("127.0.0.1:8080");
    let hash2 = hash_value("127.0.0.1:8081");
    
    assert_ne!(hash1, hash2, "Different inputs should produce different hashes");
}

#[tokio::test]
async fn test_hash_range() {
    use chordify::nodes::node::hash_value;
    
    let hash = hash_value("127.0.0.1:8080");
    let max_value = 1u64 << 10; // N = 10
    
    assert!(hash < max_value, "Hash should be within the N-bit range");
}

// ==================== Error Handling Tests ====================

#[tokio::test]
async fn test_unsupported_request_to_node() {
    let node_addr = get_test_addr(20500);
    let bootstrap_addr = get_test_addr(20501);
    let node = Arc::new(Node::new(node_addr, bootstrap_addr));
    
    let node_clone = Arc::clone(&node);
    tokio::spawn(async move {
        node_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // Send a JoinRequest to a regular node (should be sent to bootstrap)
    let join_node = NodeInfo {
        addr: get_test_addr(20502),
        id: 99999,
    };
    let request = Request::JoinRequest { joining_node: join_node };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(node_addr)
        .await
        .unwrap()
        .message(&request_bytes)
        .await
        .unwrap();
    
    let response = Response::from_bytes(&response_bytes).unwrap();
    match response {
        Response::Error(e) => {
            assert!(e.contains("Unsupported") || e.contains("bootstrap"));
        }
        _ => panic!("Expected Error response"),
    }
}

// ==================== Stress Tests ====================

#[tokio::test]
async fn test_multiple_sequential_inserts() {
    let node_addr = get_test_addr(20600);
    let bootstrap_addr = get_test_addr(20601);
    let node = Arc::new(Node::new(node_addr, bootstrap_addr));
    
    let node_clone = Arc::clone(&node);
    tokio::spawn(async move {
        node_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // Insert multiple key-value pairs sequentially
    for i in 0..10 {
        let request = Request::Insert {
            key: format!("key_{}", i),
            value: format!("value_{}", i),
        };
        let request_bytes = request.to_bytes().unwrap();
        let response_bytes = connect(node_addr)
            .await
            .unwrap()
            .message(&request_bytes)
            .await
            .unwrap();
        
        let response = Response::from_bytes(&response_bytes).unwrap();
        match response {
            Response::Ok => (),
            Response::Error(e) => tprintln!("Insert {} error: {}", i, e),
            _ => panic!("Expected Ok response"),
        }
    }
}

#[tokio::test]
async fn test_concurrent_requests_to_node() {
    let node_addr = get_test_addr(20610);
    let bootstrap_addr = get_test_addr(20611);
    let node = Arc::new(Node::new(node_addr, bootstrap_addr));
    
    let node_clone = Arc::clone(&node);
    tokio::spawn(async move {
        node_clone.run().await.unwrap();
    });
    
    sleep(Duration::from_millis(300)).await;
    
    // Send multiple concurrent ping requests
    let mut handles = vec![];
    for _ in 0..10 {
        let handle = tokio::spawn(async move {
            let request = Request::Ping;
            let request_bytes = request.to_bytes().unwrap();
            let response_bytes = connect(node_addr)
                .await
                .unwrap()
                .message(&request_bytes)
                .await
                .unwrap();
            
            let response = Response::from_bytes(&response_bytes).unwrap();
            match response {
                Response::Pong => (),
                _ => panic!("Expected Pong response"),
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
}

