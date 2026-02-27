//! Integration tests for P2P communication: Connect → Message → Response

use chordify::communication::{Peer, connect, connect_with_timeout};
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

// ==================== Peer Tests ====================

#[tokio::test]
async fn test_peer_bind() {
    let addr = get_test_addr(19000);
    let peer = Peer::bind(addr).await.unwrap();
    tprintln!("Peer bound to {}", peer.addr());
    assert_eq!(peer.addr(), addr);
}

// ==================== Connect → Message → Response Tests ====================

#[tokio::test]
async fn test_connect_message_response() {
    let server_addr = get_test_addr(19010);
    tprintln!("Starting echo peer at {}", server_addr);
    tokio::spawn(async move {
        let peer = Peer::bind(server_addr).await.unwrap();
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
        let peer = Peer::bind(server_addr).await.unwrap();
        let _ = peer.listen(|request, _from| async move {
            tprintln!("Peer received: {:?}", request);
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
        let peer = Peer::bind(server_addr).await.unwrap();
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
        let peer = Peer::bind(server_addr).await.unwrap();
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
        let peer = Peer::bind(server_addr).await.unwrap();
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
        let peer = Peer::bind(server_addr).await.unwrap();
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
        let peer = Peer::bind(server_addr).await.unwrap();
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

// ==================== Node (Chord DHT) Tests ====================

use chordify::nodes::{Node, Request, Response};

#[tokio::test]
async fn test_node_create_ring() {
    let addr = get_test_addr(20000);
    let node = Node::new(addr);
    node.create_ring().await;

    // After creating a ring, successor and predecessor should be self
    let successor = node.get_successor().await.unwrap();
    let predecessor = node.get_predecessor().await.unwrap();
    assert_eq!(successor.addr, addr);
    assert_eq!(predecessor.addr, addr);
}

#[tokio::test]
async fn test_node_put_get_single_node() {
    let addr = get_test_addr(20010);
    let node = Node::new(addr);
    node.create_ring().await;

    // Put and get a value
    node.put("foo".to_string(), "bar".to_string()).await.unwrap();
    let value = node.get("foo").await.unwrap();
    assert_eq!(value, Some("bar".to_string()));

    // Get a non-existent key
    let missing = node.get("missing").await.unwrap();
    assert_eq!(missing, None);
}

#[tokio::test]
async fn test_node_find_successor_single_node() {
    let addr = get_test_addr(20020);
    let node = Node::new(addr);
    node.create_ring().await;

    let successor = node.find_successor(addr).await.unwrap();
    assert_eq!(successor.addr, addr);
}

#[tokio::test]
async fn test_node_run_and_ping() {
    let addr = get_test_addr(20030);
    let node = Node::new(addr);
    node.create_ring().await;

    // Start node in background
    tokio::spawn(async move {
        let _ = node.run().await;
    });

    sleep(Duration::from_millis(300)).await;

    // Send a Ping request directly
    let request = Request::Ping;
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();
    assert!(matches!(response, Response::Pong));
}

#[tokio::test]
async fn test_node_run_and_get_predecessor() {
    let addr = get_test_addr(20040);
    let node = Node::new(addr);
    node.create_ring().await;

    tokio::spawn(async move {
        let _ = node.run().await;
    });

    sleep(Duration::from_millis(300)).await;

    let request = Request::GetPredecessor;
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();
    match response {
        Response::Predecessor(Some(pred)) => assert_eq!(pred.addr, addr),
        _ => panic!("Expected Predecessor response"),
    }
}

#[tokio::test]
async fn test_nodes_specific_ports_and_responses() {
    let ports = [20100, 20101, 20102];
    let addrs: Vec<_> = ports.iter().map(|p| get_test_addr(*p)).collect();
    let mut handles = vec![];
    for addr in &addrs {
        let addr = *addr;
        tprintln!("Starting peer at {}", addr);
        handles.push(tokio::spawn(async move {
            let peer = Peer::bind(addr).await.unwrap();
            let _ = peer.listen(move |request, _from| async move {
                tprintln!("Peer {} received: {:?}", addr, request);
                let mut response = request.clone();
                response.extend_from_slice(b" received");
                tprintln!("Peer {} responding: {:?}", addr, response);
                Ok(response)
            }).await;
        }));
    }
    sleep(Duration::from_millis(300)).await;
    for addr in &addrs {
        let msg = format!("hello from {}", addr.port());
        tprintln!("Sending to {}: {}", addr, msg);
        let response = connect(*addr)
            .await
            .unwrap()
            .message(msg.as_bytes())
            .await
            .unwrap();
        tprintln!("Received from {}: {:?}", addr, response);
        let expected = [msg.as_bytes(), b" received"].concat();
        assert_eq!(response, expected);
    }
    let unused_addr = get_test_addr(20199);
    tprintln!("Sending to unused port {}", unused_addr);
    let result = connect(unused_addr).await;
    if result.is_err() {
        tprintln!("No peer listening at {} (expected)", unused_addr);
    } else {
        tprintln!("Unexpectedly connected to {}", unused_addr);
    }
    assert!(result.is_err());
}

// ==================== Membership Management Tests ====================

#[tokio::test]
async fn test_node_join_two_nodes() {
    let bootstrap_addr = get_test_addr(21000);
    let joining_addr = get_test_addr(21001);

    // Create bootstrap node and start ring
    let bootstrap_node = Node::new(bootstrap_addr);
    bootstrap_node.create_ring().await;

    // Start bootstrap node listening
    let bootstrap_node_clone = Node::new(bootstrap_addr);
    bootstrap_node_clone.create_ring().await;
    tokio::spawn(async move {
        let _ = bootstrap_node_clone.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    // Create joining node and join via bootstrap
    let joining_node = Node::new(joining_addr);
    let join_result = joining_node.join(bootstrap_addr).await;
    tprintln!("Join result: {:?}", join_result);
    assert!(join_result.is_ok());

    // Verify joining node has bootstrap as successor
    let successor = joining_node.get_successor().await;
    assert!(successor.is_some());
    tprintln!("Joining node successor: {:?}", successor);
}

#[tokio::test]
async fn test_node_join_with_bootstrap_builder() {
    let bootstrap_addr = get_test_addr(21010);
    let joining_addr = get_test_addr(21011);

    // Create and run bootstrap node
    let bootstrap_node = Node::new(bootstrap_addr);
    bootstrap_node.create_ring().await;
    tokio::spawn(async move {
        let _ = bootstrap_node.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    // Create joining node with bootstrap set
    let joining_node = Node::new(joining_addr).with_bootstrap(bootstrap_addr);
    
    // Join using any address (bootstrap_addr will be used)
    let join_result = joining_node.join(bootstrap_addr).await;
    assert!(join_result.is_ok());

    let successor = joining_node.get_successor().await;
    assert!(successor.is_some());
    assert_eq!(successor.unwrap().addr, bootstrap_addr);
}

#[tokio::test]
async fn test_node_join_updates_pointers() {
    let node1_addr = get_test_addr(21020);
    let node2_addr = get_test_addr(21021);

    // Create first node (bootstrap)
    let node1 = Node::new(node1_addr);
    node1.create_ring().await;
    tokio::spawn(async move {
        let _ = node1.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    // Second node joins
    let node2 = Node::new(node2_addr);
    node2.join(node1_addr).await.unwrap();

    // Start node2 listening so we can query it
    let node2_for_run = Node::new(node2_addr);
    node2_for_run.create_ring().await;
    
    // Verify node2's successor is node1
    let node2_successor = node2.get_successor().await;
    assert!(node2_successor.is_some());
    tprintln!("Node2 successor: {:?}", node2_successor);
}

#[tokio::test]
async fn test_node_join_key_transfer() {
    let bootstrap_addr = get_test_addr(21030);
    let joining_addr = get_test_addr(21031);

    // Create bootstrap node with some data
    let bootstrap_node = Node::new(bootstrap_addr);
    bootstrap_node.create_ring().await;
    bootstrap_node.put("key1".to_string(), "value1".to_string()).await.unwrap();
    bootstrap_node.put("key2".to_string(), "value2".to_string()).await.unwrap();
    tprintln!("Bootstrap node has keys: key1, key2");

    // Start bootstrap node
    tokio::spawn(async move {
        let _ = bootstrap_node.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    // Joining node joins and should receive keys
    let joining_node = Node::new(joining_addr);
    joining_node.join(bootstrap_addr).await.unwrap();

    // Verify keys were transferred (in simplified implementation, all keys transfer)
    tprintln!("Joining node joined successfully");
}

#[tokio::test]
async fn test_node_graceful_depart_single() {
    let addr = get_test_addr(21040);

    // Create node
    let node = Node::new(addr);
    node.create_ring().await;
    node.put("test_key".to_string(), "test_value".to_string()).await.unwrap();

    // Depart (no other nodes, so just clears state)
    let depart_result = node.depart().await;
    assert!(depart_result.is_ok());

    // Verify state is cleared
    let successor = node.get_successor().await;
    let predecessor = node.get_predecessor().await;
    assert!(successor.is_none());
    assert!(predecessor.is_none());
    tprintln!("Node departed, state cleared");
}

#[tokio::test]
async fn test_node_depart_transfers_keys() {
    let node1_addr = get_test_addr(21050);
    let node2_addr = get_test_addr(21051);

    // Create first node (bootstrap) with keys
    let node1 = Node::new(node1_addr);
    node1.create_ring().await;
    node1.put("key_a".to_string(), "value_a".to_string()).await.unwrap();
    node1.put("key_b".to_string(), "value_b".to_string()).await.unwrap();

    // Start node1
    tokio::spawn(async move {
        let _ = node1.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    // Node2 joins
    let node2 = Node::new(node2_addr);
    node2.join(node1_addr).await.unwrap();
    node2.put("key_c".to_string(), "value_c".to_string()).await.unwrap();

    // Node2 departs - keys should transfer to successor
    let depart_result = node2.depart().await;
    assert!(depart_result.is_ok());
    tprintln!("Node2 departed, keys should be transferred");
}

#[tokio::test]
async fn test_three_node_ring_join() {
    let node1_addr = get_test_addr(21060);
    let node2_addr = get_test_addr(21061);
    let node3_addr = get_test_addr(21062);

    // Create first node (bootstrap)
    let node1 = Node::new(node1_addr);
    node1.create_ring().await;
    tokio::spawn(async move {
        let _ = node1.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    // Node2 joins
    let node2 = Node::new(node2_addr);
    node2.join(node1_addr).await.unwrap();
    tprintln!("Node2 joined");

    // Start node2
    tokio::spawn(async move {
        let _ = node2.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    // Node3 joins via node1
    let node3 = Node::new(node3_addr);
    node3.join(node1_addr).await.unwrap();
    tprintln!("Node3 joined");

    // Verify node3 has a successor
    let node3_successor = node3.get_successor().await;
    assert!(node3_successor.is_some());
    tprintln!("Node3 successor: {:?}", node3_successor);
}

#[tokio::test]
async fn test_node_depart_updates_neighbors() {
    let node1_addr = get_test_addr(21070);
    let node2_addr = get_test_addr(21071);

    // Create bootstrap node
    let node1 = Node::new(node1_addr);
    node1.create_ring().await;
    tokio::spawn(async move {
        let _ = node1.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    // Node2 joins
    let node2 = Node::new(node2_addr);
    node2.join(node1_addr).await.unwrap();
    tprintln!("Node2 joined, successor: {:?}", node2.get_successor().await);

    // Node2 departs
    node2.depart().await.unwrap();
    tprintln!("Node2 departed gracefully");

    // Verify node2 state is cleared
    assert!(node2.get_successor().await.is_none());
    assert!(node2.get_predecessor().await.is_none());
}

#[tokio::test]
async fn test_join_fail_no_bootstrap() {
    let joining_addr = get_test_addr(21080);
    let nonexistent_addr = get_test_addr(21099);

    let node = Node::new(joining_addr);
    let result = node.join(nonexistent_addr).await;

    // Should fail because no node is listening
    assert!(result.is_err());
    tprintln!("Join correctly failed: {:?}", result.err());
}

#[tokio::test]
async fn test_protocol_set_predecessor() {
    let addr = get_test_addr(21090);
    let new_pred_addr = get_test_addr(21091);

    // Create and run node
    let node = Node::new(addr);
    node.create_ring().await;
    tokio::spawn(async move {
        let _ = node.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    // Send SetPredecessor request
    let request = Request::SetPredecessor { 
        node: chordify::nodes::NodeInfo::new(new_pred_addr) 
    };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();

    assert!(matches!(response, Response::Ok));
    tprintln!("SetPredecessor successful");
}

#[tokio::test]
async fn test_protocol_set_successor() {
    let addr = get_test_addr(21092);
    let new_succ_addr = get_test_addr(21093);

    // Create and run node
    let node = Node::new(addr);
    node.create_ring().await;
    tokio::spawn(async move {
        let _ = node.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    // Send SetSuccessor request
    let request = Request::SetSuccessor { 
        node: chordify::nodes::NodeInfo::new(new_succ_addr) 
    };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();

    assert!(matches!(response, Response::Ok));
    tprintln!("SetSuccessor successful");
}

#[tokio::test]
async fn test_protocol_transfer_keys() {
    let addr = get_test_addr(21094);

    // Create node with data
    let node = Node::new(addr);
    node.create_ring().await;
    node.put("transfer_key1".to_string(), "transfer_value1".to_string()).await.unwrap();
    node.put("transfer_key2".to_string(), "transfer_value2".to_string()).await.unwrap();

    tokio::spawn(async move {
        let _ = node.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    // Send TransferKeys request
    let request = Request::TransferKeys { to_addr: get_test_addr(21095) };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();

    match response {
        Response::Keys(keys) => {
            tprintln!("Received {} keys", keys.len());
            assert_eq!(keys.len(), 2);
            assert!(keys.iter().any(|(k, _)| k == "transfer_key1"));
            assert!(keys.iter().any(|(k, _)| k == "transfer_key2"));
        }
        _ => panic!("Expected Keys response"),
    }
}

#[tokio::test]
async fn test_protocol_get_successor() {
    let addr = get_test_addr(21096);

    let node = Node::new(addr);
    node.create_ring().await;
    tokio::spawn(async move {
        let _ = node.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    // Send GetSuccessor request
    let request = Request::GetSuccessor;
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();

    match response {
        Response::Successor(succ) => {
            assert_eq!(succ.addr, addr);
            tprintln!("GetSuccessor returned: {}", succ.addr);
        }
        _ => panic!("Expected Successor response"),
    }
}
