//! Integration tests for P2P communication: Connect → Message → Response

use chordify::communication::{Peer, connect, connect_with_timeout};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

// Helper to get an available port for testing
fn get_test_addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{}", port).parse().unwrap()
}

// ==================== Peer Tests ====================

#[tokio::test]
async fn test_peer_bind() {
    let addr = get_test_addr(19000);
    let peer = Peer::bind(addr).await.unwrap();
    assert_eq!(peer.addr(), addr);
}

// ==================== Connect → Message → Response Tests ====================

#[tokio::test]
async fn test_connect_message_response() {
    let server_addr = get_test_addr(19010);
    
    // Start peer with echo handler
    tokio::spawn(async move {
        let peer = Peer::bind(server_addr).await.unwrap();
        let _ = peer.listen(|request, _from| async move {
            Ok(request) // echo
        }).await;
    });
    
    // Wait for server to start
    sleep(Duration::from_millis(300)).await;
    
    // Connect, send message, get response
    let response = connect(server_addr)
        .await
        .unwrap()
        .message(b"hello world")
        .await
        .unwrap();
    
    assert_eq!(response, b"hello world");
}

#[tokio::test]
async fn test_connect_send_fire_and_forget() {
    let server_addr = get_test_addr(19011);
    
    // Start peer with handler that just logs
    tokio::spawn(async move {
        let peer = Peer::bind(server_addr).await.unwrap();
        let _ = peer.listen(|request, _from| async move {
            assert_eq!(request, b"fire-and-forget");
            Ok(Vec::new())
        }).await;
    });
    
    sleep(Duration::from_millis(300)).await;
    
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
