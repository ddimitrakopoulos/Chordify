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
    sleep(Duration::from_millis(100)).await;
    
    // Connect, send message, get response
    let response = connect(server_addr)
        .await
        .unwrap()
        .message(b"Hello, peer!")
        .await
        .unwrap();
    
    assert_eq!(response, b"Hello, peer!");
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
    
    sleep(Duration::from_millis(100)).await;
    
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
    
    sleep(Duration::from_millis(100)).await;
    
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
    
    sleep(Duration::from_millis(100)).await;
    
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
    
    sleep(Duration::from_millis(100)).await;
    
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
    
    sleep(Duration::from_millis(100)).await;
    
    let response = connect(server_addr)
        .await
        .unwrap()
        .message(b"hello")
        .await
        .unwrap();
    
    assert_eq!(response, b"olleh");
}
