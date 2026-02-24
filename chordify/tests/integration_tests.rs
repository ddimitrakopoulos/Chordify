//! Integration tests for P2P Peer communication

use chordify::communication::{Peer, Connection};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

// Helper to get an available port for testing
fn get_test_addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{}", port).parse().unwrap()
}

// ==================== Peer Tests ====================

#[tokio::test]
async fn test_peer_creation() {
    let addr = get_test_addr(19000);
    let peer = Peer::new(addr);
    assert_eq!(peer.addr(), addr);
}

// ==================== Connection Tests ====================

#[tokio::test]
async fn test_connect_send_receive() {
    let server_addr = get_test_addr(19010);
    let peer = Peer::new(server_addr);
    
    // Start peer in background with echo handler
    tokio::spawn(async move {
        let _ = peer.listen(|mut conn| async move {
            if let Ok(Some(data)) = conn.receive().await {
                let _ = conn.answer(&data).await;
            }
        }).await;
    });
    
    // Wait for server to start
    sleep(Duration::from_millis(100)).await;
    
    // Connect and send message
    let mut conn = Connection::connect(server_addr).await.unwrap();
    let msg = b"Hello, peer!";
    conn.send(msg).await.unwrap();
    
    // Receive response
    let response = conn.receive().await.unwrap().unwrap();
    assert_eq!(response, msg);
}

#[tokio::test]
async fn test_multiple_messages() {
    let server_addr = get_test_addr(19020);
    let peer = Peer::new(server_addr);
    
    // Start peer with echo handler
    tokio::spawn(async move {
        let _ = peer.listen(|mut conn| async move {
            loop {
                match conn.receive().await {
                    Ok(Some(data)) => {
                        if conn.answer(&data).await.is_err() {
                            break;
                        }
                    }
                    _ => break,
                }
            }
        }).await;
    });
    
    sleep(Duration::from_millis(100)).await;
    
    let mut conn = Connection::connect(server_addr).await.unwrap();
    
    // Send multiple messages
    for i in 0..5 {
        let msg = format!("Message {}", i).into_bytes();
        conn.send(&msg).await.unwrap();
        let response = conn.receive().await.unwrap().unwrap();
        assert_eq!(response, msg);
    }
}

#[tokio::test]
async fn test_connection_to_nonexistent_peer() {
    let nonexistent_addr = get_test_addr(19999);
    let result = Connection::connect(nonexistent_addr).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_connection_timeout() {
    let nonexistent_addr = get_test_addr(19998);
    let result = Connection::connect_with_timeout(nonexistent_addr, Duration::from_millis(100)).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_large_message() {
    let server_addr = get_test_addr(19030);
    let peer = Peer::new(server_addr);
    
    tokio::spawn(async move {
        let _ = peer.listen(|mut conn| async move {
            if let Ok(Some(data)) = conn.receive().await {
                let _ = conn.answer(&data).await;
            }
        }).await;
    });
    
    sleep(Duration::from_millis(100)).await;
    
    let mut conn = Connection::connect(server_addr).await.unwrap();
    
    // Send a large message (1 MB)
    let msg = vec![0u8; 1024 * 1024];
    conn.send(&msg).await.unwrap();
    
    let response = conn.receive().await.unwrap().unwrap();
    assert_eq!(response.len(), msg.len());
}

#[tokio::test]
async fn test_concurrent_connections() {
    let server_addr = get_test_addr(19040);
    let peer = Peer::new(server_addr);
    
    tokio::spawn(async move {
        let _ = peer.listen(|mut conn| async move {
            if let Ok(Some(data)) = conn.receive().await {
                let _ = conn.answer(&data).await;
            }
        }).await;
    });
    
    sleep(Duration::from_millis(100)).await;
    
    // Spawn multiple concurrent connections
    let mut handles = vec![];
    for i in 0..5 {
        let handle = tokio::spawn(async move {
            let mut conn = Connection::connect(server_addr).await.unwrap();
            let msg = format!("Concurrent {}", i).into_bytes();
            conn.send(&msg).await.unwrap();
            let response = conn.receive().await.unwrap().unwrap();
            assert_eq!(response, msg);
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
}
