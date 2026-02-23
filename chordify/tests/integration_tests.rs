//! Integration tests for Server and Client communication

use chordify::communication::{Server, Client, NodeId};
use chordify::communication::protocol::NodeInfo;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

// Helper to get an available port
fn get_test_addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{}", port).parse().unwrap()
}

// ==================== Server Tests ====================

#[tokio::test]
async fn test_server_creation() {
    let addr = get_test_addr(18000);
    let server = Server::new(addr);
    let state = server.state();
    let state = state.read().await;
    
    assert_eq!(state.addr, addr);
    assert_eq!(state.id, NodeId::from_address(&addr));
}

#[tokio::test]
async fn test_server_state_info() {
    let addr = get_test_addr(18001);
    let server = Server::new(addr);
    let state = server.state();
    let state = state.read().await;
    
    let info = state.info();
    assert_eq!(info.addr, addr);
    assert_eq!(info.id, NodeId::from_address(&addr));
}

#[tokio::test]
async fn test_server_initial_storage_empty() {
    let addr = get_test_addr(18002);
    let server = Server::new(addr);
    let state = server.state();
    let state = state.read().await;
    
    assert!(state.storage.is_empty());
}

#[tokio::test]
async fn test_server_initial_successor_none() {
    let addr = get_test_addr(18003);
    let server = Server::new(addr);
    let state = server.state();
    let state = state.read().await;
    
    assert!(state.successor.is_none());
}

#[tokio::test]
async fn test_server_initial_predecessor_none() {
    let addr = get_test_addr(18004);
    let server = Server::new(addr);
    let state = server.state();
    let state = state.read().await;
    
    assert!(state.predecessor.is_none());
}

// ==================== Client-Server Integration Tests ====================

#[tokio::test]
async fn test_ping_pong() {
    let server_addr = get_test_addr(18010);
    let server = Server::new(server_addr);
    
    // Start server in background
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    
    // Wait for server to start
    sleep(Duration::from_millis(100)).await;
    
    // Create client and ping
    let client_addr = get_test_addr(18011);
    let client = Client::new(client_addr);
    
    let result = client.ping(server_addr).await;
    assert!(result.is_ok());
    assert!(result.unwrap());
}

#[tokio::test]
async fn test_get_id() {
    let server_addr = get_test_addr(18020);
    let server = Server::new(server_addr);
    let expected_id = NodeId::from_address(&server_addr);
    
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    
    sleep(Duration::from_millis(100)).await;
    
    let client_addr = get_test_addr(18021);
    let client = Client::new(client_addr);
    
    let result = client.get_id(server_addr).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), expected_id);
}

#[tokio::test]
async fn test_insert_and_query() {
    let server_addr = get_test_addr(18030);
    let server = Server::new(server_addr);
    
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    
    sleep(Duration::from_millis(100)).await;
    
    let client_addr = get_test_addr(18031);
    let client = Client::new(client_addr);
    
    // Insert
    let insert_result = client.insert(
        server_addr,
        "Song Title".to_string(),
        "Artist Name".to_string(),
    ).await;
    assert!(insert_result.is_ok());
    assert!(insert_result.unwrap());
    
    // Query
    let query_result = client.query(server_addr, "Song Title".to_string()).await;
    assert!(query_result.is_ok());
    assert_eq!(query_result.unwrap(), Some("Artist Name".to_string()));
}

#[tokio::test]
async fn test_query_nonexistent_key() {
    let server_addr = get_test_addr(18040);
    let server = Server::new(server_addr);
    
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    
    sleep(Duration::from_millis(100)).await;
    
    let client_addr = get_test_addr(18041);
    let client = Client::new(client_addr);
    
    let result = client.query(server_addr, "nonexistent".to_string()).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[tokio::test]
async fn test_delete() {
    let server_addr = get_test_addr(18050);
    let server = Server::new(server_addr);
    
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    
    sleep(Duration::from_millis(100)).await;
    
    let client_addr = get_test_addr(18051);
    let client = Client::new(client_addr);
    
    // Insert first
    client.insert(server_addr, "to_delete".to_string(), "value".to_string()).await.unwrap();
    
    // Verify it exists
    let query = client.query(server_addr, "to_delete".to_string()).await.unwrap();
    assert!(query.is_some());
    
    // Delete
    let delete_result = client.delete(server_addr, "to_delete".to_string()).await;
    assert!(delete_result.is_ok());
    assert!(delete_result.unwrap());
    
    // Verify it's gone
    let query_after = client.query(server_addr, "to_delete".to_string()).await.unwrap();
    assert!(query_after.is_none());
}

#[tokio::test]
async fn test_delete_nonexistent() {
    let server_addr = get_test_addr(18060);
    let server = Server::new(server_addr);
    
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    
    sleep(Duration::from_millis(100)).await;
    
    let client_addr = get_test_addr(18061);
    let client = Client::new(client_addr);
    
    let result = client.delete(server_addr, "never_existed".to_string()).await;
    assert!(result.is_ok());
    assert!(!result.unwrap()); // Should return false for nonexistent key
}

#[tokio::test]
async fn test_get_overlay() {
    let server_addr = get_test_addr(18070);
    let server = Server::new(server_addr);
    
    // Set up successor and predecessor
    {
        let state = server.state();
        let mut state = state.write().await;
        state.successor = Some(state.info());
        state.predecessor = Some(state.info());
    }
    
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    
    sleep(Duration::from_millis(100)).await;
    
    let client_addr = get_test_addr(18071);
    let client = Client::new(client_addr);
    
    let result = client.get_overlay(server_addr).await;
    assert!(result.is_ok());
    let nodes = result.unwrap();
    assert!(!nodes.is_empty());
}

#[tokio::test]
async fn test_get_successor() {
    let server_addr = get_test_addr(18080);
    let server = Server::new(server_addr);
    
    // Set successor
    {
        let state = server.state();
        let mut state = state.write().await;
        state.successor = Some(state.info());
    }
    
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    
    sleep(Duration::from_millis(100)).await;
    
    let client_addr = get_test_addr(18081);
    let client = Client::new(client_addr);
    
    let result = client.get_successor(server_addr).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[tokio::test]
async fn test_get_predecessor() {
    let server_addr = get_test_addr(18090);
    let server = Server::new(server_addr);
    
    // Set predecessor
    {
        let state = server.state();
        let mut state = state.write().await;
        state.predecessor = Some(state.info());
    }
    
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    
    sleep(Duration::from_millis(100)).await;
    
    let client_addr = get_test_addr(18091);
    let client = Client::new(client_addr);
    
    let result = client.get_predecessor(server_addr).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[tokio::test]
async fn test_multiple_inserts() {
    let server_addr = get_test_addr(18100);
    let server = Server::new(server_addr);
    
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    
    sleep(Duration::from_millis(100)).await;
    
    let client_addr = get_test_addr(18101);
    let client = Client::new(client_addr);
    
    // Insert multiple keys
    for i in 0..10 {
        let key = format!("song_{}", i);
        let value = format!("artist_{}", i);
        let result = client.insert(server_addr, key, value).await;
        assert!(result.is_ok());
    }
    
    // Query all
    for i in 0..10 {
        let key = format!("song_{}", i);
        let expected = format!("artist_{}", i);
        let result = client.query(server_addr, key).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(expected));
    }
}

#[tokio::test]
async fn test_overwrite_key() {
    let server_addr = get_test_addr(18110);
    let server = Server::new(server_addr);
    
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    
    sleep(Duration::from_millis(100)).await;
    
    let client_addr = get_test_addr(18111);
    let client = Client::new(client_addr);
    
    // Insert
    client.insert(server_addr, "key".to_string(), "value1".to_string()).await.unwrap();
    
    // Overwrite
    client.insert(server_addr, "key".to_string(), "value2".to_string()).await.unwrap();
    
    // Query - should get latest value
    let result = client.query(server_addr, "key".to_string()).await.unwrap();
    assert_eq!(result, Some("value2".to_string()));
}

// ==================== Error Handling Tests ====================

#[tokio::test]
async fn test_connection_to_nonexistent_server() {
    let client_addr = get_test_addr(18120);
    let client = Client::new(client_addr);
    
    // Try to connect to a server that doesn't exist
    let nonexistent_addr = get_test_addr(19999);
    let result = client.ping(nonexistent_addr).await;
    
    assert!(result.is_err());
}

#[tokio::test]
async fn test_client_timeout() {
    let client_addr = get_test_addr(18130);
    let client = Client::with_timeout(client_addr, Duration::from_millis(100));
    
    // Try to connect to a server that doesn't exist
    let nonexistent_addr = get_test_addr(19998);
    let result = client.ping(nonexistent_addr).await;
    
    assert!(result.is_err());
}

// ==================== Concurrent Access Tests ====================

#[tokio::test]
async fn test_concurrent_requests() {
    let server_addr = get_test_addr(18140);
    let server = Server::new(server_addr);
    
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    
    sleep(Duration::from_millis(100)).await;
    
    // Spawn multiple clients making concurrent requests
    let mut handles = vec![];
    for i in 0..5 {
        let client_addr = get_test_addr(18141 + i);
        let handle = tokio::spawn(async move {
            let client = Client::new(client_addr);
            let key = format!("concurrent_key_{}", i);
            let value = format!("concurrent_value_{}", i);
            
            // Insert
            let insert = client.insert(server_addr, key.clone(), value.clone()).await;
            assert!(insert.is_ok());
            
            // Query
            let query = client.query(server_addr, key).await;
            assert!(query.is_ok());
            assert_eq!(query.unwrap(), Some(value));
        });
        handles.push(handle);
    }
    
    // Wait for all to complete
    for handle in handles {
        handle.await.unwrap();
    }
}
