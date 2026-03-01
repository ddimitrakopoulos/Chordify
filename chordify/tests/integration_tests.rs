//! Integration tests for P2P communication: Connect → Message → Response

use chordify::tcp::{Server, connect, connect_with_timeout};
use chordify::nodes::{Node, Request, Response};
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

// ==================== Node (Chord DHT) Tests ====================
// NOTE: Node::create_ring() is now private. Use BootstrapNode for ring creation.

#[tokio::test]
async fn test_node_create_ring() {
    let addr = get_test_addr(20000);
    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;

    // After creating a ring, successor and predecessor should be self
    let successor = bootstrap.get_successor().await.unwrap();
    let predecessor = bootstrap.get_predecessor().await.unwrap();
    assert_eq!(successor.addr, addr);
    assert_eq!(predecessor.addr, addr);
}

#[tokio::test]
async fn test_node_put_get_single_node() {
    let addr = get_test_addr(20010);
    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;

    // Put and get a value
    bootstrap.put("foo".to_string(), "bar".to_string()).await.unwrap();
    let value = bootstrap.get("foo").await.unwrap();
    assert_eq!(value, Some("bar".to_string()));

    // Get a non-existent key
    let missing = bootstrap.get("missing").await.unwrap();
    assert_eq!(missing, None);
}

#[tokio::test]
async fn test_node_find_successor_single_node() {
    let addr = get_test_addr(20020);
    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;

    let successor = bootstrap.find_successor(addr).await.unwrap();
    assert_eq!(successor.addr, addr);
}

#[tokio::test]
async fn test_node_run_and_ping() {
    let addr = get_test_addr(20030);
    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;

    // Start node in background
    tokio::spawn(async move {
        let _ = bootstrap.run().await;
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
    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;

    tokio::spawn(async move {
        let _ = bootstrap.run().await;
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
            let peer = Server::bind(addr).await.unwrap();
            let _ = peer.listen(move |request, _from| async move {
                tprintln!("Server {} received: {:?}", addr, request);
                let mut response = request.clone();
                response.extend_from_slice(b" received");
                tprintln!("Server {} responding: {:?}", addr, response);
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

// ==================== BootstrapNode Tests ====================

#[tokio::test]
async fn test_bootstrap_node_create_ring() {
    let addr = get_test_addr(23000);
    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;

    let successor = bootstrap.get_successor().await.unwrap();
    let predecessor = bootstrap.get_predecessor().await.unwrap();
    assert_eq!(successor.addr, addr);
    assert_eq!(predecessor.addr, addr);
    tprintln!("Bootstrap ring created at {}", addr);
}

#[tokio::test]
async fn test_bootstrap_node_cannot_depart() {
    let addr = get_test_addr(23010);
    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;

    let result = bootstrap.depart().await;
    assert!(result.is_err());
    tprintln!("Bootstrap correctly refused to depart");
}

#[tokio::test]
async fn test_bootstrap_node_register_unregister() {
    let addr = get_test_addr(23020);
    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;

    let node_info = chordify::NodeInfo::new(get_test_addr(23021));
    bootstrap.register_node(node_info.clone()).await;

    let members = bootstrap.get_ring_members().await;
    assert!(members.iter().any(|n| n.addr == node_info.addr));

    bootstrap.unregister_node(node_info.addr).await;
    let members = bootstrap.get_ring_members().await;
    assert!(!members.iter().any(|n| n.addr == node_info.addr));
}

#[tokio::test]
async fn test_bootstrap_node_put_get() {
    let addr = get_test_addr(23040);
    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;

    bootstrap.put("key".to_string(), "value".to_string()).await.unwrap();
    let value = bootstrap.get("key").await.unwrap();
    assert_eq!(value, Some("value".to_string()));
}

#[tokio::test]
async fn test_bootstrap_node_handles_ping() {
    let addr = get_test_addr(23070);
    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;

    tokio::spawn(async move {
        let _ = bootstrap.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    let request = Request::Ping;
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();
    assert!(matches!(response, Response::Pong));
}

#[tokio::test]
async fn test_bootstrap_node_handles_get_predecessor() {
    let addr = get_test_addr(23080);
    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;

    tokio::spawn(async move {
        let _ = bootstrap.run().await;
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
async fn test_bootstrap_node_handles_get_successor() {
    let addr = get_test_addr(23090);
    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;

    tokio::spawn(async move {
        let _ = bootstrap.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    let request = Request::GetSuccessor;
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();

    match response {
        Response::Successor(succ) => assert_eq!(succ.addr, addr),
        _ => panic!("Expected Successor response"),
    }
}

#[tokio::test]
async fn test_bootstrap_duplicate_register_ignored() {
    let addr = get_test_addr(23160);
    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;

    let node_info = chordify::NodeInfo::new(get_test_addr(23161));
    
    bootstrap.register_node(node_info.clone()).await;
    bootstrap.register_node(node_info.clone()).await;

    let members = bootstrap.get_ring_members().await;
    let count = members.iter().filter(|n| n.addr == node_info.addr).count();
    assert_eq!(count, 1);
}

// ==================== Bootstrap-Coordinated Join/Depart Tests ====================

#[tokio::test]
async fn test_join_via_bootstrap() {
    let bootstrap_addr = get_test_addr(24000);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    bootstrap.create_ring().await;
    
    tokio::spawn(async move {
        let _ = bootstrap.run().await;
    });
    sleep(Duration::from_millis(300)).await;
    
    // Create node and start listening
    let node_addr = get_test_addr(24001);
    tokio::spawn({
        let addr = node_addr;
        async move {
            let node = Node::new(addr);
            let _ = node.run().await;
        }
    });
    sleep(Duration::from_millis(200)).await;
    
    // Join via bootstrap
    let node = Node::new(node_addr);
    let result = node.join(bootstrap_addr).await;
    assert!(result.is_ok(), "join should succeed");
    
    let successor = node.get_successor().await;
    assert!(successor.is_some(), "Node should have a successor after joining");
    tprintln!("Node joined via bootstrap, successor: {:?}", successor.map(|s| s.addr));
}

#[tokio::test]
async fn test_join_request_returns_successor_and_predecessor() {
    let bootstrap_addr = get_test_addr(24010);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    bootstrap.create_ring().await;
    
    tokio::spawn(async move {
        let _ = bootstrap.run().await;
    });
    sleep(Duration::from_millis(300)).await;
    
    let joining_node = chordify::NodeInfo::new(get_test_addr(24011));
    let request = Request::JoinRequest { joining_node };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(bootstrap_addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();
    
    match response {
        Response::JoinSuccess { successor, predecessor: _ } => {
            assert_eq!(successor.addr, bootstrap_addr);
            tprintln!("JoinSuccess received");
        }
        _ => panic!("Expected JoinSuccess response"),
    }
}

#[tokio::test]
async fn test_depart_via_bootstrap() {
    let bootstrap_addr = get_test_addr(24020);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    bootstrap.create_ring().await;
    
    tokio::spawn(async move {
        let _ = bootstrap.run().await;
    });
    sleep(Duration::from_millis(300)).await;
    
    // Create and join node
    let node_addr = get_test_addr(24021);
    let node = Node::new(node_addr);
    node.join(bootstrap_addr).await.unwrap();
    
    // Depart via bootstrap
    let result = node.depart(bootstrap_addr).await;
    assert!(result.is_ok(), "depart should succeed");
    
    assert!(node.get_successor().await.is_none());
    assert!(node.get_predecessor().await.is_none());
    tprintln!("Node departed via bootstrap successfully");
}

#[tokio::test]
async fn test_depart_request_protocol() {
    let bootstrap_addr = get_test_addr(24030);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    bootstrap.create_ring().await;
    
    let departing_node = chordify::NodeInfo::new(get_test_addr(24031));
    bootstrap.register_node(departing_node.clone()).await;
    
    tokio::spawn(async move {
        let _ = bootstrap.run().await;
    });
    sleep(Duration::from_millis(300)).await;
    
    let request = Request::DepartRequest { departing_node };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(bootstrap_addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();
    
    assert!(matches!(response, Response::DepartSuccess));
}

#[tokio::test]
async fn test_coordinate_join_updates_bootstrap_pointers() {
    let bootstrap_addr = get_test_addr(24040);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    bootstrap.create_ring().await;
    
    assert_eq!(bootstrap.get_successor().await.unwrap().addr, bootstrap_addr);
    
    let joining_addr = get_test_addr(24041);
    tokio::spawn({
        let addr = joining_addr;
        async move {
            let node = Node::new(addr);
            let _ = node.run().await;
        }
    });
    sleep(Duration::from_millis(200)).await;
    
    let result = bootstrap.coordinate_join(joining_addr).await;
    assert!(result.is_ok());
    
    // Bootstrap pointers should be updated
    let new_succ = bootstrap.get_successor().await;
    let new_pred = bootstrap.get_predecessor().await;
    
    let succ_is_joining = new_succ.as_ref().map(|s| s.addr) == Some(joining_addr);
    let pred_is_joining = new_pred.as_ref().map(|p| p.addr) == Some(joining_addr);
    assert!(succ_is_joining || pred_is_joining);
}

#[tokio::test]
async fn test_coordinate_depart_removes_node() {
    let bootstrap_addr = get_test_addr(24050);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    bootstrap.create_ring().await;
    
    let node_addr = get_test_addr(24051);
    
    tokio::spawn({
        let addr = node_addr;
        let bootstrap_addr = bootstrap_addr;
        async move {
            let node = Node::new(addr);
            let _ = node.join(bootstrap_addr).await;
            let _ = node.run().await;
        }
    });
    sleep(Duration::from_millis(300)).await;
    
    let _ = bootstrap.coordinate_join(node_addr).await;
    
    let result = bootstrap.coordinate_depart(node_addr).await;
    assert!(result.is_ok());
    
    let members = bootstrap.get_ring_members().await;
    assert!(!members.iter().any(|n| n.addr == node_addr));
}

#[tokio::test]
async fn test_bootstrap_cannot_coordinate_own_depart() {
    let bootstrap_addr = get_test_addr(24060);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    bootstrap.create_ring().await;
    
    let result = bootstrap.coordinate_depart(bootstrap_addr).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_receive_keys_protocol() {
    let addr = get_test_addr(24070);
    
    tokio::spawn({
        let addr = addr;
        async move {
            let node = Node::new(addr);
            let _ = node.run().await;
        }
    });
    sleep(Duration::from_millis(300)).await;
    
    let keys = vec![
        ("key1".to_string(), "value1".to_string()),
        ("key2".to_string(), "value2".to_string()),
    ];
    let request = Request::ReceiveKeys { keys };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();
    
    assert!(matches!(response, Response::Ok));
}

#[tokio::test]
async fn test_transfer_keys_to_protocol() {
    let source_addr = get_test_addr(24080);
    let target_addr = get_test_addr(24081);
    
    // Start target
    tokio::spawn({
        let addr = target_addr;
        async move {
            let node = Node::new(addr);
            let _ = node.run().await;
        }
    });
    sleep(Duration::from_millis(200)).await;
    
    // Start source with data
    tokio::spawn({
        let addr = source_addr;
        async move {
            let bootstrap = BootstrapNode::new(addr);
            bootstrap.create_ring().await;
            bootstrap.inner().put("key".to_string(), "value".to_string()).await.unwrap();
            let _ = bootstrap.inner().run().await;
        }
    });
    sleep(Duration::from_millis(200)).await;
    
    let request = Request::TransferKeysTo { target_addr };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(source_addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();
    
    assert!(matches!(response, Response::Ok));
}

#[tokio::test]
async fn test_regular_node_rejects_join_request() {
    let addr = get_test_addr(24090);
    
    tokio::spawn({
        let addr = addr;
        async move {
            let node = Node::new(addr);
            let _ = node.run().await;
        }
    });
    sleep(Duration::from_millis(300)).await;
    
    let joining_node = chordify::NodeInfo::new(get_test_addr(24091));
    let request = Request::JoinRequest { joining_node };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();
    
    match response {
        Response::Error(msg) => assert!(msg.contains("bootstrap")),
        _ => panic!("Expected Error response"),
    }
}

#[tokio::test]
async fn test_regular_node_rejects_depart_request() {
    let addr = get_test_addr(24100);
    
    tokio::spawn({
        let addr = addr;
        async move {
            let node = Node::new(addr);
            let _ = node.run().await;
        }
    });
    sleep(Duration::from_millis(300)).await;
    
    let departing_node = chordify::NodeInfo::new(get_test_addr(24101));
    let request = Request::DepartRequest { departing_node };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();
    
    match response {
        Response::Error(msg) => assert!(msg.contains("bootstrap")),
        _ => panic!("Expected Error response"),
    }
}

#[tokio::test]
async fn test_multiple_nodes_join_via_bootstrap() {
    let bootstrap_addr = get_test_addr(24110);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    bootstrap.create_ring().await;
    
    tokio::spawn(async move {
        let _ = bootstrap.run().await;
    });
    sleep(Duration::from_millis(300)).await;
    
    for i in 0..3 {
        let node_addr = get_test_addr(24111 + i);
        
        tokio::spawn({
            let addr = node_addr;
            async move {
                let node = Node::new(addr);
                let _ = node.run().await;
            }
        });
        sleep(Duration::from_millis(100)).await;
        
        let joining_node = chordify::NodeInfo::new(node_addr);
        let request = Request::JoinRequest { joining_node };
        let request_bytes = request.to_bytes().unwrap();
        let response_bytes = connect(bootstrap_addr).await.unwrap().message(&request_bytes).await.unwrap();
        let response = Response::from_bytes(&response_bytes).unwrap();
        
        assert!(matches!(response, Response::JoinSuccess { .. }));
        tprintln!("Node {} joined", i);
    }
}

#[tokio::test]
async fn test_join_depart_rejoin_cycle() {
    let bootstrap_addr = get_test_addr(24120);
    let bootstrap = BootstrapNode::new(bootstrap_addr);
    bootstrap.create_ring().await;
    
    tokio::spawn(async move {
        let _ = bootstrap.run().await;
    });
    sleep(Duration::from_millis(300)).await;
    
    let node_addr = get_test_addr(24121);
    
    tokio::spawn({
        let addr = node_addr;
        async move {
            let node = Node::new(addr);
            let _ = node.run().await;
        }
    });
    sleep(Duration::from_millis(200)).await;
    
    // Join
    let joining_node = chordify::NodeInfo::new(node_addr);
    let request = Request::JoinRequest { joining_node: joining_node.clone() };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(bootstrap_addr).await.unwrap().message(&request_bytes).await.unwrap();
    assert!(matches!(Response::from_bytes(&response_bytes).unwrap(), Response::JoinSuccess { .. }));
    
    // Depart
    let request = Request::DepartRequest { departing_node: joining_node.clone() };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(bootstrap_addr).await.unwrap().message(&request_bytes).await.unwrap();
    assert!(matches!(Response::from_bytes(&response_bytes).unwrap(), Response::DepartSuccess));
    
    // Rejoin
    let request = Request::JoinRequest { joining_node };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(bootstrap_addr).await.unwrap().message(&request_bytes).await.unwrap();
    assert!(matches!(Response::from_bytes(&response_bytes).unwrap(), Response::JoinSuccess { .. }));
    
    tprintln!("Join-depart-rejoin cycle completed");
}

#[tokio::test]
async fn test_join_fail_no_bootstrap() {
    let joining_addr = get_test_addr(24130);
    let nonexistent_addr = get_test_addr(24199);

    let node = Node::new(joining_addr);
    let result = node.join(nonexistent_addr).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_protocol_set_predecessor() {
    let addr = get_test_addr(24140);
    let new_pred_addr = get_test_addr(24141);

    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;
    tokio::spawn(async move {
        let _ = bootstrap.inner().run().await;
    });
    sleep(Duration::from_millis(300)).await;

    let request = Request::SetPredecessor { 
        node: chordify::NodeInfo::new(new_pred_addr) 
    };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();

    assert!(matches!(response, Response::Ok));
}

#[tokio::test]
async fn test_protocol_set_successor() {
    let addr = get_test_addr(24142);
    let new_succ_addr = get_test_addr(24143);

    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;
    tokio::spawn(async move {
        let _ = bootstrap.inner().run().await;
    });
    sleep(Duration::from_millis(300)).await;

    let request = Request::SetSuccessor { 
        node: chordify::NodeInfo::new(new_succ_addr) 
    };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();

    assert!(matches!(response, Response::Ok));
}

#[tokio::test]
async fn test_protocol_transfer_keys() {
    let addr = get_test_addr(24144);

    let bootstrap = BootstrapNode::new(addr);
    bootstrap.create_ring().await;
    bootstrap.inner().put("key1".to_string(), "value1".to_string()).await.unwrap();
    bootstrap.inner().put("key2".to_string(), "value2".to_string()).await.unwrap();

    tokio::spawn(async move {
        let _ = bootstrap.inner().run().await;
    });
    sleep(Duration::from_millis(300)).await;

    let request = Request::TransferKeys { to_addr: get_test_addr(24145) };
    let request_bytes = request.to_bytes().unwrap();
    let response_bytes = connect(addr).await.unwrap().message(&request_bytes).await.unwrap();
    let response = Response::from_bytes(&response_bytes).unwrap();

    match response {
        Response::Keys(keys) => {
            assert_eq!(keys.len(), 2);
        }
        _ => panic!("Expected Keys response"),
    }
}
