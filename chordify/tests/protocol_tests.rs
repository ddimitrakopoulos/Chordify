//! Tests for Message Protocol - serialization and message types

use chordify::communication::protocol::{Message, MessagePayload, Request, Response, NodeInfo};
use chordify::NodeId;
use std::net::SocketAddr;

// ==================== Message Serialization Tests ====================

#[test]
fn test_message_serialization_roundtrip() {
    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let msg = Message::new(addr, MessagePayload::Request(Request::Ping));
    
    let bytes = msg.to_bytes().unwrap();
    let recovered = Message::from_bytes(&bytes).unwrap();
    
    assert_eq!(msg.id, recovered.id);
    assert_eq!(msg.sender, recovered.sender);
}

#[test]
fn test_message_response_preserves_id() {
    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let request = Message::new(addr, MessagePayload::Request(Request::Ping));
    let response = Message::response(request.id, addr, MessagePayload::Response(Response::Pong));
    
    assert_eq!(request.id, response.id, "Response should have same ID as request");
}

#[test]
fn test_message_new_generates_unique_ids() {
    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let msg1 = Message::new(addr, MessagePayload::Request(Request::Ping));
    let msg2 = Message::new(addr, MessagePayload::Request(Request::Ping));
    
    // IDs should be different (based on nanosecond timestamp)
    // Note: In rare cases they might be equal if generated at exact same nanosecond
    // But this is extremely unlikely
    assert_ne!(msg1.id, msg2.id, "Different messages should have different IDs");
}

// ==================== NodeInfo Tests ====================

#[test]
fn test_node_info_creation() {
    let addr: SocketAddr = "192.168.1.100:5000".parse().unwrap();
    let info = NodeInfo::new(addr);
    
    assert_eq!(info.addr, addr);
    assert_eq!(info.id, NodeId::from_address(&addr));
}

#[test]
fn test_node_info_equality() {
    let addr: SocketAddr = "10.0.0.1:3000".parse().unwrap();
    let info1 = NodeInfo::new(addr);
    let info2 = NodeInfo::new(addr);
    
    assert_eq!(info1, info2, "Same address should produce equal NodeInfo");
}

#[test]
fn test_node_info_serialization() {
    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let info = NodeInfo::new(addr);
    
    let json = serde_json::to_string(&info).unwrap();
    let recovered: NodeInfo = serde_json::from_str(&json).unwrap();
    
    assert_eq!(info, recovered);
}

// ==================== Request Serialization Tests ====================

#[test]
fn test_request_ping_serialization() {
    let req = Request::Ping;
    let json = serde_json::to_string(&req).unwrap();
    let recovered: Request = serde_json::from_str(&json).unwrap();
    
    assert!(matches!(recovered, Request::Ping));
}

#[test]
fn test_request_get_id_serialization() {
    let req = Request::GetId;
    let json = serde_json::to_string(&req).unwrap();
    let recovered: Request = serde_json::from_str(&json).unwrap();
    
    assert!(matches!(recovered, Request::GetId));
}

#[test]
fn test_request_insert_serialization() {
    let req = Request::Insert {
        key: "Song Title".to_string(),
        value: "Artist - Album".to_string(),
    };
    let json = serde_json::to_string(&req).unwrap();
    let recovered: Request = serde_json::from_str(&json).unwrap();
    
    if let Request::Insert { key, value } = recovered {
        assert_eq!(key, "Song Title");
        assert_eq!(value, "Artist - Album");
    } else {
        panic!("Expected Insert request");
    }
}

#[test]
fn test_request_query_serialization() {
    let req = Request::Query { key: "test_key".to_string() };
    let json = serde_json::to_string(&req).unwrap();
    let recovered: Request = serde_json::from_str(&json).unwrap();
    
    if let Request::Query { key } = recovered {
        assert_eq!(key, "test_key");
    } else {
        panic!("Expected Query request");
    }
}

#[test]
fn test_request_query_wildcard_serialization() {
    let req = Request::Query { key: "*".to_string() };
    let json = serde_json::to_string(&req).unwrap();
    let recovered: Request = serde_json::from_str(&json).unwrap();
    
    if let Request::Query { key } = recovered {
        assert_eq!(key, "*");
    } else {
        panic!("Expected Query request");
    }
}

#[test]
fn test_request_delete_serialization() {
    let req = Request::Delete { key: "to_delete".to_string() };
    let json = serde_json::to_string(&req).unwrap();
    let recovered: Request = serde_json::from_str(&json).unwrap();
    
    if let Request::Delete { key } = recovered {
        assert_eq!(key, "to_delete");
    } else {
        panic!("Expected Delete request");
    }
}

#[test]
fn test_request_find_successor_serialization() {
    let id = NodeId::from_key("target");
    let req = Request::FindSuccessor { id };
    let json = serde_json::to_string(&req).unwrap();
    let recovered: Request = serde_json::from_str(&json).unwrap();
    
    if let Request::FindSuccessor { id: recovered_id } = recovered {
        assert_eq!(recovered_id, id);
    } else {
        panic!("Expected FindSuccessor request");
    }
}

#[test]
fn test_request_join_serialization() {
    let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let node_info = NodeInfo::new(addr);
    let req = Request::Join { node_info: node_info.clone() };
    let json = serde_json::to_string(&req).unwrap();
    let recovered: Request = serde_json::from_str(&json).unwrap();
    
    if let Request::Join { node_info: recovered_info } = recovered {
        assert_eq!(recovered_info, node_info);
    } else {
        panic!("Expected Join request");
    }
}

#[test]
fn test_request_depart_serialization() {
    let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let node_info = NodeInfo::new(addr);
    let req = Request::Depart { node_info: node_info.clone() };
    let json = serde_json::to_string(&req).unwrap();
    let recovered: Request = serde_json::from_str(&json).unwrap();
    
    if let Request::Depart { node_info: recovered_info } = recovered {
        assert_eq!(recovered_info, node_info);
    } else {
        panic!("Expected Depart request");
    }
}

#[test]
fn test_request_notify_serialization() {
    let addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();
    let node_info = NodeInfo::new(addr);
    let req = Request::Notify { node_info: node_info.clone() };
    let json = serde_json::to_string(&req).unwrap();
    let recovered: Request = serde_json::from_str(&json).unwrap();
    
    if let Request::Notify { node_info: recovered_info } = recovered {
        assert_eq!(recovered_info, node_info);
    } else {
        panic!("Expected Notify request");
    }
}

#[test]
fn test_request_get_overlay_serialization() {
    let req = Request::GetOverlay;
    let json = serde_json::to_string(&req).unwrap();
    let recovered: Request = serde_json::from_str(&json).unwrap();
    
    assert!(matches!(recovered, Request::GetOverlay));
}

// ==================== Response Serialization Tests ====================

#[test]
fn test_response_pong_serialization() {
    let resp = Response::Pong;
    let json = serde_json::to_string(&resp).unwrap();
    let recovered: Response = serde_json::from_str(&json).unwrap();
    
    assert!(matches!(recovered, Response::Pong));
}

#[test]
fn test_response_ok_serialization() {
    let resp = Response::Ok;
    let json = serde_json::to_string(&resp).unwrap();
    let recovered: Response = serde_json::from_str(&json).unwrap();
    
    assert!(matches!(recovered, Response::Ok));
}

#[test]
fn test_response_id_serialization() {
    let id = NodeId::from_key("node");
    let resp = Response::Id(id);
    let json = serde_json::to_string(&resp).unwrap();
    let recovered: Response = serde_json::from_str(&json).unwrap();
    
    if let Response::Id(recovered_id) = recovered {
        assert_eq!(recovered_id, id);
    } else {
        panic!("Expected Id response");
    }
}

#[test]
fn test_response_insert_ack_success() {
    let resp = Response::InsertAck { success: true };
    let json = serde_json::to_string(&resp).unwrap();
    let recovered: Response = serde_json::from_str(&json).unwrap();
    
    if let Response::InsertAck { success } = recovered {
        assert!(success);
    } else {
        panic!("Expected InsertAck response");
    }
}

#[test]
fn test_response_insert_ack_failure() {
    let resp = Response::InsertAck { success: false };
    let json = serde_json::to_string(&resp).unwrap();
    let recovered: Response = serde_json::from_str(&json).unwrap();
    
    if let Response::InsertAck { success } = recovered {
        assert!(!success);
    } else {
        panic!("Expected InsertAck response");
    }
}

#[test]
fn test_response_query_result_found() {
    let resp = Response::QueryResult {
        key: "song".to_string(),
        value: Some("artist".to_string()),
    };
    let json = serde_json::to_string(&resp).unwrap();
    let recovered: Response = serde_json::from_str(&json).unwrap();
    
    if let Response::QueryResult { key, value } = recovered {
        assert_eq!(key, "song");
        assert_eq!(value, Some("artist".to_string()));
    } else {
        panic!("Expected QueryResult response");
    }
}

#[test]
fn test_response_query_result_not_found() {
    let resp = Response::QueryResult {
        key: "unknown".to_string(),
        value: None,
    };
    let json = serde_json::to_string(&resp).unwrap();
    let recovered: Response = serde_json::from_str(&json).unwrap();
    
    if let Response::QueryResult { key, value } = recovered {
        assert_eq!(key, "unknown");
        assert!(value.is_none());
    } else {
        panic!("Expected QueryResult response");
    }
}

#[test]
fn test_response_delete_ack() {
    let resp = Response::DeleteAck { success: true };
    let json = serde_json::to_string(&resp).unwrap();
    let recovered: Response = serde_json::from_str(&json).unwrap();
    
    if let Response::DeleteAck { success } = recovered {
        assert!(success);
    } else {
        panic!("Expected DeleteAck response");
    }
}

#[test]
fn test_response_error() {
    let resp = Response::Error { message: "Something went wrong".to_string() };
    let json = serde_json::to_string(&resp).unwrap();
    let recovered: Response = serde_json::from_str(&json).unwrap();
    
    if let Response::Error { message } = recovered {
        assert_eq!(message, "Something went wrong");
    } else {
        panic!("Expected Error response");
    }
}

#[test]
fn test_response_overlay() {
    let addr1: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let nodes = vec![NodeInfo::new(addr1), NodeInfo::new(addr2)];
    
    let resp = Response::Overlay { nodes: nodes.clone() };
    let json = serde_json::to_string(&resp).unwrap();
    let recovered: Response = serde_json::from_str(&json).unwrap();
    
    if let Response::Overlay { nodes: recovered_nodes } = recovered {
        assert_eq!(recovered_nodes.len(), 2);
        assert_eq!(recovered_nodes[0], nodes[0]);
        assert_eq!(recovered_nodes[1], nodes[1]);
    } else {
        panic!("Expected Overlay response");
    }
}

#[test]
fn test_response_join_ack() {
    let addr1: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let successor = NodeInfo::new(addr1);
    let predecessor = Some(NodeInfo::new(addr2));
    
    let resp = Response::JoinAck {
        successor: successor.clone(),
        predecessor: predecessor.clone(),
    };
    let json = serde_json::to_string(&resp).unwrap();
    let recovered: Response = serde_json::from_str(&json).unwrap();
    
    if let Response::JoinAck { successor: s, predecessor: p } = recovered {
        assert_eq!(s, successor);
        assert_eq!(p, predecessor);
    } else {
        panic!("Expected JoinAck response");
    }
}

#[test]
fn test_response_node_info_some() {
    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let info = NodeInfo::new(addr);
    let resp = Response::NodeInfo(Some(info.clone()));
    let json = serde_json::to_string(&resp).unwrap();
    let recovered: Response = serde_json::from_str(&json).unwrap();
    
    if let Response::NodeInfo(Some(recovered_info)) = recovered {
        assert_eq!(recovered_info, info);
    } else {
        panic!("Expected NodeInfo(Some) response");
    }
}

#[test]
fn test_response_node_info_none() {
    let resp = Response::NodeInfo(None);
    let json = serde_json::to_string(&resp).unwrap();
    let recovered: Response = serde_json::from_str(&json).unwrap();
    
    assert!(matches!(recovered, Response::NodeInfo(None)));
}

#[test]
fn test_response_found_successor() {
    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let info = NodeInfo::new(addr);
    let resp = Response::FoundSuccessor(info.clone());
    let json = serde_json::to_string(&resp).unwrap();
    let recovered: Response = serde_json::from_str(&json).unwrap();
    
    if let Response::FoundSuccessor(recovered_info) = recovered {
        assert_eq!(recovered_info, info);
    } else {
        panic!("Expected FoundSuccessor response");
    }
}

// ==================== Full Message Tests ====================

#[test]
fn test_full_message_with_insert_request() {
    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let payload = MessagePayload::Request(Request::Insert {
        key: "Hello".to_string(),
        value: "World".to_string(),
    });
    let msg = Message::new(addr, payload);
    
    let bytes = msg.to_bytes().unwrap();
    let recovered = Message::from_bytes(&bytes).unwrap();
    
    if let MessagePayload::Request(Request::Insert { key, value }) = recovered.payload {
        assert_eq!(key, "Hello");
        assert_eq!(value, "World");
    } else {
        panic!("Expected Insert request");
    }
}

#[test]
fn test_full_message_with_query_response() {
    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let payload = MessagePayload::Response(Response::QueryResult {
        key: "test".to_string(),
        value: Some("result".to_string()),
    });
    let msg = Message::new(addr, payload);
    
    let bytes = msg.to_bytes().unwrap();
    let recovered = Message::from_bytes(&bytes).unwrap();
    
    if let MessagePayload::Response(Response::QueryResult { key, value }) = recovered.payload {
        assert_eq!(key, "test");
        assert_eq!(value, Some("result".to_string()));
    } else {
        panic!("Expected QueryResult response");
    }
}

// ==================== Edge Cases ====================

#[test]
fn test_empty_string_key_value() {
    let req = Request::Insert {
        key: "".to_string(),
        value: "".to_string(),
    };
    let json = serde_json::to_string(&req).unwrap();
    let recovered: Request = serde_json::from_str(&json).unwrap();
    
    if let Request::Insert { key, value } = recovered {
        assert_eq!(key, "");
        assert_eq!(value, "");
    } else {
        panic!("Expected Insert request");
    }
}

#[test]
fn test_unicode_in_key_value() {
    let req = Request::Insert {
        key: "Τραγούδι 🎵".to_string(),
        value: "Καλλιτέχνης 🎤".to_string(),
    };
    let json = serde_json::to_string(&req).unwrap();
    let recovered: Request = serde_json::from_str(&json).unwrap();
    
    if let Request::Insert { key, value } = recovered {
        assert_eq!(key, "Τραγούδι 🎵");
        assert_eq!(value, "Καλλιτέχνης 🎤");
    } else {
        panic!("Expected Insert request");
    }
}

#[test]
fn test_special_characters_in_error() {
    let resp = Response::Error {
        message: "Error: \"quotes\" and 'apostrophes' and \\ backslash".to_string(),
    };
    let json = serde_json::to_string(&resp).unwrap();
    let recovered: Response = serde_json::from_str(&json).unwrap();
    
    if let Response::Error { message } = recovered {
        assert!(message.contains("quotes"));
        assert!(message.contains("apostrophes"));
        assert!(message.contains("backslash"));
    } else {
        panic!("Expected Error response");
    }
}
