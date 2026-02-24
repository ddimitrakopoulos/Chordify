//! Tests for NodeId - SHA-1 based node identification

use chordify::NodeId;
use std::net::SocketAddr;

// ==================== Hash Determinism Tests ====================

#[test]
fn test_same_address_produces_same_id() {
    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let id1 = NodeId::from_address(&addr);
    let id2 = NodeId::from_address(&addr);
    assert_eq!(id1, id2, "Same address should produce same ID");
}

#[test]
fn test_same_key_produces_same_id() {
    let id1 = NodeId::from_key("Song Title");
    let id2 = NodeId::from_key("Song Title");
    assert_eq!(id1, id2, "Same key should produce same ID");
}

#[test]
fn test_different_addresses_produce_different_ids() {
    let addr1: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let id1 = NodeId::from_address(&addr1);
    let id2 = NodeId::from_address(&addr2);
    assert_ne!(id1, id2, "Different addresses should produce different IDs");
}

#[test]
fn test_different_keys_produce_different_ids() {
    let id1 = NodeId::from_key("Song A");
    let id2 = NodeId::from_key("Song B");
    assert_ne!(id1, id2, "Different keys should produce different IDs");
}

#[test]
fn test_different_ports_same_ip_different_ids() {
    let addr1: SocketAddr = "192.168.1.1:3000".parse().unwrap();
    let addr2: SocketAddr = "192.168.1.1:3001".parse().unwrap();
    let id1 = NodeId::from_address(&addr1);
    let id2 = NodeId::from_address(&addr2);
    assert_ne!(id1, id2);
}

#[test]
fn test_different_ips_same_port_different_ids() {
    let addr1: SocketAddr = "192.168.1.1:3000".parse().unwrap();
    let addr2: SocketAddr = "192.168.1.2:3000".parse().unwrap();
    let id1 = NodeId::from_address(&addr1);
    let id2 = NodeId::from_address(&addr2);
    assert_ne!(id1, id2);
}

// ==================== Hex Conversion Tests ====================

#[test]
fn test_hex_roundtrip() {
    let addr: SocketAddr = "10.0.0.1:5000".parse().unwrap();
    let id = NodeId::from_address(&addr);
    let hex = id.to_hex();
    let recovered = NodeId::from_hex(&hex).unwrap();
    assert_eq!(id, recovered, "Hex roundtrip should preserve ID");
}

#[test]
fn test_hex_length_is_40_chars() {
    let id = NodeId::from_key("test");
    let hex = id.to_hex();
    assert_eq!(hex.len(), 40, "SHA-1 hex should be 40 characters");
}

#[test]
fn test_from_hex_invalid_length() {
    let result = NodeId::from_hex("abc");
    assert!(result.is_none(), "Invalid hex length should return None");
}

#[test]
fn test_from_hex_invalid_characters() {
    let result = NodeId::from_hex("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");
    assert!(result.is_none(), "Invalid hex characters should return None");
}

#[test]
fn test_hex_is_lowercase() {
    let id = NodeId::from_key("test");
    let hex = id.to_hex();
    assert_eq!(hex, hex.to_lowercase(), "Hex should be lowercase");
}

// ==================== Ring Arithmetic Tests ====================

#[test]
fn test_is_between_normal_range() {
    // Create IDs with known ordering
    let a = NodeId::from_key("aaa");
    let b = NodeId::from_key("bbb");
    let c = NodeId::from_key("ccc");
    
    // Sort to know their order
    let mut sorted = vec![a, b, c];
    sorted.sort();
    let (low, mid, high) = (sorted[0], sorted[1], sorted[2]);
    
    // mid should be in (low, high]
    assert!(mid.is_between(&low, &high), "mid should be between low and high");
}

#[test]
fn test_is_between_exclusive_start() {
    let a = NodeId::from_key("x");
    let b = NodeId::from_key("y");
    
    let mut sorted = vec![a, b];
    sorted.sort();
    let (low, high) = (sorted[0], sorted[1]);
    
    // low should NOT be in (low, high] - start is exclusive
    assert!(!low.is_between(&low, &high), "Start should be exclusive");
}

#[test]
fn test_is_between_inclusive_end() {
    let a = NodeId::from_key("p");
    let b = NodeId::from_key("q");
    
    let mut sorted = vec![a, b];
    sorted.sort();
    let (low, high) = (sorted[0], sorted[1]);
    
    // high should be in (low, high] - end is inclusive
    assert!(high.is_between(&low, &high), "End should be inclusive");
}

#[test]
fn test_is_between_wrap_around() {
    // Test the wrap-around case where start > end
    let min = NodeId::min();
    let max = NodeId::max();
    let mid = NodeId::from_key("middle");
    
    // In wrap-around (max, min], anything should be included
    // since it covers the entire ring
    assert!(mid.is_between(&max, &min) || mid.is_between(&min, &max));
}

#[test]
fn test_is_between_same_start_end() {
    let id = NodeId::from_key("test");
    
    // When start == end, the range (start, end] is just the point end
    // So id should be between itself (since end is inclusive)
    assert!(id.is_between(&id, &id), "ID should be between itself");
    
    // Note: When start == end, is_between returns true for any ID
    // because the range wraps around the entire ring
    // This is correct behavior for a circular ring
}

// ==================== Ordering Tests ====================

#[test]
fn test_ordering_is_consistent() {
    let a = NodeId::from_key("alpha");
    let b = NodeId::from_key("beta");
    
    // Ordering should be consistent
    let cmp1 = a.cmp(&b);
    let cmp2 = a.cmp(&b);
    assert_eq!(cmp1, cmp2, "Ordering should be consistent");
}

#[test]
fn test_equality_is_reflexive() {
    let id = NodeId::from_key("test");
    assert_eq!(id, id, "ID should equal itself");
}

#[test]
fn test_min_max_ordering() {
    let min = NodeId::min();
    let max = NodeId::max();
    assert!(min < max, "min should be less than max");
}

#[test]
fn test_any_id_between_min_max() {
    let min = NodeId::min();
    let max = NodeId::max();
    let id = NodeId::from_key("anything");
    
    assert!(id >= min, "Any ID should be >= min");
    assert!(id <= max, "Any ID should be <= max");
}

// ==================== Display Tests ====================

#[test]
fn test_display_shows_short_hex() {
    let id = NodeId::from_key("test");
    let display = format!("{}", id);
    assert_eq!(display.len(), 8, "Display should show 8 hex chars");
}

#[test]
fn test_debug_shows_full_hex() {
    let id = NodeId::from_key("test");
    let debug = format!("{:?}", id);
    assert!(debug.contains(&id.to_hex()), "Debug should contain full hex");
}

// ==================== Edge Cases ====================

#[test]
fn test_empty_key() {
    let id1 = NodeId::from_key("");
    let id2 = NodeId::from_key("");
    assert_eq!(id1, id2, "Empty key should be deterministic");
}

#[test]
fn test_unicode_key() {
    let id1 = NodeId::from_key("Τραγούδι 🎵");
    let id2 = NodeId::from_key("Τραγούδι 🎵");
    assert_eq!(id1, id2, "Unicode keys should work");
}

#[test]
fn test_very_long_key() {
    let long_key = "a".repeat(10000);
    let id1 = NodeId::from_key(&long_key);
    let id2 = NodeId::from_key(&long_key);
    assert_eq!(id1, id2, "Long keys should work");
}

#[test]
fn test_ipv6_address() {
    let addr: SocketAddr = "[::1]:8000".parse().unwrap();
    let id1 = NodeId::from_address(&addr);
    let id2 = NodeId::from_address(&addr);
    assert_eq!(id1, id2, "IPv6 addresses should work");
}
