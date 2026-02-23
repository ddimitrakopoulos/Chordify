//! Node ID generation using SHA-1 hash
//! 
//! Phase 2: ID Generation
//! Generates a unique 160-bit ID from ip_address:port using SHA-1

use sha1::{Sha1, Digest};
use std::fmt;
use std::net::SocketAddr;
use serde::{Serialize, Deserialize};

/// 160-bit Node ID generated from SHA-1 hash
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId {
    bytes: [u8; 20],
}

impl NodeId {
    /// Create a NodeId from raw bytes
    pub fn from_bytes(bytes: [u8; 20]) -> Self {
        Self { bytes }
    }

    /// Get the raw bytes
    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.bytes
    }

    /// Generate NodeId from a socket address (ip:port)
    pub fn from_address(addr: &SocketAddr) -> Self {
        let mut hasher = Sha1::new();
        hasher.update(addr.to_string().as_bytes());
        let result = hasher.finalize();
        let mut bytes = [0u8; 20];
        bytes.copy_from_slice(&result);
        Self { bytes }
    }

    /// Generate NodeId from a key string (for hashing song titles)
    pub fn from_key(key: &str) -> Self {
        let mut hasher = Sha1::new();
        hasher.update(key.as_bytes());
        let result = hasher.finalize();
        let mut bytes = [0u8; 20];
        bytes.copy_from_slice(&result);
        Self { bytes }
    }

    /// Convert to hex string
    pub fn to_hex(&self) -> String {
        self.bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    /// Create from hex string
    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 40 {
            return None;
        }
        let mut bytes = [0u8; 20];
        for i in 0..20 {
            bytes[i] = u8::from_str_radix(&hex[i*2..i*2+2], 16).ok()?;
        }
        Some(Self { bytes })
    }

    /// Check if self is in the range (start, end] on the circular ring
    /// Handles wrap-around case
    pub fn is_between(&self, start: &NodeId, end: &NodeId) -> bool {
        if start < end {
            self > start && self <= end
        } else {
            // Wrap-around: (start, MAX] or [MIN, end]
            self > start || self <= end
        }
    }

    /// Minimum possible ID (all zeros)
    pub fn min() -> Self {
        Self { bytes: [0u8; 20] }
    }

    /// Maximum possible ID (all ones)
    pub fn max() -> Self {
        Self { bytes: [0xff; 20] }
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Show first 8 hex characters for brevity
        write!(f, "{}", &self.to_hex()[..8])
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn test_from_address_deterministic() {
        let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
        let id1 = NodeId::from_address(&addr);
        let id2 = NodeId::from_address(&addr);
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_different_addresses_different_ids() {
        let addr1: SocketAddr = "127.0.0.1:8000".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8001".parse().unwrap();
        let id1 = NodeId::from_address(&addr1);
        let id2 = NodeId::from_address(&addr2);
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_hex_roundtrip() {
        let addr: SocketAddr = "192.168.1.1:3000".parse().unwrap();
        let id = NodeId::from_address(&addr);
        let hex = id.to_hex();
        let recovered = NodeId::from_hex(&hex).unwrap();
        assert_eq!(id, recovered);
    }

    #[test]
    fn test_is_between() {
        let a = NodeId::from_key("a");
        let b = NodeId::from_key("b");
        let c = NodeId::from_key("c");
        
        // Sort them to know their order
        let mut sorted = [a, b, c];
        sorted.sort();
        let (low, mid, high) = (sorted[0], sorted[1], sorted[2]);
        
        // mid should be between low and high
        assert!(mid.is_between(&low, &high));
        // low should not be between low and high (exclusive start)
        assert!(!low.is_between(&low, &high));
    }
}
