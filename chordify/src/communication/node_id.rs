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
    /// Not associated with any real node; for ring arithmetic only.
    pub fn min() -> Self {
        Self { bytes: [0u8; 20] }
    }

    /// Maximum possible ID (all ones)
    /// Not associated with any real node; for ring arithmetic only.
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

