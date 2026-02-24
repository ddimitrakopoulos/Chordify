//! Node ID - 160-bit identifier using SHA-1 hash
//!
//! NodeId is always derived from a node's IP address and port (SocketAddr).
//! It provides the key for positioning nodes on the Chord ring.

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
    /// Generate NodeId from a socket address (ip:port)
    pub fn from_address(addr: &SocketAddr) -> Self {
        let mut hasher = Sha1::new();
        hasher.update(addr.to_string().as_bytes());
        let result = hasher.finalize();
        let mut bytes = [0u8; 20];
        bytes.copy_from_slice(&result);
        Self { bytes }
    }

    /// Generate NodeId from a key string (for hashing data keys)
    pub fn from_key(key: &str) -> Self {
        let mut hasher = Sha1::new();
        hasher.update(key.as_bytes());
        let result = hasher.finalize();
        let mut bytes = [0u8; 20];
        bytes.copy_from_slice(&result);
        Self { bytes }
    }

    /// Convert to hex string (full 40 characters)
    pub fn to_hex(&self) -> String {
        self.bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    /// Check if self is in the range (start, end] on the circular ring
    pub fn is_between(&self, start: &NodeId, end: &NodeId) -> bool {
        if start < end {
            self > start && self <= end
        } else {
            // Wrap-around case
            self > start || self <= end
        }
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
