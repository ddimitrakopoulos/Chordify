//! Node ID generation using SHA-1 hash
//!
//! Phase 2: ID Generation
//! Generates a unique 160-bit ID from ip_address:port using SHA-1
//!
//! This module implements NodeId generation and ring arithmetic.
//! NodeId is always derived from the node's ip address and port (SocketAddr).
//! It can also be generated from arbitrary keys (e.g., song titles) for hashing purposes.
//! The NodeId type provides methods for conversion to/from byte arrays and hex strings,
//! as well as utilities for circular range checking on the ID ring.
//!
//! # Examples
//!
//! ```rust
//! use chordify::NodeId;
//! use std::net::SocketAddr;
//! let addr: SocketAddr = "192.168.1.1:8080".parse().unwrap();
//! let node_id = NodeId::from_address(&addr);
//! println!("Node ID: {}", node_id.to_hex());
//! ```
//! 
//! ```rust
//! use chordify::NodeId;
//! let key = "Some string key";
//! let node_id = NodeId::from_key(key);
//! println!("Node ID from key: {}", node_id.to_hex());
//! ```
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
    /// 
    /// # Arguments
    ///
    /// * `bytes` - A 20-byte array representing the raw bytes of the NodeId.
    /// 
    /// # Returns
    ///
    /// * A new NodeId instance with the given bytes.
    pub fn from_bytes(bytes: [u8; 20]) -> Self {
        Self { bytes }
    }

    /// Get the raw bytes
    /// 
    /// # Returns
    ///
    /// * A reference to the 20-byte array representing the raw bytes of the NodeId.
    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.bytes
    }

    /// Generate NodeId from a socket address (ip:port)
    /// 
    /// # Arguments
    ///
    /// * `addr` - A reference to a SocketAddr instance representing the IP address and port.
    /// 
    /// # Returns
    ///
    /// * A new NodeId instance generated from the given socket address.
    pub fn from_address(addr: &SocketAddr) -> Self {
        let mut hasher = Sha1::new();
        hasher.update(addr.to_string().as_bytes());
        let result = hasher.finalize();
        let mut bytes = [0u8; 20];
        bytes.copy_from_slice(&result);
        Self { bytes }
    }

    /// Generate NodeId from a key string (for hashing song titles)
    /// 
    /// # Arguments
    ///
    /// * `key` - A string slice representing the key to hash.
    /// 
    /// # Returns
    ///
    /// * A new NodeId instance generated from the given key.
    pub fn from_key(key: &str) -> Self {
        let mut hasher = Sha1::new();
        hasher.update(key.as_bytes());
        let result = hasher.finalize();
        let mut bytes = [0u8; 20];
        bytes.copy_from_slice(&result);
        Self { bytes }
    }

    /// Convert to hex string
    /// 
    /// # Returns
    ///
    /// * A String containing the hexadecimal representation of the NodeId.
    pub fn to_hex(&self) -> String {
        self.bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    /// Create from hex string
    /// 
    /// # Arguments
    ///
    /// * `hex` - A string slice representing the hexadecimal string to convert.
    /// 
    /// # Returns
    ///
    /// * An Option containing the new NodeId instance if successful, or None if the input is invalid.
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
    /// 
    /// # Arguments
    ///
    /// * `start` - A reference to the starting NodeId of the range.
    /// * `end` - A reference to the ending NodeId of the range.
    /// 
    /// # Returns
    ///
    /// * true if self is in the range (start, end], false otherwise.
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
    /// 
    /// # Returns
    ///
    /// * A NodeId instance representing the minimum possible ID.
    pub fn min() -> Self {
        Self { bytes: [0u8; 20] }
    }

    /// Maximum possible ID (all ones)
    /// Not associated with any real node; for ring arithmetic only.
    /// 
    /// # Returns
    ///
    /// * A NodeId instance representing the maximum possible ID.
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

