//! Nodes module for Chordify
//!
//! This module provides node identity (NodeId) and related utilities.
//! Higher-level Chord DHT logic will be built on top of the communication layer.

mod node_id;

pub use self::node_id::NodeId;
