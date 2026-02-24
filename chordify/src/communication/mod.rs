//! Communication module for Chordify
//!
//! A general-purpose P2P messaging layer for peer-to-peer communication.
//! 
//! This module provides low-level primitives for connecting to peers,
//! sending messages, and receiving answers. It is protocol-agnostic:
//! message types and semantics are handled at a higher level.
//!
//! # Design
//! - Peers are identified purely by their IP address and port (SocketAddr).
//! - No client/server distinction: all nodes are equal peers.
//! - Messages are raw bytes; serialization is the caller's responsibility.
//! - Provides: connect, send, answer primitives.

mod peer;

pub use peer::{Peer, Connection};
