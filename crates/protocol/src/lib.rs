//! Wire-format types for the nomai JSON-RPC protocol.
//!
//! This crate has no internal dependencies and contains no logic — only
//! types, constants, and serde derives.

pub mod error;
pub mod method;
pub mod rpc;

pub use rpc::{Id, JSONRPC_VERSION, Request, Response, RpcError};
