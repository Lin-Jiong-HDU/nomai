//! nomai-daemon library: knowledge-core daemon internals.

pub(crate) mod adaptive_search;
pub(crate) mod benchmark;
pub mod config;
pub mod daemon;
pub mod handlers;
pub mod io;
pub mod rpc;
pub mod search_cache;
pub mod serve;
pub mod shim;
pub mod socket;
