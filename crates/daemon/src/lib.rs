//! nomai-daemon library: knowledge-core daemon internals.

pub mod config;
pub mod daemon;
pub mod handlers;
pub mod io;
pub mod rpc;
pub mod search_cache;
#[cfg(unix)]
pub mod serve;
#[cfg(unix)]
pub mod socket;
