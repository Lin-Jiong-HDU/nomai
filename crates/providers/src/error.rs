//! Re-export ProviderError from the protocol crate so provider implementations
//! don't have to reference `nomai_protocol` directly.

pub use nomai_protocol::{ProviderError, ProviderErrorKind};
