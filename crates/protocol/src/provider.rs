//! Provider error type shared across crates. Lives in `protocol` because it
//! appears on the JSON-RPC wire as `error.data` for code `1002`.

use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderErrorKind {
    Auth,
    RateLimit,
    Network,
    Server,
    Unknown,
}

impl fmt::Display for ProviderErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Mirror the serde snake_case rendering so the wire string and the
        // human-readable Display form agree.
        match self {
            ProviderErrorKind::Auth => write!(f, "auth"),
            ProviderErrorKind::RateLimit => write!(f, "rate_limit"),
            ProviderErrorKind::Network => write!(f, "network"),
            ProviderErrorKind::Server => write!(f, "server"),
            ProviderErrorKind::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
#[error("provider error ({kind}): {message}")]
pub struct ProviderError {
    pub kind: ProviderErrorKind,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
}

impl ProviderError {
    pub fn new(
        kind: ProviderErrorKind,
        message: impl Into<String>,
        status: Option<u16>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            status,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&ProviderErrorKind::RateLimit).unwrap(),
            r#""rate_limit""#
        );
    }

    #[test]
    fn error_serializes_with_optional_status() {
        let with_status = ProviderError::new(
            ProviderErrorKind::Auth,
            "bad key",
            Some(401),
        );
        let s = serde_json::to_string(&with_status).unwrap();
        assert!(s.contains(r#""kind":"auth""#));
        assert!(s.contains(r#""status":401"#));

        let no_status = ProviderError::new(
            ProviderErrorKind::Network,
            "timeout",
            None,
        );
        let s = serde_json::to_string(&no_status).unwrap();
        assert!(!s.contains(r#""status""#));
    }

    #[test]
    fn error_implements_std_error() {
        let err = ProviderError::new(ProviderErrorKind::Server, "boom", Some(500));
        let _: &dyn std::error::Error = &err;
        assert!(err.to_string().contains("provider error (server)"));
    }
}
