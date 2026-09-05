//! Core error types. Maps to JSON-RPC error codes at the daemon layer.

use thiserror::Error;
use ulid::Ulid;

#[derive(Error, Debug)]
pub enum CoreError {
    #[error("entry not found: {0}")]
    NotFound(Ulid),

    #[error("{resource} not found: {id}")]
    ResourceNotFound { resource: &'static str, id: Ulid },

    #[error("validation error: {0}")]
    Validation(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("provider error: {0}")]
    Provider(#[from] nomai_protocol::ProviderError),

    #[error("storage error: {0}")]
    Storage(#[from] rusqlite::Error),

    #[error("migration error: {0}")]
    Migration(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("nomai format error: {0}")]
    NomaiFormat(#[from] crate::nomai_format::ParseError),

    #[error("config error: {0}")]
    Config(String),

    #[error("sync conflict: {message}")]
    SyncConflict {
        message: String,
        conflicted_files: Vec<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use nomai_protocol::{ProviderError, ProviderErrorKind};

    #[test]
    fn not_found_displays_ulid() {
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let err = CoreError::NotFound(id);
        assert_eq!(
            err.to_string(),
            "entry not found: 01ARZ3NDEKTSV4RRFFQ69G5FAV"
        );
    }

    #[test]
    fn resource_not_found_displays_resource_and_ulid() {
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let err = CoreError::ResourceNotFound {
            resource: "search session",
            id,
        };
        assert_eq!(
            err.to_string(),
            "search session not found: 01ARZ3NDEKTSV4RRFFQ69G5FAV"
        );
    }

    #[test]
    fn conflict_displays_message() {
        let err = CoreError::Conflict("search session expired".into());
        assert_eq!(err.to_string(), "conflict: search session expired");
    }

    #[test]
    fn validation_carries_message() {
        let err = CoreError::Validation("attrs must be a JSON object".into());
        assert!(err.to_string().contains("attrs must be a JSON object"));
    }

    #[test]
    fn provider_variant_via_from() {
        let p = ProviderError::new(ProviderErrorKind::Auth, "bad key", Some(401));
        let core: CoreError = p.into();
        assert!(matches!(core, CoreError::Provider(_)));
        assert!(core.to_string().contains("bad key"));
    }

    #[test]
    fn io_variant_via_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        let core: CoreError = io_err.into();
        assert!(matches!(core, CoreError::Io(_)));
        assert!(core.to_string().contains("missing"));
    }

    #[test]
    fn nomai_format_variant_via_from() {
        let parse_err = crate::nomai_format::ParseError::EmptyInput;
        let core: CoreError = parse_err.into();
        assert!(matches!(core, CoreError::NomaiFormat(_)));
        assert!(core.to_string().contains("empty input"));
    }
}
