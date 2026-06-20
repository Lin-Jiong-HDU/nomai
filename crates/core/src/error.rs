//! Core error types. Maps to JSON-RPC error codes at the daemon layer.

use thiserror::Error;
use ulid::Ulid;

#[derive(Error, Debug)]
pub enum CoreError {
    #[error("entry not found: {0}")]
    NotFound(Ulid),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("storage error: {0}")]
    Storage(#[from] rusqlite::Error),

    #[error("migration error: {0}")]
    Migration(String),

    #[error("config error: {0}")]
    Config(String),
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn validation_carries_message() {
        let err = CoreError::Validation("attrs must be a JSON object".into());
        assert!(err.to_string().contains("attrs must be a JSON object"));
    }
}
