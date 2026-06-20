//! Core service layer, data model, and storage for nomai.

pub mod error;
pub mod model;
pub mod service;
pub mod storage;

pub use error::CoreError;
pub use model::Entry;
// Re-exports from `service` (CreateEntry, EntryService, etc.) are added in Tasks 2–5
// once those types exist; the list above is preserved verbatim from the brief and
// uncommented when the service module is populated.
