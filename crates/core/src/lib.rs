//! Core service layer, data model, and storage for nomai.

pub mod error;
pub mod model;
pub mod service;
pub mod storage;

pub use error::CoreError;
pub use model::Entry;
pub use service::{CreateEntry, EntryService};
