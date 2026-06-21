//! Core service layer, data model, and storage for nomai.

pub mod error;
pub mod link_model;
pub mod link_service;
pub mod model;
pub mod service;
pub mod storage;

pub use error::CoreError;
pub use link_model::{
    CreateLink, Direction, Link, ListLinkQuery, ListLinkResult, NeighborsQuery, NeighborsResult,
};
pub use link_service::LinkService;
pub use model::Entry;
pub use service::{
    CreateEntry, EntryListQuery, EntryListResult, EntryService, FulltextSearchResult, ListOrder,
    SemanticSearchResult, UpdateEntry,
};
