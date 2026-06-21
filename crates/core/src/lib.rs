//! Core service layer, data model, and storage for nomai.

pub mod error;
pub mod event_model;
pub mod event_service;
pub mod link_model;
pub mod link_service;
pub mod model;
pub mod service;
pub mod storage;

pub use error::CoreError;
pub use event_model::{Event, ListEventsQuery, ListEventsResult, ListOrder, PurgeQuery};
pub use event_service::EventService;
pub use link_model::{
    CreateLink, Direction, Link, ListLinkQuery, ListLinkResult, NeighborsQuery, NeighborsResult,
};
pub use link_service::LinkService;
pub use model::Entry;
pub use service::{
    CreateEntry, EntryListQuery, EntryListResult, EntryService, FulltextSearchResult,
    SemanticSearchResult, UpdateEntry,
};
