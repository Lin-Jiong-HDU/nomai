//! Core service layer, data model, and storage for nomai.

pub mod chunk_model;
pub mod chunk_service;
pub mod error;
pub mod event_model;
pub mod event_service;
pub mod link_model;
pub mod link_service;
pub mod model;
pub mod nomai_format;
pub mod service;
pub mod storage;

pub use chunk_model::{Chunk, ChunkListResult, ChunkSearchResult, CreateChunk, Granularity};
pub use chunk_service::ChunkService;
pub use error::CoreError;
pub use event_model::{Event, ListEventsQuery, ListEventsResult, ListOrder, PurgeQuery};
pub use event_service::EventService;
pub use link_model::{
    CreateLink, Direction, Link, ListLinkQuery, ListLinkResult, NeighborsQuery, NeighborsResult,
};
pub use link_service::LinkService;
pub use model::Entry;
pub use nomai_format::{
    Block, BlockType, NomaiDoc, ParseError, parse as parse_nomai, render as render_nomai,
};
pub use service::{
    CreateEntry, EntryListQuery, EntryListResult, EntryService, FulltextSearchResult,
    SemanticSearchResult, UpdateEntry,
};
