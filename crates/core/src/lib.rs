//! Core service layer, data model, and storage for nomai.

pub mod block_model;
pub mod block_service;
pub mod chunk_model;
pub mod chunk_service;
pub mod chunking;
pub mod content_store;
pub mod error;
pub mod event_model;
pub mod event_service;
pub mod link_model;
pub mod link_service;
pub mod model;
pub mod nomai_format;
pub mod nomai_format_util;
pub mod service;
pub mod storage;

pub use block_model::{Block, BlockInput, BlockListResult, CreateBlock};
pub use block_service::BlockService;
pub use chunk_model::{Chunk, ChunkListResult, ChunkSearchResult, DimReconciliation};
pub use chunk_service::ChunkService;
pub use content_store::ContentStore;
pub use error::CoreError;
pub use event_model::{Event, ListEventsQuery, ListEventsResult, ListOrder, PurgeQuery};
pub use event_service::EventService;
pub use link_model::{
    CreateLink, Direction, Link, ListLinkQuery, ListLinkResult, NeighborsQuery, NeighborsResult,
};
pub use link_service::LinkService;
pub use model::Entry;
pub use nomai_format::{
    BlockType, NomaiDoc, ParseError, parse as parse_nomai, render as render_nomai,
};
pub use service::{
    CreateEntry, EntryListQuery, EntryListResult, EntryService, ExportResult, FulltextSearchResult,
    RebuildResult, SyncResult, UpdateEntry,
};

#[cfg(test)]
mod integration_tests {
    use crate::{Block, BlockService, ContentStore, CreateBlock};

    #[test]
    fn new_types_are_accessible_from_crate_root() {
        // Compile-time check: all new public types are exported from crate root.
        let _ = std::marker::PhantomData::<Block>;
        let _ = std::marker::PhantomData::<CreateBlock>;
        let _ = std::marker::PhantomData::<BlockService>;
        let _ = std::marker::PhantomData::<ContentStore>;
    }
}
