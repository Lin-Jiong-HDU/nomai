//! Core service layer, data model, and storage for nomai.

pub mod block_model;
pub mod block_service;
pub mod chunk_model;
pub mod chunk_service;
pub mod chunking;
pub mod content_store;
pub mod conversation_model;
pub mod conversation_service;
pub mod error;
pub mod event_model;
pub mod event_service;
pub mod fts_query;
pub mod link_model;
pub mod link_service;
pub mod memory_model;
pub mod memory_service;
pub mod model;
pub mod nomai_format;
pub mod nomai_format_util;
pub mod service;
pub mod snippet;
pub mod storage;

pub use block_model::{Block, BlockInput, BlockListResult, CreateBlock};
pub use block_service::BlockService;
pub use chunk_model::{Chunk, ChunkListResult, ChunkSearchResult, DimReconciliation};
pub use chunk_service::ChunkService;
pub use content_store::{AttachmentMeta, ContentStore};
pub use conversation_model::{
    AppendTurns, Conversation, ConversationListOrder, ConversationListQuery,
    ConversationListResult, ConversationSearchResult, ConversationWithTurns, CreateConversation,
    CreateTurn, Turn, UpdateConversation,
};
pub use conversation_service::ConversationService;
pub use error::CoreError;
pub use event_model::{Event, EventListOrder, ListEventsQuery, ListEventsResult, PurgeQuery};
pub use event_service::EventService;
pub use link_model::{
    CreateLink, Direction, Link, ListLinkQuery, ListLinkResult, NeighborsQuery, NeighborsResult,
};
pub use link_service::LinkService;
pub use memory_model::{
    AffinityEmbeddingInput, AffinityEmbeddingPlan, AffinityHit, AppliedFeedback, Clock,
    CreateSearchSession, EntryMemorySignal, FeedbackResult, FeedbackTarget, MemoryPolicy,
    SearchResultTarget, SystemClock,
};
pub use memory_service::MemorySignalsService;
pub use model::Entry;
pub use nomai_format::{
    Block as NomaiBlock, BlockType, NomaiDoc, ParseError, parse as parse_nomai,
    render as render_nomai,
};
pub use service::{
    BestMatch, BlockRef, ChunkRef, CreateEntry, EntryListOrder, EntryListQuery, EntryListResult,
    EntryService, ExportResult, FulltextSearchResult, HybridSearchResult, RebuildResult,
    SyncResult, UpdateEntry, VerifyResult, rrf_fuse,
};

#[cfg(test)]
mod integration_tests {
    use crate::{Block, BlockService, ContentStore, CreateBlock, NomaiBlock};

    #[test]
    fn new_types_are_accessible_from_crate_root() {
        // Compile-time check: all new public types are exported from crate root.
        let _ = std::marker::PhantomData::<Block>;
        let _ = std::marker::PhantomData::<CreateBlock>;
        let _ = std::marker::PhantomData::<BlockService>;
        let _ = std::marker::PhantomData::<ContentStore>;
        // parser Block re-exported as NomaiBlock.
        let _ = std::marker::PhantomData::<NomaiBlock>;
    }

    // Prove NomaiBlock IS the parser
    // `nomai_format::Block`, not the storage `block_model::Block`. PhantomData
    // alone couldn't distinguish — both are valid types named via the alias.
    // If the alias ever drifts to `block_model::Block`, this stops compiling.
    const _: fn(crate::nomai_format::Block) -> NomaiBlock = |b| b;
}
