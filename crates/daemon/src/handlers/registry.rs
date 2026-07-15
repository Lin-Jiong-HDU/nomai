//! Handler registry: maps RPC method names to handler instances.
//!
//! Extracted from mod.rs in Spec 8 Plan 2 / F-lib-3 to keep mod.rs
//! focused on test infrastructure.

use std::collections::HashMap;
use std::sync::Arc;

use crate::handlers::{
    attachment, batch, block, cache, chunk, entry, events, index, link, mcp, provider, search,
    sync, system,
};
use crate::rpc::RpcHandler;

/// Build the default method → handler registry for the daemon.
///
/// Returns the full set of built-in JSON-RPC handlers. Plugins may add
/// more via `Daemon::register_handler`.
pub fn registry() -> HashMap<&'static str, Arc<dyn RpcHandler>> {
    let mut m: HashMap<&'static str, Arc<dyn RpcHandler>> = HashMap::new();

    // entry.*
    let h = entry::Create;
    m.insert(h.method(), Arc::new(h));
    let h = entry::Get;
    m.insert(h.method(), Arc::new(h));
    let h = entry::Update;
    m.insert(h.method(), Arc::new(h));
    let h = entry::Delete;
    m.insert(h.method(), Arc::new(h));
    let h = entry::List;
    m.insert(h.method(), Arc::new(h));
    let h = entry::PurgeTransient;
    m.insert(h.method(), Arc::new(h));

    // link.*
    let h = link::Create;
    m.insert(h.method(), Arc::new(h));
    let h = link::Get;
    m.insert(h.method(), Arc::new(h));
    let h = link::Delete;
    m.insert(h.method(), Arc::new(h));
    let h = link::List;
    m.insert(h.method(), Arc::new(h));
    let h = link::Neighbors;
    m.insert(h.method(), Arc::new(h));

    // chunk.* (Plan 4: only Get + List; Create/Update/Delete removed because
    // chunks are auto-derived from blocks.)
    let h = chunk::Get;
    m.insert(h.method(), Arc::new(h));
    let h = chunk::List;
    m.insert(h.method(), Arc::new(h));

    // block.* (Plan 5: block-level RPCs on top of Plan 3 blocks storage.)
    let h = block::Append;
    m.insert(h.method(), Arc::new(h));
    let h = block::Update;
    m.insert(h.method(), Arc::new(h));
    let h = block::Delete;
    m.insert(h.method(), Arc::new(h));
    let h = block::List;
    m.insert(h.method(), Arc::new(h));
    let h = block::Get;
    m.insert(h.method(), Arc::new(h));

    // attachment.* (multimodal-image Plan 3: sibling file read/list)
    let h = attachment::Read;
    m.insert(h.method(), Arc::new(h));
    let h = attachment::List;
    m.insert(h.method(), Arc::new(h));

    // index.* (Plan 5: FS↔SQLite reconciliation. Plan 6: read-only verify.)
    let h = index::Sync;
    m.insert(h.method(), Arc::new(h));
    let h = index::Rebuild;
    m.insert(h.method(), Arc::new(h));
    let h = index::Verify;
    m.insert(h.method(), Arc::new(h));

    // system.* (Plan 6: Spec §12 migration utilities.)
    let h = system::ExportToFs;
    m.insert(h.method(), Arc::new(h));

    // events.*
    let h = events::List;
    m.insert(h.method(), Arc::new(h));
    let h = events::Get;
    m.insert(h.method(), Arc::new(h));
    let h = events::Purge;
    m.insert(h.method(), Arc::new(h));

    // search.*
    let h = search::Fulltext;
    m.insert(h.method(), Arc::new(h));
    let h = search::Semantic;
    m.insert(h.method(), Arc::new(h));

    // provider.*
    let h = provider::List;
    m.insert(h.method(), Arc::new(h));

    // cache.* (embedding cache introspection + management)
    let h = cache::Stats;
    m.insert(h.method(), Arc::new(h));
    let h = cache::Clear;
    m.insert(h.method(), Arc::new(h));

    // mcp.* (lifecycle: initialize / tools/list / tools/call)
    let h = mcp::Initialize;
    m.insert(h.method(), Arc::new(h));
    let h = mcp::ToolsList;
    m.insert(h.method(), Arc::new(h));
    let h = mcp::ToolsCall;
    m.insert(h.method(), Arc::new(h));

    // batch (multi-op atomic transaction)
    let h = batch::Batch;
    m.insert(h.method(), Arc::new(h));

    // sync.* (git-backed multi-device sync)
    let h = sync::Init;
    m.insert(h.method(), Arc::new(h));

    m
}
