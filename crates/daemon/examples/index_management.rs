//! Index management demo: verify -> drift -> sync -> rebuild -> export_to_fs.
//!
//! Shows the FS-as-truth invariant from Spec 6:
//! - `index.verify` reports drift categories without mutating
//! - external FS edits (dropping a `.nomai` in) are detected on next verify/sync
//! - `index.sync` reconciles FS <-> index (add/update/remove)
//! - `index.rebuild` wipes derived tables and reindexes every FS entry
//! - `system.export_to_fs` regenerates missing `.nomai` for DB-only entries
//!
//! Usage:
//!     cargo run --example index_management
//!
//! No network required. Uses an in-memory DB + a per-run temp dir for the
//! content store (cleaned up at process exit by the OS) so running this
//! example never pollutes the user's real knowledge store.

use std::sync::Arc;

use rusqlite::Connection;

use nomai_core::{
    BlockInput, ContentStore, CreateEntry, EntryService, NomaiDoc,
    nomai_format::{Block as ParserBlock, BlockType},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize SQLite extensions (sqlite-vec) required by migrations.
    nomai_core::storage::init_sqlite_extensions();

    // Use a per-run temp dir rather than default_knowledge_root() so we never
    // pollute the user's real knowledge store by running this example.
    let knowledge_root =
        std::env::temp_dir().join(format!("nomai-index-management-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&knowledge_root)?;
    let content_store = Arc::new(ContentStore::new(knowledge_root));

    let conn = Arc::new(std::sync::Mutex::new(Connection::open_in_memory()?));
    let entries = EntryService::new(conn.clone(), content_store.clone())?;

    println!("=== Create one entry via service (writes .nomai + indexes) ===\n");
    let entry = entries.create(CreateEntry {
        title: "Indexed Entry".into(),
        blocks: vec![BlockInput {
            r#type: "note".into(),
            text: "Created via EntryService.".into(),
            attrs: None,
        }],
        tags: None,
        attrs: None,
        source: None,
    })?;
    println!("Created entry {}", entry.id);

    println!("\n=== index.verify (clean state) ===\n");
    let verify_result = entries.verify_fs()?;
    println!(
        "  fs_only={}, db_only={}, stale_mtime={}, consistent={}",
        verify_result.fs_only,
        verify_result.db_only,
        verify_result.stale_mtime,
        verify_result.consistent
    );
    assert_eq!(verify_result.consistent, 1, "entry should be consistent");

    println!("\n=== Drop a new .nomai directly into entries/ (FS drift) ===\n");
    let orphan_id = ulid::Ulid::new();
    let orphan_doc = NomaiDoc {
        format_version: 1,
        id: orphan_id,
        title: "External Orphan".into(),
        tags: vec![],
        attrs: Default::default(),
        source: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        blocks: vec![ParserBlock {
            r#type: BlockType::Note,
            text: "I was dropped in via FS.\n".into(),
            attrs: Default::default(),
        }],
    };
    content_store.write_entry(orphan_id, &orphan_doc)?;
    println!("Dropped .nomai for orphan {}", orphan_id);

    println!("\n=== index.verify (drift detected) ===\n");
    let verify_result = entries.verify_fs()?;
    println!(
        "  fs_only={}, db_only={}, stale_mtime={}, consistent={}",
        verify_result.fs_only,
        verify_result.db_only,
        verify_result.stale_mtime,
        verify_result.consistent
    );
    assert_eq!(
        verify_result.fs_only, 1,
        "orphan should be detected as fs_only"
    );
    assert_eq!(
        verify_result.consistent, 1,
        "original entry still consistent"
    );

    println!("\n=== index.sync (reconcile FS -> index) ===\n");
    let sync_result = entries.sync_from_fs()?;
    println!(
        "  added={}, updated={}, removed={}, unchanged={}",
        sync_result.added, sync_result.updated, sync_result.removed, sync_result.unchanged
    );
    assert_eq!(sync_result.added, 1, "orphan should be indexed");

    let orphan_entry = entries.get(orphan_id)?;
    println!("  orphan now retrievable: title={:?}", orphan_entry.title);

    println!("\n=== index.rebuild (wipe derived + reindex every FS entry) ===\n");
    let rebuild_result = entries.rebuild_index()?;
    println!(
        "  reindexed={}, errors={}",
        rebuild_result.reindexed,
        rebuild_result.errors.len()
    );
    assert_eq!(rebuild_result.reindexed, 2, "both entries should reindex");

    println!("\n=== system.export_to_fs (idempotent on entries with .nomai) ===\n");
    let export_result = entries.export_to_fs()?;
    println!(
        "  exported={}, skipped={}, errors={}",
        export_result.exported,
        export_result.skipped,
        export_result.errors.len()
    );
    assert_eq!(
        export_result.exported, 0,
        "both entries already have .nomai"
    );
    assert_eq!(export_result.skipped, 2);

    println!("\nDone. FS as source of truth works end-to-end.");

    Ok(())
}
