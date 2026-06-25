# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] — 2026-06-26

First stable release. API surface is frozen: breaking changes require
a major bump (2.0). See "Migration from 0.1.0" below.

### Added (RPC additive)

- `entry.delete` ack now includes `id` field (Spec 8 Plan 1 / F-entry-1)
- `entry.list` response now includes `has_more` field (Spec 8 Plan 1 / F-entry-4)
- `events.list` response now includes `total` field (Spec 8 Plan 1 / F-events-1)
- `block.list` RPC: list blocks by `entry_id` (Spec 8 Plan 1 / F-block-1)
- `nomai_core::NomaiBlock` alias for `nomai_format::Block` (Spec 8 Plan 2 / F-lib-1)
- `nomai_daemon::DaemonBuilder` fluent constructor (Spec 8 Plan 2 / F-lib-2)
- `nomai_protocol::method::cache::{STATS, CLEAR}` constants (Spec 8 Plan 2 / F-cache-1)

### Changed (lib-side breaking)

- `nomai_core::ListOrder` (event_model layer) renamed to `EventListOrder` (Spec 8 Plan 2 / F-events-2)
- `nomai_core::service::ListOrder` renamed to `EntryListOrder`; now re-exported at crate root (Spec 8 Plan 2 / F-events-2)
- `nomai_daemon::handlers::registry()` moved to `handlers::registry::registry()` (Spec 8 Plan 2 / F-lib-3); short path `handlers::registry()` preserved via re-export
- `batch::error_to_rpc` removed; use `rpc::core_error_to_rpc_ref` (Spec 8 Plan 2 / F-batch-4)
- **Batch per-op error objects now include a `data` field** when the underlying
  `CoreError` carries context (Spec 8 Plan 2 / F-batch-4). For example, a batch
  `entry.get` op that hits a 1001 NotFound now reports
  `{ok: false, error: {code: 1001, message: "...", data: {id: "..."}}}`.
  Previously the `data` field was omitted. Clients that did not inspect `data`
  are unaffected.

### Documentation

- `chunk.{CREATE, DELETE}` constants marked as reserved in `protocol::method` (Spec 8 Plan 3 / F-chunk-1)
- `protocol::error` tests now cover all 6 business codes (Spec 8 Plan 3 / F-err-1)
- `docs/guide.md` and `README.md` synced with 1.0 API surface (Spec 8 Plan 3 / F-doc-2)
- Stale `#[allow(dead_code)]` on `Daemon::search_cache` removed (Spec 8 Plan 3 / F-doc-3)

### Migration from 0.1.0

**RPC consumers (JSON-RPC clients)**: no breaking changes — all 1.0
RPC additions are additive (new optional response fields / new
methods). Old clients ignore new fields.

**lib consumers (Rust crates using `nomai-core` / `nomai-daemon`)**:

1. Replace `use nomai_core::ListOrder;` with the appropriate variant:
   - For `EntryListQuery { order }`: `use nomai_core::EntryListOrder;`
   - For `ListEventsQuery { order }`: `use nomai_core::EventListOrder;`
2. If you called `nomai_daemon::handlers::registry()` — still works via re-export.
3. If you called `batch::error_to_rpc` — switch to `rpc::core_error_to_rpc_ref(&err)` and serialize the returned `RpcError` to `Value` yourself.
4. `Daemon::from_services` is unchanged; new `DaemonBuilder` is optional.

### Non-goals (deferred to 2.0)

The following were considered for 1.0 but deferred (see Spec 8 §7):

- `entry.list include_blocks: Option<bool>` → `bool` (RPC breaking)
- `"batch"` method → `"batch.run"` rename (RPC breaking)

## [0.1.0] — 2026-06-24

First public alpha. The API surface is stabilizing but may change before 1.0.
Single-user, single-process, single SQLite file.

### Added

- **Five primitives** over JSON-RPC 2.0 on NDJSON/stdio:
  - `entry.*` — knowledge notes (one directory per entry on disk)
  - `block.*` — typed semantic blocks within an entry
    (`claim` / `evidence` / `question` / `source` / `note` / `connection`)
  - `link.*` — directed edges between entries
  - `events.*` — append-only mutation log
  - `chunk.*` — block-derived pieces for embedding
- **File-system as source of truth**: every entry has a `.nomai` file on disk;
  SQLite holds the derived index and can be rebuilt via `index.rebuild`.
- **Batch RPC** with `$ref` inter-op references and atomic transactions.
- **MCP server**: native Model Context Protocol compatibility — Claude
  Desktop / Cursor / any MCP client connects directly over stdio.
- **Plugin registry**: `RpcHandler` trait + `register_handler`.
- **Embedding cache**: transparent `CachedEmbedder` wrapper persists
  `(model, blake3(body)) → embedding` in SQLite; repeated bodies skip the
  network API call entirely.
- **Search results cache**: transparent in-memory cache for
  `search.semantic` / `search.fulltext` with generation-based invalidation.
- **lib + daemon dual mode**: embed `nomai-core` directly, or run
  `nomai-daemon` as a stdio service.
- **Examples**: `rag`, `custom_rpc`, `import_markdown`, `graph_rag`,
  `events_sync`, `block_lifecycle`, `index_management`.

### Breaking database migrations (run automatically on next boot)

> **If you have a database from before 2026-06-23, these migrations are
> destructive and irreversible. Back up `db.sqlite` before starting.**

- **V7**: drops `entries.body` (entries no longer have a single body field —
  they have typed blocks instead).
- **V8**: drops `vec_embeddings` (per-entry embeddings retired),
  `fts_entries` (per-entry FTS retired, replaced by per-block `fts_blocks`),
  and `chunks.entry_id` (chunks are now block-addressed).

After these migrations run, existing entries without typed blocks will have
empty `.nomai` files. To regenerate from current state:

1. Stop the daemon.
2. Back up `db.sqlite`.
3. Start the daemon — migrations run automatically.
4. Call `system.export_to_fs` to generate `.nomai` files.
5. Call `index.rebuild` to re-derive chunks + FTS + embeddings.

### Known limitations

- Single-user, single-process. No concurrency control beyond SQLite's own.
- No built-in sync (the `events` primitive is the substrate; build on top).
- No CLI subcommands — every operation is an RPC over stdio.

[Unreleased]: https://github.com/Lin-Jiong-HDU/nomai/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/Lin-Jiong-HDU/nomai/releases/tag/v1.0.0
[0.1.0]: https://github.com/Lin-Jiong-HDU/nomai/releases/tag/v0.1.0
