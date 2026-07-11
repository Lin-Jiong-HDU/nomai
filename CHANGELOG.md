# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1] - 2026-07-11

### Added

- **`search.fulltext` results now carry match evidence (#5).** Each result
  adds `match_count` (number of matching blocks), `matched_block_ids`
  (soft-capped at 64), and `best_match { block_id, block_type, snippet }` —
  a ~120-char window centered on the hit with the matched substring wrapped
  in markdown `**…**`, with `…` at truncated ends (CJK-safe, char-based).
  All fields are additive; existing clients are unaffected. This closes the
  "a correct hit looks like a bug" blind spot behind #4 (where
  `search.fulltext` always returning the README entry was a misdiagnosis —
  the entry genuinely contained the tokens; the result just didn't show
  *why*). `match_count` is the true total even when the id list is capped,
  because the dedupe step now walks all matching rows instead of breaking
  at the limit (fixes a latent undercount in the old early-break dedupe).

### Fixed

- **Resident daemon no longer exits on transient `accept` errors (#3).**
  `serve`'s accept loop treated any `accept()` failure as fatal, so a single
  kernel resource shortage (EMFILE/ENOMEM) tore down the resident daemon and
  forced every client to reconnect (the shim respawn was the only self-heal).
  The loop now logs and retries on `accept` errors, breaking only on shutdown
  or idle. Also from #3: `shim::bridge` no longer swallows `io::copy` errors
  silently — a mid-response daemon RST now leaves a diagnostic on stderr
  instead of an unexplained shim exit; and the e2e shim smoke test replaces
  its fixed 500 ms bring-up sleep with a poll-connect-until-ready loop plus a
  strict `result` assertion, removing flake risk and a weak `result || error`
  form that could mask regressions.

- **`index.rebuild` / `index.sync` now re-embed chunks (#1).** Both RPCs
  re-derive chunks + FTS via `reindex_one`, which clears
  `vec_chunk_embeddings` (CASCADE on the entry DELETE) but never re-embedded —
  it relied on a v1 "background chunk embedder" that was never implemented
  (0.2.3 wired in-handler embed for `entry.create` / `block.*` / `batch` but
  missed these two paths). After a rebuild or sync, `search.semantic` returned
  empty for the affected entries until each was touched again via
  `entry.create` / `block.update` / `batch`. The handlers now re-embed every
  (re-)indexed entry; `emb_cache` (untouched by rebuild) keeps this near-zero
  API cost for unchanged bodies — only genuinely new chunk texts hit the
  provider. This also closes the latent `index.sync` regression where editing
  a `.nomai` file and syncing would silently break `search.semantic` for that
  entry.

## [0.3.0] - 2026-07-08

### Added
- **Multi-instance Claude Code support (#2).** The daemon is now a single
  resident process shared across CC instances: CC spawns the binary as
  before (zero `.mcp.json` change), and the shim auto-spawns/attaches to one
  resident `--serve` daemon over a Unix socket. This eliminates the
  `SQLITE_BUSY` conflict that occurred when multiple CC instances each opened
  the same `db.sqlite`. If the resident daemon can't start, the shim falls
  back to single-process stdio serve (previous behavior).
- `[serve] idle_timeout_secs` config (default 30): how long the resident
  daemon stays alive after the last client disconnects.

## [0.2.3] — 2026-07-07

### Added

- daemon auto-embeds chunks on `entry.create`, `block.append` /
  `block.update`, and `batch` commit. **Fixes `search.semantic` returning
  empty for production entries** — the `vec_chunk_embeddings` table was
  never populated (the "background embedder" referenced in old code
  comments was never implemented). Embeddings reuse `emb_cache`
  (unchanged bodies skip the API call). `entry.update` is not wired (it
  only changes metadata, not blocks — chunks are unchanged).

### Changed

- Embed failure surfaces as RPC provider error (1002); the entry write is
  NOT rolled back (fulltext still works; `index.rebuild` re-embeds).

## [0.2.2] — 2026-07-06

### Added

- `block.get` RPC: fetch a single block by ULID (namespace completeness —
  the other four primitives all have `get`). `BlockService::get` already
  existed in core; this adds the daemon handler, MCP descriptor, and
  schema tests. Auto-registers in `tools/list`.
- CJK fulltext search: `fts_blocks` switched to FTS5 `trigram` tokenizer
  (V10 migration). Chinese / Japanese / Korean text is now substring-
  searchable. The migration self-backfills from existing `blocks` rows,
  so existing DBs need no manual reindex.
- Short-query LIKE fallback: `search.fulltext` queries of 1–2 characters
  (trigram returns empty for them) transparently fall back to an escaped
  `LIKE '%q%'` scan. Same RPC, same params — clients need no changes.
- MCP `tools/list` now exposes `description` + real `inputSchema` for all
  internal handlers (F-mcp-2). `RpcHandler` trait gains two default methods
  `description()` and `input_schema()` for plugin authors to opt in.
  External plugins continue to work unchanged.

### Changed

- Object cache (Entry/Chunk LRU) moved from "future work" to "measured,
  declined" — `bench_object_cache.rs` showed the only beneficiary is the
  rare "get same id N times" pattern; GraphRAG uses JOIN and never calls
  `entry.get` separately.
- `docs/reference.md` "All 23 primitive RPCs" wording fixed (count had
  drifted to 29/30); now reads "all built-in RPCs" to prevent recurrence.

### Database migration

- **V10** (automatic on next daemon boot): switches `fts_blocks` to the
  FTS5 `trigram` tokenizer and self-backfills from `blocks`. No manual
  reindex or `index.sync` required — the migration is self-contained, so
  `fts_blocks` is consistent the moment V10 lands.

## [0.2.1] — 2026-06-26

Docs-only release. No code or API changes; no migration steps.

### Changed

- **Docs split**: the single-file `docs/guide.md` (765 lines) is now three
  focused documents:
  - `docs/guide.md` (216 lines) — concepts: transport, the five primitives,
    storage model, migration
  - `docs/reference.md` (416 lines, new) — full RPC reference, error codes,
    configuration, cache internals
  - `docs/lib.md` (179 lines, new) — lib mode, `DaemonBuilder`, custom RPCs
  - `README.md` Documentation section + Project layout updated to point at
    all three
- **De-branded config examples**: GLM/BigModel-specific `base_url`,
  `api_key_env`, `model`, and `dim` values in README.md and docs/guide.md
  replaced with neutral placeholders (`your-embedding-endpoint`,
  `NOMAI_EMBEDDING_API_KEY`, `your-embedding-model`, `1024`). The
  multi-vendor "Compatible endpoints" list in `reference.md` is preserved
  (it advertises multi-vendor support rather than picking one).

## [0.2.0] — 2026-06-26

Pre-1.0 release. API surface continues to evolve; breaking changes may
still land in 0.x (per semver 0.y.z allowance). See "Migration from
0.1.0" below for lib-side breaking changes in this release.

### Added (RPC additive)

- `entry.delete` ack now includes `id` field (Spec 8 Plan 1 / F-entry-1)
- `entry.list` response now includes `has_more` field (Spec 8 Plan 1 / F-entry-4)
- `events.list` response now includes `total` field (Spec 8 Plan 1 / F-events-1)
- `block.list` RPC: list blocks by `entry_id` (Spec 8 Plan 1 / F-block-1)
- `nomai_core::NomaiBlock` alias for `nomai_format::Block` (Spec 8 Plan 2 / F-lib-1)
- `nomai_daemon::DaemonBuilder` fluent constructor (Spec 8 Plan 2 / F-lib-2)
- `nomai_protocol::method::cache::{STATS, CLEAR}` constants (Spec 8 Plan 2 / F-cache-1)
- Batch per-op error objects in the `results` array now include a `data` field
  when the underlying CoreError carries context (Spec 8 Plan 2 / F-batch-4).
  Previously omitted.

### Changed (lib-side breaking)

- `nomai_core::ListOrder` (event_model layer) renamed to `EventListOrder` (Spec 8 Plan 2 / F-events-2)
- `nomai_core::service::ListOrder` renamed to `EntryListOrder`; now re-exported at crate root (Spec 8 Plan 2 / F-events-2)
- `nomai_daemon::handlers::registry()` moved to `handlers::registry::registry()` (Spec 8 Plan 2 / F-lib-3); short path `handlers::registry()` preserved via re-export
- `batch::error_to_rpc` removed; use `rpc::core_error_to_rpc_ref` (Spec 8 Plan 2 / F-batch-4)

### Documentation

- `chunk.{CREATE, DELETE}` constants marked as reserved in `protocol::method` (Spec 8 Plan 3 / F-chunk-1)
- `protocol::error` tests now cover all 6 business codes (Spec 8 Plan 3 / F-err-1)
- `docs/guide.md` and `README.md` synced with 0.2.0 API surface (Spec 8 Plan 3 / F-doc-2)
- Stale `#[allow(dead_code)]` on `Daemon::search_cache` removed (Spec 8 Plan 3 / F-doc-3)

### Migration from 0.1.0

**RPC consumers (JSON-RPC clients)**: no breaking changes — all 0.2.0
RPC additions are additive (new optional response fields / new
methods). Old clients ignore new fields.

**lib consumers (Rust crates using `nomai-core` / `nomai-daemon`)**:

1. Replace `use nomai_core::ListOrder;` with the appropriate variant:
   - For `EntryListQuery { order }`: `use nomai_core::EntryListOrder;`
   - For `ListEventsQuery { order }`: `use nomai_core::EventListOrder;`
2. If you called `nomai_daemon::handlers::registry()` — still works via re-export.
3. If you called `batch::error_to_rpc` — switch to `rpc::core_error_to_rpc_ref(&err)` and serialize the returned `RpcError` to `Value` yourself.
4. `Daemon::from_services` is unchanged; new `DaemonBuilder` is optional.

### Non-goals (deferred to future releases)

The following were considered for 0.2.0 but deferred (see Spec 8 §7).
They remain candidates for 1.0 (true API freeze) or later 0.x releases:

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

[Unreleased]: https://github.com/Lin-Jiong-HDU/nomai/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/Lin-Jiong-HDU/nomai/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/Lin-Jiong-HDU/nomai/compare/v0.2.3...v0.3.0
[0.2.3]: https://github.com/Lin-Jiong-HDU/nomai/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/Lin-Jiong-HDU/nomai/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/Lin-Jiong-HDU/nomai/releases/tag/v0.2.1
[0.2.0]: https://github.com/Lin-Jiong-HDU/nomai/releases/tag/v0.2.0
[0.1.0]: https://github.com/Lin-Jiong-HDU/nomai/releases/tag/v0.1.0
