# nomai developer guide

This guide covers the **concepts** you need to build on top of nomai — the five primitives, the storage model, and how to think about retrieval.

If you're looking for specific API signatures, see:
- [reference.md](reference.md) — full RPC reference, error codes, configuration, cache internals
- [lib.md](lib.md) — embedding nomai as a Rust library (lib mode, DaemonBuilder, custom RPCs)

For project overview and install, see the [README](../README.md) first.

## Table of contents

- [Transport: how to talk to the daemon](#transport)
- [The five primitives](#the-five-primitives)
  - [Entry](#entry)
  - [Block](#block)
  - [Links](#links)
  - [Events](#events)
  - [Chunks](#chunks)
- [Storage layer separation (lib-mode users)](#storage-layer-separation-lib-mode-users)
- [Migration from 0.1.0 to 0.2.0](#migration-from-010-to-020)
- [What's next](#whats-next)

---

## Transport

nomai-daemon speaks JSON-RPC 2.0 over NDJSON on stdio. Every line is one JSON object. Send a request on stdin, get a response on stdout.

```fish
echo '{"jsonrpc":"2.0","id":1,"method":"entry.list","params":{}}' \
  | ./target/release/nomai-daemon
```

Multiple requests can be piped at once — daemon processes them sequentially and writes one response line per request.

```fish
echo '{"jsonrpc":"2.0","id":1,"method":"entry.create","params":{"title":"a","blocks":[{"type":"note","text":"x"}]}}
{"jsonrpc":"2.0","id":2,"method":"entry.list","params":{}}' \
  | ./target/release/nomai-daemon
```

For long-lived clients, open the daemon as a subprocess and keep stdin/stdout pipes open. Each client gets its own daemon process — there is no multi-plexing server.

**Notifications**: requests without an `id` field are notifications — daemon processes them but writes no response.

For the full method list with params and return shapes, see the [RPC reference](reference.md#rpc-reference).

---

## The five primitives

nomai provides five orthogonal building blocks. You can use any subset; they don't depend on each other at the API level.

### Entry

The atomic unit of knowledge. A markdown note with structured metadata.

```json
{
  "id": "01KVM7KFNT82JWCEM31FESCEXP", // ULID, time-sortable
  "title": "Rust 入门",
  "blocks": [
    { "type": "note", "text": "Rust 是一门系统编程语言..." } // markdown
  ],
  "tags": ["rust", "programming"],
  "attrs": { "difficulty": "beginner" }, // free-form JSON object
  "source": null,
  "created_at": "2026-06-21T04:38:37Z",
  "updated_at": "2026-06-21T04:38:37Z"
}
```

- `id` is a ULID (26 chars, time-ordered, URL-safe).
- `blocks` is an ordered list of typed blocks. Block text is markdown; core does not parse markdown — each block's text is FTS-indexed (`fts_blocks`) and chunked for vector retrieval.
- `tags` is a JSON array of strings, queryable via `entry.list`.
- `attrs` is a free-form JSON object. Core validates it's an object but does not enforce schema. (Filtering by attrs is not yet implemented.)
- `source` is an optional provenance string (URL, filename, etc.).

**Creating an entry auto-embeds the blocks**: daemon chunks each block's text, calls the embedding provider, and writes the per-chunk vectors to `vec_chunk_embeddings`. The entry is then retrievable via `search.semantic` (chunk-level granularity, with entry-level rollup).

### Block

A typed semantic block within an entry. Each entry is composed of an ordered list of blocks. Block types: `claim` (assertion), `evidence` (supporting material), `question` (open question), `source` (citation pointer), `note` (freeform text), `connection` (typed link to another entry — populates the `links` table).

Blocks are the structural unit of an entry. `block.append` / `update` / `delete` mutate the entry's block list; each mutation rewrites the entry's `.nomai` file (see [Storage layer separation](#storage-layer-separation-lib-mode-users) for lib-mode caveats).

Chunks are derived from block text via the chunking algorithm — paragraph → sentence → hard-cut cascade at `config.chunking.target_size` characters (default 1024).

### Links

Directed edges between entries. One entry can link to many others with different relation types.

```json
{
  "id": "01KVN...",
  "source_id": "01KVM7KFNT82JWCEM31FESCEXP",
  "target_id": "01KVM7K3G9AA5804VFTZ483SHD",
  "relation": "references", // free-form string
  "attrs": { "weight": 0.8 },
  "created_at": "2026-06-21T..."
}
```

- `relation` is a free-form string. Common conventions: `"references"`, `"see_also"`, `"child_of"`, `"authored_by"`. Core does not enforce a vocabulary.
- Edges are directed. For symmetric relations, create both directions.
- `UNIQUE(source_id, target_id, relation)` — the same pair with the same relation can only have one link, but the same pair can have multiple different relations.
- Deleting an entry cascades to all its links (both as source and target) via DB FK constraint.

**Why this is a primitive**: GraphRAG, Obsidian-style backlinks, and citation networks all reduce to "find the neighbors of X". The core gives you that one operation; what you do with the graph is up to you.

### Events

Append-only log of every mutation. Every `entry.*` / `link.*` / `chunk.*` write emits an event with a full snapshot of the affected entity.

```json
{
  "id": "01KVNFA23RA9VQEHZ8WJNCVPJ6",
  "type": "chunk.created",
  "target_type": "chunk",
  "target_id": "01KVNFA23R9MA0A57YRVY7QP8E",
  "payload": {
    /* full entity snapshot at the time of mutation */
  },
  "created_at": "2026-06-21T16:13:07Z"
}
```

- `id` is a ULID and doubles as a cursor (time-ordered). Clients track `last_seen_id` and pull new events via `events.list(since=last_seen_id)`.
- `payload` is a full snapshot — for `*.deleted` events, the entity no longer exists in its table, but the event retains the pre-deletion state for audit/replay.
- No server-side ack. Client manages its own offset.
- Events are permanent until you call `events.purge`. There's no TTL.

**Why this is a primitive**: Async embedding retry, external sync (push to Obsidian / Git), audit logs, incremental reindex — all of these are consumers of the same event log. Build any of them in a separate process that polls `events.list`.

### Chunks

A block can be split into N chunks, each embedded independently. This unlocks fine-grained retrieval that entry-level embeddings can't provide.

```json
{
  "id": "01KVNFA2Q4008QC2V9MAATZN5H",
  "block_id": "01KVM7KFNT82JWCEM31FESCEXP",
  "ordinal": 2,
  "text": "Nomai 文化的核心是对知识和真相的不懈追求。",
  "attrs": { "section": "conclusion" },
  "created_at": "...",
  "updated_at": "..."
}
```

- Chunks are **auto-derived** from block text (Spec 6 §10). You don't create them directly — call `block.append` / `update` and the daemon handles chunking.
- `ordinal` is the chunk's position within its block (0-based).
- Chunks have their own vector space (`vec_chunk_embeddings`), independent from any entry-level vectors. Same dimension (uses the same embedding model).

**Why this is a primitive**: Long documents compressed into a single vector lose detail. With chunks, the 5KB section that's actually relevant to a query ranks higher than the document's general theme.

**Empirical comparison** (real data from this daemon):

| Query                | Retrieval                     | Top result                   | Score     |
| -------------------- | ----------------------------- | ---------------------------- | --------- |
| "Nomai 文化追求什么" | `granularity=chunk`           | chunk about "文化...追求..." | **0.707** |
| "Nomai 文化追求什么" | `granularity=entry` (default) | entire Nomai entry           | 0.453     |

---

## Storage layer separation (lib-mode users)

`BlockService` (and `EntryService`) mutate the SQLite index and chunks/embeddings, but **do NOT touch the `.nomai` file**. The daemon's RPC handlers wrap each mutation with `rerender_entry_nomai` so the FS file stays consistent.

If you embed `nomai-core` directly (lib mode, no daemon) and call `BlockService::append` / `update` / `delete` yourself, **you must rerender the `.nomai` file yourself**, or accept DB/FS drift until the next `index.sync`.

See `crates/daemon/examples/block_lifecycle.rs` for a lib-mode example that does the right thing, and `crates/daemon/src/handlers/block.rs::rerender_entry_nomai` for the production wrapper.

The drift is self-healing: on the next daemon boot (or explicit `index.sync`), the FS file's mtime will mismatch the `entries.fs_mtime` row, triggering `reindex_one` which overwrites the DB state from the (older) file. So if you mutate via lib-mode WITHOUT rerendering, your block mutation may be lost on the next sync.

---

## Migration from 0.1.0 to 0.2.0

0.2.0 includes lib-side breaking changes (Rust type renames); RPC wire
format remains additive-only. The full breaking-change list and migration
steps live in [CHANGELOG.md](../CHANGELOG.md#migration-from-010). Quick
summary:

- **RPC consumers**: no breaking changes (all 0.2.0 RPC additions are additive).
- **lib consumers**: rename `ListOrder` imports (see CHANGELOG §"Changed").
- **Daemon lib-mode users**: optional switch to `DaemonBuilder` (see [lib.md](lib.md#daemonbuilder)).

See also Spec 8 (`docs/superpowers/specs/2026-06-25-pre-1-0-api-freeze-pass-design.md`)
for the full freeze-pass audit.

---

## What's next

nomai's kernel roadmap (Spec 1-7) is complete:

- **Spec 1** — Plugin Registry + MCP compatibility (done)
- **Spec 2** — Batch RPC with $ref + atomic transactions (done)
- **Spec 3** — Lib API + Daemon accessors + from_services (done)
- **Spec 4** — Application-layer examples (done)
- **Spec 5** — Embedding cache (`emb_cache` + `CachedEmbedder` + `cache.stats` / `cache.clear`) (done)
- **Spec 6** — Content storage (block-addressed chunks, FTS5 per block) (done)
- **Spec 7** — Search results cache (`search.semantic` / `search.fulltext` in-memory cache + generation-based invalidation) (done)
- **Spec 8** — Pre-1.0 API freeze pass (done; released as 0.2.0)

Future work (not yet started):

- **Phase 4 (Collections)** — multi-project isolation, schema enforcement, ACL
- **`link.traverse`** — recursive CTE multi-hop graph traversal (use `link.neighbors` in a client-side loop for now)
- **`search.hybrid`** — RRF fusion of FTS + vector scores (compose your own in client code for now)

For implementation history and design rationale, see the spec docs in `docs/superpowers/specs/` (local-only, not in the public repo).
