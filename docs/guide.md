# nomai developer guide

This guide is for developers building on top of nomai — whether you're calling the daemon over JSON-RPC from a Python/Node/Go client, or embedding `nomai-core` directly into a Rust binary.

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
- [RPC reference](#rpc-reference)
  - [entry.\*](#entry)
  - [block.\*](#block)
  - [index.\* / system.\*](#index--system)
  - [link.\*](#link)
  - [events.\*](#events)
  - [chunk.\*](#chunk)
  - [search.\*](#search)
  - [provider.\*](#provider)
  - [batch.\*](#batch)
  - [cache.\*](#cache)
  - [MCP lifecycle](#mcp)
- [Error codes](#error-codes)
- [Configuration](#configuration)
- [Retrieval modes](#retrieval-modes)
- [Embedding cache](#embedding-cache)
- [Lib mode (embed nomai-core)](#lib-mode)
- [Custom RPCs](#custom-rpcs)
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

Chunks are derived from block text via the [chunking algorithm](#) — see §10 of the spec for the paragraph→sentence→hard-cut cascade.

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

An entry can be split into N chunks, each embedded independently. This unlocks fine-grained retrieval that entry-level embeddings can't provide.

```json
{
  "id": "01KVNFA2Q4008QC2V9MAATZN5H",
  "entry_id": "01KVM7KFNT82JWCEM31FESCEXP",
  "ordinal": 2,
  "text": "Nomai 文化的核心是对知识和真相的不懈追求。",
  "attrs": { "section": "conclusion" },
  "created_at": "...",
  "updated_at": "..."
}
```

- `ordinal` is the chunk's position within its entry (0-based). Core does not parse content — your chunking strategy decides the boundaries.
- `UNIQUE(entry_id, ordinal)` prevents accidental duplication.
- Chunks have their own vector space (`vec_chunk_embeddings`), independent from entry-level vectors. Same dimension (uses the same embedding model).
- **Core does not chunk for you.** Decide your strategy (fixed-window, sentence-aware, markdown-section) in your client code, then call `chunk.create` N times.

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

## RPC reference

All methods follow JSON-RPC 2.0. On error, response has `error: {code, message, data?}`. See [error codes](#error-codes).

### entry.{#entry-methods}

| Method         | Params                                                | Returns             | Notes                                                                             |
| -------------- | ----------------------------------------------------- | ------------------- | --------------------------------------------------------------------------------- |
| `entry.create` | `title`, `blocks: [{type, text, attrs?}]`, `tags?`, `attrs?`, `source?` | `Entry` | Auto-embeds block texts if non-empty                                              |
| `entry.get`    | `id`                                                  | `Entry`             | 1001 if not found                                                                 |
| `entry.update` | `id`, `title?`, `blocks?`, `tags?`, `attrs?`, `source?` | `Entry`           | Re-embeds if blocks change; clears embedding if blocks become empty               |
| `entry.delete` | `id`                                                  | `{"deleted": true}` | Cascades to links + chunks + chunk embeddings                                     |
| `entry.list`   | `tag?`, `limit?`(50), `offset?`(0), `order?`          | `{items, total}`    | `order`: `created_desc`(default) / `created_asc` / `updated_desc` / `updated_asc` |

**Block input shape**: `BlockInput` is `{ type: String, text: String, attrs?: Value }`. Valid types: `claim`, `evidence`, `question`, `source`, `note`, `connection` (the `@connection` type requires `target` and `relation` attrs).

### block.{#block-methods}

| Method         | Params                                  | Returns             | Notes                                         |
| -------------- | --------------------------------------- | ------------------- | --------------------------------------------- |
| `block.append` | `entry_id`, `type`, `text`, `attrs?`    | `Block`             | Auto-assigned `ordinal = max(ordinal)+1`      |
| `block.update` | `id`, `type?`, `text?`, `attrs?`        | `Block`             | Re-chunks if text changed                     |
| `block.delete` | `id`                                    | `{"deleted": true}` | 1001 if not found                             |
| `block.get`    | `id`                                    | `Block`             | 1001 if not found                             |
| `block.list`   | `entry_id`                              | `{items, total}`    | Sorted by `ordinal` ascending                 |

Each mutation rewrites the parent entry's `.nomai` file automatically (no separate RPC needed). Chunk re-derivation is automatic on `text` change.

**`@connection` blocks**: setting `type: "connection"` requires `attrs: { target: "<entry_id>", relation: "<string>" }`. These blocks also populate the `links` table — `link.neighbors` will see the typed edge.

### index.* / system.*{#index--system}

| Method              | Params | Returns                                           | Notes                                         |
| ------------------- | ------ | ------------------------------------------------- | --------------------------------------------- |
| `index.verify`      | `{}`   | `{fs_only, db_only, stale_mtime, consistent}`     | Read-only drift report                        |
| `index.sync`        | `{}`   | `{added, updated, removed, unchanged}`            | Reconcile FS → index (incremental, mtime-diff) |
| `index.rebuild`     | `{}`   | `{reindexed, errors}`                             | Wipe derived tables + reindex every FS entry  |
| `system.export_to_fs` | `{}` | `{exported, skipped, errors}`                   | Generate missing `.nomai` files from DB state |

`index.verify` is read-only. `index.sync` is incremental (diffs FS vs DB mtime). `index.rebuild` is destructive but doesn't touch `events` (daemon history) or `emb_cache` (deterministic, reusable).

Daemon runs `index.sync` automatically at boot. If FS differs from the index (e.g. user manually dropped a `.nomai` file in `entries/`), the daemon picks it up.

### link.{#link-methods}

| Method           | Params                                                                  | Returns             | Notes                                      |
| ---------------- | ----------------------------------------------------------------------- | ------------------- | ------------------------------------------ |
| `link.create`    | `source_id`, `target_id`, `relation`, `attrs?`                          | `Link`              | 1003 if source/target missing or duplicate |
| `link.get`       | `id`                                                                    | `Link`              | 1001 if not found                          |
| `link.delete`    | `id`                                                                    | `{"deleted": true}` | 1001 if not found                          |
| `link.list`      | `from?`, `to?`, `relation?`, `limit?`(50), `offset?`(0)                 | `{items, total}`    | At least one of `from`/`to` required       |
| `link.neighbors` | `id`, `relation?`, `direction?`("out"\|"in"\|"both"=both), `limit?`(50) | `{entries, links}`  | One-hop graph traversal                    |

### events.{#events-methods}

| Method         | Params                                                                                                           | Returns             | Notes               |
| -------------- | ---------------------------------------------------------------------------------------------------------------- | ------------------- | ------------------- |
| `events.list`  | `since?`(ULID, exclusive), `type?`, `target_type?`, `target_id?`, `limit?`(100), `order?`("asc"=default\|"desc") | `{items, has_more}` | Client-cursor model |
| `events.get`   | `id`                                                                                                             | `Event`             | 1001 if not found   |
| `events.purge` | `before`(ULID, exclusive), `type?`                                                                               | `{deleted: N}`      | For retention       |

### chunk.{#chunk-methods}

| Method         | Params                                  | Returns             | Notes                                         |
| -------------- | --------------------------------------- | ------------------- | --------------------------------------------- |
| `chunk.create` | `entry_id`, `ordinal`, `text`, `attrs?` | `Chunk`             | Auto-embeds text; 1003 on FK/UNIQUE violation |
| `chunk.get`    | `id`                                    | `Chunk`             | 1001 if not found                             |
| `chunk.delete` | `id`                                    | `{"deleted": true}` | Also removes chunk embedding                  |
| `chunk.list`   | `entry_id`, `limit?`(100), `offset?`(0) | `{items, total}`    | Sorted by `ordinal` ascending                 |

### search.{#search-methods}

| Method            | Params                                                          | Returns                     | Notes                                          |
| ----------------- | --------------------------------------------------------------- | --------------------------- | ---------------------------------------------- |
| `search.fulltext` | `query`, `limit?`(10), `block_type?`                            | `{items: [{entry, score}]}` | Match against `fts_blocks`; optional `block_type` filter |
| `search.semantic` | `query`, `limit?`(10), `granularity?`("entry"=default\|"chunk"), `block_type?` | `{items}`        | Chunk-level KNN via `vec_chunk_embeddings`; optional `block_type` filter |
| `search.hybrid`   | —                                                               | —                           | **Reserved**: returns -32601                   |

**Block type filter**: `block_type` accepts one of `claim` / `evidence` / `question` / `source` / `note` / `connection`. Omit for all types. Example: `search.fulltext` with `block_type: "claim"` returns only matches in claim blocks.

**`search.semantic` return shapes**:

- `granularity="entry"` (default): `{items: [{entry: Entry, score: f32}]}` — backward compatible
- `granularity="chunk"`: `{items: [{chunk: Chunk, score: f32}]}` — chunks contain `entry_id` for mapping back

### provider.{#provider-methods}

| Method          | Params | Returns                                          | Notes                            |
| --------------- | ------ | ------------------------------------------------ | -------------------------------- |
| `provider.list` | —      | `{embedding: {name, model}, llm: {name, model}}` | Active provider info from config |
| `provider.set`  | —      | —                                                | **Reserved**: returns -32601     |

### batch.{#batch}

Execute multiple mutations atomically in a single request.

| Method  | Params                            | Returns                                         |
| ------- | --------------------------------- | ----------------------------------------------- |
| `batch` | `{ops: [BatchOp], atomic?: bool}` | `{results: [{ok, result?}], rolled_back: bool}` |

Each `BatchOp` has `{id?: string, method: string, params: object}`. The `id` field enables `$ref` referencing — subsequent ops can reference earlier results:

```json
{
  "ops": [
    {
      "id": "e1",
      "method": "entry.create",
      "params": { "title": "doc", "blocks": [{ "type": "note", "text": "..." }] }
    },
    {
      "method": "chunk.create",
      "params": { "entry_id": { "$ref": "e1.id" }, "ordinal": 0, "text": "..." }
    }
  ]
}
```

`$ref` supports dot-path: `{"$ref": "op_id.field.subfield"}`.

**Atomicity**: `atomic` defaults to `true`. Any op failure rolls back the entire batch. `atomic: false` is reserved for future implementation.

**Batch embedding**: After transaction commit, all `entry.create` / `chunk.create` texts are batched into a single `embedder.embed()` call (one HTTP request regardless of op count).

**Allowed methods in batch**: mutation only (`entry.create/update/delete`, `chunk.create/delete`, `link.create/delete`). Read methods and meta-methods are rejected with code 1003.

---

### cache.{#cache}

Embedding cache introspection and management. The cache is a transparent wrapper around the configured embedding provider; it persists `(model, blake3(body)) → embedding` in the `emb_cache` SQLite table so identical bodies never trigger duplicate API calls.

| Method        | Params                                                                               | Returns                                                                     |
| ------------- | ------------------------------------------------------------------------------------ | --------------------------------------------------------------------------- |
| `cache.stats` | —                                                                                    | `{embeddings: {model, dim, rows, hits, misses, hit_rate, warn_rows, warning}}` |
| `cache.clear` | `{model?: string, before?: RFC3339, keep_recent?: N}` — all optional, freely combined | `{cleared: N, by_model: {<model>: N, ...}}`                                 |

**`cache.stats` fields**:

- `hit_rate` is `hits / (hits + misses)` over the daemon's lifetime
- `rows` is the current `COUNT(*)` in `emb_cache` for the configured model
- `warn_rows` is the configured soft-capacity threshold (default 100_000, see `[cache]` in [Configuration](#configuration))
- `warning` is `true` when `rows > warn_rows` — the cache is **never** auto-evicted, this flag only signals that you may want to run `cache.clear`

**`cache.clear` filters** — all optional and freely combinable:

| Filter        | Effect                                                                    | Example                              |
| ------------- | ------------------------------------------------------------------------- | ------------------------------------ |
| `model`       | Restrict to a single model namespace. Omit to clear every model.          | `{"model": "embedding-3"}`           |
| `before`      | Delete only rows created strictly before this RFC3339 timestamp.          | `{"before": "2026-01-01T00:00:00Z"}` |
| `keep_recent` | Keep only the N most-recent rows (by `created_at DESC`); delete the rest. | `{"keep_recent": 1000}`              |

Combinations: `{"model": "emb-3", "before": "..."}` clears `emb-3` rows older than the cutoff; `{"keep_recent": 1000}` clears every model except the global 1000 newest rows. The response always includes `by_model` so you can see which namespaces were affected.

Counters (`hits` / `misses`) are **not** reset by `cache.clear` — they reflect lifetime activity, not current contents. Restart the daemon to reset them.

See [Embedding cache](#embedding-cache) below for the caching model and design rationale.

---

### MCP lifecycle{#mcp}

nomai is a native MCP (Model Context Protocol) server. Any MCP-compatible client (Claude Desktop, Cursor, etc.) can connect via stdio and call all RPCs as tools.

| Method       | Params              | Returns                                       |
| ------------ | ------------------- | --------------------------------------------- |
| `initialize` | `{}`                | `{protocolVersion, capabilities, serverInfo}` |
| `tools/list` | `{}`                | `{tools: [{name, inputSchema}]}`              |
| `tools/call` | `{name, arguments}` | `{content: [{type: "text", text}]}`           |

All 23 primitive RPCs + batch + any custom registered RPCs appear as MCP tools automatically.

Example MCP handshake:

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"entry.list","arguments":{"limit":3}}}
```

---

## Error codes

| Code     | Meaning          | When                                                                                  |
| -------- | ---------------- | ------------------------------------------------------------------------------------- |
| `-32700` | Parse error      | Invalid JSON in request                                                               |
| `-32600` | Invalid request  | Not a valid JSON-RPC request object                                                   |
| `-32601` | Method not found | Unknown method, or reserved method (`search.hybrid`, `provider.set`, `link.traverse`) |
| `-32602` | Invalid params   | Malformed params                                                                      |
| `-32603` | Internal error   | Unexpected server error                                                               |
| `1001`   | NotFound         | Entry / link / event / chunk id does not exist                                        |
| `1002`   | Provider error   | Embedding or LLM HTTP failure (data has `kind` field)                                 |
| `1003`   | Validation error | Bad attrs (non-object), FK violation, UNIQUE conflict, missing required params        |
| `1004`   | Config error     | Missing env var, malformed config                                                     |

Error response shape:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": 1003,
    "message": "link constraint violation: ...",
    "data": { "id": "01KVM..." } // optional, context-specific
  }
}
```

---

## Configuration

Config lives at `~/.config/nomai/config.toml` (Linux), or override with `nomai-daemon --config <path>`.

```toml
[data]
db_path = "~/.local/share/nomai/db.sqlite"   # ~ expansion supported

[embedding]
base_url    = "https://open.bigmodel.cn/api/paas/v4"
api_key_env = "GLM_API_KEY"     # references env var name, not the key itself
model       = "embedding-3"
dim         = 2048              # must match model's output dim

[llm]
base_url    = "http://your-llm-endpoint/v1"
api_key_env = "NOMAI_LLM_API_KEY"
model       = "your-model"

[cache]
warn_rows   = 100000            # soft cap; cache.stats returns warning=true above
```

**API keys are referenced by env var name**, never stored in the config file. Set the env vars in your shell:

```fish
set -Ux GLM_API_KEY "sk-..."
set -Ux NOMAI_LLM_API_KEY "sk-..."
```

**Changing `dim`**: requires deleting `db.sqlite` and re-creating all entries/embeddings. The dimension is fixed at table-creation time.

**Compatible endpoints**: any OpenAI-compatible `/v1/embeddings` and `/v1/chat/completions` endpoint works — OpenAI, DeepSeek, Moonshot, Zhipu GLM, local Ollama, etc.

---

## Retrieval modes

Three ways to find entries. Pick based on what you're matching.

| Mode                 | Method                              | Matches by                            | Best for                            |
| -------------------- | ----------------------------------- | ------------------------------------- | ----------------------------------- |
| **Fulltext**         | `search.fulltext`                   | Token overlap (FTS5 bm25)             | Keyword search, exact terms         |
| **Semantic (entry)** | `search.semantic` (default)         | Cosine similarity of entry embeddings | Concept search, "find similar"      |
| **Semantic (chunk)** | `search.semantic granularity=chunk` | Cosine similarity of chunk embeddings | Long-doc RAG, sub-passage retrieval |

**Fulltext limitations**: FTS5 uses the `unicode61` tokenizer by default, which treats consecutive CJK characters as a single token. Searching for Chinese phrases may return no matches. Use semantic search for Chinese content. (Future: trigram/ICU tokenizer.)

**Combining modes**: nomai does not implement hybrid search (`search.hybrid` is reserved). For RRF fusion or re-ranking, fetch from multiple modes in your client and merge.

---

## Embedding cache{#embedding-cache}

Every embedding API call is cached transparently in the `emb_cache` SQLite table, keyed by `(model, blake3(body))`. Same body, same model → same vector — so repeated embedding work is skipped entirely.

**Why cache**: embeddings are deterministic functions of `(model, body)`. The same body re-submitted (e.g. an unchanged edit, a re-import, a chunking overlap, the same search query) produces the same vector. The cache turns these into a single SQLite lookup instead of a network round-trip + API billing.

**Where it applies** — every `embed()` call in the daemon:

- `entry.create` / `entry.update` with non-empty block text
- `chunk.create` with text
- `search.semantic` (the query is embedded before KNN)
- `batch` (commit-time batch embed of all queued texts)

**Transparency**: `CachedEmbedder` implements `EmbeddingProvider` and wraps the real provider (e.g. `OpenAiCompatibleEmbed`). The daemon always wraps the inner provider in `Daemon::new` and `from_services`. `provider.list` still reports the inner provider's identity — the wrapper delegates `name()`.

**Hit semantics**:

| Field       | Meaning                                                                |
| ----------- | ---------------------------------------------------------------------- |
| `hits`      | Embed calls served from cache (lifetime counter)                       |
| `misses`    | Embed calls that fell through to the inner provider (lifetime counter) |
| `rows`      | Current `COUNT(*)` in `emb_cache` for the configured model             |
| `hit_rate`  | `hits / (hits + misses)`, or 0.0 when both are zero                    |
| `warn_rows` | Configured soft-capacity threshold (`[cache] warn_rows`, default 100K) |
| `warning`   | `true` when `rows > warn_rows` — monitor this in your health checks    |

`cache.clear` removes rows from `emb_cache` but does not reset `hits` / `misses` — those reflect lifetime cache activity, not current contents. To reset, restart the daemon.

**Capacity and eviction**: the cache **grows without bound** and is **never auto-evicted**. This is deliberate — eviction would force a re-computation that costs real API money (the same body re-embedded). Instead, configure `warn_rows` as a soft threshold; when `cache.stats` reports `warning: true`, run `cache.clear` with the filter that fits your situation:

- `cache.clear({model: "old-model"})` — old model leftover after a config switch
- `cache.clear({before: "2026-01-01T00:00:00Z"})` — bulk-clear stale rows
- `cache.clear({keep_recent: 10000})` — trim to the most recent 10K rows

Each row is ~8KB at 2048 dims (`dim × 4 bytes + 32B hash + metadata`), so 100K rows ≈ 800MB. Pick `warn_rows` based on your disk budget; the default of 100K is a sensible starting point.

**Model isolation**: the `model` column namespaces rows; switching `config.embedding.model` starts a fresh cache namespace automatically. Old model rows remain until cleared manually.

**dim changes**: `dim` is part of the lookup condition (`WHERE model = ? AND body_hash = ? AND dim = ?`) but not the primary key, so a config change to `dim` automatically misses the old rows and re-computes. Use `cache.clear({model: ...})` to reclaim the space.

**What is NOT cached**:

- LLM completions (`llm.complete`) — non-deterministic with temperature > 0
- Fulltext search results — FTS5 is fast enough; YAGNI
- Entry / chunk objects — SQLite's own page cache covers B-tree nodes

**Cache layering**:

| Layer               | Caches                            | Owned by                 |
| ------------------- | --------------------------------- | ------------------------ |
| SQLite page cache   | B-tree nodes (disk I/O avoidance) | SQLite (`cache_size`)    |
| **emb_cache table** | **`(model, body) → vector`**      | **nomai (this section)** |
| In-memory LRU       | (future, YAGNI)                   | —                        |

---

## Lib mode

### Option 1: nomai-core directly (storage primitives only)

```toml
[dependencies]
nomai-core = { path = "..." }
```

```rust
use nomai_core::{EntryService, LinkService, EventService, ChunkService, BlockInput};

let conn = std::sync::Arc::new(std::sync::Mutex::new(
    rusqlite::Connection::open("db.sqlite")?
));
let entries = std::sync::Arc::new(EntryService::new(conn.clone())?);
let links = std::sync::Arc::new(LinkService::new(conn.clone())?);
let events = std::sync::Arc::new(EventService::new(conn.clone())?);
let chunks = std::sync::Arc::new(ChunkService::new(conn.clone())?);

let entry = entries.create(nomai_core::CreateEntry {
    title: "Hello".into(),
    blocks: vec![BlockInput {
        r#type: "note".into(),
        text: "world".into(),
        attrs: None,
    }],
    tags: None,
    attrs: None,
    source: None,
})?;
```

### Option 2: nomai-daemon (full Daemon with RPC dispatch + MCP + batch)

```toml
[dependencies]
nomai-daemon = { path = "..." }
nomai-providers = { path = "..." }
```

```rust
use nomai_daemon::daemon::Daemon;

// Construct without config.toml — pass services directly
let mut daemon = Daemon::from_services(conn, embedder, llm, 2048)?;

// Register custom RPCs (see Custom RPCs section below)
daemon.register_handler(std::sync::Arc::new(MyHandler));

// Dispatch any RPC (batch, search, CRUD, MCP tools/call, etc.)
let resp = daemon.dispatch(req).await;
```

See `crates/daemon/examples/` for complete working examples:

- `rag.rs` — Naive RAG via lib API
- `custom_rpc.rs` — Register a custom `stats` RPC
- `import_markdown.rs` — Batch import with $ref + chunking
- `graph_rag.rs` — GraphRAG via search + link.neighbors + LLM
- `events_sync.rs` — Incremental sync via events.list + cursor

All four services share the same `Arc<Mutex<Connection>>`. Emission (events) still happens automatically inside each mutation method.

**Embedding in lib mode**: `EntryService::create` writes the entry + emits the event, but does **not** call the embedding provider (that's daemon-layer orchestration). You must call `entries.write_embedding(id, &vec)` yourself after creating. Same for `chunks.write_embedding`. The `nomai-providers` crate provides the `EmbeddingProvider` trait and an OpenAI-compatible implementation.

---

## Custom RPCs

nomai's daemon uses a plugin registry (`RpcHandler` trait + `HashMap`). You can add custom RPCs without forking the codebase.

### How it works

1. **Implement `RpcHandler`** — a trait with `method()` (returns the RPC name) and `call()` (async, receives `&Daemon` + params).
2. **Register** — call `daemon.register_handler(Arc::new(YourHandler))`.
3. **That's it** — your custom RPC appears in `tools/list` and is callable by any MCP client.

### Accessing services

Custom handlers access the four core services via `&Daemon` accessors:

```rust
daemon.entries()  // &Arc<EntryService>
daemon.links()    // &Arc<LinkService>
daemon.events()   // &Arc<EventService>
daemon.chunks()   // &Arc<ChunkService>
daemon.embedder() // &Arc<dyn EmbeddingProvider>
daemon.llm()      // &Arc<dyn LlmProvider>
```

### Lib-mode construction

Use `Daemon::from_services(conn, embedder, llm, dim)` to build a Daemon without a `config.toml` file. This is the entry point for embedding nomai into your own binary.

### Example

See `crates/daemon/examples/custom_rpc.rs` for a complete working example that:

- Builds a Daemon via `from_services` (in-memory DB, no config)
- Implements a `Stats` RPC that queries entry counts
- Registers it via `register_handler`
- Dispatches the custom RPC
- Verifies it appears in MCP `tools/list`

Run: `cargo run --example custom_rpc`

### Batch composition

The `batch` RPC lets you compose multiple mutations atomically (see [RPC reference](#batch) above). Custom handlers participate in the ecosystem but are not callable from within `batch` ops (only built-in mutations are batch-eligible).

## What's next

nomai's kernel roadmap (Spec 1-5) is complete:

- **Spec 1** — Plugin Registry + MCP compatibility (done)
- **Spec 2** — Batch RPC with $ref + atomic transactions (done)
- **Spec 3** — Lib API + Daemon accessors + from_services (done)
- **Spec 4** — Application-layer examples (done)
- **Spec 5** — Embedding cache (`emb_cache` + `CachedEmbedder` + `cache.stats` / `cache.clear`) (done)

Future work (not yet started):

- **Phase 4 (Collections)** — multi-project isolation, schema enforcement, ACL
- **`link.traverse`** — recursive CTE multi-hop graph traversal (use `link.neighbors` in a client-side loop for now)
- **`search.hybrid`** — RRF fusion of FTS + vector scores (compose your own in client code for now)
- **FTS5 Chinese tokenization** — trigram/jieba for CJK fulltext search
- **Object cache (Entry/Chunk LRU)** — for GraphRAG multi-hop workloads with hot entries

For implementation history and design rationale, see the spec docs in `docs/superpowers/specs/` (local-only, not in the public repo).
