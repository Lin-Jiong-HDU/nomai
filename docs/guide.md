# nomai developer guide

This guide is for developers building on top of nomai — whether you're calling the daemon over JSON-RPC from a Python/Node/Go client, or embedding `nomai-core` directly into a Rust binary.

For project overview and install, see the [README](../README.md) first.

## Table of contents

- [Transport: how to talk to the daemon](#transport)
- [The four primitives](#the-four-primitives)
  - [Entry](#entry)
  - [Links](#links)
  - [Events](#events)
  - [Chunks](#chunks)
- [RPC reference](#rpc-reference)
  - [entry.\*](#entry)
  - [link.\*](#link)
  - [events.\*](#events)
  - [chunk.\*](#chunk)
  - [search.\*](#search)
  - [qa.\*](#qa)
  - [provider.\*](#provider)
- [Error codes](#error-codes)
- [Configuration](#configuration)
- [Retrieval modes](#retrieval-modes)
- [Lib mode (embed nomai-core)](#lib-mode)

---

## Transport

nomai-daemon speaks JSON-RPC 2.0 over NDJSON on stdio. Every line is one JSON object. Send a request on stdin, get a response on stdout.

```fish
echo '{"jsonrpc":"2.0","id":1,"method":"entry.list","params":{}}' \
  | ./target/release/nomai-daemon
```

Multiple requests can be piped at once — daemon processes them sequentially and writes one response line per request.

```fish
echo '{"jsonrpc":"2.0","id":1,"method":"entry.create","params":{"title":"a","body":"x"}}
{"jsonrpc":"2.0","id":2,"method":"entry.list","params":{}}' \
  | ./target/release/nomai-daemon
```

For long-lived clients, open the daemon as a subprocess and keep stdin/stdout pipes open. Each client gets its own daemon process — there is no multi-plexing server.

**Notifications**: requests without an `id` field are notifications — daemon processes them but writes no response.

---

## The four primitives

nomai provides four orthogonal building blocks. You can use any subset; they don't depend on each other at the API level.

### Entry

The atomic unit of knowledge. A markdown note with structured metadata.

```json
{
  "id": "01KVM7KFNT82JWCEM31FESCEXP",  // ULID, time-sortable
  "title": "Rust 入门",
  "body": "Rust 是一门系统编程语言...",  // markdown
  "tags": ["rust", "programming"],
  "attrs": {"difficulty": "beginner"},   // free-form JSON object
  "source": null,
  "created_at": "2026-06-21T04:38:37Z",
  "updated_at": "2026-06-21T04:38:37Z"
}
```

- `id` is a ULID (26 chars, time-ordered, URL-safe).
- `body` is markdown text. Core does not parse markdown — full text is FTS-indexed and embedded as a single vector.
- `tags` is a JSON array of strings, queryable via `entry.list`.
- `attrs` is a free-form JSON object. Core validates it's an object but does not enforce schema. (Filtering by attrs is not yet implemented.)
- `source` is an optional provenance string (URL, filename, etc.).

**Creating an entry auto-embeds the body**: daemon calls the embedding provider and writes the vector to `vec_embeddings`. The entry is then retrievable via `search.semantic` (entry-level granularity).

### Links

Directed edges between entries. One entry can link to many others with different relation types.

```json
{
  "id": "01KVN...",
  "source_id": "01KVM7KFNT82JWCEM31FESCEXP",
  "target_id": "01KVM7K3G9AA5804VFTZ483SHD",
  "relation": "references",    // free-form string
  "attrs": {"weight": 0.8},
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
  "payload": { /* full entity snapshot at the time of mutation */ },
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
  "attrs": {"section": "conclusion"},
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

| Query | Retrieval | Top result | Score |
|---|---|---|---|
| "Nomai 文化追求什么" | `granularity=chunk` | chunk about "文化...追求..." | **0.707** |
| "Nomai 文化追求什么" | `granularity=entry` (default) | entire Nomai entry | 0.453 |

---

## RPC reference

All methods follow JSON-RPC 2.0. On error, response has `error: {code, message, data?}`. See [error codes](#error-codes).

### entry.{#entry-methods}

| Method | Params | Returns | Notes |
|---|---|---|---|
| `entry.create` | `title`, `body`, `tags?`, `attrs?`, `source?` | `Entry` | Auto-embeds body if non-empty |
| `entry.get` | `id` | `Entry` | 1001 if not found |
| `entry.update` | `id`, `title?`, `body?`, `tags?`, `attrs?`, `source?` | `Entry` | Re-embeds if body changes; clears embedding if body becomes empty |
| `entry.delete` | `id` | `{"deleted": true}` | Cascades to links + chunks + chunk embeddings |
| `entry.list` | `tag?`, `limit?`(50), `offset?`(0), `order?` | `{items, total}` | `order`: `created_desc`(default) / `created_asc` / `updated_desc` / `updated_asc` |

### link.{#link-methods}

| Method | Params | Returns | Notes |
|---|---|---|---|
| `link.create` | `source_id`, `target_id`, `relation`, `attrs?` | `Link` | 1003 if source/target missing or duplicate |
| `link.get` | `id` | `Link` | 1001 if not found |
| `link.delete` | `id` | `{"deleted": true}` | 1001 if not found |
| `link.list` | `from?`, `to?`, `relation?`, `limit?`(50), `offset?`(0) | `{items, total}` | At least one of `from`/`to` required |
| `link.neighbors` | `id`, `relation?`, `direction?`("out"\|"in"\|"both"=both), `limit?`(50) | `{entries, links}` | One-hop graph traversal |

### events.{#events-methods}

| Method | Params | Returns | Notes |
|---|---|---|---|
| `events.list` | `since?`(ULID, exclusive), `type?`, `target_type?`, `target_id?`, `limit?`(100), `order?`("asc"=default\|"desc") | `{items, has_more}` | Client-cursor model |
| `events.get` | `id` | `Event` | 1001 if not found |
| `events.purge` | `before`(ULID, exclusive), `type?` | `{deleted: N}` | For retention |

### chunk.{#chunk-methods}

| Method | Params | Returns | Notes |
|---|---|---|---|
| `chunk.create` | `entry_id`, `ordinal`, `text`, `attrs?` | `Chunk` | Auto-embeds text; 1003 on FK/UNIQUE violation |
| `chunk.get` | `id` | `Chunk` | 1001 if not found |
| `chunk.delete` | `id` | `{"deleted": true}` | Also removes chunk embedding |
| `chunk.list` | `entry_id`, `limit?`(100), `offset?`(0) | `{items, total}` | Sorted by `ordinal` ascending |

### search.{#search-methods}

| Method | Params | Returns | Notes |
|---|---|---|---|
| `search.fulltext` | `query`, `limit?`(10) | `{items: [{entry, score}]}` | FTS5 bm25 ranking |
| `search.semantic` | `query`, `limit?`(10), `granularity?`("entry"=default\|"chunk") | `{items}` | Items shape depends on granularity (see below) |
| `search.hybrid` | — | — | **Reserved**: returns -32601 |

**`search.semantic` return shapes**:

- `granularity="entry"` (default): `{items: [{entry: Entry, score: f32}]}` — backward compatible
- `granularity="chunk"`: `{items: [{chunk: Chunk, score: f32}]}` — chunks contain `entry_id` for mapping back

### qa.{#qa-methods}

| Method | Params | Returns | Notes |
|---|---|---|---|
| `qa.ask` | `question`, `top_k?`(5), `max_tokens?` | `{answer: String, citations: [Ulid]}` | Embeds question → semantic search top-K → LLM |

`qa.ask` is the one place where nomai encodes a specific strategy (Naive RAG). It's preserved as a reference implementation. For GraphRAG, chunk-level rerank, or HyDE, compose your own flow using the primitives.

### provider.{#provider-methods}

| Method | Params | Returns | Notes |
|---|---|---|---|
| `provider.list` | — | `{embedding: {name, model}, llm: {name, model}}` | Active provider info from config |
| `provider.set` | — | — | **Reserved**: returns -32601 |

---

## Error codes

| Code | Meaning | When |
|---|---|---|
| `-32700` | Parse error | Invalid JSON in request |
| `-32600` | Invalid request | Not a valid JSON-RPC request object |
| `-32601` | Method not found | Unknown method, or reserved method (`search.hybrid`, `provider.set`, `link.traverse`) |
| `-32602` | Invalid params | Malformed params |
| `-32603` | Internal error | Unexpected server error |
| `1001` | NotFound | Entry / link / event / chunk id does not exist |
| `1002` | Provider error | Embedding or LLM HTTP failure (data has `kind` field) |
| `1003` | Validation error | Bad attrs (non-object), FK violation, UNIQUE conflict, missing required params |
| `1004` | Config error | Missing env var, malformed config |

Error response shape:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": 1003,
    "message": "link constraint violation: ...",
    "data": {"id": "01KVM..."}  // optional, context-specific
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

| Mode | Method | Matches by | Best for |
|---|---|---|---|
| **Fulltext** | `search.fulltext` | Token overlap (FTS5 bm25) | Keyword search, exact terms |
| **Semantic (entry)** | `search.semantic` (default) | Cosine similarity of entry embeddings | Concept search, "find similar" |
| **Semantic (chunk)** | `search.semantic granularity=chunk` | Cosine similarity of chunk embeddings | Long-doc RAG, sub-passage retrieval |

**Fulltext limitations**: FTS5 uses the `unicode61` tokenizer by default, which treats consecutive CJK characters as a single token. Searching for Chinese phrases may return no matches. Use semantic search for Chinese content. (Future: trigram/ICU tokenizer.)

**Combining modes**: nomai does not implement hybrid search (`search.hybrid` is reserved). For RRF fusion or re-ranking, fetch from multiple modes in your client and merge.

---

## Lib mode

If you don't want the daemon overhead, depend on `nomai-core` directly:

```toml
# Cargo.toml
[dependencies]
nomai-core = { path = "..." }
```

```rust
use nomai_core::{EntryService, LinkService, EventService, ChunkService};

let conn = std::sync::Arc::new(std::sync::Mutex::new(
    rusqlite::Connection::open("db.sqlite")?
));
let entries = std::sync::Arc::new(EntryService::new(conn.clone())?);
let links = std::sync::Arc::new(LinkService::new(conn.clone())?);
let events = std::sync::Arc::new(EventService::new(conn.clone())?);
let chunks = std::sync::Arc::new(ChunkService::new(conn.clone())?);

let entry = entries.create(nomai_core::CreateEntry {
    title: "Hello".into(),
    body: "world".into(),
    tags: None,
    attrs: None,
    source: None,
})?;
```

All four services share the same `Arc<Mutex<Connection>>`. Emission (events) still happens automatically inside each mutation method.

**Embedding in lib mode**: `EntryService::create` writes the entry + emits the event, but does **not** call the embedding provider (that's daemon-layer orchestration). You must call `entries.write_embedding(id, &vec)` yourself after creating. Same for `chunks.write_embedding`. The `nomai-providers` crate provides the `EmbeddingProvider` trait and an OpenAI-compatible implementation.

---

## What's next

- **Phase 4 (Collections)** is planned but not yet implemented — for multi-project isolation, schema enforcement, and ACL.
- **`link.traverse`** (multi-hop graph traversal) is reserved for a future phase. Until then, use `link.neighbors` in a client-side loop.
- **`search.hybrid`** is reserved. Compose your own fusion in client code.

For implementation history and design rationale, see the spec docs in `docs/superpowers/specs/` (local-only, not in the public repo).
