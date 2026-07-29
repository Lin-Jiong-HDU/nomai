# nomai reference

Complete reference for the JSON-RPC API, error codes, configuration, and cache internals. For concepts and getting started, see the [developer guide](guide.md). For embedding nomai as a Rust library, see [lib mode](lib.md).

## Table of contents

- [RPC reference](#rpc-reference)
  - [entry.\*](#entry-methods)
  - [entries.\*](#entries-methods)
  - [block.\*](#block-methods)
  - [attachment.\*](#attachment-methods)
  - [index.\* / system.\*](#index--system)
  - [link.\*](#link-methods)
  - [events.\*](#events-methods)
  - [chunk.\*](#chunk-methods)
  - [search.\*](#search-methods)
  - [provider.\*](#provider-methods)
  - [batch.\*](#batch)
  - [cache.\*](#cache)
  - [sync.\*](#sync-methods)
- [benchmark.\*](#benchmark-methods)
  - [MCP lifecycle](#mcp)
- [Error codes](#error-codes)
- [Configuration](#configuration)
- [Retrieval modes](#retrieval-modes)
- [Embedding cache](#embedding-cache)
- [Search results cache](#search-results-cache)

---

## RPC reference

All methods follow JSON-RPC 2.0. On error, response has `error: {code, message, data?}`. See [error codes](#error-codes).

### entry.{#entry-methods}

| Method         | Params                                                | Returns             | Notes                                                                             |
| -------------- | ----------------------------------------------------- | ------------------- | --------------------------------------------------------------------------------- |
| `entry.create` | `title`, `blocks: [{type, text, attrs?}]`, `tags?`, `attrs?`, `source?`, `attachments?` | `Entry` | Auto-embeds block texts if non-empty. `attachments: {filename: base64}` writes sibling files under the entry dir; `@image`/`@source` `src` validated against disk |
| `entry.get`    | `id`                                                  | `Entry`             | 1001 if not found                                                                 |
| `entry.update` | `id`, `title?`, `blocks?`, `tags?`, `attrs?`, `source?` | `Entry`           | Re-embeds if blocks change; clears embedding if blocks become empty               |
| `entry.delete` | `id`                                                  | `{"deleted": true, "id": "<ulid>"}` | Cascades to links + chunks + chunk embeddings. `id` added in 0.2.0 for consistency with `block.delete` |
| `entry.list`   | `tag?`, `limit?`(50), `offset?`(0), `order?`, `transient?`          | `{items, total, has_more}`    | `order`: `created_desc`(default) / `created_asc` / `updated_desc` / `updated_asc`. `has_more` (added in 0.2.0) is true when `total > offset + items.len()`. `transient` (added in 0.4.1): `true` → only short-term entries, `false` → only long-term, omit → all |

**Block input shape**: `BlockInput` is `{ type: String, text: String, attrs?: Value }`. Valid types: `claim`, `evidence`, `question`, `source`, `note`, `connection`, `image` (the `@connection` type requires `target` and `relation` attrs; `@image` requires `src` attr — see below).

### entries.{#entries-methods}

| Method                    | Params                                | Returns                                                                                             | Notes                                                                                                                                                                                                                                                                                                                                              |
| ------------------------- | ------------------------------------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `entries.purge_transient` | `older_than_secs?`, `dry_run?`(=true) | preview: `{dry_run, count, entries:[{id,title,created_at}], truncated}`; real: `{dry_run, deleted, ids, failed:[{id,error}]}` | Purge transient entries (those with `attrs.transient=true`). Safe by default: `dry_run=true` returns a capped preview (max 50 in `entries`, `count` is the true total). `dry_run=false` actually deletes — reuses the per-entry delete cascade and bumps the search cache generation. `older_than_secs` limits to entries older than a threshold (strict `<`). Permanent entries are never touched. Added in 0.4.1. |

### block.{#block-methods}

| Method         | Params                                  | Returns             | Notes                                         |
| -------------- | --------------------------------------- | ------------------- | --------------------------------------------- |
| `block.append` | `entry_id`, `type`, `text`, `attrs?`, `attachments?`    | `Block`             | Auto-assigned `ordinal = max(ordinal)+1`. Optional `attachments: {filename: base64}` writes sibling files |
| `block.update` | `id`, `type?`, `text?`, `attrs?`, `attachments?`        | `Block`             | Re-chunks if text changed. Optional `attachments: {filename: base64}` writes sibling files |
| `block.delete` | `id`                                    | `{"deleted": true, "id": "<ulid>"}` | 1001 if not found              |
| `block.get`    | `id`                                    | `Block`             | 1001 if not found                             |
| `block.list`   | `entry_id`                              | `{items, total}`    | Sorted by `ordinal` ascending. Added in 0.2.0 for namespace completeness |

Each mutation rewrites the parent entry's `.nomai` file automatically (no separate RPC needed). Chunk re-derivation is automatic on `text` change. Chunks are split at `config.chunking.target_size` characters (default 1024) via paragraph → sentence → hard-cut fallback.

**`@connection` blocks**: setting `type: "connection"` requires `attrs: { target: "<entry_id>", relation: "<string>" }`. These blocks also populate the `links` table — `link.neighbors` will see the typed edge.

**`@image` blocks**: setting `type: "image"` requires `attrs: { src: "<filename>" }`, where `src` is a sibling file under the entry dir (a local filename, never a URL). The block `text` is the caption — it flows through the normal chunk / FTS / embedding pipeline, so image captions are searchable via `search.semantic` / `search.fulltext` like any other block. Optional attrs: `alt` (accessibility text), `width`. The referenced file must exist on disk (validated on write); missing file → code 1003. Attach the binary itself via the `attachments` param on `entry.create` / `block.append` / `block.update`, or by placing the file in the entry dir out-of-band.

### attachment.{#attachment-methods}

| Method            | Params                          | Returns                                       | Notes                                                                                       |
| ----------------- | ------------------------------- | --------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `attachment.list` | `entry_id`                      | `{items: [{filename, size, modified}]}`       | Sibling files under the entry dir (excludes `entry.nomai`). Empty `items` if none.          |
| `attachment.read` | `entry_id`, `filename`          | `{filename, mime, base64}`                    | MIME from extension (png/jpeg/gif/webp/pdf/…, else `application/octet-stream`). base64 transport. 1003 if not found. |

### index.* / system.*{#index--system}

| Method              | Params | Returns                                           | Notes                                         |
| ------------------- | ------ | ------------------------------------------------- | --------------------------------------------- |
| `index.verify`      | `{}`   | `{fs_only, db_only, stale_mtime, consistent}`     | Read-only drift report                        |
| `index.sync`        | `{}`   | `{added, updated, removed, unchanged}`            | Reconcile FS → index (incremental, mtime-diff) |
| `index.rebuild`     | `{}`   | `{reindexed, errors}`                             | Wipe derived tables + reindex every FS entry  |
| `system.export_to_fs` | `{}` | `{exported, skipped, errors}`                   | Generate missing `.nomai` files from DB state |
| `system.restart`      | `{}` | `{ok: true}`                                     | Connection-continuous in-process rebuild of sqlite / embedder-LLM providers / reqwest `Client` / search cache. In-flight RPCs finish against the old state; subsequent RPCs hit the rebuilt one. Config error if the daemon isn't slot-backed or the rebuild fails. Emits no events. |

`index.verify` is read-only. `index.sync` is incremental (diffs FS vs DB mtime). `index.rebuild` is destructive but doesn't touch `events` (daemon history) or `emb_cache` (deterministic, reusable).

Daemon runs `index.sync` automatically at boot. If FS differs from the index (e.g. user manually dropped an `entries/<ULID>/entry.nomai` file), the daemon picks it up.

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
| `events.list`  | `since?`(ULID, exclusive), `type?`, `target_type?`, `target_id?`, `limit?`(100), `order?`("asc"=default\|"desc") | `{items, has_more, total}` | Client-cursor model. `total` (added in 0.2.0) is the total event count matching the filters |
| `events.get`   | `id`                                                                                                             | `Event`             | 1001 if not found   |
| `events.purge` | `before`(ULID, exclusive), `type?`                                                                               | `{deleted: N}`      | For retention       |

### chunk.{#chunk-methods}

| Method         | Params                                  | Returns             | Notes                                         |
| -------------- | --------------------------------------- | ------------------- | --------------------------------------------- |
| `chunk.create` | `entry_id`, `ordinal`, `text`, `attrs?` | `Chunk`             | Auto-embeds text; 1003 on FK/UNIQUE violation |
| `chunk.get`    | `id`                                    | `Chunk`             | 1001 if not found                             |
| `chunk.delete` | `id`                                    | `{"deleted": true}` | Also removes chunk embedding                  |
| `chunk.list`   | `entry_id`, `limit?`(100), `offset?`(0) | `{items, total}`    | Sorted by `ordinal` ascending                 |

Note: `chunk.create` / `chunk.delete` constants exist in `protocol::method::chunk` but return `METHOD_NOT_FOUND` (-32601) — chunks are auto-derived from blocks.

### search.{#search-methods}

| Method            | Params                                                          | Returns                     | Notes                                          |
| ----------------- | --------------------------------------------------------------- | --------------------------- | ---------------------------------------------- |
| `search.fulltext` | `query`, `limit?`(10), `block_type?`                            | `{items: [{entry, score}]}` | Match against `fts_blocks`; optional `block_type` filter |
| `search.semantic` | `query`, `limit?`(10), `granularity?`("entry"=default\|"chunk"), `block_type?` | `{items}`        | Chunk-level KNN via `vec_chunk_embeddings`; optional `block_type` filter |
| `search.hybrid`   | —                                                               | —                           | **Reserved**: returns -32601                   |

**Block type filter**: `block_type` accepts one of `claim` / `evidence` / `question` / `source` / `note` / `connection` / `image`. Omit for all types. Example: `search.fulltext` with `block_type: "claim"` returns only matches in claim blocks.

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

Embedding cache and search results cache introspection/management. The embedding cache is a transparent wrapper around the configured embedding provider; it persists `(model, blake3(body)) → embedding` in the `emb_cache` SQLite table so identical bodies never trigger duplicate API calls. The search cache is an in-memory wrapper around `search.semantic` / `search.fulltext` that skips the FTS5/KNN work when the same query is repeated within the current generation.

| Method        | Params                                                                                                                                 | Returns                                                                                                                              |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `cache.stats` | —                                                                                                                                      | `{embeddings: {...}, searches: {generation, entries, hits, misses, hit_rate, by_rpc: {semantic: {hits, misses}, fulltext: {hits, misses}}}}` |
| `cache.clear` | `{namespace?: "embeddings" \| "searches" \| "all", model?: string, before?: RFC3339, keep_recent?: N}` — all optional, freely combined | `{embeddings: {cleared, by_model} \| null, searches: {cleared} \| null}`                                                             |

**`cache.stats` — `embeddings` block fields**:

- `hit_rate` is `hits / (hits + misses)` over the daemon's lifetime
- `rows` is the current `COUNT(*)` in `emb_cache` for the configured model
- `warn_rows` is the configured soft-capacity threshold (default 100_000, see `[cache]` in [Configuration](#configuration))
- `warning` is `true` when `rows > warn_rows` — the cache is **never** auto-evicted, this flag only signals that you may want to run `cache.clear`

**`cache.stats` — `searches` block fields**:

- `generation` is the current invalidation generation (monotonically increasing; every `entry.*` / `block.*` / `index.*` mutation bumps it atomically)
- `entries` is the number of cached search results currently held in memory
- `hits` / `misses` are lifetime counters across both `search.semantic` and `search.fulltext`
- `hit_rate` is `hits / (hits + misses)` over the daemon's lifetime (0.0 when both are zero)
- `by_rpc.semantic.{hits,misses}` and `by_rpc.fulltext.{hits,misses}` break the counters out per RPC kind

Cached search results are keyed by `(generation, rpc, query_hash, limit, block_type_hash)`. Any `entry.*` / `block.*` / `index.*` mutation (`entry.create`/`update`/`delete`, `block.append`/`update`/`delete`, `index.sync`/`rebuild`) bumps `generation`, which invalidates every cached result atomically — the next search recomputes from current state. `chunk.*` and `link.*` do not bump. See [Search results cache](#search-results-cache) below.

**`cache.clear` — `namespace` parameter** (default `"embeddings"`, for backward compatibility):

| Namespace    | Effect                                                                                                                                                          |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `embeddings` | Default. Clears the `emb_cache` SQLite table subject to the filters below. `searches` is left untouched; `result.searches` is `null`.                            |
| `searches`   | Clears the in-memory search cache (drops all `entries`). `model`/`before`/`keep_recent` are ignored. `result.embeddings` is `null`; `result.searches = {cleared}`. |
| `all`        | Clears both. `result.embeddings` and `result.searches` are both populated.                                                                                       |

Omitting `namespace` entirely is equivalent to `{"namespace": "embeddings"}` — existing clients see the same `{cleared, by_model}` shape they always did (now nested under `result.embeddings`).

**`cache.clear` — embedding filters** (only consulted when `namespace ∈ {embeddings, all}`):

| Filter        | Effect                                                                    | Example                              |
| ------------- | ------------------------------------------------------------------------- | ------------------------------------ |
| `model`       | Restrict to a single model namespace. Omit to clear every model.          | `{"model": "your-embedding-model"}`  |
| `before`      | Delete only rows created strictly before this RFC3339 timestamp.          | `{"before": "2026-01-01T00:00:00Z"}` |
| `keep_recent` | Keep only the N most-recent rows (by `created_at DESC`); delete the rest. | `{"keep_recent": 1000}`              |

Combinations: `{"model": "emb-3", "before": "..."}` clears `emb-3` rows older than the cutoff; `{"keep_recent": 1000}` clears every model except the global 1000 newest rows. When embeddings are cleared, the response always includes `by_model` so you can see which namespaces were affected.

Counters (`hits` / `misses`) are **not** reset by `cache.clear` — they reflect lifetime activity, not current contents. Restart the daemon to reset them.

See [Embedding cache](#embedding-cache) below for the caching model and design rationale, and [Search results cache](#search-results-cache) for the search cache.

---

### sync.{#sync-methods}

Multi-device sync. Turns the FS-backed `knowledge_root` into a git repository
synced against a private remote; `entry.nomai` files sync as plain text
(merge-friendly), binary attachments route through Git LFS. The daemon's
`--sync-init` / `--sync` CLI flags are thin clients that dispatch these two
RPCs to the resident daemon; all git work lives in the daemon, never in
`nomai-core`. Both `sync.init` and `sync.run` are new in 0.4.2.

| Method      | Params                                  | Returns                                                       | Notes                                                                                                                                                          |
| ----------- | --------------------------------------- | ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sync.init` | `remote`(string), `branch`?(="main")    | `{initialized, knowledge_root, remote, branch, lfs_ready}`    | `1003` if `.git` already exists (idempotency), or if `git-lfs` is not on PATH. Writes `.gitignore` + `.gitattributes`, runs `git lfs install`, initial commit. |
| `sync.run`  | `{}`                                    | `{committed: bool, commit: string\|null, pushed: bool, reindexed: bool}` | `1003` if not a git repo (run `sync.init` first). On rebase conflict: `1007` with `data.conflicted_files` (repo left mid-rebase; resolve + re-run to continue). |

`sync.run` flow (serialized under the daemon's `sync_lock` so it cannot race
in-process write RPCs): `git add -A` → commit only if dirty →
`git pull --rebase origin <branch>` → `git push origin <branch>` → invoke
`index.sync` to rebuild the local SQLite index from the reconciled FS. If a
prior `sync.run` left the repo mid-rebase (a conflict you have since
resolved), the next `sync.run` skips commit and runs
`git rebase --continue` instead of `pull --rebase`.

**Conflict recovery.** When `pull --rebase` hits a same-line conflict,
`sync.run` returns `1007` *without* pushing or reindexing, and leaves
`.git/rebase-merge` in place. Edit the conflicted `entry.nomai` to remove
the `<<<<<<<` / `=======` / `>>>>>>>` markers, `git add` it, then re-run
`sync.run` — it resumes the rebase, pushes, and reindexes. `db.sqlite` is
never committed; each device rebuilds its own index at the end of every run.

First push to an empty remote: `pull --rebase` reports "no remote ref" but
does *not* start a rebase, so `sync.run` falls through to `git push` rather
than misreporting a conflict.

---

### benchmark.{#benchmark-methods}

These are development-only benchmark lifecycle methods. They are registered
only when [development].enabled = true. Otherwise they are absent from
tools/list and direct calls return -32601. Benchmark catalog files are loaded
from the configured case, suite, and baseline directories at daemon startup.
The catalog and baselines are read-only Git artifacts.

| Method | Params | Returns | Notes |
| --- | --- | --- | --- |
| benchmark.start | suite_id | {run_id, suite_id, case_count, provider: {name, embedding_model, llm_model}} | Starts one run, loads and embeds temporary fixtures, and rejects a second active run. |
| benchmark.next_case | run_id | {run_id, case_id, question} | Advances in suite order. Does not expose the reference answer, relevant IDs, rubric, fixture body, or baseline. Returns 1003 when the run is invalid or exhausted. |
| benchmark.record_answer | run_id, case_id, answer | {case_id, metrics} | Records the model answer and returns metrics for the current case. If the case enables judging, the configured LLM produces an optional judge_score. |
| benchmark.finish | run_id | RunReport plus run_id | Scores all cases, compares the matching read-only baseline when available, removes temporary fixtures, and returns the run report. |
| benchmark.abort | run_id | {run_id, aborted: true, deleted_entry_count} | Aborts the run and removes all temporary fixtures. |
| benchmark.status | {} | {enabled: true, run_id, case_id, state: "running" or "idle"} | Reports the active run state; run_id and case_id are null while idle. |

benchmark.next_case is the only model-visible source of benchmark questions.
The daemon records these calls while a case is active:
search.semantic, search.fulltext, entry.get, and block.get. Search results
are scored from their returned ranking; evidence calls are scored separately,
so retrieving the right entry does not by itself satisfy a required
search-tool constraint.

The metrics object contains the following per-case fields:

| Field | Meaning |
| --- | --- |
| hit_at_k | 1 when at least one relevant entry/block is in the top k, otherwise 0. |
| recall_at_k | Relevant entries found in the top k divided by all relevant entries. |
| precision_at_k | Relevant returned results divided by the number of results actually returned in the top k. |
| mrr | Reciprocal rank of the first relevant result, or 0. |
| ndcg | Binary relevance nDCG over the ranked results and case k. |
| required_tools_success | Whether every tool named by the case completed successfully. |
| evidence_entry_hit | Whether a successful entry.get/block.get returned a relevant fixture. |
| search_call_count | Number of recorded search calls. |
| latency_ms_total, latency_ms_average | Recorded tool-call latency in milliseconds. |
| judge_score, judge_error | Optional LLM judge result or failure message. |

The run report contains metadata, one cases item per suite case, a summary
containing averages/aggregates, and an optional baseline_comparison with
compatible, metric deltas, and threshold violations. A baseline is
incompatible when its schema, suite/case order, provider, endpoint, or model
metadata differs from the current run; such a comparison must not be treated
as a retrieval regression.

Temporary benchmark entries are hidden from normal retrieval when no
benchmark run is active, and are purged on finish, abort, or daemon startup
recovery. Cleanup does not modify the Git catalog or baseline files.

---

### MCP lifecycle{#mcp}

nomai is a native MCP (Model Context Protocol) server. Any MCP-compatible client (Claude Desktop, Cursor, etc.) can connect via stdio and call all RPCs as tools.

| Method       | Params              | Returns                                       |
| ------------ | ------------------- | --------------------------------------------- |
| `initialize` | `{}`                | `{protocolVersion, capabilities, serverInfo}` |
| `tools/list` | `{}`                | `{tools: [{name, description, inputSchema}]}` |
| `tools/call` | `{name, arguments}` | `{content: [{type: "text", text}]}`           |

All built-in RPCs + batch + any custom registered RPCs appear as MCP tools automatically.

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
| `1003`   | Validation error | Bad attrs (non-object), FK violation, UNIQUE conflict, missing required params. Attachment-specific messages: `attachment too large: <name> (<N> bytes > <max>)`, `declared source not found: <name>`, `image block missing required attr: src`, `invalid base64 for attachment: <name>`, `unsafe attachment filename: <name>`, `attachment not found: <name>` |
| `1004`   | Config error     | Missing env var, malformed config                                                     |
| `1005`   | FS error         | Filesystem I/O failure (data has `kind` field from `io::ErrorKind`)                   |
| `1006`   | .nomai format    | Parse error in a `.nomai` file (data has `parse_error` field)                         |
| `1007`   | Sync error       | Rebase conflict during `sync.run` (data has `conflicted_files` array); resolve in editor + `git add`, then re-run |

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
attachment_max_bytes = 10485760              # 10 MiB default; decode-time cap on each attachment (1003 if exceeded)

[embedding]
base_url    = "https://your-embedding-endpoint/v1"
api_key_env = "NOMAI_EMBEDDING_API_KEY"  # references env var name, not the key itself
model       = "your-embedding-model"
dim         = 1024              # must match model's output dim

[llm]
base_url    = "http://your-llm-endpoint/v1"
api_key_env = "NOMAI_LLM_API_KEY"
model       = "your-model"

[cache]
warn_rows   = 100000            # soft cap; cache.stats returns warning=true above

[chunking]
target_size = 1024              # chunk char budget; paragraph → sentence → hard cut

[development]
enabled = false                 # exposes benchmark.* only when true
benchmark_cases_dir = "benchmark/cases"
benchmark_suites_dir = "benchmark/suites"
benchmark_baselines_dir = "benchmark/baselines"
```

**API keys are referenced by env var name**, never stored in the config file. Set the env vars in your shell:

```fish
set -Ux NOMAI_EMBEDDING_API_KEY "sk-..."
set -Ux NOMAI_LLM_API_KEY "sk-..."
```

**Changing `dim`**: requires deleting `db.sqlite` and re-creating all entries/embeddings. The dimension is fixed at table-creation time.

**Changing `chunking.target_size`**: takes effect on the next `entry.create` / `block.append` / `block.update`. Existing chunks keep the old size until you run `index.rebuild` (which re-derives chunks from `.nomai` files). Mixed sizes within a knowledge base are fine for retrieval but cause KNN distance variance — prefer one size per deployment.

**Compatible endpoints**: any OpenAI-compatible `/v1/embeddings` and `/v1/chat/completions` endpoint works — OpenAI, DeepSeek, Moonshot, local Ollama, etc.

**Development benchmark settings**: development.enabled defaults to false.
When enabled, all three benchmark directories must already exist at daemon
startup. The setting is read once at startup; rerun the MCP installer with
the selected config path and restart the client/daemon after changing it.
The daemon does not create cases, suites, baselines, or benchmark reports in
those directories.

---

## Retrieval modes

Three ways to find entries. Pick based on what you're matching.

| Mode                 | Method                              | Matches by                            | Best for                            |
| -------------------- | ----------------------------------- | ------------------------------------- | ----------------------------------- |
| **Fulltext**         | `search.fulltext`                   | Trigram substring (FTS5 bm25, ≥3 chars) or LIKE fallback (<3 chars) | Keyword search, exact terms, CJK phrases |
| **Semantic (entry)** | `search.semantic` (default)         | Cosine similarity of entry embeddings | Concept search, "find similar"      |
| **Semantic (chunk)** | `search.semantic granularity=chunk` | Cosine similarity of chunk embeddings | Long-doc RAG, sub-passage retrieval |

**Fulltext tokenizer**: `fts_blocks` uses SQLite FTS5's `trigram` tokenizer. CJK (Chinese/Japanese/Korean) text is matched by 3-character substring, so Chinese phrases now return matches. Queries of ≥3 characters go through FTS5 bm25 ranking. Queries of 1–2 characters (e.g. `"Go"`, `"管理"`) fall back to a `LIKE '%q%'` scan automatically — still case-insensitive and deduped to entries, but ordered by recency rather than relevance. For semantic matching on short terms, use `search.semantic`.

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

`cache.clear({namespace: "embeddings"})` (the default) removes rows from `emb_cache` but does not reset `hits` / `misses` — those reflect lifetime cache activity, not current contents. To reset, restart the daemon.

**Capacity and eviction**: the cache **grows without bound** and is **never auto-evicted**. This is deliberate — eviction would force a re-computation that costs real API money (the same body re-embedded). Instead, configure `warn_rows` as a soft threshold; when `cache.stats` reports `warning: true`, run `cache.clear` with the filter that fits your situation:

- `cache.clear({model: "old-model"})` — old model leftover after a config switch (`namespace` defaults to `"embeddings"`)
- `cache.clear({before: "2026-01-01T00:00:00Z"})` — bulk-clear stale rows
- `cache.clear({keep_recent: 10000})` — trim to the most recent 10K rows

Each row is ~8KB at 2048 dims (`dim × 4 bytes + 32B hash + metadata`), so 100K rows ≈ 800MB. Pick `warn_rows` based on your disk budget; the default of 100K is a sensible starting point.

**Model isolation**: the `model` column namespaces rows; switching `config.embedding.model` starts a fresh cache namespace automatically. Old model rows remain until cleared manually.

**dim changes**: `dim` is part of the lookup condition (`WHERE model = ? AND body_hash = ? AND dim = ?`) but not the primary key, so a config change to `dim` automatically misses the old rows and re-computes. Use `cache.clear({model: ...})` to reclaim the space.

**What is NOT cached**:

- LLM completions (`llm.complete`) — non-deterministic with temperature > 0
- Entry / chunk objects — SQLite's own page cache covers B-tree nodes

(Search results from `search.semantic` / `search.fulltext` **are** cached — see [Search results cache](#search-results-cache) below.)

**Cache layering**:

| Layer                 | Caches                                       | Owned by                          |
| --------------------- | -------------------------------------------- | --------------------------------- |
| SQLite page cache     | B-tree nodes (disk I/O avoidance)            | SQLite (`cache_size`)             |
| **emb_cache table**   | **`(model, body) → vector`**                 | **nomai (this section)**          |
| **search cache**      | **`(rpc, query, limit, ...) → results`**     | **nomai ([Search results cache](#search-results-cache))** |
| In-memory LRU         | measured, declined — see `crates/core/examples/bench_object_cache.rs` | —                                 |

---

## Search results cache{#search-results-cache}

Every `search.semantic` and `search.fulltext` call is wrapped in a transparent in-memory cache. Same query (same RPC, same query text, same limit, same block-type filter) within the current generation returns the previous result without re-running FTS5 or KNN. The cache is invalidated wholesale on any mutation.

**Why cache**: search results are deterministic functions of `(query, current index state)` for any given generation. Within a readheavy workload (the common case for a knowledge base — many queries between writes), the same query is often repeated by different agents/sessions, and re-running it just re-runs the same SQLite work. The cache turns the second call into a hash-map lookup.

**Where it applies** — both read RPCs:

- `search.semantic` (entry and chunk granularity)
- `search.fulltext`

**Transparency**: the cache wraps the search handlers inside `Daemon::dispatch`. Clients see no API change — same params in, same result out, just faster on a hit.

**Invalidation — generation counter**: the daemon holds a monotonically increasing `generation` counter. Every mutation that could change search results bumps it atomically:

| Hook point | When it bumps |
| ---------- | ------------- |
| `entry.create` / `entry.update` / `entry.delete` | After the entry write lands |
| `block.append` / `block.update` / `block.delete` | After the block write lands |
| `index.sync` / `index.rebuild` | After the index refreshes (`index.sync` only when it actually mutates) |

Cached entries are keyed by the current `generation`, so a bump effectively drops them all — the next search misses and recomputes from current state. There is no per-entry invalidation logic and no staleness window: a cached result is, by construction, consistent with the most recent mutation the daemon has applied.

**Hit semantics** (mirrored in `cache.stats` → `searches`):

| Field                          | Meaning                                                                |
| ------------------------------ | ---------------------------------------------------------------------- |
| `generation`                   | Current generation (bumps on every mutation)                           |
| `entries`                      | Cached results currently held in memory                                |
| `hits` / `misses`              | Lifetime counters across both RPCs                                     |
| `hit_rate`                     | `hits / (hits + misses)`, or 0.0 when both are zero                    |
| `by_rpc.semantic.{hits,misses}`   | Counters for `search.semantic`                                      |
| `by_rpc.fulltext.{hits,misses}`   | Counters for `search.fulltext`                                      |

**Capacity**: in-memory, unbounded, never auto-evicted (only invalidated by generation bumps). A single cached entry is small (a key + a JSON result array), and a busy read workload produces a bounded working set of distinct queries — typical deployments won't see meaningful growth. If you want to drop everything, run `cache.clear({namespace: "searches"})`.

**Cache key**: `(generation, rpc, query_hash, limit, block_type_hash)`. Two calls hit the cache only if all five match — different limit, different granularity, or a different generation all miss.

**Counter reset**: `hits` / `misses` are **not** reset by `cache.clear({namespace: "searches"})` — they reflect lifetime activity. Restart the daemon to reset them.
