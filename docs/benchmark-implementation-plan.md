# Benchmark Capability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `nomai` daemon 增加一个仅在 development mode 开启时暴露的 benchmark capability，使模型通过 `benchmark.next_case` 获取预设问题并真实调用搜索/证据工具，最后得到可和 Git baseline 比较的端到端回归报告。

**Architecture:** benchmark 不实现完整 agent，也不接管模型循环；它是 `nomai-daemon` 内的可选 RPC/MCP handler 集合和一个单活跃 run 的 runtime。Git 中的 case/suite/baseline 由 `nomai-kb` 管理，`benchmark.start` 将 case fixture 作为带 benchmark 标记的临时 nomai entries 加载，daemon 在 dispatch 层记录搜索和证据调用，`finish` 计算指标、比较只读 baseline，并清理临时 entries。

**Tech Stack:** Rust 2021 workspace、`serde`/`toml`/`serde_json`、现有 `RpcHandler` + MCP `tools/list`、SQLite/`EntryService`、`tokio`、`chrono`、`ulid`、现有 `LlmProvider`（可选 judge）。不新增 agent framework 或新的持久化服务。

## Global Constraints

- `development.enabled` 默认必须是 `false`；关闭时 benchmark handler 不注册，不出现在 `tools/list`，直接调用也返回现有 `METHOD_NOT_FOUND`。
- 修改 development 配置后，支持流程是重新运行 `install.sh` 或 `install-codex.sh` 并重启客户端；daemon 不提供动态热加载，启动时读取一次配置。
- 只有 daemon 的 development 配置决定工具是否暴露；skill 的存在不能绕过这个 gate。
- benchmark case、suite、baseline 必须是 `nomai-kb` Git 文件；模型和 RPC 不能写入或替换 baseline。
- 不实现完整 agent runner；模型必须通过 `benchmark.next_case` 获取问题，并自行调用真实 MCP 工具。
- 只允许一个 active benchmark run；`finish`、`abort` 和 daemon 启动恢复都必须清理 benchmark 临时 entries。
- 临时 entries 使用 `transient=true`、`benchmark_run_id`、`benchmark_case_id` 三个 attrs 标记；普通 entry/search/block 读取路径必须将其排除，删除产生的 append-only event 必须保留。
- 不改变 development mode 关闭时的现有行为和 `tools_list_snapshot.json`；所有已有构造函数保持兼容，新增参数使用 builder/helper，而不是修改现有 positional API。
- 所有新增 Rust 代码遵循当前模块风格：handler 使用零大小类型实现 `RpcHandler`，参数使用 `serde` + `schemars`，阻塞 SQLite/文件操作放入现有 `blocking` 模式，错误统一转换为 `CoreError`。
- 每个任务先写针对行为的失败测试，再写最小实现；每个任务独立运行相关测试并提交一个小 commit。不得创建或修改 `docs/superpowers` 下的文件。

## File Map

`nomai`：

- Modify `crates/daemon/src/config.rs`: 增加 `DevelopmentConfig`、默认值和 enabled 时的路径校验。
- Create `crates/daemon/src/benchmark/mod.rs`: runtime、active run、fixture 生命周期、trace 收集和对外 session API。
- Create `crates/daemon/src/benchmark/cases.rs`: case/suite/baseline 文件的 typed schema、目录加载和引用校验。
- Create `crates/daemon/src/benchmark/metrics.rs`: retrieval/evidence/tool/latency 指标计算。
- Create `crates/daemon/src/benchmark/baseline.rs`: baseline metadata compatibility 和 threshold delta 比较。
- Create `crates/daemon/src/handlers/benchmark.rs`: `benchmark.start/next_case/record_answer/finish/abort/status` handler。
- Modify `crates/daemon/src/handlers/registry.rs`, `crates/daemon/src/handlers/mod.rs`: 条件注册和模块导出。
- Modify `crates/daemon/src/daemon.rs`, `crates/daemon/src/rpc.rs`: runtime 注入、启动恢复、dispatch 级调用 instrumentation。
- Modify `crates/core/src/service.rs`, `crates/core/src/chunk_service.rs` 及对应单元测试：benchmark entry predicate、读路径过滤和 cleanup API。
- Modify `crates/protocol/src/method.rs`: benchmark method constants。
- Create `crates/daemon/tests/benchmark_e2e.rs`: enabled/disabled MCP、完整 model-like workflow、cleanup、baseline 和错误路径测试。
- Modify `crates/daemon/tests/snapshot_test.rs` only if a helper API requires it; keep the committed disabled snapshot unchanged.

`nomai-kb`：

- Create `/Users/johnlin/Dev/rust/nomai-kb/benchmark/cases/search-rust-errors-001.toml` and at least one second case covering fulltext/evidence access。
- Create `/Users/johnlin/Dev/rust/nomai-kb/benchmark/suites/search-regression.toml`。
- Create `/Users/johnlin/Dev/rust/nomai-kb/benchmark/baselines/search-regression.json`，只包含固定 metadata/metrics/threshold，不提供写入工具。
- Create `/Users/johnlin/Dev/rust/nomai-kb/skills/nomai-benchmark/SKILL.md`。
- Modify `/Users/johnlin/Dev/rust/nomai-kb/install.sh`, `/Users/johnlin/Dev/rust/nomai-kb/install.ps1`, `/Users/johnlin/Dev/rust/nomai-kb/install-codex.sh`, `/Users/johnlin/Dev/rust/nomai-kb/install-codex.ps1`: 维持 skill auto-discovery，重新注册 daemon，并输出 development mode 的安装/重启约束。
- Modify `/Users/johnlin/Dev/rust/nomai-kb/README.md`: 配置示例、真实工具调用顺序、case/suite/baseline 目录约定和安全限制。

**Test fixture convention:** snippets below use test-local helpers, never production APIs. Add these helpers to the named test module before the test that calls them: `parse_test_config_without_development() -> Config` and `config_with_development(enabled: bool, cases_dir: PathBuf) -> Config` in `config.rs`; `test_development_dirs() -> DevelopmentConfig` and `dirs_with_suite_case(case_id: &str) -> DevelopmentConfig` in `benchmark/cases.rs`; `case_with_two_relevant_ids() -> CaseSpec`, `trace_rank_two_hit() -> CaseTrace`, `resolved_ids() -> ResolvedFixtureIds`, `current_report(model: &str) -> RunReport`, and `baseline(model: &str) -> BaselineFile` in the relevant benchmark test modules; and `create_entry(svc: &EntryService, attrs: Value) -> Entry` plus `event_types(svc: &EntryService) -> Vec<String>` in the core service tests. These helpers may use `tempfile`, the existing `EntryService::for_test`, and existing null providers, but must not be added to the public library API.

---

### Task 1: Development Configuration And Handler Gate

**Files:**
- Modify: `crates/daemon/src/config.rs`
- Modify: `crates/daemon/src/handlers/registry.rs`
- Modify: `crates/daemon/src/handlers/mod.rs`
- Modify: `crates/protocol/src/method.rs`
- Test: `crates/daemon/src/config.rs`, `crates/daemon/src/handlers/registry.rs`

**Interfaces:**
- Produce `pub struct DevelopmentConfig { pub enabled: bool, pub benchmark_cases_dir: PathBuf, pub benchmark_suites_dir: PathBuf, pub benchmark_baselines_dir: PathBuf }` with `Default`.
- Produce `pub fn registry_with_benchmark(enabled: bool) -> HashMap<&'static str, Arc<dyn RpcHandler>>`; preserve `pub fn registry()` as `registry_with_benchmark(false)` for existing callers.
- Add `nomai_protocol::method::benchmark::{START, NEXT_CASE, RECORD_ANSWER, FINISH, ABORT, STATUS}` constants.

- [ ] **Step 1: Write failing config and registry tests.**

```rust
#[test]
fn development_defaults_to_disabled_and_relative_dirs() {
    let config = parse_test_config_without_development();
    assert!(!config.development.enabled);
    assert_eq!(config.development.benchmark_cases_dir, PathBuf::from("benchmark/cases"));
}

#[test]
fn enabled_development_requires_existing_benchmark_dirs() {
    let config = config_with_development(true, PathBuf::from("/missing/cases"));
    assert!(matches!(config.validate(), Err(ConfigError::Invalid(_))));
}

#[test]
fn registry_does_not_expose_benchmark_when_disabled() {
    let methods = registry_with_benchmark(false);
    assert!(!methods.contains_key("benchmark.start"));
}
```

- [ ] **Step 2: Run the focused tests and verify failure.**

Run: `cargo test -p nomai-daemon config::tests::development -- --nocapture && cargo test -p nomai-daemon handlers::registry::tests -- --nocapture`

Expected: compile/test failure because `Config.development`, `ConfigError::Invalid`, the method constants, and `registry_with_benchmark` do not yet exist.

- [ ] **Step 3: Implement the minimal config and gate.** Add `#[serde(default)] pub development: DevelopmentConfig` to `Config`; validate the three directories only when `enabled=true`; register benchmark handlers only in the true branch. Keep the existing `registry()` wrapper unchanged for all old test helpers.

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct DevelopmentConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_benchmark_cases_dir")]
    pub benchmark_cases_dir: PathBuf,
    #[serde(default = "default_benchmark_suites_dir")]
    pub benchmark_suites_dir: PathBuf,
    #[serde(default = "default_benchmark_baselines_dir")]
    pub benchmark_baselines_dir: PathBuf,
}

impl Default for DevelopmentConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            benchmark_cases_dir: PathBuf::from("benchmark/cases"),
            benchmark_suites_dir: PathBuf::from("benchmark/suites"),
            benchmark_baselines_dir: PathBuf::from("benchmark/baselines"),
        }
    }
}
```

- [ ] **Step 4: Run focused and compatibility tests.**

Run: `cargo test -p nomai-daemon config::tests handlers::registry::tests`

Expected: PASS, including old minimal TOML parsing tests and disabled registry behavior.

- [ ] **Step 5: Commit the isolated gate.**

```bash
git add crates/daemon/src/config.rs crates/daemon/src/handlers/registry.rs crates/daemon/src/handlers/mod.rs crates/protocol/src/method.rs
git commit -m "feat: gate benchmark handlers behind development mode"
```

### Task 2: Case, Suite, Fixture, And Baseline Parsing

**Files:**
- Create: `crates/daemon/src/benchmark/mod.rs`
- Create: `crates/daemon/src/benchmark/cases.rs`
- Create: `crates/daemon/src/benchmark/baseline.rs`
- Modify: `crates/daemon/src/lib.rs`
- Test: `crates/daemon/src/benchmark/cases.rs`, `crates/daemon/src/benchmark/baseline.rs`

**Interfaces:**
- Produce `pub(crate) struct BenchmarkCatalog` with `load(config: &DevelopmentConfig) -> Result<Self, CoreError>`, `suite(&self, id: &str) -> Result<&SuiteSpec, CoreError>`, and `case(&self, id: &str) -> Result<&CaseSpec, CoreError>`.
- Produce typed specs:

```rust
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CaseSpec {
    pub id: String,
    pub question: String,
    #[serde(default)] pub fixtures: Vec<FixtureEntrySpec>,
    pub retrieval: RetrievalSpec,
    pub answer: AnswerSpec,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct FixtureEntrySpec {
    pub id: Ulid,
    pub title: String,
    #[serde(default)] pub tags: Vec<String>,
    #[serde(default)] pub attrs: serde_json::Value,
    pub blocks: Vec<FixtureBlockSpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct FixtureBlockSpec {
    pub id: Ulid,
    #[serde(rename = "type")] pub block_type: String,
    pub text: String,
    #[serde(default)] pub attrs: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RetrievalSpec {
    pub required_tools: Vec<String>,
    pub relevant_entry_ids: Vec<Ulid>,
    #[serde(default)] pub relevant_block_ids: Vec<Ulid>,
    pub k: u32,
}
```

- [ ] **Step 1: Write parser tests for valid data and each rejection.** Cover duplicate case IDs, suite references to unknown cases, invalid block type, `k=0`, missing relevant fixture IDs, and a baseline JSON with schema version, suite ID, case IDs, embedding model, LLM model, metrics, and thresholds.

```rust
#[test]
fn loads_case_and_resolves_fixture_ids() {
    let catalog = BenchmarkCatalog::load(&test_development_dirs()).unwrap();
    let case = catalog.case("search-rust-errors-001").unwrap();
    assert_eq!(case.retrieval.k, 5);
    assert!(case.fixtures.iter().any(|entry| entry.id == FIXTURE_ENTRY_ID));
}

#[test]
fn rejects_suite_with_unknown_case() {
    let err = BenchmarkCatalog::load(&dirs_with_suite_case("missing")).unwrap_err();
    assert!(err.to_string().contains("unknown case"));
}
```

- [ ] **Step 2: Run the parser tests and verify failure.**

Run: `cargo test -p nomai-daemon benchmark::cases benchmark::baseline`

Expected: FAIL to compile because the benchmark modules and typed schemas are absent.

- [ ] **Step 3: Implement typed TOML/JSON loading.** Read only files under configured directories, sort paths for deterministic runs, deserialize with `toml::from_str`, validate all cross-file references and fixture IDs, and convert IO/parse errors to `CoreError::Config` containing the file path. Baselines are deserialized read-only; no API may serialize them back.

- [ ] **Step 4: Run focused tests and a format check.**

Run: `cargo fmt --all -- --check && cargo test -p nomai-daemon benchmark::cases benchmark::baseline`

Expected: PASS with deterministic catalog ordering and actionable path-specific validation errors.

- [ ] **Step 5: Commit the catalog boundary.**

```bash
git add crates/daemon/src/lib.rs crates/daemon/src/benchmark
git commit -m "feat: add benchmark catalog and baseline schemas"
```

### Task 3: Retrieval Metrics And Baseline Comparison

**Files:**
- Create: `crates/daemon/src/benchmark/metrics.rs`
- Modify: `crates/daemon/src/benchmark/baseline.rs`
- Test: `crates/daemon/src/benchmark/metrics.rs`, `crates/daemon/src/benchmark/baseline.rs`

**Interfaces:**
- Produce `pub(crate) struct CaseTrace` containing ordered `ToolTrace` values and optional answer text.
- Produce `pub(crate) struct CaseMetrics` with `hit_at_k`, `recall_at_k`, `mrr`, `ndcg`, `required_tools_success`, `evidence_entry_hit`, `search_call_count`, `latency_ms_total`, `latency_ms_average`, and optional `judge_score`.
- Produce `pub(crate) fn score_case(case: &CaseSpec, trace: &CaseTrace, resolved: &ResolvedFixtureIds) -> CaseMetrics`.
- Produce `pub(crate) fn compare_baseline(current: &RunReport, baseline: &BaselineFile) -> BaselineComparison` where comparison reports metadata compatibility, each metric delta, threshold violations, and never mutates the baseline.

- [ ] **Step 1: Write exact metric tests before implementation.** Use a relevant set of two IDs and a ranked result list where the first relevant result is rank 2; assert `hit@k=1`, `recall@k=0.5`, `MRR=0.5`, binary `nDCG=1/log2(3)`, required-tool failure when a required call errors, and evidence hit only when `entry.get`/`block.get` accessed a resolved relevant ID.

```rust
#[test]
fn scores_ranked_retrieval_and_evidence() {
    let metrics = score_case(&case_with_two_relevant_ids(), &trace_rank_two_hit(), &resolved_ids());
    assert_eq!(metrics.hit_at_k, 1.0);
    assert_eq!(metrics.recall_at_k, 0.5);
    assert_eq!(metrics.mrr, 0.5);
    let expected_ndcg = (1.0 / 3.0_f64.log2()) / (1.0 + 1.0 / 3.0_f64.log2());
    assert!((metrics.ndcg - expected_ndcg).abs() < 1e-9);
    assert!(metrics.evidence_entry_hit);
}

#[test]
fn incompatible_baseline_is_not_compared_as_a_pass() {
    let result = compare_baseline(&current_report("model-b"), &baseline("model-a"));
    assert!(!result.compatible);
    assert!(result.violations.iter().any(|v| v.contains("model")));
}
```

- [ ] **Step 2: Run the metric tests and verify failure.**

Run: `cargo test -p nomai-daemon benchmark::metrics benchmark::baseline`

Expected: FAIL because scoring and comparison functions are not defined.

- [ ] **Step 3: Implement deterministic scoring.** Use top-`k` ordered IDs from successful `search.semantic`/`search.fulltext` traces, binary relevance, `MRR=1/rank` for the first relevant result, `nDCG=DCG/IDCG`, zero for empty retrieved/relevant sets only after schema validation, and aggregate required-tool success from the trace. Preserve raw call traces and latency so the report can explain a regression.

- [ ] **Step 4: Implement baseline metadata and thresholds.** Require matching schema version, suite/case IDs, embedding model, LLM model, and provider metadata before deltas are considered valid. A threshold violation is `current < minimum` for lower-bound metrics or `current > maximum` for upper-bound latency/call-count metrics. Return the comparison in the report, never update the JSON file.

- [ ] **Step 5: Run focused tests.**

Run: `cargo test -p nomai-daemon benchmark::metrics benchmark::baseline`

Expected: PASS with exact floating-point assertions using a small tolerance and with incompatible metadata marked `compatible=false`.

- [ ] **Step 6: Commit scoring.**

```bash
git add crates/daemon/src/benchmark/metrics.rs crates/daemon/src/benchmark/baseline.rs
git commit -m "feat: score benchmark retrieval and compare baselines"
```

### Task 4: Runtime Lifecycle And Temporary Entry Isolation

**Files:**
- Modify: `crates/core/src/service.rs`
- Modify: `crates/core/src/chunk_service.rs`
- Modify: `crates/daemon/src/benchmark/mod.rs`
- Modify: `crates/daemon/src/daemon.rs`
- Test: `crates/core/src/service.rs`, `crates/daemon/src/benchmark/mod.rs`

**Interfaces:**
- Produce core constants/functions:

```rust
pub const BENCHMARK_ENTRY_PREDICATE: &str = "(json_extract(e.attrs, '$.benchmark_run_id') IS NOT NULL AND json_extract(e.attrs, '$.benchmark_case_id') IS NOT NULL)";
pub fn EntryService::purge_benchmark_entries(&self) -> Result<Vec<Ulid>, CoreError>;
pub fn EntryService::is_benchmark_entry(&self, id: Ulid) -> Result<bool, CoreError>;
```

- Produce runtime APIs:

```rust
pub(crate) struct BenchmarkRuntime { /* catalog, config paths, Mutex<Option<ActiveRun>> */ }
pub(crate) fn BenchmarkRuntime::new(config: DevelopmentConfig, daemon_entries: Arc<EntryService>) -> Result<Self, CoreError>;
pub(crate) fn BenchmarkRuntime::recover_stale_entries(&self) -> Result<Vec<Ulid>, CoreError>;
pub(crate) fn BenchmarkRuntime::start(&self, suite_id: &str) -> Result<StartResult, CoreError>;
pub(crate) fn BenchmarkRuntime::next_case(&self, run_id: &str) -> Result<NextCaseResult, CoreError>;
pub(crate) fn BenchmarkRuntime::record_rpc(&self, method: &str, params: &Value, result: &Result<Value, CoreError>, latency: Duration);
pub(crate) fn BenchmarkRuntime::record_answer(&self, run_id: &str, case_id: &str, answer: String, llm: &dyn LlmProvider) -> Result<CaseReport, CoreError>;
pub(crate) fn BenchmarkRuntime::finish(&self, run_id: &str, llm: &dyn LlmProvider) -> Result<RunReport, CoreError>;
pub(crate) fn BenchmarkRuntime::abort(&self, run_id: &str) -> Result<AbortResult, CoreError>;
pub(crate) fn BenchmarkRuntime::status(&self) -> StatusResult;
```

- [ ] **Step 1: Write core isolation tests.** Create an ordinary entry and a benchmark-marked entry. Assert default `entry.list`, `entry.get`, fulltext search, semantic search, block get/list, and chunk get/list never return the marked entry; assert `purge_benchmark_entries` deletes only marked rows and leaves `events` rows.

```rust
#[test]
fn benchmark_entries_are_hidden_and_cleanup_preserves_delete_events() {
    let svc = EntryService::for_test().unwrap();
    let ordinary = create_entry(&svc, json!({}));
    let temporary = create_entry(&svc, json!({
        "transient": true,
        "benchmark_run_id": "run-1",
        "benchmark_case_id": "case-1"
    }));
    assert!(svc.get(ordinary.id).is_ok());
    assert!(matches!(svc.get(temporary.id), Err(CoreError::NotFound(_))));
    let deleted = svc.purge_benchmark_entries().unwrap();
    assert_eq!(deleted, vec![temporary.id]);
    assert!(event_types(&svc).iter().any(|t| t == "entry.deleted"));
}
```

- [ ] **Step 2: Run the isolation tests and verify failure.**

Run: `cargo test -p nomai-core benchmark_entries -- --nocapture`

Expected: FAIL because existing transient filtering does not know the benchmark markers and no cleanup API exists.

- [ ] **Step 3: Implement exclusion and cleanup.** Extend SQL predicates in entry list/get, block/chunk access, FTS, and vector search to exclude `BENCHMARK_ENTRY_PREDICATE`; keep the existing transient demotion behavior for non-benchmark transient entries. Delete through `EntryService::delete` so file cleanup and `entry.deleted` events use existing semantics. In runtime startup, purge stale entries before loading a new catalog; call `search_cache.clear()` and `search_cache.bump_generation()` after load/cleanup.

- [ ] **Step 4: Implement fixture load and run state.** `start` rejects an existing active run, loads the suite, creates each fixture with `CreateEntry` plus attrs `{transient:true, benchmark_run_id, benchmark_case_id}`, and stores source fixture ID → actual entry/block ID mappings. The returned object contains only run metadata. `next_case` advances in suite order and returns only `{run_id, case_id, question}`; it must not serialize reference answers, relevant IDs, rubric, or baseline data.

- [ ] **Step 5: Implement lifecycle cleanup tests.** Cover finish, abort, a second start while active, `next_case` after the suite is exhausted, invalid run IDs, and a fresh runtime recovering entries from a previous run. Assert all paths leave no benchmark rows and ordinary rows remain.

- [ ] **Step 6: Run core and runtime tests.**

Run: `cargo test -p nomai-core benchmark_entries && cargo test -p nomai-daemon benchmark::mod`

Expected: PASS; cleanup is idempotent and stale recovery does not remove ordinary transient entries.

- [ ] **Step 7: Commit lifecycle/isolation.**

```bash
git add crates/core/src/service.rs crates/core/src/chunk_service.rs crates/daemon/src/benchmark/mod.rs crates/daemon/src/daemon.rs
git commit -m "feat: isolate benchmark fixtures and manage run lifecycle"
```

### Task 5: Dispatch Instrumentation Without Search Behavior Changes

**Files:**
- Modify: `crates/daemon/src/rpc.rs`
- Modify: `crates/daemon/src/daemon.rs`
- Modify: `crates/daemon/src/handlers/search.rs`
- Test: `crates/daemon/src/daemon.rs`, `crates/daemon/src/benchmark/mod.rs`

**Interfaces:**
- `Daemon` stores `pub(crate) benchmark: Option<Arc<BenchmarkRuntime>>`; all normal test constructors use `None` unless an explicit development helper is requested.
- `RpcHandler::call` signatures remain unchanged.
- `BenchmarkRuntime::record_rpc` extracts, for the supported methods, `search.semantic` chunk/entry IDs and scores, `search.fulltext` entry/block IDs and scores, `entry.get` entry/block IDs, `block.get` block/entry IDs, success/error, and elapsed duration.

- [ ] **Step 1: Write instrumentation tests.** Use a fake `BenchmarkRuntime` and a fake handler response to assert a dispatch records one trace with the exact method, `ok`, latency `>= 0`, ordered returned IDs, and error status. Add a regression assertion that two identical search calls still use the existing cache and return byte-equivalent JSON.

- [ ] **Step 2: Run focused tests and verify failure.**

Run: `cargo test -p nomai-daemon dispatch_records_benchmark_trace search_cache`

Expected: FAIL because dispatch does not call the runtime and trace extraction is absent.

- [ ] **Step 3: Instrument at the dispatch chokepoint.** Clone params before handler execution, measure with `Instant`, call the handler exactly once, then call `record_rpc` for the supported methods before mapping the result to `Response`. Do not alter handler output, cache key, cache generation, transient ranking, or error mapping.

```rust
let started = std::time::Instant::now();
let result = handler.call(self, params.clone()).await;
if let Some(runtime) = &self.benchmark {
    runtime.record_rpc(req.method.as_str(), &params, &result, started.elapsed());
}
```

- [ ] **Step 4: Run search and instrumentation tests.**

Run: `cargo test -p nomai-daemon dispatch_records_benchmark_trace search:: -- --nocapture`

Expected: PASS and existing search serialization/cache tests remain green.

- [ ] **Step 5: Commit instrumentation.**

```bash
git add crates/daemon/src/rpc.rs crates/daemon/src/daemon.rs crates/daemon/src/handlers/search.rs
git commit -m "feat: instrument benchmark tool traces at dispatch"
```

### Task 6: Benchmark RPC Handlers And Optional Judge

**Files:**
- Create: `crates/daemon/src/handlers/benchmark.rs`
- Modify: `crates/daemon/src/handlers/registry.rs`
- Modify: `crates/daemon/src/handlers/mod.rs`
- Modify: `crates/daemon/src/benchmark/mod.rs`
- Test: `crates/daemon/src/handlers/benchmark.rs`

**Interfaces:**
- Implement zero-sized `Start`, `NextCase`, `RecordAnswer`, `Finish`, `Abort`, and `Status` types, each implementing `RpcHandler` with the method constants from Task 1.
- Wire JSON input/output as follows:

```text
benchmark.start        {"suite_id":"search-regression"}
                        -> {"run_id":"...","suite_id":"...","case_count":2,"provider":{...}}
benchmark.next_case     {"run_id":"..."}
                        -> {"run_id":"...","case_id":"...","question":"..."}
benchmark.record_answer {"run_id":"...","case_id":"...","answer":"..."}
                        -> {"case_id":"...","metrics":{...}}
benchmark.finish        {"run_id":"..."}
                        -> {"run_id":"...","cases":[...],"summary":{...},"baseline_comparison":{...}}
benchmark.abort         {"run_id":"..."}
                        -> {"run_id":"...","aborted":true,"deleted_entry_count":N}
benchmark.status         {}
                        -> {"enabled":true,"run_id":null|"...","case_id":null|"...","state":"idle|running|finished"}
```

- [ ] **Step 1: Write handler tests for schemas and information boundaries.** Assert `tools/list` schemas require the documented fields; `next_case` output contains no `reference`, `relevant_entry_ids`, `relevant_block_ids`, `judge`, `baseline`, or fixture body; wrong/missing run IDs return validation errors; all handlers are unavailable through a disabled registry.

- [ ] **Step 2: Run handler tests and verify failure.**

Run: `cargo test -p nomai-daemon handlers::benchmark`

Expected: FAIL because the handlers and registration are absent.

- [ ] **Step 3: Implement handler calls.** Parse params with `serde_json::from_value`, return `CoreError::Validation` for malformed/mismatched IDs, delegate lifecycle mutations to `BenchmarkRuntime`, and mark only fixture-mutating lifecycle calls as `is_mutating=true` if they write the knowledge root. `record_answer` passes the configured `LlmProvider` only when `answer.judge=true`; construct `CompletionRequest` with a fixed system rubric, the case question/reference, and model answer, parse a bounded numeric score, and preserve judge errors in the report without hiding retrieval metrics.

- [ ] **Step 4: Run focused tests.**

Run: `cargo fmt --all -- --check && cargo test -p nomai-daemon handlers::benchmark`

Expected: PASS, including schema snapshots for all six tools and no gold-data leakage from `next_case`.

- [ ] **Step 5: Commit handlers.**

```bash
git add crates/daemon/src/handlers/benchmark.rs crates/daemon/src/handlers/registry.rs crates/daemon/src/handlers/mod.rs crates/daemon/src/benchmark
git commit -m "feat: expose benchmark lifecycle RPC handlers"
```

### Task 7: Daemon Wiring, MCP Exposure, And End-To-End Tests

**Files:**
- Modify: `crates/daemon/src/daemon.rs`
- Modify: `crates/daemon/src/serve.rs` only if startup construction needs the existing config path flow
- Modify: `crates/daemon/tests/snapshot_test.rs` only to add an enabled helper; do not change disabled snapshot JSON
- Create: `crates/daemon/tests/benchmark_e2e.rs`

**Interfaces:**
- Production `Daemon::from_arc` builds the runtime only when `config.development.enabled`, runs stale cleanup at startup even when the previous process left benchmark rows, and passes `config.development.enabled` to registry construction.
- Existing `Daemon::for_test` and `Daemon::from_services` remain disabled; add `Daemon::from_services_with_development(..., development: DevelopmentConfig) -> Result<Self, CoreError>` or an equivalent explicit helper for integration tests without changing the old nine-argument function.

- [ ] **Step 1: Write the full model-like E2E test.** Build an enabled test daemon with temporary case/suite/baseline directories, call MCP `tools/list`, `benchmark.start`, repeatedly call `benchmark.next_case`, call `search.semantic`/`search.fulltext` and `entry.get` using the returned question, then call `benchmark.record_answer` and `benchmark.finish`. Assert the report contains metrics and baseline delta, while `next_case` never contains gold fields.

- [ ] **Step 2: Write disabled and recovery E2E tests.** Assert disabled `tools/list` exactly equals the existing snapshot, disabled direct `benchmark.start` returns `-32601`, and a new daemon removes stale marked rows while preserving ordinary entries/events.

- [ ] **Step 3: Run the new tests and verify failure.**

Run: `cargo test -p nomai-daemon --test benchmark_e2e -- --nocapture`

Expected: FAIL until constructor wiring, registry gating, runtime injection, and MCP dispatch are complete.

- [ ] **Step 4: Wire constructors and startup recovery.** Use the real `Config` path in `Daemon::from_arc`; keep lib-mode constructors deterministic and disabled by default. Ensure `tools/list` derives its list from `daemon.handlers`, so no separate MCP allowlist can drift.

- [ ] **Step 5: Run all daemon tests and snapshot verification.**

Run: `cargo test -p nomai-daemon --all-targets`

Expected: PASS; `snapshot_test` remains unchanged without `UPDATE_SNAPSHOTS`, and enabled tests see exactly six additional benchmark tools.

- [ ] **Step 6: Commit daemon integration.**

```bash
git add crates/daemon/src/daemon.rs crates/daemon/src/serve.rs crates/daemon/tests/benchmark_e2e.rs crates/daemon/tests/snapshot_test.rs
git commit -m "test: verify benchmark MCP workflow and disabled mode"
```

### Task 8: Git Benchmark Assets And `nomai-benchmark` Skill

**Files:**
- Create: `/Users/johnlin/Dev/rust/nomai-kb/benchmark/cases/search-rust-errors-001.toml`
- Create: `/Users/johnlin/Dev/rust/nomai-kb/benchmark/cases/search-rust-errors-002.toml`
- Create: `/Users/johnlin/Dev/rust/nomai-kb/benchmark/suites/search-regression.toml`
- Create: `/Users/johnlin/Dev/rust/nomai-kb/benchmark/baselines/search-regression.json`
- Create: `/Users/johnlin/Dev/rust/nomai-kb/skills/nomai-benchmark/SKILL.md`

**Interfaces:**
- Case TOML must contain `id`, `question`, inline fixture entries/blocks with stable ULIDs, `[retrieval]` (`required_tools`, `relevant_entry_ids`, `relevant_block_ids`, `k`), and `[answer]` (`reference`, `judge`).
- Suite TOML contains `id` and ordered `cases = [...]`; baseline JSON contains schema version, suite/case IDs, provider/model metadata, fixed metrics, and explicit lower/upper thresholds.
- The skill must instruct the model to execute only this sequence:

```text
benchmark.start -> benchmark.next_case -> real search/evidence tools -> benchmark.record_answer -> benchmark.next_case ... -> benchmark.finish
```

- [ ] **Step 1: Add representative cases and baseline fixtures.** Include one semantic retrieval case and one fulltext + evidence case. Use stable fixture ULIDs that satisfy parser validation. Ensure the baseline is checked in manually and has no generated timestamp or machine-specific path.

- [ ] **Step 2: Write the skill.** State that `benchmark.next_case` is the sole source of the question, that the model must not infer or inspect gold IDs/reference text, that it should call the required search tool and then fetch evidence when useful, that `record_answer` receives the model's answer only, and that it must call `abort` if a case cannot be completed. State that benchmark tools appear only when daemon `development.enabled=true`.

- [ ] **Step 3: Validate assets against the Rust parser.**

Run from `/Users/johnlin/Dev/rust/nomai`: `cargo test -p nomai-daemon benchmark::cases -- --nocapture`, with the parser test pointed at `/Users/johnlin/Dev/rust/nomai-kb/benchmark`. The `nomai-kb` repository itself is not a Cargo workspace.

Expected: PASS; both cases load, suite ordering is stable, and all relevant fixture IDs resolve.

- [ ] **Step 4: Commit only Git assets and skill in `nomai-kb`.**

```bash
cd /Users/johnlin/Dev/rust/nomai-kb
git add benchmark skills/nomai-benchmark/SKILL.md
git commit -m "feat: add nomai benchmark cases and skill"
```

### Task 9: Installer, Configuration, And User Documentation

**Files:**
- Modify: `/Users/johnlin/Dev/rust/nomai-kb/install.sh`
- Modify: `/Users/johnlin/Dev/rust/nomai-kb/install.ps1`
- Modify: `/Users/johnlin/Dev/rust/nomai-kb/install-codex.sh`
- Modify: `/Users/johnlin/Dev/rust/nomai-kb/install-codex.ps1`
- Modify: `/Users/johnlin/Dev/rust/nomai-kb/README.md`
- Test: `/Users/johnlin/Dev/rust/nomai-kb/tests/install_smoke.sh`

**Interfaces:**
- Installers continue auto-discovering every `skills/*/SKILL.md`, including `nomai-benchmark`; they do not implement a second TOML parser or decide tool exposure themselves.
- Each installer must always re-register the configured daemon command, print the configured `NOMAI_CONFIG` path, and print: `development.enabled` is read at daemon startup; after changing it, rerun this installer and restart Claude Code/Codex.
- If the configured benchmark directories are missing, the daemon reports the config error; installers must not silently enable benchmark mode or create untracked benchmark data.

- [ ] **Step 1: Write installer smoke tests.** With a temporary `HOME`, fake `claude` and `codex` executables that record arguments, and a sibling fake executable, assert the new skill is linked, MCP registration still contains the selected config path, re-running replaces the old registration, and no installer writes into the benchmark directories or edits baseline files.

- [ ] **Step 2: Run the smoke tests and shell syntax checks.** Create `/Users/johnlin/Dev/rust/nomai-kb/tests/install_smoke.sh` in Step 1 with `set -euo pipefail`, a temporary `HOME`, fake `claude`/`codex` recorders, and cleanup via `trap`.

Run from `/Users/johnlin/Dev/rust/nomai-kb`: `bash -n install.sh install-codex.sh && bash tests/install_smoke.sh`

Expected: the new smoke test fails until output/registration behavior is implemented; after implementation it reports the skill link and registration command.

- [ ] **Step 3: Update all four scripts symmetrically.** Preserve current binary detection, Claude scopes, Codex API-key injection, Windows symlink/copy fallback, and idempotent remove/add behavior. Only add benchmark-aware messaging and ensure the explicit `NOMAI_CONFIG` path is passed on every registration.

- [ ] **Step 4: Document the config and workflow.** Add a TOML example:

```toml
[development]
enabled = false
benchmark_cases_dir = "/Users/johnlin/Dev/rust/nomai-kb/benchmark/cases"
benchmark_suites_dir = "/Users/johnlin/Dev/rust/nomai-kb/benchmark/suites"
benchmark_baselines_dir = "/Users/johnlin/Dev/rust/nomai-kb/benchmark/baselines"
```

Document that enabling requires setting `enabled=true`, running the relevant installer, restarting the client, and verifying `tools/list`; disabling follows the same reinstall/restart path. Explain that baselines are read-only Git artifacts and that benchmark fixture entries are automatically removed on finish/abort/startup recovery.

- [ ] **Step 5: Run installer checks and commit `nomai-kb`.**

```bash
bash -n /Users/johnlin/Dev/rust/nomai-kb/install.sh /Users/johnlin/Dev/rust/nomai-kb/install-codex.sh
bash /Users/johnlin/Dev/rust/nomai-kb/tests/install_smoke.sh
cd /Users/johnlin/Dev/rust/nomai-kb
git add install.sh install.ps1 install-codex.sh install-codex.ps1 README.md tests
git commit -m "docs: document benchmark installation and development mode"
```

Expected: PASS on POSIX smoke tests; PowerShell changes are syntax-reviewed against the existing Windows installer flow.

### Task 10: End-To-End Verification And Handoff

**Files:**
- Modify only files identified by failing verification; do not update snapshots or baselines merely to make tests pass.
- Review: `docs/benchmark-implementation-plan.md`, both repository status/logs, and every benchmark-facing public response.

- [ ] **Step 1: Run formatting, lint, and workspace tests.**

Run in `nomai`:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Expected: formatting and clippy pass with no new warnings; the baseline workspace count remains at least the pre-feature `597 passed, 2 ignored` plus the new tests.

- [ ] **Step 2: Run the benchmark-specific black-box checks.** Build the release daemon, point a temporary config at the checked-in `nomai-kb/benchmark` directories, verify disabled `tools/list`, rerun the installer, restart the MCP client, verify the six enabled tools, execute one full case, and inspect `entry.list`/search after `finish`.

Expected: the model sees no benchmark tools before reinstall/restart, receives the question only from `benchmark.next_case`, records real search/evidence calls, receives metrics and baseline comparison, and sees no benchmark fixture after cleanup.

- [ ] **Step 3: Verify safety and determinism.** Confirm `benchmark.finish` cannot write baseline files, repeated `finish`/`abort` are rejected or idempotent by the documented contract, stale cleanup leaves ordinary transient entries, failed searches are represented in the report, and report case order equals suite order. Run the same local case twice and compare the structural report fields excluding latency/timestamps.

- [ ] **Step 4: Review Rust consistency.** Check public API compatibility, `Arc` ownership, short mutex scopes, no blocking filesystem/SQLite work across async awaits, `CoreError` conversion, deterministic path ordering, no `unwrap` on user data, and no new dependency unless an existing workspace dependency cannot provide the behavior.

- [ ] **Step 5: Check both worktrees before handoff.**

```bash
git -C /Users/johnlin/Dev/rust/nomai status --short
git -C /Users/johnlin/Dev/rust/nomai-kb status --short
git -C /Users/johnlin/Dev/rust/nomai diff --check
git -C /Users/johnlin/Dev/rust/nomai-kb diff --check
```

Expected: only intended implementation/docs/assets are present; there are no files under the prohibited documentation directory and no generated benchmark report/baseline changes.

## Self-Review

- Spec coverage: config gate and reinstall contract are covered by Tasks 1 and 9; `benchmark.next_case` question-only boundary by Tasks 4, 6, 7, and 8; real search/evidence instrumentation by Task 5; all six tools by Task 6; fixture load/cleanup/stale recovery by Task 4 and Task 7; metrics and baseline compatibility by Task 3; no agent runner by the architecture and skill constraints; Git assets and skill installation by Tasks 8 and 9.
- Placeholder scan: the plan contains no `TBD`, `TODO`, or unspecified “add appropriate handling” steps; each implementation boundary names a file, function/type, test command, and expected result.
- Type consistency: `DevelopmentConfig`, `BenchmarkCatalog`, `CaseSpec`, `CaseTrace`, `CaseMetrics`, `BenchmarkRuntime`, and `registry_with_benchmark(bool)` are introduced before consumers; runtime IDs are explicitly resolved from stable fixture IDs before metric scoring; handler payload names match the runtime methods and skill sequence.
