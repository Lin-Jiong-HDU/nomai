use std::collections::{BTreeMap, HashMap, HashSet};

use serde::Serialize;
use ulid::Ulid;

use crate::benchmark::cases::CaseSpec;

#[derive(Debug, Clone, Default)]
pub(crate) struct CaseTrace {
    pub tool_traces: Vec<ToolTrace>,
    pub answer: Option<String>,
    pub judge_score: Option<f64>,
    pub judge_error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ToolTrace {
    pub method: String,
    pub ok: bool,
    pub latency_ms: u64,
    pub results: Vec<RetrievedResult>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RetrievedResult {
    pub entry_id: Option<String>,
    pub block_id: Option<String>,
    pub score: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ResolvedFixtureIds {
    pub entries: HashMap<Ulid, Ulid>,
    pub blocks: HashMap<Ulid, Ulid>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CaseMetrics {
    pub hit_at_k: f64,
    pub recall_at_k: f64,
    pub mrr: f64,
    pub ndcg: f64,
    pub required_tools_success: bool,
    pub evidence_entry_hit: bool,
    pub search_call_count: u32,
    pub latency_ms_total: u64,
    pub latency_ms_average: u64,
    pub judge_score: Option<f64>,
    pub judge_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CaseReport {
    pub case_id: String,
    pub metrics: CaseMetrics,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SummaryMetrics {
    pub hit_at_k: f64,
    pub recall_at_k: f64,
    pub mrr: f64,
    pub ndcg: f64,
    pub required_tools_success: f64,
    pub evidence_entry_hit: f64,
    pub search_call_count: f64,
    pub latency_ms_total: f64,
    pub latency_ms_average: f64,
    pub judge_score: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RunMetadata {
    pub schema_version: u32,
    pub suite_id: String,
    pub case_ids: Vec<String>,
    pub provider_name: String,
    pub provider_base_url: String,
    pub embedding_model: String,
    pub llm_model: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RunReport {
    pub metadata: RunMetadata,
    pub cases: Vec<CaseReport>,
    pub summary: SummaryMetrics,
    pub baseline_comparison: Option<BaselineComparison>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BaselineComparison {
    pub compatible: bool,
    pub deltas: BTreeMap<String, f64>,
    pub violations: Vec<String>,
}

pub(crate) fn score_case(
    case: &CaseSpec,
    trace: &CaseTrace,
    resolved: &ResolvedFixtureIds,
) -> CaseMetrics {
    let relevant_entries: HashSet<String> = case
        .retrieval
        .relevant_entry_ids
        .iter()
        .filter_map(|id| resolved.entries.get(id))
        .map(ToString::to_string)
        .collect();
    let relevant_blocks: HashSet<String> = case
        .retrieval
        .relevant_block_ids
        .iter()
        .filter_map(|id| resolved.blocks.get(id))
        .map(ToString::to_string)
        .collect();

    let ranked: Vec<&RetrievedResult> = trace
        .tool_traces
        .iter()
        .filter(|call| call.ok && is_search_method(&call.method))
        .flat_map(|call| call.results.iter())
        .collect();
    let top_k = ranked.iter().take(case.retrieval.k as usize);
    let relevant_in_top_k: HashSet<String> = top_k
        .flat_map(|result| relevant_keys(result, &relevant_entries, &relevant_blocks))
        .collect();
    let relevant_count = relevant_entries.len() + relevant_blocks.len();

    let first_relevant_rank = ranked
        .iter()
        .position(|result| !relevant_keys(result, &relevant_entries, &relevant_blocks).is_empty());
    let hit_at_k = if relevant_in_top_k.is_empty() {
        0.0
    } else {
        1.0
    };
    let recall_at_k = if relevant_count == 0 {
        0.0
    } else {
        relevant_in_top_k.len() as f64 / relevant_count as f64
    };
    let mrr = first_relevant_rank
        .map(|rank| 1.0 / (rank as f64 + 1.0))
        .unwrap_or(0.0);
    let ndcg = ndcg_binary(
        &ranked,
        case.retrieval.k as usize,
        &relevant_entries,
        &relevant_blocks,
    );

    let required_tools_success = case.retrieval.required_tools.iter().all(|required| {
        trace
            .tool_traces
            .iter()
            .any(|call| call.method == *required && call.ok)
    });
    let evidence_entry_hit = trace.tool_traces.iter().any(|call| {
        call.ok
            && is_evidence_method(&call.method)
            && call.results.iter().any(|result| {
                result
                    .entry_id
                    .as_ref()
                    .is_some_and(|id| relevant_entries.contains(id))
                    || result
                        .block_id
                        .as_ref()
                        .is_some_and(|id| relevant_blocks.contains(id))
            })
    });
    let search_call_count = trace
        .tool_traces
        .iter()
        .filter(|call| is_search_method(&call.method))
        .count() as u32;
    let latency_ms_total = trace.tool_traces.iter().map(|call| call.latency_ms).sum();
    let latency_ms_average = if trace.tool_traces.is_empty() {
        0
    } else {
        latency_ms_total / trace.tool_traces.len() as u64
    };

    CaseMetrics {
        hit_at_k,
        recall_at_k,
        mrr,
        ndcg,
        required_tools_success,
        evidence_entry_hit,
        search_call_count,
        latency_ms_total,
        latency_ms_average,
        judge_score: trace.judge_score,
        judge_error: trace.judge_error.clone(),
    }
}

pub(crate) fn summarize(cases: &[CaseReport]) -> SummaryMetrics {
    if cases.is_empty() {
        return SummaryMetrics {
            hit_at_k: 0.0,
            recall_at_k: 0.0,
            mrr: 0.0,
            ndcg: 0.0,
            required_tools_success: 0.0,
            evidence_entry_hit: 0.0,
            search_call_count: 0.0,
            latency_ms_total: 0.0,
            latency_ms_average: 0.0,
            judge_score: None,
        };
    }

    let n = cases.len() as f64;
    let judge_scores: Vec<f64> = cases
        .iter()
        .filter_map(|case| case.metrics.judge_score)
        .collect();
    SummaryMetrics {
        hit_at_k: cases.iter().map(|c| c.metrics.hit_at_k).sum::<f64>() / n,
        recall_at_k: cases.iter().map(|c| c.metrics.recall_at_k).sum::<f64>() / n,
        mrr: cases.iter().map(|c| c.metrics.mrr).sum::<f64>() / n,
        ndcg: cases.iter().map(|c| c.metrics.ndcg).sum::<f64>() / n,
        required_tools_success: cases
            .iter()
            .filter(|c| c.metrics.required_tools_success)
            .count() as f64
            / n,
        evidence_entry_hit: cases
            .iter()
            .filter(|c| c.metrics.evidence_entry_hit)
            .count() as f64
            / n,
        search_call_count: cases
            .iter()
            .map(|c| c.metrics.search_call_count as f64)
            .sum::<f64>(),
        latency_ms_total: cases
            .iter()
            .map(|c| c.metrics.latency_ms_total as f64)
            .sum(),
        latency_ms_average: cases
            .iter()
            .map(|c| c.metrics.latency_ms_average as f64)
            .sum::<f64>()
            / n,
        judge_score: if judge_scores.is_empty() {
            None
        } else {
            Some(judge_scores.iter().sum::<f64>() / judge_scores.len() as f64)
        },
    }
}

fn is_search_method(method: &str) -> bool {
    matches!(method, "search.semantic" | "search.fulltext")
}

fn is_evidence_method(method: &str) -> bool {
    matches!(method, "entry.get" | "block.get")
}

fn relevant_keys(
    result: &RetrievedResult,
    relevant_entries: &HashSet<String>,
    relevant_blocks: &HashSet<String>,
) -> Vec<String> {
    let mut keys = Vec::with_capacity(2);
    if let Some(entry_id) = &result.entry_id
        && relevant_entries.contains(entry_id)
    {
        keys.push(entry_id.clone());
    }
    if let Some(block_id) = &result.block_id
        && relevant_blocks.contains(block_id)
    {
        keys.push(block_id.clone());
    }
    keys
}

fn ndcg_binary(
    ranked: &[&RetrievedResult],
    k: usize,
    relevant_entries: &HashSet<String>,
    relevant_blocks: &HashSet<String>,
) -> f64 {
    let dcg: f64 = ranked
        .iter()
        .take(k)
        .enumerate()
        .filter(|(_, result)| !relevant_keys(result, relevant_entries, relevant_blocks).is_empty())
        .map(|(rank, _)| 1.0 / (rank as f64 + 2.0).log2())
        .sum();
    let relevant_count = relevant_entries.len() + relevant_blocks.len();
    let ideal_count = relevant_count.min(k);
    let idcg: f64 = (0..ideal_count)
        .map(|rank| 1.0 / (rank as f64 + 2.0).log2())
        .sum();
    if idcg == 0.0 { 0.0 } else { dcg / idcg }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::benchmark::cases::{
        AnswerSpec, CaseSpec, FixtureBlockSpec, FixtureEntrySpec, RetrievalSpec,
    };

    fn id(value: u128) -> Ulid {
        Ulid::from(value)
    }

    fn case_with_two_relevant_ids() -> (CaseSpec, ResolvedFixtureIds) {
        let first = id(1);
        let second = id(2);
        (
            CaseSpec {
                id: "case".into(),
                question: "question".into(),
                fixtures: vec![FixtureEntrySpec {
                    id: first,
                    title: "entry".into(),
                    tags: vec![],
                    attrs: serde_json::json!({}),
                    blocks: vec![],
                }],
                retrieval: RetrievalSpec {
                    required_tools: vec!["search.fulltext".into(), "entry.get".into()],
                    relevant_entry_ids: vec![first, second],
                    relevant_block_ids: vec![],
                    k: 3,
                },
                answer: AnswerSpec {
                    reference: "answer".into(),
                    judge: false,
                },
            },
            ResolvedFixtureIds {
                entries: HashMap::from([(first, id(11)), (second, id(22))]),
                blocks: HashMap::new(),
            },
        )
    }

    #[test]
    fn scores_ranked_retrieval_and_evidence() {
        let (case, resolved) = case_with_two_relevant_ids();
        let trace = CaseTrace {
            tool_traces: vec![
                ToolTrace {
                    method: "search.fulltext".into(),
                    ok: true,
                    latency_ms: 10,
                    results: vec![
                        RetrievedResult {
                            entry_id: Some(id(99).to_string()),
                            ..Default::default()
                        },
                        RetrievedResult {
                            entry_id: Some(id(11).to_string()),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                ToolTrace {
                    method: "entry.get".into(),
                    ok: true,
                    latency_ms: 5,
                    results: vec![RetrievedResult {
                        entry_id: Some(id(11).to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let metrics = score_case(&case, &trace, &resolved);
        assert_eq!(metrics.hit_at_k, 1.0);
        assert_eq!(metrics.recall_at_k, 0.5);
        assert_eq!(metrics.mrr, 0.5);
        let expected_ndcg = (1.0 / 3.0_f64.log2()) / (1.0 + 1.0 / 3.0_f64.log2());
        assert!((metrics.ndcg - expected_ndcg).abs() < 1e-9);
        assert!(metrics.required_tools_success);
        assert!(metrics.evidence_entry_hit);
        assert_eq!(metrics.search_call_count, 1);
        assert_eq!(metrics.latency_ms_total, 15);
    }

    #[test]
    fn failed_required_tool_and_empty_results_score_as_failures() {
        let (case, resolved) = case_with_two_relevant_ids();
        let trace = CaseTrace {
            tool_traces: vec![ToolTrace {
                method: "search.fulltext".into(),
                ok: false,
                error: Some("failed".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let metrics = score_case(&case, &trace, &resolved);
        assert_eq!(metrics.hit_at_k, 0.0);
        assert_eq!(metrics.recall_at_k, 0.0);
        assert!(!metrics.required_tools_success);
        assert!(!metrics.evidence_entry_hit);
    }

    #[test]
    fn a_search_result_can_hit_relevant_entry_and_block_ids() {
        let entry = id(1);
        let block = id(2);
        let case = CaseSpec {
            id: "case".into(),
            question: "question".into(),
            fixtures: vec![FixtureEntrySpec {
                id: entry,
                title: "entry".into(),
                tags: vec![],
                attrs: serde_json::json!({}),
                blocks: vec![FixtureBlockSpec {
                    id: block,
                    block_type: "note".into(),
                    text: "text".into(),
                    attrs: serde_json::json!({}),
                }],
            }],
            retrieval: RetrievalSpec {
                required_tools: vec!["search.fulltext".into()],
                relevant_entry_ids: vec![entry],
                relevant_block_ids: vec![block],
                k: 5,
            },
            answer: AnswerSpec {
                reference: "answer".into(),
                judge: false,
            },
        };
        let resolved = ResolvedFixtureIds {
            entries: HashMap::from([(entry, id(11))]),
            blocks: HashMap::from([(block, id(22))]),
        };
        let trace = CaseTrace {
            tool_traces: vec![ToolTrace {
                method: "search.fulltext".into(),
                ok: true,
                results: vec![RetrievedResult {
                    entry_id: Some(id(11).to_string()),
                    block_id: Some(id(22).to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let metrics = score_case(&case, &trace, &resolved);
        assert_eq!(metrics.hit_at_k, 1.0);
        assert_eq!(metrics.recall_at_k, 1.0);
        assert!(metrics.ndcg > 0.0);
    }

    #[test]
    fn summarizes_case_metrics_as_numeric_baseline_values() {
        let metrics = CaseMetrics {
            hit_at_k: 1.0,
            recall_at_k: 0.5,
            mrr: 0.5,
            ndcg: 0.75,
            required_tools_success: true,
            evidence_entry_hit: false,
            search_call_count: 2,
            latency_ms_total: 30,
            latency_ms_average: 15,
            judge_score: Some(0.8),
            judge_error: None,
        };
        let summary = summarize(&[CaseReport {
            case_id: "case".into(),
            metrics,
        }]);
        assert_eq!(summary.required_tools_success, 1.0);
        assert_eq!(summary.evidence_entry_hit, 0.0);
        assert_eq!(summary.search_call_count, 2.0);
        assert_eq!(summary.judge_score, Some(0.8));
    }
}
