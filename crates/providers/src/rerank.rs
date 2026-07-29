//! Reranking types and built-in implementations.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::ProviderError;
use crate::traits::{LlmProvider, Reranker};
use crate::types::{ChatMessage, CompletionRequest, MessageRole};

/// A candidate document for reranking.
///
/// `id` is an opaque caller-defined identifier (ULID, URL, etc.).
/// `content` is the text to score against the query.
/// `score` is the original retrieval score (preserved in output for
/// tie-breaking / fusion by the caller).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RerankCandidate {
    pub id: String,
    pub content: String,
    pub score: f32,
}

/// A reranked candidate with updated score.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RerankedCandidate {
    pub id: String,
    pub content: String,
    /// The original score passed in by the caller.
    pub original_score: f32,
    /// The reranker-assigned relevance score (0.0–1.0, higher = more relevant).
    pub rerank_score: f32,
    /// Optional human-readable reason for the score.
    pub reason: Option<String>,
}

/// A no-op reranker that returns candidates unchanged.
///
/// `rerank_score` is set equal to `original_score` and the input order
/// is preserved. This is the default when no `[reranking]` config is
/// present.
pub struct NoopReranker;

#[async_trait]
impl Reranker for NoopReranker {
    async fn rerank(
        &self,
        _query: &str,
        candidates: &[RerankCandidate],
        top_n: usize,
    ) -> Result<Vec<RerankedCandidate>, ProviderError> {
        let mut out: Vec<RerankedCandidate> = candidates
            .iter()
            .take(top_n)
            .map(|c| RerankedCandidate {
                id: c.id.clone(),
                content: c.content.clone(),
                original_score: c.score,
                rerank_score: c.score,
                reason: None,
            })
            .collect();
        out.truncate(top_n);
        Ok(out)
    }

    fn name(&self) -> &str {
        "noop"
    }
}

/// LLM-based reranker that prompts a language model to score document
/// relevance on a 1-5 scale, normalized to 0-1.
///
/// Candidates exceeding `max_candidates` are truncated by original score
/// before the LLM call to bound token consumption. On any LLM failure,
/// falls back to returning candidates in their original order with
/// `rerank_score = original_score` — the caller never gets an error
/// from reranking.
pub struct LLMReranker {
    llm: Arc<dyn LlmProvider>,
    model: String,
    max_candidates: usize,
}

impl LLMReranker {
    pub fn new(llm: Arc<dyn LlmProvider>, model: String) -> Self {
        Self {
            llm,
            model,
            max_candidates: 20,
        }
    }

    /// Override the default max_candidates (20).
    pub fn with_max_candidates(mut self, n: usize) -> Self {
        self.max_candidates = n;
        self
    }
}

#[async_trait]
impl Reranker for LLMReranker {
    async fn rerank(
        &self,
        query: &str,
        candidates: &[RerankCandidate],
        top_n: usize,
    ) -> Result<Vec<RerankedCandidate>, ProviderError> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        // Truncate to max_candidates by original score (descending).
        let mut truncated: Vec<&RerankCandidate> = candidates.iter().collect();
        if truncated.len() > self.max_candidates {
            truncated.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            truncated.truncate(self.max_candidates);
        }

        // Build prompt.
        let docs_text: String = truncated
            .iter()
            .enumerate()
            .map(|(i, c)| format!("[{}] {}\n", i + 1, c.content))
            .collect::<Vec<_>>()
            .join("\n");

        let prompt = format!(
            "You are a relevance scoring assistant. Given a query and a list of documents, \
             rate each document's relevance to the query on a scale of 1-5 (1=irrelevant, \
             5=directly answers).\n\n\
             Query: {query}\n\n\
             Documents:\n{docs_text}\n\
             Return a JSON array of ratings:\n\
             [{{\"doc_id\": <number>, \"relevance\": <1-5>, \"reason\": \"<brief>\"}}]"
        );

        // Call LLM.
        let req = CompletionRequest {
            system: Some(
                "You are a precise document relevance judge. Always return valid JSON.".into(),
            ),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: prompt,
            }],
            max_tokens: Some(512),
            temperature: Some(0.0),
        };

        let response = match self.llm.complete(req).await {
            Ok(r) => r.content,
            Err(_) => {
                // Fallback: preserve original order and scores.
                let mut out: Vec<RerankedCandidate> = candidates
                    .iter()
                    .map(|c| RerankedCandidate {
                        id: c.id.clone(),
                        content: c.content.clone(),
                        original_score: c.score,
                        rerank_score: c.score,
                        reason: Some("LLM reranker unavailable; original order preserved".into()),
                    })
                    .collect();
                out.truncate(top_n);
                return Ok(out);
            }
        };

        // Parse JSON array of ratings.
        let ratings: Vec<serde_json::Value> = match serde_json::from_str(&response) {
            Ok(v) => v,
            Err(_) => {
                // Malformed LLM response -> fallback.
                let mut out: Vec<RerankedCandidate> = candidates
                    .iter()
                    .map(|c| RerankedCandidate {
                        id: c.id.clone(),
                        content: c.content.clone(),
                        original_score: c.score,
                        rerank_score: c.score,
                        reason: Some("LLM response unparseable; original order preserved".into()),
                    })
                    .collect();
                out.truncate(top_n);
                return Ok(out);
            }
        };

        // Build reranked output from LLM ratings.
        let mut reranked: Vec<RerankedCandidate> = ratings
            .iter()
            .filter_map(|r| {
                let doc_id = r["doc_id"].as_u64()? as usize;
                let relevance = r["relevance"].as_u64()? as f32;
                let reason = r["reason"].as_str().map(String::from);
                let candidate = truncated.get(doc_id - 1)?;
                Some(RerankedCandidate {
                    id: candidate.id.clone(),
                    content: candidate.content.clone(),
                    original_score: candidate.score,
                    rerank_score: (relevance - 1.0) / 4.0, // normalize 1-5 -> 0-1
                    reason,
                })
            })
            .collect();

        // Fill in any candidates that weren't rated by the LLM.
        for c in &truncated {
            if !reranked.iter().any(|r| r.id == c.id) {
                reranked.push(RerankedCandidate {
                    id: c.id.clone(),
                    content: c.content.clone(),
                    original_score: c.score,
                    rerank_score: c.score,
                    reason: Some("Not rated by LLM; original score preserved".into()),
                });
            }
        }

        // Sort by rerank_score descending.
        reranked.sort_by(|a, b| {
            b.rerank_score
                .partial_cmp(&a.rerank_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        reranked.truncate(top_n);
        Ok(reranked)
    }

    fn name(&self) -> &str {
        &self.model
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_reranker_preserves_order_and_score() {
        let reranker = NoopReranker;
        let candidates = vec![
            RerankCandidate {
                id: "a".into(),
                content: "text a".into(),
                score: 0.9,
            },
            RerankCandidate {
                id: "b".into(),
                content: "text b".into(),
                score: 0.5,
            },
        ];
        let result = reranker.rerank("query", &candidates, 10).await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, "a");
        assert_eq!(result[0].original_score, 0.9);
        assert_eq!(result[0].rerank_score, 0.9);
        assert_eq!(result[1].id, "b");
        assert!(result[0].reason.is_none());
    }

    #[tokio::test]
    async fn noop_reranker_respects_top_n() {
        let reranker = NoopReranker;
        let candidates: Vec<RerankCandidate> = (0..10)
            .map(|i| RerankCandidate {
                id: format!("id_{i}"),
                content: format!("text {i}"),
                score: (10 - i) as f32,
            })
            .collect();
        let result = reranker.rerank("q", &candidates, 3).await.unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn rerank_candidate_roundtrips_json() {
        let c = RerankCandidate {
            id: "x".into(),
            content: "hello".into(),
            score: 0.75,
        };
        let json = serde_json::to_string(&c).unwrap();
        let back: RerankCandidate = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "x");
        assert_eq!(back.score, 0.75);
    }

    // --- LLMReranker tests ---

    #[tokio::test]
    async fn llm_reranker_falls_back_on_provider_error() {
        use crate::error::ProviderErrorKind;
        use crate::traits::LlmProvider;
        use crate::types::{CompletionRequest, CompletionResponse};

        struct FailingLlm;
        #[async_trait]
        impl LlmProvider for FailingLlm {
            async fn complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<CompletionResponse, crate::error::ProviderError> {
                Err(crate::error::ProviderError::new(
                    ProviderErrorKind::Server,
                    "boom",
                    None,
                ))
            }
            fn name(&self) -> &str {
                "failing"
            }
        }

        let llm = Arc::new(FailingLlm);
        let reranker = LLMReranker::new(llm, "test-model".into());
        let candidates = vec![RerankCandidate {
            id: "a".into(),
            content: "text".into(),
            score: 0.9,
        }];
        // On failure, fallback: rerank_score = original_score, original order.
        let result = reranker.rerank("query", &candidates, 10).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].rerank_score, 0.9);
        assert_eq!(result[0].original_score, 0.9);
    }

    #[tokio::test]
    async fn llm_reranker_normalizes_scores_zero_to_one() {
        use crate::types::CompletionResponse;

        struct MockLlm {
            response: String,
        }
        #[async_trait]
        impl crate::traits::LlmProvider for MockLlm {
            async fn complete(
                &self,
                _req: crate::types::CompletionRequest,
            ) -> Result<CompletionResponse, crate::error::ProviderError> {
                Ok(CompletionResponse {
                    content: self.response.clone(),
                })
            }
            fn name(&self) -> &str {
                "mock"
            }
        }

        // LLM returns relevance ratings 1-5.
        let llm = Arc::new(MockLlm {
            response: r#"[{"doc_id":1,"relevance":5,"reason":"perfect"},{"doc_id":2,"relevance":3,"reason":"ok"}]"#.into(),
        });
        let reranker = LLMReranker::new(llm, "mock".into());
        let candidates = vec![
            RerankCandidate {
                id: "a".into(),
                content: "A".into(),
                score: 0.8,
            },
            RerankCandidate {
                id: "b".into(),
                content: "B".into(),
                score: 0.6,
            },
        ];
        let result = reranker.rerank("q", &candidates, 10).await.unwrap();
        assert_eq!(result.len(), 2);
        // relevance=5 -> (5-1)/4 = 1.0
        assert!((result[0].rerank_score - 1.0).abs() < 0.01);
        assert_eq!(result[0].reason.as_deref(), Some("perfect"));
        // relevance=3 -> (3-1)/4 = 0.5
        assert!((result[1].rerank_score - 0.5).abs() < 0.01);
    }

    #[tokio::test]
    async fn llm_reranker_respects_max_candidates() {
        use std::sync::atomic::{AtomicU32, Ordering};

        struct CountingLlm {
            calls: AtomicU32,
        }
        #[async_trait]
        impl crate::traits::LlmProvider for CountingLlm {
            async fn complete(
                &self,
                _req: crate::types::CompletionRequest,
            ) -> Result<crate::types::CompletionResponse, crate::error::ProviderError> {
                self.calls.fetch_add(1, Ordering::Relaxed);
                Ok(crate::types::CompletionResponse {
                    content: r#"[{"doc_id":1,"relevance":3,"reason":"ok"}]"#.into(),
                })
            }
            fn name(&self) -> &str {
                "counting"
            }
        }

        let llm = Arc::new(CountingLlm {
            calls: AtomicU32::new(0),
        });
        let reranker = LLMReranker::new(llm.clone(), "test".into()).with_max_candidates(2);

        // 10 candidates, max_candidates=2 -> only 2 sent to LLM.
        let candidates: Vec<RerankCandidate> = (0..10)
            .map(|i| RerankCandidate {
                id: format!("id_{i}"),
                content: format!("text {i}"),
                score: (10 - i) as f32,
            })
            .collect();
        let result = reranker.rerank("q", &candidates, 5).await.unwrap();
        // LLM called exactly once.
        assert_eq!(llm.calls.load(Ordering::Relaxed), 1);
        // Result limited by top_n (5), not max_candidates.
        assert!(result.len() <= 5);
    }
}
