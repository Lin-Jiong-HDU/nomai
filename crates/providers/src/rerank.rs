//! Reranking types and built-in implementations.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::ProviderError;
use crate::traits::Reranker;

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
}
