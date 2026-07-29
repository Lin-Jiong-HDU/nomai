//! Embedding and LLM provider traits.

use async_trait::async_trait;

use crate::error::ProviderError;
use crate::rerank::{RerankCandidate, RerankedCandidate};
use crate::types::CompletionRequest;

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Embed a batch of texts. Returns one vector per input, in order.
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ProviderError>;

    /// The dimensionality of the vectors produced by `embed`.
    fn dim(&self) -> usize;

    /// A short, human-readable provider identifier (e.g. "openai-compatible").
    fn name(&self) -> &str;
}

#[async_trait]
pub trait LlmProvider: Send + Sync {
    async fn complete(
        &self,
        req: CompletionRequest,
    ) -> Result<crate::types::CompletionResponse, ProviderError>;
    fn name(&self) -> &str;
}

/// Rerank candidate documents against a query.
///
/// Implementations may use LLM-based scoring, cross-encoder models, or
/// any other relevance estimation strategy. The trait is intentionally
/// generic: `id` and `content` are owned `String`s so callers can pass
/// arbitrary identifiers (ULIDs, URLs, etc.) without coupling to nomai
/// core types.
#[async_trait]
pub trait Reranker: Send + Sync {
    /// Rerank candidates against `query`. Returns candidates sorted by
    /// `rerank_score` descending, limited to `top_n`. Input order is
    /// preserved for equal scores (stable sort).
    async fn rerank(
        &self,
        query: &str,
        candidates: &[RerankCandidate],
        top_n: usize,
    ) -> Result<Vec<RerankedCandidate>, ProviderError>;

    /// Short human-readable identifier (e.g. "llm-gpt-4o-mini", "noop").
    fn name(&self) -> &str;
}
