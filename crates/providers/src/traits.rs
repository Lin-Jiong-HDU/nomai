//! Embedding and LLM provider traits.

use async_trait::async_trait;

use crate::error::ProviderError;
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
    async fn complete(&self, req: CompletionRequest) -> Result<crate::types::CompletionResponse, ProviderError>;
    fn name(&self) -> &str;
}
