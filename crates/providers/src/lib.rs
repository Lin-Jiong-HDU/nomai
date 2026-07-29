//! Embedding and LLM provider traits and implementations for nomai.

pub mod cached;
pub mod error;
pub mod openai;
pub mod rerank;
pub mod traits;
pub mod types;

pub use cached::{CacheStats, CachedEmbedder, ClearOptions, ClearResult};
pub use error::{ProviderError, ProviderErrorKind};
pub use openai::{OpenAiCompatibleEmbed, OpenAiCompatibleLlm};
pub use rerank::{NoopReranker, RerankCandidate, RerankedCandidate};
pub use traits::{EmbeddingProvider, LlmProvider, Reranker};
pub use types::{ChatMessage, CompletionRequest, CompletionResponse, MessageRole};
