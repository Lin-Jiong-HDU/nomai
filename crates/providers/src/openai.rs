//! OpenAI-compatible provider implementations. Populated by Tasks 3 and 4.
//!
//! Until then, the following names exist only so that [`crate`] can re-export
//! them in advance; they are intentionally unimplemented stubs.

use crate::traits::{EmbeddingProvider, LlmProvider};

/// Stub; concrete impl lands in Task 3.
pub type OpenAiCompatibleEmbed = dyn EmbeddingProvider;

/// Stub; concrete impl lands in Task 4.
pub type OpenAiCompatibleLlm = dyn LlmProvider;
