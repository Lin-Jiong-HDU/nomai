//! Shared types for LLM chat completions.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageRole {
    System,
    User,
    Assistant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: MessageRole,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    pub messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionResponse {
    pub content: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_role_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&MessageRole::Assistant).unwrap(),
            r#""assistant""#
        );
    }

    #[test]
    fn completion_request_roundtrips_with_optional_fields_skipped() {
        let req = CompletionRequest {
            system: None,
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: "hi".into(),
            }],
            max_tokens: None,
            temperature: None,
        };
        let s = serde_json::to_string(&req).unwrap();
        assert!(!s.contains(r#""system""#));
        assert!(!s.contains(r#""max_tokens""#));
        assert!(!s.contains(r#""temperature""#));
        assert!(s.contains(r#""messages""#));
    }
}
