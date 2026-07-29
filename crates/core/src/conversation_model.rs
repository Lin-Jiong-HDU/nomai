//! Conversation data model: sessions + turns for agent dialogue storage.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

/// A conversation session — a sequence of turns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conversation {
    pub id: Ulid,
    pub title: String,
    pub tags: Vec<String>,
    pub attrs: Value,
    pub turn_count: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A single turn (message) within a conversation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Turn {
    pub id: Ulid,
    pub conversation_id: Ulid,
    pub ordinal: u32,
    pub role: String,    // "user" | "assistant" | "system" | "tool"
    pub content: String, // markdown
    pub attrs: Value,
    pub created_at: DateTime<Utc>,
}

/// Input for ConversationService::create.
#[derive(Debug, Deserialize)]
pub struct CreateConversation {
    pub title: Option<String>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub attrs: Option<Value>,
    /// Optional initial turns. When present, all turns are created
    /// within the same transaction as the conversation.
    #[serde(default)]
    pub turns: Option<Vec<CreateTurn>>,
}

/// A single turn to create (used in both create and append).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTurn {
    pub role: String,
    pub content: String,
    #[serde(default)]
    pub attrs: Option<Value>,
}

/// Input for ConversationService::append_turns.
#[derive(Debug, Deserialize)]
pub struct AppendTurns {
    pub conversation_id: Ulid,
    pub turns: Vec<CreateTurn>,
}

/// Input for ConversationService::update.
#[derive(Debug, Default, Deserialize)]
pub struct UpdateConversation {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub attrs: Option<Value>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationListOrder {
    #[default]
    CreatedDesc,
    CreatedAsc,
    UpdatedDesc,
    UpdatedAsc,
}

#[derive(Debug, Deserialize)]
pub struct ConversationListQuery {
    pub tag: Option<String>,
    #[serde(default = "default_conversation_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
    #[serde(default)]
    pub order: ConversationListOrder,
    #[serde(default)]
    pub transient: Option<bool>,
}

fn default_conversation_limit() -> u32 {
    50
}

impl Default for ConversationListQuery {
    fn default() -> Self {
        Self {
            tag: None,
            limit: default_conversation_limit(),
            offset: 0,
            order: ConversationListOrder::default(),
            transient: None,
        }
    }
}

#[derive(Debug)]
pub struct ConversationListResult {
    pub items: Vec<Conversation>,
    pub total: u64,
    pub has_more: bool,
}

/// A conversation with all its turns loaded.
#[derive(Debug, Clone, Serialize)]
pub struct ConversationWithTurns {
    #[serde(flatten)]
    pub conversation: Conversation,
    pub turns: Vec<Turn>,
}

/// A single search hit over conversations.
#[derive(Debug)]
pub struct ConversationSearchResult {
    pub conversation: Conversation,
    pub turn: Turn,
    pub snippet: String,
    pub score: f32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversation_list_order_default_is_created_desc() {
        let q = ConversationListQuery::default();
        assert_eq!(q.order, ConversationListOrder::CreatedDesc);
        assert_eq!(q.limit, 50);
    }

    #[test]
    fn conversation_list_order_deserializes_snake_case() {
        let q: ConversationListQuery = serde_json::from_str(r#"{"order":"updated_asc"}"#).unwrap();
        assert_eq!(q.order, ConversationListOrder::UpdatedAsc);
    }

    #[test]
    fn create_turn_roundtrips_json() {
        let turn = CreateTurn {
            role: "user".into(),
            content: "hello".into(),
            attrs: Some(serde_json::json!({"token_count": 5})),
        };
        let json = serde_json::to_string(&turn).unwrap();
        let back: CreateTurn = serde_json::from_str(&json).unwrap();
        assert_eq!(back.role, "user");
        assert_eq!(back.content, "hello");
    }
}
