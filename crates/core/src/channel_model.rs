//! Channel data model: a named append-only message log plus server-side
//! per-subscriber read cursors.
//!
//! The cursor is an integer `seq` rather than the ULID `id` — see the
//! V13 migration comment for the monotonicity argument.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

use crate::error::CoreError;

/// Maximum byte length for channel / sender / subscriber names.
pub const CHANNEL_NAME_MAX: usize = 128;

/// Maximum value accepted for `recv`'s `limit`.
pub const CHANNEL_LIMIT_MAX: u32 = 200;

/// A single message in a channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelMessage {
    /// Server-assigned read cursor. Strictly increasing per channel.
    pub seq: i64,
    /// ULID identity, consistent with the rest of the store.
    pub id: Ulid,
    pub channel: String,
    /// Free-form sender label. Never used for routing — the channel name
    /// is the only address.
    pub sender: String,
    pub text: String,
    pub attrs: Value,
    pub created_at: DateTime<Utc>,
}

/// Input for `ChannelService::send`.
#[derive(Debug, Deserialize)]
pub struct SendMessage {
    pub channel: String,
    pub text: String,
    #[serde(default)]
    pub sender: Option<String>,
    #[serde(default)]
    pub attrs: Option<Value>,
}

/// Input for `ChannelService::recv`.
#[derive(Debug, Deserialize)]
pub struct RecvMessages {
    pub channel: String,
    /// Server-side cursor key. Mutually exclusive with `since`.
    #[serde(default)]
    pub subscriber: Option<String>,
    /// Explicit client-side lower bound (exclusive). Mutually exclusive
    /// with `subscriber`.
    #[serde(default)]
    pub since: Option<i64>,
    #[serde(default = "default_channel_limit")]
    pub limit: u32,
}

fn default_channel_limit() -> u32 {
    50
}

/// Result of `ChannelService::recv`.
#[derive(Debug, Serialize)]
pub struct RecvResult {
    pub items: Vec<ChannelMessage>,
    /// Highest `seq` present in the channel (0 when empty).
    pub latest: i64,
    /// Position after this read. Equals the subscriber's new cursor when
    /// `subscriber` was given, else the greatest returned `seq` (0 if none).
    pub cursor: i64,
}

/// One row of `ChannelService::list`.
#[derive(Debug, Serialize)]
pub struct ChannelSummary {
    pub channel: String,
    pub message_count: u64,
    pub last_seq: i64,
    pub last_message_at: Option<DateTime<Utc>>,
}

/// Validate a channel / sender / subscriber name.
pub(crate) fn validate_name(field: &str, value: &str) -> Result<String, CoreError> {
    if value.is_empty() || value.len() > CHANNEL_NAME_MAX {
        return Err(CoreError::Validation(format!(
            "{field} must be 1..={CHANNEL_NAME_MAX} bytes"
        )));
    }
    Ok(value.to_string())
}

/// `attrs` defaults to `{}` and must be a JSON object when provided.
pub(crate) fn validate_attrs(attrs: Option<Value>) -> Result<Value, CoreError> {
    let attrs = attrs.unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    if !attrs.is_object() {
        return Err(CoreError::Validation("attrs must be a JSON object".into()));
    }
    Ok(attrs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_name_rejects_empty_and_oversized() {
        assert!(validate_name("channel", "").is_err());
        assert!(validate_name("channel", &"a".repeat(CHANNEL_NAME_MAX)).is_ok());
        assert!(validate_name("channel", &"a".repeat(CHANNEL_NAME_MAX + 1)).is_err());
    }

    #[test]
    fn validate_name_counts_bytes_not_chars() {
        // 64 CJK chars = 192 bytes > 128 → rejected. Documented as "bytes".
        let cjk = "频".repeat(64);
        assert_eq!(cjk.chars().count(), 64);
        assert_eq!(cjk.len(), 192);
        assert!(validate_name("channel", &cjk).is_err());
    }

    #[test]
    fn validate_attrs_defaults_to_empty_object_and_rejects_scalars() {
        assert_eq!(validate_attrs(None).unwrap(), serde_json::json!({}));
        assert!(validate_attrs(Some(serde_json::json!({"k": 1}))).is_ok());
        assert!(validate_attrs(Some(serde_json::json!([1, 2]))).is_err());
        assert!(validate_attrs(Some(serde_json::json!("x"))).is_err());
    }

    #[test]
    fn recv_defaults_limit_to_50() {
        let p: RecvMessages = serde_json::from_str(r#"{"channel":"c"}"#).unwrap();
        assert_eq!(p.limit, 50);
        assert!(p.subscriber.is_none());
        assert!(p.since.is_none());
    }
}
