//! channel.* handlers — append-only message log + server-side cursors.
//!
//! All four follow the zero-sized-struct + `RpcHandler` pattern. SQLite
//! calls go through `tokio::task::spawn_blocking` via the `blocking` helper.
//!
//! None of them is `is_mutating()`: messages live only in SQLite and never
//! touch the `knowledge_root` work tree, so they must NOT take `sync_lock`.
//! The trait contract (`crate::rpc::RpcHandler::is_mutating`) scopes that
//! flag to file-tree mutations — `index.sync` returns `false` for the same
//! reason.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use nomai_core::{CoreError, RecvMessages, SendMessage};

use crate::daemon::Daemon;
use crate::rpc::RpcHandler;

use super::entry::blocking;

// ── Send ────────────────────────────────────────────────────────────

pub struct Send;
#[async_trait]
impl RpcHandler for Send {
    fn method(&self) -> &'static str {
        "channel.send"
    }
    /// Explicit even though the trait default is `false`: `send` looks like
    /// a write, and mis-marking it would serialize every message against
    /// `sync.run`'s git rebase for no benefit.
    fn is_mutating(&self) -> bool {
        false
    }
    fn description(&self) -> &'static str {
        "Append a message to a named channel. Channels are append-only logs shared by every agent on this machine; there is no sender identity or routing, so the channel name is the only address. Returns the created message with its server-assigned seq."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "channel": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 128,
                    "description": "Channel name. Free-form; use a convention like \"handoff\" or \"notice\"."
                },
                "text": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Message body (markdown). Keep it short — put substance in an entry and reference it via attrs.refs."
                },
                "sender": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 128,
                    "description": "Optional self-declared sender label. Purely descriptive; it does not route anything."
                },
                "attrs": {
                    "type": "object",
                    "description": "Free-form JSON. Convention: {\"kind\": \"handoff\", \"refs\": [\"<entry ulid>\"]}."
                }
            },
            "required": ["channel", "text"],
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: SendMessage = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
        let svc = daemon.channels.clone();
        let result = blocking(move || svc.send(p)).await??;
        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

// ── Recv ────────────────────────────────────────────────────────────

pub struct Recv;
#[async_trait]
impl RpcHandler for Recv {
    fn method(&self) -> &'static str {
        "channel.recv"
    }
    fn description(&self) -> &'static str {
        "Read messages from a channel. Pass subscriber to read from (and advance) a server-side cursor — use this so a new session does not re-read everything. Pass since for a one-off history read that writes no cursor. Pass neither to get the most recent messages. Returns items ascending, plus latest (channel max seq) and cursor (position after this read)."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "channel": {"type": "string", "minLength": 1, "maxLength": 128},
                "subscriber": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 128,
                    "description": "Stable name for this reader. The daemon remembers its position across sessions. Mutually exclusive with since."
                },
                "since": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Exclusive lower bound on seq. Pure read: writes no cursor. Mutually exclusive with subscriber."
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 200,
                    "default": 50
                }
            },
            "required": ["channel"],
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let p: RecvMessages = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
        let svc = daemon.channels.clone();
        let result = blocking(move || svc.recv(p)).await??;
        serde_json::to_value(&result).map_err(|e| CoreError::Config(format!("serialize: {e}")))
    }
}

// ── List ────────────────────────────────────────────────────────────

pub struct List;
#[async_trait]
impl RpcHandler for List {
    fn method(&self) -> &'static str {
        "channel.list"
    }
    fn description(&self) -> &'static str {
        "List every channel that has at least one message, with its message count, highest seq, and last message time. Newest activity first. Use this to discover which channel names are actually in use."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(crate::handlers::params::empty_param_schema())
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let _: Value = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
        let svc = daemon.channels.clone();
        let items = blocking(move || svc.list()).await??;
        Ok(json!({ "items": items }))
    }
}

// ── Purge ───────────────────────────────────────────────────────────

pub struct Purge;
#[async_trait]
impl RpcHandler for Purge {
    fn method(&self) -> &'static str {
        "channel.purge"
    }
    fn description(&self) -> &'static str {
        "Permanently delete messages with seq below before_seq from one channel. Returns {deleted: N}. Subscriber cursors are left alone. before_seq is required and must be positive — there is no delete-everything shortcut."
    }
    fn input_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "properties": {
                "channel": {"type": "string", "minLength": 1, "maxLength": 128},
                "before_seq": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Delete messages with seq strictly less than this."
                }
            },
            "required": ["channel", "before_seq"],
            "additionalProperties": false
        }))
    }
    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        #[derive(Deserialize)]
        struct Params {
            channel: String,
            before_seq: i64,
        }
        let p: Params = serde_json::from_value(params)
            .map_err(|e| CoreError::Validation(format!("invalid params: {e}")))?;
        let svc = daemon.channels.clone();
        let deleted = blocking(move || svc.purge(&p.channel, p.before_seq)).await??;
        Ok(json!({ "deleted": deleted }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_handlers_are_never_mutating() {
        // Messages live only in SQLite; taking sync_lock here would serialize
        // every message against sync.run's git rebase for no benefit.
        assert!(!Send.is_mutating());
        assert!(!Recv.is_mutating());
        assert!(!List.is_mutating());
        assert!(!Purge.is_mutating());
    }

    #[test]
    fn channel_handlers_declare_methods_and_schemas() {
        assert_eq!(Send.method(), "channel.send");
        assert_eq!(Recv.method(), "channel.recv");
        assert_eq!(List.method(), "channel.list");
        assert_eq!(Purge.method(), "channel.purge");
        for h in [&Send as &dyn RpcHandler, &Recv, &List, &Purge] {
            assert!(!h.description().is_empty(), "{}", h.method());
            assert!(h.input_schema().is_some(), "{}", h.method());
        }
    }
}
