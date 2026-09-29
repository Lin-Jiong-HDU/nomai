//! ChannelService: a named append-only message log with server-side
//! per-subscriber read cursors.
//!
//! Channels are SQLite-only — no FS persistence, no events, no embedding.
//! Unlike `events.list`'s client-side cursor, read positions are held
//! server-side because the consumers are LLM agents whose context does not
//! survive a session boundary.

use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{Connection, params};
use ulid::Ulid;

use crate::channel_model::{ChannelMessage, SendMessage, validate_attrs, validate_name};
use crate::error::CoreError;
use crate::storage;

pub struct ChannelService {
    conn: Arc<Mutex<Connection>>,
    // No ContentStore — channels never touch knowledge_root.
}

impl ChannelService {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Result<Self, CoreError> {
        {
            let mut guard = conn.lock().unwrap();
            storage::run_migrations(&mut guard)?;
        }
        Ok(Self { conn })
    }

    #[doc(hidden)]
    pub fn for_test() -> Result<Self, CoreError> {
        crate::storage::init_sqlite_extensions();
        let conn = Arc::new(Mutex::new(Connection::open_in_memory()?));
        // Run migrations via EntryService so every table exists.
        let tmp = tempfile::tempdir()?;
        let content_store = Arc::new(crate::content_store::ContentStore::new_with_cleanup(
            tmp.path().to_path_buf(),
            tmp,
        ));
        crate::EntryService::new(conn.clone(), content_store, 1024)?;
        Self::new(conn)
    }

    // ── Send ────────────────────────────────────────────────────────

    /// Append a message. `seq` is assigned by SQLite (`AUTOINCREMENT`), so
    /// it is strictly increasing per channel and never reused.
    pub fn send(&self, p: SendMessage) -> Result<ChannelMessage, CoreError> {
        let channel = validate_name("channel", &p.channel)?;
        if p.text.is_empty() {
            return Err(CoreError::Validation(
                "message text must not be empty".into(),
            ));
        }
        let sender = match p.sender {
            Some(ref s) => validate_name("sender", s)?,
            None => String::new(),
        };
        let attrs = validate_attrs(p.attrs)?;

        let id = Ulid::new();
        let created_at = Utc::now();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO channel_messages (id, channel, sender, text, attrs, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                id.to_string(),
                channel,
                sender,
                p.text,
                attrs.to_string(),
                created_at.to_rfc3339(),
            ],
        )
        .map_err(storage::map_constraint_violation)?;

        Ok(ChannelMessage {
            seq: conn.last_insert_rowid(),
            id,
            channel,
            sender,
            text: p.text,
            attrs,
            created_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn svc() -> ChannelService {
        ChannelService::for_test().unwrap()
    }

    fn send(s: &ChannelService, channel: &str, text: &str) -> ChannelMessage {
        s.send(SendMessage {
            channel: channel.into(),
            text: text.into(),
            sender: None,
            attrs: None,
        })
        .unwrap()
    }

    /// Review Focus #1: same-millisecond sends must still get strictly
    /// increasing seq. This is the whole reason the cursor is INTEGER
    /// AUTOINCREMENT rather than a ULID.
    #[test]
    fn seq_is_strictly_increasing_across_rapid_sends() {
        let s = svc();
        let seqs: Vec<i64> = (0..100).map(|i| send(&s, "c", &format!("m{i}")).seq).collect();
        assert_eq!(seqs[0], 1);
        for w in seqs.windows(2) {
            assert!(w[1] > w[0], "seq must strictly increase: {seqs:?}");
        }
        let mut uniq = seqs.clone();
        uniq.dedup();
        assert_eq!(uniq.len(), seqs.len(), "seq must be unique");
    }

    /// Review Focus #2: attrs is free JSON and must round-trip byte-exactly,
    /// including nesting, unicode and mixed number types.
    #[test]
    fn attrs_round_trips_exactly() {
        let s = svc();
        let attrs = json!({
            "kind": "handoff",
            "refs": ["01M3NF0HSGWBF1P7RTZCTENSPG"],
            "nested": {"a": [1, 2.5, true, null], "中文": "值"},
            "count": 7
        });
        let sent = s
            .send(SendMessage {
                channel: "handoff".into(),
                text: "t".into(),
                sender: Some("agent-a".into()),
                attrs: Some(attrs.clone()),
            })
            .unwrap();
        assert_eq!(sent.attrs, attrs);

        let conn = s.conn.lock().unwrap();
        let stored: String = conn
            .query_row(
                "SELECT attrs FROM channel_messages WHERE seq = ?1",
                params![sent.seq],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&stored).unwrap(),
            attrs
        );
    }

    /// Review Focus #3: the spec sets no upper bound on `text`; an agent
    /// pasting a whole file into a handoff message is a legitimate use.
    #[test]
    fn large_text_round_trips() {
        let s = svc();
        let big = "x".repeat(1024 * 1024);
        let m = send(&s, "c", &big);
        let conn = s.conn.lock().unwrap();
        let back: String = conn
            .query_row(
                "SELECT text FROM channel_messages WHERE seq = ?1",
                params![m.seq],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(back.len(), big.len());
    }

    #[test]
    fn send_rejects_empty_channel_text_and_bad_attrs() {
        let s = svc();
        let mk = |channel: &str,
                  text: &str,
                  sender: Option<&str>,
                  attrs: Option<serde_json::Value>| SendMessage {
            channel: channel.into(),
            text: text.into(),
            sender: sender.map(Into::into),
            attrs,
        };
        assert!(matches!(
            s.send(mk("", "t", None, None)),
            Err(CoreError::Validation(_))
        ));
        assert!(matches!(
            s.send(mk("c", "", None, None)),
            Err(CoreError::Validation(_))
        ));
        assert!(matches!(
            s.send(mk("c", "t", Some(""), None)),
            Err(CoreError::Validation(_))
        ));
        assert!(matches!(
            s.send(mk("c", "t", None, Some(json!([1])))),
            Err(CoreError::Validation(_))
        ));
    }

    #[test]
    fn send_defaults_sender_to_empty_and_attrs_to_object() {
        let s = svc();
        let m = send(&s, "c", "hello");
        assert_eq!(m.sender, "");
        assert_eq!(m.attrs, json!({}));
        assert_eq!(m.channel, "c");
        assert_eq!(m.text, "hello");
    }
}
