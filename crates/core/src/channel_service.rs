//! ChannelService: a named append-only message log with server-side
//! per-subscriber read cursors.
//!
//! Channels are SQLite-only — no FS persistence, no events, no embedding.
//! Unlike `events.list`'s client-side cursor, read positions are held
//! server-side because the consumers are LLM agents whose context does not
//! survive a session boundary.

use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use ulid::Ulid;

use crate::channel_model::{
    CHANNEL_LIMIT_MAX, ChannelMessage, RecvMessages, RecvResult, SendMessage, validate_attrs,
    validate_name,
};
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

    // ── Recv ────────────────────────────────────────────────────────

    /// Read messages. Three mutually-exclusive modes:
    ///
    /// | `subscriber` | `since` | behaviour                                  |
    /// |---|---|---|
    /// | given | — | read from the persistent cursor, then advance it      |
    /// | — | given | pure history read; writes no cursor                 |
    /// | — | — | return the last `limit` messages (ascending)           |
    pub fn recv(&self, p: RecvMessages) -> Result<RecvResult, CoreError> {
        let channel = validate_name("channel", &p.channel)?;
        if p.subscriber.is_some() && p.since.is_some() {
            return Err(CoreError::Validation(
                "subscriber and since are mutually exclusive".into(),
            ));
        }
        if p.limit == 0 || p.limit > CHANNEL_LIMIT_MAX {
            return Err(CoreError::Validation(format!(
                "limit must be 1..={CHANNEL_LIMIT_MAX}"
            )));
        }
        if let Some(since) = p.since
            && since < 0
        {
            return Err(CoreError::Validation("since must be >= 0".into()));
        }
        let subscriber = match p.subscriber {
            Some(ref s) => Some(validate_name("subscriber", s)?),
            None => None,
        };

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let (items, cursor) = match (&subscriber, p.since) {
            (Some(sub), None) => {
                let from = read_cursor(&tx, &channel, sub)?;
                let items = read_after(&tx, &channel, from, p.limit)?;
                let cursor = match items.last() {
                    Some(last) => {
                        write_cursor(&tx, &channel, sub, last.seq)?;
                        last.seq
                    }
                    None => from,
                };
                (items, cursor)
            }
            (None, Some(since)) => {
                let items = read_after(&tx, &channel, since, p.limit)?;
                let cursor = items.last().map(|m| m.seq).unwrap_or(0);
                (items, cursor)
            }
            (None, None) => {
                let mut items = read_tail(&tx, &channel, p.limit)?;
                items.reverse();
                let cursor = items.last().map(|m| m.seq).unwrap_or(0);
                (items, cursor)
            }
            // Rejected above; kept exhaustive without a panic path.
            (Some(_), Some(_)) => (Vec::new(), 0),
        };

        let latest = latest_seq(&tx, &channel)?;
        tx.commit()?;
        Ok(RecvResult {
            items,
            latest,
            cursor,
        })
    }
}

/// `seq` values greater than `from`, ascending, capped at `limit`.
fn read_after(
    conn: &Connection,
    channel: &str,
    from: i64,
    limit: u32,
) -> Result<Vec<ChannelMessage>, CoreError> {
    let mut stmt = conn.prepare(
        "SELECT seq, id, channel, sender, text, attrs, created_at
         FROM channel_messages
         WHERE channel = ?1 AND seq > ?2
         ORDER BY seq ASC
         LIMIT ?3",
    )?;
    let rows = stmt.query_map(params![channel, from, limit], row_to_message)?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// The newest `limit` messages, in DESCENDING order (caller reverses).
fn read_tail(
    conn: &Connection,
    channel: &str,
    limit: u32,
) -> Result<Vec<ChannelMessage>, CoreError> {
    let mut stmt = conn.prepare(
        "SELECT seq, id, channel, sender, text, attrs, created_at
         FROM channel_messages
         WHERE channel = ?1
         ORDER BY seq DESC
         LIMIT ?2",
    )?;
    let rows = stmt.query_map(params![channel, limit], row_to_message)?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// Highest `seq` in the channel, or 0 when it has no messages.
fn latest_seq(conn: &Connection, channel: &str) -> Result<i64, CoreError> {
    let v: Option<i64> = conn.query_row(
        "SELECT MAX(seq) FROM channel_messages WHERE channel = ?1",
        params![channel],
        |r| r.get(0),
    )?;
    Ok(v.unwrap_or(0))
}

/// The subscriber's stored position, or 0 if it has never read.
fn read_cursor(conn: &Connection, channel: &str, subscriber: &str) -> Result<i64, CoreError> {
    let v: Option<i64> = conn
        .query_row(
            "SELECT last_seq FROM channel_cursors WHERE channel = ?1 AND subscriber = ?2",
            params![channel, subscriber],
            |r| r.get(0),
        )
        .optional()?;
    Ok(v.unwrap_or(0))
}

/// Advance the subscriber's position. Only ever moves forward.
fn write_cursor(
    conn: &Connection,
    channel: &str,
    subscriber: &str,
    last_seq: i64,
) -> Result<(), CoreError> {
    conn.execute(
        "INSERT INTO channel_cursors (channel, subscriber, last_seq, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(channel, subscriber) DO UPDATE SET
             last_seq = excluded.last_seq,
             updated_at = excluded.updated_at",
        params![channel, subscriber, last_seq, Utc::now().to_rfc3339()],
    )?;
    Ok(())
}

/// Map a `channel_messages` row. Column order must match the SELECT lists.
fn row_to_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<ChannelMessage> {
    let seq: i64 = row.get(0)?;
    let id_str: String = row.get(1)?;
    let channel: String = row.get(2)?;
    let sender: String = row.get(3)?;
    let text: String = row.get(4)?;
    let attrs_json: String = row.get(5)?;
    let created_str: String = row.get(6)?;

    Ok(ChannelMessage {
        seq,
        id: storage::from_text(1, &id_str, Ulid::from_string)?,
        channel,
        sender,
        text,
        attrs: storage::from_text(5, &attrs_json, |s| serde_json::from_str(s))?,
        created_at: storage::from_text(6, &created_str, |s| {
            chrono::DateTime::parse_from_rfc3339(s).map(|d| d.with_timezone(&Utc))
        })?,
    })
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

    fn recv(
        s: &ChannelService,
        channel: &str,
        subscriber: Option<&str>,
        since: Option<i64>,
        limit: u32,
    ) -> RecvResult {
        s.recv(RecvMessages {
            channel: channel.into(),
            subscriber: subscriber.map(Into::into),
            since,
            limit,
        })
        .unwrap()
    }

    #[test]
    fn subscriber_cursor_advances_and_is_not_re_read() {
        let s = svc();
        send(&s, "c", "one");
        send(&s, "c", "two");

        let first = recv(&s, "c", Some("b"), None, 50);
        assert_eq!(first.items.len(), 2);
        assert_eq!(first.items[0].text, "one");
        assert_eq!(first.items[1].text, "two");
        assert_eq!(first.latest, 2);
        assert_eq!(first.cursor, 2);

        // Nothing new → empty, cursor unchanged.
        let second = recv(&s, "c", Some("b"), None, 50);
        assert!(second.items.is_empty());
        assert_eq!(second.cursor, 2);
        assert_eq!(second.latest, 2);

        // New message → only the new one.
        send(&s, "c", "three");
        let third = recv(&s, "c", Some("b"), None, 50);
        assert_eq!(third.items.len(), 1);
        assert_eq!(third.items[0].text, "three");
    }

    #[test]
    fn two_subscribers_have_independent_cursors() {
        let s = svc();
        send(&s, "c", "one");
        send(&s, "c", "two");

        let a = recv(&s, "c", Some("a"), None, 50);
        assert_eq!(a.items.len(), 2);
        // b has never read → still sees everything.
        let b = recv(&s, "c", Some("b"), None, 50);
        assert_eq!(b.items.len(), 2);
        // a is now caught up and unaffected by b's read.
        assert!(recv(&s, "c", Some("a"), None, 50).items.is_empty());
    }

    #[test]
    fn since_does_not_write_a_cursor() {
        let s = svc();
        send(&s, "c", "one");
        send(&s, "c", "two");

        let r = recv(&s, "c", None, Some(0), 50);
        assert_eq!(r.items.len(), 2);
        assert_eq!(r.cursor, 2);

        // A later subscriber read still starts from the beginning.
        let r2 = recv(&s, "c", Some("late"), None, 50);
        assert_eq!(r2.items.len(), 2);
    }

    #[test]
    fn no_cursor_reads_the_tail() {
        let s = svc();
        for i in 1..=5 {
            send(&s, "c", &format!("m{i}"));
        }
        let r = recv(&s, "c", None, None, 2);
        assert_eq!(r.items.len(), 2);
        assert_eq!(r.items[0].text, "m4", "tail must still be ascending");
        assert_eq!(r.items[1].text, "m5");
        assert_eq!(r.latest, 5);
        assert_eq!(r.cursor, 5);
    }

    #[test]
    fn empty_result_does_not_write_a_cursor_row() {
        let s = svc();
        let r = recv(&s, "empty", Some("b"), None, 50);
        assert!(r.items.is_empty());
        assert_eq!(r.latest, 0);
        assert_eq!(r.cursor, 0);

        let conn = s.conn.lock().unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM channel_cursors", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0, "an empty read must not create cursor state");
    }

    /// Review Focus #4: a client cursor ahead of the server (stale or
    /// hand-edited) must return empty without erroring, and must not
    /// clobber the subscriber's existing position.
    #[test]
    fn since_beyond_latest_is_empty_and_harmless() {
        let s = svc();
        send(&s, "c", "one");
        recv(&s, "c", Some("b"), None, 50); // cursor at 1

        let ahead = recv(&s, "c", None, Some(9999), 50);
        assert!(ahead.items.is_empty());
        assert_eq!(ahead.latest, 1);

        // The subscriber cursor is untouched, so a later send is delivered.
        send(&s, "c", "two");
        let after = recv(&s, "c", Some("b"), None, 50);
        assert_eq!(after.items.len(), 1);
        assert_eq!(after.items[0].text, "two");
    }

    /// Review Focus #5: channel names are NOT normalized. Pins the
    /// behaviour so a typo'd channel is a known, documented footgun
    /// rather than a surprise.
    #[test]
    fn channel_names_are_not_normalized() {
        let s = svc();
        send(&s, "handoff", "lower");
        send(&s, "Handoff", "upper");
        send(&s, "handoff ", "trailing-space");

        assert_eq!(recv(&s, "handoff", None, Some(0), 50).items.len(), 1);
        assert_eq!(recv(&s, "Handoff", None, Some(0), 50).items.len(), 1);
        assert_eq!(recv(&s, "handoff ", None, Some(0), 50).items.len(), 1);
    }

    #[test]
    fn recv_rejects_conflicting_and_out_of_range_params() {
        let s = svc();
        assert!(matches!(
            s.recv(RecvMessages {
                channel: "c".into(),
                subscriber: Some("b".into()),
                since: Some(1),
                limit: 50,
            }),
            Err(CoreError::Validation(_))
        ));
        assert!(matches!(
            s.recv(RecvMessages {
                channel: "c".into(),
                subscriber: None,
                since: None,
                limit: 0,
            }),
            Err(CoreError::Validation(_))
        ));
        assert!(matches!(
            s.recv(RecvMessages {
                channel: "c".into(),
                subscriber: None,
                since: None,
                limit: CHANNEL_LIMIT_MAX + 1,
            }),
            Err(CoreError::Validation(_))
        ));
        assert!(matches!(
            s.recv(RecvMessages {
                channel: "c".into(),
                subscriber: None,
                since: Some(-1),
                limit: 50,
            }),
            Err(CoreError::Validation(_))
        ));
    }

    #[test]
    fn limit_boundaries_are_inclusive() {
        let s = svc();
        for i in 1..=CHANNEL_LIMIT_MAX {
            send(&s, "c", &format!("m{i}"));
        }
        let r = recv(&s, "c", None, Some(0), CHANNEL_LIMIT_MAX);
        assert_eq!(r.items.len(), CHANNEL_LIMIT_MAX as usize);
    }
}
