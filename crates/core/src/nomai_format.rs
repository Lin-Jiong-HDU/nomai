//! `.nomai` text format parser and renderer (Spec 6 §4).
//!
//! A `.nomai` file has two sections:
//! 1. Header (metadata): `#key value` lines
//! 2. Body (typed blocks): `@type attrs\n\nbody` blocks
//!
//! See `docs/superpowers/specs/2026-06-22-content-storage-design.md` §4 for
//! the format specification.

use chrono::{DateTime, Utc};
use serde_json::Map as JsonMap;
use thiserror::Error;
use ulid::Ulid;

/// A parsed `.nomai` document.
#[derive(Debug, Clone, PartialEq)]
pub struct NomaiDoc {
    pub format_version: u32,
    pub id: Ulid,
    pub title: String,
    pub tags: Vec<String>,
    pub attrs: JsonMap<String, serde_json::Value>,
    pub source: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub blocks: Vec<Block>,
}

/// One typed block in a `.nomai` document's body.
#[derive(Debug, Clone, PartialEq)]
pub struct Block {
    pub r#type: BlockType,
    pub text: String,
    pub attrs: JsonMap<String, serde_json::Value>,
}

/// Closed set of block types. Unknown strings parse to `ParseError::UnknownBlockType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockType {
    Claim,
    Evidence,
    Question,
    Source,
    Note,
    Connection,
}

impl BlockType {
    pub fn as_str(&self) -> &'static str {
        match self {
            BlockType::Claim => "claim",
            BlockType::Evidence => "evidence",
            BlockType::Question => "question",
            BlockType::Source => "source",
            BlockType::Note => "note",
            BlockType::Connection => "connection",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<BlockType> {
        match s {
            "claim" => Some(BlockType::Claim),
            "evidence" => Some(BlockType::Evidence),
            "question" => Some(BlockType::Question),
            "source" => Some(BlockType::Source),
            "note" => Some(BlockType::Note),
            "connection" => Some(BlockType::Connection),
            _ => None,
        }
    }
}

/// Parser error. `line` is 1-based; 0 means "before any line was read"
/// (e.g. empty input).
#[derive(Error, Debug, PartialEq)]
pub enum ParseError {
    #[error("line {line}: missing required header key: {key}")]
    MissingHeader { line: usize, key: &'static str },

    #[error("line {line}: unsupported format_version: {version} (only 1 supported)")]
    UnsupportedVersion { line: usize, version: String },

    #[error("line {line}: unknown block type: {ty}")]
    UnknownBlockType { line: usize, ty: String },

    #[error("line {line}: @connection missing required attr: {attr}")]
    MissingConnectionAttr { line: usize, attr: &'static str },

    #[error("line {line}: invalid value for {field}: {reason}")]
    InvalidValue {
        line: usize,
        field: &'static str,
        reason: String,
    },

    #[error("line {line}: syntax error: {reason}")]
    Syntax { line: usize, reason: String },

    #[error("empty input")]
    EmptyInput,
}

/// Parse a `.nomai` document.
pub fn parse(input: &str) -> Result<NomaiDoc, ParseError> {
    if input.is_empty() {
        return Err(ParseError::EmptyInput);
    }

    let mut format_version: Option<u32> = None;
    let mut id: Option<Ulid> = None;
    let mut title: Option<String> = None;
    let mut tags: Vec<String> = Vec::new();
    let mut attrs = JsonMap::new();
    let mut source: Option<String> = None;
    let mut created_at: Option<DateTime<Utc>> = None;
    let mut updated_at: Option<DateTime<Utc>> = None;

    let mut lines = input.lines().enumerate().peekable();

    // Phase 1: header
    while let Some((idx, raw)) = lines.peek().copied() {
        let line_no = idx + 1;
        if raw.is_empty() {
            lines.next();
            continue;
        }
        if raw.starts_with('@') {
            break;
        }
        if !raw.starts_with('#') {
            return Err(ParseError::Syntax {
                line: line_no,
                reason: format!("expected `#key value` header or `@type` block; got: {raw:?}"),
            });
        }
        lines.next();

        let rest = &raw[1..];
        let (key, value) = split_header_kv(rest).ok_or_else(|| ParseError::Syntax {
            line: line_no,
            reason: format!("expected `#key value`; got: {rest:?}"),
        })?;

        match key {
            "format_version" => {
                let v: u32 = value.parse().map_err(|_| ParseError::InvalidValue {
                    line: line_no,
                    field: "format_version",
                    reason: format!("not a u32: {value:?}"),
                })?;
                format_version = Some(v);
            }
            "id" => {
                let parsed: Ulid =
                    value
                        .parse()
                        .map_err(|e: ulid::DecodeError| ParseError::InvalidValue {
                            line: line_no,
                            field: "id",
                            reason: e.to_string(),
                        })?;
                id = Some(parsed);
            }
            "title" => {
                title = Some(unescape_value(value).map_err(|e| ParseError::InvalidValue {
                    line: line_no,
                    field: "title",
                    reason: e,
                })?);
            }
            "created_at" => {
                let t: DateTime<Utc> =
                    value
                        .parse()
                        .map_err(|e: chrono::ParseError| ParseError::InvalidValue {
                            line: line_no,
                            field: "created_at",
                            reason: e.to_string(),
                        })?;
                created_at = Some(t);
            }
            "updated_at" => {
                let t: DateTime<Utc> =
                    value
                        .parse()
                        .map_err(|e: chrono::ParseError| ParseError::InvalidValue {
                            line: line_no,
                            field: "updated_at",
                            reason: e.to_string(),
                        })?;
                updated_at = Some(t);
            }
            "tags" => {
                tags = parse_tags(value).map_err(|reason| ParseError::InvalidValue {
                    line: line_no,
                    field: "tags",
                    reason,
                })?;
            }
            "source" => {
                source = Some(unescape_value(value).map_err(|e| ParseError::InvalidValue {
                    line: line_no,
                    field: "source",
                    reason: e,
                })?);
            }
            _ => {
                let val = unescape_value(value).map_err(|e| ParseError::InvalidValue {
                    line: line_no,
                    field: "attrs",
                    reason: e,
                })?;
                attrs.insert(key.to_string(), serde_json::Value::String(val));
            }
        }
    }

    let format_version = format_version.ok_or(ParseError::MissingHeader {
        line: 0,
        key: "format_version",
    })?;
    if format_version != 1 {
        return Err(ParseError::UnsupportedVersion {
            line: 0,
            version: format_version.to_string(),
        });
    }
    let id = id.ok_or(ParseError::MissingHeader { line: 0, key: "id" })?;
    let title = title.ok_or(ParseError::MissingHeader {
        line: 0,
        key: "title",
    })?;
    let created_at = created_at.ok_or(ParseError::MissingHeader {
        line: 0,
        key: "created_at",
    })?;
    let updated_at = updated_at.ok_or(ParseError::MissingHeader {
        line: 0,
        key: "updated_at",
    })?;

    // Phase 2: blocks
    let mut blocks: Vec<Block> = Vec::new();
    while let Some((idx, raw)) = lines.peek().copied() {
        let line_no = idx + 1;
        if raw.is_empty() {
            lines.next();
            continue;
        }
        if !raw.starts_with('@') {
            return Err(ParseError::Syntax {
                line: line_no,
                reason: format!("expected `@type` block header; got: {raw:?}"),
            });
        }
        lines.next();

        let header_rest = &raw[1..];
        let (ty_str, attr_str) = split_block_header(header_rest);
        let block_type =
            BlockType::from_str(ty_str).ok_or_else(|| ParseError::UnknownBlockType {
                line: line_no,
                ty: ty_str.to_string(),
            })?;
        let attrs = parse_block_attrs(attr_str, line_no)?;

        if block_type == BlockType::Connection {
            if !attrs.contains_key("target") {
                return Err(ParseError::MissingConnectionAttr {
                    line: line_no,
                    attr: "target",
                });
            }
            if !attrs.contains_key("relation") {
                return Err(ParseError::MissingConnectionAttr {
                    line: line_no,
                    attr: "relation",
                });
            }
        }

        // Collect body lines until next @type or EOF.
        let mut body_lines: Vec<String> = Vec::new();
        while let Some((_, body_raw)) = lines.peek().copied() {
            if body_raw.starts_with('@') && !body_raw.starts_with("\\@") {
                break;
            }
            lines.next();
            let line_to_push = if body_raw.starts_with("\\@") {
                &body_raw[1..] // drop the leading backslash
            } else {
                body_raw
            };
            body_lines.push(line_to_push.to_string());
        }

        // Strip trailing blank-line separators (Spec 6 §4.1): the blank
        // line(s) before the next @type or EOF are block separators and are
        // not part of the body. Internal paragraph breaks are preserved.
        while body_lines.last().map(|s| s.is_empty()).unwrap_or(false) {
            body_lines.pop();
        }

        let text = if body_lines.is_empty() {
            String::new()
        } else {
            let mut s = body_lines.join("\n");
            s.push('\n');
            s
        };

        blocks.push(Block {
            r#type: block_type,
            text,
            attrs,
        });
    }

    Ok(NomaiDoc {
        format_version,
        id,
        title,
        tags,
        attrs,
        source,
        created_at,
        updated_at,
        blocks,
    })
}

/// Split a block header line (after the leading `@`) into `(type, attrs_str)`.
/// e.g. "evidence src=paper.pdf#L42 strength=strong" -> ("evidence", "src=paper.pdf#L42 strength=strong").
/// e.g. "claim" -> ("claim", "").
fn split_block_header(s: &str) -> (&str, &str) {
    match s.find(char::is_whitespace) {
        Some(idx) => (&s[..idx], s[idx..].trim_start()),
        None => (s, ""),
    }
}

/// Parse space-separated `key=value` pairs from a block header's attr string.
/// A value may be a bare token or a `"..."`-quoted string (which may contain
/// whitespace). Same value-escaping rules (`\"`) as header values, handled by
/// `unescape_value`.
fn parse_block_attrs(
    s: &str,
    line_no: usize,
) -> Result<JsonMap<String, serde_json::Value>, ParseError> {
    let mut out = JsonMap::new();
    let s = s.trim();
    if s.is_empty() {
        return Ok(out);
    }
    for pair in split_attr_tokens(s) {
        let eq = pair.find('=').ok_or_else(|| ParseError::Syntax {
            line: line_no,
            reason: format!("block attr missing `=`: {pair:?}"),
        })?;
        let key = &pair[..eq];
        let value = &pair[eq + 1..];
        let val = unescape_value(value).map_err(|e| ParseError::InvalidValue {
            line: line_no,
            field: "block_attr",
            reason: e,
        })?;
        out.insert(key.to_string(), serde_json::Value::String(val));
    }
    Ok(out)
}

/// Split a block header's attr string into `key=value` tokens, honoring
/// `"..."`-quoted segments (which may contain whitespace) and `\"` escapes
/// within them. Outside quotes, whitespace separates tokens.
fn split_attr_tokens(s: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quote {
            current.push(c);
            if c == '\\' {
                if let Some(&next) = chars.peek() {
                    current.push(next);
                    chars.next();
                }
            } else if c == '"' {
                in_quote = false;
            }
            continue;
        }
        match c {
            '"' => {
                current.push(c);
                in_quote = true;
            }
            w if w.is_whitespace() => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            other => current.push(other),
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

/// Split "key value" or "key \"quoted value\"" — returns (key, value).
/// Returns None if the line has no key.
fn split_header_kv(s: &str) -> Option<(&str, &str)> {
    let mut iter = s.splitn(2, char::is_whitespace);
    let key = iter.next()?;
    let rest = iter.next()?.trim_start_matches(' ');
    Some((key, rest))
}

/// Unescape a header value. Bare tokens pass through; quoted values unwrap
/// the quotes and process `\"`.
fn unescape_value(s: &str) -> Result<String, String> {
    let s = s.trim();
    if !s.starts_with('"') {
        return Ok(s.to_string());
    }
    if !s.ends_with('"') || s.len() < 2 {
        return Err(format!("unterminated quoted value: {s:?}"));
    }
    let inner = &s[1..s.len() - 1];
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('"') => out.push('"'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    Ok(out)
}

/// Parse comma-separated tags, honoring `"..."` for tags containing commas.
fn parse_tags(s: &str) -> Result<Vec<String>, String> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }
    let mut tags = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quote {
            if c == '"' {
                in_quote = false;
            } else if c == '\\' {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            } else {
                current.push(c);
            }
            continue;
        }
        match c {
            '"' => {
                // A quote starting a tag segment discards leading whitespace
                // accumulated before it (e.g. `a, "b" -> ["a", "b"]`).
                if current.trim().is_empty() {
                    current.clear();
                }
                in_quote = true;
            }
            ',' => {
                tags.push(std::mem::take(&mut current));
            }
            other => current.push(other),
        }
    }
    if in_quote {
        return Err(format!("unterminated quoted tag in: {s:?}"));
    }
    tags.push(current);
    Ok(tags)
}

/// Render a `NomaiDoc` to `.nomai` text. Infallible for well-formed docs.
///
/// Round-trip property: `parse(&render(doc)).unwrap() == doc` for any
/// well-formed `doc` (i.e. one that was itself produced by `parse`).
pub fn render(doc: &NomaiDoc) -> String {
    let mut out = String::new();
    out.push_str(&format!("#format_version {}\n", doc.format_version));
    out.push_str(&format!("#id {}\n", doc.id));
    out.push_str(&format!("#title {}\n", escape_header_value(&doc.title)));

    if !doc.tags.is_empty() {
        out.push_str("#tags ");
        out.push_str(&render_tags(&doc.tags));
        out.push('\n');
    }
    for (key, value) in &doc.attrs {
        out.push_str(&format!("#{} {}\n", key, render_attr_value(value)));
    }
    if let Some(source) = &doc.source {
        out.push_str(&format!("#source {}\n", escape_header_value(source)));
    }
    out.push_str(&format!("#created_at {}\n", doc.created_at.to_rfc3339()));
    out.push_str(&format!("#updated_at {}\n", doc.updated_at.to_rfc3339()));

    for block in &doc.blocks {
        out.push('\n');
        out.push('@');
        out.push_str(block.r#type.as_str());
        for (key, value) in &block.attrs {
            out.push(' ');
            out.push_str(key);
            out.push('=');
            out.push_str(&render_attr_value(value));
        }
        out.push('\n');
        out.push_str(&render_block_body(&block.text));
    }

    out
}

/// Escape a header value. Bare tokens (no spaces, quotes, or commas) pass
/// through unchanged; anything else is wrapped in `"..."` with `"` escaped
/// as `\"`. The empty string is also quoted to distinguish it from a missing
/// value on round-trip.
fn escape_header_value(s: &str) -> String {
    let needs_quote = s.is_empty() || s.contains(' ') || s.contains('"') || s.contains(',');
    if !needs_quote {
        return s.to_string();
    }
    let escaped = s.replace('"', "\\\"");
    format!("\"{escaped}\"")
}

/// Render a JSON value as a `.nomai` attribute value. Non-string scalars
/// (numbers, bools) are stringified via serde_json's `Display`; strings use
/// the same escaping as header values.
fn render_attr_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => escape_header_value(s),
        other => escape_header_value(&other.to_string()),
    }
}

/// Render a tag list as a comma-separated `#tags` value. Plain tags (no
/// comma or quote) pass through; tags containing either get wrapped in
/// `"..."` with `"` escaped as `\"`.
fn render_tags(tags: &[String]) -> String {
    let parts: Vec<String> = tags
        .iter()
        .map(|t| {
            if t.contains(',') || t.contains('"') {
                let escaped = t.replace('"', "\\\"");
                format!("\"{escaped}\"")
            } else {
                t.to_string()
            }
        })
        .collect();
    parts.join(",")
}

/// Render a block body to `.nomai` text. Each body line that begins with
/// `@` is escaped as `\@` so the parser doesn't mistake it for a new block
/// header. The trailing newline (always present in a parsed body) is
/// preserved; empty bodies render as the empty string.
fn render_block_body(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        let line_no_newline = line.strip_suffix('\n').unwrap_or(line);
        if line_no_newline.starts_with('@') {
            out.push('\\');
        }
        out.push_str(line_no_newline);
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_type_round_trip() {
        for s in [
            "claim",
            "evidence",
            "question",
            "source",
            "note",
            "connection",
        ] {
            let ty = BlockType::from_str(s).unwrap();
            assert_eq!(ty.as_str(), s);
        }
    }

    #[test]
    fn block_type_unknown_returns_none() {
        assert!(BlockType::from_str("definition").is_none());
    }

    #[test]
    fn parse_minimal_valid_header() {
        let input = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z
";
        let doc = parse(input).unwrap();
        assert_eq!(doc.format_version, 1);
        assert_eq!(doc.id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert_eq!(doc.title, "Hello");
        assert!(doc.tags.is_empty());
        assert!(doc.attrs.is_empty());
        assert_eq!(doc.source, None);
        assert!(doc.blocks.is_empty());
        assert_eq!(
            doc.created_at,
            "2026-06-23T10:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn parse_empty_input_errors() {
        assert_eq!(parse("").unwrap_err(), ParseError::EmptyInput);
    }

    #[test]
    fn parse_missing_format_version_errors() {
        let input = "\
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z
";
        assert_eq!(
            parse(input).unwrap_err(),
            ParseError::MissingHeader {
                line: 0,
                key: "format_version"
            }
        );
    }

    #[test]
    fn parse_unsupported_version_errors() {
        let input = "\
#format_version 2
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z
";
        assert_eq!(
            parse(input).unwrap_err(),
            ParseError::UnsupportedVersion {
                line: 0,
                version: "2".into()
            }
        );
    }

    #[test]
    fn parse_missing_id_errors_with_line_zero() {
        let input = "\
#format_version 1
#title Hello
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z
";
        assert_eq!(
            parse(input).unwrap_err(),
            ParseError::MissingHeader { line: 0, key: "id" }
        );
    }

    #[test]
    fn parse_invalid_ulid_errors() {
        let input = "\
#format_version 1
#id NOT-A-ULID
#title Hello
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z
";
        match parse(input).unwrap_err() {
            ParseError::InvalidValue { line, field, .. } => {
                assert_eq!(line, 2);
                assert_eq!(field, "id");
            }
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn parse_tags_and_source() {
        let input = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#tags astronomy, astronomy-history
#source https://example.com/research
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z
";
        let doc = parse(input).unwrap();
        assert_eq!(doc.tags, vec!["astronomy", " astronomy-history"]);
        assert_eq!(doc.source.as_deref(), Some("https://example.com/research"));
    }

    #[test]
    fn parse_unknown_keys_go_to_attrs() {
        let input = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#author John Doe
#year 2024
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z
";
        let doc = parse(input).unwrap();
        assert_eq!(doc.attrs.len(), 2);
        assert_eq!(
            doc.attrs["author"],
            serde_json::Value::String("John Doe".into())
        );
        assert_eq!(doc.attrs["year"], serde_json::Value::String("2024".into()));
    }

    #[test]
    fn parse_quoted_header_value() {
        let input = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title \"Hello World\"
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z
";
        let doc = parse(input).unwrap();
        assert_eq!(doc.title, "Hello World");
    }

    #[test]
    fn parse_quoted_value_with_escaped_quote() {
        let input = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title \"He said \\\"hi\\\"\"
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z
";
        let doc = parse(input).unwrap();
        assert_eq!(doc.title, "He said \"hi\"");
    }

    #[test]
    fn parse_tags_with_quoted_comma() {
        let input = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#tags astronomy, \"science, history\", math
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z
";
        let doc = parse(input).unwrap();
        assert_eq!(doc.tags, vec!["astronomy", "science, history", " math"]);
    }

    #[test]
    fn parse_single_block_no_attrs() {
        let input = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z

@claim
Earth orbits the sun.
";
        let doc = parse(input).unwrap();
        assert_eq!(doc.blocks.len(), 1);
        let block = &doc.blocks[0];
        assert_eq!(block.r#type, BlockType::Claim);
        assert!(block.attrs.is_empty());
        assert_eq!(block.text, "Earth orbits the sun.\n");
    }

    #[test]
    fn parse_multiple_blocks() {
        let input = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z

@claim
First claim.

@evidence src=paper.pdf#L42
Evidence text.

@question
Why?
";
        let doc = parse(input).unwrap();
        assert_eq!(doc.blocks.len(), 3);
        assert_eq!(doc.blocks[0].r#type, BlockType::Claim);
        assert_eq!(doc.blocks[0].text, "First claim.\n");
        assert_eq!(doc.blocks[1].r#type, BlockType::Evidence);
        assert_eq!(
            doc.blocks[1].attrs["src"],
            serde_json::Value::String("paper.pdf#L42".into())
        );
        assert_eq!(doc.blocks[1].text, "Evidence text.\n");
        assert_eq!(doc.blocks[2].r#type, BlockType::Question);
    }

    #[test]
    fn parse_block_with_multiple_attrs() {
        let input = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z

@evidence src=paper.pdf strength=strong
Text.
";
        let doc = parse(input).unwrap();
        let attrs = &doc.blocks[0].attrs;
        assert_eq!(attrs.len(), 2);
        assert_eq!(attrs["src"], serde_json::Value::String("paper.pdf".into()));
        assert_eq!(
            attrs["strength"],
            serde_json::Value::String("strong".into())
        );
    }

    #[test]
    fn parse_block_with_empty_body() {
        let input = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z

@connection target=01HXZ...geocentrism relation=refutes
";
        let doc = parse(input).unwrap();
        let block = &doc.blocks[0];
        assert_eq!(block.r#type, BlockType::Connection);
        assert_eq!(block.text, "");
        assert_eq!(
            block.attrs["target"],
            serde_json::Value::String("01HXZ...geocentrism".into())
        );
        assert_eq!(
            block.attrs["relation"],
            serde_json::Value::String("refutes".into())
        );
    }

    #[test]
    fn parse_unknown_block_type_errors() {
        let input = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z

@definition
Some text.
";
        assert_eq!(
            parse(input).unwrap_err(),
            ParseError::UnknownBlockType {
                line: 7,
                ty: "definition".into()
            }
        );
    }

    #[test]
    fn parse_connection_missing_target_errors() {
        let input = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z

@connection relation=refutes
";
        assert_eq!(
            parse(input).unwrap_err(),
            ParseError::MissingConnectionAttr {
                line: 7,
                attr: "target"
            }
        );
    }

    #[test]
    fn parse_connection_missing_relation_errors() {
        let input = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z

@connection target=01HXZ...geo
";
        assert_eq!(
            parse(input).unwrap_err(),
            ParseError::MissingConnectionAttr {
                line: 7,
                attr: "relation"
            }
        );
    }

    #[test]
    fn parse_body_with_escaped_at() {
        let input = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z

@note
\\@claim should be literal.
";
        let doc = parse(input).unwrap();
        assert_eq!(doc.blocks[0].text, "@claim should be literal.\n");
    }

    #[test]
    fn parse_body_with_multiple_paragraphs() {
        let input = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z

@note
First paragraph.

Second paragraph.

Third.
";
        let doc = parse(input).unwrap();
        assert_eq!(
            doc.blocks[0].text,
            "First paragraph.\n\nSecond paragraph.\n\nThird.\n"
        );
    }

    #[test]
    fn render_minimal_doc() {
        let doc = NomaiDoc {
            format_version: 1,
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            title: "Hello".into(),
            tags: vec![],
            attrs: JsonMap::new(),
            source: None,
            created_at: "2026-06-23T10:00:00Z".parse().unwrap(),
            updated_at: "2026-06-23T10:00:00Z".parse().unwrap(),
            blocks: vec![],
        };
        let rendered = render(&doc);
        assert!(rendered.contains("#format_version 1"));
        assert!(rendered.contains("#id 01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(rendered.contains("#title Hello"));
    }

    #[test]
    fn round_trip_minimal() {
        let original = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Hello
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T10:00:00Z

@claim
Earth orbits the sun.
";
        let doc = parse(original).unwrap();
        let rendered = render(&doc);
        let reparsed = parse(&rendered).unwrap();
        assert_eq!(doc, reparsed);
    }

    #[test]
    fn round_trip_rich_doc() {
        let original = "\
#format_version 1
#id 01ARZ3NDEKTSV4RRFFQ69G5FAV
#title Heliocentrism
#tags astronomy, astronomy-history
#source https://example.com/research
#created_at 2026-06-23T10:00:00Z
#updated_at 2026-06-23T11:30:00Z

@claim confidence=high
Earth orbits the sun.

@evidence src=paper.pdf#L42 strength=strong
Kepler's laws show ellipses.

@question status=open
Why?

@connection target=01HXZ...geo relation=refutes
";
        let doc = parse(original).unwrap();
        let rendered = render(&doc);
        let reparsed = parse(&rendered).unwrap();
        assert_eq!(doc, reparsed);
    }

    #[test]
    fn render_escapes_at_at_line_start_in_body() {
        let doc = NomaiDoc {
            format_version: 1,
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            title: "t".into(),
            tags: vec![],
            attrs: JsonMap::new(),
            source: None,
            created_at: "2026-06-23T10:00:00Z".parse().unwrap(),
            updated_at: "2026-06-23T10:00:00Z".parse().unwrap(),
            blocks: vec![Block {
                r#type: BlockType::Note,
                text: "@claim should be literal.\n".into(),
                attrs: JsonMap::new(),
            }],
        };
        let rendered = render(&doc);
        assert!(rendered.contains("\\@claim should be literal."));
    }

    #[test]
    fn render_timestamps_use_rfc3339() {
        let doc = NomaiDoc {
            format_version: 1,
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap(),
            title: "t".into(),
            tags: vec![],
            attrs: JsonMap::new(),
            source: None,
            created_at: "2026-06-23T10:00:00Z".parse().unwrap(),
            updated_at: "2026-06-23T11:30:00Z".parse().unwrap(),
            blocks: vec![],
        };
        let rendered = render(&doc);
        assert!(
            rendered.contains("#created_at 2026-06-23T10:00:00"),
            "created_at must be RFC3339, got: {rendered}"
        );
        assert!(
            rendered.contains("#updated_at 2026-06-23T11:30:00"),
            "updated_at must be RFC3339, got: {rendered}"
        );
        // And round-trip still works
        let reparsed = parse(&rendered).unwrap();
        assert_eq!(doc, reparsed);
    }

    #[test]
    fn parse_spec_example_4_4_verbatim() {
        let input = "\
#format_version 1
#id 01HXY8K2P3M4N5Q6R7S8T9V0WX
#title Heliocentrism
#tags astronomy, astronomy-history
#created_at 2026-06-22T10:00:00Z
#updated_at 2026-06-22T11:30:00Z
#source https://example.com/research
#author \"John Doe\"

@claim confidence=high
Earth orbits the sun, not the other way around.

@evidence src=paper.pdf#L42 strength=strong
Kepler's laws show that planetary orbits are ellipses
with the sun at one focus. This is inconsistent with
geocentric models.

@evidence src=https://nasa.gov/orbits
NASA observations confirm Kepler's predictions to
within measurement error.

@question status=open
Why did Ptolemy's geocentric model persist for 1400 years?

@source src=principia.pdf author=\"Isaac Newton\" year=1687
Newton's Principia unified Kepler's laws with mechanics.

@connection target=01HXY...geocentrism relation=refutes

@note
This entry was created for the astronomy seminar.
";
        let doc = parse(input).unwrap();
        assert_eq!(doc.format_version, 1);
        assert_eq!(doc.id.to_string(), "01HXY8K2P3M4N5Q6R7S8T9V0WX");
        assert_eq!(doc.title, "Heliocentrism");
        assert_eq!(doc.tags, vec!["astronomy", " astronomy-history"]);
        assert_eq!(doc.source.as_deref(), Some("https://example.com/research"));
        assert_eq!(
            doc.attrs["author"],
            serde_json::Value::String("John Doe".into())
        );

        assert_eq!(doc.blocks.len(), 7);
        assert_eq!(doc.blocks[0].r#type, BlockType::Claim);
        assert_eq!(
            doc.blocks[0].attrs["confidence"],
            serde_json::Value::String("high".into())
        );
        assert_eq!(doc.blocks[2].r#type, BlockType::Evidence);
        assert_eq!(
            doc.blocks[2].attrs["src"],
            serde_json::Value::String("https://nasa.gov/orbits".into())
        );
        assert_eq!(doc.blocks[5].r#type, BlockType::Connection);
        assert_eq!(
            doc.blocks[5].attrs["target"],
            serde_json::Value::String("01HXY...geocentrism".into())
        );
        assert_eq!(
            doc.blocks[5].attrs["relation"],
            serde_json::Value::String("refutes".into())
        );
        assert_eq!(doc.blocks[5].text, "");
    }

    #[test]
    fn round_trip_spec_example_4_4() {
        let input = "\
#format_version 1
#id 01HXY8K2P3M4N5Q6R7S8T9V0WX
#title Heliocentrism
#tags astronomy, astronomy-history
#created_at 2026-06-22T10:00:00Z
#updated_at 2026-06-22T11:30:00Z
#source https://example.com/research
#author \"John Doe\"

@claim confidence=high
Earth orbits the sun.

@connection target=01HXY...geo relation=refutes
";
        let doc = parse(input).unwrap();
        let reparsed = parse(&render(&doc)).unwrap();
        assert_eq!(doc, reparsed);
    }
}
