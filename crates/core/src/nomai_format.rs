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
    let blocks: Vec<Block> = Vec::new();

    for (idx, raw_line) in input.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw_line;

        if line.is_empty() {
            continue;
        }
        if line.starts_with('@') {
            // Body starts here; not handled in this task — leave blocks empty.
            // (Task 5+ extends this loop to consume the body.)
            break;
        }
        if !line.starts_with('#') {
            return Err(ParseError::Syntax {
                line: line_no,
                reason: format!(
                    "expected `#key value` header line or `@type` block; got: {line:?}"
                ),
            });
        }

        let rest = &line[1..];
        let (key, value) = split_header_kv(rest).ok_or_else(|| ParseError::Syntax {
            line: line_no,
            reason: format!("expected `#key value`; got: {rest:?}"),
        })?;

        match key.as_str() {
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
                title = Some(
                    unescape_value(&value).map_err(|e| ParseError::InvalidValue {
                        line: line_no,
                        field: "title",
                        reason: e,
                    })?,
                );
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
                tags = parse_tags(&value).map_err(|reason| ParseError::InvalidValue {
                    line: line_no,
                    field: "tags",
                    reason,
                })?;
            }
            "source" => {
                source = Some(
                    unescape_value(&value).map_err(|e| ParseError::InvalidValue {
                        line: line_no,
                        field: "source",
                        reason: e,
                    })?,
                );
            }
            _ => {
                let val = unescape_value(&value).map_err(|e| ParseError::InvalidValue {
                    line: line_no,
                    field: "attrs",
                    reason: e,
                })?;
                attrs.insert(key, serde_json::Value::String(val));
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

/// Split "key value" or "key \"quoted value\"" — returns (key, value).
/// Returns None if the line has no key.
fn split_header_kv(s: &str) -> Option<(String, String)> {
    let mut iter = s.splitn(2, char::is_whitespace);
    let key = iter.next()?.to_string();
    let rest = iter.next()?.trim_start_matches(' ');
    Some((key, rest.to_string()))
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

/// Render a `NomaiDoc` back to `.nomai` text. Infallible for well-formed docs.
pub fn render(_doc: &NomaiDoc) -> String {
    unimplemented!("filled in by Task 8")
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
}
