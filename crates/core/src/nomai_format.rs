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
    let tags: Vec<String> = Vec::new();
    let attrs = JsonMap::new();
    let source: Option<String> = None;
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
            _ => {
                // Task 4 will fill in tags / source / attrs fallback.
                // For now, skip silently so the minimal test passes.
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
/// the quotes and process `\"`. Tasks 4 expands this; v1 minimal version
/// handles only bare tokens.
fn unescape_value(s: &str) -> Result<String, String> {
    Ok(s.to_string())
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
}
