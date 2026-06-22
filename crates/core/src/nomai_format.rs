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

/// Parse a `.nomai` document. See module docs for the format.
pub fn parse(_input: &str) -> Result<NomaiDoc, ParseError> {
    unimplemented!("filled in by Task 2")
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
        for s in ["claim", "evidence", "question", "source", "note", "connection"] {
            let ty = BlockType::from_str(s).unwrap();
            assert_eq!(ty.as_str(), s);
        }
    }

    #[test]
    fn block_type_unknown_returns_none() {
        assert!(BlockType::from_str("definition").is_none());
    }
}
