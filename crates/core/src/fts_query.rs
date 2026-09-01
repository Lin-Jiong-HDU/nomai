//! FTS5 query literalization.
//!
//! User queries are bound straight into `fts_* MATCH ?`, and FTS5 parses
//! that string as a query expression: `word:` is a column filter, `-word`
//! is NOT, and bare AND/OR/NOT/NEAR are operators. An unescaped user query
//! like "agent: memory" therefore fails with `no such column: agent` — a
//! storage-layer error — instead of searching. `escape` rewrites a query
//! into a sequence of quoted literal terms so everything the user typed is
//! matched as text:
//!
//! - Non-alphanumeric characters become token separators, mirroring how the
//!   trigram tokenizer split the indexed text, so "agent: memory" and
//!   "node.js" match the same rows their space-separated forms would.
//! - Each resulting token is wrapped in double quotes, neutralizing FTS5
//!   operator words ("AND", "*", "-", "(" ...) without changing how plain
//!   terms match.
//! - A query that reduces to no tokens (e.g. ":::") yields an empty string;
//!   callers skip the MATCH entirely and return no rows.

pub(crate) fn escape(query: &str) -> String {
    let normalized: String = query
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { ' ' })
        .collect();
    normalized
        .split_whitespace()
        .map(|token| format!("\"{token}\""))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::escape;

    #[test]
    fn quotes_plain_tokens() {
        assert_eq!(escape("agent memory"), "\"agent\" \"memory\"");
    }

    #[test]
    fn punctuation_becomes_separator() {
        assert_eq!(escape("agent: memory"), "\"agent\" \"memory\"");
        assert_eq!(escape("rust -language"), "\"rust\" \"language\"");
        // Mirror the trigram tokenizer: underscores split tokens on the
        // indexing side too, so "rrf_fuse" is indexed as "rrf" "fuse".
        assert_eq!(escape("rrf_fuse"), "\"rrf\" \"fuse\"");
    }

    #[test]
    fn cjk_survives() {
        assert_eq!(escape("记忆原语"), "\"记忆原语\"");
    }

    #[test]
    fn punctuation_only_reduces_to_empty() {
        assert_eq!(escape(":::"), "");
        assert_eq!(escape(""), "");
    }
}
