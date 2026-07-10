//! Snippet extraction for `search.fulltext` explainability.
//! Spec: docs/superpowers/specs/2026-07-10-fulltext-explainability-design.md §5.
//!
//! `extract_snippet` builds a ~`WINDOW`-character window of `text` centered
//! on the first occurrence of `query`, wraps the hit in markdown `**...**`,
//! and adds `…` at truncated ends. Character-based (CJK-safe).

/// Snippet window size in characters (includes the hit substring).
const WINDOW: usize = 120;

/// Build a ~`WINDOW`-char snippet centered on the first occurrence of
/// `query`. See module docs. Public for `service::build_results`.
pub fn extract_snippet(text: &str, query: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let total = chars.len();

    if query.is_empty() || total == 0 {
        return chars.iter().take(WINDOW).collect();
    }

    let q: Vec<char> = query.chars().collect();
    let Some(hs) = find_first(&chars, &q) else {
        // Defensive: trigram MATCH implies substring presence; if absent,
        // return a plain truncation with no highlight.
        return chars.iter().take(WINDOW).collect();
    };
    let he = hs + q.len();

    // Padding only when the hit is shorter than the window.
    let left = WINDOW.saturating_sub(q.len()) / 2;
    let right = WINDOW.saturating_sub(q.len()).saturating_sub(left);
    let ws = hs.saturating_sub(left);
    let we = (he + right).min(total);

    let pre: String = chars[ws..hs].iter().collect();
    let hit: String = chars[hs..he].iter().collect();
    let post: String = chars[he..we].iter().collect();

    let mut out = String::new();
    // Ellipsis only in normal padding mode (left>0 / right>0); when the hit
    // itself fills the window we emit just the hit per spec §5.
    if ws > 0 && left > 0 {
        out.push('…');
    }
    out.push_str(&pre);
    out.push_str("**");
    out.push_str(&hit);
    out.push_str("**");
    out.push_str(&post);
    if we < total && right > 0 {
        out.push('…');
    }
    out
}

/// First index where `needle` appears as a contiguous sub-slice in `hay`.
fn find_first(hay: &[char], needle: &[char]) -> Option<usize> {
    if needle.is_empty() || needle.len() > hay.len() {
        return None;
    }
    (0..=hay.len() - needle.len()).find(|&i| hay[i..i + needle.len()] == *needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_highlight_wraps_hit() {
        let s = extract_snippet("hello world", "world");
        assert!(s.contains("**world**"));
    }

    #[test]
    fn ellipsis_when_text_exceeds_window() {
        let text = format!("{}target{}", "a".repeat(100), "b".repeat(100));
        let s = extract_snippet(&text, "target");
        assert!(s.starts_with('…'), "prefix ellipsis: {s}");
        assert!(s.ends_with('…'), "suffix ellipsis: {s}");
        assert!(s.contains("**target**"));
    }

    #[test]
    fn short_text_no_ellipsis() {
        let s = extract_snippet("short target text", "target");
        assert!(!s.contains('…'));
        assert!(s.contains("**target**"));
    }

    #[test]
    fn cjk_char_based_window() {
        let text = "这是一段中文目标词周围还有一些其他字符用于测试窗口截取逻辑";
        let s = extract_snippet(text, "目标词");
        assert!(s.contains("**目标词**"));
        assert!(!s.contains(char::REPLACEMENT_CHARACTER), "no broken chars: {s}");
    }

    #[test]
    fn first_occurrence_only_highlighted() {
        let s = extract_snippet("cat cat cat", "cat");
        assert_eq!(s.matches("**cat**").count(), 1);
    }

    #[test]
    fn query_absent_returns_plain_truncation() {
        let s = extract_snippet("nothing here", "absent");
        assert!(!s.contains("**"));
        assert_eq!(s, "nothing here");
    }

    #[test]
    fn hit_len_ge_window_emits_hit_only() {
        let q = "x".repeat(130); // >= WINDOW=120
        let text = format!("prefix{}suffix", q);
        let s = extract_snippet(&text, &q);
        assert!(s.contains(&format!("**{}**", q)));
        assert!(!s.contains('…'), "no ellipsis when hit fills window: {s}");
        assert!(!s.contains("prefix"), "no padding leaked: {s}");
    }

    #[test]
    fn empty_query_returns_prefix_no_highlight() {
        let text = "a".repeat(200);
        let s = extract_snippet(&text, "");
        assert!(!s.contains("**"));
        assert_eq!(s.chars().count(), WINDOW);
    }
}
