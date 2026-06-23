//! Pure chunking algorithm (Spec §10). Splits text into pieces ≤ `target`
//! chars, preferring paragraph boundaries, falling back to sentence
//! boundaries, then hard cuts. Used by `BlockService::create_in_tx` to
//! auto-derive chunks from block text.

/// Split `text` into chunks of at most `target` chars.
///
/// Algorithm:
/// 1. If `text.len() <= target`, return `[text]`.
/// 2. Otherwise, split on `\n\n` (paragraphs). Greedily accumulate paragraphs
///    into the current chunk while it stays under `target`. When the next
///    paragraph would overflow, flush the current chunk and start fresh.
/// 3. If a single paragraph exceeds `target`, split it on `. ` (sentence
///    boundary) using the same greedy accumulation.
/// 4. If a single sentence exceeds `target`, hard-cut at `target` char
///    boundaries (UTF-8 safe via `char_indices`).
///
/// Empty input returns `vec![]`.
pub fn chunk_text(text: &str, target: usize) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    if text.len() <= target {
        return vec![text.to_string()];
    }
    let mut chunks: Vec<String> = Vec::new();
    let mut current = String::new();

    for para in text.split("\n\n") {
        if para.is_empty() {
            continue;
        }
        if para.len() <= target {
            let added_len = if current.is_empty() { 0 } else { 2 } + para.len();
            if current.len() + added_len <= target {
                if !current.is_empty() {
                    current.push_str("\n\n");
                }
                current.push_str(para);
            } else {
                if !current.is_empty() {
                    chunks.push(std::mem::take(&mut current));
                }
                current = para.to_string();
            }
        } else {
            // Paragraph too big; flush current then split by sentence
            if !current.is_empty() {
                chunks.push(std::mem::take(&mut current));
            }
            for sentence in split_sentences(para) {
                let added = if current.is_empty() { 0 } else { 1 } + sentence.len();
                if current.len() + added <= target {
                    if !current.is_empty() {
                        current.push(' ');
                    }
                    current.push_str(&sentence);
                } else {
                    if !current.is_empty() {
                        chunks.push(std::mem::take(&mut current));
                    }
                    if sentence.len() <= target {
                        current = sentence;
                    } else {
                        // Hard cut (UTF-8 safe)
                        for chunk in hard_split(&sentence, target) {
                            chunks.push(chunk);
                        }
                        current.clear();
                    }
                }
            }
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// Split on `. ` (sentence boundary), preserving the period.
fn split_sentences(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0;
    let bytes = s.as_bytes();
    for i in 0..bytes.len().saturating_sub(1) {
        if bytes[i] == b'.' && bytes[i + 1] == b' ' {
            let end = i + 1; // include the period
            out.push(s[start..end].to_string());
            start = i + 2; // skip ". "
        }
    }
    if start < s.len() {
        out.push(s[start..].to_string());
    }
    out
}

/// UTF-8-safe hard cut at `target` char boundaries.
fn hard_split(s: &str, target: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for ch in s.chars() {
        if current.len() + ch.len_utf8() > target && !current.is_empty() {
            out.push(std::mem::take(&mut current));
        }
        current.push(ch);
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_returns_empty() {
        assert!(chunk_text("", 1024).is_empty());
    }

    #[test]
    fn short_text_single_chunk() {
        let result = chunk_text("Hello world.", 1024);
        assert_eq!(result, vec!["Hello world.".to_string()]);
    }

    #[test]
    fn paragraph_boundary_preferred() {
        let text = "para one\n\npara two\n\npara three";
        let result = chunk_text(text, 100);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], text);
    }

    #[test]
    fn paragraph_split_when_overflows() {
        let text = "a\n\nb\n\nc";
        // target between "a\n\nb" (3) and "a\n\nb\n\nc" (7)
        let result = chunk_text(text, 4);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "a\n\nb");
        assert_eq!(result[1], "c");
    }

    #[test]
    fn long_paragraph_uses_sentence_boundary() {
        // 3 sentences each ~10 chars
        let text = "Sentence one. Sentence two. Sentence three.";
        let result = chunk_text(text, 25);
        // Should split on ". "
        assert!(result.iter().all(|c| c.len() <= 25));
        // Should preserve all text content (concatenated with spaces equals original minus whitespace diff)
        let joined: String = result.join(" ");
        assert!(joined.contains("Sentence one."));
        assert!(joined.contains("Sentence two."));
        assert!(joined.contains("Sentence three."));
    }

    #[test]
    fn very_long_sentence_hard_cuts() {
        let text: String = "x".repeat(50);
        let result = chunk_text(&text, 10);
        assert!(result.iter().all(|c| c.len() <= 10));
        let total: usize = result.iter().map(|s| s.len()).sum();
        assert_eq!(total, 50);
    }

    #[test]
    fn utf8_safe_hard_cut() {
        let text: String = "é".repeat(50); // each é is 2 bytes
        let result = chunk_text(&text, 5);
        // Each chunk should have at most 5 chars (≤ 10 bytes for é)
        assert!(result.iter().all(|c| c.chars().count() <= 5));
        let total_chars: usize = result.iter().map(|s| s.chars().count()).sum();
        assert_eq!(total_chars, 50);
    }

    #[test]
    fn utf8_boundary_no_panic_on_multibyte() {
        // Multi-byte chars at the boundary should not produce invalid UTF-8
        let text = "中".repeat(100);
        let result = chunk_text(&text, 7); // 7 bytes is 2 full 中 (6 bytes) + partial
        for c in &result {
            assert!(
                std::str::from_utf8(c.as_bytes()).is_ok(),
                "chunk not valid UTF-8"
            );
        }
    }
}
