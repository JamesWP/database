/// Forward-only character scanner over a UTF-8 string slice.
///
/// Exposes a `char`-based API so the lexer's existing match arms are unchanged.
/// Internally tracks byte positions so `current_slice()` is a zero-copy borrow
/// of the original input.
///
/// Invariant: `token_start <= pos <= input.len()`; both positions are always on
/// UTF-8 character boundaries.
pub(super) struct Scanner<'a> {
    input: &'a [u8],
    pos: usize,         // byte offset of next character to consume
    token_start: usize, // byte offset of start of current token
}

impl<'a> Scanner<'a> {
    pub(super) fn new(input: &'a str) -> Self {
        Scanner {
            input: input.as_bytes(),
            pos: 0,
            token_start: 0,
        }
    }

    /// The next character without consuming it. Returns `'\0'` at end-of-input.
    pub(super) fn peek(&self) -> char {
        // For SQL input the first byte is always ASCII (single-byte), so this
        // is branchless in the common case. Non-ASCII only appears inside string
        // literal *content*, where the lexer scans until a closing quote rather
        // than matching individual characters.
        match self.input.get(self.pos) {
            Some(&b) if b < 128 => b as char,
            Some(_) => self.peek_char_slow(),
            None => '\0',
        }
    }

    /// The character after next without consuming either. Returns `'\0'` at end-of-input.
    pub(super) fn peek_next(&self) -> char {
        let next_pos = self.pos + self.current_char_len();
        match self.input.get(next_pos) {
            Some(&b) if b < 128 => b as char,
            Some(_) => Scanner {
                input: self.input,
                pos: next_pos,
                token_start: 0,
            }
            .peek_char_slow(),
            None => '\0',
        }
    }

    /// Consume and return the next character. Returns `'\0'` at end-of-input.
    pub(super) fn advance(&mut self) -> char {
        let c = self.peek();
        if c != '\0' {
            self.pos += c.len_utf8();
        }
        c
    }

    /// True when all input has been consumed.
    pub(super) fn is_at_end(&self) -> bool {
        self.pos >= self.input.len()
    }

    /// Record the current position as the start of the next token.
    /// Call this after consuming any leading whitespace, before scanning
    /// the token body.
    pub(super) fn mark_token_start(&mut self) {
        self.token_start = self.pos;
    }

    /// The source text from the last `mark_token_start()` to the current
    /// position — a zero-copy slice of the original input string.
    pub(super) fn current_slice(&self) -> &'a str {
        std::str::from_utf8(&self.input[self.token_start..self.pos])
            .expect("token_start..pos is always a valid UTF-8 boundary")
    }

    /// The nth byte of the current token (0-indexed from `token_start`),
    /// lowercased for ASCII. Returns `0` if `n` is out of range.
    /// Used for O(1) keyword trie dispatch.
    pub(super) fn token_byte_at(&self, n: usize) -> u8 {
        self.input
            .get(self.token_start + n)
            .map(|b| b.to_ascii_lowercase())
            .unwrap_or(0)
    }

    /// True if the current token equals `keyword` case-insensitively (ASCII).
    /// Used for the final keyword confirmation step.
    pub(super) fn token_eq_keyword(&self, keyword: &str) -> bool {
        let slice = &self.input[self.token_start..self.pos];
        slice.eq_ignore_ascii_case(keyword.as_bytes())
    }

    // --- private helpers ---

    fn current_char_len(&self) -> usize {
        let c = self.peek();
        if c == '\0' {
            0
        } else {
            c.len_utf8()
        }
    }

    fn peek_char_slow(&self) -> char {
        std::str::from_utf8(&self.input[self.pos..])
            .ok()
            .and_then(|s| s.chars().next())
            .unwrap_or('\0')
    }
}

#[cfg(test)]
mod tests {
    use super::Scanner;

    // --- peek / peek_next ---

    #[test]
    fn peek_returns_first_char() {
        let s = Scanner::new("ab");
        assert_eq!(s.peek(), 'a');
    }

    #[test]
    fn peek_next_returns_second_char() {
        let s = Scanner::new("ab");
        assert_eq!(s.peek_next(), 'b');
    }

    #[test]
    fn peek_does_not_advance() {
        let s = Scanner::new("ab");
        assert_eq!(s.peek(), 'a');
        assert_eq!(s.peek(), 'a');
    }

    #[test]
    fn peek_at_end_returns_null() {
        let s = Scanner::new("");
        assert_eq!(s.peek(), '\0');
    }

    #[test]
    fn peek_next_at_last_char_returns_null() {
        let s = Scanner::new("x");
        assert_eq!(s.peek_next(), '\0');
    }

    #[test]
    fn peek_next_at_end_returns_null() {
        let s = Scanner::new("");
        assert_eq!(s.peek_next(), '\0');
    }

    // --- advance ---

    #[test]
    fn advance_returns_current_char() {
        let mut s = Scanner::new("x");
        assert_eq!(s.advance(), 'x');
    }

    #[test]
    fn advance_moves_position() {
        let mut s = Scanner::new("ab");
        s.advance();
        assert_eq!(s.peek(), 'b');
    }

    #[test]
    fn advance_at_end_returns_null() {
        let mut s = Scanner::new("");
        assert_eq!(s.advance(), '\0');
    }

    #[test]
    fn advance_at_end_does_not_overflow() {
        let mut s = Scanner::new("a");
        s.advance(); // consume 'a'
        s.advance(); // at end
        s.advance(); // past end — must not panic
        assert_eq!(s.peek(), '\0');
    }

    #[test]
    fn is_at_end_false_while_input_remains() {
        let s = Scanner::new("x");
        assert!(!s.is_at_end());
    }

    #[test]
    fn is_at_end_true_after_full_consumption() {
        let mut s = Scanner::new("x");
        s.advance();
        assert!(s.is_at_end());
    }

    // --- mark_token_start / current_slice ---

    #[test]
    fn current_slice_empty_before_advance() {
        let mut s = Scanner::new("hello");
        s.mark_token_start();
        assert_eq!(s.current_slice(), "");
    }

    #[test]
    fn current_slice_single_char() {
        let mut s = Scanner::new("hello");
        s.mark_token_start();
        s.advance();
        assert_eq!(s.current_slice(), "h");
    }

    #[test]
    fn current_slice_multi_char() {
        let mut s = Scanner::new("hello");
        s.mark_token_start();
        s.advance();
        s.advance();
        s.advance();
        assert_eq!(s.current_slice(), "hel");
    }

    #[test]
    fn current_slice_after_whitespace_skip() {
        let mut s = Scanner::new("   word");
        // advance past whitespace
        s.advance();
        s.advance();
        s.advance();
        // mark start before the word
        s.mark_token_start();
        s.advance();
        s.advance();
        s.advance();
        s.advance();
        assert_eq!(s.current_slice(), "word");
    }

    #[test]
    fn mark_resets_previous_start() {
        let mut s = Scanner::new("abcde");
        s.mark_token_start();
        s.advance();
        s.advance();
        // reset
        s.mark_token_start();
        s.advance();
        s.advance();
        assert_eq!(s.current_slice(), "cd");
    }

    #[test]
    fn current_slice_is_zero_copy() {
        let input = "hello world";
        let mut s = Scanner::new(input);
        s.advance(); // skip 'h'
        s.advance(); // skip 'e'
        s.mark_token_start();
        s.advance();
        s.advance();
        s.advance();
        let slice = s.current_slice();
        // Verify the slice is a sub-slice of the original input (same memory)
        let input_range = input.as_ptr() as usize..input.as_ptr() as usize + input.len();
        let slice_start = slice.as_ptr() as usize;
        assert!(
            input_range.contains(&slice_start),
            "current_slice should be a zero-copy view into the original input"
        );
        assert_eq!(slice, "llo");
    }

    // --- advance + peek interleaving ---

    #[test]
    fn advance_then_peek() {
        let mut s = Scanner::new("ab");
        s.advance();
        assert_eq!(s.peek(), 'b');
    }

    #[test]
    fn advance_all_then_peek() {
        let mut s = Scanner::new("ab");
        s.advance();
        s.advance();
        assert_eq!(s.peek(), '\0');
    }

    #[test]
    fn sequential_tokens() {
        let mut s = Scanner::new("abcde");
        s.mark_token_start();
        s.advance();
        s.advance();
        s.advance();
        let first = s.current_slice();
        assert_eq!(first, "abc");

        s.mark_token_start();
        s.advance();
        s.advance();
        let second = s.current_slice();
        assert_eq!(second, "de");
    }

    // --- token_byte_at ---

    #[test]
    fn token_byte_at_returns_correct_byte() {
        let mut s = Scanner::new("SELECT");
        s.mark_token_start();
        for _ in 0..6 {
            s.advance();
        }
        assert_eq!(s.token_byte_at(0), b's');
    }

    #[test]
    fn token_byte_at_lowercases() {
        let mut s = Scanner::new("SELECT");
        s.mark_token_start();
        for _ in 0..6 {
            s.advance();
        }
        assert_eq!(s.token_byte_at(0), b's'); // 'S' → b's'
        assert_eq!(s.token_byte_at(1), b'e'); // 'E' → b'e'
    }

    #[test]
    fn token_byte_at_out_of_range_returns_zero() {
        let mut s = Scanner::new("ab");
        s.mark_token_start();
        s.advance();
        s.advance();
        assert_eq!(s.token_byte_at(99), 0);
    }

    // --- token_eq_keyword ---

    #[test]
    fn token_eq_keyword_matches_exact() {
        let mut s = Scanner::new("select");
        s.mark_token_start();
        for _ in 0..6 {
            s.advance();
        }
        assert!(s.token_eq_keyword("select"));
    }

    #[test]
    fn token_eq_keyword_matches_case_insensitive() {
        let mut s = Scanner::new("SELECT");
        s.mark_token_start();
        for _ in 0..6 {
            s.advance();
        }
        assert!(s.token_eq_keyword("select"));
    }

    #[test]
    fn token_eq_keyword_rejects_wrong_keyword() {
        let mut s = Scanner::new("select");
        s.mark_token_start();
        for _ in 0..6 {
            s.advance();
        }
        assert!(!s.token_eq_keyword("set"));
    }

    #[test]
    fn token_eq_keyword_rejects_prefix() {
        let mut s = Scanner::new("sel");
        s.mark_token_start();
        for _ in 0..3 {
            s.advance();
        }
        assert!(!s.token_eq_keyword("select"));
    }
}
