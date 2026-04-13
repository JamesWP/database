# Phase BC — Lexer Performance

Replace the character-iterator + accumulating-String model with a purpose-built `Scanner`
abstraction, eliminating the `peekmore` crate and removing the per-character heap writes that
make the lexer a top-3 CPU consumer during bulk INSERT workloads.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 133 | 1 | Introduce `Scanner`; wire into `Lexer`; drop `peekmore` crate (2 commits) | — |
| 134 | 1 | Eliminate `to_lowercase()` allocation and `chars().nth(n)` re-iteration in keyword matching | 133 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

`perf record` on the Sakila INSERT benchmark (15 045 samples) shows these lexer-attributed
symbols in the flat profile:

| Symbol | Self % |
|--------|--------|
| `Lexer::scan_token` | 9.15 % |
| `Lexer::peek` | 2.36 % |
| `str::to_lowercase` | 1.38 % |
| `Lexer::advance` | 1.33 % |
| `String::clone` (lexeme) | 1.51 % |

Plus the heap allocator (`malloc` 5.6 %, `_int_malloc` 6.8 %, `_int_free` 4.5 %, `cfree`
4.7 %) shows the allocator under significant pressure — a large fraction traced back to
`parse()` call stacks. Four design choices in the current lexer drive this:

1. **`PeekMoreIterator<Chars<'a>>`** — the `peekmore` crate adds indirection and iterator
   overhead compared to plain index-into-bytes access. `peek()` appears as its own 2.36 %
   symbol because every character requires a virtual dispatch through the iterator.

2. **`curent_lexeme: String` accumulator** — every `advance()` call does
   `self.curent_lexeme.push(c)`, heap-writing one character at a time. `make_token` then calls
   `curent_lexeme.clone()`, so a token with N characters causes N `push` operations plus one
   heap allocation for the clone. The `String::clone` entry (1.51 %) captures just the clone
   half; the incremental `push` overhead is folded into `scan_token` and `advance`.

3. **`to_lowercase()` in `identifier()`** — `self.curent_lexeme.clone().to_lowercase()` creates
   a second temporary `String` for every identifier or keyword seen, even keywords whose type
   already encodes their text. Shows up directly as `str::to_lowercase` at 1.38 %.

4. **`chars().nth(n)` in keyword trie** — the dispatch tree matches keywords by calling
   `ident.chars().nth(1)`, `.nth(2)`, etc. Each call re-iterates the string from the start,
   making keyword dispatch O(k²) in identifier length.

This phase fixes all four in two committed steps.

---

## Stubs

None.

---

## 133. Introduce `Scanner`; wire into `Lexer`; drop `peekmore` (Track 1)

### What Changes

#### New type: `Scanner<'a>`

A small, purpose-built scanner that replaces `PeekMoreIterator<Chars<'a>>` and
`curent_lexeme: String`. Lives in `src/frontend/scanner.rs` and is private to the
`frontend` module.

The public API uses `char` — identical to what `PeekMoreIterator<Chars>` provided —
so none of the `match` arms in the lexer need to change. Internally the scanner
tracks positions as byte offsets into `&'a [u8]`, which is what makes `current_slice()`
a zero-copy `&'a str` slice with no allocation.

```rust
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
    pos: usize,          // byte offset of next character to consume
    token_start: usize,  // byte offset of start of current token
}

impl<'a> Scanner<'a> {
    pub(super) fn new(input: &'a str) -> Self {
        Scanner { input: input.as_bytes(), pos: 0, token_start: 0 }
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
            Some(_) => Scanner { input: self.input, pos: next_pos, token_start: 0 }.peek_char_slow(),
            None => '\0',
        }
    }

    /// Consume and return the next character. Returns `'\0'` at end-of-input.
    pub(super) fn advance(&mut self) -> char {
        let c = self.peek();
        self.pos += c.len_utf8().max(if c == '\0' { 0 } else { 1 });
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

    // --- private helpers ---

    fn current_char_len(&self) -> usize {
        self.peek().len_utf8().max(if self.is_at_end() { 0 } else { 1 })
    }

    fn peek_char_slow(&self) -> char {
        std::str::from_utf8(&self.input[self.pos..])
            .ok()
            .and_then(|s| s.chars().next())
            .unwrap_or('\0')
    }
}
```

`peek()` and `is_at_end()` take `&self` — no mutable borrow needed (no iterator state to
advance). `advance()` takes `&mut self` only to increment `pos`.

#### `Lexer` — before and after

**Before:**

```rust
pub(crate) struct Lexer<'a> {
    input: PeekMoreIterator<Chars<'a>>,
    line: usize,
    column: usize,
    start: Pos,
    curent_lexeme: String,
    tokens: Vec<Token>,
}
```

**After:**

```rust
pub(crate) struct Lexer<'a> {
    scanner: Scanner<'a>,  // replaces input + curent_lexeme
    line: usize,
    column: usize,
    start: Pos,
    tokens: Vec<Token>,
}
```

`Lexer` keeps thin wrappers so all call sites inside lexer methods remain readable and
unchanged in style — and since the `Scanner` API returns `char`, the return types match
the existing wrappers exactly:

```rust
fn peek(&self)          -> char { self.scanner.peek() }
fn peek_next(&self)     -> char { self.scanner.peek_next() }
fn is_at_end(&self)     -> bool { self.scanner.is_at_end() }

fn advance(&mut self) -> char {
    self.column += 1;         // line/column tracking stays in Lexer
    self.scanner.advance()
}
```

#### `scan_token` — mark start, match arms unchanged

```rust
fn scan_token(&mut self) -> Token {
    self.skip_whitespace();
    self.scanner.mark_token_start();          // ← replaces curent_lexeme.clear()
    self.start = Pos { col: self.column, line: self.line };
    let c = self.advance();
    match c {
        '(' => self.make_token(Type::LeftParen),   // ← all existing char arms unchanged
        // ...
        '\'' => self.string('\''),
        '"'  => self.string('"'),
        '0'..='9' => self.number(),
        'a'..='z' | 'A'..='Z' | '_' => self.identifier(),
        c => self.make_token(Type::Error(Error::UnknownCharacter(c))),
    }
}
```

#### `make_token` — lexeme from `current_slice()`

```rust
fn make_token(&self, tipe: Type) -> Token {
    Token {
        tipe,
        lexeme: self.scanner.current_slice().to_string(),
        start:  Pos { col: self.start.col, line: self.start.line },
        end:    Pos { col: self.column,    line: self.line },
    }
}
```

One allocation of the exact final size, replacing N incremental `push` calls + a `clone`.

#### Other lexer methods — what changes and what doesn't

The only changes in the lexer's scanning methods are:
- Remove all `self.curent_lexeme` references (`.clear()`, `.push()`, `.clone()`, `.len()`).
- Replace `self.curent_lexeme` reads with `self.scanner.current_slice()`.
- `string()`: the escape-expansion loop reads from `self.scanner.current_slice()` instead of
  `self.curent_lexeme`.
- `number()`: `self.curent_lexeme.contains('.')` etc. → `self.scanner.current_slice().contains('.')`.
- `identifier()`: `self.curent_lexeme.clone().to_lowercase()` → `self.scanner.current_slice().to_lowercase()` (one allocation instead of two; `to_lowercase()` addressed fully in item 134).

Everything else — all `match` arms, all `char` literals, `check_next`, `skip_whitespace`,
`is_digit`, `is_alpha` — is **unchanged**.

#### Remove `peekmore` dependency

```toml
# Cargo.toml — delete:
peekmore = "1.3.0"
```

Remove `use peekmore::{PeekMore, PeekMoreIterator};` and `use std::str::Chars;` from
`lexer.rs`.

### Background

`Scanner` is intentionally minimal: byte positions internally, `char` API externally. The
`char` API means the lexer's match arms (`'('`, `'\''`, `'0'..='9'`, etc.) are untouched —
this keeps the diff focused on removing the accumulator, not on a mechanical find-replace of
every character literal.

`current_slice()` is zero-copy because the byte positions used internally correspond exactly
to offsets into the original `&str`. `token_start..pos` is always a valid UTF-8 boundary
because `advance()` increments `pos` by `char::len_utf8()`, never splitting a multi-byte
sequence.

Line and column tracking remain in `Lexer` because they are a semantic lexer concern (they
appear in `Token::start`/`end` and error messages), not a raw-traversal scanner concern.

### Key Files

- `src/frontend/scanner.rs` — new file: `Scanner`
- `src/frontend/mod.rs` — add `pub(super) mod scanner;`
- `src/frontend/lexer.rs` — replace `peekmore` + `curent_lexeme` with `scanner: Scanner`
- `Cargo.toml` — remove `peekmore`

### Tests

All existing lexer tests (17) and SQL integration tests (`cargo test test_sql_`) must pass
with no behaviour change after step 133.2.

Step 133.1 adds a `#[cfg(test)]` module directly in `src/frontend/scanner.rs` with the
following tests — each is a separate `#[test]` function:

**`peek` / `peek_next`**
- `peek_returns_first_char` — `Scanner::new("ab").peek() == 'a'`
- `peek_next_returns_second_char` — `peek_next() == 'b'` without advancing
- `peek_does_not_advance` — calling `peek()` twice gives the same char
- `peek_at_end_returns_null` — `new("").peek() == '\0'`
- `peek_next_at_last_char_returns_null` — single-char input, `peek_next() == '\0'`
- `peek_next_at_end_returns_null` — empty input, `peek_next() == '\0'`

**`advance`**
- `advance_returns_current_char` — `advance()` on `"x"` returns `'x'`
- `advance_moves_position` — after one `advance()`, `peek()` returns the next char
- `advance_at_end_returns_null` — `advance()` on exhausted scanner returns `'\0'`
- `advance_at_end_does_not_overflow` — calling `advance()` repeatedly past end stays safe
- `is_at_end_false_while_input_remains` — not at end after construction on non-empty input
- `is_at_end_true_after_full_consumption` — `is_at_end()` after advancing past all chars

**`mark_token_start` / `current_slice`**
- `current_slice_empty_before_advance` — `mark_token_start()` then no advance → `""`
- `current_slice_single_char` — mark, advance once → slice is one character
- `current_slice_multi_char` — mark, advance N times → slice matches those N characters
- `current_slice_after_whitespace_skip` — advance past whitespace, mark, advance through
  a word → slice contains only the word, not the whitespace
- `mark_resets_previous_start` — mark twice; second mark resets the start; slice reflects
  only chars after the second mark
- `current_slice_is_zero_copy` — verify the returned `&str` is a sub-slice of the original
  input (same pointer range), not a new allocation; use pointer comparison on the bytes

**`advance` + `peek` interleaving**
- `advance_then_peek` — advance once, then `peek()` returns the next char
- `advance_all_then_peek` — consume all chars, `peek()` returns `'\0'`
- `sequential_tokens` — simulate two tokens: mark, advance 3, slice; mark, advance 2, slice;
  verify both slices are correct

These tests exercise every public method of `Scanner` and every edge case (empty input,
single-char input, end-of-input sentinel, mark/advance sequencing). They run as part of
the normal `cargo test` suite since `scanner.rs` is a module in the library crate.

### Implementation Steps (2 commits)

#### Step 133.1 — Introduce `Scanner` with full unit tests

1. Create `src/frontend/scanner.rs` with the `Scanner` struct and all methods above.
2. Add `pub(super) mod scanner;` to `src/frontend/mod.rs`.
3. Write all unit tests listed in the Tests section above inside a `#[cfg(test)]` module in
   `scanner.rs`.
4. `cargo fmt && cargo build && cargo test`.

**Commit:** `frontend: add Scanner — forward-only char scanner with zero-copy token slices`

#### Step 133.2 — Wire `Scanner` into `Lexer`; drop `peekmore`

1. Replace `input: PeekMoreIterator<Chars<'a>>` and `curent_lexeme: String` with
   `scanner: Scanner<'a>` in `Lexer`.
2. Add thin `peek` / `peek_next` / `advance` / `is_at_end` wrappers on `Lexer`.
3. Update `scan_token`: call `self.scanner.mark_token_start()`, convert all `match` arms to
   byte literals.
4. Update `make_token`: use `self.scanner.current_slice().to_string()`.
5. Update `check_next`, `skip_whitespace`, `string`, `number`, `identifier` for `u8`.
6. Update `is_digit` / `is_alpha` to accept `u8`.
7. Remove `peekmore` from `Cargo.toml` and `use peekmore::...` from `lexer.rs`.
8. `cargo fmt && cargo build && cargo test`.

**Commit:** `lexer: replace peekmore + curent_lexeme with Scanner; drop peekmore crate`

---

## 134. Eliminate `to_lowercase()` allocation and `chars().nth(n)` re-iteration (Track 1)

### What Changes

**`identifier()` keyword matching — before:**

```rust
fn identifier(&mut self) -> Token {
    // ... consume loop ...
    let ident: String = self.curent_lexeme.clone().to_lowercase();  // 2 allocations (item 133 leaves to_lowercase in place)
    let ident = ident.as_str();

    let tipe = match ident.chars().next().unwrap() {      // re-iterates each time
        's' => match ident.chars().nth(1) {               // O(n) for each nth()
            Some('e') => match ident.chars().nth(2) { ... }
            ...
        }
        // ...
    };
```

**`identifier()` keyword matching — after:**

```rust
fn identifier(&mut self) -> Token {
    // ... consume loop ...
    let raw = self.scanner.current_slice();  // &str, zero-copy, no allocation

    let tipe = match raw.as_bytes()[0].to_ascii_lowercase() {   // O(1) byte index
        b's' => match raw.as_bytes().get(1).map(|b| b.to_ascii_lowercase()) {
            Some(b'e') => match raw.as_bytes().get(2).map(|b| b.to_ascii_lowercase()) {
                Some(b'l') => match_keyword(raw, "select", Type::Select),
                Some(b't') => match_keyword(raw, "set",    Type::Set),
                _ => make_identifier(raw),
            },
            _ => make_identifier(raw),
        },
        // ... all arms converted to byte index dispatch ...
    };

    self.make_token(tipe)
}
```

**New helpers:**

```rust
/// Case-insensitive match of a scanned identifier against a keyword.
/// Returns Type::Identifier if they don't match.
fn match_keyword(raw: &str, keyword: &str, tipe: Type) -> Type {
    if raw.eq_ignore_ascii_case(keyword) { tipe }
    else { make_identifier(raw) }
}

/// Build an Identifier token value: one to_ascii_lowercase allocation.
fn make_identifier(raw: &str) -> Type {
    Type::Identifier(raw.to_ascii_lowercase())
}
```

`match_reserved` is deleted and replaced by `match_keyword` + `make_identifier`.
The dispatch trie switches its branch condition from `chars().nth(n)` to
`raw.as_bytes().get(n)` — O(1) index, no re-iteration — but the surrounding `&str` API
is preserved throughout.

**Summary of allocation count per identifier token:**

| Case | Before | After |
|------|--------|-------|
| Keyword (e.g. `SELECT`) | 2 Strings (clone + to_lowercase) | 0 allocations |
| Identifier (e.g. `rental_id`) | 2 Strings (clone + to_lowercase) | 1 String (to_ascii_lowercase) |

### Background

SQL INSERT workloads produce many keyword tokens (`INSERT`, `INTO`, `VALUES`, column names)
and many identifier tokens (table and column names). The `to_lowercase()` + `clone()` pair
was the dominant allocation site inside `identifier()`.

`eq_ignore_ascii_case` on `&[u8]` is a direct constant-time comparison with no allocation.
Direct byte indexing `raw[n].to_ascii_lowercase()` replaces `chars().nth(n)` which re-walks
the string from the start each time.

`to_ascii_lowercase()` (used for the final `Identifier(String)`) is preferred over
`to_lowercase()` because SQL identifiers are always ASCII and `to_ascii_lowercase` is faster
(no Unicode case mapping).

### Key Files

- `src/frontend/lexer.rs` — `identifier()`, `match_reserved` → `match_keyword` + `make_identifier`

### Tests

All existing lexer and SQL integration tests must pass. The only observable difference is that
`identifier_from_bytes` uses `to_ascii_lowercase` instead of `to_lowercase`; for ASCII input
these are identical.

### Implementation Steps (1 commit)

#### Step 134.1 — Byte keyword trie, eliminate `to_lowercase()` allocation

1. Add `match_keyword(raw: &[u8], keyword: &[u8], tipe: Type) -> Type` helper.
2. Add `identifier_from_bytes(raw: &[u8]) -> Type` helper.
3. Delete `match_reserved`.
4. Rewrite `identifier()`:
   - Remove `let ident: String = raw.to_lowercase()`.
   - Change the trie `match` to operate on `raw[0].to_ascii_lowercase()` and
     `raw.get(n).map(|b| b.to_ascii_lowercase())`.
   - Replace every `match_reserved(ident, "...", Type::...)` call with
     `match_keyword(raw, "...", Type::...)`.
   - Replace every `Type::Identifier(ident.to_owned())` fall-through with
     `identifier_from_bytes(raw)`.
5. `cargo fmt && cargo build && cargo test`.

**Commit:** `lexer: eliminate to_lowercase() allocation and chars().nth(n) in keyword matching`

---

## Verification

- [ ] `cargo test` — all tests pass after each commit independently
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `peekmore` removed from `Cargo.toml` and `Cargo.lock`
- [ ] `Scanner` unit tests pass; `current_slice()` invariant covered
- [ ] `Lexer` struct has no `curent_lexeme` field and no `peekmore` import
- [ ] No `to_lowercase()` call remaining in `identifier()`
- [ ] No `chars().nth` call remaining in `identifier()`
- [ ] Perf improvement measurable with `perf record` against sakila INSERT benchmark (see CLAUDE.md profiling section)
