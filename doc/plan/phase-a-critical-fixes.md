# Phase A — Critical Fixes & Test Foundations

Phase A addresses safety-critical issues, builds test infrastructure, and fills the most urgent test coverage gaps.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 1 | 7.2 | SQL test harness | — |
| 2 | 5.1 | CellReader unsafe pointer fix | — |
| 3 | 7.1 | Cell/CellReader tests | 2 |
| 4 | 6.1 | Cache file handle in Pager | — |
| 5 | 7.1 | Pager tests | 4 |
| 6 | 6.2 | Serialization error handling | — |

---

## 1. SQL Test Harness (Track 7.2)

### What Changes

Build an automated end-to-end SQL test runner to replace manual testing.

### Key Files

- New: `tests/sql_runner.rs` — Rust integration test
- New: `tests/sql/basic_crud.sql` + `tests/sql/basic_crud.expected`

### Implementation Approach

1. Create `tests/sql_runner.rs` that discovers `.sql`/`.expected` pairs in `tests/sql/`.
2. For each pair: create a temp database via `BTree::new()`, execute each SQL line via `db::execute()`, collect output rows as strings, compare to `.expected` file contents.
3. First test script `basic_crud.sql` mirrors `manual_tests/test_sql_mode.sql` (CREATE TABLE, INSERT, SELECT with 3 tables, 16 rows, 10 queries).
4. The `db::execute()` function (`src/db.rs`) returns `Vec<Vec<ScalarValue>>` — format these as tab-separated or JSON lines for comparison.

### Tests

The harness _is_ the test. Adding a new test = adding a `.sql`/`.expected` file pair.

---

## 2. CellReader Unsafe Pointer Fix (Track 5.1)

### What Changes

Eliminate undefined behavior in `CellReader`. Currently `buf: &'a [u8]` points into `node: Box<NodePage>` via `unsafe { std::slice::from_raw_parts() }`. When `node` is reassigned during overflow reads (line 31), `buf` becomes a dangling pointer.

### Key Files

- `src/storage/cell_reader.rs` — lines 39, 62

### Implementation Approach

1. Replace `buf: &'a [u8]` with `buf: Vec<u8>` (owned data).
2. When constructing a CellReader, copy the value bytes out of the node page into the owned Vec.
3. For overflow pages, append continuation bytes to the owned Vec rather than reassigning the slice.
4. Remove the `unsafe` block entirely.
5. The lifetime parameter `'a` on CellReader only needs to borrow the Pager (for reading overflow pages), not the node data.

### Tests

- Existing tests must pass.
- New test: read a value that spans overflow pages, verify data integrity.

---

## 3. Cell/CellReader Tests (Track 7.1)

### What Changes

Add tests for `src/storage/cell.rs` and `src/storage/cell_reader.rs` — currently 0 tests.

### Key Files

- `src/storage/cell.rs` — add `#[cfg(test)]` module
- `src/storage/cell_reader.rs` — add `#[cfg(test)]` module

### Tests to Add

- `test_cell_roundtrip` — create Cell, encode, decode, verify key + value match
- `test_cell_overflow_value` — value > 55 bytes (CHUNK_THRESHOLD), verify it splits across pages and reads back correctly
- `test_cell_multi_page_overflow` — value > 155 bytes (spans 2+ overflow pages)
- `test_decode_as_json_array` — decode `[1, "alice", 30]`, verify correct ScalarValues
- `test_decode_as_json_array_types` — verify integer, float, string, bool types
- `test_cell_empty_value` — zero-length value

---

## 4. Cache File Handle in Pager (Track 6.1)

### What Changes

The Pager opens a new `File` on every read/write operation (`src/storage/pager.rs` lines 101-125). Refactor to hold a persistent file handle.

### Key Files

- `src/storage/pager.rs`

### Implementation Approach

1. Add `file: File` field to the `Pager` struct, opened at construction time with read+write.
2. Replace `file_at_page_readonly()` and `file_at_page_readwrite()` with methods that seek to the correct offset on the existing handle: `offset = page_idx * PAGE_SIZE`.
3. Use `&self.file` for reads and `&mut self.file` (or `File` supports `seek` + `read`/`write` on `&File` via OS handles) for writes.
4. Remove the helper methods that open files.

### Tests

- All existing tests must pass (this is a refactoring).
- Verify multi-page operations (write page 0, write page 5, read page 0, read page 5).

---

## 5. Pager Tests (Track 7.1)

### What Changes

Add tests for `src/storage/pager.rs` — currently only 2 tests.

### Key Files

- `src/storage/pager.rs` — add to existing `#[cfg(test)]` module

### Tests to Add

- `test_pager_persistence` — write pages to a file, drop the Pager, create a new Pager on the same file, read pages back, verify data matches
- `test_pager_free_list_roundtrip` — allocate 3 pages, deallocate middle page, allocate again, verify reused page
- `test_pager_page_boundary` — write exactly 4096 bytes to a page, verify no overflow

---

## 6. Serialization Error Handling (Track 6.2)

### What Changes

Replace three `todo!()` calls in JSON encode error handling at `src/storage/pager.rs` lines 168-170.

### Key Files

- `src/storage/pager.rs`

### Implementation Approach

1. Create an `EncodingError` variant for each JSON error category (Syntax, Data, Eof) — or consolidate into a single `SerializationError(String)` variant.
2. Replace each `todo!()` with `Err(EncodingError::SerializationError(...))`.
3. Ensure callers handle the new error variants (most already handle `EncodingError::NotEnoughSpaceInPage`).

### Tests

- Existing tests pass.
- Add a test that attempts to encode a value that triggers a serialization error (e.g., an intentionally malformed struct if possible, or verify the error path exists via code inspection).

---

## Verification

For each item, before considering it done:
- [ ] Tests written first (TDD)
- [ ] All new tests pass: `cargo test --bin database`
- [ ] All existing tests still pass
- [ ] Code formatted: `cargo fmt`
- [ ] No compiler warnings: `cargo build 2>&1 | grep -i warning`
