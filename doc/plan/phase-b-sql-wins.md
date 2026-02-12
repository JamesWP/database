# Phase B — Quick SQL Wins + Backfill Tests

Phase B adds high-visibility SQL features (SELECT *, NULL), completes cursor API gaps, and backfills test coverage across the storage and integration layers.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 7 | 1.1 | SELECT * | — |
| 8 | 2.1 | NULL support | — |
| 9 | 7.1 | B-tree tests | — |
| 10 | 5.3 | Complete cursor.last() | — |
| 11 | 5.2 | find() returns found/not-found | — |
| 12 | 7.1 | DB integration tests | — |
| 13 | 7.2 | SQL test scripts | Phase A item 1 |

---
Important: Each item should be committed seperately, follow 'Git Workflow' in CLAUDE.md

## 7. SELECT * (Track 1.1)

### What Changes

Parser doesn't handle `*` in SELECT. Unblocks the ignored `test_select_star` test.

### Key Files

- `src/frontend/parser.rs` — recognize `Token::Star` in column list
- `src/frontend/ast.rs` — add `ColumnExpression::Wildcard` or `SelectItem::Star`
- `src/planner.rs` — resolve `*` to all columns from table DDL via catalog lookup

### Implementation Approach

1. In the parser, when parsing select columns, check for `*` token. If found, produce a wildcard AST node instead of parsing column expressions.
2. In the planner, when encountering the wildcard, look up the table's column list from the catalog DDL (same as how INSERT resolves columns). Generate a `Project` with all column indices.
3. Un-ignore `test_select_star` and verify it passes.

### Tests

- `test_select_star` (existing, currently ignored)
- `test_select_star_multi_column` — table with 5 columns, SELECT *, verify all returned
- Parser test: `SELECT * FROM t` produces correct AST

---

## 8. NULL Support (Track 2.1)

### What Changes

Add `ScalarValue::Null` variant and handle NULL throughout the system.

### Key Files

- `src/engine/scalarvalue.rs` — add Null variant, handle in all operations
- `src/compiler/expr.rs` — line 51, currently panics on `Literal::Null`
- `src/compiler/nodes.rs` — line 15, NULL literal handling
- `src/engine.rs` — handle NULL in arithmetic/comparison/logical operations

### Implementation Approach

1. Add `ScalarValue::Null` to the enum.
2. Implement SQL NULL semantics: any arithmetic with NULL returns NULL, any comparison with NULL returns NULL (not true/false), logical AND/OR follow 3-value logic.
3. Update `compile_expr` to emit `StoreValue(reg, ScalarValue::Null)` for null literals.
4. Update Display trait for Null.

### Tests

- `test_null_literal` — SELECT NULL returns Null
- `test_null_arithmetic` — NULL + 1 returns NULL
- `test_null_comparison` — NULL = NULL returns NULL (not true)
- `test_null_and_logic` — NULL AND true = NULL, NULL AND false = false
- `test_null_or_logic` — NULL OR true = true, NULL OR false = NULL

---

## 9. B-tree Tests (Track 7.1)

### Tests to Add in `src/storage/btree.rs`

- `test_empty_table_scan` — call `first()` on empty tree, verify `get_entry()` returns None
- `test_duplicate_key_insert` — insert key=1 twice with different values, verify overwrite
- `test_find_nonexistent_key` — find() for key not in tree, verify behavior
- `test_cursor_prev_from_middle` — insert 10 keys, navigate to middle, call prev(), verify correct key
- `test_large_tree_ordering` — insert 1000+ keys in random order, scan and verify sorted

---

## 10. Complete cursor.last() (Track 5.3)

### What Changes

`cursor.last()` has `todo!()` at line 294 for interior nodes.

### Key Files

- `src/storage/btree.rs` — line 294

### Implementation Approach

Implement recursive descent to rightmost leaf: follow last edge of each interior node until reaching a leaf, then position at last cell.

### Tests

- `test_cursor_last_single_page` — small tree, last() returns highest key
- `test_cursor_last_multi_level` — tree with splits (interior nodes), last() returns highest key
- `test_cursor_last_then_prev` — last() then prev() navigates backward correctly

---

## 11. find() Returns Found/Not-Found (Track 5.2)

### What Changes

`find()` returns nothing — caller can't tell if key was found. Change return type to `bool`.

### Key Files

- `src/storage/btree.rs` — lines 303-326

### Implementation Approach

1. Change `find(&mut self, key: u64)` to `find(&mut self, key: u64) -> bool`.
2. Return `true` for `SearchResult::Found`, `false` for `SearchResult::NotPresent`.
3. Update all callers.

### Tests

- `test_find_returns_true_for_existing` — insert key, find returns true
- `test_find_returns_false_for_missing` — find non-existent key returns false

---

## 12. DB Integration Tests (Track 7.1)

### Tests to Add in `src/db.rs`

- `test_persistence` — insert data, drop BTree, reopen from same file path, SELECT returns same rows
- `test_multi_table` — create two tables, insert into both, query each independently
- `test_large_insert` — insert 100+ rows, SELECT with WHERE filter, verify correct subset
- `test_select_nonexistent_table` — SELECT from unknown table returns error
- `test_insert_wrong_column_count` — INSERT with too few/many values returns error

---

## 13. SQL Test Scripts (Track 7.2)

### New Files in `tests/sql/`

- `where_clauses.sql` / `.expected` — various predicates (=, !=, <, >, <=, >=, AND, OR)
- `expressions.sql` / `.expected` — arithmetic in SELECT list, column expressions
- `multi_table.sql` / `.expected` — create and query multiple tables
- `error_cases.sql` / `.expected` — malformed SQL, missing tables (verify error messages)

---

## Verification

For each item, before considering it done:
- [ ] Tests written first (TDD)
- [ ] All new tests pass: `cargo test --bin database`
- [ ] All existing tests still pass
- [ ] Code formatted: `cargo fmt`
- [ ] No compiler warnings: `cargo build 2>&1 | grep -i warning`
