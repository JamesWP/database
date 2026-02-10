# Phase D — Core CRUD

Phase D adds mutation operations (UPDATE, DELETE, DROP TABLE) and the storage-layer delete primitive they depend on, plus cursor invalidation safety.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 19 | 3.1 | B-tree delete (leaf cell removal) | — |
| 20 | 1.3 | UPDATE t SET col=expr WHERE ... | — |
| 21 | 1.2 | DELETE FROM t WHERE ... | 19 |
| 22 | 1.4 | DROP TABLE t | 19 |
| 23 | 5.4 | Cursor stack invalidation | 19 |

---

## 19. B-tree Delete (Track 3.1)

### What Changes

Add `delete(key: u64)` to the cursor/B-tree for row removal by primary key.

### Key Files

- `src/storage/btree.rs` — new `Cursor::delete()` method
- `src/storage/node.rs` — add `LeafNode::remove_cell(index)`

### Implementation Approach

1. Use `find(key)` to position cursor on the target leaf and cell.
2. If key matches, remove cell from leaf's `cells` vector.
3. Write modified page via `pager.encode_and_set()`.
4. Skip rebalancing for v1 — sparse pages are acceptable.
5. Free overflow pages if the deleted cell had them (or leak with TODO for v1).
6. Invalidate cursor state after delete.

### Tests

- `test_btree_delete_single` — insert, delete, verify gone
- `test_btree_delete_nonexistent` — delete missing key, no-op
- `test_btree_delete_from_multi_page` — delete from tree with splits
- `test_btree_delete_then_scan` — scan after delete shows only remaining keys
- `test_btree_delete_all` — delete all keys one by one, tree is empty

---

## 20. UPDATE (Track 1.3)

### What Changes

Support SQL UPDATE via scan + overwrite (reinsert with same key).

### Key Files

- `src/frontend/ast.rs` — add `Statement::Update(UpdateStatement)` with assignments and WHERE
- `src/frontend/parser.rs` — parse `UPDATE t SET col=expr WHERE condition`
- `src/frontend/lexer.rs` — ensure UPDATE and SET keywords
- `src/planner.rs` — add `LogicalPlan::Update`
- `src/engine/` — compile and execute

### Implementation Approach

1. Parse `UPDATE t SET col1=expr1, col2=expr2 [WHERE cond]`.
2. Planner resolves column names to indices, creates Update plan node.
3. Execution: scan table (with optional filter), for each matching row read key + full data, compute new values, reinsert with same key (existing insert handles overwrite).
4. Return count of updated rows.

### Tests

- `test_update_single_row` — UPDATE WHERE id=1, verify via SELECT
- `test_update_all_rows` — UPDATE without WHERE
- `test_update_no_match` — UPDATE WHERE false, 0 rows affected
- `test_update_multiple_columns` — SET col1=x, col2=y

---

## 21. DELETE (Track 1.2)

### What Changes

Support SQL DELETE using B-tree delete from item 19.

### Key Files

- `src/frontend/ast.rs` — add `Statement::Delete(DeleteStatement)`
- `src/frontend/parser.rs` — parse `DELETE FROM t [WHERE cond]`
- `src/planner.rs` — add `LogicalPlan::Delete`
- `src/storage/btree.rs` — uses `Cursor::delete(key)`

### Implementation Approach

1. Parse `DELETE FROM t [WHERE condition]`.
2. Execution: scan table, collect primary keys of matching rows into a Vec (collect first to avoid mutating during iteration), then delete each key.
3. Return count of deleted rows.

### Tests

- `test_delete_single_row` — DELETE WHERE id=1, verify gone
- `test_delete_all_rows` — DELETE without WHERE
- `test_delete_no_match` — 0 rows affected
- `test_delete_then_insert` — delete then insert new row

---

## 22. DROP TABLE (Track 1.4)

### What Changes

Support `DROP TABLE` to remove a table and its catalog entry.

### Key Files

- `src/frontend/ast.rs` — add `Statement::Drop(DropTableStatement)`
- `src/frontend/parser.rs` — parse `DROP TABLE name`
- `src/storage/btree.rs` — delete catalog row via B-tree delete

### Implementation Approach

1. Look up table in db_schema catalog, get its catalog row key.
2. Delete the catalog row using B-tree delete on the db_schema tree.
3. Page reclamation (freeing the table's pages) is deferred — orphaned pages are acceptable for v1.

### Tests

- `test_drop_table_removes_catalog` — after DROP, table not in catalog
- `test_drop_table_prevents_queries` — SELECT from dropped table errors
- `test_drop_nonexistent` — DROP unknown table errors
- `test_drop_and_recreate` — DROP then CREATE same name succeeds

---

## 23. Cursor Stack Invalidation (Track 5.4)

### What Changes

Clear stale cursor navigation state after mutations.

### Key Files

- `src/storage/btree.rs` — CursorState lines 23-24

### Implementation Approach

1. Add `CursorState::invalidate()` — clears `stack` and `leaf_iterator`.
2. Call `invalidate()` at end of `insert()` and `delete()`.
3. Document: after mutation, caller must call `find()`, `first()`, or `last()` to re-establish position.

### Tests

- `test_cursor_invalidated_after_insert` — insert then next() doesn't panic
- `test_cursor_refind_after_insert` — insert, find(), navigate correctly
- `test_cursor_invalidated_after_delete` — delete, verify unpositioned

---

## Verification

For each item:
- [ ] Tests written first (TDD)
- [ ] All tests pass: `cargo test --bin database`
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning`
