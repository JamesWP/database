# Phase D2 — Refinements

Phase D2 completes deferred work from Phase C and Phase D. It fixes multi-row DELETE/UPDATE by adopting SQLite's collect-then-mutate pattern, then cleans up cursor invalidation. Also adds expression functions (LENGTH, UPPER, LOWER, ABS).

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 24 | 5.4 | Collect-then-mutate bytecode infrastructure | — |
| 25 | 5.4 | Rewrite DELETE/UPDATE codegen and clean up cursor invalidation | 24 |
| 26 | 4.1 | Expression functions (LENGTH, UPPER, LOWER, ABS) | — |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

## 24. Collect-Then-Mutate Bytecode Infrastructure (Track 5.4)

### What Changes

Add bytecode operations for collecting keys during a scan phase and replaying them during a mutation phase. This is the foundation for fixing DELETE and UPDATE to use SQLite's proven collect-then-mutate pattern.

### Current Problem

DELETE and UPDATE currently mutate the B-tree during cursor iteration. After `DeleteCursor` calls `invalidate()` (clears `leaf_iterator` and `stack`), `next()` becomes a no-op and the loop terminates after processing only the first matching row.

**Evidence**: `DELETE FROM temp` on 3 rows deletes only 1 — confirmed in `tests/sql/delete.expected`.

### How SQLite Solves This

SQLite uses a **two-phase collect-then-mutate** pattern for DELETE and UPDATE:
- Phase 1: Scan with a read cursor, collect matching rowids into a `RowSet`
- Phase 2: Iterate the `RowSet`, seek to each rowid, delete/update

SQLite has dedicated opcodes for this: `OP_RowSetAdd` (add rowid to set) and `OP_RowSetRead` (read next rowid, jump if empty).

The separate cursor save/restore mechanism (`CURSOR_REQUIRESEEK`) exists for *other* cursors that happen to be open on the same tree (subqueries, triggers) — not for the scanning cursor itself.

PostgreSQL avoids the problem entirely via Lehman-Yao B-link trees and MVCC — too complex for our embedded use case.

### Key Files

- `src/engine/program.rs` — `Operation` enum, `MoveOperation` enum
- `src/engine/registers.rs` — `RegisterValue` enum
- `src/engine.rs` — operation execution
- `src/storage/btree.rs` — need `Find` cursor movement

### Implementation Approach

**New register type:**
```rust
pub enum RegisterValue {
    None,
    ScalarValue(ScalarValue),
    CursorHandle(CursorHandle),
    KeyList(Vec<u64>),   // NEW: collected keys for two-phase mutations
}
```

**New bytecode operations:**
```rust
// Initialize empty key list in register
InitKeyList(Reg),

// Append key from src_reg to list in list_reg
AppendKey(/*list*/ Reg, /*key*/ Reg),

// Pop next key into dest_reg. If list is empty, jump to target.
PopKey(/*dest*/ Reg, /*list*/ Reg, JumpTarget),
```

**New cursor movement:**
```rust
pub enum MoveOperation {
    First,
    Next,
    Last,
    Find(Reg),  // NEW: seek to key in register
}
```

The `Find(key_reg)` movement positions the cursor at the given key using `cursor.find(key)`. This is generally useful beyond just collect-then-mutate.

### Implementation Steps

1. Add `KeyList(Vec<u64>)` variant to `RegisterValue`
2. Add `InitKeyList`, `AppendKey`, `PopKey` to `Operation` enum
3. Add `Find(Reg)` to `MoveOperation`
4. Implement execution for each new operation in `engine.rs`:
   - `InitKeyList(reg)`: set register to `KeyList(Vec::new())`
   - `AppendKey(list, key)`: push key (as u64) onto the list
   - `PopKey(dest, list, jump)`: pop from list into dest, or jump if empty
   - `MoveCursor(cursor, Find(key_reg))`: call `cursor.find(key)` to position
5. Add register accessor helpers: `key_list_mut()`, etc.

### Tests

Unit tests in `engine.rs`:
- `test_key_list_append_and_pop` — init, append 3 keys, pop all 3 in order, verify jump on empty
- `test_move_cursor_find` — open cursor, insert keys, use `Find` to seek to specific key
- `test_collect_then_delete_pattern` — hand-written bytecode: scan → collect → seek → delete, verify all rows removed

---

## 25. Rewrite DELETE/UPDATE Codegen and Clean Up Invalidation (Track 5.4)

### What Changes

Rewrite `codegen_delete` and `codegen_update` to use the collect-then-mutate pattern from item 24. Then remove cursor invalidation since mutations no longer happen during scans.

### Key Files

- `src/compiler/nodes.rs` — `codegen_delete()`, `codegen_update()`
- `src/storage/btree.rs` — remove `invalidate()`, remove TODO comments
- `tests/sql/delete.sql` / `delete.expected` — fix expected output

### Implementation Approach

**DELETE codegen (two-phase):**
```
INIT:
  Open(cursor, rootpage)
  MoveCursor(cursor, First)
  InitKeyList(key_list)
  StoreValue(counter, 0)

PHASE 1 — COLLECT:
  collect_start:
    CanReadCursor(flag, cursor)
    GoToIfFalse(phase2, flag)
    ReadCursor(read_regs, cursor)     // read for filter
    [evaluate filter if present]
    ReadKey(key_reg, cursor)
    AppendKey(key_list, key_reg)
    IncrementValue(counter)
    MoveCursor(cursor, Next)
    GoTo(collect_start)

PHASE 2 — DELETE:
  phase2:
    PopKey(key_reg, key_list, done)
    MoveCursor(cursor, Find(key_reg))
    DeleteCursor(cursor)
    GoTo(phase2)

  done:
    Yield(counter)
```

**UPDATE codegen (two-phase):**
```
INIT:
  Open(cursor, rootpage)
  MoveCursor(cursor, First)
  InitKeyList(key_list)
  StoreValue(counter, 0)

PHASE 1 — COLLECT:
  collect_start:
    CanReadCursor(flag, cursor)
    GoToIfFalse(phase2, flag)
    ReadCursor(read_regs, cursor)     // read for filter
    [evaluate filter if present]
    ReadKey(key_reg, cursor)
    AppendKey(key_list, key_reg)
    IncrementValue(counter)
    MoveCursor(cursor, Next)
    GoTo(collect_start)

PHASE 2 — UPDATE:
  phase2:
    PopKey(key_reg, key_list, done)
    MoveCursor(cursor, Find(key_reg))
    ReadCursor(read_regs, cursor)     // re-read current values
    [compute new values from assignments]
    WriteCursor(cursor, key_reg, new_values)
    GoTo(phase2)

  done:
    Yield(counter)
```

Note: UPDATE re-reads the row in phase 2 after seeking. This is slightly redundant (values were already read in phase 1 for filtering) but keeps the implementation simple and correct. The row won't have changed between phases in our single-threaded model.

**Clean up cursor invalidation:**
1. Remove `CursorState::invalidate()` method — no longer called from anywhere
2. Remove the commented-out `self.cursor_state.invalidate()` and TODO in `insert()`
3. Remove the `invalidate()` call in `delete_current()` — the scanning cursor has already moved past this position before deletion happens in phase 2
4. Add doc comments noting the two-phase mutation pattern

### Tests

SQL integration tests — fix existing + add new:
- Fix `tests/sql/delete.expected`: `DELETE FROM temp` (3 rows) should show `3` deleted, empty SELECT
- Add to `tests/sql/delete.sql`:
  - `DELETE FROM table WHERE condition` matching multiple rows
  - `DELETE FROM table` followed by INSERT to verify table is reusable
- Add to `tests/sql/update.sql` (or verify existing):
  - `UPDATE table SET col = val` affecting all rows
  - `UPDATE table SET col = col + 1 WHERE condition` affecting multiple rows

Unit tests:
- `test_codegen_delete_bytecode` — verify two-phase pattern in emitted bytecode
- Run full `cargo test` to verify no regressions

---

## 26. Expression Functions (Track 4.1)

### What Changes

Add LENGTH(), UPPER(), LOWER(), ABS() scalar functions.

Deferred from Phase C item 16.

### Key Files

- `src/frontend/parser.rs` — recognize `identifier(expr)` as function call
- `src/frontend/ast.rs` — add `Expression::FunctionCall { name, args }`
- `src/planner.rs` — add `PlanExpr::FunctionCall`, validate function names
- `src/compiler/expr.rs` — compile function calls to bytecode
- `src/engine/program.rs` — add function call operations
- `src/engine/scalarvalue.rs` — implement length(), to_uppercase(), to_lowercase(), abs()

### Implementation Approach

1. **AST**: Add `Expression::FunctionCall { name: String, args: Vec<Expression> }`
2. **Parser**: When identifier followed by `(`, parse as function call
   - Parse argument list: `func(arg1, arg2, ...)`
   - For now, all functions take exactly 1 argument
3. **Planner**: Add `PlanExpr::FunctionCall { name: String, args: Vec<PlanExpr> }`
   - Validate function name is one of: LENGTH, UPPER, LOWER, ABS (case-insensitive)
   - Validate argument count (all take 1 arg for v1)
4. **Compiler**: Emit function-specific operations
   - Add: LengthValue(dest, arg), UpperValue(dest, arg), LowerValue(dest, arg), AbsValue(dest, arg)
   - Each operation: read arg register, compute result, store in dest register
5. **Engine**: Implement operations
   - LENGTH(s) → integer length of string, NULL for NULL
   - UPPER(s) → uppercase string, NULL for NULL
   - LOWER(s) → lowercase string, NULL for NULL
   - ABS(n) → absolute value of number, NULL for NULL
   - Type checking: LENGTH/UPPER/LOWER require string, ABS requires number
   - For type mismatches, either error or coerce (e.g., LENGTH(42) → "42".len())

### Tests

SQL integration tests (`tests/sql/functions.sql`):
- `SELECT LENGTH('hello')` → 5
- `SELECT UPPER('hello')` → "HELLO"
- `SELECT LOWER('HELLO')` → "hello"
- `SELECT ABS(-42)` → 42
- `SELECT ABS(-3.14)` → 3.14
- `SELECT LENGTH(name) FROM users` — works with column references
- `SELECT UPPER(name) FROM users WHERE LENGTH(name) > 5` — composition
- `SELECT LENGTH(NULL)` → NULL
- Error cases:
  - Unknown function: `SELECT FOO(1)`
  - Wrong argument count: `SELECT LENGTH(1, 2)` or `SELECT UPPER()`

Unit tests:
- Parser: test function call parsing
- Planner: test function validation
- ScalarValue: test each function implementation with NULL handling

---

## Suggested Ordering

```
24 → 25 → 26
```

Items 24 and 25 must be sequential (25 depends on 24).
Item 26 is independent and could be done in parallel or after.

Recommended: Do 24→25→26 sequentially for clear progression.

## Verification

For each item:
- [ ] Tests written first (TDD where applicable)
- [ ] All tests pass: `cargo test`
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Git commit with clear message per CLAUDE.md workflow
