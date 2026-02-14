# Phase D2 — Refinements

Phase D2 completes deferred work from Phase C and Phase D. It eliminates cursor invalidation by making cursors robust to mutations, allowing UPDATE and DELETE to work naively without collect-then-update workarounds. Also adds expression functions (LENGTH, UPPER, LOWER, ABS).

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 24 | 5.4 | Key-based cursor positioning | — |
| 25 | 5.4 | Remove cursor invalidation and collect workarounds | 24 |
| 26 | 4.1 | Expression functions (LENGTH, UPPER, LOWER, ABS) | — |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

## 24. Key-Based Cursor Positioning (Track 5.4)

### What Changes

Redesign cursor navigation to use key-based positioning instead of page/cell indices, making cursors robust to structural changes during iteration.

### Current Problem

Cursor navigation uses `leaf_iterator: Option<(page_idx, cell_index)>` which becomes invalid when:
- Insert causes page split (page indices change)
- Delete removes cells (cell indices shift)
- This forces collect-then-update patterns in DELETE and UPDATE

### Key Files

- `src/storage/btree.rs` — CursorState, navigation methods (first, next, prev, find)
- `src/storage/node.rs` — may need key-at-index helpers

### Implementation Approach

**Core idea**: Instead of storing (page, cell_index), store the current key and use find() to reposition.

**Option A: Store current key**
```rust
pub struct CursorState {
    root_page: u32,
    stack: Vec<InteriorNodeIterator>,
    // OLD: leaf_iterator: Option<(u32, usize)>,
    // NEW: current_key: Option<u64>,
    current_key: Option<u64>,
    at_end: bool,  // Distinguish "after last key" from "no position"
}
```

**Navigation changes**:

1. **first()**: Set `current_key = None`, find first key in tree, set `current_key = Some(first_key)`

2. **next()**:
   - If `current_key.is_none()`, no-op
   - Find current_key to position cursor
   - Read next cell in leaf; if exists, update current_key
   - If no next cell in leaf, scan up tree to find next interior node, descend to next leaf
   - If no next key exists, set `at_end = true`

3. **prev()**: Similar to next() but in reverse

4. **find(key)**: Set `current_key = Some(key)` if found, perform find operation

5. **get_entry()**:
   - If `current_key.is_none()` or `at_end`, return None
   - Use find(current_key) to position, read entry

**Key insight**: Every cursor operation (except first/last) internally does find() to establish position. This is less efficient but makes cursors immune to mutations.

**Optimization**: Cache the page/cell position as a hint:
```rust
pub struct CursorState {
    root_page: u32,
    stack: Vec<InteriorNodeIterator>,
    current_key: Option<u64>,
    at_end: bool,
    // Hint: last known position (may be stale)
    position_hint: Option<(u32, usize)>,
}
```

Before doing find(), check if hint is still valid:
- Read page at position_hint
- Check if cell at index still has current_key
- If yes, use it; if no, fall back to find()

This gives O(1) when no mutations, O(log n) after mutations.

**Option B: Iterator-style with key tracking**
Store (current_key, direction) and implement stateless iteration:
- Each next() call: find(current_key) + advance
- More expensive but maximally robust

**Recommended**: Option A with position hint for good balance.

### Implementation Steps

1. Add `current_key` and `at_end` fields to CursorState
2. Update `first()` to set current_key to first key in tree
3. Update `next()`:
   - Use find(current_key) to position
   - Advance to next key
   - Update current_key
4. Update `prev()` similarly
5. Update `get_entry()` to use find(current_key) if needed
6. Remove page/cell index from CursorState (or keep as hint)
7. Remove invalidate() calls (no longer needed)

### Tests

- `test_cursor_survives_insert_during_scan` — insert while iterating, verify iteration continues correctly
- `test_cursor_survives_delete_during_scan` — delete while iterating, verify iteration continues
- `test_cursor_survives_split` — trigger page split during iteration
- `test_cursor_next_after_delete_current` — delete current position, next() still works
- Performance: benchmark before/after to measure overhead

---

## 25. Remove Cursor Invalidation and Collect Workarounds (Track 5.4)

### What Changes

Remove cursor invalidation logic and simplify DELETE/UPDATE to work naively now that cursors survive mutations.

### Key Files

- `src/storage/btree.rs` — remove invalidate() calls and TODO comments
- `src/compiler/nodes.rs` — simplify codegen_delete to not collect keys first

### Implementation Approach

1. **Remove invalidation**:
   - Delete `CursorState::invalidate()` method (no longer needed)
   - Remove all calls to `invalidate()` from insert() and delete_current()
   - Remove TODO comments about invalidation

2. **Simplify DELETE** (if using collect pattern):
   - Change from collect-then-delete to delete-during-scan
   - Scan loop: read key, check filter, delete_current(), next()
   - Cursor automatically repositions after delete_current()
   - No need to collect keys first

3. **Verify UPDATE** (already works naively):
   - UPDATE already does mutate-during-iteration
   - Should work without changes after item 24
   - Verify test suite passes

4. **Document behavior**:
   - Add doc comments: "Cursors use key-based positioning and survive mutations"
   - Note performance characteristics: find() on each navigation after mutation

### Tests

- Run existing DELETE and UPDATE tests (should all pass)
- Remove cursor invalidation tests or update them to verify NO invalidation
- Add tests:
  - `test_delete_all_via_iteration` — DELETE in a simple loop, no collect
  - `test_update_all_via_iteration` — UPDATE in a simple loop, no collect

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
