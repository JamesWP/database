# Phase D3 — Cursor Stability

Phase D3 adds SQLite-style cursor save/restore so that cursors survive B-tree mutations. Phase D2 fixed DELETE/UPDATE correctness via collect-then-mutate (separating reads from writes), but cursors themselves are still fragile — `invalidate()` destroys all position state. This phase makes cursors generally robust, which is needed for future features like subqueries, triggers, and correlated scans where multiple cursors may be open on the same tree.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 27 | 5.4 | CursorPosition state machine | D2 item 25 |
| 28 | 5.4 | Save/restore on mutation | 27 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

## Background

### Why this matters after D2

D2 adopts collect-then-mutate for DELETE/UPDATE, so the *scanning* cursor never sees mutations. But other scenarios still need cursor stability:

- **Subqueries**: `DELETE FROM t WHERE id IN (SELECT ...)` — inner query opens a second cursor on the same or related tree
- **Triggers**: BEFORE/AFTER triggers may read from the table being mutated
- **Correlated scans**: Future joins or nested loops with mutations
- **General robustness**: Any `insert()` or `delete_current()` call currently leaves the cursor in a broken state; callers must know to reposition manually

### How SQLite does it

SQLite's `BtCursor` has a state machine: `CURSOR_VALID → CURSOR_REQUIRESEEK → CURSOR_VALID`.

Before any B-tree mutation, all *other* cursors on the same tree save their position (key copied to `pCur->nKey`). State transitions to `CURSOR_REQUIRESEEK`. On next use, `restoreCursorPosition()` calls `btreeMoveto()` to seek back to the saved key. The fast path (no mutation) stays `CURSOR_VALID` with zero overhead.

---

## 27. CursorPosition State Machine (Track 5.4)

### What Changes

Replace the flat `(stack, leaf_iterator)` cursor fields with a `CursorPosition` enum that explicitly models the cursor's lifecycle states.

### Key Files

- `src/storage/btree.rs` — `CursorState`, all cursor navigation methods

### Design

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
enum CursorPosition {
    /// Initial state, or after an operation that leaves cursor unpositioned
    Unpositioned,

    /// Cursor is at a valid leaf cell — fast path, use indices directly
    Valid {
        stack: Vec<InteriorNodeIterator>,
        leaf: LeafNodeIterator,  // (page_idx, cell_index)
    },

    /// Position was invalidated by a mutation — lazy seek on next use
    RequiresSeek {
        saved_key: u64,
    },

    /// Iterated past the last key
    AtEnd,
}

pub struct CursorState {
    root_page: u32,
    position: CursorPosition,
}
```

### Why an enum

The current design uses `Option<LeafNodeIterator>` to conflate three distinct states: "not yet positioned", "positioned", and "past end". Adding `RequiresSeek` as a fourth state makes the flat field approach untenable. An enum makes each state explicit and prevents illegal transitions at the type level.

### Implementation Steps

1. Define `CursorPosition` enum with `Unpositioned`, `Valid`, `AtEnd` variants (no `RequiresSeek` yet — that's item 28)
2. Replace `stack` and `leaf_iterator` fields in `CursorState` with `position: CursorPosition`
3. Update `first()`: descend to leftmost leaf, set `Valid { stack, leaf }`; if tree empty, set `AtEnd`
4. Update `last()`: descend to rightmost leaf, set `Valid`; if tree empty, set `Unpositioned`
5. Update `find()`: descend from root, set `Valid` if found; position at insertion point if not
6. Update `next()`: match on position — if `Valid`, advance within leaf or ascend stack; if reaches end, set `AtEnd`; if `Unpositioned` or `AtEnd`, no-op
7. Update `prev()`: mirror of `next()`
8. Update `get_entry()`: if `Valid`, read from leaf indices; otherwise return `None`
9. Update `delete_current()`: read from `Valid`, perform deletion, set `Unpositioned` (item 28 will change this to `RequiresSeek`)
10. Remove `invalidate()` method — replaced by setting `position = Unpositioned`

### Tests

This is a refactor — all existing tests must continue to pass with no behavior change:
- `cargo test` — full suite
- Specifically verify: cursor navigation tests, DELETE/UPDATE SQL tests, B-tree unit tests
- Add `test_cursor_position_states` — verify state transitions: `Unpositioned → first() → Valid → next() past end → AtEnd`

---

## 28. Save/Restore on Mutation (Track 5.4)

### What Changes

Add the `RequiresSeek` state and `ensure_positioned()` lazy-seek helper, so cursors automatically recover after B-tree mutations.

### Key Files

- `src/storage/btree.rs` — `CursorPosition::RequiresSeek`, `ensure_positioned()`, `delete_current()`, `insert()`

### Implementation Approach

**Add `RequiresSeek` variant** (already defined in item 27's enum, just not used yet).

**Add `ensure_positioned()` helper:**
```rust
fn ensure_positioned(&mut self) {
    if let CursorPosition::RequiresSeek { saved_key } = self.cursor_state.position {
        self.find(saved_key);
        // find() sets position to Valid (or positions at nearest key)
    }
}
```

**Update mutation methods:**

1. **`delete_current()`**: Before deleting, read the current key. After deleting, set `RequiresSeek { saved_key }`. On next use, `find(saved_key)` positions at the next key >= saved_key (since saved_key was deleted), which is the correct successor — no row skipped.

2. **`insert()`**: Before inserting, read the current key (if positioned). After inserting, set `RequiresSeek { saved_key }`. Re-enable the currently commented-out invalidation in `insert()` — but as `RequiresSeek` instead of the old `invalidate()`.

**Update navigation to call `ensure_positioned()`:**

3. **`next()`**: Call `ensure_positioned()` first, then advance as normal.
4. **`prev()`**: Call `ensure_positioned()` first, then retreat as normal.
5. **`get_entry()`**: Call `ensure_positioned()` first, then read.

**Key semantics after delete:**
- Delete key K → `RequiresSeek { saved_key: K }`
- `next()` calls `ensure_positioned()` → `find(K)` → K is gone, cursor lands on next key >= K
- This is the correct position — the successor of the deleted key
- No row skipped, no row revisited

**Key semantics after insert:**
- Insert at key K → `RequiresSeek { saved_key: K }`
- `next()` → `find(K)` → lands on K → advance to K+1
- Works correctly even if the insert caused a page split

### Performance

| Operation | Before (item 27) | After (item 28) |
|-----------|-------------------|------------------|
| `next()` (no mutation) | O(1) | O(1) — no change, `Valid` path unchanged |
| `next()` (after mutation) | broken (Unpositioned → no-op) | O(log n) via find() — correct |
| `get_entry()` (after mutation) | None | O(log n) via find() — correct |
| Full read-only scan | O(n) | O(n) — no change |

### Implementation Steps

1. Add `ensure_positioned()` method to `Cursor`
2. Update `next()`, `prev()`, `get_entry()` to call `ensure_positioned()` first
3. Update `delete_current()`: save key, perform delete, set `RequiresSeek { saved_key }`
4. Update `insert()`: if currently `Valid`, save key, perform insert, set `RequiresSeek { saved_key }`; remove the TODO comment and commented-out invalidation
5. Verify D2's collect-then-mutate still works (it should — phase 2 uses `Find` to position before each mutation, so `RequiresSeek` from a previous delete in the loop is immediately resolved by the next `Find`)

### Tests

- `test_cursor_next_after_delete` — delete current key, verify `next()` lands on correct successor
- `test_cursor_next_after_delete_last` — delete the last key, verify `next()` reaches `AtEnd`
- `test_cursor_next_after_insert` — insert during scan, verify iteration continues correctly
- `test_cursor_survives_split` — insert enough to trigger page split, verify all keys still visited
- `test_cursor_delete_all_forward` — delete every key via `first()` + loop of `delete_current()` + `next()`
- `test_cursor_get_entry_after_mutation` — verify `get_entry()` works after insert/delete
- `cargo test` — full suite, no regressions

---

## Suggested Ordering

```
27 → 28
```

Item 27 is a pure refactor (enum replaces flat fields, no behavior change). Item 28 adds the new `RequiresSeek` behavior on top.

## Verification

For each item:
- [ ] Tests written first (TDD where applicable)
- [ ] All tests pass: `cargo test`
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Git commit with clear message per CLAUDE.md workflow
