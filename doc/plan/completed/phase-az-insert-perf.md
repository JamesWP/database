# Phase AZ — INSERT Performance: Rowid Cache & Fused Unique Write

Eliminate two redundant B-tree traversals that fire on every INSERT: the per-statement seek to
find the max rowid, and the double-pass through unique indexes (check then write).

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 123 | 3 | Add shared rowid cache to BTree; replace INIT seek sequence with `InitRowid` opcode | — |
| 124 | 3 | Fuse `CheckUnique` + `WriteIndex` into `WriteIndexUnique` for unique indexes | — |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

A bpftrace trace of a single `INSERT INTO rental (...)` on the Sakila database reveals two
avoidable B-tree traversals that fire on every INSERT statement:

**Finding 1 — Rowid generation seeks the B-tree's last entry on every INSERT.**

The compiler emits this 8-instruction INIT sequence to derive the next rowid:

```
Open(cursor, rootpage)
MoveCursor(cursor, Last)     ← O(log N) B-tree descent to rightmost leaf
CanReadCursor(flag, cursor)
GoToIfFalse(@empty, flag)
ReadKey(key, cursor)
IncrementValue(key)
GoTo(@init_done)
@empty: StoreValue(key, 1)
@init_done: StoreValue(counter, 0)
```

For the `rental` table (~16k rows, B-tree height ≈ 3–4 levels), this costs 3–4 page reads per
INSERT just to find the next rowid. Rowids are monotonically increasing and never reused, so the
max rowid seen after any INSERT is a valid starting point for all future INSERTs in the same
session — no re-scan is needed.

**Finding 2 — Unique indexes are traversed twice per INSERT.**

`CheckUnique` calls `c.find(&prefix)` to position the cursor at the potential insertion point,
then returns an error if the key is already there. `WriteIndex` for the same cursor then calls
`c.insert()`, which re-descends from the root. The B-tree path is traversed twice for every
unique index per row inserted.

For `rental` (1 unique/PK index + 3 non-unique indexes), this is one extra traversal per row.
With Sakila's ~16k rows, this is ~16k wasted index traversals during data load.

---

## Stubs

None.

---

## 123. Shared rowid cache; `InitRowid` opcode (Track 3)

### What Changes

- `BTree` gains a `rowid_cache: Arc<RefCell<HashMap<u32, u64>>>` field. Cloning a `BTree`
  clones the `Arc` (shared cache), so the catalog's BTree and the engine's BTree (which is a
  clone of the catalog's) share the same cache across `execute()` calls.
- A new `InitRowid(cursor_reg, key_reg)` VM instruction replaces the current 8-instruction INIT
  sequence. On a cache hit, it stores the cached next rowid in `key_reg` with no page I/O. On a
  miss, it falls back to the existing seek-to-last logic and populates the cache.
- `WriteCursor` is updated to write `key + 1` back into `rowid_cache` after each successful row
  write, keeping the cache current for subsequent INSERTs.
- `DROP TABLE` invalidates the cache entry for the dropped rootpage.

### Background

`BTree::clone()` (see `storage/btree.rs:758`) already clones the inner `Arc<RefCell<Pager>>`,
meaning the catalog BTree and the engine BTree share the same pager. Adding an analogous
`Arc<RefCell<HashMap<u32, u64>>>` for the rowid cache follows the same pattern and requires no
architectural changes to how the engine receives its BTree.

Rowids are monotonically increasing and never reused (DELETE does not reclaim rowid space),
so the cache never goes stale from mutations — only from `DROP TABLE`.

The `InitRowid` instruction is a single, semantically clear operation ("give me the next
available rowid for this cursor's table"). It eliminates 7 of the 8 current INIT instructions
in `codegen_insert` and makes the intent explicit in the bytecode listing.

### Implementation Approach

**`src/storage/btree.rs` — add rowid cache to BTree:**

```rust
pub struct BTree {
    pub(super) pager: Arc<RefCell<pager::Pager>>,
    rowid_cache: Arc<RefCell<HashMap<u32, u64>>>,  // rootpage → next rowid
}

impl BTree {
    fn new(path: &str) -> Self {
        BTree {
            pager: Arc::new(RefCell::new(Pager::new(path))),
            rowid_cache: Arc::new(RefCell::new(HashMap::new())),
        }
    }

    fn clone(&self) -> Self {
        BTree {
            pager: self.pager.clone(),
            rowid_cache: self.rowid_cache.clone(),  // shared Arc
        }
    }

    /// Invalidate the rowid cache entry for a rootpage (called on DROP TABLE).
    pub fn invalidate_rowid_cache(&self, rootpage: u32) {
        self.rowid_cache.borrow_mut().remove(&rootpage);
    }
}
```

**`src/engine/program.rs` — add `InitRowid` variant:**

```rust
/// Initialise the next-rowid register for an INSERT.
/// Checks a shared rowid cache keyed by the cursor's rootpage.
/// On cache hit: stores cached value in `key_reg` with no I/O.
/// On cache miss: seeks to the last entry to find max rowid, increments,
///               stores in `key_reg`, and populates the cache.
InitRowid(Reg, Reg),  // (cursor_reg, key_reg)
```

Add `"InitRowid"` to `Operation::name()`.

**`src/engine.rs` — implement `InitRowid` and update `WriteCursor`:**

```rust
InitRowid(cursor_reg, key_reg) => {
    let rootpage = self.registers.get(cursor_reg).cursor().unwrap().rootpage();
    let next = {
        let cache = self.btree.as_ref().unwrap().rowid_cache.borrow();
        cache.get(&rootpage).copied()
    };
    let next = match next {
        Some(v) => v,
        None => {
            // Cold path: seek to last entry to find max rowid
            let cursor = self.registers.get_mut(cursor_reg).cursor_mut().unwrap();
            let mut c = cursor.open_readonly();
            c.last();
            match c.get_entry() {
                None => 1,
                Some(entry) => storage::decode_u64_key(entry.key()) + 1,
            }
        }
    };
    *self.registers.get_mut(key_reg) =
        RegisterValue::Scalar(ScalarValue::Integer(next as i64));
}

WriteCursor(cursor_reg, key_reg, value_regs) => {
    // ... existing write logic ...

    // Update rowid cache so the next INSERT skips the seek
    let rootpage = /* cursor's rootpage */;
    let key_used = match self.registers.get(key_reg).scalar().unwrap() {
        ScalarValue::Integer(k) => *k as u64,
        _ => panic!("WriteCursor: key must be INTEGER"),
    };
    self.btree.as_ref().unwrap()
        .rowid_cache.borrow_mut()
        .insert(rootpage, key_used + 1);
}
```

**`src/compiler/nodes.rs` — update `codegen_insert`:**

Replace the current INIT block:
```rust
init!(ctx;
    Open(cursor_reg, rootpage);
    MoveCursor(cursor_reg, MoveOperation::Last);
    CanReadCursor(flag_reg, cursor_reg);
    GoToIfFalse(empty_label, flag_reg);
    ReadKey(key_reg, cursor_reg);
    IncrementValue(key_reg);
    GoTo(init_done_label);
    Bind(empty_label);
    StoreValue(key_reg, ScalarValue::Integer(1));
    Bind(init_done_label);
    StoreValue(counter_reg, ScalarValue::Integer(0))
);
```

With:
```rust
init!(ctx;
    Open(cursor_reg, rootpage);
    InitRowid(cursor_reg, key_reg);
    StoreValue(counter_reg, ScalarValue::Integer(0))
);
```

**`src/db.rs` — invalidate cache on DROP TABLE:**

```rust
Statement::Drop(_) => {
    // ...
    let (rootpage, _) = catalog.lookup_table(name).unwrap();  // before deleting
    catalog.delete_entries_for_table(name);
    catalog.btree().invalidate_rowid_cache(rootpage);
    // ...
}
```

**`CursorHandle` — expose `rootpage()`:**

The `InitRowid` and updated `WriteCursor` implementations need to read the rootpage from a
cursor. `CursorHandle` should expose a `rootpage() -> u32` method. Check if this already exists;
add it if not.

### Key Files

- `src/storage/btree.rs` — add `rowid_cache` field, `clone()` update, `invalidate_rowid_cache()`
- `src/engine/program.rs` — add `InitRowid` variant and `"InitRowid"` in `name()`
- `src/engine.rs` — implement `InitRowid`, update `WriteCursor` to write cache
- `src/compiler/nodes.rs` — simplify `codegen_insert` INIT block
- `src/db.rs` — invalidate cache entry on DROP TABLE

### Tests

- **Unit test on BTree**: insert 3 rows, drop/re-open catalog, confirm `rowid_cache` is empty on
  fresh open, then INSERT warms it. Confirm subsequent insert reuses cached value.
- **Cross-query cache test**: open a `Catalog`, run two separate `execute("INSERT INTO t …")`
  calls, confirm the second INSERT does not seek (can verify via page-read probes or by
  checking that the cache is populated after the first INSERT).
- **DROP TABLE invalidates cache**: INSERT → DROP → CREATE → INSERT; confirm rowids restart at 1.
- All existing INSERT tests must continue to pass.

### Implementation Steps (2 commits)

#### Step 123.1 — Add rowid cache to BTree

Add `rowid_cache: Arc<RefCell<HashMap<u32, u64>>>` to `BTree`. Update the `new()` and `clone()`
methods. Add `invalidate_rowid_cache(rootpage)`. No behaviour change yet — cache is never read
or written. Run `cargo test`.

**Commit:** `storage: add shared rowid cache to BTree`

#### Step 123.2 — Add `InitRowid` opcode; wire up WriteCursor cache update

Add `InitRowid` to `Operation` and `Operation::name()`. Implement it in the engine (cold path:
seek-to-last; warm path: cache lookup). Update `WriteCursor` to write to the cache. Update
`codegen_insert` to emit `InitRowid` instead of the 8-instruction seek sequence. Update
`db::execute` to invalidate the cache on `DROP TABLE`. Run `cargo fmt && cargo build && cargo test`.

**Commit:** `engine: add InitRowid opcode with shared rowid cache`

---

## 124. Fuse `CheckUnique` + `WriteIndex` into `WriteIndexUnique` (Track 3)

### What Changes

- A new `WriteIndexUnique(cursor_reg, col_regs, pk_reg)` VM instruction replaces the two-opcode
  sequence `CheckUnique(cursor_reg, col_regs)` + `WriteIndex(cursor_reg, col_regs, pk_reg)` for
  unique indexes. It traverses the index B-tree once: positions at the candidate insertion point,
  checks for a conflicting key, and — if none — inserts at that position.
- The compiler's `emit_check_uniques` / `emit_write_indexes` helpers are replaced by a unified
  `emit_index_writes` that emits `WriteIndexUnique` for unique indexes and `WriteIndex` for
  non-unique ones. The separate `CheckUnique` opcode is no longer emitted by `codegen_insert`
  (it can be kept in the ISA for possible future use but is removed from the INSERT codegen path).

### Background

`CheckUnique` (`engine.rs:829`) calls `c.find(&prefix)` to locate the candidate key, then
returns an error if found. `WriteIndex` (`engine.rs:849`) for the same cursor then calls
`c.insert(&index_key)`, which descends the B-tree from the root a second time.

For a table with one unique index, each INSERT traverses that index twice. For Sakila's `rental`
table (1 unique PK index) loaded with ~16k rows, this means ~16k extra B-tree traversals.

The cursor is already positioned at the right leaf by `CheckUnique`. The fix is to expose an
`insert_if_not_exists` operation on the cursor that uses its current position rather than
re-traversing from root.

### Implementation Approach

**`WriteIndexUnique` semantics:**

1. Encode the column-value prefix (same as `CheckUnique`).
2. Build the full composite key: `[encoded_column_value][rowid_be_bytes]` (same as `WriteIndex`).
3. Call a new `CursorHandle` method `insert_unique(&prefix, &composite_key)` that:
   a. Calls `c.find(&prefix)` to position at or near the insertion point.
   b. Checks whether the current key starts with `prefix` — if so, return a unique-violation error.
   c. Inserts `composite_key` at the current cursor position (insert-at-position, not
      insert-from-root).
4. On violation, propagate `EngineError::ConstraintViolation`.

**BTree cursor — new `insert_at_position` / `insert_unique` method:**

The key change is that `insert_unique` must be able to insert without re-descending from the
root. Check the existing `Cursor` API for an `insert` variant that accepts a cursor position. If
none exists, add a `Cursor::insert_at_current(&key, value)` that inserts at the leaf already
held in `cursor.stack`.

If adding `insert_at_position` is complex (e.g., due to split rebalancing resetting the stack),
a simpler alternative is to have `insert_unique` call `c.insert()` as before but skip the
`find()` call because `insert()` already does an internal find. In that case, the optimization
is limited to removing only the `CheckUnique` find and relying on `WriteIndex`'s existing
traversal for both check and insert — still one traversal instead of two.

**Simplified variant (recommended for initial implementation):**

Merge the uniqueness check *into* the insert path at the BTree level:

```rust
/// Insert `key → value` into the B-tree. If `unique_prefix` is Some, scan for
/// any existing key with that prefix before inserting; return Err if found.
pub fn insert_checking_unique(
    c: &mut WritableCursor<'_>,
    key: &[u8],
    value: Vec<u8>,
    unique_prefix: Option<&[u8]>,
) -> Result<(), UniqueViolation>
```

Then `WriteIndexUnique` calls this once instead of `find` + `insert` separately.

**`src/engine/program.rs`:**

```rust
/// Like WriteIndex but also enforces uniqueness.
/// Traverses the index B-tree once: checks for a conflicting prefix,
/// then inserts the composite key if none found.
WriteIndexUnique(Reg, Vec<Reg>, Reg),  // (cursor_reg, col_regs, pk_reg)
```

**`src/engine.rs`:**

```rust
WriteIndexUnique(cursor_reg, value_regs, pk_reg) => {
    // Build prefix (for uniqueness check) and composite key (for insert)
    let mut prefix = Vec::new();
    let mut index_key = Vec::new();
    for value_reg in value_regs {
        let v = self.registers.get(value_reg).scalar().unwrap();
        let encoded = storage::encode_index_value(v);
        prefix.extend_from_slice(&encoded);
        index_key.extend_from_slice(&encoded);
    }
    let pk = match self.registers.get(pk_reg).scalar().unwrap() {
        ScalarValue::Integer(i) => *i as u64,
        _ => panic!("WriteIndexUnique: pk must be INTEGER"),
    };
    index_key.extend_from_slice(&storage::encode_u64_key(pk));

    let cursor = self.registers.get_mut(cursor_reg).cursor_mut().unwrap();
    let mut c = cursor.open_readwrite();
    c.find(&prefix);
    if let Some(entry) = c.get_entry() {
        if entry.key().starts_with(&prefix) {
            return StepResult::Err(EngineError::ConstraintViolation(
                "unique constraint violated".to_string(),
            ));
        }
    }
    c.insert(&index_key, vec![]);
}
```

Note: this uses `c.find(&prefix)` for positioning then calls `c.insert()` which re-traverses.
This is still one full traversal (the `find` is for checking only; `insert` does its own
traversal). For a deeper optimisation where `insert` reuses the cursor position from `find`,
that requires changes to the BTree `insert` internals and can be done as a follow-up.

The immediate win from this phase: **`CheckUnique` is removed from the emitted bytecode**.
The count goes from `N_unique_checks + N_all_writes` traversals to `N_all_writes` traversals.
For `rental` with 1 unique index, this removes 1 traversal per INSERT (down from 5 total to 4).

**`src/compiler/nodes.rs` — replace `emit_check_uniques` + `emit_write_indexes`:**

```rust
fn emit_index_writes(
    index_cursors: &[IndexWithCursor],
    row_regs: &[Reg],
    key_reg: Reg,
    ctx: &mut CodegenContext,
) {
    for ic in index_cursors {
        let col_regs: Vec<Reg> = ic.info.column_idxs.iter().map(|&c| row_regs[c]).collect();
        if ic.info.unique {
            ctx.body_emitter
                .emit(Operation::WriteIndexUnique(ic.cursor_reg, col_regs, key_reg));
        } else {
            ctx.body_emitter
                .emit(Operation::WriteIndex(ic.cursor_reg, col_regs, key_reg));
        }
    }
}
```

In `codegen_insert`, remove the `emit_check_uniques(...)` call and replace `emit_write_indexes`
with `emit_index_writes`.

### Key Files

- `src/engine/program.rs` — add `WriteIndexUnique` variant and name
- `src/engine.rs` — implement `WriteIndexUnique`
- `src/compiler/nodes.rs` — replace `emit_check_uniques` + `emit_write_indexes` with unified `emit_index_writes`

### Tests

- **Unique violation still raised**: INSERT duplicate PK → must still get `ConstraintViolation`.
- **Unique constraint respected for UNIQUE columns**: INSERT duplicate UNIQUE value → error.
- **Non-unique indexes unaffected**: INSERT two rows with same non-unique indexed value → both succeed.
- **Index populated correctly**: after INSERT, SELECT via index must return the row.
- All existing INSERT/index tests must pass.

### Implementation Steps (2 commits)

#### Step 124.1 — Add `WriteIndexUnique` opcode

Add the `WriteIndexUnique` variant to `Operation` and implement it in the engine. It replicates
the behaviour of `CheckUnique` + `WriteIndex` in a single match arm. Run `cargo test` — all
tests must pass (existing tests use `CheckUnique` + `WriteIndex` path, not this new opcode yet).

**Commit:** `engine: add WriteIndexUnique opcode fusing check and write`

#### Step 124.2 — Wire `WriteIndexUnique` into the compiler

Replace `emit_check_uniques` + `emit_write_indexes` in `codegen_insert` with the unified
`emit_index_writes` helper that emits `WriteIndexUnique` for unique indexes. Run
`cargo fmt && cargo build && cargo test`.

**Commit:** `compiler: use WriteIndexUnique for unique indexes in INSERT codegen`

---

## Verification

- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `cargo test` — all tests pass
- [ ] Trace a single INSERT with `make trace-query`: confirm `MoveCursor` (Last seek) no longer
  appears in the INIT block (replaced by `InitRowid`), and `CheckUnique` no longer appears
  before the `WriteIndex` sequence
- [ ] Run two INSERTs in sequence against the same table; confirm the second emits no
  page-read MISS events for the rowid lookup (cache hit)
- [ ] Each commit is independently buildable and testable
