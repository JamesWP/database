# Phase AC — Covering Index Optimization

Eliminate redundant table B-tree lookups when all required columns are already encoded in the index key.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 103 | 4.1 | Add `DecodeIndexValue` VM op and `decode_index_value_from_bytes` helper | — |
| 104 | 4.2 | Add `IndexScanOutput` enum; extend `IndexScan` to carry it; update codegen and EXPLAIN | 103 |
| 105 | 4.3 | Optimizer: when `RowidLookup(IndexScan)` is covering, set `IndexScan.output` to `Columns` and remove the `RowidLookup` | 104 |
| 106 | 4.4 | Optimizer + Codegen: skip right table lookup in `IndexJoin` when index covers all right columns | 104 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

Currently, equality and range index scans work in two steps: `IndexScan` yields a rowid, then `RowidLookup` fetches the actual column values from the table B-tree. This second lookup is unnecessary when every column needed by the query is already encoded in the index key itself — a "covering index".

Similarly, `IndexJoin` always opens a right-table cursor and does a rowid lookup for each left row. If all required right-side columns are encoded in the index, the right table read can be skipped.

Rather than adding a separate `IndexOnlyScan` node, this phase extends `IndexScan` with an `output: IndexScanOutput` field — an enum that makes the two modes explicit. This follows the existing `Scan { with_key: bool }` precedent and avoids duplicating cursor/bounds/advance logic in a new variant.

### IndexScanOutput enum

```rust
/// Describes what an IndexScan yields per matching entry.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexScanOutput {
    /// Yield the rowid as a single integer register (current behaviour).
    Rowid,
    /// Decode column values directly from the index key; no table B-tree access.
    /// Each entry is the 0-based position of a desired column within the index key.
    /// Output registers are in the order of this Vec.
    Columns(Vec<usize>),
}
```

`IndexScan` gains one new field:

```rust
IndexScan {
    index_rootpage: u32,
    index_col_idx: usize,
    lower_bound: Option<(Literal, bool)>,
    upper_bound: Option<(Literal, bool)>,
    output: IndexScanOutput,   // NEW — defaults to Rowid
},
```

All existing code that constructs `IndexScan` sets `output: IndexScanOutput::Rowid`, preserving current behaviour. The optimizer sets `output: IndexScanOutput::Columns(positions)` when it detects a covering index.

### Current vs. Optimised Plan

Current plan for `SELECT price FROM products WHERE price = 200` (index on `price`):

```
Project [price:0]
  RowidLookup products [cols: id, sku, price]
    IndexScan via idx_price [= 200]
```

Optimised plan (items 104 + 105):

```
Project [price:0]
  IndexScan via idx_price [= 200, cols: price]
```

Three nodes become one; the table B-tree is never opened.

### Index Key Format (background)

Index keys are encoded as:

```
[tag][col0_bytes] [tag][col1_bytes] ... [8 bytes: rowid_be_u64]
```

Tags:
- `0x00` — NULL (tag only, 1 byte total)
- `0x01` — INTEGER (tag + 8 bytes, sign-flipped big-endian)
- `0x02` — REAL (tag + 8 bytes, sortable IEEE 754)
- `0x03` — TEXT (tag + UTF-8 bytes + `0x00` NUL terminator)

The encoding is self-describing: given a blob starting at a column boundary, the tag byte determines the type and consumed byte count. This is what `DecodeIndexValue` exploits.

---

## 103. DecodeIndexValue VM Operation (Track 4.1)

### What Changes

- New public function `decode_index_value_from_bytes` in `src/storage/btree.rs`.
- New VM instruction `DecodeIndexValue(dest: Reg, blob: Reg)` in `src/engine/program.rs`.
- Engine execution: decode one tagged column value from the start of `blob`, write it to `dest`, and advance `blob` in-place past the consumed bytes.

### Background

The decoder is the inverse of `encode_index_value`. Extracting it as a standalone helper and VM op ensures the logic is tested independently before it is used in the covering-index codegen paths (items 104 and 106).

### Implementation Approach

`decode_index_value_from_bytes` in `src/storage/btree.rs`:

```rust
/// Decode one index-column value from the start of `bytes`.
/// Returns `(ScalarValue, bytes_consumed)`.
pub fn decode_index_value_from_bytes(bytes: &[u8]) -> (ScalarValue, usize) {
    match bytes[0] {
        0x00 => (ScalarValue::Null, 1),
        0x01 => {
            let mut raw = i64::from_be_bytes(bytes[1..9].try_into().unwrap());
            raw ^= i64::MIN; // undo sign-flip
            (ScalarValue::Integer(raw), 9)
        }
        0x02 => {
            let bits = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
            let bits = if bits >> 63 == 1 {
                bits ^ 0x8000_0000_0000_0000
            } else {
                bits ^ 0xFFFF_FFFF_FFFF_FFFF
            };
            (ScalarValue::Floating(f64::from_bits(bits)), 9)
        }
        0x03 => {
            let end = bytes[1..].iter().position(|&b| b == 0x00).unwrap_or(bytes.len() - 1);
            let s = std::str::from_utf8(&bytes[1..1 + end]).unwrap_or("").to_string();
            (ScalarValue::String(s), 1 + end + 1) // tag + content + NUL
        }
        tag => panic!("decode_index_value_from_bytes: unknown tag 0x{tag:02x}"),
    }
}
```

The `DecodeIndexValue(dest, blob)` VM instruction in `src/engine.rs`:

```rust
Instruction::DecodeIndexValue(dest, blob_reg) => {
    let bytes = registers.get_blob(blob_reg);
    let (value, consumed) = decode_index_value_from_bytes(&bytes);
    registers.set(dest, value.into());
    registers.set(blob_reg, RegisterValue::Blob(bytes[consumed..].to_vec()));
}
```

### Key Files

- `src/storage/btree.rs` — `decode_index_value_from_bytes`
- `src/engine/program.rs` — `DecodeIndexValue(Reg, Reg)` variant
- `src/engine.rs` — instruction execution

### Tests

Unit tests for `decode_index_value_from_bytes` covering all four types, round-tripping with `encode_index_value`, including negative integers, empty string, and Unicode text. Verify `consumed` byte counts.

### Implementation Steps (1 commit)

#### Step 103.1 — Add `decode_index_value_from_bytes` and `DecodeIndexValue` op

**Commit:** `Storage+VM: add decode_index_value_from_bytes and DecodeIndexValue operation`

---

## 104. IndexScanOutput Enum + Extended IndexScan (Track 4.2)

### What Changes

- New `IndexScanOutput` enum in `src/planner/mod.rs`.
- `output: IndexScanOutput` field added to `LogicalPlan::IndexScan`.
- All existing `IndexScan` constructor sites gain `output: IndexScanOutput::Rowid`.
- `codegen_index_scan` branches on `output`: `Rowid` path unchanged; `Columns(positions)` path decodes values from the key blob using `BlobDropLast` + `DecodeIndexValue`.
- `node_output_cols` and `format_plan` in `src/explain.rs` updated.

### Background

This item wires up the new enum and makes the codegen work, but does not yet change which plans the optimizer emits — all existing plans still use `Rowid`. Item 105 adds the optimizer rule that sets `Columns`.

### Implementation Approach

`IndexScanOutput` in `src/planner/mod.rs`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum IndexScanOutput {
    /// Yield the rowid as a single integer register.
    Rowid,
    /// Decode column values directly from the index key.
    /// Each `usize` is the 0-based position of a desired column in the index key.
    Columns(Vec<usize>),
}
```

`codegen_index_scan` — `Columns(positions)` branch (after the bounds/prefix check):

```rust
// Strip the 8-byte trailing rowid from the key blob.
let data_blob = ctx.registers.alloc();
body!(ctx; BlobDropLast(data_blob, key_blob_reg, 8));

// Decode each column in index order; emit only the requested positions.
let max_pos = *positions.iter().max().unwrap();
for pos in 0..=max_pos {
    let tmp = ctx.registers.alloc();
    body!(ctx; DecodeIndexValue(tmp, data_blob));
    if let Some(out_idx) = positions.iter().position(|&p| p == pos) {
        out_regs[out_idx] = tmp;
    }
    // tmp is discarded (blob already advanced in-place) if pos not needed
}
```

EXPLAIN for covering mode:

```
IndexScan via idx_price [= 200, cols: price]
```

`node_output_cols` for `IndexScan`:
- `Rowid` → `vec!["rowid".to_string()]` (unchanged)
- `Columns(positions)` → resolve each position to the index's column name via `ExplainSchema`

### Key Files

- `src/planner/mod.rs` — `IndexScanOutput` enum; `output` field on `IndexScan`
- `src/compiler/nodes.rs` — `codegen_index_scan` branching on `output`
- `src/explain.rs` — `node_output_cols` and `format_plan` for both modes

### Tests

- Compiler unit test: `IndexScan { output: Columns([0]) }` emits `BlobDropLast` + `DecodeIndexValue` instead of `BlobSliceLast` + `DecodeU64Key`.
- `node_output_cols` for `Columns` mode returns index column names.
- All existing tests still pass (all plans still use `Rowid` mode at this point).

### Implementation Steps (1 commit)

#### Step 104.1 — Add `IndexScanOutput` enum, extend `IndexScan`, update codegen and EXPLAIN

**Commit:** `Planner+Compiler: add IndexScanOutput enum and covering codegen path to IndexScan`

---

## 105. Optimizer: Covering RowidLookup Elimination (Track 4.3)

### What Changes

- `index_covers_columns` helper in `src/planner/optimizer.rs`.
- `RowidLookup` arm of `optimize()`: when the input is an `IndexScan` and all `RowidLookup.columns` are in the index, return the `IndexScan` with `output: Columns(positions)` and drop the `RowidLookup`.
- `can_elide_sort` updated to pass through `IndexScan { output: Columns(..) }`.
- New SQL integration test `tests/sql/index_covering.sql`.

### Implementation Approach

`index_covers_columns` checks whether all entries of `required_cols` (physical table column indices) are encoded in a given index:

```rust
/// Returns Some(index_positions) if every column in `required_cols` is
/// encoded in the index at `index_rootpage` on `table_rootpage`.
/// `index_positions[i]` is the 0-based key position of `required_cols[i]`.
fn index_covers_columns(
    index_rootpage: u32,
    table_rootpage: u32,
    required_cols: &[usize],
    btree: &BTree,
) -> Option<Vec<usize>> { ... }
```

`RowidLookup` arm in `optimize()`:

```rust
LogicalPlan::RowidLookup { input, table_rootpage, columns } => {
    let opt_input = optimize(*input, btree);
    if let LogicalPlan::IndexScan { index_rootpage, index_col_idx,
                                    ref lower_bound, ref upper_bound, .. } = opt_input {
        if let Some(positions) = index_covers_columns(
            index_rootpage, table_rootpage, &columns, btree
        ) {
            return LogicalPlan::IndexScan {
                index_rootpage,
                index_col_idx,
                lower_bound: lower_bound.clone(),
                upper_bound: upper_bound.clone(),
                output: IndexScanOutput::Columns(positions),
            };
        }
    }
    LogicalPlan::RowidLookup { input: Box::new(opt_input), table_rootpage, columns }
}
```

### SQL Test Cases (`tests/sql/index_covering.sql`)

```sql
CREATE TABLE products (id INTEGER, sku TEXT, price INTEGER)
-- > Table 'products' created
CREATE INDEX idx_price ON products (price)
-- > Index 'idx_price' created

INSERT INTO products VALUES (1, 'aaa', 100)
-- > 1
INSERT INTO products VALUES (2, 'bbb', 200)
-- > 1
INSERT INTO products VALUES (3, 'ccc', 150)
-- > 1

-- Covering: only the indexed column is needed
EXPLAIN SELECT price FROM products WHERE price = 200
-- > 0, "Project [price:0]"
-- > 1, "  IndexScan via idx_price [= 200, cols: price]"

SELECT price FROM products WHERE price = 200
-- > 200

-- Covering: range scan on indexed column
SELECT price FROM products WHERE price > 100 ORDER BY price
-- > 150
-- > 200

-- Non-covering: id is not in idx_price → RowidLookup stays
EXPLAIN SELECT id FROM products WHERE price = 200
-- > 0, "Project [id:0]"
-- > 1, "  RowidLookup products [cols: id]"
-- > 2, "    IndexScan via idx_price [= 200]"
```

### Key Files

- `src/planner/optimizer.rs` — `index_covers_columns`, updated `RowidLookup` arm, `can_elide_sort`
- `tests/sql/index_covering.sql`

### Tests

- Planner unit test: covering query → `IndexScan { output: Columns(..) }` with no `RowidLookup`.
- Planner unit test: non-covering query → `RowidLookup(IndexScan { output: Rowid })`.

### Implementation Steps (1 commit)

#### Step 105.1 — Optimizer: eliminate RowidLookup when IndexScan covers all needed columns

**Commit:** `Optimizer: eliminate RowidLookup when IndexScan covers all needed columns`

---

## 106. Covering IndexJoin (Track 4.4)

### What Changes

- Add `right_covered: bool` and `right_index_positions: Vec<usize>` fields to `LogicalPlan::IndexJoin`.
- `IndexJoin` arm of `optimize()` calls `index_covers_columns` to populate these fields.
- `codegen_index_join` skips right table cursor and `ReadCursor` when `right_covered` is true; decodes right columns from the index key blob instead.
- Updated EXPLAIN rendering.

### Implementation Approach

New fields on `IndexJoin`:

```rust
IndexJoin {
    left: Box<LogicalPlan>,
    index_rootpage: u32,
    right_table_rootpage: u32,
    right_columns: Vec<usize>,
    left_key_col_idx: usize,
    left_col_count: usize,
    /// True when all right_columns are present in the index key.
    right_covered: bool,
    /// 0-based index key positions for each element of right_columns.
    /// Empty when right_covered is false.
    right_index_positions: Vec<usize>,
},
```

All existing `IndexJoin` constructor sites gain `right_covered: false, right_index_positions: vec![]`.

In `codegen_index_join`, when `right_covered`:
- Skip `Open(right_cursor_reg, right_table_rootpage)` in INIT.
- Replace `MoveCursor(right_cursor, Find(pk_reg)); ReadCursor(right_regs, right_cursor)` with:
  - `BlobDropLast(data_blob, key_blob, 8)` — strip trailing rowid
  - For each position `0..=max_right_pos`: `DecodeIndexValue(tmp_or_out, data_blob)`, emitting to the appropriate `right_regs` slot.

EXPLAIN annotation for a covering `IndexJoin`:

```
IndexJoin users via idx_users_id [left_key=user_id:1, cols: id, name (covering)]
```

### Key Files

- `src/planner/mod.rs` — new fields on `IndexJoin`
- `src/planner/optimizer.rs` — `IndexJoin` arm calls `index_covers_columns`
- `src/compiler/nodes.rs` — `codegen_index_join` branches on `right_covered`
- `src/explain.rs` — `(covering)` annotation when `right_covered`

### Tests

- Planner unit test: join where right columns ⊆ index columns → `right_covered: true`.
- Planner unit test: join where right needs a non-indexed column → `right_covered: false`.
- SQL integration test: EXPLAIN shows `(covering)`; query returns correct results.
- Existing `join_index.sql` passes unchanged (`right_covered: false` since `name` is not indexed).

### Implementation Steps (1 commit)

#### Step 106.1 — Covering IndexJoin: skip right table when index covers all right columns

**Commit:** `Optimizer+Compiler: skip right table lookup in IndexJoin when index covers all right columns`

---

## Verification

- [ ] `cargo test` — all tests pass
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `EXPLAIN SELECT price FROM products WHERE price = 200` → `IndexScan ... cols: price` (no `RowidLookup`)
- [ ] `EXPLAIN SELECT id FROM products WHERE price = 200` → `RowidLookup(IndexScan)` (non-covering)
- [ ] `SELECT price FROM products WHERE price > 100 ORDER BY price` returns correct rows
- [ ] Sort elision still works through `IndexScan { output: Columns(..) }`
- [ ] Covering `IndexJoin` EXPLAIN shows `(covering)` annotation
- [ ] Non-covering `IndexJoin` EXPLAIN unchanged
- [ ] TEXT column covering index works (NUL-terminated decoding)
- [ ] All existing SQL tests pass unchanged
