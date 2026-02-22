# Phase G4 — Advanced Indexing

Builds on phase-g3-indexing.md to add multi-column indexes, TEXT support, range scans, and variable-length keys.

## Items

(all completed)

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Completed Items

| # | Track | Item | Completed |
|---|-------|------|-----------|
| 41 | 3.6 | Variable-length B-tree keys | 2026-02-18 |
| 40 | 4.5 | Range scan optimization (>, >=, <, <=) | 2026-02-21 |
| 45 | 3.7 | Remove rowid from index entry value | 2026-02-22 |
| 46 | 5 | Engine: ReadCurrentKey + blob ops | 2026-02-22 |
| 47 | 4.9 | Split IndexScan into IndexScan + RowidLookup | 2026-02-22 |
| 42 | 4.6 | Multi-column indexes | 2026-02-22 |
| 43 | 4.7 | TEXT column indexes | 2026-02-22 |
| 44 | 4.8 | Type conversions and NULL handling | 2026-02-22 |

---

## Overview

Phase G3 established basic indexing with:
- Single-column INTEGER indexes
- Equality predicates only (`WHERE col = value`)
- Fixed u64 keys

Phase G4 extends this foundation with:
- **Range scans**: Use indexes for `>`, `<`, `>=`, `<=`, `BETWEEN`
- **Variable-length keys**: Change B-tree API to support arbitrary-length keys
- **Multi-column indexes**: `CREATE INDEX idx ON table(col1, col2, col3)`
- **TEXT indexes**: Index string columns with lexicographic ordering
- **Type safety**: Handle NULL values, type mismatches, coercions

---

## 40. Range Scan Optimization (Track 4.5)

### What Changes

Extend index scans to support range predicates: `WHERE age > 25`, `WHERE age BETWEEN 20 AND 30`.

### Background

Currently (G3), indexes only handle equality (`WHERE age = 25`). But the index B-tree maintains sort order, so we can efficiently scan ranges:

```
Index on age:
  key → primary_key
  20 → 5
  25 → 2
  25 → 4
  30 → 1
  30 → 3
  35 → 6

Query: WHERE age > 25
  1. Seek to first key > 25 → finds 30
  2. Scan forward: 30→1, 30→3, 35→6
  3. For each primary_key, fetch row from table
```

### Implementation Approach

**Planner changes:**

Extend `try_plan_index_scan()` to detect range predicates:
- Extract `(column, op, literal)` from BinaryOp nodes
- For `>`, `>=`, `<`, `<=`: plan IndexRangeScan
- For `BETWEEN`: plan IndexRangeScan with lower and upper bounds

Add `LogicalPlan::IndexRangeScan` variant:
```rust
IndexRangeScan {
    index_rootpage: u32,
    lower_bound: Option<(Literal, bool)>,  // (value, inclusive)
    upper_bound: Option<(Literal, bool)>,
    table_rootpage: u32,
    columns: Vec<usize>,
}
```

**Compiler changes:**

Add `codegen_index_range_scan()`:
- If lower_bound exists: `MoveCursor(Find(lower_key))` or `MoveCursor(Next)` if not inclusive
- Loop: check if current key satisfies upper_bound, read row, yield
- Advance cursor, repeat

**Engine changes:**

Add comparison operations for bounds checking:
- `Operation::CompareKeys(result_reg, key_reg1, key_reg2)` → stores -1, 0, or 1

### Key Files

- `src/planner.rs` — detect range predicates, plan IndexRangeScan
- `src/compiler/nodes.rs` — codegen_index_range_scan
- `src/engine/program.rs` — CompareKeys operation
- `src/engine.rs` — execute CompareKeys

### Tests

**SQL tests:**
```sql
CREATE TABLE data (id INTEGER, value INTEGER)
INSERT INTO data VALUES (1, 10), (2, 20), (3, 30), (4, 40)
CREATE INDEX idx_value ON data(value)

-- Greater than
SELECT id FROM data WHERE value > 20
-- Expected: 3, 4

-- Less than or equal
SELECT id FROM data WHERE value <= 30
-- Expected: 1, 2, 3

-- BETWEEN
SELECT id FROM data WHERE value BETWEEN 20 AND 35
-- Expected: 2, 3
```

### Implementation Steps (3 commits)

#### Step 40.1 — Planner: detect range predicates

Add range predicate detection in `try_plan_index_scan()`:
- Match BinaryOp::GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual
- Extract bounds and plan IndexRangeScan

**Commit:** Add IndexRangeScan logical plan node

#### Step 40.2 — Compiler: range scan bytecode

Implement `codegen_index_range_scan()`:
- Seek to lower bound
- Loop with upper bound check
- Yield matching rows

**Commit:** Compile IndexRangeScan to bytecode

#### Step 40.3 — Tests: range scan queries

Add SQL integration tests for `>`, `<`, `>=`, `<=`, `BETWEEN`.

**Commit:** Integration tests for range scans

---

## 41. Variable-Length B-tree Keys (Track 3.6) ✓ Completed 2026-02-18

### What Was Built

Changed B-tree API from fixed `u64` keys to variable-length `Vec<u8>` / `&[u8]` keys throughout the storage layer.

**API:**
```rust
pub fn insert(&mut self, key: &[u8], value: Vec<u8>)
pub fn find(&mut self, key: &[u8]) -> bool
pub fn delete(&mut self, key: &[u8])
```

Legacy `_u64` convenience wrappers remain for rowid-based table access:
```rust
pub fn insert_u64(&mut self, key: u64, value: Value)
pub fn find_u64(&mut self, key: u64) -> bool
pub fn delete_u64(&mut self, key: u64)
```

### Actual Implementation (differs from original plan)

**Cell / Node serialization:**
Keys are stored as `Vec<u8>` in `Cell`, `LeafNodePage`, and `InteriorNodePage`. Serialization uses **CBOR** (not the manual key_len prefix the plan described); CBOR handles variable-length fields naturally.

**Byte comparison:**
Both `LeafNodePage::search()` and `InteriorNodePage::search()` use Rust's `.cmp()` on `&[u8]` slices, giving lexicographic ordering.

**Integer key encoding:**
```rust
pub fn encode_integer_key(i: i64) -> Vec<u8> {
    let encoded = (i as u64) ^ 0x8000_0000_0000_0000; // flip sign bit
    encoded.to_be_bytes().to_vec()
}
```
The sign-bit flip ensures negative integers sort before positive ones under byte comparison (e.g. `-1 < 0 < 1`).

**NULL key encoding:**
Index entries for NULL values use `vec![0u8; 8]` (eight zero bytes), which sorts before any sign-bit-flipped integer. Type-tag prefix (0x00) described in the original plan was not implemented.

**Engine (WriteIndex):**
Only INTEGER and NULL are encoded in `WriteIndex`; TEXT support is deferred to item 43. The composite index key is:
```
[encode_integer_key(column_value)][encode_u64_key(primary_key)]
```

**CellReader:**
`key()` returns `&[u8]` directly from the stored `Vec<u8>`.

### Key Files

- `src/storage/cell.rs` — `Cell { key: Vec<u8>, value: Vec<u8>, continuation }`
- `src/storage/node.rs` — `LeafNodePage` and `InteriorNodePage` with byte-slice search
- `src/storage/btree.rs` — `insert/find/delete(&[u8])`, `encode_integer_key`, `encode_u64_key`
- `src/storage/cell_reader.rs` — `key() -> &[u8]`
- `src/engine.rs` — `WriteIndex` encodes INTEGER/NULL keys

---

## 45. Remove Rowid from Index Entry Value (Track 3.7)

### What Changes

Index B-tree entries currently store the rowid in **both** the key and the value:

```
Key:   [encoded_col_value 8 bytes][encoded_rowid 8 bytes]   ← rowid here for uniqueness
Value: [rowid]  (CBOR-encoded [ScalarValue::Integer(pk)])   ← duplicate
```

After this item, the value is empty (zero-length byte slice):

```
Key:   [encoded_col_value 8 bytes][encoded_rowid 8 bytes]
Value: []
```

The rowid is recovered from the trailing 8 bytes of the key rather than by CBOR-decoding the value.

### Why

- Removes a CBOR encode on every `WriteIndex` and a CBOR decode on every index lookup.
- Makes the key self-contained — the value field carries no semantic meaning for index B-trees.
- Simplifies the format before multi-column keys (item 42) add more structure to the key.

### Implementation Approach

**`WriteIndex` in `src/engine.rs`:**

```rust
// Before:
let index_value_encoded = vec![pk_value.clone()];
let mut encoded = Vec::new();
ciborium::ser::into_writer(&index_value_encoded, &mut encoded).unwrap();
c.insert(&index_key, encoded);

// After:
c.insert(&index_key, vec![]);  // empty value
```

**IndexScan bytecode in `src/compiler/nodes.rs`:**

Currently reads the rowid from the value field:
```rust
ReadCursor([pk_reg], index_cursor)        // CBOR-decode value → pk
MoveCursor(table_cursor, Find(pk_reg))
```

After this change, the rowid is extracted from the key tail using a new `ReadIndexRowid` instruction (or folded into item 46's `ReadCurrentKey` + blob-slice approach):
```rust
ReadIndexRowid(pk_reg, index_cursor)      // extract last 8 bytes of key, decode u64
MoveCursor(table_cursor, Find(pk_reg))
```

For now, introduce `ReadIndexRowid(dest, cursor)` as a small targeted instruction that reads the last 8 bytes of the current key and decodes them as a u64 rowid. Item 46 will subsume this into the general `ReadCurrentKey` approach.

**`PopulateIndex` remains unchanged** — it already builds the key correctly; only the value payload changes.

### Key Files

- `src/engine.rs` — `WriteIndex`: write empty value; add `ReadIndexRowid` handler
- `src/engine/program.rs` — add `ReadIndexRowid(Reg, Reg)` variant
- `src/compiler/nodes.rs` — replace `ReadCursor([pk], idx_cur)` with `ReadIndexRowid`

### Tests

- Existing index scan tests should pass unchanged (observable behaviour is identical).
- Add a unit test asserting that index entry values are empty after insert.
- Verify range scans still return correct rowids.

### Implementation Steps (2 commits)

#### Step 45.1 — WriteIndex writes empty value

Change `WriteIndex` in engine to `c.insert(&index_key, vec![])`. No change to compiler yet (index scans still attempt to `ReadCursor` the value, which will now yield an empty array and panic or misparse — temporarily broken).

**Commit:** WriteIndex: store empty value in index B-tree entries

#### Step 45.2 — Add ReadIndexRowid; update compiler

Add `Operation::ReadIndexRowid(dest, cursor)` to the program. Implement in engine by reading the last 8 bytes of the current key and decoding as a u64 rowid. Update `codegen_index_scan` to emit `ReadIndexRowid` instead of `ReadCursor([pk])`. All index scan tests pass.

**Commit:** Extract rowid from index key tail; drop ReadCursor on index entries

---

## 46. Engine: `ReadCurrentKey` + Blob Ops; Simplify Index Instructions (Track 5)

### What Changes

Add a general `ReadCurrentKey(dest, cursor)` instruction that reads the raw key bytes of the current cursor entry into a register as a `ScalarValue::Blob`. Then introduce blob comparison operations that let the compiler express index bound-checking as general value operations rather than specialised engine instructions.

**Instructions removed:**
- `KeyMatchesPrefix(dest, cursor, prefix)` — replaced by `ReadCurrentKey` + `BlobStartsWith`
- `KeyExceedsBound(dest, cursor, bound, inclusive)` — replaced by `ReadCurrentKey` + `BlobPrefixCmp`
- `ReadIndexRowid(dest, cursor)` (from item 45) — replaced by `ReadCurrentKey` + `BlobSliceTail`

**Instructions added:**
- `ReadCurrentKey(dest, cursor)` — reads raw key bytes as `Blob`
- `BlobStartsWith(dest, blob, prefix)` — true if blob starts with prefix bytes
- `BlobPrefixLt(dest, blob, bound)` — true if blob's first `len(bound)` bytes are `< bound`
- `BlobPrefixLe(dest, blob, bound)` — true if blob's first `len(bound)` bytes are `<= bound`
- `BlobSliceTail(dest, blob, offset)` — extract `blob[offset..]` as a new blob
- `DecodeU64Key(dest, blob)` — decode 8-byte blob as big-endian u64 → `Integer`

**Existing instruction `ReadKey` (table-only u64 decode)** is unified: it becomes a synonym for `ReadCurrentKey` + `DecodeU64Key`, or kept as a convenience.

### Background: Current Instruction Inventory

```
Specialised index instructions (current):
  EncodeIndexKey(dest, src)              — scalar → sortable blob
  WriteIndex(cursor, val, pk)            — composite key write + CBOR value
  KeyMatchesPrefix(dest, cursor, prefix) — key starts_with check
  KeyExceedsBound(dest, cursor, b, incl) — key prefix comparison for bounds

Table-specific:
  ReadKey(dest, cursor)    — decode key as u64 integer
  WriteCursor(cursor, key, values) — insert with u64 key + CBOR row

General:
  Open, MoveCursor, ReadCursor, CanReadCursor, DeleteCursor
```

After this item:

```
Specialised index instructions:
  EncodeIndexKey(dest, src)              — keep; encoding is domain logic
  WriteIndex removed / folded into WriteCursorBlob
  KeyMatchesPrefix removed
  KeyExceedsBound removed

New general blob primitives:
  ReadCurrentKey(dest, cursor)
  BlobStartsWith(dest, blob, prefix)
  BlobPrefixLt / BlobPrefixLe(dest, blob, bound)
  BlobSliceTail(dest, blob, offset)
  DecodeU64Key(dest, blob)

Table-specific (unchanged):
  ReadKey(dest, cursor)       — convenience alias or kept for clarity
  WriteCursor(cursor, key, values)
```

### Rewritten IndexScan Bytecode

**Before (equality scan, current):**
```
EncodeIndexKey(lower_blob, col_val_reg)
MoveCursor(idx, Find(lower_blob))
LOOP:
  CanReadCursor(flag, idx)
  GoToIfFalse(done, flag)
  KeyMatchesPrefix(in_range, idx, lower_blob)
  GoToIfFalse(done, in_range)          ← upper bound check via KeyMatchesPrefix
  ReadCursor([pk_reg], idx)             ← CBOR-decode value
  MoveCursor(tbl, Find(pk_reg))
  ReadCursor(out_regs, tbl)
  Yield(out_regs)
  MoveCursor(idx, Next)
  GoTo(LOOP)
```

**After (equality scan, refactored):**
```
EncodeIndexKey(lower_blob, col_val_reg)
MoveCursor(idx, Find(lower_blob))
LOOP:
  CanReadCursor(flag, idx)
  GoToIfFalse(done, flag)
  ReadCurrentKey(key_blob, idx)
  BlobStartsWith(in_range, key_blob, lower_blob)    ← replaces KeyMatchesPrefix
  GoToIfFalse(done, in_range)
  BlobSliceTail(pk_blob, key_blob, 8)               ← extract rowid from key tail
  DecodeU64Key(pk_reg, pk_blob)                     ← replaces ReadIndexRowid
  MoveCursor(tbl, Find(pk_reg))
  ReadCursor(out_regs, tbl)
  Yield(out_regs)
  MoveCursor(idx, Next)
  GoTo(LOOP)
```

More instructions per loop iteration, but each does one thing. The same `ReadCurrentKey` and blob ops work for range scans, multi-column key checking (item 42), and future needs.

### Key Files

- `src/engine/program.rs` — add new `Operation` variants
- `src/engine.rs` — implement new operations; remove `KeyMatchesPrefix`, `KeyExceedsBound` handlers
- `src/compiler/nodes.rs` — rewrite `codegen_index_scan` to use new primitives

### Tests

- All existing index scan tests should pass after refactor.
- Unit tests for each new blob operation independently.
- Bytecode snapshot tests (if any) will need updating.

### Implementation Steps (3 commits)

#### Step 46.1 — Add `ReadCurrentKey` and blob operations to engine

Add `Operation::ReadCurrentKey`, `BlobStartsWith`, `BlobPrefixLt`, `BlobPrefixLe`, `BlobSliceTail`, `DecodeU64Key`. Implement handlers in `src/engine.rs`. Unit-test each. No compiler changes yet — existing index scans still use old instructions.

**Commit:** Add ReadCurrentKey and blob value operations to engine

#### Step 46.2 — Rewrite compiler to use new primitives

Update `codegen_index_scan` to emit `ReadCurrentKey` + blob ops instead of `KeyMatchesPrefix`, `KeyExceedsBound`, `ReadIndexRowid`. Run all tests.

**Commit:** Rewrite index scan codegen using ReadCurrentKey + blob ops

#### Step 46.3 — Remove disused specialised instructions

Delete `KeyMatchesPrefix`, `KeyExceedsBound`, and `ReadIndexRowid` from `program.rs` and `engine.rs`. Confirm no remaining uses. Update any documentation or REPL disassembly.

**Commit:** Remove KeyMatchesPrefix, KeyExceedsBound, ReadIndexRowid

---

## 47. Split IndexScan into IndexScan + RowidLookup (Track 4.9)

### What Changes

The current `LogicalPlan::IndexScan` node knows about both the index B-tree and the table B-tree. It is split into two composable nodes:

- **`IndexScan`** — scans the index B-tree and yields one rowid per matching entry. Knows nothing about the table.
- **`RowidLookup`** — pulls rowids from its input, fetches the corresponding row from the table B-tree via `Find`, and yields the requested columns.

**Before:**
```rust
LogicalPlan::IndexScan {
    index_rootpage: u32,
    lower_bound: Option<(Literal, bool)>,
    upper_bound: Option<(Literal, bool)>,
    table_rootpage: u32,    // ← table knowledge in index node
    columns: Vec<usize>,    // ← column selection in index node
}
```

**After:**
```rust
LogicalPlan::IndexScan {
    index_rootpage: u32,
    lower_bound: Option<(Literal, bool)>,
    upper_bound: Option<(Literal, bool)>,
    // no table_rootpage, no columns
}

LogicalPlan::RowidLookup {
    input: Box<LogicalPlan>,  // typically IndexScan
    table_rootpage: u32,
    columns: Vec<usize>,
}
```

**Plan tree before:**
```
Project [id, name]
  IndexScan [idx_age, age = 30, table=users, cols: id, name]
```

**Plan tree after:**
```
Project [id, name]
  RowidLookup [users, cols: id, name]
    IndexScan [idx_age, age = 30]
```

### Covering Index Optimization

When all projected columns are present in the index key itself (a covering index), the planner can omit `RowidLookup` entirely:

```sql
CREATE INDEX idx_age ON users(age)
SELECT age FROM users WHERE age > 30

-- Plan (covering):
Project [age]
  IndexScan [idx_age, > 30]
  -- no RowidLookup; age is read directly from the index key

-- Plan (non-covering):
Project [id, age]
  RowidLookup [users, cols: id, age]
    IndexScan [idx_age, > 30]
```

V1 of this item does not need to implement covering index detection — the planner can always emit `RowidLookup`. The split makes the optimisation easy to add later.

### Bytecode Shape

**`IndexScan` codegen** (uses item 46's blob ops):
```
Open(idx_cursor, index_rootpage)
[position to lower bound: EncodeIndexKey + MoveCursor(Find)]

LOOP:
  CanReadCursor(flag, idx_cursor)
  GoToIfFalse(done, flag)
  [upper bound check: ReadCurrentKey + BlobPrefixLt/Le]
  ReadCurrentKey(key_blob, idx_cursor)
  BlobSliceTail(pk_blob, key_blob, 8)   ← rowid is last 8 bytes
  DecodeU64Key(pk_reg, pk_blob)
  Yield([pk_reg])                        ← yield rowid to parent
  MoveCursor(idx_cursor, Next)
  GoTo(LOOP)
```

**`RowidLookup` codegen**:
```
Open(tbl_cursor, table_rootpage)

[call child IndexScan to get pk_reg]

LOOP:
  MoveCursor(tbl_cursor, Find(pk_reg))
  ReadCursor(out_regs, tbl_cursor)
  Yield(out_regs)                        ← yield full row to parent
  [advance child IndexScan]
  GoTo(LOOP)
```

`RowidLookup` is a standard pipeline node: pull from child, look up, yield. No blob operations needed.

### Why This Is the Right Split

| Concern | IndexScan | RowidLookup |
|---------|-----------|-------------|
| Index B-tree cursor | ✓ | — |
| Table B-tree cursor | — | ✓ |
| Bound checking | ✓ | — |
| Column projection | — | ✓ |
| Blob key operations | ✓ | — |
| Table row decoding | — | ✓ |
| Reusable for future rowid sources | — | ✓ |

Future rowid sources that can feed `RowidLookup`: rowid lists from `IN (1, 2, 3)`, bitmap intersections of multiple indexes, `ROWID BETWEEN x AND y`.

### Planner Changes

In `try_plan_index_scan()`, instead of returning:
```rust
Ok(LogicalPlan::IndexScan { ..., table_rootpage, columns })
```

Return:
```rust
Ok(LogicalPlan::RowidLookup {
    input: Box::new(LogicalPlan::IndexScan { index_rootpage, lower_bound, upper_bound }),
    table_rootpage,
    columns,
})
```

### Key Files

- `src/planner.rs` — split `IndexScan` variant; update `try_plan_index_scan()`
- `src/compiler/nodes.rs` — `codegen_index_scan()` → `codegen_index_scan()` + `codegen_rowid_lookup()`
- `src/engine/program.rs` — no new operations needed (uses item 46's blob ops)
- `tests/sql/` — all existing index scan tests should pass unchanged

### Tests

All existing index scan SQL tests must pass after the refactor — observable behaviour is identical. Add an `EXPLAIN` test (item L.57) asserting the two-node shape:

```sql
EXPLAIN SELECT id FROM users WHERE age = 30
-- > ...
-- > RowidLookup users [cols: id]
-- >   IndexScan idx_age [= 30]
```

### Implementation Steps (3 commits)

#### Step 47.1 — Split LogicalPlan variants

Add `LogicalPlan::RowidLookup`. Update `LogicalPlan::IndexScan` to remove `table_rootpage` and `columns`. Update planner's `try_plan_index_scan()` to emit `RowidLookup { input: IndexScan }`. Update any exhaustive matches (compiler, EXPLAIN formatter) to handle the new variant — compiler can temporarily panic on `RowidLookup` until step 47.2.

**Commit:** Split IndexScan into IndexScan + RowidLookup in logical plan

#### Step 47.2 — Implement RowidLookup and IndexScan codegens

Rewrite `codegen_index_scan()` to emit only the index-scanning loop, yielding rowids. Add `codegen_rowid_lookup()` that opens a table cursor, pulls rowids from its child, and emits full rows. All index scan tests pass.

**Commit:** Codegen RowidLookup and simplified IndexScan

#### Step 47.3 — Update EXPLAIN formatter

Add `RowidLookup` and the simplified `IndexScan` to `format_node()` in `src/explain.rs`. Add SQL test asserting the two-node plan shape.

**Commit:** EXPLAIN output for RowidLookup and IndexScan

---

## 42. Multi-Column Indexes (Track 4.6)

### What Changes

Support indexes on multiple columns: `CREATE INDEX idx ON users(last_name, first_name, age)`.

### Background

Multi-column indexes enable efficient queries with multiple filters:

```sql
CREATE INDEX idx_name_age ON users(last_name, first_name, age)

-- Can use index for:
SELECT * FROM users WHERE last_name = 'Smith'
SELECT * FROM users WHERE last_name = 'Smith' AND first_name = 'John'
SELECT * FROM users WHERE last_name = 'Smith' AND first_name = 'John' AND age > 30

-- Cannot use index for:
SELECT * FROM users WHERE first_name = 'John'  -- not leftmost column
SELECT * FROM users WHERE age = 30  -- not leftmost column
```

### Implementation Approach

**Key encoding:**

Concatenate column encodings in order:
```rust
key = [col1_type][col1_bytes][col2_type][col2_bytes][col3_type][col3_bytes]
```

Type prefix ensures cross-type ordering (NULL < INTEGER < REAL < TEXT).

Example:
```
(last_name="Smith", first_name="John", age=30)
→ [0x03]["Smith"][0x03]["John"][0x01][encode_int(30)]

Where:
  0x01 = INTEGER type
  0x03 = TEXT type
```

**Parser changes:**

Extend `parse_create_index()` to accept column list:
```rust
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub column_names: Vec<String>,  // was: column_name
}
```

Parse: `CREATE INDEX idx ON table(col1, col2, col3)`

**Catalog changes:**

Store column list in catalog SQL, parse at lookup time.

**Planner changes:**

Match query predicates against index column prefix:
- `WHERE col1 = ? AND col2 = ?` → can use index on (col1, col2, col3)
- `WHERE col2 = ?` → cannot use index (not leftmost)

**Compiler/Engine changes:**

Encode multiple column values into concatenated key bytes.

### Key Files

- `src/frontend/parser.rs` — parse multi-column syntax
- `src/frontend/ast.rs` — column_names: Vec<String>
- `src/storage/btree.rs` — IndexInfo with Vec<String>
- `src/planner.rs` — match predicates to index prefix
- `src/compiler/nodes.rs` — encode multi-column keys
- `src/engine.rs` — concatenate encoded column values

### Tests

```sql
CREATE TABLE users (id INTEGER, last TEXT, first TEXT, age INTEGER)
CREATE INDEX idx_name_age ON users(last, first, age)

INSERT INTO users VALUES (1, 'Smith', 'Alice', 30)
INSERT INTO users VALUES (2, 'Smith', 'Bob', 25)
INSERT INTO users VALUES (3, 'Jones', 'Charlie', 30)

-- Use index (matches prefix)
SELECT * FROM users WHERE last = 'Smith'
-- Expected: 1, 2

-- Use index (matches longer prefix)
SELECT * FROM users WHERE last = 'Smith' AND first = 'Alice'
-- Expected: 1

-- Cannot use index (first not leftmost)
SELECT * FROM users WHERE first = 'Alice'
-- Falls back to table scan
```

### Implementation Steps (5 commits)

#### Step 42.1 — Parser: multi-column syntax

Parse `CREATE INDEX idx ON table(col1, col2, col3)`.

**Commit:** Parse multi-column index syntax

#### Step 42.2 — Catalog: store column list

Update IndexInfo to store `column_names: Vec<String>`.

**Commit:** Catalog stores multi-column index metadata

#### Step 42.3 — Key encoding: concatenate columns

Implement key encoding:
```rust
fn encode_multi_column_key(values: &[ScalarValue]) -> Vec<u8> {
    let mut key = Vec::new();
    for value in values {
        key.push(type_tag(value));  // NULL=0, INT=1, REAL=2, TEXT=3
        key.extend_from_slice(&encode_value(value));
    }
    key
}
```

**Commit:** Encode multi-column keys with type prefixes

#### Step 42.4 — Planner: prefix matching

Match query predicates to index column prefix:
- `WHERE col1=? AND col2=?` on index(col1, col2, col3) → matches
- `WHERE col2=?` on index(col1, col2) → no match

**Commit:** Planner matches predicates to index prefix

#### Step 42.5 — Tests: multi-column indexes

Add SQL integration tests for multi-column indexes with prefix matching.

**Commit:** Integration tests for multi-column indexes

---

## 43. TEXT Column Indexes (Track 4.7)

### What Changes

Support indexes on TEXT columns: `CREATE INDEX idx_name ON users(name)`.

**Depends on:** Variable-length keys (item 41)

### Background

TEXT values are stored as UTF-8 bytes. UTF-8 has the property that byte-order comparison matches lexicographic order for ASCII and most Unicode.

### Implementation Approach

**Key encoding:**

TEXT values map directly to bytes:
```rust
fn encode_text_key(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}
```

**Ordering:**
- UTF-8 bytes are naturally ordered
- `"apple" < "banana" < "cherry"`
- Case-sensitive by default (like SQLite)

**NULL handling:**

Add type prefix:
```
NULL → [0x00]
TEXT → [0x03][utf8_bytes]
```

This ensures NULL sorts before all text values.

**Collation (future):**

For V1, use binary collation (byte comparison). Future versions can add:
- NOCASE collation (case-insensitive)
- Locale-aware collation
- Custom collation functions

### Key Files

- `src/db.rs` — allow TEXT in CREATE INDEX validation
- `src/compiler/nodes.rs` — encode TEXT values as bytes
- `src/engine.rs` — handle ScalarValue::String in index operations

### Tests

```sql
CREATE TABLE products (id INTEGER, name TEXT, price INTEGER)
CREATE INDEX idx_name ON products(name)

INSERT INTO products VALUES (1, 'apple', 100)
INSERT INTO products VALUES (2, 'banana', 150)
INSERT INTO products VALUES (3, 'cherry', 200)

-- Equality
SELECT * FROM products WHERE name = 'banana'
-- Expected: 2

-- Range
SELECT * FROM products WHERE name > 'banana'
-- Expected: 3

-- Prefix (future: LIKE optimization)
SELECT * FROM products WHERE name LIKE 'ba%'
-- Expected: 2
```

### Implementation Steps (3 commits)

#### Step 43.1 — Remove INTEGER-only restriction

Update `db.rs` to allow TEXT columns in CREATE INDEX.

**Commit:** Allow TEXT columns in CREATE INDEX

#### Step 43.2 — Encode TEXT as UTF-8 bytes

Update encoder to handle TEXT:
```rust
ScalarValue::String(s) => {
    let mut key = vec![0x03];  // TEXT type tag
    key.extend_from_slice(s.as_bytes());
    key
}
```

**Commit:** Encode TEXT values as UTF-8 bytes in index keys

#### Step 43.3 — Tests: TEXT indexes

Add SQL integration tests for TEXT indexes with equality and range queries.

**Commit:** Integration tests for TEXT indexes

---

## 44. Type Conversions and NULL Handling (Track 4.8)

### What Changes

Robust handling of edge cases:
- NULL values in indexed columns
- Type mismatches (inserting TEXT into INTEGER-indexed column)
- Type coercions (implicit conversions)
- Empty strings, zero-length keys

### Background

Real-world data is messy:
```sql
CREATE INDEX idx_age ON users(age)  -- age is INTEGER

-- What happens here?
INSERT INTO users (name, age) VALUES ('Alice', NULL)
INSERT INTO users (name, age) VALUES ('Bob', '25')  -- TEXT instead of INTEGER
INSERT INTO users (name, age) VALUES ('Charlie', 25.7)  -- REAL instead of INTEGER
```

### Implementation Approach

**NULL handling:**

Reserve key prefix `0x00` for NULL:
```rust
fn encode_index_key(value: &ScalarValue) -> Vec<u8> {
    match value {
        ScalarValue::Null => vec![0x00],
        ScalarValue::Integer(i) => {
            let mut key = vec![0x01];  // INTEGER type tag
            key.extend_from_slice(&encode_integer_key(*i).to_be_bytes());
            key
        }
        ScalarValue::String(s) => {
            let mut key = vec![0x03];  // TEXT type tag
            key.extend_from_slice(s.as_bytes());
            key
        }
        // ...
    }
}
```

Queries: `WHERE col IS NULL` can use index by seeking to `0x00` prefix.

**Type coercion:**

Option 1: **Strict** - reject type mismatches at INSERT time
Option 2: **Coerce** - convert compatible types (TEXT "25" → INTEGER 25)
Option 3: **Store as-is** - allow mixed types in index (complex)

Recommend Option 1 for V1 (strict validation), Option 2 for V2.

**Validation at INSERT:**

In `plan_insert()`, check that value types match indexed column types:
```rust
for index in indexes {
    let column_type = get_column_type(index.column_idx);
    let value_type = get_value_type(value);
    if !compatible(column_type, value_type) {
        return Err(PlanError::TypeMismatch { ... });
    }
}
```

### Key Files

- `src/planner.rs` — type validation at INSERT time
- `src/compiler/nodes.rs` — encode NULL with type prefix
- `src/engine.rs` — handle NULL in WriteIndex
- `src/db.rs` — error messages for type mismatches

### Tests

```sql
CREATE TABLE data (id INTEGER, value INTEGER)
CREATE INDEX idx_value ON data(value)

-- NULL handling
INSERT INTO data VALUES (1, NULL)
INSERT INTO data VALUES (2, 10)
SELECT * FROM data WHERE value IS NULL
-- Expected: 1

-- Type mismatch (should fail)
INSERT INTO data VALUES (3, 'text')
-- Expected: ERROR: type mismatch

-- Coercion (future)
INSERT INTO data VALUES (4, '20')
-- V1: ERROR
-- V2: coerce to INTEGER 20
```

### Implementation Steps (4 commits)

#### Step 44.1 — NULL encoding with type prefix

Add 0x00 prefix for NULL in all index encodings.

**Commit:** Encode NULL with 0x00 prefix in index keys

#### Step 44.2 — Type validation at INSERT

Validate value types match column types before inserting into index.

**Commit:** Validate types when inserting into indexes

#### Step 44.3 — IS NULL index optimization

Plan `WHERE col IS NULL` to use index scan with key prefix 0x00.

**Commit:** Optimize IS NULL queries with indexes

#### Step 44.4 — Tests: NULL and type mismatches

Add comprehensive tests for NULL handling and type validation.

**Commit:** Integration tests for NULL and type handling

---

## Verification

For each item:
- [ ] Tests written first (TDD)
- [ ] All tests pass: `cargo test --bin database`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Each commit is independently testable

**End-to-end verification:**
```bash
# Range scans
cargo test test_sql_index_range_scan

# Variable-length keys
cargo test test_variable_length_keys

# Multi-column indexes
cargo test test_multi_column_index

# TEXT indexes
cargo test test_text_index

# NULL handling
cargo test test_index_null_handling
```

---

## Summary of Capabilities After G4

| Feature | G3 (Basic) | G4 (Advanced) |
|---------|-----------|---------------|
| Column types | INTEGER only | INTEGER, REAL, TEXT |
| Number of columns | Single | Multi-column |
| Predicates | Equality (`=`) | Equality, ranges (`>`, `<`, `BETWEEN`) |
| Key format | Fixed u64 | Variable-length bytes |
| NULL support | Limited | Full support with type prefix |
| Type safety | Basic | Validation + coercion |

**Performance impact:**
- Range queries: O(log N + M) instead of O(N) table scan
- Multi-column: Single index can satisfy multiple predicates
- TEXT indexes: Fast string lookups and prefix matching

**Future enhancements** (Phase G5+):
- Covering indexes (avoid table lookup)
- Partial indexes (`WHERE age > 18`)
- Expression indexes (`CREATE INDEX ON users(LOWER(email))`)
- Full-text search indexes
- Unique constraint enforcement
- Index-only scans
