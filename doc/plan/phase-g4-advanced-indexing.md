# Phase G4 — Advanced Indexing

Builds on phase-g3-indexing.md to add multi-column indexes, TEXT support, range scans, and variable-length keys.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 40 | 4.5 | Range scan optimization (>, <, BETWEEN) | G3 |
| 42 | 4.6 | Multi-column indexes | 41 |
| 43 | 4.7 | TEXT column indexes | 41 |
| 44 | 4.8 | Type conversions and NULL handling | — |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Completed Items

| # | Track | Item | Completed |
|---|-------|------|-----------|
| 41 | 3.6 | Variable-length B-tree keys | 2026-02-18 |

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

## 41. Variable-Length B-tree Keys (Track 3.6)

### What Changes

Change B-tree API from fixed `u64` keys to variable-length `Vec<u8>` keys.

**Current:**
```rust
pub fn insert(&mut self, key: u64, value: Vec<u8>)
```

**New:**
```rust
pub fn insert(&mut self, key: &[u8], value: Vec<u8>)
```

This enables:
- TEXT indexes (UTF-8 bytes)
- Multi-column indexes (concatenated encodings)
- Future: composite keys, binary data

### Background

SQLite and other databases use variable-length keys. The B-tree stores:
- Interior nodes: `[key1, ptr1, key2, ptr2, ...]`
- Leaf nodes: `[key1, value1, key2, value2, ...]`

Keys are compared lexicographically (byte-by-byte).

### Implementation Approach

This is a **major refactor** touching all B-tree code:

**Storage layer changes:**

1. **Cell format** (`src/storage/cell.rs`):
   - Change from `(u64 key, Vec<u8> value)` to `(Vec<u8> key, Vec<u8> value)`
   - Add key length prefix: `[key_len: u32][key_bytes][value_bytes]`

2. **Node format** (`src/storage/node.rs`):
   - LeafNode: store variable-length keys
   - InteriorNode: store variable-length keys
   - Update binary search to use byte comparison

3. **Cursor** (`src/storage/btree.rs`):
   - Change `find(u64)` to `find(&[u8])`
   - Update comparison logic to lexicographic

4. **Cell reader** (`src/storage/cell_reader.rs`):
   - Read key length prefix
   - Return `&[u8]` instead of `u64`

**Engine changes:**

Update all `MoveCursor(Find(key))` operations to encode keys as bytes.

**Backward compatibility:**

Option 1: **Breaking change** - require rebuilding databases
Option 2: **Version migration** - detect old format, convert on read (complex)

Recommend Option 1 for simplicity (V1 database format not stable yet).

### Key Files

- `src/storage/cell.rs` — variable-length key encoding
- `src/storage/node.rs` — node layout with variable keys
- `src/storage/btree.rs` — API change, cursor operations
- `src/storage/cell_reader.rs` — read variable-length keys
- `src/engine.rs` — encode keys as bytes

### Tests

**Critical tests:**
- Insert/read with keys of varying lengths (1 byte, 100 bytes, 1000 bytes)
- Lexicographic ordering: `"a" < "ab" < "b" < "ba"`
- Empty keys (if allowed)
- B-tree splits with variable-length keys
- Verify integrity after splits

### Implementation Steps (7 commits)

#### Step 41.1 — Cell: variable-length key format

Update Cell struct and serialization:
```rust
pub struct Cell {
    pub key: Vec<u8>,  // was: u64
    pub value: Vec<u8>,
    pub continuation: Option<u32>,
}
```

Add CBOR encoding with key_len prefix.

**Commit:** Change Cell to support variable-length keys

#### Step 41.2 — Node: variable-length keys in LeafNode

Update LeafNode to store variable-length keys, maintain sorted order.

**Commit:** LeafNode with variable-length keys

#### Step 41.3 — Node: variable-length keys in InteriorNode

Update InteriorNode to store variable-length keys as separators.

**Commit:** InteriorNode with variable-length keys

#### Step 41.4 — Cursor: lexicographic comparison

Change find() to accept `&[u8]`, use memcmp-style comparison.

**Commit:** Cursor find with byte slice keys

#### Step 41.5 — BTree API: change signature

Update public API:
```rust
impl Cursor {
    pub fn insert(&mut self, key: &[u8], value: Vec<u8>)
    pub fn find(&mut self, key: &[u8])
    pub fn get_entry(&mut self) -> Option<CellReader>  // returns &[u8] key
}
```

**Commit:** Change BTree API to variable-length keys

#### Step 41.6 — Engine: encode keys as bytes

Update engine to encode register values as `Vec<u8>` before inserting.

For integers:
```rust
fn encode_key_bytes(value: &ScalarValue) -> Vec<u8> {
    match value {
        ScalarValue::Integer(i) => encode_integer_key(*i).to_be_bytes().to_vec(),
        ScalarValue::String(s) => s.as_bytes().to_vec(),
        // ...
    }
}
```

**Commit:** Engine encodes keys as byte arrays

#### Step 41.7 — Tests: variable-length key integrity

Add comprehensive tests:
- Ordering: `["a", "ab", "abc", "b"]`
- Long keys (1KB)
- Mixed lengths
- Splits with variable-length keys

**Commit:** Integration tests for variable-length keys

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
