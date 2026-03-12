# Phase AF — Covering Indexes

Elide the primary-table B-tree lookup when all projected columns are already
encoded in the index key, by extending `IndexScan` with an optional
`output_columns` field and adding a `DecodeIndexColumns` VM instruction.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 116 | 7 | SQL test baseline: correctness tests for covering-eligible index queries | — |
| 117 | 4 | Implement covering index optimization (planner + VM instruction + compiler) | 116 |
| 118 | 7 | SQL tests verifying EXPLAIN shows covering IndexScan where expected | 117 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

A *covering index* is an index that already contains every column a query
needs.  When this condition is met, the engine can skip the secondary lookup
into the primary B-tree entirely — the answer is in the index key itself.

**Current execution path** for `SELECT age FROM users WHERE age = 30`
(with `CREATE INDEX idx_age ON users(age)`):

```
Project [age:0]
  RowidLookup users [cols: age]     ← seeks primary B-tree once per match
    IndexScan via idx_age [= 30]    ← yields rowid only
```

**Optimised execution path** (this phase):

```
Project [age:0]
  IndexScan via idx_age [= 30] [age]   ← decodes value from key; no table touch
```

`IndexScan` gains an optional `output_columns` field.  When it is `None`
(the default), the node yields only the rowid as today.  When it is `Some`,
it decodes and yields the specified column values directly from the index key,
and the `RowidLookup` wrapper is dropped entirely.

The optimisation applies when every column requested by `RowidLookup` is
present in the index key.  If even one column is absent, the existing
`RowidLookup(IndexScan)` path is left unchanged.

**Index key encoding recap** (from `encode_index_value`):

| Column type | Bytes in key |
|-------------|-------------|
| NULL        | `0x00` (1 byte) |
| INTEGER     | `0x01` + sign-bit-flipped i64 BE (9 bytes) |
| FLOAT       | `0x02` + IEEE 754 u64 BE (9 bytes) |
| TEXT        | `0x03` + UTF-8 bytes + `0x00` NUL terminator (variable) |

Followed always by the 8-byte big-endian rowid suffix (unchanged).

---

## 116. SQL test baseline: correctness for covering-eligible queries (Track 7)

### What Changes

New file `tests/sql/covering_index_baseline.sql` containing SQL test cases
that:

- Select *only* indexed columns from a table (covering candidates).
- Mix covered and non-covered projections in the same session to verify both
  work correctly side by side.
- Cover INTEGER, TEXT, and multi-column indexes.
- Include equality and range predicates.

All these tests **pass with the current `RowidLookup` implementation** and
**must continue to pass** unchanged after Item 117 is added.  They form the
correctness baseline.

### Test Cases

```sql
-- tests/sql/covering_index_baseline.sql

-- Setup
CREATE TABLE products (id INTEGER, sku TEXT, price INTEGER, stock INTEGER)
-- > Table 'products' created
INSERT INTO products VALUES (1, 'AAA', 100, 50)
-- > 1
INSERT INTO products VALUES (2, 'BBB', 200, 30)
-- > 1
INSERT INTO products VALUES (3, 'CCC', 150, 10)
-- > 1
INSERT INTO products VALUES (4, 'DDD', 200, 0)
-- > 1
INSERT INTO products VALUES (5, 'EEE', 100, 20)
-- > 1

CREATE INDEX idx_price ON products(price)
-- > Index 'idx_price' created
CREATE INDEX idx_sku ON products(sku)
-- > Index 'idx_sku' created

-- 1. Select only the indexed INTEGER column (covering candidate)
SELECT price FROM products WHERE price = 200 ORDER BY price
-- > 200
-- > 200

-- 2. Select only the indexed TEXT column (covering candidate)
SELECT sku FROM products WHERE sku = 'AAA'
-- > "AAA"

-- 3. Select a non-indexed column alongside the indexed one (NOT covering — needs RowidLookup)
SELECT sku, stock FROM products WHERE sku = 'AAA'
-- > "AAA", 50

-- 4. Range scan: select only the indexed column
SELECT price FROM products WHERE price > 100 ORDER BY price
-- > 150
-- > 200
-- > 200

-- 5. Range scan: select indexed + non-indexed (NOT covering)
SELECT price, stock FROM products WHERE price > 100 ORDER BY price
-- > 150, 10
-- > 200, 30
-- > 200, 0

-- 6. Equality with ORDER BY: select indexed column only
SELECT price FROM products WHERE price = 100 ORDER BY price
-- > 100
-- > 100

-- Multi-column index
CREATE TABLE events (id INTEGER, category TEXT, priority INTEGER, label TEXT)
-- > Table 'events' created
INSERT INTO events VALUES (1, 'work', 1, 'meeting')
-- > 1
INSERT INTO events VALUES (2, 'work', 2, 'deadline')
-- > 1
INSERT INTO events VALUES (3, 'home', 1, 'chores')
-- > 1
INSERT INTO events VALUES (4, 'work', 1, 'standup')
-- > 1

CREATE INDEX idx_cat_pri ON events(category, priority)
-- > Index 'idx_cat_pri' created

-- 7. Select both columns of a multi-column index (covering candidate)
SELECT category, priority FROM events WHERE category = 'work' ORDER BY priority
-- > "work", 1
-- > "work", 1
-- > "work", 2

-- 8. Select only the first column of the multi-column index (covering candidate)
SELECT category FROM events WHERE category = 'home'
-- > "home"

-- 9. Select an index column plus a non-index column (NOT covering)
SELECT category, label FROM events WHERE category = 'work' ORDER BY label
-- > "work", "deadline"
-- > "work", "meeting"
-- > "work", "standup"
```

### Key Files

- `tests/sql/covering_index_baseline.sql` — new test file

### Tests

Run with `cargo test test_sql_covering_index_baseline`.

### Implementation Steps (1 commit)

#### Step 116.1 — Add covering_index_baseline.sql test file

**Commit:** `Tests: add covering index baseline SQL correctness tests`

---

## 117. Implement covering index optimization (Track 4)

### What Changes

1. **Extend `IndexScan`** with `output_columns: Option<Vec<usize>>` in
   `src/planner/mod.rs`.
2. **Optimizer rule** in `src/planner/optimizer.rs`: rewrite
   `RowidLookup { input: IndexScan { output_columns: None, .. }, columns }` →
   `IndexScan { output_columns: Some(mapped), .. }` (RowidLookup dropped) when
   every requested column is present in the index.
3. **New VM instruction** `DecodeIndexColumns` in `src/engine/program.rs`.
4. **Compiler codegen** updated in `src/compiler/nodes.rs`: emit
   `DecodeIndexColumns` instead of `BlobSliceLast + DecodeU64Key + Yield(rowid)`
   when `output_columns` is set.
5. **EXPLAIN support** in `src/explain.rs`: append column names to the
   `IndexScan` line when `output_columns` is set; `node_output_cols` returns
   index column names instead of `["rowid"]`.

### Background

The index key encodes column values sequentially followed by the 8-byte rowid
suffix.  Decoding requires walking the byte stream, reading the type tag, and
parsing each field.  A single `DecodeIndexColumns` VM instruction handles this
walk for all existing column types (NULL / INTEGER / FLOAT / TEXT), keeping the
compiler output concise.

The `IndexScan` node today always sits inside a `RowidLookup` (or at the
bottom of a join probe chain).  Adding `output_columns` lets it stand alone as
a leaf that produces real column values, which is all the planner needs to
drop the wrapping `RowidLookup`.

### Implementation Approach

#### Extend IndexScan

```rust
// src/planner/mod.rs  (inside pub enum LogicalPlan)

IndexScan {
    index_rootpage: u32,
    index_col_idx: usize,
    lower_bound: Option<(Literal, bool)>,
    upper_bound: Option<(Literal, bool)>,
    /// When None  — yield only the rowid (existing behaviour).
    /// When Some  — decode these index-key column positions and yield them.
    ///   output_columns[i] = 0-based index key column position for output slot i.
    output_columns: Option<Vec<usize>>,
}
```

All existing construction sites set `output_columns: None` — no behaviour
change for non-covering paths.

#### Optimizer rule

Add a tree-rewrite pass in `src/planner/optimizer.rs` that runs after the
existing index-substitution rules.  It matches:

```
RowidLookup {
    input: IndexScan { index_rootpage, lower_bound, upper_bound,
                       output_columns: None, .. },
    table_rootpage,
    columns,   // table column indices the query needs
}
```

Steps:

1. Get table column names via `schema.table_columns(table_rootpage)` →
   `["id", "name", "age", ...]`.
2. Get index column names via `schema.index_columns(index_rootpage)` →
   `["age"]`.
3. For each table column index `c` in `columns`:
   - `name = table_cols[c]`
   - Find `j` where `index_cols[j] == name`.
   - If not found → **not covering**; leave the tree unchanged and return.
   - Otherwise push `j` into `output_columns`.
4. All columns mapped → return:

```rust
LogicalPlan::IndexScan {
    index_rootpage,
    index_col_idx,   // unchanged
    lower_bound,
    upper_bound,
    output_columns: Some(output_columns),
}
```

#### New VM instruction

```rust
// src/engine/program.rs

/// Decode N column values from the start of an index key blob.
/// Walks the byte stream sequentially, consuming one type-tagged field
/// per dest register.  Does NOT touch the trailing 8-byte rowid suffix.
/// N = dest.len(); the compiler sets N = max(output_columns) + 1 so
/// all needed index columns are decoded into consecutive registers.
DecodeIndexColumns {
    dest: Vec<Reg>,  // one register per index column decoded (in key order)
    src:  Reg,       // blob register with the raw index key
}
```

Executor (in `src/engine.rs` or the dispatch file that handles `Operation`):

```rust
Operation::DecodeIndexColumns { dest, src } => {
    let blob = registers.get_blob(src)?;
    let mut pos = 0usize;
    for &d in dest {
        let val = match *blob.get(pos).ok_or(Error::IndexKeyTruncated)? {
            0x00 => { pos += 1; ScalarValue::Null }
            0x01 => {
                let bits = u64::from_be_bytes(blob[pos+1..pos+9].try_into()?);
                pos += 9;
                ScalarValue::Integer((bits ^ (1u64 << 63)) as i64)
            }
            0x02 => {
                let bits = u64::from_be_bytes(blob[pos+1..pos+9].try_into()?);
                // Reverse sortable float encoding (verify against encode_index_value)
                let bits = if bits & (1u64 << 63) != 0 { bits & !(1u64 << 63) } else { !bits };
                pos += 9;
                ScalarValue::Float(f64::from_bits(bits))
            }
            0x03 => {
                let start = pos + 1;
                let nul = blob[start..].iter().position(|&b| b == 0)
                    .ok_or(Error::IndexKeyTruncated)?;
                let s = String::from_utf8(blob[start..start + nul].to_vec())?;
                pos = start + nul + 1;
                ScalarValue::Text(s)
            }
            other => return Err(Error::IndexKeyUnknownType(other)),
        };
        registers.set(d, val);
    }
    Ok(StepResult::Continue)
}
```

> **Note on float encoding:** confirm the round-trip by checking
> `encode_index_value` in `src/storage/btree.rs` before finalising the
> decoder.

#### Compiler codegen

In `codegen_index_scan` (or its equivalent in `src/compiler/nodes.rs`),
branch on `output_columns`:

```rust
match output_columns {
    None => {
        // existing path: extract rowid from key suffix, Yield([rowid_reg])
        emit!(ctx, BlobSliceLast(rowid_blob, key_blob, 8));
        emit!(ctx, DecodeU64Key(rowid_reg, rowid_blob));
        emit!(ctx, Yield(vec![rowid_reg]));
    }
    Some(cols) => {
        // covering path: decode all needed index columns, assemble output
        let num_to_decode = cols.iter().copied().max().map(|m| m + 1).unwrap_or(0);
        let col_regs: Vec<Reg> = (0..num_to_decode).map(|_| ctx.alloc_reg()).collect();
        emit!(ctx, DecodeIndexColumns { dest: col_regs.clone(), src: key_blob });
        // assemble output in the order cols specifies
        let output_regs: Vec<Reg> = cols.iter().map(|&j| col_regs[j]).collect();
        emit!(ctx, Yield(output_regs));
    }
}
```

#### EXPLAIN support

In `src/explain.rs`:

1. `node_output_cols` — `IndexScan` arm:

```rust
LogicalPlan::IndexScan { index_rootpage, output_columns, .. } => {
    match output_columns {
        None => vec!["rowid".to_string()],
        Some(cols) => cols.iter().map(|&j| {
            schema.indexes.get(index_rootpage)
                .and_then(|m| m.column_names.get(j))
                .cloned()
                .unwrap_or_else(|| format!("col:{j}"))
        }).collect(),
    }
}
```

2. `collect_rows` — `IndexScan` summary: append column list when covering:

```rust
LogicalPlan::IndexScan { index_rootpage, lower_bound, upper_bound, output_columns, .. } => {
    let index = schema.index_name(*index_rootpage);
    let pred  = format_index_predicate(lower_bound, upper_bound);
    match output_columns {
        None => format!("{indent}IndexScan via {index} [{pred}]"),
        Some(cols) => {
            let names: Vec<String> = cols.iter().map(|&j| {
                schema.indexes.get(index_rootpage)
                    .and_then(|m| m.column_names.get(j))
                    .cloned()
                    .unwrap_or_else(|| format!("col:{j}"))
            }).collect();
            format!("{indent}IndexScan via {index} [{pred}] [{}]", names.join(", "))
        }
    }
}
```

### Key Files

- `src/planner/mod.rs` — add `output_columns` field to `IndexScan`
- `src/planner/optimizer.rs` — covering index rewrite rule
- `src/engine/program.rs` — `DecodeIndexColumns` instruction definition
- `src/engine.rs` (or dispatch) — `DecodeIndexColumns` executor
- `src/compiler/nodes.rs` — branch in `codegen_index_scan` on `output_columns`
- `src/explain.rs` — `node_output_cols` and `collect_rows` arms for `IndexScan`

### Tests

**Unit tests for `DecodeIndexColumns`** — add a `#[cfg(test)]` block in the
executor source file (wherever `DecodeIndexColumns` is dispatched).  Build
key blobs using `encode_index_value` (already `pub` in
`src/storage/btree.rs`) rather than crafting raw bytes by hand, so the tests
are coupled to the encoder and will catch any future encoding change
automatically.

A full index key is: `encode_index_value(col1) ++ encode_index_value(col2) ++ ... ++ rowid.to_be_bytes()`.
`DecodeIndexColumns` only consumes the column portion; the rowid suffix is
ignored (as with the existing `BlobSliceLast` path).

```rust
use crate::storage::btree::encode_index_value;
use crate::engine::scalarvalue::ScalarValue;

fn make_key(cols: &[ScalarValue], rowid: u64) -> Vec<u8> {
    let mut key = Vec::new();
    for col in cols {
        key.extend_from_slice(&encode_index_value(col));
    }
    key.extend_from_slice(&rowid.to_be_bytes());
    key
}

#[test]
fn decode_index_columns_integer_positive() {
    let key = make_key(&[ScalarValue::Integer(42)], 0);
    // assert decoded value == ScalarValue::Integer(42)
}

#[test]
fn decode_index_columns_integer_negative() {
    let key = make_key(&[ScalarValue::Integer(-1)], 0);
    // assert decoded value == ScalarValue::Integer(-1)
}

#[test]
fn decode_index_columns_string() {
    let key = make_key(&[ScalarValue::String("hello".into())], 0);
    // assert decoded value == ScalarValue::String("hello".into())
}

#[test]
fn decode_index_columns_null() {
    let key = make_key(&[ScalarValue::Null], 0);
    // assert decoded value == ScalarValue::Null
}

#[test]
fn decode_index_columns_multi_column() {
    // Two-column index: INTEGER then STRING
    let key = make_key(&[ScalarValue::Integer(7), ScalarValue::String("x".into())], 99);
    // assert both columns decode correctly
}

#[test]
fn decode_index_columns_truncated_returns_error() {
    // Pass an empty blob; expect an error, not a panic
}
```

**Property-based tests for `DecodeIndexColumns`** — add proptest cases in the
same `#[cfg(test)]` block.  proptest is already a dependency (used in
`src/storage/btree.rs`).  The strategy generates arbitrary `ScalarValue`
inputs (within the types that `encode_index_value` supports), encodes them,
decodes them, and asserts round-trip equality:

```rust
use proptest::prelude::*;

fn arb_scalar() -> impl Strategy<Value = ScalarValue> {
    prop_oneof![
        Just(ScalarValue::Null),
        any::<i64>().prop_map(ScalarValue::Integer),
        any::<f64>()
            .prop_filter("no NaN", |f| !f.is_nan())
            .prop_map(ScalarValue::Floating),
        ".*".prop_map(|s| ScalarValue::String(s.into())),
    ]
}

proptest! {
    #[test]
    fn prop_decode_index_columns_roundtrip_single(val in arb_scalar()) {
        let key = make_key(&[val.clone()], 0);
        let decoded = decode_index_columns_from_blob(&key, 1)?;
        prop_assert_eq!(decoded[0], val);
    }

    #[test]
    fn prop_decode_index_columns_roundtrip_multi(
        a in arb_scalar(),
        b in arb_scalar(),
        rowid in any::<u64>(),
    ) {
        let key = make_key(&[a.clone(), b.clone()], rowid);
        let decoded = decode_index_columns_from_blob(&key, 2)?;
        prop_assert_eq!(decoded[0], a);
        prop_assert_eq!(decoded[1], b);
        // rowid suffix is untouched — verify key length is unchanged
    }
}
```

`decode_index_columns_from_blob` is a thin test helper (or the inner
function extracted from the executor) that takes a blob and a column count
and returns `Vec<ScalarValue>` — avoids needing a full register file in
tests.

**Unit test for EXPLAIN covering IndexScan** — add to the `#[cfg(test)]`
block in `src/explain.rs`:

```rust
#[test]
fn test_explain_covering_index_scan() {
    let mut schema = ExplainSchema::empty();
    schema.indexes.insert(5, IndexMeta {
        name: "idx_age".to_string(),
        table_name: "users".to_string(),
        column_names: vec!["age".to_string()],
    });

    let plan = LogicalPlan::IndexScan {
        index_rootpage: 5,
        index_col_idx: 0,
        lower_bound: Some((Literal::Integer(30), true)),
        upper_bound: Some((Literal::Integer(30), true)),
        output_columns: Some(vec![0]),   // covering: output index col 0 (age)
    };

    let rows = format_plan(&plan, &schema);
    assert_eq!(rows.len(), 1);
    // Should include column name, not just the predicate
    assert!(rows[0].1.contains("idx_age"), "got: {}", rows[0].1);
    assert!(rows[0].1.contains("= 30"),    "got: {}", rows[0].1);
    assert!(rows[0].1.contains("[age]"),   "got: {}", rows[0].1);
}

#[test]
fn test_explain_non_covering_index_scan_unchanged() {
    // output_columns: None  →  existing format, no column list appended
    let plan = LogicalPlan::IndexScan {
        index_rootpage: 5,
        index_col_idx: 0,
        lower_bound: Some((Literal::Integer(30), true)),
        upper_bound: Some((Literal::Integer(30), true)),
        output_columns: None,
    };
    let rows = format_plan(&plan, &ExplainSchema::empty());
    assert!(rows[0].1.contains("IndexScan"), "got: {}", rows[0].1);
    assert!(!rows[0].1.contains('['), "should have no column list: {}", rows[0].1);
}
```

All tests in `tests/sql/covering_index_baseline.sql` must continue to pass
unchanged.  Run:

```bash
cargo test test_sql_covering_index_baseline
cargo test   # full suite — no regressions
```

### Implementation Steps (1 commit)

#### Step 117.1 — Extend IndexScan with covering index support end-to-end

Add `output_columns` to `IndexScan`, the optimizer rule, `DecodeIndexColumns`,
compiler branch, and EXPLAIN update in one commit.

**Commit:** `Feature: covering index optimization — skip primary B-tree lookup when all projected columns are in the index key`

---

## 118. SQL tests verifying EXPLAIN shows covering IndexScan (Track 7)

### What Changes

New file `tests/sql/covering_index.sql` containing EXPLAIN-based tests that
verify the optimiser produces a covering `IndexScan` where it should, and
preserves `RowidLookup(IndexScan)` where it should not.

These tests **fail before Item 117** (EXPLAIN shows `RowidLookup`) and
**pass after Item 117** (EXPLAIN shows the column-annotated `IndexScan`).

### Test Cases

```sql
-- tests/sql/covering_index.sql

CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)
-- > Table 'users' created
INSERT INTO users VALUES (1, 'Alice', 30)
-- > 1
INSERT INTO users VALUES (2, 'Bob', 25)
-- > 1
INSERT INTO users VALUES (3, 'Charlie', 30)
-- > 1

CREATE INDEX idx_age ON users(age)
-- > Index 'idx_age' created
CREATE INDEX idx_name ON users(name)
-- > Index 'idx_name' created

-- 1. Covered: select only the indexed INTEGER column
EXPLAIN SELECT age FROM users WHERE age = 30
-- > 0, "Project [age:0]"
-- > 1, "  IndexScan via idx_age [= 30] [age]"

-- 2. NOT covered: also selects 'name', not in idx_age
EXPLAIN SELECT age, name FROM users WHERE age = 30
-- > 0, "Project [age:0, name:1]"
-- > 1, "  RowidLookup users [cols: age, name]"
-- > 2, "    IndexScan via idx_age [= 30]"

-- 3. Covered: select only the indexed TEXT column
EXPLAIN SELECT name FROM users WHERE name = 'Alice'
-- > 0, "Project [name:0]"
-- > 1, "  IndexScan via idx_name [= 'Alice'] [name]"

-- 4. NOT covered: also selects 'id', not in idx_name
EXPLAIN SELECT name, id FROM users WHERE name = 'Alice'
-- > 0, "Project [name:0, id:1]"
-- > 1, "  RowidLookup users [cols: name, id]"
-- > 2, "    IndexScan via idx_name [= 'Alice']"

-- 5. Covered range scan
EXPLAIN SELECT age FROM users WHERE age > 25
-- > 0, "Project [age:0]"
-- > 1, "  IndexScan via idx_age [> 25] [age]"

-- Multi-column index
CREATE TABLE orders (id INTEGER, status TEXT, priority INTEGER, note TEXT)
-- > Table 'orders' created
INSERT INTO orders VALUES (1, 'open', 1, 'rush')
-- > 1
INSERT INTO orders VALUES (2, 'closed', 2, 'normal')
-- > 1
INSERT INTO orders VALUES (3, 'open', 3, 'low')
-- > 1

CREATE INDEX idx_status_priority ON orders(status, priority)
-- > Index 'idx_status_priority' created

-- 6. Covered: both columns of multi-column index
EXPLAIN SELECT status, priority FROM orders WHERE status = 'open'
-- > 0, "Project [status:0, priority:1]"
-- > 1, "  IndexScan via idx_status_priority [= 'open'] [status, priority]"

-- 7. Covered: only the leading column
EXPLAIN SELECT status FROM orders WHERE status = 'open'
-- > 0, "Project [status:0]"
-- > 1, "  IndexScan via idx_status_priority [= 'open'] [status]"

-- 8. NOT covered: 'note' is not in the index
EXPLAIN SELECT status, note FROM orders WHERE status = 'open'
-- > 0, "Project [status:0, note:1]"
-- > 1, "  RowidLookup orders [cols: status, note]"
-- > 2, "    IndexScan via idx_status_priority [= 'open']"

-- Verify result correctness after optimization
SELECT age FROM users WHERE age = 30 ORDER BY age
-- > 30
-- > 30
SELECT name FROM users WHERE name = 'Alice'
-- > "Alice"
SELECT status, priority FROM orders WHERE status = 'open' ORDER BY priority
-- > "open", 1
-- > "open", 3
```

### Key Files

- `tests/sql/covering_index.sql` — new test file

### Tests

```bash
cargo test test_sql_covering_index
cargo test   # full suite
```

### Implementation Steps (1 commit)

#### Step 118.1 — Add covering_index.sql EXPLAIN + result verification tests

**Commit:** `Tests: add SQL tests verifying covering IndexScan plan shape and correctness`

---

## Verification

- [ ] `cargo test` — all tests pass (no regressions)
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `covering_index_baseline.sql` tests pass before and after Item 117
- [ ] `covering_index.sql` EXPLAIN tests show `IndexScan … [cols]` for covered queries
- [ ] `covering_index.sql` EXPLAIN tests show `RowidLookup` for non-covered queries
- [ ] Result rows from covered queries match the RowidLookup path exactly
- [ ] Multi-column index covering works for 1-of-N and all-N column projections
- [ ] TEXT column covering decodes correctly (variable-length NUL-terminated encoding)
- [ ] Each commit is independently buildable
