# Phase AF — Covering Indexes

Elide the primary-table B-tree lookup when all projected columns are already
encoded in the index key, by introducing a `CoveringIndexScan` plan node and a
`DecodeIndexColumns` VM instruction.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 116 | 7 | SQL test baseline: correctness tests for covering-eligible index queries | — |
| 117 | 4 | Implement covering index optimization (planner + VM instruction + compiler) | 116 |
| 118 | 7 | SQL tests verifying EXPLAIN shows `CoveringIndexScan` where expected | 117 |

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
    IndexScan via idx_age [= 30]    ← yields rowids
```

**Optimised execution path** (this phase):

```
Project [age:0]
  CoveringIndexScan via idx_age [= 30] [age]   ← decodes value from key, no table touch
```

The optimisation applies when every column requested by the `RowidLookup` node
is present in the index's key encoding.  When even one column is absent, the
existing `RowidLookup(IndexScan)` path is used unchanged.

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

1. **New plan node** `LogicalPlan::CoveringIndexScan` in `src/planner/mod.rs`.
2. **Optimizer rule** in `src/planner/optimizer.rs`: rewrite
   `RowidLookup { input: IndexScan, columns: C }` → `CoveringIndexScan` when
   every column in `C` is present in the index.
3. **New VM instruction** `DecodeIndexColumns` in `src/engine/program.rs`.
4. **Compiler codegen** `codegen_covering_index_scan` in
   `src/compiler/nodes.rs`.
5. **EXPLAIN support** in `src/explain.rs`.

### Background

The index key encodes column values sequentially followed by the 8-byte rowid
suffix.  Decoding requires walking the byte stream, reading the type tag, and
parsing each field according to its encoding.  A single
`DecodeIndexColumns` VM instruction handles this walk for all existing column
types (NULL / INTEGER / FLOAT / TEXT), keeping the compiler output concise.

### Implementation Approach

#### New plan node

```rust
// src/planner/mod.rs  (inside pub enum LogicalPlan)

/// Like IndexScan but decodes column values from the index key directly,
/// skipping the primary B-tree lookup entirely.
/// `output_columns[i]` is the 0-based index key column position whose
/// decoded value becomes output register i.
CoveringIndexScan {
    index_rootpage: u32,
    lower_bound: Option<(Literal, bool)>,
    upper_bound: Option<(Literal, bool)>,
    /// Maps output position → index key column position (0-based).
    output_columns: Vec<usize>,
    /// Total number of columns in the index key (needed by DecodeIndexColumns).
    index_col_count: usize,
}
```

`output_columns` is computed by the optimizer: for each table column index
requested by `RowidLookup`, find its position in the index key.

#### Optimizer rule

Add to `src/planner/optimizer.rs` a post-pass that rewrites the plan tree
bottom-up after the existing index substitution rules run.  Match pattern:

```
RowidLookup {
    input: IndexScan { index_rootpage, lower_bound, upper_bound, .. },
    table_rootpage,
    columns,       // table column indices requested by the query
}
```

Steps:

1. Look up the table columns by name: `schema.table_columns(table_rootpage)` →
   `["id", "name", "age", ...]`.
2. Look up the index columns: `schema.index_columns(index_rootpage)` →
   `["age"]`.
3. For each table column index `c` in `columns`:
   - Get the column name `name = table_cols[c]`.
   - Find position `j` in `index_cols` where `index_cols[j] == name`.
   - If not found → **not covering**, return the tree unchanged.
   - Otherwise record `j` in `output_columns`.
4. All columns found → emit:

```rust
LogicalPlan::CoveringIndexScan {
    index_rootpage,
    lower_bound,
    upper_bound,
    output_columns,
    index_col_count: index_cols.len(),
}
```

#### New VM instruction

```rust
// src/engine/program.rs

/// Decode `dest.len()` column values from the beginning of an index key blob.
/// Walks the byte stream, consuming one type-tagged field per dest register.
/// Does NOT touch the trailing 8-byte rowid suffix.
DecodeIndexColumns {
    dest: Vec<Reg>,   // destination registers (one per index column to decode)
    src: Reg,         // register holding the raw key blob
}
```

Executor implementation (in `src/engine.rs` or the appropriate dispatch file):

```rust
Operation::DecodeIndexColumns { dest, src } => {
    let blob = registers.get_blob(src)?;
    let mut pos = 0usize;
    for &d in dest {
        let val = match *blob.get(pos).ok_or(Error::IndexKeyTruncated)? {
            0x00 => { pos += 1; ScalarValue::Null }
            0x01 => {
                let bits = u64::from_be_bytes(blob[pos+1..pos+9].try_into()?);
                let i = (bits ^ (1u64 << 63)) as i64;
                pos += 9;
                ScalarValue::Integer(i)
            }
            0x02 => {
                let bits = u64::from_be_bytes(blob[pos+1..pos+9].try_into()?);
                // Reverse the sortable float encoding:
                // if high bit is set (original was positive), just clear it;
                // otherwise flip all bits.
                let bits = if bits & (1u64 << 63) != 0 {
                    bits & !(1u64 << 63)
                } else {
                    !bits
                };
                pos += 9;
                ScalarValue::Float(f64::from_bits(bits))
            }
            0x03 => {
                let start = pos + 1;
                let nul = blob[start..]
                    .iter()
                    .position(|&b| b == 0)
                    .ok_or(Error::IndexKeyTruncated)?;
                let s = String::from_utf8(blob[start..start+nul].to_vec())?;
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

> **Note on float encoding:** confirm the encoding/decoding round-trip by
> checking `encode_index_value` in `src/storage/btree.rs`.  If the encoding
> differs from the snippet above, adjust accordingly.

#### Compiler codegen

```rust
// src/compiler/nodes.rs

fn codegen_covering_index_scan(
    ctx: &mut CodegenContext,
    index_rootpage: u32,
    lower_bound: &Option<(Literal, bool)>,
    upper_bound: &Option<(Literal, bool)>,
    output_columns: &[usize],
    index_col_count: usize,
) -> Result<()> {
    let cursor = ctx.alloc_reg();
    let key_blob = ctx.alloc_reg();
    // One register per index column (we decode all of them)
    let col_regs: Vec<Reg> = (0..index_col_count).map(|_| ctx.alloc_reg()).collect();

    emit!(ctx, Open(cursor, index_rootpage));

    // Position cursor (identical to IndexScan lower-bound logic)
    let start_label = ctx.new_label();
    codegen_index_seek(ctx, cursor, lower_bound)?;   // reuse existing helper

    ctx.place_label(start_label);

    // Upper bound check
    let halt_label = ctx.new_label();
    codegen_index_upper_bound_check(ctx, cursor, key_blob, upper_bound, halt_label)?;

    // Decode all index columns from current key
    emit!(ctx, ReadCurrentKey(key_blob, cursor));
    emit!(ctx, DecodeIndexColumns { dest: col_regs.clone(), src: key_blob });

    // Assemble output registers in the order output_columns specifies
    let output_regs: Vec<Reg> = output_columns.iter().map(|&j| col_regs[j]).collect();
    emit!(ctx, Yield(output_regs));

    emit!(ctx, MoveCursor(cursor, Next));
    emit!(ctx, GoTo(start_label));

    ctx.place_label(halt_label);
    Ok(())
}
```

Integrate by adding a `LogicalPlan::CoveringIndexScan { .. }` arm in the
top-level `codegen_plan` match in `src/compiler/nodes.rs`, mirroring the
`IndexScan` arm.

#### EXPLAIN support

In `src/explain.rs`, add:

1. `node_output_cols` arm for `CoveringIndexScan`:

```rust
LogicalPlan::CoveringIndexScan { index_rootpage, output_columns, .. } => {
    output_columns
        .iter()
        .map(|&j| {
            schema.indexes
                .get(index_rootpage)
                .and_then(|m| m.column_names.get(j))
                .cloned()
                .unwrap_or_else(|| format!("col:{j}"))
        })
        .collect()
}
```

2. `collect_rows` summary arm:

```rust
LogicalPlan::CoveringIndexScan {
    index_rootpage,
    lower_bound,
    upper_bound,
    output_columns,
    ..
} => {
    let index = schema.index_name(*index_rootpage);
    let pred = format_index_predicate(lower_bound, upper_bound);
    let cols: Vec<String> = output_columns
        .iter()
        .map(|&j| {
            schema.indexes
                .get(index_rootpage)
                .and_then(|m| m.column_names.get(j))
                .cloned()
                .unwrap_or_else(|| format!("col:{j}"))
        })
        .collect();
    format!("{indent}CoveringIndexScan via {index} [{pred}] [{}]", cols.join(", "))
}
```

3. `plan_children` arm: `LogicalPlan::CoveringIndexScan { .. } => vec![]`

### Key Files

- `src/planner/mod.rs` — add `CoveringIndexScan` variant to `LogicalPlan`
- `src/planner/optimizer.rs` — add covering index rewrite rule
- `src/engine/program.rs` — add `DecodeIndexColumns` instruction
- `src/engine.rs` (or equivalent dispatch) — execute `DecodeIndexColumns`
- `src/compiler/nodes.rs` — `codegen_covering_index_scan` + dispatch arm
- `src/explain.rs` — `node_output_cols`, `collect_rows`, `plan_children` arms

### Tests

All tests in `tests/sql/covering_index_baseline.sql` must continue to pass
unchanged (they prove correctness).  Run with:

```bash
cargo test test_sql_covering_index_baseline
cargo test  # full suite — no regressions
```

### Implementation Steps (1 commit)

#### Step 117.1 — Implement CoveringIndexScan end-to-end

Add the plan node, optimizer rule, VM instruction + executor, compiler codegen,
and EXPLAIN support in one commit.

**Commit:** `Feature: covering index optimization — skip primary B-tree lookup when all projected columns are in the index key`

---

## 118. SQL tests verifying EXPLAIN shows CoveringIndexScan (Track 7)

### What Changes

New file `tests/sql/covering_index.sql` containing EXPLAIN-based tests that
verify the optimiser produces `CoveringIndexScan` where it should, and
preserves `RowidLookup` where it should not.

These tests **fail before Item 117** (EXPLAIN shows `RowidLookup`) and
**pass after Item 117** (EXPLAIN shows `CoveringIndexScan`).

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
-- > 1, "  CoveringIndexScan via idx_age [= 30] [age]"

-- 2. NOT covered: also selects 'name', which is not in idx_age
EXPLAIN SELECT age, name FROM users WHERE age = 30
-- > 0, "Project [age:0, name:1]"
-- > 1, "  RowidLookup users [cols: age, name]"
-- > 2, "    IndexScan via idx_age [= 30]"

-- 3. Covered: select only the indexed TEXT column
EXPLAIN SELECT name FROM users WHERE name = 'Alice'
-- > 0, "Project [name:0]"
-- > 1, "  CoveringIndexScan via idx_name [= 'Alice'] [name]"

-- 4. NOT covered: also selects 'id', which is not in idx_name
EXPLAIN SELECT name, id FROM users WHERE name = 'Alice'
-- > 0, "Project [name:0, id:1]"
-- > 1, "  RowidLookup users [cols: name, id]"
-- > 2, "    IndexScan via idx_name [= 'Alice']"

-- 5. Covered range scan: select only the indexed column
EXPLAIN SELECT age FROM users WHERE age > 25
-- > 0, "Project [age:0]"
-- > 1, "  CoveringIndexScan via idx_age [> 25] [age]"

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
-- > 1, "  CoveringIndexScan via idx_status_priority [= 'open'] [status, priority]"

-- 7. Covered: only the leading column (still in the index)
EXPLAIN SELECT status FROM orders WHERE status = 'open'
-- > 0, "Project [status:0]"
-- > 1, "  CoveringIndexScan via idx_status_priority [= 'open'] [status]"

-- 8. NOT covered: requests 'note', which is not in the index
EXPLAIN SELECT status, note FROM orders WHERE status = 'open'
-- > 0, "Project [status:0, note:1]"
-- > 1, "  RowidLookup orders [cols: status, note]"
-- > 2, "    IndexScan via idx_status_priority [= 'open']"

-- Verify query results are correct after optimization (not just EXPLAIN)
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
cargo test  # full suite
```

### Implementation Steps (1 commit)

#### Step 118.1 — Add covering_index.sql EXPLAIN + result verification tests

**Commit:** `Tests: add SQL tests verifying CoveringIndexScan plan shape and correctness`

---

## Verification

- [ ] `cargo test` — all tests pass (no regressions)
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `covering_index_baseline.sql` tests pass before and after Item 117
- [ ] `covering_index.sql` EXPLAIN tests show `CoveringIndexScan` for covered queries
- [ ] `covering_index.sql` EXPLAIN tests show `RowidLookup` for non-covered queries
- [ ] Result rows from covered queries are identical to those from the RowidLookup path
- [ ] Multi-column index covering works correctly (both 1-of-2 and 2-of-2 column projections)
- [ ] TEXT column covering decodes correctly (variable-length NUL-terminated encoding)
- [ ] Each commit is independently buildable
