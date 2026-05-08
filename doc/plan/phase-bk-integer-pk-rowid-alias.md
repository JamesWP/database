# Phase BK — INTEGER PRIMARY KEY as Rowid Alias

Make `INTEGER PRIMARY KEY` columns behave as SQLite-style rowid aliases: the
user-supplied PK value becomes the B-tree key, the column is not stored
redundantly in the CBOR row body, and a `rowid()` function exposes the key of
the current row for any table.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| BK-1 | 2 | Schema: `rowid_column: Option<usize>`; suppress `_pk_` implicit index for `INTEGER PRIMARY KEY` tables | — |
| BK-2 | 3/4 | INSERT: use PK value as B-tree key; strip PK from CBOR array; `WriteCursor` unique flag | BK-1 |
| BK-3 | 4 | Scan: scan-space column index convention; read PK and rowid from B-tree key | BK-1 |
| BK-4 | 1/4 | `rowid()` zero-argument function | BK-3 |
| BK-5 | 7 | SQL integration tests; update existing tests | BK-2, BK-3, BK-4 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

### Current vs. target behaviour

Today, `CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)` followed by
`INSERT INTO t VALUES (1, 'alice')` stores:

- **B-tree key**: auto-incremented internal rowid (e.g. `1`)
- **CBOR row body**: `[1, "alice"]` — id is stored again in the row

This wastes space and diverges from SQLite semantics, where `INTEGER PRIMARY KEY`
is a rowid alias: the PK value *is* the B-tree key and is not stored separately
in the row body.

After this phase:

- **B-tree key**: `1` (the user-supplied PK value)
- **CBOR row body**: `["alice"]` — only non-PK columns

The uniqueness guarantee that previously came from a `_pk_` implicit index is
now enforced directly by `WriteCursor` with `unique = true`, using the same
seek-and-check pattern already implemented in `WriteIndex`.

### Scope

- **In scope**: `INTEGER PRIMARY KEY` columns only (matching SQLite's rowid-alias rule).
- **Out of scope**: `TEXT PRIMARY KEY` and other non-integer PK types — these continue
  to use the existing implicit `_pk_` index.
- **In scope**: Auto-assigning the PK when omitted from INSERT — the assigned value
  is `max(rowid) + 1`, using the existing rowid cache (same as the current internal
  rowid logic). The engine already tracks the next rowid per table; for rowid-alias
  tables this now also determines the assigned PK column value.
- **Out of scope**: `AUTOINCREMENT` keyword (Phase BL) — that changes the assignment
  rule to `max(rowid ever seen) + 1`, using a separate sequence table, so that deleted
  rowids are never reused. Phase BK uses the simpler `max(current rowid) + 1`.
- **Out of scope**: `rowid()` in join queries (only single-table SELECT for now).

### How existing tests are affected

All SQL test files create fresh databases; no on-disk migration is needed. The
existing `unique_constraints.sql` test creates tables with `INTEGER PRIMARY KEY`
and expects duplicate-key errors — this continues to work because `WriteCursor`
with `unique = true` enforces the same semantics (the mechanism changes, the
observable behaviour does not).

---

## Stubs

| Stub | Behaviour | TODO marker | Completed by |
|------|-----------|-------------|--------------|
| `AUTOINCREMENT` keyword | Parsed and ignored; INSERT still auto-assigns `max(rowid)+1` | `TODO(phase-bk): autoincrement sequence table` | Phase BL |
| `rowid()` in joins | Returns `PlanError::UnsupportedStatement` | `TODO(phase-bk): rowid in joins` | Phase BL or later |

---

## Key design: scan-space column index convention

The central design decision in this phase is how `LogicalPlan::Scan` represents
which columns to read. The convention chosen here cleanly unifies rowid aliases,
`rowid()`, and regular columns without any special fields on the Scan node.

**Scan-space column index encoding:**

| Scan index | Physical meaning | Compiler action |
|---|---|---|
| `0` | B-tree key | `ReadKey(reg, cursor)` |
| `k > 0` | CBOR slot `k − 1` | `ReadCursor` slot `k−1` |

The `LogicalPlan::Scan` node carries only `{ rootpage, columns: Vec<usize> }` where
every element is a scan-space index. The compiler's read logic is trivial:

```rust
// pseudocode
for (i, &scan_idx) in columns.iter().enumerate() {
    if scan_idx == 0 {
        emit ReadKey(output_regs[i], cursor);
    } else {
        // CBOR slot = scan_idx - 1, handled via alloc_block
    }
}
```

No `with_key: bool`, no `rowid_col: Option<usize>`, no `rowid_reg` on `NodeOutput`
or `ExprContext`. The compiler is completely unaware of rowid as a concept.

**Planner translation (schema-space → scan-space):**

The planner translates schema column indices to scan indices using
`table.rowid_column()`:

| Table type | Schema col `i` | Scan index |
|---|---|---|
| No alias (`rowid_col = None`) | any `i` | `i + 1` |
| Alias at `pk` | `i == pk` | `0` |
| Alias at `pk` | `i < pk` | `i + 1` |
| Alias at `pk` | `i > pk` | `i` |
| Any | rowid (virtual col N) | `0` |

For a no-alias table `(name, age)` scanning columns `[name, age]`:
- scan indices: `[1, 2]` → ReadCursor slots 0 and 1. No key read.

For a rowid-alias table `(id PK, name)` scanning columns `[id, name]`:
- `id` → scan `0` (key), `name` → scan `1` (CBOR slot 0).
- scan indices: `[0, 1]` → ReadKey + ReadCursor slot 0.

For a rowid-alias table `(a, id PK, b)` (pk at index 1) scanning `[a, id, b]`:
- `a` → scan `1` (CBOR 0), `id` → scan `0` (key), `b` → scan `2` (CBOR 1).
- scan indices: `[1, 0, 2]`.

The CBOR layout always packs non-PK columns in their original relative order with
no gaps. This is a storage invariant set by INSERT (BK-2) and assumed by Scan (BK-3).

The same convention applies symmetrically to INSERT. The planner builds the
`value_regs` list in insert-space order: index `0` holds the user-supplied key
register (if the PK column is present in the VALUES), and indices `k > 0` hold
CBOR slot `k−1`. The compiler's rule is:

- Insert index `0` present → user supplied the key → `WriteCursor(key=reg[0], values=reg[1..], unique=true)`
- Insert index `0` absent → auto-assign → `InitRowid` + `WriteCursor(unique=false)`

No `rowid_col` or `pk_omitted` fields are needed on `InsertPlan`. Whether the key
was supplied is visible directly from the values list.

**rowid() in expressions:**

`rowid()` is resolved by the planner to `ColumnRef` pointing to the output slot of
scan index `0`. The planner adds `0` to the Scan's column list if not already present
and assigns it an output position. No `PlanExpr::Rowid` variant is needed.

---

## BK-1. Schema: `rowid_column`; suppress `_pk_` index for INTEGER PK (Track 2)

### What Changes

**`Column` struct** (`src/planner/schema.rs`):

No new field needed on `Column` itself. Instead, a helper method on `Table`
identifies the rowid alias column at plan time:

```rust
impl Table {
    /// Returns the index of the INTEGER PRIMARY KEY column, if any.
    /// This column is stored as the B-tree key (rowid alias), not in the CBOR row body.
    pub fn rowid_column(&self) -> Option<usize> {
        self.columns.iter().position(|c| {
            c.primary_key && matches!(c.data_type, Some(DataType::Integer) | None)
            // DataType::None is accepted for bare `id PRIMARY KEY` which defaults to INTEGER
        })
    }
}
```

`DataType::None` (no explicit type annotation) is treated as integer-affinity,
matching SQLite's behaviour. Only explicitly non-integer types (TEXT, REAL, BLOB)
are excluded.

**`db.rs`** — suppress `_pk_` implicit index for `INTEGER PRIMARY KEY` columns:

```rust
for col in &ct.columns {
    let is_pk = col.constraints.contains(&ColumnConstraint::PrimaryKey);
    let is_uq = col.constraints.contains(&ColumnConstraint::Unique);
    let is_integer_pk = is_pk && matches!(
        col.data_type,
        Some(DataType::Integer) | None
    );
    // INTEGER PRIMARY KEY is a rowid alias — no implicit index needed.
    if is_uq || (is_pk && !is_integer_pk) {
        let prefix = if is_pk { "_pk_" } else { "_uq_" };
        let index_name = format!("{}{}_{}", prefix, ct.table_name, col.name);
        // ... (unchanged index creation) ...
    }
}
```

### Key Files

- `src/planner/schema.rs` — `Table::rowid_column()` helper
- `src/db.rs` — skip `_pk_` index for `INTEGER PRIMARY KEY` columns

### Tests

```rust
#[test]
fn rowid_column_integer_pk() {
    let mut db = TestDb::default();
    execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", &mut db.btree).unwrap();
    let table = resolve_table("t", &db.btree).unwrap();
    assert_eq!(table.rowid_column(), Some(0));
}

#[test]
fn rowid_column_text_pk_is_none() {
    let mut db = TestDb::default();
    execute("CREATE TABLE t (id TEXT PRIMARY KEY, name TEXT)", &mut db.btree).unwrap();
    let table = resolve_table("t", &db.btree).unwrap();
    assert_eq!(table.rowid_column(), None); // TEXT PK → not a rowid alias
}

#[test]
fn rowid_column_no_pk_is_none() {
    let mut db = TestDb::default();
    execute("CREATE TABLE t (id INTEGER, name TEXT)", &mut db.btree).unwrap();
    let table = resolve_table("t", &db.btree).unwrap();
    assert_eq!(table.rowid_column(), None);
}

#[test]
fn integer_pk_does_not_create_pk_index() {
    let mut db = TestDb::default();
    execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", &mut db.btree).unwrap();
    let snap = db.btree.catalog();
    // No _pk_ index should exist for this table
    assert!(snap.lookup_index_info("_pk_t_id").is_none());
}
```

### Implementation Steps (1 commit)

#### Step BK-1.1 — Schema helper + suppress `_pk_` index for INTEGER PK

**Commit:** `Schema: identify INTEGER PRIMARY KEY as rowid alias; suppress _pk_ index`

---

## BK-2. INSERT: PK value as B-tree key; strip PK from CBOR; `WriteCursor` unique flag (Track 3/4)

### What Changes

#### Extend `WriteCursor` with a `unique` flag

`WriteCursor` already takes `(cursor_reg, key_reg, value_regs)`. Add a `unique:
bool` fourth parameter — the same pattern used by `WriteIndex(cursor, col_values,
pk, unique)`, which already does a seek-and-check in the engine before writing.

```rust
// src/engine/program.rs  (existing op, signature extended)
WriteCursor(Reg, Reg, Vec<Reg>, bool), // (cursor, key, values, unique)
```

When `unique = true`, the engine seeks to `key_reg` in the B-tree before
inserting. If a row with that exact key is found, it returns
`EngineError::ConstraintViolation` before any write occurs. When `unique =
false` (the existing non-PK path), the behaviour is unchanged.

#### Planner changes

`InsertPlan` needs no new fields. The planner applies the same `to_scan_index`
translation used for Scan: it reorders the user-supplied value registers into
insert-space order before handing them to the compiler.

For `INSERT INTO t VALUES (42, 'alice')` on `(id INTEGER PK, name)`:
- `id` (schema 0, pk) → insert index `0` (key register = 42)
- `name` (schema 1) → insert index `1` (CBOR slot 0 = 'alice')
- `value_regs = [reg(42), reg('alice')]`

For `INSERT INTO t (name) VALUES ('alice')` on `(id INTEGER PK, name)`:
- PK not supplied → insert index `0` absent
- `name` → insert index `1`
- `value_regs = [reg('alice')]`  (no index-0 entry)

For a non-alias table `(a, b)`:
- `a` → insert index `1`, `b` → insert index `2`
- `value_regs = [reg(a), reg(b)]`  (no index-0 entry → auto-assign rowid)

#### Auto-assignment when PK is omitted

When insert index `0` is absent from `value_regs`, the engine auto-assigns
`max(current rowid) + 1` using the existing `InitRowid` + rowid cache mechanism.
This covers both non-alias tables (always) and rowid-alias tables where the user
omitted the PK column.

#### Compiler changes (`codegen_insert`)

The compiler inspects only whether `value_regs[0]` exists (insert index 0):

**Key present (`value_regs` has an index-0 entry):**

1. `key_reg = value_regs[0]`
2. `WriteCursor(cursor_reg, key_reg, value_regs[1..], unique=true)` — raises
   `ConstraintViolation` on duplicate key.
3. No `IncrementValue`.

**Key absent (no index-0 entry):**

1. `InitRowid(cursor_reg, key_reg)` — auto-assign `max(rowid) + 1`.
2. `WriteCursor(cursor_reg, key_reg, value_regs[0..], unique=false)` — rowid
   cache guarantees no duplicate.
3. `IncrementValue(key_reg)` — advance the cache.

No `rowid_col`, no `pk_omitted`. The distinction is structural: is there a
key register in the values list or not?

### Key Files

- `src/engine/program.rs` — add `unique: bool` to `WriteCursor` variant + Display
- `src/engine.rs` — execute `WriteCursor`: seek-and-check when `unique = true`
- `src/planner/` — `plan_insert`: apply `to_scan_index` translation to reorder value registers into insert-space order
- `src/compiler/nodes.rs` — `codegen_insert`: key-present vs key-absent branches; no `rowid_col` or `pk_omitted`

### Tests

```rust
#[test]
fn insert_integer_pk_uses_pk_as_btree_key() {
    let mut db = TestDb::default();
    execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", &mut db.btree).unwrap();
    execute("INSERT INTO t VALUES (42, 'alice')", &mut db.btree).unwrap();
    // verify first B-tree key is 42
}

#[test]
fn insert_integer_pk_duplicate_raises_constraint_violation() {
    let mut db = TestDb::default();
    execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", &mut db.btree).unwrap();
    execute("INSERT INTO t VALUES (1, 'alice')", &mut db.btree).unwrap();
    let result = execute("INSERT INTO t VALUES (1, 'bob')", &mut db.btree);
    assert!(matches!(result, Err(ExecuteError::ConstraintViolation(_))));
}

#[test]
fn insert_integer_pk_auto_assigned_when_omitted() {
    let mut db = TestDb::default();
    execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", &mut db.btree).unwrap();
    execute("INSERT INTO t (name) VALUES ('alice')", &mut db.btree).unwrap();
    execute("INSERT INTO t (name) VALUES ('bob')", &mut db.btree).unwrap();
    let rows = query("SELECT id FROM t ORDER BY id", &mut db.btree).unwrap();
    assert_eq!(rows[0][0], ScalarValue::Integer(1));
    assert_eq!(rows[1][0], ScalarValue::Integer(2));
}

#[test]
fn insert_explicit_pk_then_auto_continues_from_max() {
    let mut db = TestDb::default();
    execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", &mut db.btree).unwrap();
    execute("INSERT INTO t VALUES (100, 'alice')", &mut db.btree).unwrap();
    execute("INSERT INTO t (name) VALUES ('bob')", &mut db.btree).unwrap();
    let rows = query("SELECT id FROM t ORDER BY id DESC LIMIT 1", &mut db.btree).unwrap();
    assert_eq!(rows[0][0], ScalarValue::Integer(101));
}
```

### Implementation Steps (2 commits)

#### Step BK-2.1 — VM: extend `WriteCursor` with `unique` flag

Add `unique: bool` as a fourth field to `WriteCursor`. Update Display. In the
engine, seek and check when `unique = true`. Update all existing `WriteCursor`
emit sites to pass `false`.

**Commit:** `VM: extend WriteCursor with unique flag for key uniqueness enforcement`

#### Step BK-2.2 — Planner + compiler: rowid-alias INSERT

Update `plan_insert` to apply `to_scan_index` translation, placing the PK value
at insert index 0 when supplied. Update `codegen_insert` to branch on whether
index 0 is present (user key, `unique=true`) or absent (auto-assign, `unique=false`).

**Commit:** `Compiler: INSERT uses PK value as B-tree key for INTEGER PRIMARY KEY tables`

---

## BK-3. Scan: scan-space column indices (Track 4)

### What Changes

#### `LogicalPlan::Scan` loses `with_key` and `rowid_col`

```rust
// Before
Scan {
    rootpage: u32,
    columns: Vec<usize>,
    with_key: bool,
    rowid_col: Option<usize>,
}

// After
Scan {
    rootpage: u32,
    columns: Vec<usize>,  // scan-space: 0=key, k>0=CBOR[k-1]
}
```

See the "Key design" section above for the full encoding and translation rules.

#### Planner: translate schema indices → scan indices when building Scan

The planner calls `table.rowid_column()` once when building a Scan node and uses
it to translate the collected schema column indices to scan-space indices. This
translation is entirely within the planner; the compiler sees only scan indices.

```rust
fn to_scan_index(schema_col: usize, rowid_col: Option<usize>) -> usize {
    match rowid_col {
        Some(pk) if schema_col == pk => 0,         // PK column → key
        Some(pk) if schema_col > pk => schema_col, // shift down (CBOR slot = schema_col - 1)
        _ => schema_col + 1,                       // no alias or col before pk → shift up
    }
}
```

The virtual rowid column (schema index `N = table.columns.len()`) always maps to
scan index `0`.

#### `codegen_scan`: trivial physical read

The compiler's only job is to map scan indices to read operations:

```
scan_idx == 0  →  ReadKey(reg, cursor)
scan_idx  > 0  →  ReadCursor slot (scan_idx - 1)
```

Because `ReadCursor` reads a contiguous block of CBOR slots 0..max, the compiler
allocates `all_regs` of size `max_cbor_slot + 1` and indexes into it. Scan index
0 gets its own `ReadKey` call. There is no `cbor_idx` closure, no `pk_reg` /
`key_reg` distinction, and no `rowid_reg` on `NodeOutput`.

```
// Example: Scan { columns: [0, 2] } on table (id PK, name)
//   col 0 → ReadKey  →  r0 = id
//   col 2 → ReadCursor slot 1  →  r1 = name (CBOR slot 1 is the 2nd non-PK col)
//
// Wait — for table (id PK, name), name is CBOR slot 0.
// The planner would produce columns: [0, 1]
//   col 0 → ReadKey  →  r0 = id
//   col 1 → ReadCursor slot 0  →  r1 = name
```

Key register allocation in `codegen_scan`:

```rust
let key_reg = if columns.contains(&0) { Some(ctx.registers.alloc()) } else { None };

let max_cbor = columns.iter().filter(|&&c| c > 0).map(|&c| c - 1).max();
let all_regs = max_cbor.map(|m| ctx.registers.alloc_block(m + 1));

// Build output_regs in column order
let output_regs: Vec<Reg> = columns.iter().map(|&c| {
    if c == 0 { key_reg.unwrap() }
    else { all_regs.as_ref().unwrap()[c - 1] }
}).collect();

// Emit reads
if let Some(kr) = key_reg { body!(ctx; ReadKey(kr, cursor_reg)); }
if let Some(regs) = all_regs { body!(ctx; ReadCursor(regs, cursor_reg)); }
```

#### `NodeOutput`: `rowid_reg` field removed

`NodeOutput` no longer carries `rowid_reg`. It was only needed to thread the key
register through Filter and Project to expression compilation. Under the
scan-space convention, the key is just another output register — if scan index 0
was requested, its register is in `output_regs` at the appropriate slot. Filter
and Project propagate `output_regs` unchanged (as they already do), so the key
register reaches expression compilation automatically.

#### `RowidLookup`: same treatment

`codegen_rowid_lookup` applies identical scan-space logic. The planner translates
schema indices to scan indices when building a `RowidLookup` node, and the
compiler uses the same trivial read rule.

### Key Files

- `src/planner/mod.rs` — remove `with_key` and `rowid_col` from `LogicalPlan::Scan`
- `src/planner/select.rs` — `to_scan_index` translation; apply to all Scan construction sites
- `src/compiler/nodes.rs` — `codegen_scan`: scan-space read logic; remove `rowid_reg` from `NodeOutput`; update `codegen_rowid_lookup`
- `src/compiler/expr.rs` — remove `rowid_reg` from `ExprContext`
- `src/explain.rs` — update Scan display (remove `with_key` / `rowid_col` annotations)

### Tests

```rust
#[test]
fn select_integer_pk_reads_from_key() {
    let mut db = TestDb::default();
    execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", &mut db.btree).unwrap();
    execute("INSERT INTO t VALUES (42, 'alice')", &mut db.btree).unwrap();
    let rows = query("SELECT id, name FROM t", &mut db.btree).unwrap();
    assert_eq!(rows, vec![vec![ScalarValue::Integer(42), ScalarValue::Text("alice".into())]]);
}

#[test]
fn select_non_pk_column_only() {
    let mut db = TestDb::default();
    execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", &mut db.btree).unwrap();
    execute("INSERT INTO t VALUES (42, 'alice')", &mut db.btree).unwrap();
    let rows = query("SELECT name FROM t", &mut db.btree).unwrap();
    assert_eq!(rows, vec![vec![ScalarValue::Text("alice".into())]]);
}

#[test]
fn select_pk_in_middle_of_schema() {
    let mut db = TestDb::default();
    execute("CREATE TABLE t (a TEXT, id INTEGER PRIMARY KEY, b TEXT)", &mut db.btree).unwrap();
    execute("INSERT INTO t VALUES ('x', 5, 'y')", &mut db.btree).unwrap();
    let rows = query("SELECT a, id, b FROM t", &mut db.btree).unwrap();
    assert_eq!(rows[0], vec![
        ScalarValue::Text("x".into()),
        ScalarValue::Integer(5),
        ScalarValue::Text("y".into()),
    ]);
}
```

### Implementation Steps (2 commits)

#### Step BK-3.1 — Planner: scan-space index convention; update EXPLAIN

Remove `with_key` and `rowid_col` from `LogicalPlan::Scan`. Add `to_scan_index`
helper. Update all Scan construction sites in `select.rs` and `dml.rs`. Update
`explain.rs`. Add a stub codegen arm returning `Err(UnsupportedStatement)` for
tables that need a key read (scan index 0 in columns) so existing tests continue
to pass.

**Commit:** `Planner: scan-space column indices for Scan node; remove with_key and rowid_col`

#### Step BK-3.2 — Compiler: `codegen_scan` scan-space reads; remove `rowid_reg`

Implement the full `codegen_scan` using scan-space logic. Remove `rowid_reg` from
`NodeOutput` and `ExprContext`. Update `codegen_rowid_lookup`. Add unit tests.

**Commit:** `Compiler: codegen_scan uses scan-space indices; remove rowid_reg`

---

## BK-4. `rowid()` zero-argument function (Track 1/4)

### What Changes

`rowid()` returns the B-tree key of the current row. For `INTEGER PRIMARY KEY`
tables this equals the PK column value; for other tables it returns the internal
auto-incremented rowid.

#### Lexer / Parser

`rowid` is parsed as a zero-argument function call (like `random()`). No new
lexer token needed — the existing identifier + `()` path handles it. Phase BK
implements `rowid()` (with parens) only; bare `rowid` as a column name can be
added later.

#### Planner: resolve `rowid()` to a `ColumnRef`

There is no `PlanExpr::Rowid` variant. Instead, the planner resolves
`FunctionCall { name: "rowid", args: [] }` to a `ColumnRef` pointing to the
output slot that holds the B-tree key (scan index 0):

1. When the expression resolver encounters `rowid()`, it ensures scan index `0`
   is included in the Scan node's `columns` list (adding it if absent).
2. It returns `ColumnRef(output_position_of_scan_0)` — the same kind of
   expression used for any other column reference.

Because scan index 0 is just another entry in `columns`, the compiler emits
`ReadKey` for it as part of normal scan codegen. No `with_key` flag, no
`rowid_reg`, no special expression compilation path.

For rowid-alias tables, `rowid()` and the PK column both resolve to scan index
`0`. If both are in the SELECT list, the planner deduplicates: both `ColumnRef`s
point to the same output slot, and only one `ReadKey` is emitted.

#### Existing `ast_expr_uses_rowid` helper

The planner already has a helper that walks an AST expression looking for
`rowid()` calls. Under the old design this set `with_key = true` on the Scan.
Under the new design, the expression resolver handles this inline: when it
encounters `rowid()` it adds scan index 0 to the column set. The
`ast_expr_uses_rowid` helper may be removed or repurposed as part of this item.

### Key Files

- `src/planner/resolver.rs` — resolve `FunctionCall("rowid", [])` to `ColumnRef`;
  add scan index 0 to the Scan's column set; remove or repurpose `ast_expr_uses_rowid`
- `src/explain.rs` — render the scan index 0 column as `rowid` in EXPLAIN output

### Tests

```rust
#[test]
fn rowid_function_returns_btree_key() {
    let mut db = TestDb::default();
    execute("CREATE TABLE t (name TEXT)", &mut db.btree).unwrap();
    execute("INSERT INTO t VALUES ('a')", &mut db.btree).unwrap();
    execute("INSERT INTO t VALUES ('b')", &mut db.btree).unwrap();
    let rows = query("SELECT rowid() FROM t ORDER BY rowid()", &mut db.btree).unwrap();
    assert_eq!(rows[0][0], ScalarValue::Integer(1));
    assert_eq!(rows[1][0], ScalarValue::Integer(2));
}

#[test]
fn rowid_function_equals_pk_for_integer_pk_table() {
    let mut db = TestDb::default();
    execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", &mut db.btree).unwrap();
    execute("INSERT INTO t VALUES (42, 'alice')", &mut db.btree).unwrap();
    let rows = query("SELECT id, rowid() FROM t", &mut db.btree).unwrap();
    assert_eq!(rows[0][0], rows[0][1]); // id == rowid()
}
```

### Implementation Steps (2 commits)

#### Step BK-4.1 — Planner: resolve `rowid()` to ColumnRef; EXPLAIN

Update the expression resolver to map `rowid()` to a ColumnRef at scan index 0.
Update EXPLAIN. Tests for `rowid()` in SELECT and WHERE.

**Commit:** `Planner: resolve rowid() to ColumnRef at scan index 0`

#### Step BK-4.2 — SQL-level tests for `rowid()`

Add SQL integration tests exercising `rowid()` on both regular and rowid-alias
tables.

**Commit:** `Tests: SQL integration tests for rowid() function`

---

## BK-5. SQL Integration Tests (Track 7)

### Test Files

**`tests/sql/integer_pk_rowid_alias.sql`** — new file covering the full rowid-alias
behaviour:

```sql
-- Basic rowid-alias insert and select
CREATE TABLE votes (id INTEGER PRIMARY KEY, label TEXT)
-- > Table 'votes' created

INSERT INTO votes VALUES (10, 'yes')
-- > 1 row inserted
INSERT INTO votes VALUES (20, 'no')
-- > 1 row inserted
INSERT INTO votes VALUES (5, 'maybe')
-- > 1 row inserted

-- SELECT returns all three; ORDER BY id uses natural B-tree order
SELECT id, label FROM votes ORDER BY id
-- > 5, "maybe"
-- > 10, "yes"
-- > 20, "no"

-- Duplicate PK rejected
INSERT INTO votes VALUES (10, 'duplicate')
-- > ERROR: constraint violation

-- rowid() returns same value as PK column
SELECT id, rowid() FROM votes ORDER BY id
-- > 5, 5
-- > 10, 10
-- > 20, 20

-- rowid() on a non-PK table returns internal rowid
CREATE TABLE items (name TEXT)
-- > Table 'items' created
INSERT INTO items VALUES ('a')
-- > 1 row inserted
INSERT INTO items VALUES ('b')
-- > 1 row inserted
SELECT name, rowid() FROM items ORDER BY rowid()
-- > "a", 1
-- > "b", 2

-- PK column at a non-zero position
CREATE TABLE events (ts INTEGER, kind TEXT, id INTEGER PRIMARY KEY)
-- > Table 'events' created
INSERT INTO events VALUES (1000, 'click', 7)
-- > 1 row inserted
SELECT id, kind, ts FROM events
-- > 7, "click", 1000

-- Omitting PK column auto-assigns max(rowid)+1
INSERT INTO votes (label) VALUES ('auto')
-- > 1 row inserted
SELECT id, label FROM votes ORDER BY id DESC LIMIT 1
-- > 21, "auto"

-- Non-integer PK tables still use implicit index (no change)
CREATE TABLE tags (name TEXT PRIMARY KEY, count INTEGER)
-- > Table 'tags' created
INSERT INTO tags VALUES ('rust', 3)
-- > 1 row inserted
INSERT INTO tags VALUES ('rust', 5)
-- > ERROR: constraint violation
INSERT INTO tags VALUES ('go', 1)
-- > 1 row inserted
SELECT name, count FROM tags ORDER BY name
-- > "go", 1
-- > "rust", 3
```

**Update `tests/sql/unique_constraints.sql`** — verify the file still passes
unchanged (duplicate PK detection still works via `WriteCursor` with `unique =
true`). Run `cargo test test_sql_unique_constraints` to confirm.

### Implementation Steps (1 commit)

#### Step BK-5.1 — SQL integration tests

Add `tests/sql/integer_pk_rowid_alias.sql`. Run `cargo run --bin update-sql-tests
integer_pk_rowid_alias` to capture expected output; verify it matches the
`-- >` annotations above.

**Commit:** `Tests: SQL integration tests for INTEGER PRIMARY KEY rowid alias`

---

## Verification

- [ ] `cargo test --workspace` — all tests pass including `unique_constraints`
- [ ] `cargo fmt && cargo build --workspace 2>&1 | grep warning` — zero warnings
- [ ] `INSERT INTO t VALUES (42, 'alice')` → B-tree key is 42
- [ ] CBOR row body contains only non-PK columns (inspect with `btree inspect page`)
- [ ] Duplicate PK raises `ConstraintViolation` (via `WriteCursor` `unique = true`)
- [ ] `SELECT id FROM t` reads id from B-tree key (scan index 0), not CBOR
- [ ] `SELECT name FROM t` reads correctly — CBOR slot 0 despite being schema column 1
- [ ] `rowid()` returns B-tree key; equals PK for rowid-alias tables
- [ ] `rowid()` compiles to a ColumnRef with no special register handling
- [ ] `LogicalPlan::Scan` has no `with_key` or `rowid_col` fields
- [ ] `NodeOutput` and `ExprContext` have no `rowid_reg` field
- [ ] `INSERT INTO t (name) VALUES ('x')` on rowid-alias table auto-assigns `max(rowid)+1`
- [ ] After explicit high PK insert, next auto-assigned PK continues from `max+1`
- [ ] `TEXT PRIMARY KEY` tables still use `_pk_` implicit index (unchanged)
- [ ] `ORDER BY id` on rowid-alias table reflects natural B-tree key order
- [ ] Each commit is independently testable
