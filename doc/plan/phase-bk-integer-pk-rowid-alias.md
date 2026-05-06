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
| BK-3 | 4 | Scan: read rowid-alias PK from B-tree key; adjust CBOR column indices | BK-1 |
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

This reuses the same seek-and-check logic already present in `WriteIndex` — no
new VM operation is needed.

#### Planner changes

The `InsertPlan` (or `LogicalPlan::Insert`) gains a `rowid_col: Option<usize>` field:

```rust
// in src/planner/nodes.rs (or wherever Insert is)
pub struct InsertPlan {
    // ... existing fields ...
    /// If Some(i), column i is the INTEGER PRIMARY KEY rowid alias.
    /// The compiler uses its value as the B-tree key and omits it from the CBOR body.
    pub rowid_col: Option<usize>,
}
```

`plan_insert` sets `rowid_col` from `table.rowid_column()`.

It also sets a new flag `pk_omitted: bool` when the user's column list (or
positional VALUES count) omits the PK column — signalling the compiler to
auto-assign instead of reading the PK from the row.

#### Auto-assignment when PK is omitted

When the PK column is absent from the INSERT, the engine auto-assigns
`max(current rowid) + 1` using the existing `InitRowid` + rowid cache
mechanism that already handles internal rowids. The assigned value is also
written into the CBOR-excluded PK slot for SELECT to read back via `ReadKey`.

This is identical to the current internal rowid assignment — the only change is
that the assigned value is now the B-tree key *and* the user-visible PK column.

Phase BL will change the assignment rule for tables declared with `AUTOINCREMENT`
to `max(rowid ever seen) + 1` using a separate sequence table.

#### Compiler changes (`codegen_insert`)

When `rowid_col = Some(pk_idx)`:

**Case A — PK supplied by user (`pk_omitted = false`):**

1. **Skip `InitRowid`** — the key comes from `reordered_regs[pk_idx]` directly.
2. **Build `value_regs`** excluding `reordered_regs[pk_idx]`.
3. **`WriteCursor(cursor_reg, key_reg, value_regs, true)`** — `unique = true`
   causes the engine to check for a duplicate key and raise `ConstraintViolation`
   before writing.
4. **No `IncrementValue(key_reg)`** — each explicit key stands on its own.

**Case B — PK omitted (`pk_omitted = true`):**

1. **Keep `InitRowid(cursor_reg, key_reg)`** — auto-assign `max(rowid) + 1`.
2. **Build `value_regs`** from all columns (PK column not in the VALUES, so not
   in `reordered_regs` at all — the key register already holds the assigned value).
3. **`WriteCursor(cursor_reg, key_reg, value_regs, false)`** — `unique = false`
   because the rowid cache guarantees no duplicate.
4. **`IncrementValue(key_reg)`** — advance the cache for the next row.

For the non-rowid-alias path (no `rowid_col`), `WriteCursor` is emitted with
`unique = false` (unchanged behaviour).

```
// BK INSERT codegen for rowid-alias table, PK supplied:
Open(cursor_reg, rootpage)
[child plan...]
child_on_tuple:
  CopyValue(key_reg, reordered_regs[pk_idx])
  [other WriteIndex calls for non-PK unique/secondary indexes]
  WriteCursor(cursor_reg, key_reg, [non-pk column regs], unique=true)  // raises ConstraintViolation on dup
  IncrementValue(counter_reg)
  GoTo(child.next)
child_on_done:
  GoTo(cont.on_tuple)

// BK INSERT codegen for rowid-alias table, PK omitted (auto-assign):
Open(cursor_reg, rootpage)
InitRowid(cursor_reg, key_reg)             // max(rowid)+1 from cache
[child plan...]
child_on_tuple:
  [other WriteIndex calls]
  WriteCursor(cursor_reg, key_reg, [all non-pk column regs], unique=false)
  IncrementValue(key_reg)                  // advance cache
  IncrementValue(counter_reg)
  GoTo(child.next)
child_on_done:
  GoTo(cont.on_tuple)
```

### Key Files

- `src/engine/program.rs` — add `unique: bool` to `WriteCursor` variant + Display
- `src/engine.rs` — execute `WriteCursor`: add seek-and-check when `unique = true`
- `src/planner/` — `InsertPlan.rowid_col`; `InsertPlan.pk_omitted`; `plan_insert` sets both
- `src/compiler/nodes.rs` — `codegen_insert`: two code paths (PK supplied vs. auto-assigned);
  strip PK from `value_regs` in both cases; pass `unique` flag to `WriteCursor`

### Tests

```rust
#[test]
fn insert_integer_pk_uses_pk_as_btree_key() {
    // INSERT INTO t VALUES (42, 'alice') → B-tree key should be 42
    let mut db = TestDb::default();
    execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", &mut db.btree).unwrap();
    execute("INSERT INTO t VALUES (42, 'alice')", &mut db.btree).unwrap();
    let key = /* first key in B-tree */ ...;
    assert_eq!(key, 42u64);
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
    // Auto-assigned IDs should be 1, 2
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
    // Auto-assigned after explicit 100 → should be 101
    let rows = query("SELECT id FROM t ORDER BY id DESC LIMIT 1", &mut db.btree).unwrap();
    assert_eq!(rows[0][0], ScalarValue::Integer(101));
}
```

### Implementation Steps (2 commits)

#### Step BK-2.1 — VM: extend `WriteCursor` with `unique` flag

Add `unique: bool` as a fourth field to the `WriteCursor` variant. Update
Display. In the engine, when `unique = true`, seek to the key before inserting
and return `ConstraintViolation` if found. Update all existing `WriteCursor`
emit sites to pass `false` (preserves current behaviour). Add unit test.

**Commit:** `VM: extend WriteCursor with unique flag for key uniqueness enforcement`

#### Step BK-2.2 — Planner + compiler: rowid-alias INSERT

Add `InsertPlan.rowid_col` and `InsertPlan.pk_omitted`, set from schema. Update
`codegen_insert` with two code paths: explicit PK (use value as key, emit
`WriteCursor` with `unique = true`) and auto-assigned PK (`InitRowid`, emit
`WriteCursor` with `unique = false`). Strip PK from CBOR value registers in
both cases.

**Commit:** `Compiler: INSERT uses PK value as B-tree key for INTEGER PRIMARY KEY tables`

---

## BK-3. Scan: read PK column from B-tree key; adjust CBOR indices (Track 4)

### What Changes

#### `LogicalPlan::Scan` gains `rowid_col: Option<usize>`

```rust
Scan {
    rootpage: u32,
    columns: Vec<usize>,
    with_key: bool,
    rowid_col: Option<usize>,  // NEW
},
```

The planner sets this from `table.rowid_column()` when building a `Scan` node.

#### CBOR index adjustment

For a table with `rowid_col = Some(pk_idx)`, the CBOR row body has one fewer
element — all non-PK columns are packed in their original relative order:

```
Table columns: [id(PK), name, age]    (indices 0, 1, 2)
CBOR body:     [name, age]            (CBOR indices 0, 1)
```

The mapping from table column index `c` to CBOR array index:

```rust
fn cbor_index(table_col: usize, rowid_col: Option<usize>) -> usize {
    match rowid_col {
        Some(pk) if table_col > pk => table_col - 1,
        _ => table_col,
    }
}
```

#### `codegen_scan` changes

```rust
pub fn codegen_scan(
    rootpage: u32,
    columns: &[usize],
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
    with_key: bool,
    rowid_col: Option<usize>,  // NEW
) -> NodeOutput {
    let cursor_reg = ctx.registers.alloc();
    let flag_reg = ctx.registers.alloc();

    // Allocate a register for the PK (key) column if needed
    let pk_reg = rowid_col.and_then(|pk_idx| {
        if columns.contains(&pk_idx) { Some(ctx.registers.alloc()) } else { None }
    });

    // CBOR array has one fewer slot for rowid-alias tables
    let max_cbor_idx = columns.iter()
        .filter(|&&c| Some(c) != rowid_col)
        .map(|&c| cbor_index(c, rowid_col))
        .max();
    let num_read = max_cbor_idx.map(|m| m + 1).unwrap_or(0);
    let all_regs = ctx.registers.alloc_block(num_read);

    // Build output_regs:
    //   - PK column  → pk_reg
    //   - other cols → all_regs[cbor_index(col, rowid_col)]
    let mut output_regs: Vec<Reg> = columns.iter().map(|&c| {
        if Some(c) == rowid_col {
            pk_reg.unwrap()
        } else {
            all_regs[cbor_index(c, rowid_col)]
        }
    }).collect();

    let key_reg = if with_key {
        // with_key appends key register for index population (existing usage)
        let r = ctx.registers.alloc();
        output_regs.push(r);
        Some(r)
    } else {
        None
    };

    // INIT / BODY (unchanged Open, MoveCursor, CanReadCursor, GoToIfFalse)
    // ...

    body!(ctx; ReadCursor(all_regs.clone(), cursor_reg));

    // Read the B-tree key for the PK column (rowid alias) and/or with_key
    if let Some(pkr) = pk_reg {
        body!(ctx; ReadKey(pkr, cursor_reg));
    }
    if let Some(kr) = key_reg {
        body!(ctx; ReadKey(kr, cursor_reg));
    }

    body!(ctx; MoveCursor(cursor_reg, MoveOperation::Next); GoTo(cont.on_tuple));
    // ...
}
```

When `rowid_col = Some(0)` and only `name` (index 1) is requested:
- `pk_reg = None` (PK column not in `columns`)
- `num_read = 1`, `all_regs = [r_name]`
- `ReadCursor([r_name], cursor_reg)` — reads CBOR[0] into r_name

When `rowid_col = Some(0)` and both `id` (0) and `name` (1) are requested:
- `pk_reg = Some(r_id)`
- `num_read = 1`, `all_regs = [r_name]`
- `ReadCursor([r_name], cursor_reg)` — CBOR[0] → r_name
- `ReadKey(r_id, cursor_reg)` — B-tree key → r_id

#### `RowidLookup` codegen

`RowidLookup` (used by `IndexScan`) calls the table B-tree to fetch columns for a
given rowid. For rowid-alias tables, the rowid IS the PK column — `RowidLookup`
should pass the same `rowid_col` parameter so it reads the PK from the key and
adjusts CBOR indices for the other columns.

Check `codegen_rowid_lookup` and add `rowid_col: Option<usize>` in the same way.

### Key Files

- `src/planner/mod.rs` — `Scan.rowid_col` field; planner sets it
- `src/compiler/nodes.rs` — `codegen_scan` signature + implementation; `codegen_rowid_lookup` (same treatment)
- `src/explain.rs` — `Scan` display: add `[rowid-alias: col_name]` annotation when `rowid_col` is set

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
    // Requesting only name — PK register not allocated
    let mut db = TestDb::default();
    execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", &mut db.btree).unwrap();
    execute("INSERT INTO t VALUES (42, 'alice')", &mut db.btree).unwrap();
    let rows = query("SELECT name FROM t", &mut db.btree).unwrap();
    assert_eq!(rows, vec![vec![ScalarValue::Text("alice".into())]]);
}

#[test]
fn select_pk_in_middle_of_schema() {
    // Table (a TEXT, id INTEGER PRIMARY KEY, b TEXT) — PK at index 1
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

#### Step BK-3.1 — Planner: `Scan.rowid_col`; update EXPLAIN; stub codegen

Add `rowid_col` to `LogicalPlan::Scan`. Update the planner to set it from
`table.rowid_column()`. Update `explain.rs` to render it. Add a stub codegen arm
that returns `Err(UnsupportedStatement)` when `rowid_col.is_some()`.

**Commit:** `Planner: add rowid_col to Scan node; update EXPLAIN`

#### Step BK-3.2 — Compiler: `codegen_scan` CBOR adjustment; `codegen_rowid_lookup`

Implement the full `codegen_scan` changes (CBOR index mapping, `ReadKey` for PK).
Apply the same treatment to `codegen_rowid_lookup`. Add unit tests.

**Commit:** `Compiler: read INTEGER PRIMARY KEY from B-tree key in table scan`

---

## BK-4. `rowid()` zero-argument function (Track 1/4)

### What Changes

`rowid()` returns the B-tree key of the current row in a table scan. For
`INTEGER PRIMARY KEY` tables, this equals the PK column value. For tables
without an INTEGER PK, it returns the internal auto-incremented rowid.

#### Lexer / Parser

`rowid` is parsed as a zero-argument function call (like `random()`). No new
lexer token is needed — the existing identifier + `()` path handles it.

Alternatively, since SQLite treats `rowid` as a special column name (without
parens), supporting both `rowid` as a bare column alias and `rowid()` as a
function would be clean. For Phase BK, implement `rowid()` (with parens) only,
matching the `random()` precedent. Bare `rowid` can be added later.

#### Planner

A new `PlanExpr` variant:

```rust
pub enum PlanExpr {
    // ... existing variants ...
    /// rowid() — the B-tree key of the current scan row.
    Rowid,
}
```

In the expression resolver, `Function { name: "rowid", args: [] }` → `PlanExpr::Rowid`.

#### Scan must emit `ReadKey` when `rowid()` is referenced

The planner detects whether any expression in the output/filter references
`PlanExpr::Rowid`. If so, it sets `with_key: true` on the `Scan` node (the
existing mechanism already appends the key register to `output_regs`).

For rowid-alias tables, `ReadKey` is already emitted for the PK register —
`rowid()` can reuse that same register, so no double read.

The compiler resolves `PlanExpr::Rowid` to the key register during codegen.

#### `codegen_scan` must expose the key register

The existing `with_key: true` path already allocates a key register and appends
it to `output_regs`. The compiler for `rowid()` expressions simply needs to know
which register index that is. Expose it via `NodeOutput`:

```rust
pub struct NodeOutput {
    pub next: Label,
    pub reset: Option<Label>,
    pub output_regs: Vec<Reg>,
    pub rowid_reg: Option<Reg>,  // NEW: key register, set when with_key or rowid_col
}
```

The rowid expression codegen (`codegen_expr` for `PlanExpr::Rowid`) reads
`ctx.rowid_reg` (stored in `CodegenContext` after the child scan is compiled).

### Key Files

- `src/planner/mod.rs` — `PlanExpr::Rowid`
- `src/planner/resolver.rs` — map `Function("rowid", [])` → `PlanExpr::Rowid`; set
  `with_key: true` on the enclosing `Scan` when `rowid()` is referenced
- `src/compiler/nodes.rs` — `NodeOutput.rowid_reg`; `codegen_scan` sets it;
  `codegen_expr` arm for `PlanExpr::Rowid`
- `src/explain.rs` — render `PlanExpr::Rowid` as `rowid()`

### Tests

```rust
#[test]
fn rowid_function_returns_btree_key() {
    // For non-PK table, rowid() returns the internal rowid
}

#[test]
fn rowid_function_equals_pk_for_integer_pk_table() {
    // For INTEGER PRIMARY KEY table, rowid() == id column
}
```

### Implementation Steps (2 commits)

#### Step BK-4.1 — Planner: `PlanExpr::Rowid`; resolver; `with_key` propagation; EXPLAIN

Add variant, resolver arm, `with_key` auto-propagation, EXPLAIN rendering. Stub
codegen arm. Tests pass.

**Commit:** `Planner: add PlanExpr::Rowid; propagate with_key for rowid() references`

#### Step BK-4.2 — Compiler: `NodeOutput.rowid_reg`; `codegen_expr` for `PlanExpr::Rowid`

Implement the codegen. Add SQL-level tests with `rowid()`.

**Commit:** `Compiler: implement rowid() function — returns current row B-tree key`

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
- [ ] `SELECT id FROM t` reads id from B-tree key, not CBOR
- [ ] `SELECT name FROM t` reads correctly with adjusted CBOR indices
- [ ] `rowid()` returns B-tree key; equals PK for rowid-alias tables
- [ ] `INSERT INTO t (name) VALUES ('x')` on rowid-alias table auto-assigns `max(rowid)+1`
- [ ] After explicit high PK insert, next auto-assigned PK continues from `max+1`
- [ ] `TEXT PRIMARY KEY` tables still use `_pk_` implicit index (unchanged)
- [ ] `ORDER BY id` on rowid-alias table reflects natural B-tree key order
- [ ] Each commit is independently testable
