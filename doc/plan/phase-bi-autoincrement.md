# Phase BI — INTEGER PRIMARY KEY AUTOINCREMENT

Enable auto-assigned row IDs for tables that declare an integer primary key column:

```sql
CREATE TABLE votes (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  winner_id INTEGER,
  loser_id  INTEGER,
  voted_at  INTEGER
)

INSERT INTO votes (winner_id, loser_id, voted_at)
VALUES (1, 2, 1234567890)
-- id is assigned automatically; no need to supply it
```

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| BI-1 | 1 | Parser + AST: `ColumnConstraint::Autoincrement` | — |
| BI-2 | 2 | Schema: `Column.autoincrement` flag; catalog stores the PK column index | BI-1 |
| BI-3 | 4 | INSERT planner + compiler: fill omitted autoincrement column with rowid | BI-2 |
| BI-4 | 7 | SQL integration tests | BI-3 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

### How it fits the existing architecture

Every INSERT already auto-assigns a monotonically increasing rowid (the B-tree key, tracked
via `get_cached_next_rowid` / `set_cached_next_rowid` in the engine). For `INTEGER PRIMARY
KEY` tables, the declared PK column *is* that rowid — users expect to read it back as a
regular column value.

Currently, INSERT always stores the user-supplied value for every column in the CBOR row
array. There is no mechanism to omit a column and have the engine fill it automatically.

This phase wires the two together:

1. `CREATE TABLE` records which column (if any) is the autoincrement PK.
2. At INSERT time, if that column is absent from the column list (or if no column list is
   given and the VALUES count is one short), the compiler inserts the rowid register into
   the row at the correct position — before writing the CBOR array.

The rowid register is already allocated by `codegen_insert` (it holds the key written to
the B-tree). Placing its value into the row array is the only new codegen step.

### V1 Scope

- Column-level `INTEGER PRIMARY KEY AUTOINCREMENT` only (not `INTEGER PRIMARY KEY` alone —
  the explicit `AUTOINCREMENT` keyword is required to opt in; this avoids changing existing
  `INTEGER PRIMARY KEY` tables that supply the ID manually).
- Single autoincrement column per table (standard SQL).
- INSERT with explicit column list that omits the PK column, or INSERT with positional
  VALUES where the count equals the non-PK column count. If the user supplies a value for
  the PK column, it is accepted and used as the explicit key (matching SQLite behaviour —
  the autoincrement column acts like a regular column when a value is provided).
- SELECT reads the PK column value from the stored row normally — no special handling at
  read time.

### Why not `INTEGER PRIMARY KEY` without `AUTOINCREMENT`?

SQLite treats `INTEGER PRIMARY KEY` as a rowid alias even without `AUTOINCREMENT`. Matching
that exactly would require changing how existing tables store their PK column (currently as
a regular value in the CBOR array, not linked to the rowid). That is a larger migration.
`AUTOINCREMENT` is the opt-in signal used in the target application, so V1 requires the
explicit keyword.

---

## BI-1. Parser + AST: `ColumnConstraint::Autoincrement`

### What Changes

**Lexer** (`src/frontend/lexer.rs`):

Add `Autoincrement` token, matched by keyword `"autoincrement"`:

```rust
b'a' => self.match_keyword("autoincrement", Type::Autoincrement),
```

(Verify that the existing `match_keyword` chain for `'a'` doesn't conflict.)

**AST** (`src/frontend/ast.rs`):

```rust
pub enum ColumnConstraint {
    PrimaryKey,
    Unique,
    NotNull,
    Autoincrement,  // NEW
}
```

**Parser** (`src/frontend/parser.rs`): in `parse_column_constraints()`, add:

```rust
lexer::Type::Autoincrement => {
    self.input.advance();
    cs.push(ast::ColumnConstraint::Autoincrement);
}
```

`AUTOINCREMENT` appears after `PRIMARY KEY` in standard SQL, so a typical column reads:
`id INTEGER PRIMARY KEY AUTOINCREMENT`. Both constraints are pushed independently; the
planner checks for the combination.

### Key Files

- `src/frontend/lexer.rs` — `Autoincrement` token
- `src/frontend/ast.rs` — `ColumnConstraint::Autoincrement` variant
- `src/frontend/parser.rs` — parse `Autoincrement` in column constraints

### Tests

```rust
#[test]
fn parse_autoincrement_constraint() {
    let stmt = parse("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)").unwrap();
    let ct = stmt.as_create_table().unwrap();
    assert!(ct.columns[0].constraints.contains(&ColumnConstraint::PrimaryKey));
    assert!(ct.columns[0].constraints.contains(&ColumnConstraint::Autoincrement));
    assert!(ct.columns[1].constraints.is_empty());
}
```

### Implementation Steps (1 commit)

#### Step BI-1.1 — Lexer, AST, parser: ColumnConstraint::Autoincrement

**Commit:** `Parser: add AUTOINCREMENT column constraint`

---

## BI-2. Schema: `Column.autoincrement` and Catalog Storage

### What Changes

**Schema** (`src/planner/schema.rs` or wherever `Column` is defined):

```rust
pub struct Column {
    pub name: String,
    pub primary_key: bool,
    pub unique: bool,
    pub autoincrement: bool,  // NEW
}
```

`resolve_table` reads `ColumnDef.constraints` and sets `autoincrement` when
`ColumnConstraint::Autoincrement` is present.

**Planner** (`src/planner/select.rs` or `nodes.rs`): add a helper:

```rust
/// Returns the index of the autoincrement column, if any.
fn autoincrement_column(columns: &[Column]) -> Option<usize> {
    columns.iter().position(|c| c.autoincrement && c.primary_key)
}
```

Only a column that is both `primary_key` and `autoincrement` qualifies.

No changes to the catalog storage format are needed: the DDL string is already stored in
`db_schema` and re-parsed at plan time. The `AUTOINCREMENT` keyword in the DDL is preserved
as-is, so `resolve_table` picks it up automatically once the parser and schema struct are
updated.

### Key Files

- Schema struct definition — `autoincrement: bool` field
- `src/planner/` — `resolve_table` populates `autoincrement`; `autoincrement_column` helper

### Tests

```rust
#[test]
fn schema_loads_autoincrement_flag() {
    let mut db = TestDb::new();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)").unwrap();
    let table = resolve_table("t", db.btree()).unwrap();
    assert!(table.columns[0].autoincrement);
    assert!(!table.columns[1].autoincrement);
}

#[test]
fn schema_non_autoincrement_table_has_no_flag() {
    let mut db = TestDb::new();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    let table = resolve_table("t", db.btree()).unwrap();
    assert!(!table.columns[0].autoincrement);
}
```

### Implementation Steps (1 commit)

#### Step BI-2.1 — Schema: autoincrement flag; autoincrement_column helper

**Commit:** `Schema: load autoincrement flag from column constraints`

---

## BI-3. INSERT Planner + Compiler: Fill Autoincrement Column

### What Changes

The INSERT planner must detect when the autoincrement column is absent from the user's
column list and arrange for the rowid to fill it.

**Planner** (`src/planner/nodes.rs` or `insert.rs`): extend `LogicalPlan::Insert` with:

```rust
pub struct InsertPlan {
    // ... existing fields ...
    /// If Some(i), column i is autoincrement and was omitted from the INSERT.
    /// The compiler fills it from the auto-assigned rowid.
    pub fill_autoincrement_at: Option<usize>,
}
```

`plan_insert` logic:

```rust
let ak = autoincrement_column(&table.columns);
let fill_autoincrement_at = ak.and_then(|col_idx| {
    // Column list given and excludes the PK column?
    if let Some(ref col_names) = stmt.columns {
        if !col_names.iter().any(|n| n.eq_ignore_ascii_case(&table.columns[col_idx].name)) {
            return Some(col_idx);
        }
    } else {
        // No column list: check if VALUES count == total columns - 1
        if values_count == table.columns.len() - 1 {
            return Some(col_idx);
        }
    }
    None
});
```

If `fill_autoincrement_at` is `Some(i)`, the VALUES list is treated as if the PK column
value is `NULL` at position `i`; the compiler replaces that `NULL` with the rowid register
after it is allocated.

**Compiler** (`src/compiler/nodes.rs`): in `codegen_insert`, after the rowid register
`r_rowid` is set (the key that will be written to the B-tree), insert the rowid value at
position `fill_autoincrement_at` in the column value registers:

```rust
if let Some(pk_idx) = insert_plan.fill_autoincrement_at {
    // r_rowid already holds the next rowid (set by the key-generation logic)
    col_regs.insert(pk_idx, r_rowid);
}
```

The CBOR encoding step that follows builds the row from `col_regs` in order, so the PK
column now contains the rowid value.

The CHECK UNIQUE operation emitted by Phase R for `PRIMARY KEY` columns continues to work:
the value being checked is the rowid register, which is already unique by construction.
Emit `CheckUnique` only when the user supplies an explicit PK value (to guard against
accidental duplicates); skip it when `fill_autoincrement_at` is set, since the rowid is
guaranteed unique.

### Key Files

- `src/planner/` — `InsertPlan.fill_autoincrement_at`; `plan_insert` logic
- `src/compiler/nodes.rs` — `codegen_insert`: splice rowid register into col_regs at pk_idx

### Tests

Deferred to BI-4 (SQL integration tests cover all compiler behaviour).

### Implementation Steps (1 commit)

#### Step BI-3.1 — INSERT planner + compiler: fill autoincrement column with rowid

**Commit:** `Compiler: fill autoincrement PK column with auto-assigned rowid on INSERT`

---

## BI-4. SQL Integration Tests

### Test File

```sql
-- tests/sql/autoincrement.sql

CREATE TABLE votes (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  winner_id INTEGER,
  loser_id  INTEGER,
  voted_at  INTEGER
)
-- > Table 'votes' created

-- INSERT without id column: id assigned automatically
INSERT INTO votes (winner_id, loser_id, voted_at) VALUES (1, 2, 1000)
-- > 1 row inserted
INSERT INTO votes (winner_id, loser_id, voted_at) VALUES (3, 4, 2000)
-- > 1 row inserted
INSERT INTO votes (winner_id, loser_id, voted_at) VALUES (5, 6, 3000)
-- > 1 row inserted

-- IDs are 1, 2, 3 (auto-assigned rowids)
SELECT id, winner_id, loser_id FROM votes ORDER BY id
-- > 1, 1, 2
-- > 2, 3, 4
-- > 3, 5, 6

-- Explicit id value still accepted
INSERT INTO votes (id, winner_id, loser_id, voted_at) VALUES (100, 7, 8, 4000)
-- > 1 row inserted
SELECT id FROM votes ORDER BY id
-- > 1
-- > 2
-- > 3
-- > 100

-- Next auto-assigned id continues from max (101 or next rowid > 100)
INSERT INTO votes (winner_id, loser_id, voted_at) VALUES (9, 10, 5000)
-- > 1 row inserted
SELECT id FROM votes ORDER BY id DESC LIMIT 1
-- > 101

-- Table without AUTOINCREMENT: no change in behaviour
CREATE TABLE manual (id INTEGER PRIMARY KEY, name TEXT)
-- > Table 'manual' created
INSERT INTO manual VALUES (42, 'alice')
-- > 1 row inserted
SELECT id, name FROM manual
-- > 42, "alice"

-- Duplicate explicit PK still rejected
INSERT INTO manual VALUES (42, 'bob')
-- > ERROR: constraint violation
```

### Implementation Steps (1 commit)

#### Step BI-4.1 — SQL integration tests for AUTOINCREMENT

**Commit:** `Tests: SQL integration tests for INTEGER PRIMARY KEY AUTOINCREMENT`

---

## Verification

- [ ] `cargo test --workspace` — all tests pass
- [ ] `cargo fmt && cargo build --workspace 2>&1 | grep warning` — zero warnings
- [ ] `INSERT INTO votes (winner_id, loser_id, voted_at) VALUES (...)` — id auto-assigned
- [ ] Successive inserts get sequential IDs starting from 1
- [ ] Explicit id value in column list is accepted and stored correctly
- [ ] Next auto-id after an explicit high ID continues from max+1
- [ ] Table without `AUTOINCREMENT` is unaffected
- [ ] Duplicate explicit PK still raises `ConstraintViolation`
- [ ] Each commit is independently testable
