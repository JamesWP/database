# Phase R — PRIMARY KEY & UNIQUE Constraints

Enforce uniqueness at INSERT time: `PRIMARY KEY` and `UNIQUE` column constraints are parsed, stored in the schema, and checked by the engine before writing a row.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 75 | 2 | Lexer & AST: add PRIMARY KEY / UNIQUE tokens and `ColumnConstraint` | — |
| 76 | 2 | Parser: parse column constraints in CREATE TABLE | 75 |
| 77 | 2 | Schema: store constraint flags in `Column`; planner loads them | 76 |
| 78 | 2 | Engine: add `CheckUnique` operation and `ConstraintViolation` error | — |
| 79 | 2 | Compiler: `codegen_insert` emits `CheckUnique` for constrained columns | 77, 78 |
| 80 | 7 | SQL regression tests for UNIQUE and PRIMARY KEY constraint violations | 79 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

Currently all INSERT operations auto-assign a monotonically increasing integer rowid and never check for value collisions in user-defined columns. If the user creates a table with `id INTEGER PRIMARY KEY`, a second `INSERT INTO t VALUES (1, 'x')` would silently overwrite the first row (same rowid-key) or succeed without complaint.

This phase adds first-class `PRIMARY KEY` and `UNIQUE` column constraints:

- **UNIQUE** — no two rows may have the same non-NULL value in this column.
- **PRIMARY KEY** — implies UNIQUE + (for V1) NOT NULL; at most one per table.

### V1 Scope

- Column-level constraints only (no composite `UNIQUE (a, b)` table constraint).
- `INTEGER` and `TEXT` columns only (matches existing index key encoding support).
- Enforcement on INSERT only; UPDATE is a follow-up phase.
- Constraint checking uses a dedicated UNIQUE index created implicitly alongside the table.
- Duplicate detection returns a clear `ConstraintViolation` error; no silent overwrites.

### Architecture

A UNIQUE constraint is implemented as an implicit index. When `CREATE TABLE` sees `UNIQUE` or `PRIMARY KEY` on a column, it creates a secondary B-tree (an index) for that column — identically to `CREATE INDEX`, but marked as `unique=true` in `db_schema`. On INSERT, the compiler emits a `CheckUnique(index_cursor_reg, value_reg)` operation before `WriteIndex` and `WriteCursor`. The engine handler looks up the key in the index and returns an error if it already exists.

This avoids a separate uniqueness datastructure: the existing index infrastructure handles storage and lookup.

---

## 75. Lexer & AST: `ColumnConstraint` (Track 2)

### What Changes

**Lexer** (`src/frontend/lexer.rs`):
- Add tokens: `Primary`, `Key`, `Unique`, `Not`, `Null`

> `Not` and `Null` may already exist — check and add only what is missing.

**AST** (`src/frontend/ast.rs`):

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    PrimaryKey,
    Unique,
    NotNull,
}

pub struct ColumnDef {
    pub name: String,
    pub type_name: Option<DataType>,
    pub constraints: Vec<ColumnConstraint>,  // NEW
}
```

Update all `ColumnDef { name, type_name }` construction sites to include `constraints: vec![]`.

### Key Files

- `src/frontend/lexer.rs` — new token variants
- `src/frontend/ast.rs` — `ColumnConstraint` enum; `ColumnDef.constraints`

### Tests

No new tests yet; compilation confirms the AST change is consistent.

### Implementation Steps (1 commit)

#### Step 75.1 — Add ColumnConstraint to AST and new lexer tokens

**Commit:** AST: add ColumnConstraint enum and constraints field to ColumnDef

---

## 76. Parser: Parse Column Constraints (Track 2)

### What Changes

`parse_column_def` (in `src/frontend/parser.rs`) consumes zero or more constraint tokens after the type name:

```rust
fn parse_column_def(&mut self) -> ParseResult<ast::ColumnDef> {
    let name = self.parse_identifier()?;
    let type_name = self.parse_optional_data_type();
    let constraints = self.parse_column_constraints();
    Ok(ast::ColumnDef { name, type_name, constraints })
}

fn parse_column_constraints(&mut self) -> Vec<ast::ColumnConstraint> {
    let mut cs = Vec::new();
    loop {
        match self.input.peek() {
            Token::Primary => {
                self.input.advance();
                self.input.expect(Expect::Key).ok(); // consume KEY
                cs.push(ast::ColumnConstraint::PrimaryKey);
            }
            Token::Unique => {
                self.input.advance();
                cs.push(ast::ColumnConstraint::Unique);
            }
            Token::Not => {
                self.input.advance();
                self.input.expect(Expect::Null).ok(); // consume NULL
                cs.push(ast::ColumnConstraint::NotNull);
            }
            _ => break,
        }
    }
    cs
}
```

### Key Files

- `src/frontend/parser.rs` — `parse_column_def`, `parse_column_constraints`

### Tests

```rust
#[test]
fn test_parse_primary_key_constraint() {
    let stmt = parse("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    let ct = stmt.as_create_table().unwrap();
    assert!(ct.columns[0].constraints.contains(&ColumnConstraint::PrimaryKey));
    assert!(ct.columns[1].constraints.is_empty());
}

#[test]
fn test_parse_unique_constraint() {
    let stmt = parse("CREATE TABLE t (email TEXT UNIQUE)").unwrap();
    let ct = stmt.as_create_table().unwrap();
    assert!(ct.columns[0].constraints.contains(&ColumnConstraint::Unique));
}
```

### Implementation Steps (1 commit)

#### Step 76.1 — Parse PRIMARY KEY and UNIQUE in column definitions

**Commit:** Parser: parse PRIMARY KEY and UNIQUE column constraints

---

## 77. Schema: Store Constraint Flags (Track 2)

### What Changes

- `schema::Column` gains `unique: bool` and `primary_key: bool` fields.
- `resolve_table` in `src/planner.rs` reads `ColumnDef.constraints` when loading schema.
- `plan_create_table` (in `src/planner.rs`) generates `CREATE INDEX`-equivalent metadata for constrained columns and stores them in `db_schema`.
- `LogicalPlan::CreateTable` gains `unique_columns: Vec<usize>` — the column indices that require implicit unique indexes. The engine handler (or compiler) creates these indexes in `db_schema` at `CREATE TABLE` time.

### Background

The catalog stores CREATE TABLE DDL as the source of truth. When `resolve_table` re-parses the DDL to extract columns, the parsed `ColumnDef.constraints` now carry constraint info. So schema loading picks it up for free once the parser is extended (item 76).

For implicit unique indexes: when `plan_create_table` sees a column with `PrimaryKey` or `Unique`, it adds it to `unique_columns`. The engine handler creates a new B-tree root page and inserts an entry in `db_schema` of `type='index'` with `unique=true` and the associated table/column, exactly as `CREATE INDEX` does but with a system-generated name `_pk_<table>_<col>` or `_uq_<table>_<col>`.

```rust
pub struct Column {
    pub name: String,
    pub primary_key: bool,   // NEW
    pub unique: bool,         // NEW
}
```

### Key Files

- `src/planner.rs` — `schema::Column`; `resolve_table`; `plan_create_table`; `LogicalPlan::CreateTable`
- `src/compiler/nodes.rs` — `codegen_create_table` — create implicit index entries in `db_schema`

### Tests

```rust
#[test]
fn test_schema_loads_primary_key_flag() {
    let mut db = TestDb::new();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    let table = resolve_table("users", db.btree()).unwrap();
    assert!(table.columns[0].primary_key);
    assert!(!table.columns[1].primary_key);
}
```

### Implementation Steps (2 commits)

#### Step 77.1 — Extend schema::Column with constraint flags; update resolve_table

Add `primary_key` and `unique` to `Column`; populate from parsed `ColumnDef.constraints` in `resolve_table`. Add schema test.

**Commit:** Schema: load primary_key and unique flags from column constraints

#### Step 77.2 — CREATE TABLE creates implicit unique indexes for constrained columns

`plan_create_table` populates `unique_columns`; `codegen_create_table` creates B-tree entries in `db_schema` for each. The implicit index name is `_pk_<table>_<col>` / `_uq_<table>_<col>`.

**Commit:** Compiler: CREATE TABLE creates implicit unique indexes for PRIMARY KEY / UNIQUE columns

---

## 78. Engine: `CheckUnique` Operation (Track 2)

### What Changes

A new `CheckUnique(index_cursor_reg, value_reg)` operation is added. The engine handler:

1. Encodes the value as an index key prefix (same encoding as `WriteIndex` column value part).
2. Calls `cursor.find(prefix)` on the index.
3. If the key exists, returns an error: `ExecuteError::ConstraintViolation(message)`.
4. If not found, no-op — the insert proceeds.

```rust
/// Abort with ConstraintViolation if the index already contains value_reg.
CheckUnique(Reg, Reg),   // (index_cursor_reg, value_reg)
```

`ExecuteError` (in `src/db.rs`) gains:

```rust
ConstraintViolation(String),
```

The engine's `step()` return type must be able to propagate this error. Currently `step()` returns `Result<StepSuccess, StepError>`. Add `StepError::ConstraintViolation(String)` and map to `ExecuteError::ConstraintViolation` at the `run()` boundary.

### Key Files

- `src/engine/program.rs` — `CheckUnique` variant + Display
- `src/engine.rs` — match arm; `StepError::ConstraintViolation`
- `src/db.rs` — `ExecuteError::ConstraintViolation`
- `src/compiler/emitter.rs` — add `CheckUnique` to no-jump exhaustive arm

### Tests

```rust
#[test]
fn test_check_unique_passes_when_no_duplicate() {
    // Build program: Open index cursor, WriteIndex, then CheckUnique with different value
    // Expect: no error
}

#[test]
fn test_check_unique_fails_on_duplicate() {
    // Build program: Open index cursor, WriteIndex, then CheckUnique with same value
    // Expect: ConstraintViolation error
}
```

### Implementation Steps (1 commit)

#### Step 78.1 — Add CheckUnique operation and ConstraintViolation error

**Commit:** Engine: add CheckUnique operation and ConstraintViolation error

---

## 79. Compiler: Emit `CheckUnique` on INSERT (Track 2)

### What Changes

`codegen_insert` (in `src/compiler/nodes.rs`) receives `unique_column_indexes` from the planner (via `LogicalPlan::Insert.indexes` or a new field). For each constrained column, it emits `CheckUnique(index_cursor_reg, value_reg)` immediately before `WriteIndex` and `WriteCursor`.

### Background

The existing `WriteIndex` path in `codegen_insert` already opens a cursor for each index and encodes values. Inserting `CheckUnique` before `WriteIndex` reuses the same cursor and value registers — no extra allocation is needed.

```rust
// For each unique/pk index:
ctx.body_emitter.emit(Operation::CheckUnique(
    index_cursor_regs[i],
    index_col_regs[i][0],   // single-column constraint in V1
));
ctx.body_emitter.emit(Operation::WriteIndex(
    index_cursor_regs[i],
    index_col_regs[i].clone(),
    key_reg,
));
```

The planner needs to flag which indexes are unique. Extend `IndexMaintenanceInfo`:

```rust
pub struct IndexMaintenanceInfo {
    pub rootpage: u32,
    pub column_idxs: Vec<usize>,
    pub unique: bool,    // NEW — true for implicit PK/UNIQUE indexes
}
```

`plan_insert` sets `unique = true` for implicit indexes created in item 77, `false` for user-created `CREATE INDEX` indexes.

### Key Files

- `src/planner.rs` — `IndexMaintenanceInfo.unique`; `plan_insert` marks implicit indexes as unique
- `src/compiler/nodes.rs` — `codegen_insert`: emit `CheckUnique` before `WriteIndex` when `unique`

### Tests

Integration tests (deferred to item 80).

### Implementation Steps (1 commit)

#### Step 79.1 — Emit CheckUnique in codegen_insert for unique indexes

Extend `IndexMaintenanceInfo` with `unique`, update `plan_insert`, emit `CheckUnique` in `codegen_insert`. Full test suite must pass.

**Commit:** Compiler: emit CheckUnique before WriteIndex for unique/primary-key columns

---

## 80. SQL Regression Tests (Track 7)

### What Changes

`tests/sql/unique_constraints.sql` — comprehensive coverage of UNIQUE and PRIMARY KEY enforcement.

### Test File

```sql
-- tests/sql/unique_constraints.sql

CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)
-- > Table 'users' created

INSERT INTO users VALUES (1, 'alice')
-- > 1 row inserted

INSERT INTO users VALUES (2, 'bob')
-- > 1 row inserted

-- Duplicate primary key — must error
INSERT INTO users VALUES (1, 'duplicate')
-- > ERROR: constraint violation

-- Non-duplicate succeeds
INSERT INTO users VALUES (3, 'carol')
-- > 1 row inserted

SELECT id FROM users ORDER BY id
-- > 1
-- > 2
-- > 3

-- UNIQUE column (not pk)
CREATE TABLE emails (id INTEGER, addr TEXT UNIQUE)
-- > Table 'emails' created

INSERT INTO emails VALUES (1, 'a@example.com')
-- > 1 row inserted

INSERT INTO emails VALUES (2, 'b@example.com')
-- > 1 row inserted

INSERT INTO emails VALUES (3, 'a@example.com')
-- > ERROR: constraint violation

SELECT id FROM emails ORDER BY id
-- > 1
-- > 2

-- NULL values are not checked for uniqueness (standard SQL behaviour: two NULLs are allowed)
-- (deferred to follow-up phase if NULL in indexes not yet supported)
```

### Key Files

- `tests/sql/unique_constraints.sql` — new test file

### Tests

`cargo test test_sql_unique_constraints`

### Implementation Steps (1 commit)

#### Step 80.1 — Add SQL regression tests for UNIQUE and PRIMARY KEY

**Commit:** Tests: SQL regression tests for UNIQUE and PRIMARY KEY constraints

---

## Verification

- [ ] Tests pass: `cargo test`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Each commit is independently testable
- [ ] `INSERT` with duplicate PRIMARY KEY → clear `ConstraintViolation` error
- [ ] `INSERT` with duplicate UNIQUE column → clear `ConstraintViolation` error
- [ ] `INSERT` with non-duplicate values → succeeds as before
- [ ] `cargo test test_sql_unique_constraints` — all constraint tests pass
- [ ] Existing INSERT tests (`cargo test test_sql_`) — all unaffected
- [ ] `CREATE TABLE t (id INTEGER PRIMARY KEY)` → implicit index visible in `btree tables`
