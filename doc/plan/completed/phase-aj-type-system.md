# Phase AJ — Type System & Schema Compatibility

Make the sakila `sqlite-sakila-schema.sql` CREATE TABLE statements execute successfully, and load the INSERT data, by expanding the parser's type vocabulary, adding DEFAULT value support, skipping unsupported constraint syntax, and applying implicit type coercion on INSERT.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 107 | 1 | Lexer: block comment support `/* ... */` | — |
| 108 | 1 | Parser: type aliases — VARCHAR, CHAR, INT, SMALLINT, DECIMAL, TIMESTAMP, etc. | — |
| 109.1 | 1 | Refactor: move unique determination to planner; store UNIQUE DDL for implicit indexes | — |
| 109.2 | 1 | Parser: `CREATE UNIQUE INDEX` syntax | 109.1 |
| 110 | 1 | Parser: table-level constraints — skip CONSTRAINT, FOREIGN KEY, CHECK | 108 |
| 111 | 1 | Parser + AST: DEFAULT value in column definitions | 108 |
| 112 | 2 | Schema: propagate `DataType` through `Column`; fix index validation | 108 |
| 113 | 2 | INSERT: implicit type coercion (string '1' → integer 1) | 112 |
| 114 | 2 | INSERT: fill omitted columns with DEFAULT or NULL | 111, 112 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

The sakila schema currently fails at the very first token of the first CREATE TABLE because `VARCHAR` is not a recognised type keyword, and the block comment `/* ... */` at the top of the file is not handled. After this phase, the full schema (without triggers or views) loads cleanly, and the INSERT data file populates all 18 tables.

### What is stripped for the milestone

For the "AG/AJ milestone" of running sakila, triggers and views are still removed from the schema file (they require phases AL and AN). Foreign key constraints are parsed and silently ignored (enforcement is phase AV). Everything else loads.

### Sakila-specific edge cases discovered

1. **Block comments** — the schema file opens with `/* ... */`, which our lexer doesn't handle.
2. **`BLOB SUB_TYPE TEXT`** — a Firebird/InterBase-style type annotation that must be skipped.
3. **`CREATE UNIQUE INDEX`** — the rental table's uniqueness constraint uses this form, which the parser doesn't currently accept.
4. **Quoted integer values in INSERT** — sakila's ETL-generated INSERT files wrap all values in single quotes: `Values ('1','English','2006-02-15 05:02:19.000')`. The `language_id` column is INTEGER, so `'1'` must be coerced.
5. **`DEFAULT` before `NOT NULL`** — sakila uses `rental_duration SMALLINT DEFAULT 3 NOT NULL`, so DEFAULT can appear before other constraints.
6. **Table-level constraints mixed with columns** — after the last column def, `PRIMARY KEY (col_id)`, `CONSTRAINT name FOREIGN KEY ...`, and `CONSTRAINT name CHECK(...)` appear as comma-separated items. The parser loop currently tries to parse these as column defs.

---

## Stubs

| Stub | Behaviour | TODO marker | Completed by |
|------|-----------|-------------|--------------|
| FOREIGN KEY constraints | Parsed and silently ignored by `skip_table_constraint()` | `TODO(phase-aj): enforce FK referential integrity` | Phase AV |
| CHECK constraints | Parsed and silently ignored by `skip_table_constraint()` | `TODO(phase-aj): enforce CHECK expressions` | Phase AV |
| NOT NULL + missing default on INSERT | Omitted columns without a DEFAULT are filled with NULL rather than rejected | `TODO(phase-aj): reject NOT NULL columns with no default` | Future (NOT NULL enforcement phase) |
| Expression DEFAULTs e.g. `DEFAULT (DATETIME('now'))` | Parenthesised expression consumed and treated as no default | `TODO(phase-aj): evaluate expression defaults` | Phase AO (date/time functions) |

---

## 107. Lexer: block comment support `/* ... */` (Track 1)

### What Changes

`skip_whitespace()` in `src/frontend/lexer.rs` currently only handles `--` line comments. Add a `/* ... */` block comment handler.

### Implementation Approach

In `skip_whitespace()`, after detecting `/` followed by `*`, consume all characters until `*/` is found (or EOF). Nested block comments are **not** supported (matching SQLite behaviour).

```rust
'/' if self.peek_next() == '*' => {
    self.advance(); // consume '/'
    self.advance(); // consume '*'
    loop {
        if self.is_at_end() { break; }
        if self.peek() == '*' && self.peek_next() == '/' {
            self.advance(); // consume '*'
            self.advance(); // consume '/'
            break;
        }
        if self.peek() == '\n' { self.line += 1; self.column = 0; }
        self.advance();
    }
}
```

### Key Files

- `src/frontend/lexer.rs` — `skip_whitespace()`

### Tests

```rust
#[test]
fn test_block_comment_ignored() {
    let tokens = lex("/* this is a comment */ SELECT");
    assert!(tokens.iter().any(|t| matches!(t.tipe(), Type::Select)));
}

#[test]
fn test_block_comment_multiline() {
    let tokens = lex("/*\nline one\nline two\n*/ 42");
    assert!(matches!(tokens[0].tipe(), Type::IntegerNumber(42)));
}
```

### Implementation Steps (1 commit)

#### Step 107.1 — Lexer: handle `/* */` block comments in `skip_whitespace`

**Commit:** `Lexer: add block comment support (/* ... */)`

---

## 108. Parser: type aliases (Track 1)

### What Changes

`parse_optional_data_type()` in `src/frontend/parser.rs` currently only matches the four `lexer::Type` keyword variants (Integer, Text, Real, Blob). All other type names arrive as `Identifier` tokens. This item adds alias recognition by matching on identifier values.

### Type Mapping

| SQL type(s) | Stored as | Notes |
|-------------|-----------|-------|
| `VARCHAR(n)`, `CHAR(n)` | `DataType::Text` | Consume `(n)` parameter |
| `INT`, `SMALLINT`, `TINYINT`, `BIGINT` | `DataType::Integer` | No parameter |
| `DECIMAL(p,s)`, `NUMERIC(p,s)` | `DataType::Real` | Consume `(p,s)` parameter |
| `TIMESTAMP`, `DATETIME`, `DATE`, `TIME` | `DataType::Text` | No parameter |
| `BLOB SUB_TYPE TEXT` | `DataType::Blob` | Consume two extra identifier tokens |

### Implementation Approach

```rust
fn parse_optional_data_type(&mut self) -> Option<ast::DataType> {
    match self.input.peek() {
        lexer::Type::Integer => { self.input.advance(); Some(ast::DataType::Integer) }
        lexer::Type::Text    => { self.input.advance(); Some(ast::DataType::Text) }
        lexer::Type::Real    => { self.input.advance(); Some(ast::DataType::Real) }
        lexer::Type::Blob    => {
            self.input.advance();
            // Handle "BLOB SUB_TYPE TEXT" (Firebird annotation)
            if let lexer::Type::Identifier(s) = self.input.peek() {
                if s.to_lowercase() == "sub_type" {
                    self.input.advance(); // consume SUB_TYPE
                    self.input.advance(); // consume TEXT (Type::Text keyword)
                }
            }
            Some(ast::DataType::Blob)
        }
        lexer::Type::Identifier(s) => {
            let dt = match s.to_lowercase().as_str() {
                "varchar" | "char" | "nvarchar" | "nchar" => {
                    self.input.advance();
                    self.consume_optional_type_param(); // skip (n)
                    Some(ast::DataType::Text)
                }
                "int" | "smallint" | "tinyint" | "bigint" | "int4" | "int8" => {
                    self.input.advance();
                    Some(ast::DataType::Integer)
                }
                "decimal" | "numeric" => {
                    self.input.advance();
                    self.consume_optional_type_param(); // skip (p,s)
                    Some(ast::DataType::Real)
                }
                "timestamp" | "datetime" | "date" | "time" => {
                    self.input.advance();
                    Some(ast::DataType::Text)
                }
                _ => None,
            };
            dt
        }
        _ => None,
    }
}

/// Consume an optional `(...)` parameter list after a type name, e.g. `(45)` or `(5,2)`.
fn consume_optional_type_param(&mut self) {
    if matches!(self.input.peek(), lexer::Type::LeftParen) {
        self.input.advance(); // consume '('
        let mut depth = 1;
        loop {
            match self.input.peek() {
                lexer::Type::LeftParen  => { depth += 1; self.input.advance(); }
                lexer::Type::RightParen => {
                    depth -= 1;
                    self.input.advance();
                    if depth == 0 { break; }
                }
                lexer::Type::Eof => break,
                _ => { self.input.advance(); }
            }
        }
    }
}
```

### Key Files

- `src/frontend/parser.rs` — `parse_optional_data_type()` + new `consume_optional_type_param()`

### Tests

```sql
-- tests/sql/type_aliases.sql
CREATE TABLE t1 (
  a VARCHAR(45) NOT NULL,
  b CHAR(1),
  c INT NOT NULL,
  d SMALLINT,
  e DECIMAL(4,2),
  f TIMESTAMP,
  g DATETIME,
  h BLOB SUB_TYPE TEXT
)
-- > Table 't1' created

INSERT INTO t1 VALUES ('hello', 'Y', 1, 2, 3.14, '2024-01-01', '2024-01-01', 'data')
-- > 1 row inserted

SELECT a, c FROM t1
-- > hello | 1
```

### Implementation Steps (1 commit)

#### Step 108.1 — Parser: type aliases and parameterised types in `parse_optional_data_type`

**Commit:** `Parser: accept SQL type aliases (VARCHAR, INT, DECIMAL, TIMESTAMP, etc.)`

---

## 109. Parser: `CREATE UNIQUE INDEX` syntax (Track 1)

### What Changes

The sakila schema contains:
```sql
CREATE UNIQUE INDEX idx_rental_uq ON rental (rental_date,inventory_id,customer_id);
```

The parser currently handles `CREATE INDEX` but not `CREATE UNIQUE INDEX`. After `CREATE`, if it sees `UNIQUE`, it returns an error.

### Implementation Approach

#### Step 109.1 — Refactor: move unique determination out of the storage layer (preparatory)

`lookup_indexes_for_table()` in `btree.rs` currently derives `unique` from the `_pk_`/`_uq_` name prefix. This is wrong in principle — the storage layer should not interpret DDL semantics. SQLite's model is the right one: uniqueness is recorded in the DDL and inferred by parsing it at a higher layer.

Changes (existing code only, no new SQL syntax):

1. **`db.rs`** — fix the implicit index DDL stored for PRIMARY KEY and UNIQUE column constraints to use `CREATE UNIQUE INDEX` instead of `CREATE INDEX`. The stored SQL becomes the single source of truth for uniqueness.
2. **`btree.rs`** — `IndexInfo` drops `unique: bool` and adds `sql: String`. `lookup_indexes_for_table()` removes the `_pk_`/`_uq_` prefix check entirely and returns the raw SQL. The `_pk_`/`_uq_` names are retained for namespacing but are no longer load-bearing.
3. **`dml.rs`** — when building `IndexMaintenanceInfo` from `IndexInfo`, call `parse(&info.sql)` to get a `CreateIndexStatement` and read `stmt.unique`. This mirrors `resolve_table()`.

After this step, existing PRIMARY KEY and UNIQUE column constraint enforcement is unchanged — only the mechanism for determining uniqueness has moved to the correct layer.

#### Step 109.2 — Parser + AST: `CREATE UNIQUE INDEX` syntax (new feature)

In the `Statement::Create` arm of the top-level parser, after consuming `CREATE`, peek:
- If `UNIQUE` → advance past `UNIQUE`, expect `INDEX`, then call `parse_create_index_statement()`
- If `INDEX` → call `parse_create_index_statement()` as before

Add a `unique: bool` field to `CreateIndexStatement`:

```rust
#[derive(Debug)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub column_names: Vec<String>,
    pub unique: bool,     // new field
}
```

Parser change:
```rust
lexer::Type::Create => {
    self.input.advance();
    match self.input.peek() {
        lexer::Type::Unique => {
            self.input.advance(); // consume UNIQUE
            Ok(ast::Statement::CreateIndex(
                self.parse_create_index_statement_with_unique(true)?
            ))
        }
        lexer::Type::Table => ...
        lexer::Type::Index => Ok(ast::Statement::CreateIndex(
            self.parse_create_index_statement_with_unique(false)?
        )),
        ...
    }
}
```

In `db.rs`, the `CreateIndex` handler stores `CREATE UNIQUE INDEX` in the catalog DDL when `ci.unique` is true — consistent with step 109.1's convention. The planner (`dml.rs`) already parses the SQL after step 109.1, so unique enforcement for the new index is automatic.

### Key Files

- `src/storage/btree.rs` — `IndexInfo`: drop `unique`, add `sql`; remove prefix check (step 109.1)
- `src/db.rs` — store `CREATE UNIQUE INDEX` for implicit unique indexes (step 109.1) and explicit (step 109.2)
- `src/planner/dml.rs` — parse `IndexInfo.sql` to determine `unique` (step 109.1)
- `src/frontend/ast.rs` — `CreateIndexStatement.unique` field (step 109.2)
- `src/frontend/parser.rs` — `CREATE UNIQUE INDEX` dispatch (step 109.2)

### Tests

```sql
-- tests/sql/create_unique_index.sql
CREATE TABLE rental (id INTEGER, rental_date TEXT, inventory_id INTEGER, customer_id INTEGER)
-- > Table 'rental' created

CREATE UNIQUE INDEX idx_rental_uq ON rental (rental_date, inventory_id, customer_id)
-- > Index 'idx_rental_uq' created

INSERT INTO rental VALUES (1, '2005-05-24', 367, 130)
-- > 1 row inserted

INSERT INTO rental VALUES (2, '2005-05-24', 367, 130)
-- > ERROR: UNIQUE constraint violation
```

### Implementation Steps (2 commits)

#### Step 109.1 — Refactor: unique determination via DDL parse; remove name-prefix check from storage

**Commit:** `Storage: move unique index determination to planner; store UNIQUE DDL for implicit indexes`

#### Step 109.2 — Parser + AST: `CREATE UNIQUE INDEX`; propagate `unique` flag through db.rs

**Commit:** `Parser: support CREATE UNIQUE INDEX syntax`

---

## 110. Parser: table-level constraints — skip (Track 1)

### What Changes

The `parse_create_table_statement_after_create()` loop currently tries to parse every comma-separated item as a column definition. Table-level constraints (`PRIMARY KEY (cols)`, `CONSTRAINT name FOREIGN KEY ...`, `CONSTRAINT name CHECK(...)`) appear after the last column definition and cause parse errors.

The fix: after consuming a `,`, peek at the next token. If it indicates a table-level constraint, consume and discard the whole constraint entry.

### Token Recognition

| Peeked token | Type |
|---|---|
| `Type::Primary` | Table-level `PRIMARY KEY (...)` |
| `Type::Unique` | Table-level `UNIQUE KEY (...)` |
| `Identifier("constraint")` | Named constraint — any kind |
| `Identifier("foreign")` | Unnamed `FOREIGN KEY (...)` |
| `Identifier("check")` | Unnamed `CHECK(...)` |

### Implementation Approach

```rust
fn is_table_level_constraint(t: &lexer::Type) -> bool {
    match t {
        lexer::Type::Primary | lexer::Type::Unique => true,
        lexer::Type::Identifier(s) => matches!(s.as_str(),
            "constraint" | "foreign" | "check"),
        _ => false,
    }
}

fn skip_table_constraint(&mut self) {
    // Consume tokens until the next unbalanced ',' or ')' at depth 0
    let mut depth = 0usize;
    loop {
        match self.input.peek() {
            lexer::Type::LeftParen  => { depth += 1; self.input.advance(); }
            lexer::Type::RightParen if depth > 0 => { depth -= 1; self.input.advance(); }
            lexer::Type::RightParen | lexer::Type::Eof => break,  // end of CREATE TABLE
            lexer::Type::Comma if depth == 0 => break,            // next item
            _ => { self.input.advance(); }
        }
    }
}
```

In the column-parsing loop:
```rust
while let lexer::Type::Comma = self.input.peek() {
    self.input.advance(); // consume ','
    if is_table_level_constraint(&self.input.peek()) {
        self.skip_table_constraint();
    } else {
        columns.push(self.parse_column_def()?);
    }
}
```

### Key Files

- `src/frontend/parser.rs` — `parse_create_table_statement_after_create()` + `skip_table_constraint()`

### Tests

```sql
-- tests/sql/table_constraints.sql
CREATE TABLE city (
  city_id INTEGER NOT NULL,
  city VARCHAR(50) NOT NULL,
  country_id INT NOT NULL,
  last_update TIMESTAMP NOT NULL,
  PRIMARY KEY (city_id),
  CONSTRAINT fk_city_country FOREIGN KEY (country_id) REFERENCES country (country_id) ON DELETE NO ACTION ON UPDATE CASCADE
)
-- > Table 'city' created

CREATE TABLE film (
  film_id INTEGER NOT NULL,
  title VARCHAR(255) NOT NULL,
  special_features VARCHAR(100) DEFAULT NULL,
  CONSTRAINT CHECK_special_features CHECK(special_features is null or special_features like '%Trailers%'),
  CONSTRAINT CHECK_special_rating CHECK(rating in ('G','PG'))
)
-- > Table 'film' created
```

### Implementation Steps (1 commit)

#### Step 110.1 — Parser: detect and skip table-level constraints in CREATE TABLE

**Commit:** `Parser: skip table-level CONSTRAINT / FOREIGN KEY / CHECK / PRIMARY KEY`

---

## 111. Parser + AST: DEFAULT value in column definitions (Track 1)

### What Changes

Column definitions in sakila use `DEFAULT NULL`, `DEFAULT 'Y'`, `DEFAULT 3`, `DEFAULT 4.99`. These appear in the column constraint list (in any order — `DEFAULT` can appear before or after `NOT NULL`).

### AST Change

```rust
// src/frontend/ast.rs
pub struct ColumnDef {
    pub name: String,
    pub type_name: Option<DataType>,
    pub constraints: Vec<ColumnConstraint>,
    pub default: Option<DefaultValue>,   // new
}

/// A DEFAULT value as parsed from DDL.
#[derive(Debug, Clone, PartialEq)]
pub enum DefaultValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
}
```

### Parser Change

In `parse_column_constraints()`, add a `DEFAULT` arm. A `Default` lexer token is added for this.

**Lexer** (`lexer.rs`): add `Type::Default` and `"default"` keyword to the identifier matcher (under `'d'`).

**Parser**: parse the default value expression as one of the literal types. For simplicity, only literal defaults are supported (which covers all sakila cases). Expression defaults like `DEFAULT (DATETIME('now'))` — which sakila does NOT use in this form since they're handled by triggers — are consumed but ignored (the parenthesized expression is skipped with `consume_optional_type_param`).

```rust
lexer::Type::Default => {
    self.input.advance(); // consume DEFAULT
    let val = match self.input.peek() {
        lexer::Type::Null => {
            self.input.advance();
            Some(ast::DefaultValue::Null)
        }
        lexer::Type::String(s) => {
            self.input.advance();
            Some(ast::DefaultValue::Text(s))
        }
        lexer::Type::IntegerNumber(n) => {
            self.input.advance();
            Some(ast::DefaultValue::Integer(n))
        }
        lexer::Type::FloatingPointNumber(f) => {
            self.input.advance();
            Some(ast::DefaultValue::Float(f))
        }
        lexer::Type::Minus => {
            // Handle DEFAULT -1, DEFAULT -4.99 etc.
            self.input.advance();
            match self.input.peek() {
                lexer::Type::IntegerNumber(n) => {
                    self.input.advance();
                    Some(ast::DefaultValue::Integer(-n))
                }
                lexer::Type::FloatingPointNumber(f) => {
                    self.input.advance();
                    Some(ast::DefaultValue::Float(-f))
                }
                _ => None,
            }
        }
        lexer::Type::LeftParen => {
            // DEFAULT (expr) — skip the whole parenthesized expression
            self.consume_optional_type_param();
            None // treat as no default (expression defaults handled elsewhere)
        }
        _ => None, // unrecognised default, skip
    };
    // Store val in the ColumnDef being built — returned to caller
    default = val;
}
```

The `parse_column_def()` function needs a local `default` variable that accumulates across the constraint loop, then sets `ColumnDef::default`.

### Key Files

- `src/frontend/lexer.rs` — `Type::Default` keyword
- `src/frontend/ast.rs` — `ColumnDef::default`, `DefaultValue` enum
- `src/frontend/parser.rs` — `parse_column_constraints()` DEFAULT arm

### Tests

```sql
-- tests/sql/default_values.sql
CREATE TABLE t (
  id INTEGER NOT NULL,
  label VARCHAR(20) DEFAULT 'unlabelled',
  score DECIMAL(4,2) DEFAULT 0.0,
  active SMALLINT DEFAULT 1 NOT NULL,
  note TEXT DEFAULT NULL
)
-- > Table 't' created
```

### Implementation Steps (2 commits)

#### Step 111.1 — Lexer + AST: `Type::Default` token, `DefaultValue` enum, `ColumnDef::default` field

**Commit:** `AST: add DefaultValue enum and ColumnDef::default field; lexer: add DEFAULT keyword`

#### Step 111.2 — Parser: parse DEFAULT literal in `parse_column_constraints`

**Commit:** `Parser: parse DEFAULT literal values in column definitions`

---

## 112. Schema: propagate DataType through Column (Track 2)

### What Changes

`Column` in `src/planner/schema.rs` currently has no type information — only `primary_key` and `unique`. This prevents type coercion in INSERT and causes incorrect index creation validation for aliased types (a `VARCHAR(45)` column arrives as `None` type and is rejected by the index validator).

### Schema Change

```rust
// src/planner/schema.rs
pub struct Column {
    pub name: String,
    pub data_type: Option<DataType>,   // new
    pub primary_key: bool,
    pub unique: bool,
    pub default: Option<DefaultValue>, // new — from ColumnDef::default
}
```

`resolve_table()` already re-parses the DDL from the catalog; it just needs to copy `col.type_name` and `col.default` into `Column`.

### Index Creation Fix

In `db.rs`, the `CreateIndex` handler currently rejects non-Integer/non-Text columns:

```rust
if !matches!(column_def.type_name, Some(DataType::Integer) | Some(DataType::Text)) {
    return Err(ExecuteError::ColumnNotInteger { ... });
}
```

With type aliases properly resolved (e.g. `VARCHAR` → `DataType::Text`), this check will naturally pass. The error message can be updated to be more accurate: "only INTEGER and TEXT columns support indexing".

### Key Files

- `src/planner/schema.rs` — `Column` struct, `resolve_table()`
- `src/db.rs` — index creation now works for TEXT aliases

### Tests

```rust
#[test]
fn test_schema_preserves_varchar_as_text() {
    // CREATE TABLE with VARCHAR column → Column.data_type == Some(DataType::Text)
}

#[test]
fn test_index_on_varchar_column_succeeds() {
    // CREATE INDEX on a VARCHAR column must not return ColumnNotInteger
}
```

### Implementation Steps (1 commit)

#### Step 112.1 — Schema: add `data_type` and `default` to `Column`; fix index validation

**Commit:** `Schema: propagate DataType and DefaultValue through Column; fix index validation for type aliases`

---

## 113. INSERT: implicit type coercion (Track 2)

### What Changes

Sakila's INSERT files were generated by an ETL tool that wraps every value in single quotes:

```sql
Insert into language (language_id, name, last_update)
Values ('1', 'English', '2006-02-15 05:02:19.000');
```

`language_id` is an `INTEGER` column. The literal `'1'` is parsed as `Literal::String("1")`, which gets stored as a text cell in the B-tree. When later read as an integer, comparisons and index lookups fail.

The fix: in `plan_insert()`, after evaluating each value literal, compare it to the target column's declared type and coerce if needed.

### Coercion Rules

| Column type | Value type | Action |
|---|---|---|
| Integer | String | `s.parse::<i64>()` → `Literal::Integer` or error |
| Real | String | `s.parse::<f64>()` → `Literal::Float` or error |
| Real | Integer | `Literal::Float(n as f64)` |
| Integer | Float | `Literal::Integer(f as i64)` (truncate) |
| Text | any | no coercion (everything is representable as text) |
| Blob | any | no coercion |
| * | Null | always pass through as Null |

Only coerce in INSERT (not in UPDATE expressions, which are planned separately and can access the column resolver for type info if needed later).

### Implementation Approach

In `dml.rs`:

```rust
fn coerce_literal(lit: Literal, target_type: Option<&DataType>) -> Result<Literal, PlanError> {
    match (lit, target_type) {
        (Literal::String(s), Some(DataType::Integer)) => {
            s.parse::<i64>()
                .map(Literal::Integer)
                .map_err(|_| PlanError::TypeMismatch { expected: "INTEGER".into(), got: s })
        }
        (Literal::String(s), Some(DataType::Real)) => {
            s.parse::<f64>()
                .map(Literal::Float)
                .map_err(|_| PlanError::TypeMismatch { expected: "REAL".into(), got: s })
        }
        (Literal::Integer(n), Some(DataType::Real)) => Ok(Literal::Float(n as f64)),
        (other, _) => Ok(other),  // pass through
    }
}
```

Call this after `eval_constant()` for each value in a VALUES row, using `table.columns[table_columns[i]].data_type` as the target type.

A new `PlanError::TypeMismatch` variant is added.

### Key Files

- `src/planner/dml.rs` — `plan_insert()`, new `coerce_literal()` helper
- `src/planner/mod.rs` — `PlanError::TypeMismatch`

### Tests

```sql
-- tests/sql/type_coercion.sql
CREATE TABLE t (id INTEGER, rate REAL, label TEXT)
-- > Table 't' created

INSERT INTO t (id, rate, label) VALUES ('1', '4.99', 42)
-- > 1 row inserted

SELECT id, rate FROM t
-- > 1 | 4.99
```

### Implementation Steps (1 commit)

#### Step 113.1 — INSERT: coerce string literals to target column type

**Commit:** `INSERT: implicit type coercion for string-quoted numeric values`

---

## 114. INSERT: fill omitted columns with DEFAULT or NULL (Track 2)

### What Changes

When an INSERT specifies an explicit column list that omits some columns, the omitted columns currently cause a column count mismatch error. With DEFAULT values now stored in `Column`, omitted columns can be filled in automatically.

### Behaviour

1. If the omitted column has `default: Some(v)` → use `v` converted to `Literal`
2. If the omitted column has `default: None` and `not_null: false` → use `Literal::Null`
3. If the omitted column has `default: None` and `not_null: true` → error: `PlanError::MissingRequiredColumn`

> Note: `not_null` is not yet stored in `Column`. For this phase, treat all omitted columns without defaults as `Null` (permissive), matching SQLite's default behaviour. The NOT NULL enforcement path can be tightened in a later phase.

### Implementation Approach

In `plan_insert()`, instead of failing when `table_columns.len() != num_table_columns` (for the no-explicit-columns case) or checking value count against `table_columns.len()`, after building each `literals` row for the provided columns, produce a full-width row filled with defaults:

```rust
fn make_full_row(
    provided: &[(usize, Literal)],      // (column_index, value)
    all_columns: &[Column],
) -> Vec<Literal> {
    let mut row = all_columns.iter().map(|col| {
        col.default.as_ref().map(default_value_to_literal).unwrap_or(Literal::Null)
    }).collect::<Vec<_>>();
    for (col_idx, lit) in provided {
        row[*col_idx] = lit.clone();
    }
    row
}
```

The `LogicalPlan::Insert.table_columns` field would then always be `0..num_table_columns` (all columns), and the compiler emits a full row. This simplifies the compiler too.

### Key Files

- `src/planner/dml.rs` — `plan_insert()`, new `make_full_row()` helper
- `src/planner/mod.rs` — `PlanError::MissingRequiredColumn` (future)

### Tests

```sql
-- tests/sql/default_insert.sql
CREATE TABLE t (
  id INTEGER NOT NULL,
  label TEXT DEFAULT 'unknown',
  score REAL DEFAULT 0.0
)
-- > Table 't' created

INSERT INTO t (id) VALUES (1)
-- > 1 row inserted

SELECT id, label, score FROM t
-- > 1 | unknown | 0.0

INSERT INTO t (id, label) VALUES (2, 'hello')
-- > 1 row inserted

SELECT id, label, score FROM t WHERE id = 2
-- > 2 | hello | 0.0
```

### Implementation Steps (1 commit)

#### Step 114.1 — INSERT: fill omitted columns with DEFAULT or NULL

**Commit:** `INSERT: fill omitted columns with DEFAULT value or NULL`

---

## Verification

- [ ] `cargo test` — all existing tests pass
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] Parse the full sakila schema (with triggers and views stripped): `cargo run -- sakila.db sql < sqlite-sakila-schema-stripped.sql`
- [ ] All 16 CREATE TABLE statements succeed
- [ ] All 24 CREATE INDEX statements succeed (23 regular + 1 `CREATE UNIQUE INDEX`)
- [ ] Load the full INSERT data: all ~46k rows inserted across 16 tables
- [ ] Integer coercion: `'1'` inserted into an INTEGER column reads back as integer 1
- [ ] DEFAULT: omitting a column with a DEFAULT uses the default value
- [ ] Block comments `/* ... */` at the start of the schema file are skipped
- [ ] `BLOB SUB_TYPE TEXT` is accepted and stored as Blob
- [ ] `CONSTRAINT name FOREIGN KEY ...` is silently ignored
- [ ] `CONSTRAINT name CHECK(...)` is silently ignored
- [ ] Each commit is independently testable
