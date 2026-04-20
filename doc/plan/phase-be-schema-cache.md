# Phase BE — Cache Parsed Table Schema: Eliminate DDL Re-parsing

Parse each table's CREATE TABLE DDL exactly once during `CatalogSnapshot::build()` and store
the result as a `TableInfo` struct, so `resolve_table` and the optimizer perform a cheap map
lookup instead of re-parsing the DDL string on every query.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 138 | 3 | Add `TableInfo`/`ColumnInfo` to `CatalogSnapshot`; parse DDL once in `build()`; add `lookup_table_info()`; update `resolve_table()` to use it | — |
| 139 | 4 | Fix optimizer: replace three `parse(&sql)` call sites with `lookup_table_info()` | 138 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

`perf record` on the Sakila INSERT benchmark shows
`database::planner::schema::resolve_table` at **17 %** of total execution time. The hot
path is:

```
execute() → plan() → resolve_table() → catalog.lookup_table() → parse(&sql)
```

`CatalogSnapshot` already caches the raw `(rootpage, DDL_string)` pair so the B-tree scan
happens only once per DDL change. But `resolve_table` still calls `parse(&sql)` on every
query — including every one of the ~15 000 INSERTs in the Sakila data load — to extract
column names, types, and constraints from the DDL string. The parsed `Table` struct is
discarded after each use.

Separately, the optimizer has three more `parse(&sql)` call sites that re-parse the same
DDL string to resolve a column name to a positional index when checking whether an index
covers a scan or a join predicate.

The fix is to parse each table's DDL **once** during `CatalogSnapshot::build()` and cache
the result as a `TableInfo` struct. All subsequent lookups are a `HashMap::get`, which is
O(1) with no allocation.

`catalog_cache.rs` already imports `crate::frontend::{parse, ast::Statement}` for index
DDL parsing, so adding `TableInfo` population there incurs no new cross-layer dependency.

---

## Stubs

None.

---

## 138. Cache parsed table schema in `CatalogSnapshot` (Track 3)

### What Changes

#### New types in `src/storage/catalog_cache.rs`

```rust
use crate::frontend::ast::{ColumnConstraint, DataType, DefaultValue, Statement};

/// Parsed column metadata extracted once during `CatalogSnapshot::build()`.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: Option<DataType>,
    pub default: Option<DefaultValue>,
    pub primary_key: bool,
    pub unique: bool,
}

/// Parsed table metadata stored in the catalog snapshot.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub rootpage: u32,
    pub columns: Vec<ColumnInfo>,
}
```

#### `CatalogSnapshot` — add `parsed_tables` field

```rust
#[derive(Clone, Default)]
pub struct CatalogSnapshot {
    pub tables: HashMap<String, (u32, String)>,           // unchanged; still needed by cold paths
    pub(super) by_rootpage: HashMap<u32, String>,
    pub indexes: HashMap<String, Vec<IndexInfo>>,
    pub parsed_tables: HashMap<String, TableInfo>,        // NEW
}
```

The raw `tables` map is kept because several cold-path callers (`db.rs` CREATE TABLE
existence check, CREATE INDEX column validation, REPL commands) use `lookup_table()` and
are not on any hot path. Removing them is a separate cleanup concern.

#### `CatalogSnapshot::build()` — parse table DDL once

```rust
"table" => {
    snapshot.by_rootpage.insert(rootpage, name.clone());
    snapshot.tables.insert(name.clone(), (rootpage, sql.clone()));
    // Parse once; silently skip if DDL is malformed (should not happen in practice).
    if let Some(info) = parse_table_info(rootpage, &sql) {
        snapshot.parsed_tables.insert(name, info);
    }
}
```

#### New private helper `parse_table_info`

```rust
fn parse_table_info(rootpage: u32, sql: &str) -> Option<TableInfo> {
    let ct = match parse(sql) {
        Ok(Statement::CreateTable(ct)) => ct,
        _ => return None,
    };
    Some(TableInfo {
        rootpage,
        columns: ct
            .columns
            .into_iter()
            .map(|col| ColumnInfo {
                name: col.name,
                data_type: col.type_name,
                default: col.default,
                primary_key: col.constraints.contains(&ColumnConstraint::PrimaryKey),
                unique: col.constraints.contains(&ColumnConstraint::Unique)
                    || col.constraints.contains(&ColumnConstraint::PrimaryKey),
            })
            .collect(),
    })
}
```

This mirrors the conversion logic currently in `resolve_table()`, consolidated here.

#### New method `CatalogSnapshot::lookup_table_info`

```rust
/// Return the pre-parsed table metadata for `table_name`, or `None` if not found.
#[inline(never)]
pub fn lookup_table_info(&self, table_name: &str) -> Option<&TableInfo> {
    probe!(database, catalog_lookup_table_info);
    self.parsed_tables.get(table_name)
}
```

#### `resolve_table()` in `src/planner/schema.rs` — remove `parse()` call

**Before:**
```rust
pub fn resolve_table(table_name: &str, catalog: &BTree) -> Result<Table, PlanError> {
    let (rootpage, sql) = catalog
        .catalog()
        .lookup_table(table_name)
        .ok_or_else(|| PlanError::TableNotFound(table_name.to_string()))?;

    let stmt = parse(&sql).map_err(|_| PlanError::UnsupportedStatement)?;
    let create = match stmt {
        Statement::CreateTable(c) => c,
        _ => return Err(PlanError::UnsupportedStatement),
    };
    let columns = create.columns.into_iter().map(|col| Column { ... }).collect();
    Ok(Table { name: table_name.to_string(), rootpage, columns })
}
```

**After:**
```rust
pub fn resolve_table(table_name: &str, catalog: &BTree) -> Result<Table, PlanError> {
    let info = catalog
        .catalog()
        .lookup_table_info(table_name)
        .ok_or_else(|| PlanError::TableNotFound(table_name.to_string()))?;

    Ok(Table {
        name: table_name.to_string(),
        rootpage: info.rootpage,
        columns: info.columns.iter().map(|col| Column {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            default: col.default.clone(),
            primary_key: col.primary_key,
            unique: col.unique,
        }).collect(),
    })
}
```

No `parse()` call; the only work is a `HashMap::get` and a `Vec::clone`.

Remove the now-unused imports from `schema.rs`:
```rust
// Remove:
use crate::frontend::ast::{ColumnConstraint, DataType, DefaultValue, Statement};
use crate::frontend::parse;
// Keep only what Column / Table definitions need:
use crate::frontend::ast::{DataType, DefaultValue};
```

### Background

`CatalogSnapshot` already parses index DDL during `build()` (via
`extract_columns_from_index_sql`) and stores the result in `IndexInfo`. This item applies
the same pattern to table DDL, replacing the deferred per-call parse in `resolve_table()`
with an eager parse at snapshot construction time.

`DataType` and `DefaultValue` are simple enums with no heap state beyond `DefaultValue::Text(String)`.
Cloning them in the `Column` conversion is cheap. The `TableInfo` stored in the snapshot
owns its data (no lifetime parameters), so `lookup_table_info` can return a plain `&TableInfo`.

### Key Files

- `src/storage/catalog_cache.rs` — `ColumnInfo`, `TableInfo`, `parsed_tables` field,
  `lookup_table_info()`, `parse_table_info()`, updated `build()`
- `src/planner/schema.rs` — `resolve_table()` rewrote; remove `parse` import

### Tests

- All existing SQL integration tests (`cargo test test_sql_`) must pass.
- All existing unit tests in `schema.rs` (`test_schema_loads_primary_key_flag`,
  `test_schema_loads_unique_flag`, `test_schema_preserves_varchar_as_text`) must pass
  unchanged — they already call `resolve_table()` through the full path.
- Add a test in `catalog_cache.rs` (or the `catalog.rs` test module) that calls
  `catalog().lookup_table_info("users")` after a `CREATE TABLE users (...)` and asserts:
  - `info.rootpage` is nonzero
  - `info.columns[0].name == "id"` and `info.columns[0].primary_key` as expected
  - `info.columns[1].name == "name"` and `info.columns[1].data_type == Some(DataType::Text)`

### Implementation Steps (1 commit)

1. Add `ColumnInfo`, `TableInfo` structs to `catalog_cache.rs`; expand `use
   crate::frontend::ast` import to include `ColumnConstraint`, `DataType`, `DefaultValue`.
2. Add `parsed_tables: HashMap<String, TableInfo>` to `CatalogSnapshot`; update
   `#[derive(Clone, Default)]` (both are already derived, `HashMap` implements both).
3. Add `parse_table_info` private helper.
4. In `CatalogSnapshot::build()`, call `parse_table_info` for each `"table"` entry and
   insert into `parsed_tables`.
5. Add `lookup_table_info()` method to `CatalogSnapshot`.
6. Rewrite `resolve_table()` in `schema.rs` to use `lookup_table_info()`; remove
   `parse` and `ColumnConstraint` / `Statement` imports.
7. Add the new unit test.
8. `cargo fmt && cargo build && cargo test`.

**Commit:** `catalog: cache parsed TableInfo in CatalogSnapshot; resolve_table uses lookup`

---

## 139. Fix optimizer: replace `parse(&sql)` with `lookup_table_info()` (Track 4)

### What Changes

The optimizer has three call sites that call `lookup_table()` to get the raw DDL string,
then immediately call `parse()` on it to find a column's position by name. After item 138,
`lookup_table_info()` makes this a direct field access.

#### Site 1 — `find_index_for_filter` (optimizer.rs ~line 299)

This finds an index whose leading column matches a filter column by resolving the index
column name to a table column index.

**Before:**
```rust
.and_then(|col_name| {
    catalog
        .catalog()
        .lookup_table(&table_name)
        .and_then(|(_, sql)| {
            parse(&sql).ok().and_then(|stmt| match stmt {
                Statement::CreateTable(ct) => {
                    ct.columns.iter().position(|c| c.name == *col_name)
                }
                _ => None,
            })
        })
})
```

**After:**
```rust
.and_then(|col_name| {
    catalog
        .catalog()
        .lookup_table_info(&table_name)
        .and_then(|info| info.columns.iter().position(|c| c.name == *col_name))
})
```

#### Site 2 — join index selection (optimizer.rs ~line 381)

Same pattern as site 1 but inside the join optimizer loop.

**Before:**
```rust
catalog
    .catalog()
    .lookup_table(&table_name)
    .and_then(|(_, sql)| {
        parse(&sql).ok().and_then(|stmt| match stmt {
            Statement::CreateTable(ct) => {
                ct.columns.iter().position(|c| c.name == *col_name)
            }
            _ => None,
        })
    })
```

**After:**
```rust
catalog
    .catalog()
    .lookup_table_info(&table_name)
    .and_then(|info| info.columns.iter().position(|c| c.name == *col_name))
```

#### Site 3 — `try_covering_index_scan` (optimizer.rs ~line 683)

This builds a `Vec<String>` of all column names to map table-column-index → column name.

**Before:**
```rust
let table_name = catalog.catalog().lookup_table_by_rootpage(table_rootpage)?;
let (_, table_sql) = catalog.catalog().lookup_table(&table_name)?;
let table_cols: Vec<String> = match parse(&table_sql).ok()? {
    Statement::CreateTable(ct) => ct.columns.into_iter().map(|c| c.name).collect(),
    _ => return None,
};
```

**After:**
```rust
let table_name = catalog.catalog().lookup_table_by_rootpage(table_rootpage)?;
let info = catalog.catalog().lookup_table_info(&table_name)?;
let table_cols: Vec<String> = info.columns.iter().map(|c| c.name.clone()).collect();
```

#### Remove unused imports from `optimizer.rs`

After all three sites are fixed, the `parse` function and `Statement` type are no longer
used in `optimizer.rs`. Remove their imports:
```rust
// Remove:
use crate::frontend::ast::Statement;
use crate::frontend::parse;
```

Verify with `cargo build 2>&1 | grep warning` that no unused-import warnings remain.

### Background

Each of the three sites was introduced to resolve a column name (from an `IndexInfo`) to a
positional index in the table schema. Without a cached parsed schema, the only way to do
this was to re-parse the DDL on the spot. With `TableInfo.columns` already populated at
snapshot time, the resolution is a `Vec::iter().position()` call with no parse step.

Sites 1 and 2 fire on every query that goes through the index selection path (i.e. every
query with a WHERE clause on an indexed column). Site 3 fires whenever the covering-index
optimisation is attempted. All three are therefore on the hot path for queries that use
indexes, including the Sakila workload after schema load.

### Key Files

- `src/planner/optimizer.rs` — three `parse(&sql)` sites replaced; remove `parse` and
  `Statement` imports

### Tests

- All existing SQL integration tests pass.
- All optimizer unit tests (if any) pass.
- Regression: run the full suite against the Sakila insert script to confirm no plan
  regressions for indexed queries.

### Implementation Steps (1 commit)

1. Replace site 1 (filter index selection) with `lookup_table_info()`.
2. Replace site 2 (join index selection) with `lookup_table_info()`.
3. Replace site 3 (`try_covering_index_scan`) with `lookup_table_info()`.
4. Remove `use crate::frontend::ast::Statement` and `use crate::frontend::parse` from
   `optimizer.rs` (verify with `cargo build` that they are truly unused).
5. `cargo fmt && cargo build && cargo test`.

**Commit:** `optimizer: replace parse(&sql) with lookup_table_info(); remove parse import`

---

## Verification

- [ ] `cargo test` — all tests pass after each commit independently
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `cargo test test_sql_` — all SQL integration tests pass
- [ ] `resolve_table()` in `schema.rs` has no `parse(...)` call
- [ ] `optimizer.rs` has no `parse(...)` call and no `Statement` import
- [ ] `CatalogSnapshot` has a `parsed_tables` field populated during `build()`
- [ ] `lookup_table_info()` returns `None` (not a panic) for an unknown table name
- [ ] Perf: `perf record` against Sakila INSERT benchmark shows `resolve_table` cost
  substantially reduced from the baseline 17 %
