# Phase AJ-1 — Extract Catalog Layer from BTree

Preparatory refactor for Phase AJ. `BTree` currently mixes pure key-value storage with SQL-level schema management. This phase extracts all catalog/schema awareness into a new `Catalog` module, leaving `BTree` as a clean key-value API.

## Items

| # | Item | Depends on |
|---|------|------------|
| AJ1-1 | Create `src/catalog.rs`: move all schema methods out of `btree.rs` | — |
| AJ1-2 | Remove `ZeroPage::schema_root_page`; hardcode catalog root = page 1 | AJ1-1 |
| AJ1-3 | Update all callers to use `Catalog` instead of `BTree` for schema operations | AJ1-1, AJ1-2 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

### Current structure

```
BTree  ←  everything: K-V ops + catalog scans + DDL storage + schema bootstrap
```

### Target structure

```
db.rs / planner  →  Catalog  →  BTree (pure K-V)
```

`Catalog` owns `BTree`. It provides all schema lookup and mutation methods. Callers that need raw cursor operations call `catalog.btree()` / `catalog.btree_mut()`.

`BTree` retains only: `new(path)`, `open(root_page)`, `create_tree()`, `file_size_pages()`, and cursor operations.

---

## What moves out of `btree.rs`

| Symbol | Moves to |
|--------|---------|
| `IndexInfo` struct | `src/catalog.rs` |
| `insert_schema_entry()` | `Catalog::insert_entry()` |
| `lookup_table()` | `Catalog::lookup_table()` |
| `lookup_table_name_by_rootpage()` | `Catalog::lookup_table_by_rootpage()` |
| `lookup_indexes_for_table()` | `Catalog::lookup_indexes_for_table()` |
| `scan_schema_entries()` | `Catalog::scan_entries()` |
| `delete_schema_entries_for_table()` | `Catalog::delete_entries_for_table()` |
| `bootstrap_schema()` | `Catalog::bootstrap()` (called from `Catalog::new`) |
| `allocate_schema_key()` | `Catalog::next_key()` (private) |
| `extract_columns_from_index_sql()` | private helper in `catalog.rs` |
| `schema_root_page()` (method) | removed; see AJ1-2 |
| `ZeroPage::schema_root_page` (field) | removed; see AJ1-2 |

---

## AJ1-1. Create `src/catalog.rs`

### Catalog struct

```rust
pub struct Catalog {
    btree: BTree,
}

impl Catalog {
    /// Create a brand-new database at `path` and bootstrap the catalog.
    /// The caller is responsible for ensuring the path does not already exist.
    pub fn create(path: &str) -> Self {
        let btree = BTree::new(path);
        let mut cat = Catalog { btree };
        cat.bootstrap();
        cat
    }

    /// Open an existing database at `path`. The catalog must already be present.
    pub fn open(path: &str) -> Self {
        Catalog { btree: BTree::new(path) }
    }

    /// Access the underlying BTree for raw K-V operations within a function body.
    pub fn btree(&self) -> &BTree { &self.btree }
    pub fn btree_mut(&mut self) -> &mut BTree { &mut self.btree }
}

/// Wrap an already-initialised BTree in a Catalog (equivalent to `open`).
/// Does not bootstrap — the database must already contain a catalog.
impl From<BTree> for Catalog {
    fn from(btree: BTree) -> Self { Catalog { btree } }
}

/// Unwrap the inner BTree, discarding catalog context.
impl From<Catalog> for BTree {
    fn from(catalog: Catalog) -> Self { catalog.btree }
}
```

`BTree::new()` is simplified — it only opens the pager; it no longer calls `bootstrap_schema()`. Bootstrap is a deliberate database-creation step called explicitly from `Catalog::create()`, not a side effect of any conversion.

Call sites (e.g. the REPL) check whether the file exists and call `Catalog::create` or `Catalog::open` accordingly. `From`/`Into` are for ownership transfer within already-initialised databases. Within a function that holds a `Catalog` and needs to pass `&mut BTree` to the engine or compiler, use `catalog.btree_mut()`.

### Catalog public API

```rust
impl Catalog {
    /// Insert a new entry (table or index) into the catalog.
    pub fn insert_entry(
        &mut self,
        obj_type: &str,  // "table" or "index"
        name: &str,
        tbl_name: &str,
        rootpage: u32,
        sql: &str,
    );

    /// Look up a table by name. Returns (rootpage, sql) if found.
    pub fn lookup_table(&self, table_name: &str) -> Option<(u32, String)>;

    /// Reverse lookup: find table name given its root page.
    pub fn lookup_table_by_rootpage(&self, rootpage: u32) -> Option<String>;

    /// Return all index metadata for a given table.
    pub fn lookup_indexes_for_table(&self, table_name: &str) -> Vec<IndexInfo>;

    /// Return all catalog entries as raw tuples.
    pub fn scan_entries(&self) -> Vec<(String, String, String, u32, String)>;

    /// Delete the catalog entry for a table and all its associated indexes.
    /// Returns true if the table entry was found and deleted.
    pub fn delete_entries_for_table(&mut self, table_name: &str) -> bool;
}
```

### IndexInfo (moved from btree.rs)

```rust
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub index_name: String,
    pub column_names: Vec<String>,
    pub rootpage: u32,
    pub unique: bool,    // derived by parsing sql at the planner layer (phase 109.1)
}
```

> Note: `IndexInfo.unique` still uses the `_pk_`/`_uq_` prefix heuristic in this phase.
> Phase 109.1 replaces it with DDL parsing in the planner. The two changes are independent.

### Private helpers

```rust
/// Allocate the next auto-increment key for a catalog row.
fn next_key(&self) -> u64;

/// Bootstrap the catalog on a fresh database.
/// Creates the db_schema tree at root_page and inserts the self-referencing row.
fn bootstrap(&mut self);

/// Extract column names from a stored CREATE INDEX SQL string.
fn extract_columns_from_index_sql(sql: &str) -> Vec<String>;
```

### Key files

- `src/catalog.rs` — new module (created)
- `src/storage/btree.rs` — remove all schema methods, `IndexInfo`
- `src/lib.rs` — declare `pub mod catalog`

### Tests

Comprehensive unit tests live in a `#[cfg(test)]` module at the bottom of `src/catalog.rs`.
All tests use a `TempCatalog` helper that creates a `Catalog` backed by a temp file.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::TempCatalog;  // or inline helper

    // ---- bootstrap ----

    #[test]
    fn test_create_bootstraps_self_referencing_entry() {
        // A freshly created Catalog must contain exactly one entry:
        // ("table", "db_schema", "db_schema", 1, <DDL>)
        let cat = TempCatalog::create();
        let entries = cat.scan_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "table");
        assert_eq!(entries[0].1, "db_schema");
        assert_eq!(entries[0].3, 1);
    }

    #[test]
    fn test_open_does_not_re_bootstrap() {
        // Opening an existing database must not duplicate the self-referencing entry.
        let path = crate::test::temp_db_path();
        { let _ = Catalog::create(&path); }
        let cat = Catalog::open(&path);
        let entries = cat.scan_entries();
        assert_eq!(entries.len(), 1);
    }

    // ---- insert_entry / lookup_table ----

    #[test]
    fn test_insert_and_lookup_table() {
        let mut cat = TempCatalog::new();
        let rootpage = cat.btree_mut().create_tree();
        cat.insert_entry("table", "users", "users", rootpage, "CREATE TABLE users (id INTEGER)");
        let result = cat.lookup_table("users");
        assert!(result.is_some());
        let (rp, sql) = result.unwrap();
        assert_eq!(rp, rootpage);
        assert!(sql.contains("users"));
    }

    #[test]
    fn test_lookup_table_not_found_returns_none() {
        let cat = TempCatalog::new();
        assert!(cat.lookup_table("nonexistent").is_none());
    }

    #[test]
    fn test_lookup_table_does_not_return_index_entry() {
        let mut cat = TempCatalog::new();
        let rp = cat.btree_mut().create_tree();
        cat.insert_entry("index", "idx_users_id", "users", rp, "CREATE INDEX idx_users_id ON users(id)");
        assert!(cat.lookup_table("idx_users_id").is_none());
    }

    #[test]
    fn test_multiple_tables_each_retrievable() {
        let mut cat = TempCatalog::new();
        for name in &["alpha", "beta", "gamma"] {
            let rp = cat.btree_mut().create_tree();
            cat.insert_entry("table", name, name, rp, &format!("CREATE TABLE {} (id INTEGER)", name));
        }
        assert!(cat.lookup_table("alpha").is_some());
        assert!(cat.lookup_table("beta").is_some());
        assert!(cat.lookup_table("gamma").is_some());
    }

    // ---- lookup_table_by_rootpage ----

    #[test]
    fn test_lookup_table_by_rootpage() {
        let mut cat = TempCatalog::new();
        let rp = cat.btree_mut().create_tree();
        cat.insert_entry("table", "things", "things", rp, "CREATE TABLE things (x TEXT)");
        assert_eq!(cat.lookup_table_by_rootpage(rp), Some("things".to_string()));
    }

    #[test]
    fn test_lookup_table_by_rootpage_not_found() {
        let cat = TempCatalog::new();
        assert!(cat.lookup_table_by_rootpage(999).is_none());
    }

    // ---- lookup_indexes_for_table ----

    #[test]
    fn test_lookup_indexes_empty_for_table_with_no_indexes() {
        let mut cat = TempCatalog::new();
        let rp = cat.btree_mut().create_tree();
        cat.insert_entry("table", "users", "users", rp, "CREATE TABLE users (id INTEGER)");
        assert!(cat.lookup_indexes_for_table("users").is_empty());
    }

    #[test]
    fn test_lookup_indexes_returns_correct_columns() {
        let mut cat = TempCatalog::new();
        let tbl_rp = cat.btree_mut().create_tree();
        let idx_rp = cat.btree_mut().create_tree();
        cat.insert_entry("table", "users", "users", tbl_rp, "CREATE TABLE users (id INTEGER, name TEXT)");
        cat.insert_entry("index", "idx_users_name", "users", idx_rp,
            "CREATE INDEX idx_users_name ON users(name)");
        let indexes = cat.lookup_indexes_for_table("users");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].index_name, "idx_users_name");
        assert_eq!(indexes[0].column_names, vec!["name"]);
        assert_eq!(indexes[0].rootpage, idx_rp);
    }

    #[test]
    fn test_lookup_indexes_multi_column() {
        let mut cat = TempCatalog::new();
        let tbl_rp = cat.btree_mut().create_tree();
        let idx_rp = cat.btree_mut().create_tree();
        cat.insert_entry("table", "rental", "rental", tbl_rp,
            "CREATE TABLE rental (id INTEGER, date TEXT, inv INTEGER)");
        cat.insert_entry("index", "idx_rental_uq", "rental", idx_rp,
            "CREATE INDEX idx_rental_uq ON rental(date,inv)");
        let indexes = cat.lookup_indexes_for_table("rental");
        assert_eq!(indexes[0].column_names, vec!["date", "inv"]);
    }

    #[test]
    fn test_lookup_indexes_only_returns_indexes_for_named_table() {
        let mut cat = TempCatalog::new();
        for tbl in &["a", "b"] {
            let rp = cat.btree_mut().create_tree();
            cat.insert_entry("table", tbl, tbl, rp, &format!("CREATE TABLE {} (id INTEGER)", tbl));
            let idx_rp = cat.btree_mut().create_tree();
            cat.insert_entry("index", &format!("idx_{}", tbl), tbl, idx_rp,
                &format!("CREATE INDEX idx_{} ON {}(id)", tbl, tbl));
        }
        let indexes = cat.lookup_indexes_for_table("a");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].index_name, "idx_a");
    }

    // ---- scan_entries ----

    #[test]
    fn test_scan_entries_returns_all() {
        let mut cat = TempCatalog::new();
        let rp = cat.btree_mut().create_tree();
        cat.insert_entry("table", "t1", "t1", rp, "CREATE TABLE t1 (x INTEGER)");
        let entries = cat.scan_entries();
        // 1 bootstrap entry + 1 inserted
        assert_eq!(entries.len(), 2);
    }

    // ---- delete_entries_for_table ----

    #[test]
    fn test_delete_removes_table_and_its_indexes() {
        let mut cat = TempCatalog::new();
        let tbl_rp = cat.btree_mut().create_tree();
        let idx_rp = cat.btree_mut().create_tree();
        cat.insert_entry("table", "users", "users", tbl_rp, "CREATE TABLE users (id INTEGER)");
        cat.insert_entry("index", "_pk_users_id", "users", idx_rp,
            "CREATE INDEX _pk_users_id ON users(id)");
        assert!(cat.delete_entries_for_table("users"));
        assert!(cat.lookup_table("users").is_none());
        assert!(cat.lookup_indexes_for_table("users").is_empty());
    }

    #[test]
    fn test_delete_returns_false_for_unknown_table() {
        let mut cat = TempCatalog::new();
        assert!(!cat.delete_entries_for_table("ghost"));
    }

    #[test]
    fn test_delete_does_not_affect_other_tables() {
        let mut cat = TempCatalog::new();
        for name in &["keep", "drop"] {
            let rp = cat.btree_mut().create_tree();
            cat.insert_entry("table", name, name, rp, &format!("CREATE TABLE {} (id INTEGER)", name));
        }
        cat.delete_entries_for_table("drop");
        assert!(cat.lookup_table("keep").is_some());
        assert!(cat.lookup_table("drop").is_none());
    }

    // ---- persistence ----

    #[test]
    fn test_entries_persist_across_reopen() {
        let path = crate::test::temp_db_path();
        {
            let mut cat = Catalog::create(&path);
            let rp = cat.btree_mut().create_tree();
            cat.insert_entry("table", "persisted", "persisted", rp,
                "CREATE TABLE persisted (id INTEGER)");
        }
        let cat = Catalog::open(&path);
        assert!(cat.lookup_table("persisted").is_some());
    }
}
```

### Implementation Steps (1 commit)

#### Step AJ1-1 — Create `src/catalog.rs`; remove schema methods from `btree.rs`

**Commit:** `Catalog: extract schema catalog layer from BTree into src/catalog.rs`

---

## AJ1-2. Remove `ZeroPage::schema_root_page`; hardcode catalog root = page 1

### Motivation

`ZeroPage` in `pager.rs` has a `schema_root_page: Option<u32>` field — the only SQL concept in the file format. This is redundant: the catalog root is always allocated first on a new database and will always be page 1. Hardcoding this removes the coupling and simplifies the file format.

This is a breaking file-format change. Bump `FORMAT_VERSION` so existing databases are rejected cleanly.

### Changes

- **`pager.rs`**: remove `schema_root_page` from `ZeroPage`, remove `get_schema_root_page()` and `set_schema_root_page()`. Bump `FORMAT_VERSION`.
- **`btree.rs`**: remove `schema_root_page()` method (it delegated to `pager.borrow().get_schema_root_page()`).
- **`catalog.rs`**: `Catalog::new()` uses `const CATALOG_ROOT: u32 = 1` instead of reading from pager.

### Key files

- `src/storage/pager.rs` — `ZeroPage`, format version
- `src/storage/btree.rs` — remove `schema_root_page()` method
- `src/catalog.rs` — use constant root page

### Tests

The existing `test_new_database_has_self_referencing_entry` and `test_entries_persist_across_reopen` tests from AJ1-1 cover this change. Add a negative test:

```rust
#[test]
fn test_old_format_database_is_rejected() {
    // A database file with the old FORMAT_VERSION must return an error on open,
    // not silently corrupt data. Covered by existing format version validation
    // in pager.rs::validate_format_version().
}
```

### Implementation Steps (1 commit)

#### Step AJ1-2 — Remove `ZeroPage::schema_root_page`; hardcode catalog root = page 1

**Commit:** `Storage: remove schema_root_page from ZeroPage; hardcode catalog root at page 1`

---

## AJ1-3. Update all callers

### What changes

All call sites that previously passed `&BTree` / `&mut BTree` for schema operations now pass `&Catalog` / `&mut Catalog`. Sites that only need raw K-V (engine execution, cursor ops) continue using `catalog.btree()` / `catalog.btree_mut()`.

### Call sites to update

| File | Current | After |
|------|---------|-------|
| `src/db.rs` | `execute(&sql, &mut btree)` | `execute(&sql, &mut catalog)` |
| `src/planner/schema.rs` | `resolve_table(name, &btree)` | `resolve_table(name, &catalog)` |
| `src/planner/dml.rs` | `plan_insert(stmt, &btree)` etc. | `plan_*(stmt, &catalog)` |
| `src/repl/modes/` | construct `BTree`, pass to `execute` | construct `Catalog`, pass to `execute` |
| `src/test.rs` | `TestDb { btree: BTree }` | `TestDb { catalog: Catalog }` |

### Key files

- `src/db.rs`
- `src/planner/schema.rs`, `dml.rs`, `mod.rs`
- `src/repl/modes/` (all modes that currently hold a `BTree`)
- `src/test.rs`

### Tests

All existing `cargo test` tests must continue to pass without modification to the test assertions — only the internal plumbing changes.

### Implementation Steps (1 commit)

#### Step AJ1-3 — Update all callers to use `Catalog`

**Commit:** `Refactor: thread Catalog through db.rs, planner, and REPL in place of BTree`

---

## Verification

- [ ] `cargo test` — all existing tests pass
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning` — zero warnings
- [ ] `btree.rs` has no remaining references to "table", "index", "schema", "sql", "DDL", or `IndexInfo`
- [ ] `pager.rs` `ZeroPage` has no `schema_root_page` field
- [ ] `src/catalog.rs` unit tests all pass (all `test_` functions listed above)
- [ ] Catalog tests cover: bootstrap, insert+lookup, reverse lookup, index listing (single- and multi-column), scan, delete (table + associated indexes), persistence across reopen
- [ ] `cargo run -- test.db sql "CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1); SELECT id FROM t"` — end-to-end SQL still works
