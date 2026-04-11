use std::ops::{Deref, DerefMut};

use crate::storage::BTree;

pub use crate::storage::IndexInfo;

/// Schema catalog layer: a thin wrapper around `BTree` that provides a
/// stable public API surface.  All catalog lookup and mutation logic now
/// lives inside `BTree` itself; this type exists for backward compatibility
/// and as the public entry point for opening a database.
pub struct Catalog(pub BTree);

impl Catalog {
    /// Create a brand-new database at `path` and bootstrap the catalog.
    pub fn create(path: &str) -> Self {
        Catalog(BTree::new(path))
    }

    /// Open an existing database at `path`.
    pub fn open(path: &str) -> Self {
        Catalog(BTree::new(path))
    }

    /// Access the underlying `BTree`.
    pub fn btree(&self) -> &BTree {
        &self.0
    }

    /// Mutably access the underlying `BTree`.
    pub fn btree_mut(&mut self) -> &mut BTree {
        &mut self.0
    }
}

impl Deref for Catalog {
    type Target = BTree;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Catalog {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<BTree> for Catalog {
    fn from(btree: BTree) -> Self {
        Catalog(btree)
    }
}

impl From<Catalog> for BTree {
    fn from(catalog: Catalog) -> Self {
        catalog.0
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::{temp_db_path, TempCatalog};

    const CATALOG_ROOT: u32 = 1;

    // ---- bootstrap ----

    #[test]
    fn test_create_bootstraps_self_referencing_entry() {
        let cat = TempCatalog::new();
        let entries = cat.scan_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "table");
        assert_eq!(entries[0].1, "db_schema");
        assert_eq!(entries[0].3, CATALOG_ROOT);
    }

    #[test]
    fn test_open_does_not_re_bootstrap() {
        let path = temp_db_path();
        {
            let _ = Catalog::create(&path);
        }
        let cat = Catalog::open(&path);
        let entries = cat.scan_entries();
        assert_eq!(entries.len(), 1);
    }

    // ---- insert_entry / lookup_table ----

    #[test]
    fn test_insert_and_lookup_table() {
        let mut cat = TempCatalog::new();
        let rootpage = cat.create_tree();
        cat.insert_entry(
            "table",
            "users",
            "users",
            rootpage,
            "CREATE TABLE users (id INTEGER)",
        );
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
        let rp = cat.create_tree();
        cat.insert_entry(
            "index",
            "idx_users_id",
            "users",
            rp,
            "CREATE INDEX idx_users_id ON users(id)",
        );
        assert!(cat.lookup_table("idx_users_id").is_none());
    }

    #[test]
    fn test_multiple_tables_each_retrievable() {
        let mut cat = TempCatalog::new();
        for name in &["alpha", "beta", "gamma"] {
            let rp = cat.create_tree();
            cat.insert_entry(
                "table",
                name,
                name,
                rp,
                &format!("CREATE TABLE {} (id INTEGER)", name),
            );
        }
        assert!(cat.lookup_table("alpha").is_some());
        assert!(cat.lookup_table("beta").is_some());
        assert!(cat.lookup_table("gamma").is_some());
    }

    // ---- lookup_table_by_rootpage ----

    #[test]
    fn test_lookup_table_by_rootpage() {
        let mut cat = TempCatalog::new();
        let rp = cat.create_tree();
        cat.insert_entry(
            "table",
            "things",
            "things",
            rp,
            "CREATE TABLE things (x TEXT)",
        );
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
        let rp = cat.create_tree();
        cat.insert_entry(
            "table",
            "users",
            "users",
            rp,
            "CREATE TABLE users (id INTEGER)",
        );
        assert!(cat.lookup_indexes_for_table("users").is_empty());
    }

    #[test]
    fn test_lookup_indexes_returns_correct_columns() {
        let mut cat = TempCatalog::new();
        let tbl_rp = cat.create_tree();
        let idx_rp = cat.create_tree();
        cat.insert_entry(
            "table",
            "users",
            "users",
            tbl_rp,
            "CREATE TABLE users (id INTEGER, name TEXT)",
        );
        cat.insert_entry(
            "index",
            "idx_users_name",
            "users",
            idx_rp,
            "CREATE INDEX idx_users_name ON users(name)",
        );
        let indexes = cat.lookup_indexes_for_table("users");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].index_name, "idx_users_name");
        assert_eq!(indexes[0].column_names, vec!["name"]);
        assert_eq!(indexes[0].rootpage, idx_rp);
    }

    #[test]
    fn test_lookup_indexes_multi_column() {
        let mut cat = TempCatalog::new();
        let tbl_rp = cat.create_tree();
        let idx_rp = cat.create_tree();
        cat.insert_entry(
            "table",
            "rental",
            "rental",
            tbl_rp,
            "CREATE TABLE rental (id INTEGER, date TEXT, inv INTEGER)",
        );
        cat.insert_entry(
            "index",
            "idx_rental_uq",
            "rental",
            idx_rp,
            "CREATE INDEX idx_rental_uq ON rental(date,inv)",
        );
        let indexes = cat.lookup_indexes_for_table("rental");
        assert_eq!(indexes[0].column_names, vec!["date", "inv"]);
    }

    #[test]
    fn test_lookup_indexes_only_returns_indexes_for_named_table() {
        let mut cat = TempCatalog::new();
        for tbl in &["a", "b"] {
            let rp = cat.create_tree();
            cat.insert_entry(
                "table",
                tbl,
                tbl,
                rp,
                &format!("CREATE TABLE {} (id INTEGER)", tbl),
            );
            let idx_rp = cat.create_tree();
            cat.insert_entry(
                "index",
                &format!("idx_{}", tbl),
                tbl,
                idx_rp,
                &format!("CREATE INDEX idx_{} ON {}(id)", tbl, tbl),
            );
        }
        let indexes = cat.lookup_indexes_for_table("a");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].index_name, "idx_a");
    }

    // ---- scan_entries ----

    #[test]
    fn test_scan_entries_returns_all() {
        let mut cat = TempCatalog::new();
        let rp = cat.create_tree();
        cat.insert_entry("table", "t1", "t1", rp, "CREATE TABLE t1 (x INTEGER)");
        let entries = cat.scan_entries();
        // 1 bootstrap entry (db_schema) + 1 inserted
        assert_eq!(entries.len(), 2);
    }

    // ---- delete_entries_for_table ----

    #[test]
    fn test_delete_removes_table_and_its_indexes() {
        let mut cat = TempCatalog::new();
        let tbl_rp = cat.create_tree();
        let idx_rp = cat.create_tree();
        cat.insert_entry(
            "table",
            "users",
            "users",
            tbl_rp,
            "CREATE TABLE users (id INTEGER)",
        );
        cat.insert_entry(
            "index",
            "_pk_users_id",
            "users",
            idx_rp,
            "CREATE INDEX _pk_users_id ON users(id)",
        );
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
            let rp = cat.create_tree();
            cat.insert_entry(
                "table",
                name,
                name,
                rp,
                &format!("CREATE TABLE {} (id INTEGER)", name),
            );
        }
        cat.delete_entries_for_table("drop");
        assert!(cat.lookup_table("keep").is_some());
        assert!(cat.lookup_table("drop").is_none());
    }

    // ---- unique flag ----

    #[test]
    fn test_unique_flag_pk_prefix() {
        let mut cat = TempCatalog::new();
        let tbl_rp = cat.create_tree();
        let idx_rp = cat.create_tree();
        cat.insert_entry("table", "t", "t", tbl_rp, "CREATE TABLE t (id INTEGER)");
        cat.insert_entry(
            "index",
            "_pk_t_id",
            "t",
            idx_rp,
            "CREATE UNIQUE INDEX _pk_t_id ON t(id)",
        );
        let indexes = cat.lookup_indexes_for_table("t");
        assert!(indexes[0].sql.to_uppercase().contains("UNIQUE"));
    }

    #[test]
    fn test_unique_flag_uq_prefix() {
        let mut cat = TempCatalog::new();
        let tbl_rp = cat.create_tree();
        let idx_rp = cat.create_tree();
        cat.insert_entry("table", "t", "t", tbl_rp, "CREATE TABLE t (email TEXT)");
        cat.insert_entry(
            "index",
            "_uq_t_email",
            "t",
            idx_rp,
            "CREATE UNIQUE INDEX _uq_t_email ON t(email)",
        );
        let indexes = cat.lookup_indexes_for_table("t");
        assert!(indexes[0].sql.to_uppercase().contains("UNIQUE"));
    }

    #[test]
    fn test_unique_flag_plain_index_not_unique() {
        let mut cat = TempCatalog::new();
        let tbl_rp = cat.create_tree();
        let idx_rp = cat.create_tree();
        cat.insert_entry("table", "t", "t", tbl_rp, "CREATE TABLE t (age INTEGER)");
        cat.insert_entry(
            "index",
            "idx_t_age",
            "t",
            idx_rp,
            "CREATE INDEX idx_t_age ON t(age)",
        );
        let indexes = cat.lookup_indexes_for_table("t");
        assert!(!indexes[0].sql.to_uppercase().contains("UNIQUE"));
    }

    // ---- PRIMARY KEY / UNIQUE implicit index entries ----

    #[test]
    fn test_primary_key_implicit_index_is_unique() {
        let mut cat = TempCatalog::new();
        let tbl_rp = cat.create_tree();
        let idx_rp = cat.create_tree();
        cat.insert_entry(
            "table",
            "t",
            "t",
            tbl_rp,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
        );
        cat.insert_entry(
            "index",
            "_pk_t_id",
            "t",
            idx_rp,
            "CREATE UNIQUE INDEX _pk_t_id ON t(id)",
        );
        let indexes = cat.lookup_indexes_for_table("t");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].column_names, vec!["id"]);
        assert!(
            indexes[0].sql.to_uppercase().contains("UNIQUE"),
            "PRIMARY KEY index must be unique"
        );
    }

    #[test]
    fn test_unique_column_implicit_index_is_unique() {
        let mut cat = TempCatalog::new();
        let tbl_rp = cat.create_tree();
        let idx_rp = cat.create_tree();
        cat.insert_entry(
            "table",
            "t",
            "t",
            tbl_rp,
            "CREATE TABLE t (id INTEGER, name TEXT UNIQUE)",
        );
        cat.insert_entry(
            "index",
            "_uq_t_name",
            "t",
            idx_rp,
            "CREATE UNIQUE INDEX _uq_t_name ON t(name)",
        );
        let indexes = cat.lookup_indexes_for_table("t");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].column_names, vec!["name"]);
        assert!(
            indexes[0].sql.to_uppercase().contains("UNIQUE"),
            "UNIQUE index must be unique"
        );
    }

    // ---- cache correctness ----

    #[test]
    fn test_cache_populated_after_first_lookup() {
        let mut cat = TempCatalog::new();
        let rp = cat.create_tree();
        cat.insert_entry("table", "t", "t", rp, "CREATE TABLE t (id INTEGER)");
        // Cache is None after insert (invalidated)
        assert!(!cat.catalog_cache_populated());
        // First lookup populates the cache
        let _ = cat.lookup_table("t");
        assert!(cat.catalog_cache_populated());
    }

    #[test]
    fn test_cache_invalidated_by_insert_entry() {
        let mut cat = TempCatalog::new();
        let rp = cat.create_tree();
        cat.insert_entry("table", "a", "a", rp, "CREATE TABLE a (x INTEGER)");
        // Warm the cache
        let _ = cat.lookup_table("a");
        assert!(cat.catalog_cache_populated());
        // Another insert must clear the cache
        let rp2 = cat.create_tree();
        cat.insert_entry("table", "b", "b", rp2, "CREATE TABLE b (x INTEGER)");
        assert!(!cat.catalog_cache_populated());
        // After re-warming, both tables visible
        assert!(cat.lookup_table("a").is_some());
        assert!(cat.lookup_table("b").is_some());
    }

    #[test]
    fn test_cache_invalidated_by_delete() {
        let mut cat = TempCatalog::new();
        let rp = cat.create_tree();
        cat.insert_entry(
            "table",
            "drop_me",
            "drop_me",
            rp,
            "CREATE TABLE drop_me (x INTEGER)",
        );
        // Warm the cache
        let _ = cat.lookup_table("drop_me");
        assert!(cat.catalog_cache_populated());
        // Delete must clear the cache
        cat.delete_entries_for_table("drop_me");
        assert!(!cat.catalog_cache_populated());
        // After re-warming, table is gone
        assert!(cat.lookup_table("drop_me").is_none());
    }

    #[test]
    fn test_cache_indexes_visible_after_insert() {
        let mut cat = TempCatalog::new();
        let tbl_rp = cat.create_tree();
        let idx_rp = cat.create_tree();
        cat.insert_entry("table", "t", "t", tbl_rp, "CREATE TABLE t (id INTEGER)");
        cat.insert_entry(
            "index",
            "idx_t_id",
            "t",
            idx_rp,
            "CREATE INDEX idx_t_id ON t(id)",
        );
        let indexes = cat.lookup_indexes_for_table("t");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].index_name, "idx_t_id");
    }

    #[test]
    fn test_cache_lookup_by_rootpage() {
        let mut cat = TempCatalog::new();
        let rp = cat.create_tree();
        cat.insert_entry(
            "table",
            "things",
            "things",
            rp,
            "CREATE TABLE things (x TEXT)",
        );
        // Warm via a different method to confirm all maps built in one scan
        let _ = cat.lookup_table("things");
        assert_eq!(cat.lookup_table_by_rootpage(rp), Some("things".to_string()));
        // Cache still valid (no write happened)
        assert!(cat.catalog_cache_populated());
    }

    // ---- persistence ----

    #[test]
    fn test_entries_persist_across_reopen() {
        let path = temp_db_path();
        {
            let mut cat = Catalog::create(&path);
            let rp = cat.create_tree();
            cat.insert_entry(
                "table",
                "persisted",
                "persisted",
                rp,
                "CREATE TABLE persisted (id INTEGER)",
            );
        }
        let cat = Catalog::open(&path);
        assert!(cat.lookup_table("persisted").is_some());
    }
}
