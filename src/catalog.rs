use crate::engine::scalarvalue::ScalarValue;
use crate::storage::{decode_u64_key, BTree};

/// Metadata for a single index from the catalog.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub index_name: String,
    pub column_names: Vec<String>,
    pub rootpage: u32,
    /// Derived from name prefix: `_pk_` = primary key, `_uq_` = unique.
    /// Phase 109.1 will replace this heuristic with DDL parsing.
    pub unique: bool,
}

/// Schema catalog layer: owns a BTree and provides all SQL-level schema
/// lookup and mutation operations. The underlying BTree is pure K-V storage.
pub struct Catalog {
    btree: BTree,
}

/// Root page of the `db_schema` catalog table.
/// Always page 1 on every database (page 0 is the ZeroPage header).
const CATALOG_ROOT: u32 = 1;

impl Catalog {
    /// Create a brand-new database at `path` and bootstrap the catalog.
    /// The caller is responsible for ensuring the path does not already exist
    /// (or that the file is empty).
    pub fn create(path: &str) -> Self {
        let btree = BTree::new(path);
        let mut cat = Catalog { btree };
        if cat.btree.file_size_pages() == 0 {
            cat.bootstrap();
        }
        cat
    }

    /// Open an existing database at `path`. The catalog must already be present.
    pub fn open(path: &str) -> Self {
        Catalog {
            btree: BTree::new(path),
        }
    }

    /// Access the underlying BTree for raw K-V operations.
    pub fn btree(&self) -> &BTree {
        &self.btree
    }

    /// Mutably access the underlying BTree for raw K-V operations.
    pub fn btree_mut(&mut self) -> &mut BTree {
        &mut self.btree
    }

    // ---- public API --------------------------------------------------------

    /// Insert a new entry (table or index) into the catalog.
    pub fn insert_entry(
        &mut self,
        obj_type: &str,
        name: &str,
        tbl_name: &str,
        rootpage: u32,
        sql: &str,
    ) {
        let key = self.next_key();

        let row_values = vec![
            ScalarValue::String(obj_type.to_string()),
            ScalarValue::String(name.to_string()),
            ScalarValue::String(tbl_name.to_string()),
            ScalarValue::Integer(rootpage as i64),
            ScalarValue::String(sql.to_string()),
        ];

        let mut row = Vec::new();
        ciborium::ser::into_writer(&row_values, &mut row).unwrap();

        let mut cursor = self.btree.open(CATALOG_ROOT);
        cursor.open_readwrite().insert_u64(key, row);
    }

    /// Look up a table by name. Returns `(rootpage, sql)` if found.
    pub fn lookup_table(&self, table_name: &str) -> Option<(u32, String)> {
        let mut cursor = self.btree.open(CATALOG_ROOT);
        let mut c = cursor.open_readonly();
        c.first();
        loop {
            match c.get_entry() {
                None => return None,
                Some(mut reader) => {
                    let values = reader.decode_as_array();
                    if values.len() >= 5 {
                        let obj_type = values[0].as_str().unwrap_or("");
                        let name = values[1].as_str().unwrap_or("");
                        if obj_type == "table" && name == table_name {
                            let rootpage = values[3].as_u64().unwrap() as u32;
                            let sql = values[4].as_str().unwrap_or("").to_string();
                            return Some((rootpage, sql));
                        }
                    }
                }
            }
            c.next();
        }
    }

    /// Reverse lookup: find the table name for a given root page.
    pub fn lookup_table_by_rootpage(&self, rootpage: u32) -> Option<String> {
        let mut cursor = self.btree.open(CATALOG_ROOT);
        let mut c = cursor.open_readonly();
        c.first();
        loop {
            match c.get_entry() {
                None => return None,
                Some(mut reader) => {
                    let values = reader.decode_as_array();
                    if values.len() >= 4 {
                        let obj_type = values[0].as_str().unwrap_or("");
                        let name = values[1].as_str().unwrap_or("").to_string();
                        let rp = values[3].as_u64().unwrap_or(0) as u32;
                        if obj_type == "table" && rp == rootpage {
                            return Some(name);
                        }
                    }
                }
            }
            c.next();
        }
    }

    /// Return all index metadata for a given table.
    pub fn lookup_indexes_for_table(&self, table_name: &str) -> Vec<IndexInfo> {
        let mut indexes = Vec::new();
        let mut cursor = self.btree.open(CATALOG_ROOT);
        let mut c = cursor.open_readonly();
        c.first();

        loop {
            match c.get_entry() {
                None => break,
                Some(mut reader) => {
                    let values = reader.decode_as_array();
                    if values.len() >= 5 {
                        let obj_type = values[0].as_str().unwrap_or("");
                        let tbl_name = values[2].as_str().unwrap_or("");

                        if obj_type == "index" && tbl_name == table_name {
                            let name = values[1].as_str().unwrap_or("").to_string();
                            let rp = values[3].as_u64().unwrap() as u32;
                            let sql = values[4].as_str().unwrap_or("");
                            let column_names = extract_columns_from_index_sql(sql);
                            let unique = name.starts_with("_pk_") || name.starts_with("_uq_");
                            indexes.push(IndexInfo {
                                index_name: name,
                                column_names,
                                rootpage: rp,
                                unique,
                            });
                        }
                    }
                }
            }
            c.next();
        }

        indexes
    }

    /// Return all catalog entries as raw `(type, name, tbl_name, rootpage, sql)` tuples.
    pub fn scan_entries(&self) -> Vec<(String, String, String, u32, String)> {
        let mut entries = Vec::new();
        let mut cursor = self.btree.open(CATALOG_ROOT);
        let mut c = cursor.open_readonly();
        c.first();
        loop {
            match c.get_entry() {
                None => break,
                Some(mut reader) => {
                    let values = reader.decode_as_array();
                    if values.len() >= 5 {
                        let obj_type = values[0].as_str().unwrap_or("").to_string();
                        let name = values[1].as_str().unwrap_or("").to_string();
                        let tbl_name = values[2].as_str().unwrap_or("").to_string();
                        let rootpage = values[3].as_u64().unwrap_or(0) as u32;
                        let sql = values[4].as_str().unwrap_or("").to_string();
                        entries.push((obj_type, name, tbl_name, rootpage, sql));
                    }
                }
            }
            c.next();
        }
        entries
    }

    /// Delete the catalog entry for a table and all its associated indexes.
    /// Returns `true` if the table entry was found and deleted.
    pub fn delete_entries_for_table(&mut self, table_name: &str) -> bool {
        let mut deleted_any = false;
        let mut cursor = self.btree.open(CATALOG_ROOT);
        let mut c = cursor.open_readwrite();
        c.first();
        loop {
            let mut entry = match c.get_entry() {
                None => break,
                Some(reader) => reader,
            };

            let values = entry.decode_as_array();
            if values.len() >= 5 {
                let obj_type = values[0].as_str().unwrap_or("");
                let name = values[1].as_str().unwrap_or("");
                let tbl_name = values[2].as_str().unwrap_or("");

                if (obj_type == "table" && name == table_name)
                    || (obj_type == "index" && tbl_name == table_name)
                {
                    c.delete_current();
                    deleted_any = true;
                    continue;
                }
            }

            c.next();
        }
        deleted_any
    }

    // ---- private helpers ---------------------------------------------------

    /// Allocate the next auto-increment key for a catalog row.
    fn next_key(&self) -> u64 {
        let mut cursor = self.btree.open(CATALOG_ROOT);
        let mut c = cursor.open_readonly();
        c.last();
        if let Some(entry) = c.get_entry() {
            decode_u64_key(entry.key()) + 1
        } else {
            0
        }
    }

    /// Bootstrap the catalog on a fresh database.
    /// Creates the db_schema tree and inserts the self-referencing row.
    fn bootstrap(&mut self) {
        let root = self.btree.create_tree();
        assert_eq!(root, CATALOG_ROOT, "catalog root must always be page 1");
        self.insert_entry(
            "table",
            "db_schema",
            "db_schema",
            root,
            "CREATE TABLE db_schema (type TEXT, name TEXT, tbl_name TEXT, rootpage INTEGER, sql TEXT)",
        );
    }
}

impl From<BTree> for Catalog {
    fn from(btree: BTree) -> Self {
        Catalog { btree }
    }
}

impl From<Catalog> for BTree {
    fn from(catalog: Catalog) -> Self {
        catalog.btree
    }
}

/// Extract column names from a stored `CREATE INDEX` SQL string.
/// `"CREATE INDEX idx ON tbl(col1, col2)"` → `["col1", "col2"]`
fn extract_columns_from_index_sql(sql: &str) -> Vec<String> {
    if let Some(start) = sql.find('(') {
        if let Some(end) = sql[start..].find(')') {
            let inside = &sql[start + 1..start + end];
            return inside
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
    }
    vec![]
}

// ---- tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::{temp_db_path, TempCatalog};

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
        let rootpage = cat.btree_mut().create_tree();
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
        let rp = cat.btree_mut().create_tree();
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
            let rp = cat.btree_mut().create_tree();
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
        let rp = cat.btree_mut().create_tree();
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
        let rp = cat.btree_mut().create_tree();
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
        let tbl_rp = cat.btree_mut().create_tree();
        let idx_rp = cat.btree_mut().create_tree();
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
        let tbl_rp = cat.btree_mut().create_tree();
        let idx_rp = cat.btree_mut().create_tree();
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
            let rp = cat.btree_mut().create_tree();
            cat.insert_entry(
                "table",
                tbl,
                tbl,
                rp,
                &format!("CREATE TABLE {} (id INTEGER)", tbl),
            );
            let idx_rp = cat.btree_mut().create_tree();
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
        let rp = cat.btree_mut().create_tree();
        cat.insert_entry("table", "t1", "t1", rp, "CREATE TABLE t1 (x INTEGER)");
        let entries = cat.scan_entries();
        // 1 bootstrap entry (db_schema) + 1 inserted
        assert_eq!(entries.len(), 2);
    }

    // ---- delete_entries_for_table ----

    #[test]
    fn test_delete_removes_table_and_its_indexes() {
        let mut cat = TempCatalog::new();
        let tbl_rp = cat.btree_mut().create_tree();
        let idx_rp = cat.btree_mut().create_tree();
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
            let rp = cat.btree_mut().create_tree();
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
        let tbl_rp = cat.btree_mut().create_tree();
        let idx_rp = cat.btree_mut().create_tree();
        cat.insert_entry("table", "t", "t", tbl_rp, "CREATE TABLE t (id INTEGER)");
        cat.insert_entry(
            "index",
            "_pk_t_id",
            "t",
            idx_rp,
            "CREATE INDEX _pk_t_id ON t(id)",
        );
        let indexes = cat.lookup_indexes_for_table("t");
        assert!(indexes[0].unique);
    }

    #[test]
    fn test_unique_flag_uq_prefix() {
        let mut cat = TempCatalog::new();
        let tbl_rp = cat.btree_mut().create_tree();
        let idx_rp = cat.btree_mut().create_tree();
        cat.insert_entry("table", "t", "t", tbl_rp, "CREATE TABLE t (email TEXT)");
        cat.insert_entry(
            "index",
            "_uq_t_email",
            "t",
            idx_rp,
            "CREATE INDEX _uq_t_email ON t(email)",
        );
        let indexes = cat.lookup_indexes_for_table("t");
        assert!(indexes[0].unique);
    }

    #[test]
    fn test_unique_flag_plain_index_not_unique() {
        let mut cat = TempCatalog::new();
        let tbl_rp = cat.btree_mut().create_tree();
        let idx_rp = cat.btree_mut().create_tree();
        cat.insert_entry("table", "t", "t", tbl_rp, "CREATE TABLE t (age INTEGER)");
        cat.insert_entry(
            "index",
            "idx_t_age",
            "t",
            idx_rp,
            "CREATE INDEX idx_t_age ON t(age)",
        );
        let indexes = cat.lookup_indexes_for_table("t");
        assert!(!indexes[0].unique);
    }

    // ---- persistence ----

    #[test]
    fn test_entries_persist_across_reopen() {
        let path = temp_db_path();
        {
            let mut cat = Catalog::create(&path);
            let rp = cat.btree_mut().create_tree();
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
