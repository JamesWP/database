use std::collections::HashMap;

use probe::probe;

use crate::frontend::ast::Statement;
use crate::frontend::parse;

use super::cell_reader::CellReader;
use super::node_page_store::NodePageStore;
use super::page_id::PageId;

/// The fixed root page of the catalog B-tree (always page 1).
pub(super) const CATALOG_ROOT: PageId = PageId(1);

/// Index metadata extracted from the catalog B-tree.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub index_name: String,
    pub column_names: Vec<String>,
    pub rootpage: u32,
    /// The DDL SQL stored in the catalog (e.g. `CREATE UNIQUE INDEX ...`).
    /// Use this to determine uniqueness rather than the index name prefix.
    pub sql: String,
}

/// Lazy snapshot of the catalog B-tree, rebuilt on demand after any write.
/// Constructed by `BTree::cache()`; callers query it directly.
#[derive(Clone, Default)]
pub struct CatalogSnapshot {
    /// table name → (rootpage, DDL sql)
    pub tables: HashMap<String, (u32, String)>,
    /// rootpage → table name (reverse lookup)
    pub(super) by_rootpage: HashMap<u32, String>,
    /// table name → all indexes for that table
    pub indexes: HashMap<String, Vec<IndexInfo>>,
}

impl CatalogSnapshot {
    /// Scan the catalog B-tree and build a snapshot.
    ///
    /// Uses `NodePageStore::scan_leaf_cells` for traversal and `CellReader`
    /// for transparent overflow handling — no dependency on `BTree` or `Cursor`.
    #[inline(never)]
    pub(super) fn build(store: &mut NodePageStore) -> Self {
        probe!(database, catalog_scan);
        let mut snapshot = CatalogSnapshot::default();
        store.scan_leaf_cells(CATALOG_ROOT, &mut |store, page_idx, cell_idx| {
            let mut reader = match CellReader::new(store, page_idx, cell_idx) {
                Some(r) => r,
                None => return,
            };
            let values = reader.decode_as_array();
            if values.len() < 5 {
                return;
            }
            let obj_type = values[0].as_str().unwrap_or("");
            let name = values[1].as_str().unwrap_or("").to_string();
            let tbl_name = values[2].as_str().unwrap_or("").to_string();
            let rootpage = values[3].as_u64().unwrap_or(0) as u32;
            let sql = values[4].as_str().unwrap_or("").to_string();
            match obj_type {
                "table" => {
                    snapshot.by_rootpage.insert(rootpage, name.clone());
                    snapshot.tables.insert(name, (rootpage, sql));
                }
                "index" => {
                    let column_names = extract_columns_from_index_sql(&sql);
                    snapshot
                        .indexes
                        .entry(tbl_name)
                        .or_default()
                        .push(IndexInfo {
                            index_name: name,
                            column_names,
                            rootpage,
                            sql,
                        });
                }
                _ => {}
            }
        });
        snapshot
    }

    /// Look up a table by name. Returns `(rootpage, sql)` if found.
    #[inline(never)]
    pub fn lookup_table(&self, table_name: &str) -> Option<(u32, String)> {
        probe!(database, catalog_lookup_table);
        self.tables.get(table_name).cloned()
    }

    /// Reverse lookup: find the table name for a given root page.
    #[inline(never)]
    pub fn lookup_table_by_rootpage(&self, rootpage: u32) -> Option<String> {
        probe!(database, catalog_lookup_by_rootpage);
        self.by_rootpage.get(&rootpage).cloned()
    }

    /// Return all index metadata for a given table.
    #[inline(never)]
    pub fn lookup_indexes_for_table(&self, table_name: &str) -> Vec<IndexInfo> {
        probe!(database, catalog_lookup_indexes);
        self.indexes.get(table_name).cloned().unwrap_or_default()
    }
}

/// Extract column names from an index DDL string by parsing it with the SQL
/// frontend.  Returns an empty vec if the SQL cannot be parsed.
pub(super) fn extract_columns_from_index_sql(sql: &str) -> Vec<String> {
    match parse(sql) {
        Ok(Statement::CreateIndex(ci)) => ci.column_names,
        _ => vec![],
    }
}
