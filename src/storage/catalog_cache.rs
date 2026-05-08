use std::collections::HashMap;

use probe::probe;

use crate::frontend::ast::{ColumnConstraint, DataType, DefaultValue, Statement};
use crate::frontend::parse;

use super::cell_reader::CellReader;
use super::node_page_store::NodePageStore;
use super::page_id::PageId;

/// The fixed root page of the catalog B-tree (always page 1).
pub(super) const CATALOG_ROOT: PageId = PageId(1);

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

impl TableInfo {
    /// Returns the schema column index of the INTEGER PRIMARY KEY column, if any.
    /// TEXT, REAL, and BLOB primary keys are not rowid aliases and return None.
    pub fn rowid_column(&self) -> Option<usize> {
        self.columns.iter().position(|c| {
            c.primary_key
                && !matches!(
                    c.data_type,
                    Some(DataType::Text) | Some(DataType::Real) | Some(DataType::Blob)
                )
        })
    }
}

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
    /// rootpage → table name (reverse lookup)
    pub(super) by_rootpage: HashMap<u32, String>,
    /// table name → all indexes for that table
    pub indexes: HashMap<String, Vec<IndexInfo>>,
    /// table name → pre-parsed table metadata (populated at build time)
    pub parsed_tables: HashMap<String, TableInfo>,
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
                    if let Some(info) = parse_table_info(rootpage, &sql) {
                        snapshot.parsed_tables.insert(name, info);
                    }
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

    /// Return the pre-parsed table metadata for `table_name`, or `None` if not found.
    #[inline(never)]
    pub fn lookup_table_info(&self, table_name: &str) -> Option<&TableInfo> {
        probe!(database, catalog_lookup_table_info);
        self.parsed_tables.get(table_name)
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

/// Extract column names from an index DDL string by parsing it with the SQL
/// frontend.  Returns an empty vec if the SQL cannot be parsed.
pub(super) fn extract_columns_from_index_sql(sql: &str) -> Vec<String> {
    match parse(sql) {
        Ok(Statement::CreateIndex(ci)) => ci.column_names,
        _ => vec![],
    }
}
