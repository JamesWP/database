use std::collections::HashMap;

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
/// Private to the storage module.
#[derive(Clone)]
pub(super) struct CatalogSnapshot {
    /// table name → (rootpage, DDL sql)
    pub(super) tables: HashMap<String, (u32, String)>,
    /// rootpage → table name (reverse lookup)
    pub(super) by_rootpage: HashMap<u32, String>,
    /// table name → all indexes for that table
    pub(super) indexes: HashMap<String, Vec<IndexInfo>>,
}

/// Extract column names from an index DDL string like
/// `CREATE INDEX idx ON tbl(col1, col2)`.
pub(super) fn extract_columns_from_index_sql(sql: &str) -> Vec<String> {
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
