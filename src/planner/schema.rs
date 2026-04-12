use std::collections::HashMap;

use crate::frontend::ast::{ColumnConstraint, DataType, DefaultValue, Statement};
use crate::frontend::parse;
use crate::storage::BTree;

use super::PlanError;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Schema {
    pub tables: Vec<Table>,
}

#[derive(Debug, Clone)]
pub struct Table {
    #[allow(dead_code)]
    pub name: String,
    pub rootpage: u32,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: Option<DataType>,
    pub default: Option<DefaultValue>,
    pub primary_key: bool,
    pub unique: bool,
}

impl Schema {
    #[allow(dead_code)]
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.name == name)
    }
}

impl Table {
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Build a name→index map for all columns in schema order.
    pub fn column_name_map(&self) -> HashMap<String, usize> {
        self.columns
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::execute;
    use crate::test::TestDb;

    #[test]
    fn test_schema_loads_primary_key_flag() {
        let mut db = TestDb::default();
        execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            &mut db.btree,
        )
        .unwrap();
        let table = resolve_table("users", &db.btree).unwrap();
        assert!(table.columns[0].primary_key);
        assert!(table.columns[0].unique);
        assert!(!table.columns[1].primary_key);
        assert!(!table.columns[1].unique);
    }

    #[test]
    fn test_schema_loads_unique_flag() {
        let mut db = TestDb::default();
        execute(
            "CREATE TABLE emails (id INTEGER, addr TEXT UNIQUE)",
            &mut db.btree,
        )
        .unwrap();
        let table = resolve_table("emails", &db.btree).unwrap();
        assert!(!table.columns[0].unique);
        assert!(table.columns[1].unique);
        assert!(!table.columns[1].primary_key);
    }

    #[test]
    fn test_schema_preserves_varchar_as_text() {
        let mut db = TestDb::default();
        execute(
            "CREATE TABLE t (id INTEGER, name VARCHAR(45))",
            &mut db.btree,
        )
        .unwrap();
        let table = resolve_table("t", &db.btree).unwrap();
        assert_eq!(table.columns[1].data_type, Some(DataType::Text));
    }

    #[test]
    fn test_index_on_varchar_column_succeeds() {
        let mut db = TestDb::default();
        execute(
            "CREATE TABLE t (id INTEGER, name VARCHAR(45))",
            &mut db.btree,
        )
        .unwrap();
        let result = execute("CREATE INDEX idx_name ON t(name)", &mut db.btree);
        assert!(
            result.is_ok(),
            "Expected index creation to succeed, got: {result:?}"
        );
    }
}

pub fn resolve_table(table_name: &str, catalog: &BTree) -> Result<Table, PlanError> {
    let (rootpage, sql) = catalog
        .catalog()
        .lookup_table(table_name)
        .ok_or_else(|| PlanError::TableNotFound(table_name.to_string()))?;

    // Parse the stored CREATE TABLE DDL to extract column definitions
    let stmt = parse(&sql).map_err(|_| PlanError::UnsupportedStatement)?;
    let create = match stmt {
        Statement::CreateTable(c) => c,
        _ => return Err(PlanError::UnsupportedStatement),
    };

    let columns = create
        .columns
        .into_iter()
        .map(|col| Column {
            name: col.name,
            data_type: col.type_name,
            default: col.default,
            primary_key: col.constraints.contains(&ColumnConstraint::PrimaryKey),
            unique: col.constraints.contains(&ColumnConstraint::Unique)
                || col.constraints.contains(&ColumnConstraint::PrimaryKey),
        })
        .collect();

    Ok(Table {
        name: table_name.to_string(),
        rootpage,
        columns,
    })
}
