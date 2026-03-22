use std::collections::HashMap;

use crate::catalog::Catalog;
use crate::frontend::ast::{ColumnConstraint, Statement};
use crate::frontend::parse;

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
            &mut db.catalog,
        )
        .unwrap();
        let table = resolve_table("users", &db.catalog).unwrap();
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
            &mut db.catalog,
        )
        .unwrap();
        let table = resolve_table("emails", &db.catalog).unwrap();
        assert!(!table.columns[0].unique);
        assert!(table.columns[1].unique);
        assert!(!table.columns[1].primary_key);
    }
}

pub fn resolve_table(table_name: &str, catalog: &Catalog) -> Result<Table, PlanError> {
    let (rootpage, sql) = catalog
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
