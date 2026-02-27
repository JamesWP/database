use std::collections::HashMap;

use crate::frontend::ast::Statement;
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
    // Future: pub data_type: DataType,
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

pub fn resolve_table(table_name: &str, btree: &BTree) -> Result<Table, PlanError> {
    let (rootpage, sql) = btree
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
        .map(|col| Column { name: col.name })
        .collect();

    Ok(Table {
        name: table_name.to_string(),
        rootpage,
        columns,
    })
}
