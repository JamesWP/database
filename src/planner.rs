//! Query Planner - Logical Operator Tree (Option A)
//!
//! Converts AST to a tree of logical operators (LogicalPlan).
//! The compiler (future) will convert LogicalPlan to bytecode.

use crate::frontend::ast::Statement;

// ============================================================================
// Operators
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Plus,
    Negate,
    Not,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,

    // Comparison
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,

    // Logical
    And,
    Or,
}

// ============================================================================
// Plan Types
// ============================================================================

/// Reference to a column from an input node
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnRef {
    /// Column from a single-input node (Filter, Project, etc.)
    /// column_idx is the index into the input node's output columns
    Single { column_idx: usize },

    // Future: Multi { node_idx: usize, column_idx: usize } for JOINs
}

/// Literal values in expressions
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

/// Planner's expression type - like ast::Expression but with resolved columns
#[derive(Debug, Clone, PartialEq)]
pub enum PlanExpr {
    ColumnRef(ColumnRef),
    Literal(Literal),
    BinaryOp {
        op: BinaryOp,
        left: Box<PlanExpr>,
        right: Box<PlanExpr>,
    },
    UnaryOp {
        op: UnaryOp,
        operand: Box<PlanExpr>,
    },
}

/// Logical plan nodes - relational algebra operators
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Scan rows from a table (leaf node, no inputs)
    /// columns: indices of columns to read from the table schema
    Scan { table: String, columns: Vec<usize> },

    /// Filter rows based on a predicate (1 input)
    /// Pass-through: outputs all columns from its child unchanged.
    /// Only rows where predicate evaluates to true are emitted.
    Filter {
        input: Box<LogicalPlan>,
        predicate: PlanExpr,
    },

    /// Project specific columns/expressions (1 input)
    /// Transforms output: produces exactly the columns specified.
    /// ColumnRefs in expressions refer to positions in the child's output.
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<PlanExpr>,
    },

    /// Limit output rows (1 input)
    /// Pass-through: outputs all columns from its child unchanged.
    /// Only emits up to `count` rows.
    Limit {
        input: Box<LogicalPlan>,
        count: u64,
    },

    // Future: Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, ... }
}

// ============================================================================
// Schema (for column resolution)
// ============================================================================

pub mod schema {
    #[derive(Debug, Clone)]
    pub struct Schema {
        pub tables: Vec<Table>,
    }

    #[derive(Debug, Clone)]
    pub struct Table {
        pub name: String,
        pub columns: Vec<Column>,
    }

    #[derive(Debug, Clone)]
    pub struct Column {
        pub name: String,
        // Future: pub data_type: DataType,
    }

    impl Schema {
        pub fn get_table(&self, name: &str) -> Option<&Table> {
            self.tables.iter().find(|t| t.name == name)
        }
    }

    impl Table {
        pub fn get_column_index(&self, name: &str) -> Option<usize> {
            self.columns.iter().position(|c| c.name == name)
        }
    }
}

// ============================================================================
// Planning
// ============================================================================

/// Convert an AST Statement to a LogicalPlan
pub fn plan(statement: Statement, schema: &schema::Schema) -> Result<LogicalPlan, PlanError> {
    todo!()
}

#[derive(Debug, Clone, PartialEq)]
pub enum PlanError {
    TableNotFound(String),
    ColumnNotFound { table: String, column: String },
    UnsupportedStatement,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontend::parse;

    fn make_users_schema() -> schema::Schema {
        schema::Schema {
            tables: vec![schema::Table {
                name: "users".to_string(),
                columns: vec![
                    schema::Column {
                        name: "id".to_string(),
                    },
                    schema::Column {
                        name: "name".to_string(),
                    },
                    schema::Column {
                        name: "age".to_string(),
                    },
                ],
            }],
        }
    }

    fn parse_sql(sql: &str) -> Statement {
        parse(sql).expect("Failed to parse SQL")
    }

    /// Example 1: Simple SELECT
    /// SELECT id, name FROM users
    ///
    /// Expected LogicalPlan:
    /// Project { columns: [ColumnRef(0), ColumnRef(1)] }
    ///   └─ Scan { table: "users", columns: [0, 1] }
    #[test]
    fn test_simple_select() {
        let schema = make_users_schema();
        let stmt = parse_sql("SELECT id, name FROM users");

        let plan = plan(stmt, &schema).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
                columns: vec![0, 1], // id, name
            }),
            columns: vec![
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 }),
            ],
        };

        assert_eq!(plan, expected);
    }

    /// Example 2: SELECT with WHERE
    /// SELECT name FROM users WHERE age > 21
    ///
    /// Expected LogicalPlan:
    /// Project { columns: [ColumnRef(0)] }   // name (position 0 in scan output)
    ///   └─ Filter { predicate: ColumnRef(1) > 21 }   // age (position 1 in scan output)
    ///        └─ Scan { table: "users", columns: [1, 2] }   // name, age
    #[test]
    fn test_select_with_where() {
        let schema = make_users_schema();
        let stmt = parse_sql("SELECT name FROM users WHERE age > 21");

        let plan = plan(stmt, &schema).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    table: "users".to_string(),
                    columns: vec![1, 2], // name, age
                }),
                predicate: PlanExpr::BinaryOp {
                    op: BinaryOp::GreaterThan,
                    left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 })), // age
                    right: Box::new(PlanExpr::Literal(Literal::Integer(21))),
                },
            }),
            columns: vec![PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })], // name
        };

        assert_eq!(plan, expected);
    }

    /// Example 3: SELECT with LIMIT
    /// SELECT name FROM users LIMIT 10
    ///
    /// Expected LogicalPlan:
    /// Limit { count: 10 }
    ///   └─ Project { columns: [ColumnRef(0)] }
    ///        └─ Scan { table: "users", columns: [1] }
    #[test]
    fn test_select_with_limit() {
        let schema = make_users_schema();
        let stmt = parse_sql("SELECT name FROM users LIMIT 10");

        let plan = plan(stmt, &schema).expect("Planning failed");

        let expected = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Project {
                input: Box::new(LogicalPlan::Scan {
                    table: "users".to_string(),
                    columns: vec![1], // name
                }),
                columns: vec![PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })],
            }),
            count: 10,
        };

        assert_eq!(plan, expected);
    }

    /// SELECT * should expand to all columns
    /// Scan { columns: [0, 1, 2] } reads all columns
    /// Project outputs them in order
    #[test]
    #[ignore = "parser does not yet support SELECT *"]
    fn test_select_star() {
        let schema = make_users_schema();
        let stmt = parse_sql("SELECT * FROM users");

        let plan = plan(stmt, &schema).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
                columns: vec![0, 1, 2], // all columns
            }),
            columns: vec![
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 2 }),
            ],
        };

        assert_eq!(plan, expected);
    }

    /// Error case: table not found
    #[test]
    fn test_table_not_found() {
        let schema = make_users_schema();
        let stmt = parse_sql("SELECT id FROM nonexistent");

        let result = plan(stmt, &schema);

        assert_eq!(
            result,
            Err(PlanError::TableNotFound("nonexistent".to_string()))
        );
    }

    /// Error case: column not found
    #[test]
    fn test_column_not_found() {
        let schema = make_users_schema();
        let stmt = parse_sql("SELECT nonexistent FROM users");

        let result = plan(stmt, &schema);

        assert_eq!(
            result,
            Err(PlanError::ColumnNotFound {
                table: "users".to_string(),
                column: "nonexistent".to_string(),
            })
        );
    }
}
