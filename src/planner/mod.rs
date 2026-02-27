//! Query Planner - Logical Operator Tree (Option A)
//!
//! Converts AST to a tree of logical operators (LogicalPlan).
//! The compiler (future) will convert LogicalPlan to bytecode.

use crate::frontend::ast::{self, Statement};
use crate::storage::BTree;
use schema::resolve_table;

pub mod ddl;
pub(super) mod dml;
pub(crate) mod resolver;
use dml::{plan_delete, plan_insert, plan_update};
pub(super) mod select;
use select::{plan_select, plan_select_with_joins};
pub(super) mod optimizer;
use optimizer::optimize;
use resolver::ast_expr_name;

// ============================================================================
// Operators
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Plus,
    Negate,
    #[allow(dead_code)]
    Not,
    IsNull,
    IsNotNull,
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
    Like,

    // Logical
    And,
    Or,

    // Bitwise
    LeftShift,
    RightShift,
    BitOr,
    BitXor,
    BitAnd,
}

// ============================================================================
// Plan Types
// ============================================================================

/// Literal values in expressions
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    String(String),
    #[allow(dead_code)]
    Bool(bool),
    #[allow(dead_code)]
    Null,
}

/// Sort key specification for Sort node
#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    pub expr: PlanExpr,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexMaintenanceInfo {
    pub rootpage: u32,
    pub column_idxs: Vec<usize>,
}

/// Aggregate function types
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count, // COUNT(*) or COUNT(expr)
    Sum,   // SUM(expr)
    Avg,   // AVG(expr)
    Min,   // MIN(expr)
    Max,   // MAX(expr)
}

/// Aggregate expression specification
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub argument: Option<PlanExpr>, // None for COUNT(*)
}

/// Planner's expression type - like ast::Expression but with resolved columns
#[derive(Debug, Clone, PartialEq)]
pub enum PlanExpr {
    /// Reference to a column by index in the input node's output
    ColumnRef(usize),
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
    FunctionCall {
        name: String,
        args: Vec<PlanExpr>,
    },
}

/// Logical plan nodes - relational algebra operators
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Scan rows from a table (leaf node, no inputs)
    /// rootpage: the B-tree root page number for this table
    /// columns: indices of columns to read from the table schema
    Scan {
        rootpage: u32,
        columns: Vec<usize>,
        with_key: bool,
    },

    /// Scan via an index
    /// Scan via an index — handles both equality and range predicates.
    /// Equality (col = X): lower_bound = Some((X, true)), upper_bound = Some((X, true))
    /// Range (col > X): lower_bound = Some((X, false)), upper_bound = None
    /// Scan an index B-tree and yield one rowid per matching entry.
    /// Knows nothing about the table; a RowidLookup node above fetches columns.
    IndexScan {
        index_rootpage: u32,
        index_col_idx: usize, // table column index of the indexed column
        lower_bound: Option<(Literal, bool)>, // (value, inclusive)
        upper_bound: Option<(Literal, bool)>, // (value, inclusive)
    },

    /// For each rowid produced by its input, fetch the requested columns from
    /// the table B-tree and yield a full row.
    RowidLookup {
        input: Box<LogicalPlan>,
        table_rootpage: u32,
        columns: Vec<usize>,
    },

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
    Limit { input: Box<LogicalPlan>, count: u64 },

    /// Sort rows based on sort keys (1 input)
    /// Pass-through: outputs all columns from its child unchanged.
    /// Materializes all rows, sorts them, then yields in sorted order.
    Sort {
        input: Box<LogicalPlan>,
        sort_keys: Vec<SortKey>,
    },

    /// Count rows from input (1 input)
    /// Consumes all rows from child and outputs a single row with the count.
    /// Output: single integer column containing the row count.
    Count { input: Box<LogicalPlan> },

    /// Aggregate rows with grouping (1 input)
    /// Groups rows by group_keys, computes aggregates for each group.
    /// Output: group_keys + aggregate results (one column per aggregate)
    Aggregate {
        input: Box<LogicalPlan>,
        group_keys: Vec<PlanExpr>,
        aggregates: Vec<AggregateExpr>,
        having: Option<PlanExpr>,
    },

    /// Emit fixed rows (leaf node, no inputs)
    /// Useful for testing and for VALUES clauses.
    /// Each inner Vec is a row; all rows must have the same number of columns.
    Values { rows: Vec<Vec<Literal>> },

    /// Generate a sequence of integers (leaf node, no inputs)
    /// Useful for testing. Generates rows [start], [start+1], ..., [end-1]
    /// Output: single integer column
    #[allow(dead_code)]
    Sequence { start: i64, end: i64 },

    /// Insert rows into a table (1 input, typically Values)
    /// Consumes all rows from input, writes each to the table's B-tree.
    /// Output: single integer column containing the count of rows inserted.
    Insert {
        rootpage: u32,
        table_columns: Vec<usize>,
        input: Box<LogicalPlan>,
        indexes: Vec<IndexMaintenanceInfo>,
    },

    /// Update rows in a table
    /// Scans table, applies filter, updates matching rows.
    /// Output: single integer column containing the count of rows updated.
    Update {
        rootpage: u32,
        table_columns: Vec<usize>,
        assignments: Vec<(usize, PlanExpr)>, // (column_index, new_value_expr)
        filter: Option<PlanExpr>,
        indexes: Vec<IndexMaintenanceInfo>,
    },

    /// Delete rows from a table
    /// Scans table, applies filter, deletes matching rows by key.
    /// Output: single integer column containing the count of rows deleted.
    Delete {
        rootpage: u32,
        table_columns: Vec<usize>,
        filter: Option<PlanExpr>,
        indexes: Vec<IndexMaintenanceInfo>,
    },

    /// Join two tables (2 inputs)
    /// Performs nested loop join: for each left row, iterate all right rows,
    /// emit combined rows where on_condition is true.
    /// Output: left columns followed by right columns (left_column_count + right_column_count columns)
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on_condition: PlanExpr,
        left_column_count: usize, // for register offset calculation
    },

    /// Deduplicate rows from input (1 input)
    /// Materializes all rows, removes duplicates, yields unique rows.
    /// Output: same columns as input.
    Distinct { input: Box<LogicalPlan> },

    /// Populate an index B-tree from an existing table.
    /// Scans all rows in the table, encoding each row's indexed columns and
    /// primary key as a composite index key, then writes to the index B-tree.
    /// Output: no rows (yields immediately to on_done).
    PopulateIndex {
        input: Box<LogicalPlan>,
        index_rootpage: u32,
        column_idxs: Vec<usize>,
    },
}
// ============================================================================
// Schema (for column resolution)
// ============================================================================

pub mod schema;

// ============================================================================
// Planning
// ============================================================================

/// Extract output column names from a SELECT statement's column list.
///
/// Returns the names in SELECT column order. Wildcards are expanded using the
/// btree catalog to look up the table's column names.
pub fn extract_select_column_names(select: &ast::SelectStatement, btree: &BTree) -> Vec<String> {
    let mut names = Vec::new();
    for col_expr in &select.columns {
        match col_expr {
            ast::ColumnExpression::Named { name, .. } => {
                names.push(name.clone());
            }
            ast::ColumnExpression::Anonyomous(expr) => {
                names.push(ast_expr_name(expr));
            }
            ast::ColumnExpression::Wildcard => {
                // Expand wildcard using catalog
                let table_name = match &select.from {
                    ast::NamedTupleSource::Named { source, .. } => {
                        if let ast::TupleSource::Table(name) = source {
                            name.clone()
                        } else {
                            continue;
                        }
                    }
                    ast::NamedTupleSource::Anonyomous(source) => {
                        if let ast::TupleSource::Table(name) = source {
                            name.clone()
                        } else {
                            continue;
                        }
                    }
                };
                if let Ok(table) = resolve_table(&table_name, btree) {
                    for col in &table.columns {
                        names.push(col.name.clone());
                    }
                }
            }
        }
    }
    names
}

/// Convert an AST Statement to a LogicalPlan by querying the db_schema catalog.
pub fn plan(statement: Statement, btree: &BTree) -> Result<LogicalPlan, PlanError> {
    let naive = match statement {
        Statement::Select(select) => {
            if select.joins.is_empty() {
                plan_select(select, btree)?
            } else {
                plan_select_with_joins(select, btree)?
            }
        }
        Statement::CreateTable(_) | Statement::CreateIndex(_) | Statement::Drop(_) => {
            return Err(PlanError::UnsupportedStatement);
        }
        Statement::Insert(insert) => plan_insert(insert, btree)?,
        Statement::Update(update) => plan_update(update, btree)?,
        Statement::Delete(delete) => plan_delete(delete, btree)?,
        Statement::Explain(inner) => return plan(*inner, btree),
    };
    Ok(optimize(naive, btree))
}

#[derive(Debug, Clone, PartialEq)]
pub enum PlanError {
    TableNotFound(String),
    ColumnNotFound {
        table: String,
        column: String,
    },
    AmbiguousColumn(String),
    ColumnCountMismatch {
        expected: usize,
        got: usize,
    },
    UnsupportedStatement,
    UnknownFunction(String),
    InvalidFunctionArguments {
        function: String,
        expected: usize,
        got: usize,
    },
    InvalidHaving(String),
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::resolver::{
        build_column_mapping, collect_columns, collect_columns_from_column_expr, convert_expr,
        SingleTableResolver,
    };
    use super::schema;
    use super::*;
    use crate::frontend::parse;
    use crate::test::TestDb;
    use std::collections::{HashMap, HashSet};

    // ========================================================================
    // Expression Converter Tests
    // ========================================================================

    fn make_column_map() -> HashMap<String, usize> {
        // Simulates: Scan { columns: [0, 1, 2] } for users(id, name, age)
        // So id → 0, name → 1, age → 2 in scan output
        let mut map = HashMap::new();
        map.insert("id".to_string(), 0);
        map.insert("name".to_string(), 1);
        map.insert("age".to_string(), 2);
        map
    }

    #[test]
    fn test_convert_integer_literal() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        let expr = ast::Expression::Value(ast::ScalarValue::IntegerNumber(42));
        let result = convert_expr(&expr, &resolver).unwrap();

        assert_eq!(result, PlanExpr::Literal(Literal::Integer(42)));
    }

    #[test]
    fn test_convert_float_literal() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        let expr = ast::Expression::Value(ast::ScalarValue::FloatingNumber(3.14));
        let result = convert_expr(&expr, &resolver).unwrap();

        assert_eq!(result, PlanExpr::Literal(Literal::Float(3.14)));
    }

    #[test]
    fn test_convert_column_ref() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        let expr = ast::Expression::Value(ast::ScalarValue::Identifier("age".to_string()));
        let result = convert_expr(&expr, &resolver).unwrap();

        assert_eq!(result, PlanExpr::ColumnRef(2));
    }

    #[test]
    fn test_convert_qualified_column_ref() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        // users.name
        let table_expr = Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
            "users".to_string(),
        )));
        let expr = ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(
            table_expr,
            "name".to_string(),
        ));
        let result = convert_expr(&expr, &resolver).unwrap();

        assert_eq!(result, PlanExpr::ColumnRef(1));
    }

    #[test]
    fn test_convert_qualified_column_wrong_table() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        // other.name - should fail because "other" != "users"
        let table_expr = Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
            "other".to_string(),
        )));
        let expr = ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(
            table_expr,
            "name".to_string(),
        ));
        let result = convert_expr(&expr, &resolver);

        assert_eq!(result, Err(PlanError::TableNotFound("other".to_string())));
    }

    #[test]
    fn test_convert_column_not_found() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        let expr = ast::Expression::Value(ast::ScalarValue::Identifier("nonexistent".to_string()));
        let result = convert_expr(&expr, &resolver);

        assert_eq!(
            result,
            Err(PlanError::ColumnNotFound {
                table: "users".to_string(),
                column: "nonexistent".to_string(),
            })
        );
    }

    #[test]
    fn test_convert_binary_comparison() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        // age > 21
        let expr = ast::Expression::BinaryOp {
            op: ast::BinaryOp::GreaterThan,
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::IntegerNumber(21))),
        };
        let result = convert_expr(&expr, &resolver).unwrap();

        assert_eq!(
            result,
            PlanExpr::BinaryOp {
                op: BinaryOp::GreaterThan,
                left: Box::new(PlanExpr::ColumnRef(2)),
                right: Box::new(PlanExpr::Literal(Literal::Integer(21))),
            }
        );
    }

    #[test]
    fn test_convert_unary_negate() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        // -age
        let expr = ast::Expression::UnaryOp {
            op: ast::UnaryOp::Negate,
            expression: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
        };
        let result = convert_expr(&expr, &resolver).unwrap();

        assert_eq!(
            result,
            PlanExpr::UnaryOp {
                op: UnaryOp::Negate,
                operand: Box::new(PlanExpr::ColumnRef(2)),
            }
        );
    }

    #[test]
    fn test_convert_nested_expression() {
        let columns = make_column_map();
        let resolver = SingleTableResolver {
            table_ref: "users",
            columns: &columns,
        };

        // (age + 1) > 21
        let age_plus_one = ast::Expression::BinaryOp {
            op: ast::BinaryOp::Sum,
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::IntegerNumber(1))),
        };
        let expr = ast::Expression::BinaryOp {
            op: ast::BinaryOp::GreaterThan,
            lhs: Box::new(age_plus_one),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::IntegerNumber(21))),
        };
        let result = convert_expr(&expr, &resolver).unwrap();

        let expected = PlanExpr::BinaryOp {
            op: BinaryOp::GreaterThan,
            left: Box::new(PlanExpr::BinaryOp {
                op: BinaryOp::Add,
                left: Box::new(PlanExpr::ColumnRef(2)),
                right: Box::new(PlanExpr::Literal(Literal::Integer(1))),
            }),
            right: Box::new(PlanExpr::Literal(Literal::Integer(21))),
        };
        assert_eq!(result, expected);
    }

    // ========================================================================
    // Column Collection Tests
    // ========================================================================

    #[test]
    fn test_collect_simple_column() {
        let expr = ast::Expression::Value(ast::ScalarValue::Identifier("age".to_string()));
        let mut columns = HashSet::new();
        collect_columns(&expr, &mut columns);

        assert_eq!(columns, HashSet::from(["age".to_string()]));
    }

    #[test]
    fn test_collect_qualified_column() {
        // users.name
        let table_expr = Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
            "users".to_string(),
        )));
        let expr = ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(
            table_expr,
            "name".to_string(),
        ));
        let mut columns = HashSet::new();
        collect_columns(&expr, &mut columns);

        assert_eq!(columns, HashSet::from(["name".to_string()]));
    }

    #[test]
    fn test_collect_literal_no_columns() {
        let expr = ast::Expression::Value(ast::ScalarValue::IntegerNumber(42));
        let mut columns = HashSet::new();
        collect_columns(&expr, &mut columns);

        assert!(columns.is_empty());
    }

    #[test]
    fn test_collect_binary_expr_columns() {
        // age > 21
        let expr = ast::Expression::BinaryOp {
            op: ast::BinaryOp::GreaterThan,
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::IntegerNumber(21))),
        };
        let mut columns = HashSet::new();
        collect_columns(&expr, &mut columns);

        assert_eq!(columns, HashSet::from(["age".to_string()]));
    }

    #[test]
    fn test_collect_multiple_columns() {
        // name = age (contrived but tests collecting from both sides)
        let expr = ast::Expression::BinaryOp {
            op: ast::BinaryOp::Equals,
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "name".to_string(),
            ))),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
        };
        let mut columns = HashSet::new();
        collect_columns(&expr, &mut columns);

        assert_eq!(
            columns,
            HashSet::from(["name".to_string(), "age".to_string()])
        );
    }

    #[test]
    fn test_collect_nested_columns() {
        // (age + 1) > id
        let age_plus_one = ast::Expression::BinaryOp {
            op: ast::BinaryOp::Sum,
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::IntegerNumber(1))),
        };
        let expr = ast::Expression::BinaryOp {
            op: ast::BinaryOp::GreaterThan,
            lhs: Box::new(age_plus_one),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "id".to_string(),
            ))),
        };
        let mut columns = HashSet::new();
        collect_columns(&expr, &mut columns);

        assert_eq!(
            columns,
            HashSet::from(["age".to_string(), "id".to_string()])
        );
    }

    #[test]
    fn test_collect_from_column_expr_named() {
        // SELECT age AS user_age
        let col_expr = ast::ColumnExpression::Named {
            name: "user_age".to_string(),
            expression: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
        };
        let mut columns = HashSet::new();
        collect_columns_from_column_expr(&col_expr, &mut columns);

        assert_eq!(columns, HashSet::from(["age".to_string()]));
    }

    #[test]
    fn test_collect_from_column_expr_anonymous() {
        // SELECT age + 1
        let col_expr = ast::ColumnExpression::Anonyomous(Box::new(ast::Expression::BinaryOp {
            op: ast::BinaryOp::Sum,
            lhs: Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "age".to_string(),
            ))),
            rhs: Box::new(ast::Expression::Value(ast::ScalarValue::IntegerNumber(1))),
        }));
        let mut columns = HashSet::new();
        collect_columns_from_column_expr(&col_expr, &mut columns);

        assert_eq!(columns, HashSet::from(["age".to_string()]));
    }

    // ========================================================================
    // Column Mapping Tests
    // ========================================================================

    fn make_test_table() -> schema::Table {
        schema::Table {
            name: "users".to_string(),
            rootpage: 5,
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
        }
    }

    #[test]
    fn test_build_column_mapping_simple() {
        let table = make_test_table();
        let columns = HashSet::from(["id".to_string(), "name".to_string()]);

        let mapping = build_column_mapping(&columns, &table, "users").unwrap();

        // Scan should read columns [0, 1] (id, name) in table order
        assert_eq!(mapping.scan_columns, vec![0, 1]);
        // id is at scan position 0, name is at scan position 1
        assert_eq!(mapping.column_map.get("id"), Some(&0));
        assert_eq!(mapping.column_map.get("name"), Some(&1));
    }

    #[test]
    fn test_build_column_mapping_reordered() {
        let table = make_test_table();
        // Request columns in different order than table schema
        let columns = HashSet::from(["age".to_string(), "id".to_string()]);

        let mapping = build_column_mapping(&columns, &table, "users").unwrap();

        // Scan should read columns [0, 2] (id, age) in table order
        assert_eq!(mapping.scan_columns, vec![0, 2]);
        // id is at scan position 0, age is at scan position 1
        assert_eq!(mapping.column_map.get("id"), Some(&0));
        assert_eq!(mapping.column_map.get("age"), Some(&1));
    }

    #[test]
    fn test_build_column_mapping_all_columns() {
        let table = make_test_table();
        let columns = HashSet::from(["id".to_string(), "name".to_string(), "age".to_string()]);

        let mapping = build_column_mapping(&columns, &table, "users").unwrap();

        assert_eq!(mapping.scan_columns, vec![0, 1, 2]);
        assert_eq!(mapping.column_map.get("id"), Some(&0));
        assert_eq!(mapping.column_map.get("name"), Some(&1));
        assert_eq!(mapping.column_map.get("age"), Some(&2));
    }

    #[test]
    fn test_build_column_mapping_column_not_found() {
        let table = make_test_table();
        let columns = HashSet::from(["nonexistent".to_string()]);

        let result = build_column_mapping(&columns, &table, "users");

        assert_eq!(
            result,
            Err(PlanError::ColumnNotFound {
                table: "users".to_string(),
                column: "nonexistent".to_string(),
            })
        );
    }

    // ========================================================================
    // Plan Tests
    // ========================================================================

    /// Create a test database with a "users" table (id, name, age) registered in the catalog.
    /// Returns (TestDb, users_rootpage) - TestDb must be kept alive for the BTree.
    fn make_users_db() -> (TestDb, u32) {
        let mut test = TestDb::default();
        let users_root = test.btree.create_tree();
        test.btree.insert_schema_entry(
            "table",
            "users",
            "users",
            users_root,
            "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
        );
        (test, users_root)
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
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT id, name FROM users");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![0, 1], // id, name
                with_key: false,
            }),
            columns: vec![PlanExpr::ColumnRef(0), PlanExpr::ColumnRef(1)],
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
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT name FROM users WHERE age > 21");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    rootpage: users_root,
                    columns: vec![1, 2], // name, age
                    with_key: false,
                }),
                predicate: PlanExpr::BinaryOp {
                    op: BinaryOp::GreaterThan,
                    left: Box::new(PlanExpr::ColumnRef(1)), // age
                    right: Box::new(PlanExpr::Literal(Literal::Integer(21))),
                },
            }),
            columns: vec![PlanExpr::ColumnRef(0)], // name
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
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT name FROM users LIMIT 10");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Project {
                input: Box::new(LogicalPlan::Scan {
                    rootpage: users_root,
                    columns: vec![1], // name
                    with_key: false,
                }),
                columns: vec![PlanExpr::ColumnRef(0)],
            }),
            count: 10,
        };

        assert_eq!(plan, expected);
    }

    /// SELECT * should expand to all columns
    /// Scan { columns: [0, 1, 2] } reads all columns
    /// Project outputs them in order
    #[test]
    fn test_select_star() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT * FROM users");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![0, 1, 2], // all columns
                with_key: false,
            }),
            columns: vec![
                PlanExpr::ColumnRef(0),
                PlanExpr::ColumnRef(1),
                PlanExpr::ColumnRef(2),
            ],
        };

        assert_eq!(plan, expected);
    }

    #[test]
    fn test_select_star_multi_column() {
        // Create table with 5 columns
        let mut test = TestDb::default();
        let root = test.btree.create_tree();
        test.btree.insert_schema_entry(
            "table",
            "data",
            "data",
            root,
            "CREATE TABLE data (a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)",
        );

        let stmt = parse_sql("SELECT * FROM data");
        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: root,
                columns: vec![0, 1, 2, 3, 4], // all 5 columns
                with_key: false,
            }),
            columns: vec![
                PlanExpr::ColumnRef(0),
                PlanExpr::ColumnRef(1),
                PlanExpr::ColumnRef(2),
                PlanExpr::ColumnRef(3),
                PlanExpr::ColumnRef(4),
            ],
        };

        assert_eq!(plan, expected);
    }

    #[test]
    fn test_select_star_with_literal() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT *, 999 FROM users");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![0, 1, 2], // all columns
                with_key: false,
            }),
            columns: vec![
                PlanExpr::ColumnRef(0),
                PlanExpr::ColumnRef(1),
                PlanExpr::ColumnRef(2),
                PlanExpr::Literal(Literal::Integer(999)),
            ],
        };

        assert_eq!(plan, expected);
    }

    #[test]
    fn test_select_literal_star() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT 999, * FROM users");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![0, 1, 2], // all columns
                with_key: false,
            }),
            columns: vec![
                PlanExpr::Literal(Literal::Integer(999)),
                PlanExpr::ColumnRef(0),
                PlanExpr::ColumnRef(1),
                PlanExpr::ColumnRef(2),
            ],
        };

        assert_eq!(plan, expected);
    }

    #[test]
    fn test_select_star_with_expression() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT *, age + 10 FROM users");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![0, 1, 2], // all columns
                with_key: false,
            }),
            columns: vec![
                PlanExpr::ColumnRef(0),
                PlanExpr::ColumnRef(1),
                PlanExpr::ColumnRef(2),
                PlanExpr::BinaryOp {
                    op: BinaryOp::Add,
                    left: Box::new(PlanExpr::ColumnRef(2)),
                    right: Box::new(PlanExpr::Literal(Literal::Integer(10))),
                },
            ],
        };

        assert_eq!(plan, expected);
    }

    /// Error case: table not found
    #[test]
    fn test_table_not_found() {
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT id FROM nonexistent");

        let result = plan(stmt, &test.btree);

        assert_eq!(
            result,
            Err(PlanError::TableNotFound("nonexistent".to_string()))
        );
    }

    /// Error case: column not found
    #[test]
    fn test_column_not_found() {
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT nonexistent FROM users");

        let result = plan(stmt, &test.btree);

        assert_eq!(
            result,
            Err(PlanError::ColumnNotFound {
                table: "users".to_string(),
                column: "nonexistent".to_string(),
            })
        );
    }

    #[test]
    fn test_select_null_literal() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT NULL FROM users");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![], // No columns needed from scan
                with_key: false,
            }),
            columns: vec![PlanExpr::Literal(Literal::Null)],
        };

        assert_eq!(plan, expected);
    }

    #[test]
    fn test_select_null_with_columns() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("SELECT id, NULL, name FROM users");

        let plan = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                rootpage: users_root,
                columns: vec![0, 1], // id, name
                with_key: false,
            }),
            columns: vec![
                PlanExpr::ColumnRef(0),
                PlanExpr::Literal(Literal::Null),
                PlanExpr::ColumnRef(1),
            ],
        };

        assert_eq!(plan, expected);
    }

    // ========================================================================
    // HAVING Plan Tests
    // ========================================================================

    #[test]
    fn test_plan_having_count_star() {
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 3");
        let result = plan(stmt, &test.btree).expect("Planning failed");
        // Walk to find the Aggregate node
        fn find_aggregate(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            match plan {
                p @ LogicalPlan::Aggregate { .. } => Some(p),
                LogicalPlan::Project { input, .. } => find_aggregate(input),
                LogicalPlan::Sort { input, .. } => find_aggregate(input),
                LogicalPlan::Limit { input, .. } => find_aggregate(input),
                _ => None,
            }
        }
        let agg = find_aggregate(&result).expect("Expected Aggregate node");
        match agg {
            LogicalPlan::Aggregate { having, .. } => {
                assert!(having.is_some(), "expected HAVING predicate in plan");
            }
            _ => panic!("Expected Aggregate node"),
        }
    }

    #[test]
    fn test_plan_having_without_group_by_errors() {
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT id FROM users HAVING COUNT(*) > 1");
        let err = plan(stmt, &test.btree).expect_err("Expected planning error");
        assert!(matches!(err, PlanError::InvalidHaving(_)));
    }

    // ========================================================================
    // INSERT Plan Tests
    // ========================================================================

    #[test]
    fn test_plan_insert_basic() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("INSERT INTO users VALUES (1, 'alice', 30)");

        let result = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Insert {
            rootpage: users_root,
            table_columns: vec![0, 1, 2],
            input: Box::new(LogicalPlan::Values {
                rows: vec![vec![
                    Literal::Integer(1),
                    Literal::String("alice".to_string()),
                    Literal::Integer(30),
                ]],
            }),
            indexes: vec![],
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_plan_insert_with_columns() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("INSERT INTO users (age, name) VALUES (30, 'alice')");

        let result = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Insert {
            rootpage: users_root,
            table_columns: vec![2, 1], // age=2, name=1
            input: Box::new(LogicalPlan::Values {
                rows: vec![vec![
                    Literal::Integer(30),
                    Literal::String("alice".to_string()),
                ]],
            }),
            indexes: vec![],
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_plan_insert_column_count_mismatch() {
        let (test, _) = make_users_db();
        let stmt = parse_sql("INSERT INTO users VALUES (1, 'alice')");

        let result = plan(stmt, &test.btree);

        assert_eq!(
            result,
            Err(PlanError::ColumnCountMismatch {
                expected: 3,
                got: 2,
            })
        );
    }

    #[test]
    fn test_plan_insert_with_expressions() {
        let (test, users_root) = make_users_db();
        let stmt = parse_sql("INSERT INTO users VALUES (1+1, 'alice', 10*3)");

        let result = plan(stmt, &test.btree).expect("Planning failed");

        let expected = LogicalPlan::Insert {
            rootpage: users_root,
            table_columns: vec![0, 1, 2],
            input: Box::new(LogicalPlan::Values {
                rows: vec![vec![
                    Literal::Integer(2),
                    Literal::String("alice".to_string()),
                    Literal::Integer(30),
                ]],
            }),
            indexes: vec![],
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_plan_insert_table_not_found() {
        let (test, _) = make_users_db();
        let stmt = parse_sql("INSERT INTO nonexistent VALUES (1)");

        let result = plan(stmt, &test.btree);

        assert_eq!(
            result,
            Err(PlanError::TableNotFound("nonexistent".to_string()))
        );
    }

    #[test]
    fn test_plan_order_by_column_in_select() {
        // ORDER BY a column that's in SELECT - should work without extra projection
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT name, age FROM users ORDER BY age");

        let plan = plan(stmt, &test.btree).expect("Planning should succeed");

        // Check structure: Scan -> Project -> Sort
        if let LogicalPlan::Sort { input, sort_keys } = plan {
            assert_eq!(sort_keys.len(), 1);
            // Sort key should reference projection column 1 (age in the projection)
            if let PlanExpr::ColumnRef(column_idx) = &sort_keys[0].expr {
                assert_eq!(*column_idx, 1, "age should be at projection index 1");
            } else {
                panic!("Expected simple column reference in sort key");
            }
            // Input should be Project with 2 columns (name, age)
            if let LogicalPlan::Project { columns, .. } = *input {
                assert_eq!(
                    columns.len(),
                    2,
                    "Projection should have 2 columns (name, age)"
                );
            } else {
                panic!("Expected Project node as input to Sort");
            }
        } else {
            panic!("Expected Sort node, got {:?}", plan);
        }
    }

    #[test]
    fn test_plan_order_by_column_not_in_select() {
        // ORDER BY a column NOT in SELECT - should add extended projection and final projection
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT name FROM users ORDER BY age");

        let plan = plan(stmt, &test.btree).expect("Planning should succeed");

        // Check structure: Scan -> Project(name, age) -> Sort -> Project(name)
        if let LogicalPlan::Project { input, columns } = plan {
            assert_eq!(
                columns.len(),
                1,
                "Final projection should have 1 column (name)"
            );

            // Input should be Sort
            if let LogicalPlan::Sort {
                input: sort_input,
                sort_keys,
            } = *input
            {
                assert_eq!(sort_keys.len(), 1);
                // Sort key should reference age at projection index 1
                if let PlanExpr::ColumnRef(column_idx) = &sort_keys[0].expr {
                    assert_eq!(
                        *column_idx, 1,
                        "age should be at projection index 1 in extended projection"
                    );
                } else {
                    panic!("Expected simple column reference in sort key");
                }

                // Sort input should be extended Project with 2 columns (name, age)
                if let LogicalPlan::Project { columns, .. } = *sort_input {
                    assert_eq!(
                        columns.len(),
                        2,
                        "Extended projection should have 2 columns (name, age)"
                    );
                } else {
                    panic!("Expected Project node as input to Sort");
                }
            } else {
                panic!("Expected Sort node as input to final projection");
            }
        } else {
            panic!("Expected final Project node, got {:?}", plan);
        }
    }

    #[test]
    fn test_plan_order_by_multiple_columns() {
        // ORDER BY multiple columns - should handle both in and not in SELECT
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT name FROM users ORDER BY age DESC, name ASC");

        let plan = plan(stmt, &test.btree).expect("Planning should succeed");

        // Should have final projection to remove age
        if let LogicalPlan::Project { input, .. } = plan {
            if let LogicalPlan::Sort { sort_keys, .. } = *input {
                assert_eq!(sort_keys.len(), 2, "Should have 2 sort keys");
                assert_eq!(
                    sort_keys[0].descending, true,
                    "First sort key (age) should be DESC"
                );
                assert_eq!(
                    sort_keys[1].descending, false,
                    "Second sort key (name) should be ASC"
                );
            } else {
                panic!("Expected Sort node");
            }
        } else {
            panic!("Expected final Project node");
        }
    }

    #[test]
    fn test_plan_order_by_with_function_in_select() {
        // ORDER BY column not in SELECT, but SELECT has function expressions
        let (test, _) = make_users_db();
        let stmt = parse_sql("SELECT upper(name) FROM users ORDER BY age");

        let plan = plan(stmt, &test.btree).expect("Planning should succeed");

        // Should have structure: Scan -> Project(upper(name), age) -> Sort -> Project(upper(name))
        if let LogicalPlan::Project { input, columns } = plan {
            assert_eq!(
                columns.len(),
                1,
                "Final projection should have 1 column (upper(name))"
            );

            if let LogicalPlan::Sort { input, sort_keys } = *input {
                assert_eq!(sort_keys.len(), 1);

                // Extended projection should have 2 columns: upper(name) and age
                if let LogicalPlan::Project { columns, .. } = *input {
                    assert_eq!(
                        columns.len(),
                        2,
                        "Extended projection should have upper(name) and age"
                    );
                    // First should be function call, second should be column ref
                    assert!(
                        matches!(columns[0], PlanExpr::FunctionCall { .. }),
                        "First column should be function call"
                    );
                    assert!(
                        matches!(columns[1], PlanExpr::ColumnRef(_)),
                        "Second column should be column ref (age)"
                    );
                } else {
                    panic!("Expected Project node");
                }
            } else {
                panic!("Expected Sort node");
            }
        } else {
            panic!("Expected final Project node");
        }
    }

    #[test]
    fn test_join_resolver() {
        // Build a JoinResolver with:
        //   left: columns [id, name, dept_id], alias "e"
        //   right: columns [id, name], alias "d"
        use super::schema;
        use std::collections::HashMap;

        let left_table = schema::Table {
            name: "employees".to_string(),
            rootpage: 1,
            columns: vec![
                schema::Column {
                    name: "id".to_string(),
                },
                schema::Column {
                    name: "name".to_string(),
                },
                schema::Column {
                    name: "dept_id".to_string(),
                },
            ],
        };

        let right_table = schema::Table {
            name: "departments".to_string(),
            rootpage: 2,
            columns: vec![
                schema::Column {
                    name: "id".to_string(),
                },
                schema::Column {
                    name: "name".to_string(),
                },
            ],
        };

        // Build join column resolution maps
        let mut qualified = HashMap::new();
        let mut unqualified = HashMap::new();
        let left_col_count = left_table.columns.len();

        for (idx, col) in left_table.columns.iter().enumerate() {
            qualified.insert(("e".to_string(), col.name.clone()), idx);
            unqualified
                .entry(col.name.clone())
                .and_modify(|e| *e = None)
                .or_insert(Some(idx));
        }

        for (idx, col) in right_table.columns.iter().enumerate() {
            let combined_idx = left_col_count + idx;
            qualified.insert(("d".to_string(), col.name.clone()), combined_idx);
            unqualified
                .entry(col.name.clone())
                .and_modify(|e| *e = None)
                .or_insert(Some(combined_idx));
        }

        // Test qualified resolution
        assert_eq!(
            qualified.get(&("e".to_string(), "name".to_string())),
            Some(&1)
        );
        assert_eq!(
            qualified.get(&("d".to_string(), "name".to_string())),
            Some(&4)
        );
        assert_eq!(
            qualified.get(&("e".to_string(), "dept_id".to_string())),
            Some(&2)
        );

        // Test unqualified unique column
        assert_eq!(unqualified.get("dept_id"), Some(&Some(2)));

        // Test unqualified ambiguous columns (appear in both tables)
        assert_eq!(unqualified.get("id"), Some(&None));
        assert_eq!(unqualified.get("name"), Some(&None));

        // Test missing column
        assert_eq!(
            qualified.get(&("e".to_string(), "nonexistent".to_string())),
            None
        );
    }

    #[test]
    fn test_convert_expr_with_join_resolver() {
        use super::resolver::{convert_expr, JoinResolver};
        use super::{schema, PlanExpr};
        use std::collections::HashMap;

        let left_table = schema::Table {
            name: "employees".to_string(),
            rootpage: 1,
            columns: vec![
                schema::Column {
                    name: "id".to_string(),
                },
                schema::Column {
                    name: "dept_id".to_string(),
                },
            ],
        };

        let right_table = schema::Table {
            name: "departments".to_string(),
            rootpage: 2,
            columns: vec![schema::Column {
                name: "id".to_string(),
            }],
        };

        // Build join column resolution maps
        let mut qualified = HashMap::new();
        let mut unqualified = HashMap::new();
        let left_col_count = left_table.columns.len();

        for (idx, col) in left_table.columns.iter().enumerate() {
            qualified.insert(("e".to_string(), col.name.clone()), idx);
            unqualified
                .entry(col.name.clone())
                .and_modify(|e| *e = None)
                .or_insert(Some(idx));
        }

        for (idx, col) in right_table.columns.iter().enumerate() {
            let combined_idx = left_col_count + idx;
            qualified.insert(("d".to_string(), col.name.clone()), combined_idx);
            unqualified
                .entry(col.name.clone())
                .and_modify(|e| *e = None)
                .or_insert(Some(combined_idx));
        }

        let resolver = JoinResolver {
            qualified: &qualified,
            unqualified: &unqualified,
        };

        // Test qualified column: e.dept_id → ColumnRef(1)
        let ast_expr = ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(
            Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "e".to_string(),
            ))),
            "dept_id".to_string(),
        ));
        let plan_expr = convert_expr(&ast_expr, &resolver).unwrap();
        assert_eq!(plan_expr, PlanExpr::ColumnRef(1));

        // Test qualified column: d.id → ColumnRef(2)
        let ast_expr2 = ast::Expression::Value(ast::ScalarValue::MultiPartIdentifier(
            Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                "d".to_string(),
            ))),
            "id".to_string(),
        ));
        let plan_expr2 = convert_expr(&ast_expr2, &resolver).unwrap();
        assert_eq!(plan_expr2, PlanExpr::ColumnRef(2));

        // Test unqualified unique column: dept_id → ColumnRef(1)
        let ast_expr3 = ast::Expression::Value(ast::ScalarValue::Identifier("dept_id".to_string()));
        let plan_expr3 = convert_expr(&ast_expr3, &resolver).unwrap();
        assert_eq!(plan_expr3, PlanExpr::ColumnRef(1));

        // Test ambiguous column: id → Error
        let ast_expr4 = ast::Expression::Value(ast::ScalarValue::Identifier("id".to_string()));
        let result = convert_expr(&ast_expr4, &resolver);
        assert!(matches!(result, Err(PlanError::AmbiguousColumn(_))));

        // Test binary operation: e.dept_id = d.id
        let ast_expr5 = ast::Expression::BinaryOp {
            op: ast::BinaryOp::Equals,
            lhs: Box::new(ast::Expression::Value(
                ast::ScalarValue::MultiPartIdentifier(
                    Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                        "e".to_string(),
                    ))),
                    "dept_id".to_string(),
                ),
            )),
            rhs: Box::new(ast::Expression::Value(
                ast::ScalarValue::MultiPartIdentifier(
                    Box::new(ast::Expression::Value(ast::ScalarValue::Identifier(
                        "d".to_string(),
                    ))),
                    "id".to_string(),
                ),
            )),
        };
        let plan_expr5 = convert_expr(&ast_expr5, &resolver).unwrap();
        if let PlanExpr::BinaryOp { left, right, .. } = plan_expr5 {
            assert_eq!(*left, PlanExpr::ColumnRef(1));
            assert_eq!(*right, PlanExpr::ColumnRef(2));
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn test_plan_join() {
        use super::{plan, LogicalPlan, PlanExpr};
        use crate::test::TestDb;

        // Create TestDb and two tables
        let test = TestDb::default();
        let mut btree = test.btree;

        // Get the catalog root
        let catalog_root = btree.schema_root_page().expect("No catalog");

        // Create departments table (id, name)
        let dept_root = btree.create_tree();
        {
            use crate::engine::scalarvalue::ScalarValue;
            let ddl = "CREATE TABLE departments (id INTEGER, name TEXT)";
            let values = vec![
                ScalarValue::String("table".to_string()),
                ScalarValue::String("departments".to_string()),
                ScalarValue::String("departments".to_string()),
                ScalarValue::Integer(dept_root as i64),
                ScalarValue::String(ddl.to_string()),
            ];
            let mut cursor = btree.open(catalog_root);
            let mut c = cursor.open_readwrite();
            let mut buf = Vec::new();
            ciborium::ser::into_writer(&values, &mut buf).unwrap();
            c.insert_u64(1, buf); // key 1 (catalog row 0 is self-referencing db_schema)
        }

        // Create employees table (id, name, dept_id)
        let emp_root = btree.create_tree();
        {
            use crate::engine::scalarvalue::ScalarValue;
            let ddl = "CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER)";
            let values = vec![
                ScalarValue::String("table".to_string()),
                ScalarValue::String("employees".to_string()),
                ScalarValue::String("employees".to_string()),
                ScalarValue::Integer(emp_root as i64),
                ScalarValue::String(ddl.to_string()),
            ];
            let mut cursor = btree.open(catalog_root);
            let mut c = cursor.open_readwrite();
            let mut buf = Vec::new();
            ciborium::ser::into_writer(&values, &mut buf).unwrap();
            c.insert_u64(2, buf); // key 2
        }

        // Plan: "SELECT e.name, d.name FROM employees AS e JOIN departments AS d ON e.dept_id = d.id"
        let stmt = parse_sql(
            "SELECT e.name, d.name FROM employees AS e JOIN departments AS d ON e.dept_id = d.id",
        );
        let plan = plan(stmt, &btree).expect("Planning should succeed");

        // Verify plan structure: Project { Join { Scan(employees), Scan(departments), ... }, ... }
        if let LogicalPlan::Project { input, columns } = plan {
            assert_eq!(columns.len(), 2, "Should project 2 columns");

            // Both should be column references
            assert!(matches!(columns[0], PlanExpr::ColumnRef(_)));
            assert!(matches!(columns[1], PlanExpr::ColumnRef(_)));

            if let LogicalPlan::Join {
                left,
                right,
                on_condition,
                left_column_count,
            } = *input
            {
                assert_eq!(left_column_count, 3, "Employees has 3 columns");

                // Left should be Scan of employees
                assert!(matches!(*left, LogicalPlan::Scan { .. }));

                // Right should be Scan of departments
                assert!(matches!(*right, LogicalPlan::Scan { .. }));

                // ON condition should be a binary operation
                assert!(matches!(on_condition, PlanExpr::BinaryOp { .. }));
            } else {
                panic!("Expected Join node");
            }
        } else {
            panic!("Expected Project node");
        }
    }

    #[test]
    fn test_plan_index_scan() {
        use super::{plan, Literal, LogicalPlan};
        use crate::test::TestDb;

        let test = TestDb::default();
        let mut btree = test.btree;

        // Create table and index
        let sql_table = "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)";
        let users_root = btree.create_tree();
        btree.insert_schema_entry("table", "users", "users", users_root, sql_table);

        let sql_index = "CREATE INDEX idx_age ON users(age)";
        let index_root = btree.create_tree();
        btree.insert_schema_entry("index", "idx_age", "users", index_root, sql_index);

        // Query that should use index
        let stmt = parse_sql("SELECT name FROM users WHERE age = 30");
        let plan = plan(stmt, &btree).expect("Planning failed");

        match plan {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::RowidLookup {
                    input,
                    table_rootpage,
                    ..
                } => match *input {
                    LogicalPlan::IndexScan {
                        index_rootpage,
                        index_col_idx: _,
                        lower_bound,
                        upper_bound,
                    } => {
                        assert_eq!(index_rootpage, index_root);
                        assert_eq!(lower_bound, Some((Literal::Integer(30), true)));
                        assert_eq!(upper_bound, Some((Literal::Integer(30), true)));
                        assert_eq!(table_rootpage, users_root);
                    }
                    _ => panic!("Expected IndexScan inside RowidLookup, got {:?}", input),
                },
                _ => panic!("Expected RowidLookup, got {:?}", input),
            },
            _ => panic!("Expected Project, got {:?}", plan),
        }
    }

    #[test]
    fn test_plan_index_range_scan() {
        use super::{plan, Literal, LogicalPlan};
        use crate::test::TestDb;

        let test = TestDb::default();
        let mut btree = test.btree;

        let sql_table = "CREATE TABLE data (id INTEGER, value INTEGER)";
        let data_root = btree.create_tree();
        btree.insert_schema_entry("table", "data", "data", data_root, sql_table);

        let sql_index = "CREATE INDEX idx_value ON data(value)";
        let index_root = btree.create_tree();
        btree.insert_schema_entry("index", "idx_value", "data", index_root, sql_index);

        // Test greater than
        let stmt = parse_sql("SELECT id FROM data WHERE value > 20");
        let p = plan(stmt, &btree).expect("Planning failed");
        match p {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::RowidLookup {
                    input,
                    table_rootpage,
                    ..
                } => match *input {
                    LogicalPlan::IndexScan {
                        index_rootpage,
                        index_col_idx: _,
                        lower_bound,
                        upper_bound,
                    } => {
                        assert_eq!(index_rootpage, index_root);
                        assert_eq!(lower_bound, Some((Literal::Integer(20), false)));
                        assert_eq!(upper_bound, None);
                        assert_eq!(table_rootpage, data_root);
                    }
                    _ => panic!("Expected IndexScan, got {:?}", input),
                },
                _ => panic!("Expected RowidLookup, got {:?}", input),
            },
            _ => panic!("Expected Project, got {:?}", p),
        }

        // Test range with AND
        let stmt = parse_sql("SELECT id FROM data WHERE value >= 10 AND value <= 40");
        let p = plan(stmt, &btree).expect("Planning failed");
        match p {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::RowidLookup { input, .. } => match *input {
                    LogicalPlan::IndexScan {
                        lower_bound,
                        upper_bound,
                        ..
                    } => {
                        assert_eq!(lower_bound, Some((Literal::Integer(10), true)));
                        assert_eq!(upper_bound, Some((Literal::Integer(40), true)));
                    }
                    _ => panic!("Expected IndexScan, got {:?}", input),
                },
                _ => panic!("Expected RowidLookup, got {:?}", input),
            },
            _ => panic!("Expected Project, got {:?}", p),
        }
    }

    #[test]
    fn test_plan_multi_column_index_uses_first_column() {
        use super::{plan, Literal, LogicalPlan};
        use crate::test::TestDb;

        let test = TestDb::default();
        let mut btree = test.btree;

        let sql_table = "CREATE TABLE events (id INTEGER, year INTEGER, month INTEGER)";
        let root = btree.create_tree();
        btree.insert_schema_entry("table", "events", "events", root, sql_table);

        // Multi-column index on (year, month)
        let sql_index = "CREATE INDEX idx_year_month ON events(year, month)";
        let index_root = btree.create_tree();
        btree.insert_schema_entry("index", "idx_year_month", "events", index_root, sql_index);

        // Query on first column should use the index
        let stmt = parse_sql("SELECT id FROM events WHERE year = 2024");
        let plan = plan(stmt, &btree).expect("Planning failed");

        match plan {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::RowidLookup { input, .. } => match *input {
                    LogicalPlan::IndexScan {
                        index_rootpage,
                        index_col_idx: _,
                        lower_bound,
                        upper_bound,
                    } => {
                        assert_eq!(index_rootpage, index_root);
                        assert_eq!(lower_bound, Some((Literal::Integer(2024), true)));
                        assert_eq!(upper_bound, Some((Literal::Integer(2024), true)));
                    }
                    _ => panic!("Expected IndexScan, got {:?}", input),
                },
                _ => panic!("Expected RowidLookup, got {:?}", input),
            },
            _ => panic!("Expected Project, got {:?}", plan),
        }
    }

    #[test]
    fn test_plan_multi_column_index_not_used_for_non_first_column() {
        use super::{plan, LogicalPlan};
        use crate::test::TestDb;

        let test = TestDb::default();
        let mut btree = test.btree;

        let sql_table = "CREATE TABLE events (id INTEGER, year INTEGER, month INTEGER)";
        let root = btree.create_tree();
        btree.insert_schema_entry("table", "events", "events", root, sql_table);

        // Multi-column index on (year, month) - only second column referenced
        let sql_index = "CREATE INDEX idx_year_month ON events(year, month)";
        let index_root = btree.create_tree();
        btree.insert_schema_entry("index", "idx_year_month", "events", index_root, sql_index);

        // Query on second column should NOT use the index (falls back to table scan)
        let stmt = parse_sql("SELECT id FROM events WHERE month = 6");
        let plan = plan(stmt, &btree).expect("Planning failed");

        // Should NOT contain IndexScan (uses table scan + filter instead)
        fn contains_index_scan(p: &LogicalPlan) -> bool {
            matches!(p, LogicalPlan::IndexScan { .. })
                || match p {
                    LogicalPlan::Project { input, .. } => contains_index_scan(input),
                    LogicalPlan::Filter { input, .. } => contains_index_scan(input),
                    LogicalPlan::RowidLookup { input, .. } => contains_index_scan(input),
                    _ => false,
                }
        }
        assert!(
            !contains_index_scan(&plan),
            "Should not use index for non-first column: {:?}",
            plan
        );
    }

    #[test]
    fn test_plan_distinct() {
        use super::{plan, LogicalPlan};
        use crate::test::TestDb;

        let test = TestDb::default();
        let mut btree = test.btree;

        let sql_table = "CREATE TABLE colors (id INTEGER, category TEXT)";
        let colors_root = btree.create_tree();
        btree.insert_schema_entry("table", "colors", "colors", colors_root, sql_table);

        let stmt = parse_sql("SELECT DISTINCT category FROM colors");
        let plan = plan(stmt, &btree).expect("Planning failed");

        // Plan should be: Distinct { Project { Scan } }
        match plan {
            LogicalPlan::Distinct { input } => match *input {
                LogicalPlan::Project { input, .. } => match *input {
                    LogicalPlan::Scan { rootpage, .. } => {
                        assert_eq!(rootpage, colors_root);
                    }
                    _ => panic!("Expected Scan inside Project, got {:?}", input),
                },
                _ => panic!("Expected Project inside Distinct, got {:?}", input),
            },
            _ => panic!("Expected Distinct at top, got {:?}", plan),
        }
    }

    #[test]
    fn test_plan_delete_gathers_indexes() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let sql_table = "CREATE TABLE users (id INTEGER, age INTEGER)";
        let users_root = btree.create_tree();
        btree.insert_schema_entry("table", "users", "users", users_root, sql_table);

        let sql_index = "CREATE INDEX idx_age ON users(age)";
        let index_root = btree.create_tree();
        btree.insert_schema_entry("index", "idx_age", "users", index_root, sql_index);

        let stmt = parse_sql("DELETE FROM users WHERE id = 1");
        let plan = plan(stmt, &btree).expect("Planning failed");

        if let LogicalPlan::Delete { indexes, .. } = plan {
            assert_eq!(indexes.len(), 1);
            assert_eq!(indexes[0].column_idxs, vec![1]); // age is column index 1
            assert_eq!(indexes[0].rootpage, index_root);
        } else {
            panic!("Expected Delete plan");
        }
    }

    #[test]
    fn test_plan_update_gathers_indexes() {
        let test = TestDb::default();
        let mut btree = test.btree;

        let sql_table = "CREATE TABLE users (id INTEGER, age INTEGER)";
        let users_root = btree.create_tree();
        btree.insert_schema_entry("table", "users", "users", users_root, sql_table);

        let sql_index = "CREATE INDEX idx_age ON users(age)";
        let index_root = btree.create_tree();
        btree.insert_schema_entry("index", "idx_age", "users", index_root, sql_index);

        let stmt = parse_sql("UPDATE users SET age = 30 WHERE id = 1");
        let plan = plan(stmt, &btree).expect("Planning failed");

        if let LogicalPlan::Update { indexes, .. } = plan {
            assert_eq!(indexes.len(), 1);
            assert_eq!(indexes[0].column_idxs, vec![1]); // age is column index 1
            assert_eq!(indexes[0].rootpage, index_root);
        } else {
            panic!("Expected Update plan");
        }
    }

    fn make_btree_with_index_on_age() -> crate::storage::BTree {
        let test = TestDb::default();
        let mut btree = test.btree;
        let sql_table = "CREATE TABLE users (id INTEGER, age INTEGER)";
        let users_root = btree.create_tree();
        btree.insert_schema_entry("table", "users", "users", users_root, sql_table);
        let sql_index = "CREATE INDEX idx_age ON users(age)";
        let index_root = btree.create_tree();
        btree.insert_schema_entry("index", "idx_age", "users", index_root, sql_index);
        btree
    }

    fn plan_contains_sort(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Sort { .. } => true,
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Limit { input, .. } => plan_contains_sort(input),
            LogicalPlan::RowidLookup { input, .. } => plan_contains_sort(input),
            _ => false,
        }
    }

    #[test]
    fn test_sort_elided_for_index_scan() {
        let btree = make_btree_with_index_on_age();
        let stmt = parse_sql("SELECT id FROM users WHERE age > 20 ORDER BY age");
        let plan = plan(stmt, &btree).expect("Planning failed");
        assert!(
            !plan_contains_sort(&plan),
            "expected sort to be elided, got:\n{:#?}",
            plan
        );
    }

    #[test]
    fn test_sort_not_elided_for_desc() {
        let btree = make_btree_with_index_on_age();
        let stmt = parse_sql("SELECT id FROM users WHERE age > 20 ORDER BY age DESC");
        let plan = plan(stmt, &btree).expect("Planning failed");
        assert!(plan_contains_sort(&plan), "DESC should not be elided");
    }

    #[test]
    fn test_sort_not_elided_for_different_column() {
        let btree = make_btree_with_index_on_age();
        let stmt = parse_sql("SELECT id FROM users WHERE age > 20 ORDER BY id");
        let plan = plan(stmt, &btree).expect("Planning failed");
        assert!(
            plan_contains_sort(&plan),
            "non-indexed column should not be elided"
        );
    }
}
