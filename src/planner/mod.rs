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
use select::plan_select;
pub(super) mod optimizer;
use optimizer::{fuse_projects, optimize};
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
    pub unique: bool,
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

/// Join execution strategy, chosen by the optimizer.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinStrategy {
    /// Materialise the entire right side into a RowBuffer once, then for each left row
    /// scan the buffer checking on_condition. Always applicable.
    Hash,
    /// Re-drive the right child once per left row via its reset entry point.
    /// Used when an index-probe is available on the right side.
    NestedLoop,
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
        /// When `None`: yield only the rowid (existing behaviour).
        /// When `Some(cols)`: decode index-key column positions and yield them
        /// directly, skipping the primary B-tree lookup entirely.
        /// `cols[i]` = 0-based index-key column position for output slot i.
        output_columns: Option<Vec<usize>>,
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
        /// If Some(i), column i is an autoincrement PK that was omitted from the
        /// INSERT column list. The compiler fills it from the auto-assigned rowid.
        fill_autoincrement_at: Option<usize>,
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

    /// Join two tables (2 inputs).
    /// Output: left columns followed by right columns (left_column_count + right_column_count columns).
    /// Execution strategy is determined by the `strategy` field.
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on_condition: PlanExpr,
        strategy: JoinStrategy,
        left_column_count: usize, // for register offset calculation
    },

    /// Deduplicate rows from input (1 input)
    /// Materializes all rows, removes duplicates, yields unique rows.
    /// Output: same columns as input.
    Distinct { input: Box<LogicalPlan> },

    /// Dynamic-key counterpart to IndexScan: yields rowids only.
    /// Used as the right child of a NestedLoop join when an index exists on the join column.
    /// Column fetching is delegated to a RowidLookup node above it.
    ///
    /// `key_expr` contains `ColumnRef(i)` references that index into the *left* row's
    /// output registers — the sole exception to the left-column-free zone in right subtrees.
    /// These are resolved via `ctx.outer_regs` in `codegen_index_probe`.
    IndexProbe {
        index_rootpage: u32,
        /// Expression evaluated against left row's registers to produce the probe key.
        /// ColumnRef(i) refers to left output column i — resolved via ctx.outer_regs.
        key_expr: PlanExpr,
        index_col_idx: usize,
    },

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
/// catalog to look up the table's column names.
pub fn extract_select_column_names(select: &ast::SelectStatement, catalog: &BTree) -> Vec<String> {
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
                if let Ok(table) = resolve_table(&table_name, catalog) {
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
pub fn plan(statement: Statement, catalog: &BTree) -> Result<LogicalPlan, PlanError> {
    let naive = match statement {
        Statement::Select(select) => plan_select(select, catalog)?,
        Statement::CreateTable(_) | Statement::CreateIndex(_) | Statement::Drop(_) => {
            return Err(PlanError::UnsupportedStatement);
        }
        Statement::Insert(insert) => plan_insert(insert, catalog)?,
        Statement::Update(update) => plan_update(update, catalog)?,
        Statement::Delete(delete) => plan_delete(delete, catalog)?,
        Statement::Explain(inner) => return plan(*inner, catalog),
    };
    Ok(fuse_projects(optimize(naive, catalog)))
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
    TypeMismatch {
        expected: String,
        got: String,
    },
}
