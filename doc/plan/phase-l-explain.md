# Phase L — EXPLAIN Query Plans

Add an `EXPLAIN` modifier that prints a structured table describing the query plan, enabling both developer insight and SQL-level test assertions that specific plan nodes (e.g. `IndexScan`) are chosen.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 54 | 1 | Parse `EXPLAIN <stmt>` into AST | — |
| 55 | 1 | Recursive plan formatter | 54 |
| 56 | 1 | DB routing: EXPLAIN returns plan table | 55 |
| 57 | 7 | SQL tests asserting plan output | 56 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

Developers currently have no way to know which query plan the engine chose (TableScan vs IndexScan, etc.) without running the REPL's `planner` mode. An `EXPLAIN` modifier makes the chosen plan visible directly in SQL:

```sql
EXPLAIN SELECT id FROM users WHERE age = 30
```

### Output Schema

`EXPLAIN` returns a result table with two columns:

| Column | Type | Description |
|--------|------|-------------|
| `id`   | INTEGER | Sequential node id, starting from 0 |
| `plan` | TEXT | Description of this plan node, indented to show nesting |

Each row represents one node in the logical plan tree. Indentation in the `plan` column (2 spaces per depth level) conveys the parent–child relationship. This makes the output both machine-readable (column values can be pattern-matched in tests) and human-readable (the tree structure is visible).

### Example output

```sql
EXPLAIN SELECT id FROM users WHERE age = 30
-- without index:
```

| id | plan |
|----|------|
| 0  | `Project [id]` |
| 1  | `  Filter [col:age = 30]` |
| 2  | `    Scan users [cols: id, name, age]` |

```sql
-- with index on age:
```

| id | plan |
|----|------|
| 0  | `Project [id]` |
| 1  | `  IndexScan users via idx_age [= 30, cols: id, age]` |

In the SQL test format (tab-separated columns):

```sql
EXPLAIN SELECT id FROM users WHERE age = 30
-- > 0	Project [id]
-- > 1	  Filter [col:age = 30]
-- > 2	    Scan users [cols: id, name, age]
```

---

## 54. Parse `EXPLAIN <stmt>` (Track 1)

### What Changes

Add an `Explain` variant to the AST and teach the parser to recognise the `EXPLAIN` keyword prefix before any DML/DQL statement.

### Background

`EXPLAIN` is a SQL extension (supported by SQLite, PostgreSQL, MySQL). It wraps an arbitrary statement and returns metadata instead of executing it.

### Implementation Approach

**AST (`src/frontend/ast.rs`):**

```rust
pub enum Statement {
    // ... existing variants ...
    Explain(Box<Statement>),
}
```

**Parser (`src/frontend/parser.rs`):**

In `parse_statement()`, check for the `EXPLAIN` keyword before the normal dispatch:

```rust
fn parse_statement(&mut self) -> Result<Statement, ParseError> {
    if self.peek_keyword("EXPLAIN") {
        self.consume(); // eat EXPLAIN
        let inner = self.parse_statement()?;
        // Disallow EXPLAIN EXPLAIN
        if matches!(*inner, Statement::Explain(_)) {
            return Err(ParseError::syntax("cannot nest EXPLAIN"));
        }
        return Ok(Statement::Explain(Box::new(inner)));
    }
    // ... existing dispatch ...
}
```

### Key Files

- `src/frontend/ast.rs` — add `Explain` variant
- `src/frontend/parser.rs` — recognise `EXPLAIN` prefix

### Tests

```rust
#[test]
fn test_parse_explain_select() {
    let stmt = parse("EXPLAIN SELECT id FROM users WHERE age = 30").unwrap();
    assert!(matches!(stmt, Statement::Explain(_)));
    if let Statement::Explain(inner) = stmt {
        assert!(matches!(*inner, Statement::Select(_)));
    }
}

#[test]
fn test_parse_explain_insert() {
    let stmt = parse("EXPLAIN INSERT INTO users VALUES (1, 'alice', 30)").unwrap();
    assert!(matches!(stmt, Statement::Explain(_)));
}

#[test]
fn test_parse_explain_nested_error() {
    assert!(parse("EXPLAIN EXPLAIN SELECT 1").is_err());
}
```

### Implementation Steps (1 commit)

#### Step 54.1 — Add `Explain` to AST and parser

Add `Statement::Explain(Box<Statement>)`. Update `parse_statement()` to detect `EXPLAIN`. Add unit tests.

**Commit:** Parse EXPLAIN prefix into AST

---

## 55. Recursive Plan Formatter (Track 1)

### What Changes

Implement `fn format_plan(plan: &LogicalPlan, schema: &ExplainSchema) -> Vec<(u32, String)>` that recursively walks a `LogicalPlan` and returns a list of `(id, indented_text)` rows.

### Design

**`ExplainSchema`** carries name resolution info looked up from the catalog:

```rust
pub struct TableMeta {
    pub name: String,
    pub columns: Vec<String>,  // ordered by column index
}

pub struct IndexMeta {
    pub name: String,
    pub table_name: String,
}

pub struct ExplainSchema {
    pub tables: HashMap<u32, TableMeta>,   // rootpage → meta
    pub indexes: HashMap<u32, IndexMeta>,  // rootpage → meta
}

impl ExplainSchema {
    pub fn empty() -> Self { /* for tests */ }
    pub fn table_name(&self, rootpage: u32) -> &str { ... }
    pub fn column_name(&self, rootpage: u32, idx: usize) -> String { ... }
    pub fn index_name(&self, rootpage: u32) -> &str { ... }
}
```

**`format_plan`** returns rows in DFS pre-order (parent before children):

```rust
pub fn format_plan(plan: &LogicalPlan, schema: &ExplainSchema) -> Vec<(u32, String)> {
    let mut rows = Vec::new();
    let mut counter = 0u32;
    collect_rows(plan, schema, 0, &mut counter, &mut rows);
    rows
}

fn collect_rows(
    plan: &LogicalPlan,
    schema: &ExplainSchema,
    depth: usize,
    counter: &mut u32,
    rows: &mut Vec<(u32, String)>,
) {
    let id = *counter;
    *counter += 1;
    let indent = "  ".repeat(depth);

    let summary = match plan {
        LogicalPlan::Scan { rootpage, columns, .. } => {
            let table = schema.table_name(*rootpage);
            let cols = resolve_cols(schema, *rootpage, columns);
            format!("{}Scan {} [cols: {}]", indent, table, cols)
        }
        LogicalPlan::IndexScan { index_rootpage, lower_bound, upper_bound, table_rootpage, columns } => {
            let table = schema.table_name(*table_rootpage);
            let index = schema.index_name(*index_rootpage);
            let pred = format_index_predicate(lower_bound, upper_bound);
            let cols = resolve_cols(schema, *table_rootpage, columns);
            format!("{}IndexScan {} via {} [{}, cols: {}]", indent, table, index, pred, cols)
        }
        LogicalPlan::Filter { predicate, .. } => {
            format!("{}Filter [{}]", indent, format_expr(predicate))
        }
        LogicalPlan::Project { columns, .. } => {
            let exprs: Vec<_> = columns.iter().map(format_expr).collect();
            format!("{}Project [{}]", indent, exprs.join(", "))
        }
        LogicalPlan::Limit { count, .. } => format!("{}Limit [{}]", indent, count),
        LogicalPlan::Sort { sort_keys, .. } => {
            let keys: Vec<_> = sort_keys.iter()
                .map(|k| format!("{} {}", format_expr(&k.expr), if k.ascending { "ASC" } else { "DESC" }))
                .collect();
            format!("{}Sort [{}]", indent, keys.join(", "))
        }
        LogicalPlan::Count { .. } => format!("{}Count", indent),
        LogicalPlan::Aggregate { group_keys, aggregates, .. } => {
            format!("{}Aggregate [groups: {}, aggs: {}]", indent, group_keys.len(), aggregates.len())
        }
        LogicalPlan::Join { on_condition, .. } => {
            format!("{}Join [{}]", indent, format_expr(on_condition))
        }
        LogicalPlan::Distinct { .. } => format!("{}Distinct", indent),
        LogicalPlan::Insert { rootpage, .. } => {
            format!("{}Insert [{}]", indent, schema.table_name(*rootpage))
        }
        LogicalPlan::Update { rootpage, .. } => {
            format!("{}Update [{}]", indent, schema.table_name(*rootpage))
        }
        LogicalPlan::Delete { rootpage, .. } => {
            format!("{}Delete [{}]", indent, schema.table_name(*rootpage))
        }
        LogicalPlan::Values { rows: r } => format!("{}Values [{} rows]", indent, r.len()),
        LogicalPlan::PopulateIndex { index_rootpage, .. } => {
            format!("{}PopulateIndex [{}]", indent, schema.index_name(*index_rootpage))
        }
        LogicalPlan::Sequence { start, end } => {
            format!("{}Sequence [{}..{})", indent, start, end)
        }
    };

    rows.push((id, summary));

    // Recurse into children
    for child in plan_children(plan) {
        collect_rows(child, schema, depth + 1, counter, rows);
    }
}

/// Returns child plans in display order (left before right for Join).
fn plan_children(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
    match plan {
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Count { input }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Insert { input, .. }
        | LogicalPlan::PopulateIndex { input, .. } => vec![input],
        LogicalPlan::Join { left, right, .. } => vec![left, right],
        _ => vec![],
    }
}
```

**PlanExpr formatting** (renders column refs with names if schema is available):

```rust
fn format_expr(expr: &PlanExpr) -> String {
    match expr {
        PlanExpr::ColumnRef { index } => format!("col:{}", index),
        PlanExpr::Literal(lit) => format_literal(lit),
        PlanExpr::BinaryOp { op, left, right } =>
            format!("{} {} {}", format_expr(left), op_str(op), format_expr(right)),
        PlanExpr::UnaryOp { op, operand } =>
            format!("{}{}", unary_op_str(op), format_expr(operand)),
        PlanExpr::FunctionCall { name, args } => {
            let a: Vec<_> = args.iter().map(format_expr).collect();
            format!("{}({})", name, a.join(", "))
        }
    }
}
```

**Index predicate formatting:**

```rust
fn format_index_predicate(lower: &Option<(Literal, bool)>, upper: &Option<(Literal, bool)>) -> String {
    match (lower, upper) {
        (Some((lo, true)), Some((hi, true))) if lo == hi => format!("= {}", fmt_lit(lo)),
        (Some((lo, lo_inc)), Some((hi, hi_inc))) => {
            format!("{} {} AND {} {}", if *lo_inc { ">=" } else { ">" }, fmt_lit(lo),
                    if *hi_inc { "<=" } else { "<" }, fmt_lit(hi))
        }
        (Some((lo, inc)), None) => format!("{} {}", if *inc { ">=" } else { ">" }, fmt_lit(lo)),
        (None, Some((hi, inc))) => format!("{} {}", if *inc { "<=" } else { "<" }, fmt_lit(hi)),
        (None, None) => "full scan".to_string(),
    }
}
```

### Key Files

- `src/explain.rs` — new module with all formatting logic
- `src/lib.rs` — `mod explain;`

### Tests

```rust
#[test]
fn test_explain_scan_only() {
    let plan = LogicalPlan::Scan { rootpage: 1, columns: vec![0, 1], with_key: false };
    let rows = format_plan(&plan, &ExplainSchema::empty());
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 0);
    assert!(rows[0].1.contains("Scan"));
}

#[test]
fn test_explain_filter_scan_depth() {
    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan { rootpage: 1, columns: vec![0], with_key: false }),
        predicate: PlanExpr::Literal(Literal::Integer(1)),
    };
    let rows = format_plan(&plan, &ExplainSchema::empty());
    assert_eq!(rows.len(), 2);
    assert!(rows[0].1.starts_with("Filter"));      // depth 0, no indent
    assert!(rows[1].1.starts_with("  Scan"));      // depth 1, 2-space indent
}

#[test]
fn test_explain_join_two_children() {
    // Join should produce 3 rows: Join, left child, right child
    let plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan { rootpage: 1, columns: vec![0], with_key: false }),
        right: Box::new(LogicalPlan::Scan { rootpage: 2, columns: vec![0], with_key: false }),
        on_condition: PlanExpr::Literal(Literal::Integer(1)),
        left_column_count: 1,
    };
    let rows = format_plan(&plan, &ExplainSchema::empty());
    assert_eq!(rows.len(), 3);
}
```

### Implementation Steps (2 commits)

#### Step 55.1 — ExplainSchema and format_plan

Create `src/explain.rs`. Implement `ExplainSchema`, `format_plan()`, `collect_rows()`, `plan_children()`. Add unit tests for Scan, Filter+Scan depth, and Join children.

**Commit:** Add recursive plan formatter returning (id, plan) rows

#### Step 55.2 — PlanExpr and predicate formatting

Implement `format_expr()`, `format_index_predicate()`, `format_literal()`. Test that complex expressions render correctly.

**Commit:** Format PlanExpr and index predicates in EXPLAIN output

---

## 56. DB Layer: EXPLAIN Returns Plan Table (Track 1)

### What Changes

When `Db::execute()` receives a `Statement::Explain(inner)`, it plans the inner statement, builds an `ExplainSchema` from the catalog, formats the plan, and returns the rows as a two-column query result. No bytecode is compiled; no execution happens.

### Implementation Approach

**In `src/db.rs` execution entry point:**

```rust
pub fn execute(&mut self, sql: &str) -> Result<QueryResult, DbError> {
    let stmt = parse(sql)?;
    match stmt {
        Statement::Explain(inner) => self.run_explain(*inner),
        other => self.run_statement(other),
    }
}

fn run_explain(&self, stmt: Statement) -> Result<QueryResult, DbError> {
    let plan = self.planner.plan(stmt)?;
    let schema = self.build_explain_schema()?;
    let rows = format_plan(&plan, &schema);
    // Convert to two-column result: [INTEGER id, TEXT plan]
    let result_rows: Vec<Vec<ScalarValue>> = rows.into_iter()
        .map(|(id, text)| vec![ScalarValue::Integer(id as i64), ScalarValue::String(text)])
        .collect();
    Ok(QueryResult::rows(result_rows))
}

fn build_explain_schema(&self) -> Result<ExplainSchema, DbError> {
    let mut schema = ExplainSchema::default();
    for entry in self.btree.catalog_entries()? {
        match entry.type_.as_str() {
            "table" => {
                let cols = parse_column_names_from_ddl(&entry.sql)?;
                schema.tables.insert(entry.rootpage, TableMeta {
                    name: entry.name,
                    columns: cols,
                });
            }
            "index" => {
                schema.indexes.insert(entry.rootpage, IndexMeta {
                    name: entry.name,
                    table_name: entry.tbl_name,
                });
            }
            _ => {}
        }
    }
    Ok(schema)
}
```

**Output column names:** The result has two columns named `id` and `plan`. This fits the existing `QueryResult` model — the REPL and SQL test runner already print tab-separated column values per row, so the output for a filter-scan plan looks like:

```
0	Project [id]
1	  Filter [col:age = 30]
2	    Scan users [cols: id, name, age]
```

### Key Files

- `src/db.rs` — route `Statement::Explain`, implement `run_explain()`, `build_explain_schema()`
- `src/explain.rs` — `ExplainSchema` updated with name resolution helpers

### Tests

```rust
#[test]
fn test_explain_produces_table_scan() {
    let mut db = TestDb::new();
    db.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    let rows = db.execute_rows("EXPLAIN SELECT id, name FROM users WHERE age = 30").unwrap();
    // Each row is [id, plan_text]
    let plan_texts: Vec<&str> = rows.iter().map(|r| r[1].as_str()).collect();
    let joined = plan_texts.join("\n");
    assert!(joined.contains("Scan users"), "expected table scan, got:\n{}", joined);
    assert!(!joined.contains("IndexScan"), "expected no index scan, got:\n{}", joined);
}

#[test]
fn test_explain_produces_index_scan() {
    let mut db = TestDb::new();
    db.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    db.execute("CREATE INDEX idx_age ON users(age)").unwrap();
    let rows = db.execute_rows("EXPLAIN SELECT id FROM users WHERE age = 30").unwrap();
    let plan_texts: Vec<_> = rows.iter().map(|r| r[1].as_str()).collect();
    let joined = plan_texts.join("\n");
    assert!(joined.contains("IndexScan users via idx_age"), "expected index scan, got:\n{}", joined);
    assert!(joined.contains("= 30"), "expected equality predicate, got:\n{}", joined);
}

#[test]
fn test_explain_does_not_execute() {
    // EXPLAIN INSERT should not actually insert any rows
    let mut db = TestDb::new();
    db.execute("CREATE TABLE t (id INTEGER)").unwrap();
    db.execute("EXPLAIN INSERT INTO t VALUES (1)").unwrap();
    let rows = db.execute_rows("SELECT id FROM t").unwrap();
    assert!(rows.is_empty(), "EXPLAIN should not have inserted rows");
}
```

### Implementation Steps (2 commits)

#### Step 56.1 — Route EXPLAIN in Db::execute

Add `Statement::Explain` match arm. Stub `build_explain_schema()` returning empty schema. Verify `run_explain()` returns two-column rows and integration tests pass (with column-index-based plan text).

**Commit:** Route EXPLAIN to plan formatter in Db::execute

#### Step 56.2 — Resolve names from catalog

Implement `build_explain_schema()` scanning catalog entries. Update `ExplainSchema` helpers to resolve table/column/index names. Integration tests assert human-readable table and index names.

**Commit:** Resolve table, column, and index names in EXPLAIN output

---

## 57. SQL Tests for EXPLAIN Output (Track 7)

### What Changes

Add `tests/sql/explain.sql` that asserts specific plan nodes appear in `EXPLAIN` output, validating:
1. A table scan is chosen when no index exists.
2. An index scan is chosen when an applicable index exists.
3. Range predicates render correctly in index scan output.

### Test File Sketch

```sql
-- tests/sql/explain.sql
-- Note: exact -- > lines are filled in by update-sql-tests after implementation

CREATE TABLE products (id INTEGER, name TEXT, price INTEGER)
-- > Table 'products' created

-- No index: should use Scan + Filter
EXPLAIN SELECT id, name FROM products WHERE price = 100
-- > (filled by update-sql-tests — must contain "Scan products", not "IndexScan")

-- Add index
CREATE INDEX idx_price ON products(price)
-- > Index 'idx_price' created

-- With index: should use IndexScan
EXPLAIN SELECT id FROM products WHERE price = 100
-- > (filled by update-sql-tests — must contain "IndexScan products via idx_price")

-- Range predicate
EXPLAIN SELECT id FROM products WHERE price > 50
-- > (filled by update-sql-tests — must contain "IndexScan" and "> 50")

-- LIMIT
EXPLAIN SELECT id FROM products LIMIT 5
-- > (filled by update-sql-tests — must contain "Limit [5]" and "Scan products")
```

After running `cargo run --bin update-sql-tests explain`, the actual output lines replace the stubs and become the pinned expectations.

### Implementation Steps (1 commit)

#### Step 57.1 — Add explain.sql test file

Write the SQL test file. Run `cargo run --bin update-sql-tests explain` to capture actual output. Review the diff to confirm `IndexScan` appears for the indexed query and `Scan` appears for the non-indexed one. Commit the test with actual expected lines.

**Commit:** Add SQL integration tests for EXPLAIN output

---

## Verification

- [ ] Tests pass: `cargo test`
- [ ] SQL explain tests pass: `cargo test test_sql_explain`
- [ ] Zero warnings: `cargo fmt && cargo build 2>&1 | grep -i warning`
- [ ] Each commit is independently testable
- [ ] `EXPLAIN INSERT` does not mutate state
- [ ] `EXPLAIN SELECT` with index shows `IndexScan`; without index shows `Scan` + `Filter`

**End-to-end REPL check:**
```bash
cargo run -- test.db sql "CREATE TABLE users (id INTEGER, age INTEGER)"
cargo run -- test.db sql "CREATE INDEX idx_age ON users(age)"
cargo run -- test.db sql "EXPLAIN SELECT id FROM users WHERE age = 30"
# Expected output (two tab-separated columns):
# 0	Project [id]
# 1	  IndexScan users via idx_age [= 30, cols: id, age]
```
