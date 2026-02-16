# Phase G1 — IS NULL / IS NOT NULL + INNER JOIN

Phase G1 adds NULL-testing predicates and cross-table query support via INNER JOIN.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 32 | 2.2 | IS NULL / IS NOT NULL | Phase B: NULL support |
| 33 | 4.5 | INNER JOIN | — |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## 32. IS NULL / IS NOT NULL (Track 2.2)

### What Changes

Parser recognizes `expr IS NULL` and `expr IS NOT NULL`. New unary ops in planner/compiler/engine.

### Implementation

#### 32a. Lexer (`src/frontend/lexer.rs`)

Add two new keywords to the `Type` enum: `Is` and `Not`.

In the keyword matching trie:
- Under `'i'` branch: add `"is"` → `Type::Is`
- Under `'n'` branch: add `"not"` → `Type::Not` (currently only matches `"null"`)

#### 32b. AST (`src/frontend/ast.rs`)

Add two variants to the `UnaryOp` enum:
```rust
pub enum UnaryOp {
    Plus,
    Negate,
    IsNull,
    IsNotNull,
}
```

#### 32c. Parser (`src/frontend/parser.rs`)

After parsing a comparison expression, check for `IS [NOT] NULL` as a postfix operation:
```
if peek == Type::Is {
    advance(); // consume IS
    if peek == Type::Not {
        advance(); // consume NOT
        expect(Null);
        wrap expression in UnaryOp::IsNotNull
    } else {
        expect(Null);
        wrap expression in UnaryOp::IsNull
    }
}
```

Add `Expect::Is` variant to the Expect enum.

#### 32d. Planner (`src/planner.rs`)

Add `IsNull` and `IsNotNull` to the planner's `UnaryOp` enum. Update `convert_unary_op()` to map the new AST variants.

#### 32e. VM Operations (`src/engine/program.rs`)

Add two new operations:
```rust
IsNullValue(Reg, Reg),     // dest = (src IS NULL) → Boolean
IsNotNullValue(Reg, Reg),  // dest = (src IS NOT NULL) → Boolean
```

Update `Display` impl and `finalize_with_offset` (no-jump-target arm).

#### 32f. Compiler (`src/compiler/expr.rs`)

In `compile_unary_op`, add cases for `IsNull` and `IsNotNull` that emit the new VM operations.

#### 32g. Engine (`src/engine/mod.rs`)

Execute the operations:
```rust
IsNullValue(dest, src) => {
    registers[dest] = ScalarValue::Boolean(registers[src] == ScalarValue::Null);
}
IsNotNullValue(dest, src) => {
    registers[dest] = ScalarValue::Boolean(registers[src] != ScalarValue::Null);
}
```

### Tests

SQL test file `tests/sql/is_null.sql`:
```sql
CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)
INSERT INTO t VALUES (1, 'Alice', 30)
INSERT INTO t VALUES (2, NULL, 25)
INSERT INTO t VALUES (3, 'Charlie', NULL)
INSERT INTO t VALUES (4, NULL, NULL)
SELECT id FROM t WHERE name IS NULL
SELECT id FROM t WHERE name IS NOT NULL
SELECT id FROM t WHERE age IS NULL
SELECT id FROM t WHERE age IS NOT NULL
SELECT id FROM t WHERE name IS NULL AND age IS NOT NULL
```

---

## 33. INNER JOIN (Track 4.5)

### What Changes

Support `SELECT ... FROM a [INNER] JOIN b ON condition` with nested loop join.

### Background: Why This Is Non-Trivial

The compiler's node system uses a label-based push pipeline:
- Each node generates bytecode with entry points via `NodeContinuation { on_tuple, on_done }`
- Each node returns `NodeOutput { next, output_regs }`
- Nodes compose by wiring these labels together

**The problem:** For a nested loop join, the inner scan must restart from the beginning for each outer row. But `codegen_scan` (in `src/compiler/nodes.rs`) emits `Open()` + `MoveCursor(First)` in the **init emitter** (runs once at startup). There is no facility to reset a scan node from the body loop.

**The solution:** Write a self-contained `codegen_join` function that directly emits cursor operations for both tables. This bypasses normal node composition for the join itself, avoiding any changes to `NodeOutput`, `NodeContinuation`, or existing node codegen functions.

### Implementation

#### 33a. Lexer (`src/frontend/lexer.rs`)

Add keywords:
- `Join` — under `'j'` branch (new): `'j' => match_reserved(ident, "join", Type::Join)`
- `On` — under `'o'` branch: add `"on"` matching (currently handles `or`, `order`)
- `Inner` — under `'i'` branch: add `"inner"` matching

#### 33b. AST (`src/frontend/ast.rs`)

Add a `JoinClause` struct and a `joins` field to `SelectStatement`:

```rust
#[derive(Debug)]
pub struct JoinClause {
    pub table: NamedTupleSource,
    pub on_condition: Expression,
}

#[derive(Debug)]
pub struct SelectStatement {
    pub columns: Vec<ColumnExpression>,
    pub from: NamedTupleSource,
    pub joins: Vec<JoinClause>,              // NEW — empty for non-join queries
    pub filter: Option<Expression>,
    pub limit: Option<Expression>,
    pub order_by: Option<Vec<OrderByClause>>,
    pub group_by: Option<Vec<Expression>>,
}
```

Every existing construction of `SelectStatement` must add `joins: vec![]`.

#### 33c. Parser (`src/frontend/parser.rs`)

In `parse_select_statement()`, after parsing the FROM clause (`let from = self.parse_named_tuple_source()?;`), parse optional JOIN clauses:

```rust
let mut joins = Vec::new();
loop {
    match self.input.peek() {
        lexer::Type::Inner => {
            self.input.advance(); // consume INNER
            self.input.expect(Expect::Join)?;
            let table = self.parse_named_tuple_source()?;
            self.input.expect(Expect::On)?;
            let on_condition = self.parse_expression()?;
            joins.push(ast::JoinClause { table, on_condition });
        }
        lexer::Type::Join => {
            self.input.advance(); // consume JOIN
            let table = self.parse_named_tuple_source()?;
            self.input.expect(Expect::On)?;
            let on_condition = self.parse_expression()?;
            joins.push(ast::JoinClause { table, on_condition });
        }
        _ => break,
    }
}
```

Add `Expect::Join` and `Expect::On` variants to the Expect enum and its `expect()` implementation.

Note: Use `parse_expression()` for the ON condition (not `parse_filter_expression()` or similar) — the ON condition is a full expression, typically `a.col = b.col`.

#### 33d. Planner (`src/planner.rs`) — Column Resolution

**New LogicalPlan variant:**
```rust
pub enum LogicalPlan {
    // ... existing ...
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on_condition: PlanExpr,
        left_column_count: usize,  // for register offset calculation
    },
}
```

**New JoinExprContext for multi-table column resolution:**

The existing `ExprContext` only supports a single table. For joins, create a `JoinExprContext`:

```rust
struct JoinExprContext {
    /// Maps (table_name_or_alias, column_name) → position in combined output
    qualified: HashMap<(String, String), usize>,
    /// Maps column_name → Some(position) if unambiguous, None if ambiguous
    unqualified: HashMap<String, Option<usize>>,
}
```

Build it from both tables' column definitions:
1. Left table columns → positions `0..left_col_count`
2. Right table columns → positions `left_col_count..left_col_count + right_col_count`
3. For each column name: if it appears in both tables, mark as `None` (ambiguous) in the unqualified map

**New `convert_expr_join` function** (parallel to existing `convert_expr`):

Handles `Identifier` and `MultiPartIdentifier` using `JoinExprContext`:
- `Identifier(name)` → look up in `unqualified` map; error if `None` (ambiguous) or missing
- `MultiPartIdentifier(table_expr, column)` → extract table name, look up `(table, column)` in `qualified` map
- All other expression types recurse normally

**New `plan_select_with_joins` function:**

Called from `plan()` when `select.joins` is non-empty. Structure:

```rust
fn plan_select_with_joins(select: ast::SelectStatement, btree: &BTree) -> Result<LogicalPlan, PlanError> {
    // 1. Resolve left table (FROM clause) using existing extract_table_info + resolve_table
    let (left_name, left_ref) = extract_table_info(&select.from)?;
    let left_table = resolve_table(&left_name, btree)?;
    let left_col_count = left_table.columns.len();

    // 2. Resolve right table (first join clause)
    let join_clause = &select.joins[0]; // support single join for now
    let (right_name, right_ref) = extract_table_info(&join_clause.table)?;
    let right_table = resolve_table(&right_name, btree)?;
    let right_col_count = right_table.columns.len();

    // 3. Build JoinExprContext
    let join_ctx = build_join_expr_context(&left_table, &left_ref, &right_table, &right_ref)?;

    // 4. Build scan plans (read ALL columns from each table)
    let left_scan = LogicalPlan::Scan {
        rootpage: left_table.rootpage,
        columns: (0..left_col_count).collect(),
    };
    let right_scan = LogicalPlan::Scan {
        rootpage: right_table.rootpage,
        columns: (0..right_col_count).collect(),
    };

    // 5. Convert ON condition using join context
    let on_condition = convert_expr_join(&join_clause.on_condition, &join_ctx)?;

    // 6. Build Join plan
    let mut plan = LogicalPlan::Join {
        left: Box::new(left_scan),
        right: Box::new(right_scan),
        on_condition,
        left_column_count: left_col_count,
    };

    // 7. Add WHERE filter if present (also uses join context)
    if let Some(ref filter) = select.filter {
        let predicate = convert_expr_join(filter, &join_ctx)?;
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }

    // 8. Project SELECT columns
    // Handle SELECT * (wildcard) by expanding to all columns from both tables
    let project_columns = convert_select_columns_join(&select.columns, &join_ctx)?;
    plan = LogicalPlan::Project {
        input: Box::new(plan),
        columns: project_columns,
    };

    // 9. ORDER BY / LIMIT (same as plan_select, using join context for expressions)
    // ...

    Ok(plan)
}
```

**Column resolution example:**

For `SELECT e.name, d.name FROM employees AS e JOIN departments AS d ON e.dept_id = d.id`:
- Left table `employees` (alias `e`): columns `[id, name, dept_id]` → positions 0, 1, 2
- Right table `departments` (alias `d`): columns `[id, name]` → positions 3, 4
- `qualified[("e", "id")] = 0`, `qualified[("e", "name")] = 1`, `qualified[("e", "dept_id")] = 2`
- `qualified[("d", "id")] = 3`, `qualified[("d", "name")] = 4`
- `unqualified["dept_id"] = Some(2)` (unique to e)
- `unqualified["id"] = None` (ambiguous — in both tables)
- `unqualified["name"] = None` (ambiguous — in both tables)
- `e.name` → `ColumnRef::Single { column_idx: 1 }`
- `d.name` → `ColumnRef::Single { column_idx: 4 }`
- `e.dept_id = d.id` → `BinaryOp { Equals, ColumnRef(2), ColumnRef(3) }`

**New error variant:** `PlanError::AmbiguousColumn(String)` for unqualified references to columns in both tables.

#### 33e. Compiler (`src/compiler/nodes.rs`) — The Nested Loop

**New `codegen_join` function** — self-contained, does NOT compose child nodes via `codegen()`:

```rust
pub fn codegen_join(
    left: &LogicalPlan,
    right: &LogicalPlan,
    on_condition: &PlanExpr,
    left_column_count: usize,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // Extract Scan info — for v1, both children must be Scan nodes
    let (left_rootpage, left_columns) = match left {
        LogicalPlan::Scan { rootpage, columns } => (*rootpage, columns),
        _ => panic!("Join currently requires Scan children"),
    };
    let (right_rootpage, right_columns) = match right {
        LogicalPlan::Scan { rootpage, columns } => (*rootpage, columns),
        _ => panic!("Join currently requires Scan children"),
    };

    // Allocate registers
    let left_cursor = ctx.registers.alloc();
    let right_cursor = ctx.registers.alloc();
    let flag_reg = ctx.registers.alloc();

    // Allocate read registers for left table
    let left_num_read = left_columns.iter().max().map(|&m| m + 1).unwrap_or(0);
    let left_all_regs = ctx.registers.alloc_block(left_num_read);
    let left_output: Vec<Reg> = left_columns.iter().map(|&i| left_all_regs[i]).collect();

    // Allocate read registers for right table
    let right_num_read = right_columns.iter().max().map(|&m| m + 1).unwrap_or(0);
    let right_all_regs = ctx.registers.alloc_block(right_num_read);
    let right_output: Vec<Reg> = right_columns.iter().map(|&i| right_all_regs[i]).collect();

    // Combined output: left columns then right columns
    let mut combined_output = left_output.clone();
    combined_output.extend(right_output.clone());

    // --- INIT ---
    ctx.init_emitter.emit(Operation::Open(left_cursor, left_rootpage));
    ctx.init_emitter.emit(Operation::MoveCursor(left_cursor, MoveOperation::First));
    ctx.init_emitter.emit(Operation::Open(right_cursor, right_rootpage));
    // NOTE: Do NOT MoveCursor(right, First) here — done per outer row in body

    // --- BODY ---
    let outer_check = ctx.body_emitter.create_label();
    let inner_check = ctx.body_emitter.create_label();

    // OUTER_CHECK: read next outer row
    ctx.body_emitter.bind_label(outer_check);
    ctx.body_emitter.emit(Operation::CanReadCursor(flag_reg, left_cursor));
    ctx.body_emitter.emit_goto_if_false(cont.on_done, flag_reg);  // outer exhausted → done
    ctx.body_emitter.emit(Operation::ReadCursor(left_all_regs.clone(), left_cursor));
    ctx.body_emitter.emit(Operation::MoveCursor(left_cursor, MoveOperation::Next));
    // Reset inner cursor for this outer row
    ctx.body_emitter.emit(Operation::MoveCursor(right_cursor, MoveOperation::First));

    // INNER_CHECK: read next inner row
    ctx.body_emitter.bind_label(inner_check);
    ctx.body_emitter.emit(Operation::CanReadCursor(flag_reg, right_cursor));
    ctx.body_emitter.emit_goto_if_false(outer_check, flag_reg);   // inner exhausted → next outer
    ctx.body_emitter.emit(Operation::ReadCursor(right_all_regs.clone(), right_cursor));
    ctx.body_emitter.emit(Operation::MoveCursor(right_cursor, MoveOperation::Next));

    // Evaluate ON condition against combined registers
    let pred_reg = compile_expr(on_condition, &combined_output, ...);
    ctx.body_emitter.emit_goto_if_false(inner_check, pred_reg);   // no match → next inner
    ctx.body_emitter.emit_goto(cont.on_tuple);                    // match → emit row

    // JOIN_NEXT: parent calls this to get next matching row
    let join_next = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_label(join_next);
    ctx.body_emitter.emit_goto(inner_check);                      // resume inner iteration

    NodeOutput {
        next: join_next,
        output_regs: combined_output,
    }
}
```

**Bytecode execution flow:**

```
INIT:
  Open(left_cursor, left_rootpage)
  MoveCursor(left_cursor, First)
  Open(right_cursor, right_rootpage)

BODY:
  OUTER_CHECK:
    CanReadCursor(flag, left_cursor)
    GoToIfFalse(cont.on_done, flag)          → all done when outer exhausted
    ReadCursor(left_regs, left_cursor)
    MoveCursor(left_cursor, Next)            → advance outer (for next time)
    MoveCursor(right_cursor, First)          → RESET inner scan

  INNER_CHECK:
    CanReadCursor(flag, right_cursor)
    GoToIfFalse(OUTER_CHECK, flag)           → inner exhausted, get next outer row
    ReadCursor(right_regs, right_cursor)
    MoveCursor(right_cursor, Next)           → advance inner
    <evaluate ON condition>
    GoToIfFalse(INNER_CHECK, pred_reg)       → no match, try next inner
    GoTo(cont.on_tuple)                      → match! emit combined row

  JOIN_NEXT:                                 → parent calls here for next row
    GoTo(INNER_CHECK)                        → continue inner iteration
```

**Why this works correctly:**
1. Outer cursor is advanced BEFORE the inner loop. So when inner exhausts and jumps to OUTER_CHECK, we read the next outer row.
2. `JOIN_NEXT` resumes inner iteration — there may be more inner matches for the current outer row.
3. When the parent calls `next` after receiving a matched row, it goes to `JOIN_NEXT` → `INNER_CHECK`, which continues scanning inner rows for the current outer row.
4. When inner rows are exhausted for one outer row, control goes to `OUTER_CHECK` for the next outer row.

**Wire into `codegen()` dispatch** (around line 1170):
```rust
LogicalPlan::Join { left, right, on_condition, left_column_count } => {
    codegen_join(left, right, on_condition, *left_column_count, cont, ctx)
}
```

#### 33f. compile_expr for JOIN

The existing `compile_expr` in `src/compiler/expr.rs` takes `input_regs: &[Reg]` and maps `ColumnRef::Single { column_idx }` to `input_regs[column_idx]`. Since the join's `combined_output` is `left_regs ++ right_regs` and the planner produces column indices into this combined layout, `compile_expr` works without modification.

### Tests

SQL test file `tests/sql/inner_join.sql`:

```sql
-- Setup
CREATE TABLE departments (id INTEGER, name TEXT)
CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER)
INSERT INTO departments VALUES (1, 'Engineering')
INSERT INTO departments VALUES (2, 'Sales')
INSERT INTO departments VALUES (3, 'Marketing')
INSERT INTO employees VALUES (100, 'Alice', 1)
INSERT INTO employees VALUES (101, 'Bob', 2)
INSERT INTO employees VALUES (102, 'Charlie', 1)
INSERT INTO employees VALUES (103, 'Diana', 2)

-- Basic INNER JOIN with qualified column names
SELECT employees.name, departments.name FROM employees JOIN departments ON employees.dept_id = departments.id

-- INNER JOIN keyword variant
SELECT employees.name, departments.name FROM employees INNER JOIN departments ON employees.dept_id = departments.id

-- JOIN with table aliases
SELECT e.name, d.name FROM employees AS e JOIN departments AS d ON e.dept_id = d.id

-- JOIN with WHERE clause
SELECT e.name, d.name FROM employees AS e JOIN departments AS d ON e.dept_id = d.id WHERE d.name = 'Engineering'

-- JOIN with no matches (department 3 has no employees)
SELECT d.name, e.name FROM departments AS d JOIN employees AS e ON d.id = e.dept_id WHERE d.id = 3

-- Self-join
CREATE TABLE people (id INTEGER, name TEXT, dept INTEGER)
INSERT INTO people VALUES (1, 'Alice', 10)
INSERT INTO people VALUES (2, 'Bob', 10)
INSERT INTO people VALUES (3, 'Charlie', 20)
SELECT a.name, b.name FROM people AS a JOIN people AS b ON a.dept = b.dept WHERE a.id < b.id

-- SELECT * with JOIN (all columns from both tables)
SELECT * FROM employees AS e JOIN departments AS d ON e.dept_id = d.id WHERE e.id = 100

-- COUNT with JOIN
SELECT COUNT() FROM employees AS e JOIN departments AS d ON e.dept_id = d.id

-- ORDER BY with JOIN
SELECT e.name, d.name FROM employees AS e JOIN departments AS d ON e.dept_id = d.id ORDER BY e.name
```

---

## Verification

For each item:
- [ ] Tests written first (TDD)
- [ ] All tests pass: `cargo test --bin database`
- [ ] `cargo fmt && cargo build 2>&1 | grep -i warning`
