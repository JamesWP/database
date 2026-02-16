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

In the `parse_equality` method (line 649), after the `while` loop that handles `=` / `!=`, add a check for `IS [NOT] NULL` as a postfix operation on the parsed expression:
```rust
// After the while loop in parse_equality, before Ok(expr):
if matches!(self.input.peek(), lexer::Type::Is) {
    self.input.advance(); // consume IS
    if matches!(self.input.peek(), lexer::Type::Not) {
        self.input.advance(); // consume NOT
        self.input.expect(Expect::Null)?;
        expr = ast::Expression::UnaryOp {
            op: ast::UnaryOp::IsNotNull,
            expression: Box::new(expr),
        };
    } else {
        self.input.expect(Expect::Null)?;
        expr = ast::Expression::UnaryOp {
            op: ast::UnaryOp::IsNull,
            expression: Box::new(expr),
        };
    }
}
```

Add `Expect::Null` to the `Expect` enum (line 130) and add the matching arm in the `expect()` method (line 48): `(Expect::Null, lexer::Type::Null) => { self.advance(); Ok(()) }`. Follow the exact pattern of existing arms like `Expect::From`.

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

#### 32g. Engine (`src/engine.rs`)

Execute the operations. Note: the engine file is `src/engine.rs` (not `mod.rs`). Find the main `match` on operations and add:
```rust
IsNullValue(dest, src) => {
    let is_null = matches!(self.registers.get(*src), RegisterValue::ScalarValue(ScalarValue::Null));
    *self.registers.get_mut(*dest) = RegisterValue::ScalarValue(ScalarValue::Boolean(is_null));
}
IsNotNullValue(dest, src) => {
    let is_null = matches!(self.registers.get(*src), RegisterValue::ScalarValue(ScalarValue::Null));
    *self.registers.get_mut(*dest) = RegisterValue::ScalarValue(ScalarValue::Boolean(!is_null));
}
```
Follow the same register access patterns used by existing operations in this file (e.g., `EqualsValue`).

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

**The problem:** For a nested loop join, the inner (right) side must be re-scanned from the beginning for each outer row. But nodes generate their bytecode once — there is no built-in mechanism to "reset" a compiled node. `codegen_scan` emits `MoveCursor(First)` in the **init emitter** (runs once at startup), and its `next` label just advances the cursor.

**The solution — Materialize the right side:** Compile the right child using normal `codegen()` (so it can be *any* plan node — Scan, Filter, subquery, etc.), run it to completion during a materialization phase, and store all its rows in a row buffer. Then for each left row, iterate the buffer. This requires two new VM instructions for non-destructive buffer iteration (`RewindRowBuffer` and `NextFromRowBuffer`), since the existing `YieldFromRowBuffer` is destructive (`pop()`-based).

The left child is also compiled via normal `codegen()`, so both inputs can be arbitrary plan nodes.

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

The existing `ExprContext` only supports a single table (line 951). For joins, create a `JoinExprContext`:

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

**New `convert_expr_join` function** (parallel to existing `convert_expr` at line 959):

This function mirrors `convert_expr` exactly but uses `JoinExprContext` instead of `ExprContext` for column resolution. The structure:
```rust
fn convert_expr_join(expr: &ast::Expression, ctx: &JoinExprContext) -> Result<PlanExpr, PlanError> {
    match expr {
        ast::Expression::Value(scalar) => convert_scalar_join(scalar, ctx),
        ast::Expression::BinaryOp { op, lhs, rhs } => Ok(PlanExpr::BinaryOp {
            op: convert_binary_op(op),
            left: Box::new(convert_expr_join(lhs, ctx)?),
            right: Box::new(convert_expr_join(rhs, ctx)?),
        }),
        ast::Expression::UnaryOp { op, expression } => Ok(PlanExpr::UnaryOp {
            op: convert_unary_op(op),
            operand: Box::new(convert_expr_join(expression, ctx)?),
        }),
        ast::Expression::FunctionCall { name, args } => {
            // Same validation as convert_expr (line 972-996)
            let plan_args: Result<Vec<_>, _> =
                args.iter().map(|arg| convert_expr_join(arg, ctx)).collect();
            Ok(PlanExpr::FunctionCall { name: name.to_uppercase(), args: plan_args? })
        }
    }
}

fn convert_scalar_join(scalar: &ast::ScalarValue, ctx: &JoinExprContext) -> Result<PlanExpr, PlanError> {
    match scalar {
        ast::ScalarValue::IntegerNumber(n) => Ok(PlanExpr::Literal(Literal::Integer(*n))),
        ast::ScalarValue::FloatingNumber(n) => Ok(PlanExpr::Literal(Literal::Float(*n))),
        ast::ScalarValue::StringLiteral(s) => Ok(PlanExpr::Literal(Literal::String(s.clone()))),
        ast::ScalarValue::Null => Ok(PlanExpr::Literal(Literal::Null)),
        ast::ScalarValue::Identifier(name) => {
            // Unqualified column reference
            match ctx.unqualified.get(name) {
                Some(Some(pos)) => Ok(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: *pos })),
                Some(None) => Err(PlanError::AmbiguousColumn(name.clone())),
                None => Err(PlanError::ColumnNotFound { table: "join".to_string(), column: name.clone() }),
            }
        }
        ast::ScalarValue::MultiPartIdentifier(table_expr, column_name) => {
            // Qualified column reference (e.g., e.name)
            let ref_table = extract_identifier(table_expr)?;
            match ctx.qualified.get(&(ref_table.clone(), column_name.clone())) {
                Some(pos) => Ok(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: *pos })),
                None => Err(PlanError::ColumnNotFound { table: ref_table, column: column_name.clone() }),
            }
        }
    }
}
```

**New `plan_select_with_joins` function:**

Called from the `plan()` dispatch function (line 257). Modify the `Statement::Select` arm:
```rust
Statement::Select(select) => {
    if select.joins.is_empty() {
        plan_select(select, btree)
    } else {
        plan_select_with_joins(select, btree)
    }
}
```

Structure of `plan_select_with_joins`:

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
    // For each ColumnExpression:
    //   - Wildcard → expand to ColumnRef(0), ColumnRef(1), ..., ColumnRef(left_col_count + right_col_count - 1)
    //   - Named/Anonymous → convert using convert_expr_join
    let mut project_columns: Vec<PlanExpr> = Vec::new();
    for col_expr in &select.columns {
        match col_expr {
            ast::ColumnExpression::Wildcard => {
                for idx in 0..(left_col_count + right_col_count) {
                    project_columns.push(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: idx }));
                }
            }
            ast::ColumnExpression::Named { expression, .. } => {
                project_columns.push(convert_expr_join(expression, &join_ctx)?);
            }
            ast::ColumnExpression::Anonyomous(expression) => {
                project_columns.push(convert_expr_join(expression, &join_ctx)?);
            }
        }
    }
    plan = LogicalPlan::Project {
        input: Box::new(plan),
        columns: project_columns,
    };

    // 9. ORDER BY (if present) — follow the same pattern as plan_select (lines 508-553)
    // Convert ORDER BY expressions using convert_expr_join (not convert_expr).
    // The column remapping logic (scan_idx_to_proj_idx, extra_order_columns) works the
    // same way — the only difference is using the join context for expression conversion.
    if let Some(ref order_by) = select.order_by {
        // Same logic as plan_select: build sort keys, handle extra order columns
        // Use convert_expr_join(&clause.expression, &join_ctx) instead of convert_expr
        // ... (copy the pattern from plan_select lines 508-553)
    }

    // 10. LIMIT (if present) — same as plan_select (lines 556-563)
    if let Some(ref limit_expr) = select.limit {
        let count = extract_limit_value(limit_expr)?;
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            count,
        };
    }

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

#### 33e. New VM Instructions (`src/engine/program.rs`)

The existing `YieldFromRowBuffer` uses `buffer.pop()` (destructive, line 329 of `src/engine.rs`). For joins we need non-destructive iteration with rewind capability. Add two new instructions:

```rust
/// Reset the row buffer's read cursor to the beginning (for re-iteration).
RewindRowBuffer(Reg),

/// Read the next row from the buffer without removing it.
/// If the read cursor is past the end, jump to target.
/// Otherwise, copy row values into dest_regs and advance the read cursor.
NextFromRowBuffer(Vec<Reg>, Reg, JumpTarget),
```

**Engine implementation:**

The `RowBuffer` register value needs a read cursor. Change from `RowBuffer(Vec<Vec<ScalarValue>>)` to a struct:
```rust
pub struct RowBuffer {
    pub rows: Vec<Vec<ScalarValue>>,
    pub cursor: usize,  // read position for NextFromRowBuffer
}
```

Use the struct approach. In `src/engine/registers.rs`:

1. Add the struct definition:
```rust
#[derive(Clone, Debug)]
pub struct RowBuffer {
    pub rows: Vec<Vec<ScalarValue>>,
    pub cursor: usize,
}
```

2. Change `RegisterValue::RowBuffer(Vec<Vec<ScalarValue>>)` to `RegisterValue::RowBuffer(RowBuffer)`.

3. Update `row_buffer_mut()` to return `Option<&mut RowBuffer>` (currently returns `Option<&mut Vec<Vec<ScalarValue>>>`).

4. In `src/engine.rs`, update existing operations:
   - `InitRowBuffer(reg)`: change `Vec::new()` to `RowBuffer { rows: Vec::new(), cursor: 0 }`
   - `AppendToRowBuffer`: change `buffer.push(row)` to `buffer.rows.push(row)`
   - `SortRowBuffer`: change `buffer.sort_by(...)` to `buffer.rows.sort_by(...)`
   - `YieldFromRowBuffer`: change `buffer.pop()` to `buffer.rows.pop()`

Engine execution for the new instructions:
```rust
RewindRowBuffer(buf_reg) => {
    let buffer = self.registers.get_mut(buf_reg).row_buffer_mut().unwrap();
    buffer.cursor = 0;
}
NextFromRowBuffer(dest_regs, buf_reg, target) => {
    // Borrow checker note: must extract data and drop the buffer borrow
    // BEFORE writing to dest registers. Follow the pattern used by
    // YieldFromRowBuffer (which pops a row, then writes to dest regs).
    let maybe_row = {
        let buffer = self.registers.get_mut(buf_reg).row_buffer_mut().unwrap();
        if buffer.cursor >= buffer.rows.len() {
            None
        } else {
            let row = buffer.rows[buffer.cursor].clone();
            buffer.cursor += 1;
            Some(row)
        }
    }; // buffer borrow dropped here
    match maybe_row {
        None => {
            self.program.set_next_operation_index(target.unwrap_resolved());
        }
        Some(row) => {
            for (dest, value) in dest_regs.iter().zip(row.into_iter()) {
                *self.registers.get_mut(*dest) = RegisterValue::ScalarValue(value);
            }
        }
    }
}
```

#### 33f. Compiler (`src/compiler/nodes.rs`) — The Nested Loop (Materialize Right Side)

**Overview:** The join compiles in two phases within the generated bytecode:
1. **Materialize phase:** Run the right child to completion, storing all rows in a buffer
2. **Join phase:** For each left row, iterate the buffer checking the ON condition

Both children are compiled via normal `codegen()`, so they can be any plan node.

**New `codegen_join` function:**

```rust
pub fn codegen_join(
    left: &LogicalPlan,
    right: &LogicalPlan,
    on_condition: &PlanExpr,
    left_column_count: usize,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // --- Phase 1: Materialize right side into buffer ---

    let buffer_reg = ctx.registers.alloc();
    ctx.init_emitter.emit(Operation::InitRowBuffer(buffer_reg));

    // Compile right child via normal codegen
    // Its init code → init_emitter, body code → body_emitter
    let mat_on_tuple = ctx.body_emitter.create_label();
    let mat_on_done = ctx.body_emitter.create_label();
    let right_cont = NodeContinuation {
        on_tuple: mat_on_tuple,
        on_done: mat_on_done,
    };
    let right_output = codegen(right, &right_cont, ctx);

    // mat_on_tuple: append row to buffer, request next
    ctx.body_emitter.bind_label(mat_on_tuple);
    ctx.body_emitter.emit(Operation::AppendToRowBuffer(
        buffer_reg,
        right_output.output_regs.clone(),
    ));
    ctx.body_emitter.emit_goto(right_output.next);

    // mat_on_done: materialization complete, jump to join loop
    ctx.body_emitter.bind_label(mat_on_done);
    let join_loop_start = ctx.body_emitter.create_label(); // forward ref
    ctx.body_emitter.emit_goto(join_loop_start);

    // --- Phase 2: Nested loop join ---

    // Compile left child via normal codegen
    let left_on_tuple = ctx.body_emitter.create_label();
    let left_cont = NodeContinuation {
        on_tuple: left_on_tuple,
        on_done: cont.on_done,  // left exhausted = join done
    };
    let left_output = codegen(left, &left_cont, ctx);

    // Bind join_loop_start: after materialization, start left iteration
    ctx.body_emitter.bind_label(join_loop_start);
    ctx.body_emitter.emit_goto(left_output.next);

    // Allocate registers for right-side rows read from buffer
    let right_read_regs = ctx.registers.alloc_block(right_output.output_regs.len());

    // Combined output: left columns then right columns
    let mut combined_output = left_output.output_regs.clone();
    combined_output.extend(right_read_regs.clone());

    // left_on_tuple: got a left row, iterate buffer
    ctx.body_emitter.bind_label(left_on_tuple);
    ctx.body_emitter.emit(Operation::RewindRowBuffer(buffer_reg));

    // INNER_CHECK: read next row from buffer
    let inner_check = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_label(inner_check);
    // NextFromRowBuffer jumps to target when buffer is exhausted.
    // Use JumpTarget::Unresolved(label) — same pattern as YieldFromRowBuffer
    // in codegen_sort (see line ~640 of nodes.rs).
    ctx.body_emitter.emit(Operation::NextFromRowBuffer(
        right_read_regs.clone(),
        buffer_reg,
        JumpTarget::Unresolved(left_output.next),  // buffer done → next left row
    ));

    // Evaluate ON condition against combined registers
    let pred_reg = compile_expr(on_condition, &combined_output, &mut ExprContext {
        emitter: &mut ctx.body_emitter,
        registers: &mut ctx.registers,
    });
    ctx.body_emitter.emit_goto_if_false(inner_check, pred_reg); // no match → next buffer row
    ctx.body_emitter.emit_goto(cont.on_tuple);                  // match → emit row

    // JOIN_NEXT: parent calls this to get next matching row
    let join_next = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_label(join_next);
    ctx.body_emitter.emit_goto(inner_check);

    NodeOutput {
        next: join_next,
        output_regs: combined_output,
    }
}
```

**Important — wiring new instructions into existing infrastructure:**

Three places must be updated for each new `Operation` variant:

1. **`src/compiler/emitter.rs` — `finalize_with_offset` (line ~92):** This method has an exhaustive `match` on all `Operation` variants. Add:
   - `Operation::NextFromRowBuffer(_, _, ref mut target)` → resolve jump target (same as `YieldFromRowBuffer` arm at line ~111)
   - `Operation::RewindRowBuffer(_)` → add to the no-jump-target arm (the `| Operation::SortRowBuffer(_, _)` list at line ~148)

2. **`src/engine/program.rs` — `Display` impl:** Add formatting for both new operations (follow existing row buffer display patterns around line ~290).

3. **`src/engine.rs` — execution `match`:** Add the execution logic (already specified in section 33e above).

**Bytecode execution flow:**

```
INIT:
  InitRowBuffer(buffer)
  <right child init (e.g., Open right cursor, MoveCursor First)>
  <left child init (e.g., Open left cursor, MoveCursor First)>

BODY:
  Phase 1 — Materialize right side:
    RIGHT_CHECK:                              ← body starts here (right child's entry)
      <right child body>
      → on_tuple: MAT_ON_TUPLE
      → on_done:  MAT_ON_DONE

    MAT_ON_TUPLE:
      AppendToRowBuffer(buffer, right_regs)
      GoTo(RIGHT_CHECK)                       → get next right row

    MAT_ON_DONE:
      GoTo(JOIN_LOOP_START)                   → materialization complete

  Phase 2 — Nested loop:
    <left child body>
      → on_tuple: LEFT_ON_TUPLE
      → on_done:  cont.on_done               → join is done

    JOIN_LOOP_START:
      GoTo(LEFT_CHECK)                        → start getting left rows

    LEFT_ON_TUPLE:
      RewindRowBuffer(buffer)                 → reset buffer to start

    INNER_CHECK:
      NextFromRowBuffer(right_regs, buffer, LEFT_CHECK)  → read or jump if done
      <evaluate ON condition>
      GoToIfFalse(INNER_CHECK, pred_reg)      → no match, next buffer row
      GoTo(cont.on_tuple)                     → match! emit combined row

    JOIN_NEXT:                                → parent calls here for next row
      GoTo(INNER_CHECK)                       → continue buffer iteration
```

**Why this is modular:**
- Both `left` and `right` children are compiled via `codegen()` — they can be Scan, Filter(Scan), or any other plan node
- The right side's rows are materialized into a buffer that can be iterated multiple times
- No changes to `NodeOutput`, `NodeContinuation`, or any existing node codegen function
- Future: could optimize when right child is a simple Scan by using cursor reset instead of materialization (but the buffer approach works universally)

**Wire into `codegen()` dispatch** (around line 1170):
```rust
LogicalPlan::Join { left, right, on_condition, left_column_count } => {
    codegen_join(left, right, on_condition, *left_column_count, cont, ctx)
}
```

#### 33g. compile_expr for JOIN

The existing `compile_expr` in `src/compiler/expr.rs` takes `input_regs: &[Reg]` and maps `ColumnRef::Single { column_idx }` to `input_regs[column_idx]`. Since the join's `combined_output` is `left_regs ++ right_read_regs` and the planner produces column indices into this combined layout, `compile_expr` works without modification.

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
