use crate::engine::program::{self, JumpTarget, Label, MoveOperation, Operation, Reg};
use crate::engine::scalarvalue::ScalarValue;
use crate::planner::{Literal, LogicalPlan, PlanExpr};

use super::{compile_expr, BytecodeEmitter, ExprContext, RegisterAllocator};

/// Convert a planner Literal to an engine ScalarValue.
fn literal_to_scalar(lit: &Literal) -> ScalarValue {
    match lit {
        Literal::Integer(i) => ScalarValue::Integer(*i),
        Literal::Float(f) => ScalarValue::Floating(*f),
        Literal::String(s) => ScalarValue::String(s.clone()),
        Literal::Bool(b) => ScalarValue::Boolean(*b),
        Literal::Null => ScalarValue::Null,
    }
}

/// Codegen context with two-emitter pattern as per the plan.
/// Init code and body code are kept separate, then combined at finalization.
pub struct CodegenContext {
    /// Collects all initialization code (cursor opens, counter inits, etc.)
    pub init_emitter: BytecodeEmitter,
    /// Collects all body/loop code
    pub body_emitter: BytecodeEmitter,
    /// Register allocator shared across all nodes
    pub registers: RegisterAllocator,
}

impl CodegenContext {
    pub fn new() -> Self {
        CodegenContext {
            init_emitter: BytecodeEmitter::new(),
            body_emitter: BytecodeEmitter::new(),
            registers: RegisterAllocator::new(),
        }
    }

    /// Finalize and combine init + body code.
    /// Layout: init_code + GoTo(body_start) + body_code
    pub fn finalize(self) -> Vec<Operation> {
        let init_ops = self.init_emitter.finalize();
        let body_ops = self.body_emitter.finalize();

        let mut result = Vec::with_capacity(init_ops.len() + 1 + body_ops.len());

        // Add init code
        result.extend(init_ops);

        // Add jump to body start (which is right after this jump)
        let body_start = result.len() + 1;
        result.push(Operation::GoTo(JumpTarget::addr(body_start)));

        // Add body code, adjusting all jump targets by the offset
        let offset = result.len();
        for op in body_ops {
            result.push(adjust_jump_targets(op, offset));
        }

        result
    }
}

/// Adjust jump targets in an operation by adding an offset.
fn adjust_jump_targets(op: Operation, offset: usize) -> Operation {
    match op {
        Operation::GoTo(JumpTarget::Resolved(addr)) => {
            Operation::GoTo(JumpTarget::Resolved(addr + offset))
        }
        Operation::GoToIfFalse(JumpTarget::Resolved(addr), reg) => {
            Operation::GoToIfFalse(JumpTarget::Resolved(addr + offset), reg)
        }
        Operation::GoToIfEqualValue(JumpTarget::Resolved(addr), lhs, rhs) => {
            Operation::GoToIfEqualValue(JumpTarget::Resolved(addr + offset), lhs, rhs)
        }
        Operation::PopKey(dest, list, JumpTarget::Resolved(addr)) => {
            Operation::PopKey(dest, list, JumpTarget::Resolved(addr + offset))
        }
        Operation::YieldFromRowBuffer(regs, buffer, JumpTarget::Resolved(addr)) => {
            Operation::YieldFromRowBuffer(regs, buffer, JumpTarget::Resolved(addr + offset))
        }
        Operation::YieldFromGroupTable(regs, table, JumpTarget::Resolved(addr)) => {
            Operation::YieldFromGroupTable(regs, table, JumpTarget::Resolved(addr + offset))
        }
        // Unresolved labels should have been resolved by finalize()
        Operation::GoTo(JumpTarget::Unresolved(_))
        | Operation::GoToIfFalse(JumpTarget::Unresolved(_), _)
        | Operation::GoToIfEqualValue(JumpTarget::Unresolved(_), _, _)
        | Operation::PopKey(_, _, JumpTarget::Unresolved(_))
        | Operation::YieldFromRowBuffer(_, _, JumpTarget::Unresolved(_))
        | Operation::YieldFromGroupTable(_, _, JumpTarget::Unresolved(_)) => {
            panic!("Unresolved jump target after finalize")
        }
        // All other operations pass through unchanged
        other => other,
    }
}

/// Continuation labels that a node needs to know where to jump
pub struct NodeContinuation {
    /// Label to jump to when a tuple is ready
    pub on_tuple: Label,
    /// Label to jump to when no more tuples (exhausted)
    pub on_done: Label,
}

/// Output from a node's code generation
pub struct NodeOutput {
    /// Label to jump to to request the next tuple
    pub next: Label,
    /// Registers containing the current tuple's column values
    pub output_regs: Vec<Reg>,
}

/// Generate bytecode for a Scan node.
///
/// The scan pattern is:
/// ```text
/// INIT (init_emitter):
///   Open(cursor, table)
///   MoveCursor(cursor, First)
///
/// BODY (body_emitter, next_label = CHECK):
///   CHECK:   CanReadCursor(flag, cursor); GoToIfFalse(on_done, flag)
///   READ:    ReadCursor(output_regs, cursor)
///   ADVANCE: MoveCursor(cursor, Next)
///   EMIT:    GoTo(on_tuple)
/// ```
pub fn codegen_scan(
    rootpage: u32,
    columns: &[usize],
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // Allocate registers for cursor, flag, and output columns
    let cursor_reg = ctx.registers.alloc();
    let flag_reg = ctx.registers.alloc();

    // Compute num_read: need to read up to max(columns) + 1 values
    let num_read = columns.iter().max().map(|&m| m + 1).unwrap_or(0);
    let all_regs = ctx.registers.alloc_block(num_read);

    // Map output_regs to only the needed columns
    let output_regs: Vec<Reg> = columns.iter().map(|&i| all_regs[i]).collect();

    // INIT (init_emitter): Open cursor and move to first row
    ctx.init_emitter.emit(Operation::Open(cursor_reg, rootpage));
    ctx.init_emitter
        .emit(Operation::MoveCursor(cursor_reg, MoveOperation::First));

    // BODY (body_emitter):
    // CHECK: Label for iteration entry point
    let check_label = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_label(check_label);
    ctx.body_emitter
        .emit(Operation::CanReadCursor(flag_reg, cursor_reg));
    ctx.body_emitter.emit_goto_if_false(cont.on_done, flag_reg);

    // READ: Read current row into all registers (num_read values)
    ctx.body_emitter
        .emit(Operation::ReadCursor(all_regs.clone(), cursor_reg));

    // ADVANCE: Move cursor to next row (makes next row "pending")
    ctx.body_emitter
        .emit(Operation::MoveCursor(cursor_reg, MoveOperation::Next));

    // EMIT: Jump to tuple handler
    ctx.body_emitter.emit_goto(cont.on_tuple);

    NodeOutput {
        next: check_label,
        output_regs,
    }
}

/// Generate bytecode for a Count node.
///
/// Count consumes all rows from its child and outputs a single row
/// containing the count.
///
/// ```text
/// INIT (init_emitter):
///   counter = 0
///   <child init>
///
/// BODY (body_emitter):
///   <child body with our handlers>
///   child_on_tuple: IncrementValue(counter); GoTo(child.next)
///   child_on_done:  GoTo(on_tuple)  // count is ready
///   count_next:     GoTo(on_done)   // after yielding once, we're done
/// ```
pub fn codegen_count(
    input: &LogicalPlan,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // Allocate counter register
    let counter_reg = ctx.registers.alloc();

    // INIT: initialize counter to 0
    ctx.init_emitter
        .emit(Operation::StoreValue(counter_reg, ScalarValue::Integer(0)));

    // Create labels for child's continuations
    let child_on_tuple = ctx.body_emitter.create_label();
    let child_on_done = ctx.body_emitter.create_label();
    let child_cont = NodeContinuation {
        on_tuple: child_on_tuple,
        on_done: child_on_done,
    };

    // Compile child
    let child_output = codegen(input, &child_cont, ctx);

    // child_on_tuple: increment counter, get next from child
    ctx.body_emitter.bind_label(child_on_tuple);
    ctx.body_emitter
        .emit(Operation::IncrementValue(counter_reg));
    ctx.body_emitter.emit_goto(child_output.next);

    // child_on_done: count is ready, signal our on_tuple
    ctx.body_emitter.bind_label(child_on_done);
    ctx.body_emitter.emit_goto(cont.on_tuple);

    // count_next: after yielding once, we're done
    let count_next = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_label(count_next);
    ctx.body_emitter.emit_goto(cont.on_done);

    NodeOutput {
        next: count_next,
        output_regs: vec![counter_reg],
    }
}

/// Generate bytecode for a Values node.
///
/// Values emits a fixed set of rows (useful for testing and VALUES clauses).
///
/// ```text
/// INIT (init_emitter):
///   index = 0
///   num_rows = N
///   (store index constants for dispatch)
///
/// BODY (body_emitter):
///   CHECK:    LessThan(flag, index, num_rows); GoToIfFalse(on_done, flag)
///   DISPATCH: GoToIfEqual(ROW_i, index, i) for each row
///   ROW_0:    store row 0 values; goto EMIT
///   ROW_1:    store row 1 values; goto EMIT
///   ...
///   EMIT:     index++; goto on_tuple
/// ```
pub fn codegen_values(
    rows: &[Vec<Literal>],
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    let num_rows = rows.len();
    let num_cols = if num_rows > 0 { rows[0].len() } else { 0 };

    // Handle empty values - just go to done immediately
    if num_rows == 0 {
        let check_label = ctx.body_emitter.create_label();
        ctx.body_emitter.bind_label(check_label);
        ctx.body_emitter.emit_goto(cont.on_done);
        return NodeOutput {
            next: check_label,
            output_regs: vec![],
        };
    }

    // Allocate output registers
    let output_regs = ctx.registers.alloc_block(num_cols);

    // Allocate index counter and num_rows constant
    let index_reg = ctx.registers.alloc();
    let num_rows_reg = ctx.registers.alloc();
    let cmp_reg = ctx.registers.alloc();

    // INIT: index = 0, num_rows = N
    ctx.init_emitter
        .emit(Operation::StoreValue(index_reg, ScalarValue::Integer(0)));
    ctx.init_emitter.emit(Operation::StoreValue(
        num_rows_reg,
        ScalarValue::Integer(num_rows as i64),
    ));

    // Allocate constant registers for each row index (for dispatch comparison)
    let index_constants: Vec<Reg> = (0..num_rows)
        .map(|i| {
            let reg = ctx.registers.alloc();
            ctx.init_emitter
                .emit(Operation::StoreValue(reg, ScalarValue::Integer(i as i64)));
            reg
        })
        .collect();

    // Create labels for each row and for emit
    let row_labels: Vec<Label> = (0..num_rows)
        .map(|_| ctx.body_emitter.create_label())
        .collect();
    let emit_label = ctx.body_emitter.create_label();

    // BODY:
    // CHECK: if index >= num_rows, goto on_done
    let check_label = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_label(check_label);
    ctx.body_emitter
        .emit(Operation::LessThanValue(cmp_reg, index_reg, num_rows_reg));
    ctx.body_emitter.emit_goto_if_false(cont.on_done, cmp_reg);

    // DISPATCH: for each row, check if index == i and jump to that row
    for (i, row_label) in row_labels.iter().enumerate() {
        ctx.body_emitter
            .emit_goto_if_equal(*row_label, index_reg, index_constants[i]);
    }

    // Fallthrough safety: shouldn't reach here, but go to done
    ctx.body_emitter.emit_goto(cont.on_done);

    // Emit each row's code
    for (i, row) in rows.iter().enumerate() {
        ctx.body_emitter.bind_label(row_labels[i]);
        for (j, lit) in row.iter().enumerate() {
            let sv = literal_to_scalar(lit);
            ctx.body_emitter
                .emit(Operation::StoreValue(output_regs[j], sv.clone()));
        }
        ctx.body_emitter.emit_goto(emit_label);
    }

    // EMIT: increment index, goto on_tuple
    ctx.body_emitter.bind_label(emit_label);
    ctx.body_emitter.emit(Operation::IncrementValue(index_reg));
    ctx.body_emitter.emit_goto(cont.on_tuple);

    NodeOutput {
        next: check_label,
        output_regs,
    }
}

/// Generate bytecode for a Sequence node.
///
/// Sequence generates integers from start to end-1 (exclusive upper bound).
///
/// ```text
/// INIT (init_emitter):
///   value = start
///   end_val = end
///
/// BODY (body_emitter):
///   CHECK: LessThan(flag, value, end_val); GoToIfFalse(on_done, flag)
///   EMIT:  CopyValue(output, value); IncrementValue(value); GoTo(on_tuple)
/// ```
pub fn codegen_sequence(
    start: i64,
    end: i64,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // Allocate registers
    let value_reg = ctx.registers.alloc();
    let end_reg = ctx.registers.alloc();
    let flag_reg = ctx.registers.alloc();
    let output_reg = ctx.registers.alloc();

    // INIT: initialize value and end
    ctx.init_emitter.emit(Operation::StoreValue(
        value_reg,
        ScalarValue::Integer(start),
    ));
    ctx.init_emitter
        .emit(Operation::StoreValue(end_reg, ScalarValue::Integer(end)));

    // BODY:
    // CHECK: if value >= end, goto on_done
    let check_label = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_label(check_label);
    ctx.body_emitter
        .emit(Operation::LessThanValue(flag_reg, value_reg, end_reg));
    ctx.body_emitter.emit_goto_if_false(cont.on_done, flag_reg);

    // EMIT: copy value to output, increment, goto on_tuple
    ctx.body_emitter
        .emit(Operation::CopyValue(output_reg, value_reg));
    ctx.body_emitter.emit(Operation::IncrementValue(value_reg));
    ctx.body_emitter.emit_goto(cont.on_tuple);

    NodeOutput {
        next: check_label,
        output_regs: vec![output_reg],
    }
}

/// Generate bytecode for a Filter node.
///
/// Filter is a pass-through node that evaluates a predicate for each input tuple.
/// Tuples where the predicate is false are skipped.
///
/// ```text
/// // Child's on_tuple wired to FILTER_CHECK
/// // Child's on_done wired to parent's on_done (propagate)
///
/// BODY (body_emitter):
///   FILTER_CHECK: <compile predicate into pred_reg>
///                 GoToIfFalse(child.next_label, pred_reg)  // reject → get next
///                 GoTo(on_tuple)  // accept → emit
///
/// next_label = child.next_label  // delegate to child
/// output_regs = child.output_regs  // pass through
/// ```
pub fn codegen_filter(
    predicate: &PlanExpr,
    input: &LogicalPlan,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // Create label for our filter check
    let filter_check = ctx.body_emitter.create_label();

    // Child's on_tuple wired to FILTER_CHECK
    // Child's on_done wired to parent's on_done (propagate exhaustion)
    let child_cont = NodeContinuation {
        on_tuple: filter_check,
        on_done: cont.on_done, // propagate directly
    };

    // Compile child first
    let child_output = codegen(input, &child_cont, ctx);

    // FILTER_CHECK: compile predicate and check
    ctx.body_emitter.bind_label(filter_check);

    // Compile the predicate expression
    let pred_reg = {
        let mut expr_ctx = ExprContext {
            emitter: &mut ctx.body_emitter,
            registers: &mut ctx.registers,
        };
        compile_expr(predicate, &child_output.output_regs, &mut expr_ctx)
    };

    // If predicate is false, get next from child (reject)
    ctx.body_emitter
        .emit_goto_if_false(child_output.next, pred_reg);

    // If predicate is true (fall through), emit the tuple
    ctx.body_emitter.emit_goto(cont.on_tuple);

    // Return: delegate to child for next, pass through output registers
    NodeOutput {
        next: child_output.next,
        output_regs: child_output.output_regs,
    }
}

/// Generate bytecode for a Project node.
///
/// Project transforms input tuples by computing new expressions.
/// Each output column is the result of evaluating a PlanExpr.
///
/// ```text
/// // Child's on_tuple wired to PROJECT_COMPUTE
/// // Child's on_done wired to parent's on_done (propagate)
///
/// BODY (body_emitter):
///   PROJECT_COMPUTE: for each expr: compile_expr into output_regs[i]
///                    GoTo(on_tuple)
///
/// next_label = child.next_label  // delegate to child
/// output_regs = newly allocated registers
/// ```
pub fn codegen_project(
    columns: &[PlanExpr],
    input: &LogicalPlan,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // Create label for project computation
    let project_compute = ctx.body_emitter.create_label();

    // Child's on_tuple wired to PROJECT_COMPUTE
    // Child's on_done wired to parent's on_done (propagate exhaustion)
    let child_cont = NodeContinuation {
        on_tuple: project_compute,
        on_done: cont.on_done, // propagate directly
    };

    // Compile child first
    let child_output = codegen(input, &child_cont, ctx);

    // PROJECT_COMPUTE: compute each projection expression
    ctx.body_emitter.bind_label(project_compute);

    // Compile each expression into new output registers
    let output_regs: Vec<Reg> = columns
        .iter()
        .map(|expr| {
            let mut expr_ctx = ExprContext {
                emitter: &mut ctx.body_emitter,
                registers: &mut ctx.registers,
            };
            compile_expr(expr, &child_output.output_regs, &mut expr_ctx)
        })
        .collect();

    // Emit the transformed tuple
    ctx.body_emitter.emit_goto(cont.on_tuple);

    // Return: delegate to child for next, but with new output registers
    NodeOutput {
        next: child_output.next,
        output_regs,
    }
}

/// Generate bytecode for a Limit node.
///
/// Limit restricts the number of rows emitted to at most `count`.
/// It has init code to set up the counter.
///
/// ```text
/// INIT (init_emitter):
///   StoreValue(counter, count)
///   StoreValue(zero, 0)
///
/// // Child's on_tuple wired to LIMIT_CHECK
/// // Child's on_done wired to parent's on_done (propagate)
///
/// BODY (body_emitter):
///   LIMIT_CHECK: GoToIfEqualValue(on_done, counter, zero)  // exhausted limit
///                DecrementValue(counter)
///                GoTo(on_tuple)
///
/// next_label = child.next_label  // delegate to child
/// output_regs = child.output_regs  // pass through
/// ```
pub fn codegen_limit(
    count: u64,
    input: &LogicalPlan,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // Allocate counter and zero constant
    let counter_reg = ctx.registers.alloc();
    let zero_reg = ctx.registers.alloc();

    // INIT: initialize counter to count, zero to 0
    ctx.init_emitter.emit(Operation::StoreValue(
        counter_reg,
        ScalarValue::Integer(count as i64),
    ));
    ctx.init_emitter
        .emit(Operation::StoreValue(zero_reg, ScalarValue::Integer(0)));

    // Create label for limit check
    let limit_check = ctx.body_emitter.create_label();

    // Child's on_tuple wired to LIMIT_CHECK
    // Child's on_done wired to parent's on_done (propagate exhaustion)
    let child_cont = NodeContinuation {
        on_tuple: limit_check,
        on_done: cont.on_done, // propagate directly
    };

    // Compile child first
    let child_output = codegen(input, &child_cont, ctx);

    // LIMIT_CHECK: check if counter == 0
    ctx.body_emitter.bind_label(limit_check);
    ctx.body_emitter
        .emit_goto_if_equal(cont.on_done, counter_reg, zero_reg);

    // Decrement counter
    ctx.body_emitter
        .emit(Operation::DecrementValue(counter_reg));

    // Emit the tuple
    ctx.body_emitter.emit_goto(cont.on_tuple);

    // Return: delegate to child for next, pass through output registers
    NodeOutput {
        next: child_output.next,
        output_regs: child_output.output_regs,
    }
}

/// Generate bytecode for a Sort node.
///
/// Sort materializes all rows from its child, sorts them based on sort keys,
/// then yields them in sorted order.
///
/// ```text
/// INIT (init_emitter):
///   InitRowBuffer(buffer)
///
/// BODY (body_emitter):
///   ... child code ...
///   collect_row:
///     AppendToRowBuffer(buffer, child_output_regs)
///     GoTo(child.next)
///   sort_and_yield:
///     SortRowBuffer(buffer, sort_keys)
///   yield_loop:
///     YieldFromRowBuffer(child_output_regs, buffer, on_done)
///     GoTo(on_tuple)
///   sort_next:
///     GoTo(yield_loop)
/// ```
pub fn codegen_sort(
    sort_keys: &[crate::planner::SortKey],
    input: &LogicalPlan,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // Allocate register for row buffer
    let buffer_reg = ctx.registers.alloc();

    // INIT: initialize empty buffer
    ctx.init_emitter.emit(Operation::InitRowBuffer(buffer_reg));

    // Create labels
    let collect_row = ctx.body_emitter.create_label();
    let sort_and_yield = ctx.body_emitter.create_label();
    let yield_loop = ctx.body_emitter.create_label();

    // Child's on_tuple → collect_row
    // Child's on_done → sort_and_yield
    let child_cont = NodeContinuation {
        on_tuple: collect_row,
        on_done: sort_and_yield,
    };

    // Compile child
    let child_output = codegen(input, &child_cont, ctx);

    // collect_row: append row to buffer and continue
    ctx.body_emitter.bind_label(collect_row);
    ctx.body_emitter.emit(Operation::AppendToRowBuffer(
        buffer_reg,
        child_output.output_regs.clone(),
    ));
    ctx.body_emitter.emit_goto(child_output.next);

    // sort_and_yield: sort the buffer, then fall through to yield loop
    ctx.body_emitter.bind_label(sort_and_yield);

    // Convert PlanExpr sort keys to column-index-based sort keys
    // For now, we assume sort expressions are simple column references
    let sort_key_specs: Vec<program::SortKeySpec> = sort_keys
        .iter()
        .map(|key| {
            // Extract column index from PlanExpr
            let column_idx = match &key.expr {
                crate::planner::PlanExpr::ColumnRef(crate::planner::ColumnRef::Single {
                    column_idx,
                }) => *column_idx,
                _ => panic!("ORDER BY only supports column references for now"),
            };
            program::SortKeySpec {
                column_index: column_idx,
                descending: key.descending,
            }
        })
        .collect();

    ctx.body_emitter
        .emit(Operation::SortRowBuffer(buffer_reg, sort_key_specs));

    // yield_loop: pop rows from buffer and yield
    ctx.body_emitter.bind_label(yield_loop);
    ctx.body_emitter.emit(Operation::YieldFromRowBuffer(
        child_output.output_regs.clone(),
        buffer_reg,
        JumpTarget::Unresolved(cont.on_done),
    ));
    // If YieldFromRowBuffer succeeds (didn't jump to on_done), emit tuple
    ctx.body_emitter.emit_goto(cont.on_tuple);

    // sort_next: after parent processes tuple, yield next row
    let sort_next = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_label(sort_next);
    ctx.body_emitter.emit_goto(yield_loop);

    NodeOutput {
        next: sort_next,
        output_regs: child_output.output_regs,
    }
}

/// Generate bytecode for an Aggregate node (GROUP BY with aggregates).
///
/// Aggregate collects all rows from child, groups them by group_keys,
/// computes aggregates for each group, then yields the results.
///
/// ```text
/// INIT:
///   InitGroupTable(table)
///
/// BODY:
///   // Child emits rows → update_group
///   update_group:
///     Evaluate group_keys into key_regs
///     UpdateGroup(table, key_regs, agg_specs)
///     GoTo(child.next)
///
///   // Child done → yield_from_groups
///   yield_from_groups:
///     YieldFromGroupTable(output_regs, table, on_done)
///     GoTo(on_tuple)
///   agg_next:
///     GoTo(yield_from_groups)
/// ```
pub fn codegen_aggregate(
    group_keys: &[crate::planner::PlanExpr],
    aggregates: &[crate::planner::AggregateExpr],
    input: &LogicalPlan,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    use crate::engine::program::AggregateOp;
    use crate::planner::AggregateFunction;

    // Allocate register for group table
    let table_reg = ctx.registers.alloc();

    // INIT: initialize empty group table
    ctx.init_emitter.emit(Operation::InitGroupTable(table_reg));

    // Create labels
    let update_group = ctx.body_emitter.create_label();
    let yield_from_groups = ctx.body_emitter.create_label();

    // Child's on_tuple → update_group
    // Child's on_done → yield_from_groups
    let child_cont = NodeContinuation {
        on_tuple: update_group,
        on_done: yield_from_groups,
    };

    // Compile child
    let child_output = codegen(input, &child_cont, ctx);

    // update_group: evaluate group keys, update group, continue
    ctx.body_emitter.bind_label(update_group);

    // Evaluate group key expressions into registers
    let key_regs: Vec<Reg> = group_keys
        .iter()
        .map(|expr| {
            let mut expr_ctx = ExprContext {
                emitter: &mut ctx.body_emitter,
                registers: &mut ctx.registers,
            };
            compile_expr(expr, &child_output.output_regs, &mut expr_ctx)
        })
        .collect();

    // Build aggregate specs
    let agg_specs: Vec<program::AggregateSpec> = aggregates
        .iter()
        .map(|agg| {
            let input_reg = agg.argument.as_ref().map(|expr| {
                let mut expr_ctx = ExprContext {
                    emitter: &mut ctx.body_emitter,
                    registers: &mut ctx.registers,
                };
                compile_expr(expr, &child_output.output_regs, &mut expr_ctx)
            });

            let op = match agg.function {
                AggregateFunction::Count => AggregateOp::Count,
                AggregateFunction::Sum => AggregateOp::Sum,
                AggregateFunction::Avg => AggregateOp::Avg,
                AggregateFunction::Min => AggregateOp::Min,
                AggregateFunction::Max => AggregateOp::Max,
            };

            program::AggregateSpec { op, input_reg }
        })
        .collect();

    ctx.body_emitter.emit(Operation::UpdateGroup(
        table_reg,
        key_regs.clone(),
        agg_specs,
    ));
    ctx.body_emitter.emit_goto(child_output.next);

    // yield_from_groups: pop groups and yield
    ctx.body_emitter.bind_label(yield_from_groups);

    // Allocate output registers: group_keys + aggregates
    let num_outputs = group_keys.len() + aggregates.len();
    let output_regs: Vec<Reg> = (0..num_outputs).map(|_| ctx.registers.alloc()).collect();

    ctx.body_emitter.emit(Operation::YieldFromGroupTable(
        output_regs.clone(),
        table_reg,
        JumpTarget::Unresolved(cont.on_done),
    ));
    // If YieldFromGroupTable succeeds, emit tuple
    ctx.body_emitter.emit_goto(cont.on_tuple);

    // agg_next: after parent processes tuple, yield next group
    let agg_next = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_label(agg_next);
    ctx.body_emitter.emit_goto(yield_from_groups);

    NodeOutput {
        next: agg_next,
        output_regs,
    }
}

/// Generate bytecode for an Insert node.
///
/// Insert consumes all rows from its child (typically Values), writes each
/// to the B-tree, and outputs a single row containing the count of rows inserted.
/// This follows the same double-emitter pattern as codegen_count.
///
/// ```text
/// INIT (init_emitter):
///   Open(cursor, rootpage)
///   MoveCursor(cursor, Last)
///   CanReadCursor(flag, cursor)
///   GoToIfFalse(@empty, flag)
///   ReadKey(key, cursor)
///   IncrementValue(key)            // next key = max + 1
///   GoTo(@init_done)
///   @empty: StoreValue(key, 1)     // empty table starts at 1
///   @init_done:
///   StoreValue(counter, 0)
///
/// BODY (body_emitter):
///   <child codegen with our handlers>
///   child_on_tuple: WriteCursor(cursor, key, child.output_regs)
///                   IncrementValue(key)
///                   IncrementValue(counter)
///                   GoTo(child.next)
///   child_on_done:  GoTo(on_tuple)     // yield the count
///   insert_next:    GoTo(on_done)      // done after yielding
/// ```
pub fn codegen_insert(
    rootpage: u32,
    table_columns: &[usize],
    input: &LogicalPlan,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // Allocate registers
    let cursor_reg = ctx.registers.alloc();
    let flag_reg = ctx.registers.alloc();
    let key_reg = ctx.registers.alloc();
    let counter_reg = ctx.registers.alloc();

    // INIT: Open cursor and discover next key
    ctx.init_emitter.emit(Operation::Open(cursor_reg, rootpage));
    ctx.init_emitter
        .emit(Operation::MoveCursor(cursor_reg, MoveOperation::Last));
    ctx.init_emitter
        .emit(Operation::CanReadCursor(flag_reg, cursor_reg));

    // Branch: if table is empty, go to @empty
    let empty_label = ctx.init_emitter.create_label();
    let init_done_label = ctx.init_emitter.create_label();

    ctx.init_emitter.emit_goto_if_false(empty_label, flag_reg);

    // Non-empty: read max key, increment
    ctx.init_emitter
        .emit(Operation::ReadKey(key_reg, cursor_reg));
    ctx.init_emitter.emit(Operation::IncrementValue(key_reg));
    ctx.init_emitter.emit_goto(init_done_label);

    // @empty: start at key 1
    ctx.init_emitter.bind_label(empty_label);
    ctx.init_emitter
        .emit(Operation::StoreValue(key_reg, ScalarValue::Integer(1)));

    // @init_done: init counter
    ctx.init_emitter.bind_label(init_done_label);
    ctx.init_emitter
        .emit(Operation::StoreValue(counter_reg, ScalarValue::Integer(0)));

    // Create labels for child's continuations
    let child_on_tuple = ctx.body_emitter.create_label();
    let child_on_done = ctx.body_emitter.create_label();
    let child_cont = NodeContinuation {
        on_tuple: child_on_tuple,
        on_done: child_on_done,
    };

    // Compile child
    let child_output = codegen(input, &child_cont, ctx);

    // child_on_tuple: write row, increment key and counter, get next
    ctx.body_emitter.bind_label(child_on_tuple);

    // Reorder child output registers to match table column order
    // If table has columns [id, name, age] and user wrote INSERT(age,id,name)
    // then table_columns=[2,0,1] and child outputs are in [age,id,name] order
    // We need to reorder them to [id,name,age] order for writing
    let reordered_regs: Vec<Reg> = if table_columns.iter().enumerate().all(|(i, &col)| i == col) {
        // Fast path: columns are already in order, no reordering needed
        child_output.output_regs.clone()
    } else {
        // Need to reorder: allocate new registers and copy values
        let num_table_columns = table_columns.iter().max().map(|&m| m + 1).unwrap_or(0);
        let reordered = ctx.registers.alloc_block(num_table_columns);

        // Copy each value to its correct position
        for (i, &col_idx) in table_columns.iter().enumerate() {
            ctx.body_emitter.emit(Operation::CopyValue(
                reordered[col_idx],
                child_output.output_regs[i],
            ));
        }

        reordered
    };
    ctx.body_emitter
        .emit(Operation::WriteCursor(cursor_reg, key_reg, reordered_regs));
    ctx.body_emitter.emit(Operation::IncrementValue(key_reg));
    ctx.body_emitter
        .emit(Operation::IncrementValue(counter_reg));
    ctx.body_emitter.emit_goto(child_output.next);

    // child_on_done: all rows consumed, yield the count
    ctx.body_emitter.bind_label(child_on_done);
    ctx.body_emitter.emit_goto(cont.on_tuple);

    // insert_next: after yielding count, we're done
    let insert_next = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_label(insert_next);
    ctx.body_emitter.emit_goto(cont.on_done);

    NodeOutput {
        next: insert_next,
        output_regs: vec![counter_reg],
    }
}

/// Generate bytecode for an Update node.
///
/// Update scans a table, evaluates assignments for matching rows, and rewrites them.
/// Uses the same key for reinsert (which overwrites existing value).
/// Returns row count of updated rows.
#[allow(clippy::too_many_arguments)]
/// Generate bytecode for an Update node.
///
/// Update uses a two-phase collect-then-mutate pattern:
/// Phase 1: Scan the table and collect keys of matching rows
/// Phase 2: Iterate the collected keys, re-read each row, compute new values, and update
///
/// This avoids cursor invalidation issues when updating during iteration.
/// Returns row count of updated rows.
pub fn codegen_update(
    rootpage: u32,
    table_columns: &[usize],
    assignments: &[(usize, PlanExpr)],
    filter: &Option<PlanExpr>,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    let cursor_reg = ctx.registers.alloc();
    let key_list_reg = ctx.registers.alloc();
    let key_reg = ctx.registers.alloc();
    let counter_reg = ctx.registers.alloc();
    let flag_reg = ctx.registers.alloc();

    // Allocate registers for reading all table columns
    let read_regs = ctx.registers.alloc_block(table_columns.len());

    // INIT: open cursor, position to first, init key list and counter
    ctx.init_emitter.emit(Operation::Open(cursor_reg, rootpage));
    ctx.init_emitter
        .emit(Operation::MoveCursor(cursor_reg, MoveOperation::First));
    ctx.init_emitter.emit(Operation::InitKeyList(key_list_reg));
    ctx.init_emitter
        .emit(Operation::StoreValue(counter_reg, ScalarValue::Integer(0)));

    // PHASE 1: Collect keys
    let collect_start = ctx.body_emitter.create_label();
    let phase2_start = ctx.body_emitter.create_label();

    ctx.body_emitter.bind_label(collect_start);
    ctx.body_emitter
        .emit(Operation::CanReadCursor(flag_reg, cursor_reg));
    ctx.body_emitter.emit_goto_if_false(phase2_start, flag_reg);

    // Read all columns (needed for filter evaluation)
    ctx.body_emitter
        .emit(Operation::ReadCursor(read_regs.clone(), cursor_reg));

    // Evaluate filter if present
    if let Some(filter_expr) = filter {
        let filter_reg = {
            let mut expr_ctx = ExprContext {
                emitter: &mut ctx.body_emitter,
                registers: &mut ctx.registers,
            };
            compile_expr(filter_expr, &read_regs, &mut expr_ctx)
        };
        let skip_label = ctx.body_emitter.create_label();
        ctx.body_emitter.emit_goto_if_false(skip_label, filter_reg);

        // Filter matched: collect this key
        ctx.body_emitter
            .emit(Operation::ReadKey(key_reg, cursor_reg));
        ctx.body_emitter
            .emit(Operation::AppendKey(key_list_reg, key_reg));
        ctx.body_emitter
            .emit(Operation::IncrementValue(counter_reg));

        ctx.body_emitter.bind_label(skip_label);
    } else {
        // No filter: collect all keys
        ctx.body_emitter
            .emit(Operation::ReadKey(key_reg, cursor_reg));
        ctx.body_emitter
            .emit(Operation::AppendKey(key_list_reg, key_reg));
        ctx.body_emitter
            .emit(Operation::IncrementValue(counter_reg));
    }

    // Advance to next row
    ctx.body_emitter
        .emit(Operation::MoveCursor(cursor_reg, MoveOperation::Next));
    ctx.body_emitter.emit_goto(collect_start);

    // PHASE 2: Update collected keys
    let update_loop = ctx.body_emitter.create_label();
    let update_done = ctx.body_emitter.create_label();

    ctx.body_emitter.bind_label(phase2_start);
    ctx.body_emitter.bind_label(update_loop);

    // Pop next key, or jump to done if list is empty
    ctx.body_emitter
        .emit_pop_key(key_reg, key_list_reg, update_done);

    // Seek to this key and re-read row values
    ctx.body_emitter.emit(Operation::MoveCursor(
        cursor_reg,
        MoveOperation::Find(key_reg),
    ));
    ctx.body_emitter
        .emit(Operation::ReadCursor(read_regs.clone(), cursor_reg));

    // Compute new values from assignments
    let new_values = read_regs.clone();
    for (col_idx, expr) in assignments {
        let value_reg = {
            let mut expr_ctx = ExprContext {
                emitter: &mut ctx.body_emitter,
                registers: &mut ctx.registers,
            };
            compile_expr(expr, &read_regs, &mut expr_ctx)
        };
        ctx.body_emitter
            .emit(Operation::CopyValue(new_values[*col_idx], value_reg));
    }

    // Write updated row
    ctx.body_emitter
        .emit(Operation::WriteCursor(cursor_reg, key_reg, new_values));
    ctx.body_emitter.emit_goto(update_loop);

    // Done: yield count
    ctx.body_emitter.bind_label(update_done);
    ctx.body_emitter.emit_goto(cont.on_tuple);

    // After yielding, halt
    let after_yield = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_label(after_yield);
    ctx.body_emitter.emit_goto(cont.on_done);

    NodeOutput {
        next: after_yield,
        output_regs: vec![counter_reg],
    }
}

/// Generate bytecode for a Delete node.
///
/// Delete uses a two-phase collect-then-mutate pattern:
/// Phase 1: Scan the table and collect keys of matching rows
/// Phase 2: Iterate the collected keys and delete each one
///
/// This avoids cursor invalidation issues when deleting during iteration.
/// Returns row count of deleted rows.
pub fn codegen_delete(
    rootpage: u32,
    table_columns: &[usize],
    filter: &Option<PlanExpr>,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    let cursor_reg = ctx.registers.alloc();
    let key_list_reg = ctx.registers.alloc();
    let key_reg = ctx.registers.alloc();
    let counter_reg = ctx.registers.alloc();
    let flag_reg = ctx.registers.alloc();

    // Allocate registers for reading all table columns (needed for filter evaluation)
    let read_regs = ctx.registers.alloc_block(table_columns.len());

    // INIT: open cursor, position to first, init key list and counter
    ctx.init_emitter.emit(Operation::Open(cursor_reg, rootpage));
    ctx.init_emitter
        .emit(Operation::MoveCursor(cursor_reg, MoveOperation::First));
    ctx.init_emitter.emit(Operation::InitKeyList(key_list_reg));
    ctx.init_emitter
        .emit(Operation::StoreValue(counter_reg, ScalarValue::Integer(0)));

    // PHASE 1: Collect keys
    let collect_start = ctx.body_emitter.create_label();
    let phase2_start = ctx.body_emitter.create_label();

    ctx.body_emitter.bind_label(collect_start);
    ctx.body_emitter
        .emit(Operation::CanReadCursor(flag_reg, cursor_reg));
    ctx.body_emitter.emit_goto_if_false(phase2_start, flag_reg);

    // Read all columns (needed for filter evaluation)
    ctx.body_emitter
        .emit(Operation::ReadCursor(read_regs.clone(), cursor_reg));

    // Evaluate filter if present
    if let Some(filter_expr) = filter {
        let filter_reg = {
            let mut expr_ctx = ExprContext {
                emitter: &mut ctx.body_emitter,
                registers: &mut ctx.registers,
            };
            compile_expr(filter_expr, &read_regs, &mut expr_ctx)
        };
        let skip_label = ctx.body_emitter.create_label();
        ctx.body_emitter.emit_goto_if_false(skip_label, filter_reg);

        // Filter matched: collect this key
        ctx.body_emitter
            .emit(Operation::ReadKey(key_reg, cursor_reg));
        ctx.body_emitter
            .emit(Operation::AppendKey(key_list_reg, key_reg));
        ctx.body_emitter
            .emit(Operation::IncrementValue(counter_reg));

        ctx.body_emitter.bind_label(skip_label);
    } else {
        // No filter: collect all keys
        ctx.body_emitter
            .emit(Operation::ReadKey(key_reg, cursor_reg));
        ctx.body_emitter
            .emit(Operation::AppendKey(key_list_reg, key_reg));
        ctx.body_emitter
            .emit(Operation::IncrementValue(counter_reg));
    }

    // Advance to next row
    ctx.body_emitter
        .emit(Operation::MoveCursor(cursor_reg, MoveOperation::Next));
    ctx.body_emitter.emit_goto(collect_start);

    // PHASE 2: Delete collected keys
    let delete_loop = ctx.body_emitter.create_label();
    let delete_done = ctx.body_emitter.create_label();

    ctx.body_emitter.bind_label(phase2_start);
    ctx.body_emitter.bind_label(delete_loop);

    // Pop next key, or jump to done if list is empty
    ctx.body_emitter
        .emit_pop_key(key_reg, key_list_reg, delete_done);

    // Seek to this key and delete
    ctx.body_emitter.emit(Operation::MoveCursor(
        cursor_reg,
        MoveOperation::Find(key_reg),
    ));
    ctx.body_emitter.emit(Operation::DeleteCursor(cursor_reg));
    ctx.body_emitter.emit_goto(delete_loop);

    // Done: yield count
    ctx.body_emitter.bind_label(delete_done);
    ctx.body_emitter.emit_goto(cont.on_tuple);

    // After yielding, halt
    let after_yield = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_label(after_yield);
    ctx.body_emitter.emit_goto(cont.on_done);

    NodeOutput {
        next: after_yield,
        output_regs: vec![counter_reg],
    }
}

/// Main codegen dispatch function.
/// Routes to the appropriate codegen based on plan type.
pub fn codegen(
    plan: &LogicalPlan,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    match plan {
        LogicalPlan::Scan { rootpage, columns } => codegen_scan(*rootpage, columns, cont, ctx),
        LogicalPlan::Count { input } => codegen_count(input, cont, ctx),
        LogicalPlan::Values { rows } => codegen_values(rows, cont, ctx),
        LogicalPlan::Filter { predicate, input } => codegen_filter(predicate, input, cont, ctx),
        LogicalPlan::Project { columns, input } => codegen_project(columns, input, cont, ctx),
        LogicalPlan::Sequence { start, end } => codegen_sequence(*start, *end, cont, ctx),
        LogicalPlan::Limit { count, input } => codegen_limit(*count, input, cont, ctx),
        LogicalPlan::Sort { sort_keys, input } => codegen_sort(sort_keys, input, cont, ctx),
        LogicalPlan::Aggregate {
            input,
            group_keys,
            aggregates,
        } => codegen_aggregate(group_keys, aggregates, input, cont, ctx),
        LogicalPlan::Insert {
            rootpage,
            table_columns,
            input,
        } => codegen_insert(*rootpage, table_columns, input, cont, ctx),
        LogicalPlan::Update {
            rootpage,
            table_columns,
            assignments,
            filter,
        } => codegen_update(*rootpage, table_columns, assignments, filter, cont, ctx),
        LogicalPlan::Delete {
            rootpage,
            table_columns,
            filter,
        } => codegen_delete(*rootpage, table_columns, filter, cont, ctx),
    }
}

/// Compile a plan and add root-level handlers (yield on tuple, halt on done).
/// Returns the finalized bytecode and register count.
pub fn compile_plan(plan: &LogicalPlan) -> (Vec<Operation>, usize) {
    let mut ctx = CodegenContext::new();

    // Create root continuation labels
    let on_tuple = ctx.body_emitter.create_label();
    let on_done = ctx.body_emitter.create_label();
    let cont = NodeContinuation { on_tuple, on_done };

    // Compile the plan
    let output = codegen(plan, &cont, &mut ctx);

    // on_tuple: yield the output registers, then get next
    ctx.body_emitter.bind_label(on_tuple);
    ctx.body_emitter
        .emit(Operation::Yield(output.output_regs.clone()));
    ctx.body_emitter.emit_goto(output.next);

    // on_done: halt
    ctx.body_emitter.bind_label(on_done);
    ctx.body_emitter.emit(Operation::Halt);

    let num_registers = ctx.registers.count();
    let ops = ctx.finalize();

    (ops, num_registers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::scalarvalue::ScalarValue;
    use crate::engine::Engine;
    use crate::planner::{BinaryOp, ColumnRef, PlanExpr};
    use crate::test::TestDb;

    /// Test that codegen_scan produces correct bytecode structure
    #[test]
    fn test_codegen_scan_structure() {
        let mut ctx = CodegenContext::new();

        // Create continuation labels (in body_emitter since that's where they're used)
        let on_tuple = ctx.body_emitter.create_label();
        let on_done = ctx.body_emitter.create_label();
        let cont = NodeContinuation { on_tuple, on_done };

        let output = codegen_scan(42, &[0, 1], &cont, &mut ctx);

        // Check that we got 2 output registers
        assert_eq!(output.output_regs.len(), 2);

        // Verify register allocation: cursor, flag, 2 output columns = 4 total
        assert_eq!(ctx.registers.count(), 4);
    }

    /// Integration test: Count(Scan) - verify row counting works
    #[test]
    fn test_count_scan() {
        // Create test database with 3 rows
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor = btree.open(root);
            let mut c = cursor.open_readwrite();
            c.insert(0, b"[1, 100]".to_vec());
            c.insert(1, b"[2, 200]".to_vec());
            c.insert(2, b"[3, 300]".to_vec());
        }

        // Build plan: Count { Scan { rootpage, 2 columns } }
        let plan = LogicalPlan::Count {
            input: Box::new(LogicalPlan::Scan {
                rootpage: root,
                columns: vec![0, 1],
            }),
        };

        let (ops, num_registers) = compile_plan(&plan);

        // Run through engine
        let mut engine = Engine::with_program(&ops, num_registers, btree);
        let yields = engine.run();

        // Count should yield single row with value 3
        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(3));
    }

    /// Test Count with empty table
    #[test]
    fn test_count_empty_table() {
        // Create test database with empty table
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let plan = LogicalPlan::Count {
            input: Box::new(LogicalPlan::Scan {
                rootpage: root,
                columns: vec![0],
            }),
        };

        let (ops, num_registers) = compile_plan(&plan);

        // Run through engine
        let mut engine = Engine::with_program(&ops, num_registers, btree);
        let yields = engine.run();

        // Count should yield 0 for empty table
        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(0));
    }

    /// Test that scan actually reads the correct values
    #[test]
    fn test_scan_reads_values() {
        // Create test database with data
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        {
            let mut cursor = btree.open(root);
            let mut c = cursor.open_readwrite();
            c.insert(0, b"[10, 20]".to_vec());
            c.insert(1, b"[30, 40]".to_vec());
        }

        let plan = LogicalPlan::Scan {
            rootpage: root,
            columns: vec![0, 1],
        };

        let (ops, num_registers) = compile_plan(&plan);

        // Run through engine
        let mut engine = Engine::with_program(&ops, num_registers, btree);
        let yields = engine.run();

        // Should have 2 rows
        assert_eq!(yields.len(), 2);
        // First row: [10, 20]
        assert_eq!(yields[0][0], ScalarValue::Integer(10));
        assert_eq!(yields[0][1], ScalarValue::Integer(20));
        // Second row: [30, 40]
        assert_eq!(yields[1][0], ScalarValue::Integer(30));
        assert_eq!(yields[1][1], ScalarValue::Integer(40));
    }

    // ========================================================================
    // Values tests (no btree needed!)
    // ========================================================================

    /// Test Values emits all rows
    #[test]
    fn test_values_basic() {
        let plan = LogicalPlan::Values {
            rows: vec![
                vec![Literal::Integer(1), Literal::Integer(10)],
                vec![Literal::Integer(2), Literal::Integer(20)],
                vec![Literal::Integer(3), Literal::Integer(30)],
            ],
        };

        let (ops, num_registers) = compile_plan(&plan);

        // Values doesn't need a btree, but Engine::with_program requires one
        let test = TestDb::default();
        let btree = test.btree;

        let mut engine = Engine::with_program(&ops, num_registers, btree);
        let yields = engine.run();

        assert_eq!(yields.len(), 3);
        assert_eq!(
            yields[0],
            vec![ScalarValue::Integer(1), ScalarValue::Integer(10)]
        );
        assert_eq!(
            yields[1],
            vec![ScalarValue::Integer(2), ScalarValue::Integer(20)]
        );
        assert_eq!(
            yields[2],
            vec![ScalarValue::Integer(3), ScalarValue::Integer(30)]
        );
    }

    /// Test Values with empty rows
    #[test]
    fn test_values_empty() {
        let plan = LogicalPlan::Values { rows: vec![] };

        let (ops, num_registers) = compile_plan(&plan);

        let test = TestDb::default();
        let btree = test.btree;

        let mut engine = Engine::with_program(&ops, num_registers, btree);
        let yields = engine.run();

        assert_eq!(yields.len(), 0);
    }

    /// Test Count(Values) - count without btree
    #[test]
    fn test_count_values() {
        let plan = LogicalPlan::Count {
            input: Box::new(LogicalPlan::Values {
                rows: vec![
                    vec![Literal::Integer(1)],
                    vec![Literal::Integer(2)],
                    vec![Literal::Integer(3)],
                    vec![Literal::Integer(4)],
                    vec![Literal::Integer(5)],
                ],
            }),
        };

        let (ops, num_registers) = compile_plan(&plan);

        let test = TestDb::default();
        let btree = test.btree;

        let mut engine = Engine::with_program(&ops, num_registers, btree);
        let yields = engine.run();

        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(5));
    }

    /// Test Values with different literal types
    #[test]
    fn test_values_mixed_types() {
        let plan = LogicalPlan::Values {
            rows: vec![vec![
                Literal::Integer(42),
                Literal::Float(3.14),
                Literal::Bool(true),
                Literal::String("hello".to_string()),
            ]],
        };

        let (ops, num_registers) = compile_plan(&plan);

        let test = TestDb::default();
        let btree = test.btree;

        let mut engine = Engine::with_program(&ops, num_registers, btree);
        let yields = engine.run();

        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(42));
        assert_eq!(yields[0][1], ScalarValue::Floating(3.14));
        assert_eq!(yields[0][2], ScalarValue::Boolean(true));
        assert_eq!(yields[0][3], ScalarValue::String("hello".to_string()));
    }

    // ========================================================================
    // Sequence tests
    // ========================================================================

    /// Test Sequence generates correct range
    #[test]
    fn test_sequence_basic() {
        let plan = LogicalPlan::Sequence { start: 1, end: 4 };

        let (ops, num_registers) = compile_plan(&plan);

        let test = TestDb::default();
        let btree = test.btree;

        let mut engine = Engine::with_program(&ops, num_registers, btree);
        let yields = engine.run();

        assert_eq!(yields.len(), 3);
        assert_eq!(yields[0][0], ScalarValue::Integer(1));
        assert_eq!(yields[1][0], ScalarValue::Integer(2));
        assert_eq!(yields[2][0], ScalarValue::Integer(3));
    }

    /// Test empty Sequence (start == end)
    #[test]
    fn test_sequence_empty() {
        let plan = LogicalPlan::Sequence { start: 5, end: 5 };

        let (ops, num_registers) = compile_plan(&plan);

        let test = TestDb::default();
        let btree = test.btree;

        let mut engine = Engine::with_program(&ops, num_registers, btree);
        let yields = engine.run();

        assert_eq!(yields.len(), 0);
    }

    /// Test Count(Sequence)
    #[test]
    fn test_count_sequence() {
        let plan = LogicalPlan::Count {
            input: Box::new(LogicalPlan::Sequence { start: 0, end: 100 }),
        };

        let (ops, num_registers) = compile_plan(&plan);

        let test = TestDb::default();
        let btree = test.btree;

        let mut engine = Engine::with_program(&ops, num_registers, btree);
        let yields = engine.run();

        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(100));
    }

    // ========================================================================
    // Filter tests (using Sequence for cleaner tests)
    // ========================================================================

    /// Helper to create a filter on col[0] with a binary op
    fn filter_col0(op: BinaryOp, value: i64, input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Filter {
            predicate: PlanExpr::BinaryOp {
                op,
                left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })),
                right: Box::new(PlanExpr::Literal(Literal::Integer(value))),
            },
            input: Box::new(input),
        }
    }

    /// Helper to run a plan and return yields
    fn run_plan(plan: &LogicalPlan) -> Vec<Vec<ScalarValue>> {
        let (ops, num_registers) = compile_plan(plan);
        let test = TestDb::default();
        let btree = test.btree;
        let mut engine = Engine::with_program(&ops, num_registers, btree);
        engine.run()
    }

    /// Test Filter with equality predicate
    #[test]
    fn test_filter_equality() {
        // Filter col[0] == 5 from Sequence(1..10)
        let plan = filter_col0(
            BinaryOp::Equals,
            5,
            LogicalPlan::Sequence { start: 1, end: 10 },
        );

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(5));
    }

    /// Test Filter with greater-than predicate
    #[test]
    fn test_filter_greater_than() {
        // Filter col[0] > 7 from Sequence(1..10) -> [8, 9]
        let plan = filter_col0(
            BinaryOp::GreaterThan,
            7,
            LogicalPlan::Sequence { start: 1, end: 10 },
        );

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 2);
        assert_eq!(yields[0][0], ScalarValue::Integer(8));
        assert_eq!(yields[1][0], ScalarValue::Integer(9));
    }

    /// Test Filter that rejects all rows
    #[test]
    fn test_filter_rejects_all() {
        // Filter col[0] > 100 from Sequence(1..10) -> []
        let plan = filter_col0(
            BinaryOp::GreaterThan,
            100,
            LogicalPlan::Sequence { start: 1, end: 10 },
        );

        let yields = run_plan(&plan);
        assert_eq!(yields.len(), 0);
    }

    /// Test Filter that accepts all rows
    #[test]
    fn test_filter_accepts_all() {
        // Filter col[0] > 0 from Sequence(1..4) -> [1, 2, 3]
        let plan = filter_col0(
            BinaryOp::GreaterThan,
            0,
            LogicalPlan::Sequence { start: 1, end: 4 },
        );

        let yields = run_plan(&plan);
        assert_eq!(yields.len(), 3);
    }

    /// Test Filter with multi-column rows (using Values since Sequence is single-column)
    #[test]
    fn test_filter_multi_column() {
        // Filter on second column: col[1] == 20
        let plan = LogicalPlan::Filter {
            predicate: PlanExpr::BinaryOp {
                op: BinaryOp::Equals,
                left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 })),
                right: Box::new(PlanExpr::Literal(Literal::Integer(20))),
            },
            input: Box::new(LogicalPlan::Values {
                rows: vec![
                    vec![Literal::Integer(1), Literal::Integer(10)],
                    vec![Literal::Integer(2), Literal::Integer(20)],
                    vec![Literal::Integer(3), Literal::Integer(30)],
                ],
            }),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(2));
        assert_eq!(yields[0][1], ScalarValue::Integer(20));
    }

    /// Test Count(Filter(Sequence))
    #[test]
    fn test_count_filter_sequence() {
        // Count { Filter { col[0] > 50, Sequence(1..100) } } -> 49 values (51..99)
        let plan = LogicalPlan::Count {
            input: Box::new(filter_col0(
                BinaryOp::GreaterThan,
                50,
                LogicalPlan::Sequence { start: 1, end: 100 },
            )),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(49)); // 51..99 = 49 values
    }

    /// Test Filter with AND predicate
    #[test]
    fn test_filter_and_predicate() {
        // Filter col[0] > 3 AND col[0] < 7 from Sequence(1..10) -> [4, 5, 6]
        let plan = LogicalPlan::Filter {
            predicate: PlanExpr::BinaryOp {
                op: BinaryOp::And,
                left: Box::new(PlanExpr::BinaryOp {
                    op: BinaryOp::GreaterThan,
                    left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })),
                    right: Box::new(PlanExpr::Literal(Literal::Integer(3))),
                }),
                right: Box::new(PlanExpr::BinaryOp {
                    op: BinaryOp::LessThan,
                    left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })),
                    right: Box::new(PlanExpr::Literal(Literal::Integer(7))),
                }),
            },
            input: Box::new(LogicalPlan::Sequence { start: 1, end: 10 }),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 3);
        assert_eq!(yields[0][0], ScalarValue::Integer(4));
        assert_eq!(yields[1][0], ScalarValue::Integer(5));
        assert_eq!(yields[2][0], ScalarValue::Integer(6));
    }

    // ========================================================================
    // Project tests
    // ========================================================================

    /// Test Project that passes through a single column
    #[test]
    fn test_project_passthrough() {
        // Project [col[0]] from Sequence(1..4) -> [1], [2], [3]
        let plan = LogicalPlan::Project {
            columns: vec![PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })],
            input: Box::new(LogicalPlan::Sequence { start: 1, end: 4 }),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 3);
        assert_eq!(yields[0][0], ScalarValue::Integer(1));
        assert_eq!(yields[1][0], ScalarValue::Integer(2));
        assert_eq!(yields[2][0], ScalarValue::Integer(3));
    }

    /// Test Project with computed expression (col + 10)
    #[test]
    fn test_project_computed() {
        // Project [col[0] + 10] from Sequence(1..4) -> [11], [12], [13]
        let plan = LogicalPlan::Project {
            columns: vec![PlanExpr::BinaryOp {
                op: BinaryOp::Add,
                left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })),
                right: Box::new(PlanExpr::Literal(Literal::Integer(10))),
            }],
            input: Box::new(LogicalPlan::Sequence { start: 1, end: 4 }),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 3);
        assert_eq!(yields[0][0], ScalarValue::Integer(11));
        assert_eq!(yields[1][0], ScalarValue::Integer(12));
        assert_eq!(yields[2][0], ScalarValue::Integer(13));
    }

    /// Test Project with multiple columns (col, col * 2)
    #[test]
    fn test_project_multiple_columns() {
        // Project [col[0], col[0] * 2] from Sequence(1..4) -> [1,2], [2,4], [3,6]
        let plan = LogicalPlan::Project {
            columns: vec![
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 }),
                PlanExpr::BinaryOp {
                    op: BinaryOp::Multiply,
                    left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })),
                    right: Box::new(PlanExpr::Literal(Literal::Integer(2))),
                },
            ],
            input: Box::new(LogicalPlan::Sequence { start: 1, end: 4 }),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 3);
        assert_eq!(
            yields[0],
            vec![ScalarValue::Integer(1), ScalarValue::Integer(2)]
        );
        assert_eq!(
            yields[1],
            vec![ScalarValue::Integer(2), ScalarValue::Integer(4)]
        );
        assert_eq!(
            yields[2],
            vec![ScalarValue::Integer(3), ScalarValue::Integer(6)]
        );
    }

    /// Test Project with literal only (constant column)
    #[test]
    fn test_project_constant() {
        // Project [42] from Sequence(1..4) -> [42], [42], [42]
        let plan = LogicalPlan::Project {
            columns: vec![PlanExpr::Literal(Literal::Integer(42))],
            input: Box::new(LogicalPlan::Sequence { start: 1, end: 4 }),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 3);
        assert_eq!(yields[0][0], ScalarValue::Integer(42));
        assert_eq!(yields[1][0], ScalarValue::Integer(42));
        assert_eq!(yields[2][0], ScalarValue::Integer(42));
    }

    /// Test Project with column reordering from multi-column input
    #[test]
    fn test_project_reorder() {
        // Project [col[1], col[0]] from Values [[1, 10], [2, 20]] -> [[10, 1], [20, 2]]
        let plan = LogicalPlan::Project {
            columns: vec![
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 }),
                PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 }),
            ],
            input: Box::new(LogicalPlan::Values {
                rows: vec![
                    vec![Literal::Integer(1), Literal::Integer(10)],
                    vec![Literal::Integer(2), Literal::Integer(20)],
                ],
            }),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 2);
        assert_eq!(
            yields[0],
            vec![ScalarValue::Integer(10), ScalarValue::Integer(1)]
        );
        assert_eq!(
            yields[1],
            vec![ScalarValue::Integer(20), ScalarValue::Integer(2)]
        );
    }

    /// Test Filter(Project(...)) - filter on projected output
    #[test]
    fn test_filter_project() {
        // Filter [col[0] > 5] from Project [col[0] * 2] from Sequence(1..5)
        // Sequence: 1,2,3,4 -> Project: 2,4,6,8 -> Filter >5: 6,8
        let plan = LogicalPlan::Filter {
            predicate: PlanExpr::BinaryOp {
                op: BinaryOp::GreaterThan,
                left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })),
                right: Box::new(PlanExpr::Literal(Literal::Integer(5))),
            },
            input: Box::new(LogicalPlan::Project {
                columns: vec![PlanExpr::BinaryOp {
                    op: BinaryOp::Multiply,
                    left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })),
                    right: Box::new(PlanExpr::Literal(Literal::Integer(2))),
                }],
                input: Box::new(LogicalPlan::Sequence { start: 1, end: 5 }),
            }),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 2);
        assert_eq!(yields[0][0], ScalarValue::Integer(6));
        assert_eq!(yields[1][0], ScalarValue::Integer(8));
    }

    /// Test Project(Filter(...)) - project from filtered input
    #[test]
    fn test_project_filter() {
        // Project [col[0] * 10] from Filter [col[0] > 2] from Sequence(1..5)
        // Sequence: 1,2,3,4 -> Filter >2: 3,4 -> Project: 30,40
        let plan = LogicalPlan::Project {
            columns: vec![PlanExpr::BinaryOp {
                op: BinaryOp::Multiply,
                left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })),
                right: Box::new(PlanExpr::Literal(Literal::Integer(10))),
            }],
            input: Box::new(filter_col0(
                BinaryOp::GreaterThan,
                2,
                LogicalPlan::Sequence { start: 1, end: 5 },
            )),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 2);
        assert_eq!(yields[0][0], ScalarValue::Integer(30));
        assert_eq!(yields[1][0], ScalarValue::Integer(40));
    }

    /// Test Count(Project(...))
    #[test]
    fn test_count_project() {
        // Count from Project [col[0]] from Sequence(1..10) -> 9
        let plan = LogicalPlan::Count {
            input: Box::new(LogicalPlan::Project {
                columns: vec![PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })],
                input: Box::new(LogicalPlan::Sequence { start: 1, end: 10 }),
            }),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(9));
    }

    // ========================================================================
    // Limit tests
    // ========================================================================

    /// Test Limit returns correct number of rows
    #[test]
    fn test_limit_basic() {
        // Limit 3 from Sequence(1..10) -> [1, 2, 3]
        let plan = LogicalPlan::Limit {
            count: 3,
            input: Box::new(LogicalPlan::Sequence { start: 1, end: 10 }),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 3);
        assert_eq!(yields[0][0], ScalarValue::Integer(1));
        assert_eq!(yields[1][0], ScalarValue::Integer(2));
        assert_eq!(yields[2][0], ScalarValue::Integer(3));
    }

    /// Test Limit 0 returns no rows
    #[test]
    fn test_limit_zero() {
        // Limit 0 from Sequence(1..10) -> []
        let plan = LogicalPlan::Limit {
            count: 0,
            input: Box::new(LogicalPlan::Sequence { start: 1, end: 10 }),
        };

        let yields = run_plan(&plan);
        assert_eq!(yields.len(), 0);
    }

    /// Test Limit greater than input returns all rows
    #[test]
    fn test_limit_exceeds_input() {
        // Limit 100 from Sequence(1..4) -> [1, 2, 3]
        let plan = LogicalPlan::Limit {
            count: 100,
            input: Box::new(LogicalPlan::Sequence { start: 1, end: 4 }),
        };

        let yields = run_plan(&plan);
        assert_eq!(yields.len(), 3);
    }

    /// Test Limit 1 (edge case)
    #[test]
    fn test_limit_one() {
        // Limit 1 from Sequence(1..10) -> [1]
        let plan = LogicalPlan::Limit {
            count: 1,
            input: Box::new(LogicalPlan::Sequence { start: 1, end: 10 }),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(1));
    }

    /// Test Limit(Filter(...)) - limit from filtered input
    #[test]
    fn test_limit_filter() {
        // Limit 2 from Filter [col[0] > 5] from Sequence(1..10)
        // Sequence: 1..9 -> Filter >5: 6,7,8,9 -> Limit 2: 6,7
        let plan = LogicalPlan::Limit {
            count: 2,
            input: Box::new(filter_col0(
                BinaryOp::GreaterThan,
                5,
                LogicalPlan::Sequence { start: 1, end: 10 },
            )),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 2);
        assert_eq!(yields[0][0], ScalarValue::Integer(6));
        assert_eq!(yields[1][0], ScalarValue::Integer(7));
    }

    /// Test Filter(Limit(...)) - filter from limited input
    #[test]
    fn test_filter_limit() {
        // Filter [col[0] > 2] from Limit 5 from Sequence(1..10)
        // Sequence: 1..9 -> Limit 5: 1,2,3,4,5 -> Filter >2: 3,4,5
        let plan = LogicalPlan::Filter {
            predicate: PlanExpr::BinaryOp {
                op: BinaryOp::GreaterThan,
                left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })),
                right: Box::new(PlanExpr::Literal(Literal::Integer(2))),
            },
            input: Box::new(LogicalPlan::Limit {
                count: 5,
                input: Box::new(LogicalPlan::Sequence { start: 1, end: 10 }),
            }),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 3);
        assert_eq!(yields[0][0], ScalarValue::Integer(3));
        assert_eq!(yields[1][0], ScalarValue::Integer(4));
        assert_eq!(yields[2][0], ScalarValue::Integer(5));
    }

    /// Test Limit(Project(...)) - limit from projected input
    #[test]
    fn test_limit_project() {
        // Limit 2 from Project [col[0] * 10] from Sequence(1..10)
        // Sequence: 1..9 -> Project: 10,20,30,... -> Limit 2: 10,20
        let plan = LogicalPlan::Limit {
            count: 2,
            input: Box::new(LogicalPlan::Project {
                columns: vec![PlanExpr::BinaryOp {
                    op: BinaryOp::Multiply,
                    left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })),
                    right: Box::new(PlanExpr::Literal(Literal::Integer(10))),
                }],
                input: Box::new(LogicalPlan::Sequence { start: 1, end: 10 }),
            }),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 2);
        assert_eq!(yields[0][0], ScalarValue::Integer(10));
        assert_eq!(yields[1][0], ScalarValue::Integer(20));
    }

    /// Test Count(Limit(...))
    #[test]
    fn test_count_limit() {
        // Count from Limit 5 from Sequence(1..100) -> 5
        let plan = LogicalPlan::Count {
            input: Box::new(LogicalPlan::Limit {
                count: 5,
                input: Box::new(LogicalPlan::Sequence { start: 1, end: 100 }),
            }),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(5));
    }

    // ========================================================================
    // Insert tests
    // ========================================================================

    /// Test Insert with Values input yields row count
    #[test]
    fn test_insert_yields_count() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let plan = LogicalPlan::Insert {
            rootpage: root,
            table_columns: vec![0, 1, 2],
            input: Box::new(LogicalPlan::Values {
                rows: vec![
                    vec![
                        Literal::Integer(1),
                        Literal::String("alice".to_string()),
                        Literal::Integer(30),
                    ],
                    vec![
                        Literal::Integer(2),
                        Literal::String("bob".to_string()),
                        Literal::Integer(25),
                    ],
                ],
            }),
        };

        let (ops, num_registers) = compile_plan(&plan);

        let mut engine = Engine::with_program(&ops, num_registers, btree);
        let yields = engine.run();

        // Should yield a single row with count = 2
        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(2));
    }

    /// Test Insert then scan to verify data was written
    #[test]
    fn test_insert_then_scan() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        // First: INSERT
        let insert_plan = LogicalPlan::Insert {
            rootpage: root,
            table_columns: vec![0, 1],
            input: Box::new(LogicalPlan::Values {
                rows: vec![
                    vec![Literal::Integer(100), Literal::String("alice".to_string())],
                    vec![Literal::Integer(200), Literal::String("bob".to_string())],
                    vec![
                        Literal::Integer(300),
                        Literal::String("charlie".to_string()),
                    ],
                ],
            }),
        };

        let (ops, num_registers) = compile_plan(&insert_plan);
        let mut engine = Engine::with_program(&ops, num_registers, btree);
        let insert_yields = engine.run();
        assert_eq!(insert_yields[0][0], ScalarValue::Integer(3));

        // Get btree back from engine
        let btree = engine.take_btree().unwrap();

        // Second: SCAN to read back
        let scan_plan = LogicalPlan::Scan {
            rootpage: root,
            columns: vec![0, 1],
        };

        let (ops, num_registers) = compile_plan(&scan_plan);
        let mut engine = Engine::with_program(&ops, num_registers, btree);
        let scan_yields = engine.run();

        assert_eq!(scan_yields.len(), 3);
        assert_eq!(scan_yields[0][0], ScalarValue::Integer(100));
        assert_eq!(scan_yields[0][1], ScalarValue::String("alice".to_string()));
        assert_eq!(scan_yields[1][0], ScalarValue::Integer(200));
        assert_eq!(scan_yields[1][1], ScalarValue::String("bob".to_string()));
        assert_eq!(scan_yields[2][0], ScalarValue::Integer(300));
        assert_eq!(
            scan_yields[2][1],
            ScalarValue::String("charlie".to_string())
        );
    }

    /// Test Insert into empty table
    #[test]
    fn test_insert_into_empty_table() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let root = btree.create_tree();

        let plan = LogicalPlan::Insert {
            rootpage: root,
            table_columns: vec![0],
            input: Box::new(LogicalPlan::Values {
                rows: vec![vec![Literal::Integer(42)]],
            }),
        };

        let (ops, num_registers) = compile_plan(&plan);
        let mut engine = Engine::with_program(&ops, num_registers, btree);
        let yields = engine.run();

        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(1));
    }

    /// Test complex combination: Limit(Project(Filter(Sequence)))
    #[test]
    fn test_limit_project_filter_sequence() {
        // Limit 2 from Project [col[0] * 10] from Filter [col[0] > 5] from Sequence(1..20)
        // Sequence: 1..19 -> Filter >5: 6,7,8,... -> Project: 60,70,80,... -> Limit 2: 60,70
        let plan = LogicalPlan::Limit {
            count: 2,
            input: Box::new(LogicalPlan::Project {
                columns: vec![PlanExpr::BinaryOp {
                    op: BinaryOp::Multiply,
                    left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })),
                    right: Box::new(PlanExpr::Literal(Literal::Integer(10))),
                }],
                input: Box::new(filter_col0(
                    BinaryOp::GreaterThan,
                    5,
                    LogicalPlan::Sequence { start: 1, end: 20 },
                )),
            }),
        };

        let yields = run_plan(&plan);

        assert_eq!(yields.len(), 2);
        assert_eq!(yields[0][0], ScalarValue::Integer(60));
        assert_eq!(yields[1][0], ScalarValue::Integer(70));
    }

    /// Test DELETE bytecode generation (print only, don't execute)
    #[test]
    fn test_codegen_delete_bytecode_structure() {
        let test = TestDb::default();
        let _btree = test.btree;
        let root = 42; // Dummy root page

        // Test DELETE with no filter (delete all)
        let plan = LogicalPlan::Delete {
            rootpage: root,
            table_columns: vec![0],
            filter: None,
        };

        let (ops, _num_registers) = compile_plan(&plan);

        // Print bytecode for inspection
        println!("\nDELETE bytecode:");
        for (i, op) in ops.iter().enumerate() {
            println!("{:3}: {}", i, op);
        }

        // Just verify we got some operations
        assert!(ops.len() > 10, "Expected reasonable bytecode length");
    }
}
