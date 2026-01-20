use crate::engine::program::{JumpTarget, Label, MoveOperation, Operation, Reg};
use crate::engine::scalarvalue::ScalarValue;
use crate::planner::{Literal, LogicalPlan};

use super::{BytecodeEmitter, RegisterAllocator};

/// Convert a planner Literal to an engine ScalarValue.
fn literal_to_scalar(lit: &Literal) -> ScalarValue {
    match lit {
        Literal::Integer(i) => ScalarValue::Integer(*i),
        Literal::Float(f) => ScalarValue::Floating(*f),
        Literal::String(s) => ScalarValue::String(s.clone()),
        Literal::Bool(b) => ScalarValue::Boolean(*b),
        Literal::Null => {
            // TODO: Add proper NULL support to ScalarValue
            panic!("NULL literals not yet supported")
        }
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
        // Unresolved labels should have been resolved by finalize()
        Operation::GoTo(JumpTarget::Unresolved(_))
        | Operation::GoToIfFalse(JumpTarget::Unresolved(_), _)
        | Operation::GoToIfEqualValue(JumpTarget::Unresolved(_), _, _) => {
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
    table: &str,
    num_columns: usize,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // Allocate registers for cursor, flag, and output columns
    let cursor_reg = ctx.registers.alloc();
    let flag_reg = ctx.registers.alloc();
    let output_regs = ctx.registers.alloc_block(num_columns);

    // INIT (init_emitter): Open cursor and move to first row
    ctx.init_emitter
        .emit(Operation::Open(cursor_reg, table.to_string()));
    ctx.init_emitter
        .emit(Operation::MoveCursor(cursor_reg, MoveOperation::First));

    // BODY (body_emitter):
    // CHECK: Label for iteration entry point
    let check_label = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_label(check_label);
    ctx.body_emitter
        .emit(Operation::CanReadCursor(flag_reg, cursor_reg));
    ctx.body_emitter.emit_goto_if_false(cont.on_done, flag_reg);

    // READ: Read current row into output registers
    ctx.body_emitter
        .emit(Operation::ReadCursor(output_regs.clone(), cursor_reg));

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
    ctx.body_emitter.emit(Operation::IncrementValue(counter_reg));
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
    ctx.init_emitter
        .emit(Operation::StoreValue(num_rows_reg, ScalarValue::Integer(num_rows as i64)));

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

/// Main codegen dispatch function.
/// Routes to the appropriate codegen based on plan type.
pub fn codegen(plan: &LogicalPlan, cont: &NodeContinuation, ctx: &mut CodegenContext) -> NodeOutput {
    match plan {
        LogicalPlan::Scan { table, columns } => {
            codegen_scan(table, columns.len(), cont, ctx)
        }
        LogicalPlan::Count { input } => {
            codegen_count(input, cont, ctx)
        }
        LogicalPlan::Values { rows } => {
            codegen_values(rows, cont, ctx)
        }
        LogicalPlan::Filter { .. } => {
            // TODO: Implement filter codegen
            panic!("Filter codegen not yet implemented")
        }
        LogicalPlan::Project { .. } => {
            // TODO: Implement project codegen
            panic!("Project codegen not yet implemented")
        }
        LogicalPlan::Limit { .. } => {
            // TODO: Implement limit codegen
            panic!("Limit codegen not yet implemented")
        }
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
    use crate::test::TestDb;

    /// Test that codegen_scan produces correct bytecode structure
    #[test]
    fn test_codegen_scan_structure() {
        let mut ctx = CodegenContext::new();

        // Create continuation labels (in body_emitter since that's where they're used)
        let on_tuple = ctx.body_emitter.create_label();
        let on_done = ctx.body_emitter.create_label();
        let cont = NodeContinuation { on_tuple, on_done };

        let output = codegen_scan("test_table", 2, &cont, &mut ctx);

        // Check that we got 2 output registers
        assert_eq!(output.output_regs.len(), 2);

        // Verify register allocation: cursor, flag, 2 output columns = 4 total
        assert_eq!(ctx.registers.count(), 4);
    }

    /// Integration test: Count(Scan) - verify row counting works
    #[test]
    fn test_count_scan() {
        // Build plan: Count { Scan { "test", 2 columns } }
        let plan = LogicalPlan::Count {
            input: Box::new(LogicalPlan::Scan {
                table: "test".to_string(),
                columns: vec![0, 1],
            }),
        };

        let (ops, num_registers) = compile_plan(&plan);

        // Create test database with 3 rows
        let test = TestDb::default();
        let mut btree = test.btree;
        btree.create_tree("test");

        let mut cursor = btree.open("test").unwrap();
        let mut cursor = cursor.open_readwrite();
        cursor.insert(0, b"[1, 100]".to_vec());
        cursor.insert(1, b"[2, 200]".to_vec());
        cursor.insert(2, b"[3, 300]".to_vec());
        drop(cursor);

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
        let plan = LogicalPlan::Count {
            input: Box::new(LogicalPlan::Scan {
                table: "test".to_string(),
                columns: vec![0],
            }),
        };

        let (ops, num_registers) = compile_plan(&plan);

        // Create test database with empty table
        let test = TestDb::default();
        let mut btree = test.btree;
        btree.create_tree("test");

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
        let plan = LogicalPlan::Scan {
            table: "test".to_string(),
            columns: vec![0, 1],
        };

        let (ops, num_registers) = compile_plan(&plan);

        // Create test database with data
        let test = TestDb::default();
        let mut btree = test.btree;
        btree.create_tree("test");

        let mut cursor = btree.open("test").unwrap();
        let mut cursor = cursor.open_readwrite();
        cursor.insert(0, b"[10, 20]".to_vec());
        cursor.insert(1, b"[30, 40]".to_vec());
        drop(cursor);

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
        assert_eq!(yields[0], vec![ScalarValue::Integer(1), ScalarValue::Integer(10)]);
        assert_eq!(yields[1], vec![ScalarValue::Integer(2), ScalarValue::Integer(20)]);
        assert_eq!(yields[2], vec![ScalarValue::Integer(3), ScalarValue::Integer(30)]);
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
            rows: vec![
                vec![
                    Literal::Integer(42),
                    Literal::Float(3.14),
                    Literal::Bool(true),
                    Literal::String("hello".to_string()),
                ],
            ],
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
}
