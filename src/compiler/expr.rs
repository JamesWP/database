use crate::engine::program::{Operation, Reg};
use crate::engine::scalarvalue::ScalarValue;
use crate::planner::{BinaryOp, ColumnRef, Literal, PlanExpr, UnaryOp};

use super::emitter::BytecodeEmitter;
use super::registers::RegisterAllocator;

/// Context for expression compilation, providing access to shared state.
pub struct ExprContext<'a> {
    pub emitter: &'a mut BytecodeEmitter,
    pub registers: &'a mut RegisterAllocator,
}

/// Compile a PlanExpr to bytecode.
///
/// `input_regs` contains the registers holding the current row's column values.
/// For a ColumnRef with index i, we copy from input_regs[i].
///
/// Returns the register containing the expression result.
pub fn compile_expr(
    expr: &PlanExpr,
    input_regs: &[Reg],
    ctx: &mut ExprContext,
) -> Reg {
    match expr {
        PlanExpr::ColumnRef(col_ref) => {
            compile_column_ref(col_ref, input_regs, ctx)
        }
        PlanExpr::Literal(lit) => {
            compile_literal(lit, ctx)
        }
        PlanExpr::BinaryOp { op, left, right } => {
            compile_binary_op(op, left, right, input_regs, ctx)
        }
        PlanExpr::UnaryOp { op, operand } => {
            compile_unary_op(op, operand, input_regs, ctx)
        }
    }
}

fn compile_column_ref(
    col_ref: &ColumnRef,
    input_regs: &[Reg],
    ctx: &mut ExprContext,
) -> Reg {
    match col_ref {
        ColumnRef::Single { column_idx } => {
            // Copy the value from the input register to a new register
            let src = input_regs[*column_idx];
            let dest = ctx.registers.alloc();
            ctx.emitter.emit(Operation::CopyValue(dest, src));
            dest
        }
    }
}

fn compile_literal(lit: &Literal, ctx: &mut ExprContext) -> Reg {
    let dest = ctx.registers.alloc();
    let scalar = match lit {
        Literal::Integer(i) => ScalarValue::Integer(*i),
        Literal::Float(f) => ScalarValue::Floating(*f),
        Literal::Bool(b) => ScalarValue::Boolean(*b),
        Literal::String(s) => ScalarValue::String(s.clone()),
        Literal::Null => {
            // TODO: Add proper NULL support to ScalarValue and VM
            panic!("NULL literals not yet supported")
        }
    };
    ctx.emitter.emit(Operation::StoreValue(dest, scalar));
    dest
}

fn compile_binary_op(
    op: &BinaryOp,
    left: &PlanExpr,
    right: &PlanExpr,
    input_regs: &[Reg],
    ctx: &mut ExprContext,
) -> Reg {
    let left_reg = compile_expr(left, input_regs, ctx);
    let right_reg = compile_expr(right, input_regs, ctx);
    let dest = ctx.registers.alloc();

    let operation = match op {
        // Arithmetic
        BinaryOp::Add => Operation::AddValue(dest, left_reg, right_reg),
        BinaryOp::Subtract => Operation::SubtractValue(dest, left_reg, right_reg),
        BinaryOp::Multiply => Operation::MultiplyValue(dest, left_reg, right_reg),
        BinaryOp::Divide => Operation::DivideValue(dest, left_reg, right_reg),
        BinaryOp::Remainder => Operation::RemainderValue(dest, left_reg, right_reg),

        // Comparison
        BinaryOp::Equals => Operation::EqualsValue(dest, left_reg, right_reg),
        BinaryOp::NotEquals => Operation::NotEqualsValue(dest, left_reg, right_reg),
        BinaryOp::GreaterThan => Operation::GreaterThanValue(dest, left_reg, right_reg),
        BinaryOp::GreaterThanOrEqual => Operation::GreaterThanOrEqualValue(dest, left_reg, right_reg),
        BinaryOp::LessThan => Operation::LessThanValue(dest, left_reg, right_reg),
        BinaryOp::LessThanOrEqual => Operation::LessThanOrEqualValue(dest, left_reg, right_reg),

        // Logical
        BinaryOp::And => Operation::AndValue(dest, left_reg, right_reg),
        BinaryOp::Or => Operation::OrValue(dest, left_reg, right_reg),

        // TODO: Add bitwise operations to VM (LeftShiftValue, RightShiftValue, etc.)
        BinaryOp::LeftShift
        | BinaryOp::RightShift
        | BinaryOp::BitOr
        | BinaryOp::BitXor
        | BinaryOp::BitAnd => {
            panic!("Bitwise operations not yet implemented")
        }
    };

    ctx.emitter.emit(operation);
    dest
}

fn compile_unary_op(
    op: &UnaryOp,
    operand: &PlanExpr,
    input_regs: &[Reg],
    ctx: &mut ExprContext,
) -> Reg {
    let operand_reg = compile_expr(operand, input_regs, ctx);
    let dest = ctx.registers.alloc();

    let operation = match op {
        UnaryOp::Negate => Operation::NegateValue(dest, operand_reg),
        UnaryOp::Not => Operation::NotValue(dest, operand_reg),
        UnaryOp::Plus => {
            // Plus is a no-op, just copy the value
            Operation::CopyValue(dest, operand_reg)
        }
    };

    ctx.emitter.emit(operation);
    dest
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile_integer_literal() {
        let mut emitter = BytecodeEmitter::new();
        let mut registers = RegisterAllocator::new();

        let expr = PlanExpr::Literal(Literal::Integer(42));
        let result = {
            let mut ctx = ExprContext {
                emitter: &mut emitter,
                registers: &mut registers,
            };
            compile_expr(&expr, &[], &mut ctx)
        };

        assert_eq!(result.index(), 0);
        let ops = emitter.finalize();
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            Operation::StoreValue(r, ScalarValue::Integer(42)) => {
                assert_eq!(r.index(), 0);
            }
            _ => panic!("Expected StoreValue(Integer(42))"),
        }
    }

    #[test]
    fn test_compile_float_literal() {
        let mut emitter = BytecodeEmitter::new();
        let mut registers = RegisterAllocator::new();

        let expr = PlanExpr::Literal(Literal::Float(3.14));
        let result = {
            let mut ctx = ExprContext {
                emitter: &mut emitter,
                registers: &mut registers,
            };
            compile_expr(&expr, &[], &mut ctx)
        };

        assert_eq!(result.index(), 0);
        let ops = emitter.finalize();
        assert_eq!(ops.len(), 1);
    }

    #[test]
    fn test_compile_column_ref() {
        let mut emitter = BytecodeEmitter::new();
        let mut registers = RegisterAllocator::new();

        // Simulate input registers from a scan (columns 0 and 1)
        let input_regs = vec![Reg::new(10), Reg::new(11)];

        let expr = PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 1 });
        let result = {
            let mut ctx = ExprContext {
                emitter: &mut emitter,
                registers: &mut registers,
            };
            compile_expr(&expr, &input_regs, &mut ctx)
        };

        let ops = emitter.finalize();
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            Operation::CopyValue(dest, src) => {
                assert_eq!(dest.index(), result.index());
                assert_eq!(src.index(), 11); // column 1 from input_regs
            }
            _ => panic!("Expected CopyValue"),
        }
    }

    #[test]
    fn test_compile_binary_add() {
        let mut emitter = BytecodeEmitter::new();
        let mut registers = RegisterAllocator::new();

        // 10 + 5
        let expr = PlanExpr::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(PlanExpr::Literal(Literal::Integer(10))),
            right: Box::new(PlanExpr::Literal(Literal::Integer(5))),
        };

        let result = {
            let mut ctx = ExprContext {
                emitter: &mut emitter,
                registers: &mut registers,
            };
            compile_expr(&expr, &[], &mut ctx)
        };

        let ops = emitter.finalize();
        assert_eq!(ops.len(), 3); // StoreValue, StoreValue, AddValue
        match &ops[2] {
            Operation::AddValue(dest, _, _) => {
                assert_eq!(dest.index(), result.index());
            }
            _ => panic!("Expected AddValue"),
        }
    }

    #[test]
    fn test_compile_comparison() {
        let mut emitter = BytecodeEmitter::new();
        let mut registers = RegisterAllocator::new();

        // 10 > 5
        let expr = PlanExpr::BinaryOp {
            op: BinaryOp::GreaterThan,
            left: Box::new(PlanExpr::Literal(Literal::Integer(10))),
            right: Box::new(PlanExpr::Literal(Literal::Integer(5))),
        };

        let result = {
            let mut ctx = ExprContext {
                emitter: &mut emitter,
                registers: &mut registers,
            };
            compile_expr(&expr, &[], &mut ctx)
        };

        let ops = emitter.finalize();
        assert_eq!(ops.len(), 3);
        match &ops[2] {
            Operation::GreaterThanValue(dest, _, _) => {
                assert_eq!(dest.index(), result.index());
            }
            _ => panic!("Expected GreaterThanValue"),
        }
    }

    #[test]
    fn test_compile_unary_negate() {
        let mut emitter = BytecodeEmitter::new();
        let mut registers = RegisterAllocator::new();

        // -42
        let expr = PlanExpr::UnaryOp {
            op: UnaryOp::Negate,
            operand: Box::new(PlanExpr::Literal(Literal::Integer(42))),
        };

        let result = {
            let mut ctx = ExprContext {
                emitter: &mut emitter,
                registers: &mut registers,
            };
            compile_expr(&expr, &[], &mut ctx)
        };

        let ops = emitter.finalize();
        assert_eq!(ops.len(), 2); // StoreValue, NegateValue
        match &ops[1] {
            Operation::NegateValue(dest, _) => {
                assert_eq!(dest.index(), result.index());
            }
            _ => panic!("Expected NegateValue"),
        }
    }

    #[test]
    fn test_compile_nested_expression() {
        let mut emitter = BytecodeEmitter::new();
        let mut registers = RegisterAllocator::new();

        // (a + 5) * 2, where a is column 0
        let input_regs = vec![Reg::new(100)]; // a is in register 100

        let expr = PlanExpr::BinaryOp {
            op: BinaryOp::Multiply,
            left: Box::new(PlanExpr::BinaryOp {
                op: BinaryOp::Add,
                left: Box::new(PlanExpr::ColumnRef(ColumnRef::Single { column_idx: 0 })),
                right: Box::new(PlanExpr::Literal(Literal::Integer(5))),
            }),
            right: Box::new(PlanExpr::Literal(Literal::Integer(2))),
        };

        let result = {
            let mut ctx = ExprContext {
                emitter: &mut emitter,
                registers: &mut registers,
            };
            compile_expr(&expr, &input_regs, &mut ctx)
        };

        let ops = emitter.finalize();
        // CopyValue(a), StoreValue(5), AddValue, StoreValue(2), MultiplyValue
        assert_eq!(ops.len(), 5);
        match &ops[4] {
            Operation::MultiplyValue(dest, _, _) => {
                assert_eq!(dest.index(), result.index());
            }
            _ => panic!("Expected MultiplyValue"),
        }
    }

    #[test]
    fn test_compile_logical_and() {
        let mut emitter = BytecodeEmitter::new();
        let mut registers = RegisterAllocator::new();

        // true AND false
        let expr = PlanExpr::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(PlanExpr::Literal(Literal::Bool(true))),
            right: Box::new(PlanExpr::Literal(Literal::Bool(false))),
        };

        {
            let mut ctx = ExprContext {
                emitter: &mut emitter,
                registers: &mut registers,
            };
            compile_expr(&expr, &[], &mut ctx);
        }

        let ops = emitter.finalize();
        assert_eq!(ops.len(), 3);
        match &ops[2] {
            Operation::AndValue(_, _, _) => {}
            _ => panic!("Expected AndValue"),
        }
    }
}
