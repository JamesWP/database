pub mod emitter;
pub mod expr;
pub mod nodes;
pub mod registers;

pub use emitter::BytecodeEmitter;
pub use expr::{compile_expr, ExprContext};
pub use nodes::{
    codegen, codegen_count, codegen_filter, codegen_limit, codegen_project, codegen_scan,
    codegen_sequence, codegen_values, compile_plan, CodegenContext, NodeContinuation, NodeOutput,
};
pub use registers::RegisterAllocator;

use crate::engine::program::Operation;
use crate::planner::LogicalPlan;

/// A compiled program ready for execution by the VM.
#[derive(Debug)]
pub struct CompiledProgram {
    /// The bytecode operations
    pub operations: Vec<Operation>,
    /// Number of registers needed to execute the program
    pub num_registers: usize,
}

impl CompiledProgram {
    /// Get the operations as a slice.
    pub fn operations(&self) -> &[Operation] {
        &self.operations
    }

    /// Get the number of registers needed.
    pub fn num_registers(&self) -> usize {
        self.num_registers
    }
}

/// Compile a LogicalPlan into a CompiledProgram.
///
/// This is the main entry point to the compiler.
pub fn compile(plan: &LogicalPlan) -> CompiledProgram {
    let (operations, num_registers) = compile_plan(plan);
    CompiledProgram {
        operations,
        num_registers,
    }
}
