pub mod emitter;
pub mod expr;
pub mod nodes;
pub mod registers;

pub use emitter::BytecodeEmitter;
pub use expr::{compile_expr, ExprContext};
pub use nodes::compile_plan;
pub use registers::RegisterAllocator;

use crate::engine::program::Operation;
use crate::planner::LogicalPlan;

/// Emit a sequence of operations to `ctx.body_emitter`.
///
/// Supports:
/// - `Bind(label)` — bind_here(label)
/// - `GoTo(label)` — emit_goto(label)
/// - `GoToIfFalse(label, reg)` — emit_goto_if_false(label, reg)
/// - `GoToIfEqualValue(label, a, b)` — emit_goto_if_equal(label, a, b)
/// - `Op(args...)` — emit(Operation::Op(args...))
#[macro_export]
macro_rules! body {
    ($ctx:expr $(;)?) => {};

    ($ctx:expr; Bind($label:expr) $(; $($rest:tt)*)?) => {
        $ctx.body_emitter.bind_here($label);
        $(body!($ctx; $($rest)*);)?
    };

    ($ctx:expr; GoTo($label:expr) $(; $($rest:tt)*)?) => {
        $ctx.body_emitter.emit_goto($label);
        $(body!($ctx; $($rest)*);)?
    };

    ($ctx:expr; GoToIfFalse($label:expr, $reg:expr) $(; $($rest:tt)*)?) => {
        $ctx.body_emitter.emit_goto_if_false($label, $reg);
        $(body!($ctx; $($rest)*);)?
    };

    ($ctx:expr; GoToIfEqualValue($label:expr, $a:expr, $b:expr) $(; $($rest:tt)*)?) => {
        $ctx.body_emitter.emit_goto_if_equal($label, $a, $b);
        $(body!($ctx; $($rest)*);)?
    };

    // Zero-argument operation (e.g., Halt)
    ($ctx:expr; $op:ident $(; $($rest:tt)*)?) => {
        $ctx.body_emitter.emit(Operation::$op);
        $(body!($ctx; $($rest)*);)?
    };

    ($ctx:expr; $op:ident($($arg:expr),*) $(; $($rest:tt)*)?) => {
        $ctx.body_emitter.emit(Operation::$op($($arg),*));
        $(body!($ctx; $($rest)*);)?
    };
}

/// Emit a sequence of operations to `ctx.init_emitter`.
///
/// Supports `Bind`, `GoTo`, `GoToIfFalse`, `GoToIfEqualValue`, and general `Op(args...)`.
#[macro_export]
macro_rules! init {
    ($ctx:expr $(;)?) => {};

    ($ctx:expr; Bind($label:expr) $(; $($rest:tt)*)?) => {
        $ctx.init_emitter.bind_here($label);
        $(init!($ctx; $($rest)*);)?
    };

    ($ctx:expr; GoTo($label:expr) $(; $($rest:tt)*)?) => {
        $ctx.init_emitter.emit_goto($label);
        $(init!($ctx; $($rest)*);)?
    };

    ($ctx:expr; GoToIfFalse($label:expr, $reg:expr) $(; $($rest:tt)*)?) => {
        $ctx.init_emitter.emit_goto_if_false($label, $reg);
        $(init!($ctx; $($rest)*);)?
    };

    ($ctx:expr; GoToIfEqualValue($label:expr, $a:expr, $b:expr) $(; $($rest:tt)*)?) => {
        $ctx.init_emitter.emit_goto_if_equal($label, $a, $b);
        $(init!($ctx; $($rest)*);)?
    };

    ($ctx:expr; $op:ident($($arg:expr),*) $(; $($rest:tt)*)?) => {
        $ctx.init_emitter.emit(Operation::$op($($arg),*));
        $(init!($ctx; $($rest)*);)?
    };
}

/// A compiled program ready for execution by the VM.
#[derive(Debug, Clone)]
pub struct CompiledProgram {
    /// The bytecode operations
    pub operations: Vec<Operation>,
    /// Number of registers needed to execute the program
    pub num_registers: usize,
    /// Output column names (empty for non-query statements)
    pub column_names: Vec<String>,
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
        column_names: vec![],
    }
}
