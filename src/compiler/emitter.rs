use crate::engine::program::{Operation, Reg};

/// A label represents a position in the bytecode that can be used as a jump target.
/// Labels may be created before their position is known (forward references).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Label(usize);

/// Tracks a forward reference that needs to be patched when finalized.
#[derive(Debug)]
struct ForwardRef {
    /// Index of the instruction containing the forward reference
    instruction_index: usize,
    /// The label being referenced
    label: Label,
}

/// BytecodeEmitter collects bytecode instructions and handles label-based jumps.
/// It supports forward references through a two-phase approach:
/// 1. Emit instructions with placeholder addresses for unbound labels
/// 2. Patch all forward references when finalize() is called
pub struct BytecodeEmitter {
    operations: Vec<Operation>,
    /// Maps label IDs to their bound positions (None if not yet bound)
    label_positions: Vec<Option<usize>>,
    /// Forward references that need patching
    forward_refs: Vec<ForwardRef>,
}

impl BytecodeEmitter {
    pub fn new() -> Self {
        BytecodeEmitter {
            operations: Vec::new(),
            label_positions: Vec::new(),
            forward_refs: Vec::new(),
        }
    }

    /// Create a new label that can be bound later.
    pub fn create_label(&mut self) -> Label {
        let id = self.label_positions.len();
        self.label_positions.push(None);
        Label(id)
    }

    /// Bind a label to the current position (the next instruction that will be emitted).
    pub fn bind_label(&mut self, label: Label) {
        let Label(id) = label;
        assert!(
            self.label_positions[id].is_none(),
            "Label already bound"
        );
        self.label_positions[id] = Some(self.operations.len());
    }

    /// Returns the current position (index of next instruction to be emitted).
    pub fn current_position(&self) -> usize {
        self.operations.len()
    }

    /// Emit an operation at the current position.
    pub fn emit(&mut self, op: Operation) {
        self.operations.push(op);
    }

    /// Emit a GoTo instruction to the given label.
    pub fn emit_goto(&mut self, label: Label) {
        let Label(id) = label;
        let target = match self.label_positions[id] {
            Some(pos) => pos,
            None => {
                // Forward reference: use placeholder and record for patching
                self.forward_refs.push(ForwardRef {
                    instruction_index: self.operations.len(),
                    label,
                });
                0 // placeholder
            }
        };
        self.operations.push(Operation::GoTo(target));
    }

    /// Emit a GoToIfFalse instruction: jump to label if register is false.
    pub fn emit_goto_if_false(&mut self, label: Label, reg: Reg) {
        let Label(id) = label;
        let target = match self.label_positions[id] {
            Some(pos) => pos,
            None => {
                self.forward_refs.push(ForwardRef {
                    instruction_index: self.operations.len(),
                    label,
                });
                0 // placeholder
            }
        };
        // Note: The third reg is unused in the current VM implementation
        self.operations.push(Operation::GoToIfFalse(target, reg, reg));
    }

    /// Emit a GoToIfEqualValue instruction: jump to label if lhs == rhs.
    pub fn emit_goto_if_equal(&mut self, label: Label, lhs: Reg, rhs: Reg) {
        let Label(id) = label;
        let target = match self.label_positions[id] {
            Some(pos) => pos,
            None => {
                self.forward_refs.push(ForwardRef {
                    instruction_index: self.operations.len(),
                    label,
                });
                0 // placeholder
            }
        };
        self.operations.push(Operation::GoToIfEqualValue(target, lhs, rhs));
    }

    /// Finalize the bytecode by patching all forward references.
    /// Returns the final list of operations.
    pub fn finalize(mut self) -> Vec<Operation> {
        for fwd_ref in &self.forward_refs {
            let Label(id) = fwd_ref.label;
            let target = self.label_positions[id]
                .expect("Label was never bound");

            // Patch the instruction
            let op = &mut self.operations[fwd_ref.instruction_index];
            match op {
                Operation::GoTo(ref mut addr) => *addr = target,
                Operation::GoToIfFalse(ref mut addr, _, _) => *addr = target,
                Operation::GoToIfEqualValue(ref mut addr, _, _) => *addr = target,
                _ => panic!("Unexpected operation type in forward reference"),
            }
        }
        self.operations
    }
}

impl Default for BytecodeEmitter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::scalarvalue::ScalarValue;

    #[test]
    fn test_emit_sequence() {
        let mut emitter = BytecodeEmitter::new();
        emitter.emit(Operation::StoreValue(Reg::new(0), ScalarValue::Integer(1)));
        emitter.emit(Operation::StoreValue(Reg::new(1), ScalarValue::Integer(2)));
        emitter.emit(Operation::Halt);

        let ops = emitter.finalize();
        assert_eq!(ops.len(), 3);
    }

    #[test]
    fn test_backward_jump() {
        let mut emitter = BytecodeEmitter::new();

        // Create and immediately bind a label
        let loop_start = emitter.create_label();
        emitter.bind_label(loop_start);

        emitter.emit(Operation::StoreValue(Reg::new(0), ScalarValue::Integer(1)));
        emitter.emit_goto(loop_start);

        let ops = emitter.finalize();
        assert_eq!(ops.len(), 2);
        match &ops[1] {
            Operation::GoTo(addr) => assert_eq!(*addr, 0),
            _ => panic!("Expected GoTo"),
        }
    }

    #[test]
    fn test_forward_jump() {
        let mut emitter = BytecodeEmitter::new();

        // Create label but don't bind yet
        let skip_label = emitter.create_label();

        emitter.emit_goto(skip_label); // Forward reference
        emitter.emit(Operation::StoreValue(Reg::new(0), ScalarValue::Integer(1)));

        // Now bind the label
        emitter.bind_label(skip_label);
        emitter.emit(Operation::Halt);

        let ops = emitter.finalize();
        assert_eq!(ops.len(), 3);
        match &ops[0] {
            Operation::GoTo(addr) => assert_eq!(*addr, 2), // Should point to Halt
            _ => panic!("Expected GoTo"),
        }
    }

    #[test]
    fn test_forward_conditional_jump() {
        let mut emitter = BytecodeEmitter::new();

        let end_label = emitter.create_label();
        let r0 = Reg::new(0);

        emitter.emit(Operation::StoreValue(r0, ScalarValue::Boolean(false)));
        emitter.emit_goto_if_false(end_label, r0);
        emitter.emit(Operation::StoreValue(r0, ScalarValue::Integer(1)));

        emitter.bind_label(end_label);
        emitter.emit(Operation::Halt);

        let ops = emitter.finalize();
        assert_eq!(ops.len(), 4);
        match &ops[1] {
            Operation::GoToIfFalse(addr, _, _) => assert_eq!(*addr, 3),
            _ => panic!("Expected GoToIfFalse"),
        }
    }

    #[test]
    fn test_multiple_forward_refs_same_label() {
        let mut emitter = BytecodeEmitter::new();

        let end_label = emitter.create_label();
        let r0 = Reg::new(0);

        emitter.emit_goto(end_label);
        emitter.emit(Operation::StoreValue(r0, ScalarValue::Integer(1)));
        emitter.emit_goto(end_label);
        emitter.emit(Operation::StoreValue(r0, ScalarValue::Integer(2)));

        emitter.bind_label(end_label);
        emitter.emit(Operation::Halt);

        let ops = emitter.finalize();
        assert_eq!(ops.len(), 5);

        match &ops[0] {
            Operation::GoTo(addr) => assert_eq!(*addr, 4),
            _ => panic!("Expected GoTo"),
        }
        match &ops[2] {
            Operation::GoTo(addr) => assert_eq!(*addr, 4),
            _ => panic!("Expected GoTo"),
        }
    }

    #[test]
    #[should_panic(expected = "Label already bound")]
    fn test_double_bind_panics() {
        let mut emitter = BytecodeEmitter::new();
        let label = emitter.create_label();
        emitter.bind_label(label);
        emitter.bind_label(label); // Should panic
    }

    #[test]
    #[should_panic(expected = "Label was never bound")]
    fn test_unbound_label_panics() {
        let mut emitter = BytecodeEmitter::new();
        let label = emitter.create_label();
        emitter.emit_goto(label);
        emitter.finalize(); // Should panic because label was never bound
    }
}
