use crate::engine::program::{JumpTarget, Label, Operation, Reg};

/// BytecodeEmitter collects bytecode instructions and handles label-based jumps.
/// Jump targets are represented using the JumpTarget enum which can be either
/// Unresolved(Label) or Resolved(usize). During finalization, all unresolved
/// labels are resolved to concrete addresses.
pub struct BytecodeEmitter {
    operations: Vec<Operation>,
    /// Maps label IDs to their bound positions (None if not yet bound)
    label_positions: Vec<Option<usize>>,
    /// Counter for generating unique label IDs
    next_label_id: usize,
}

impl BytecodeEmitter {
    pub fn new() -> Self {
        BytecodeEmitter {
            operations: Vec::new(),
            label_positions: Vec::new(),
            next_label_id: 0,
        }
    }

    /// Create a new label that can be bound later.
    pub fn create_label(&mut self) -> Label {
        let id = self.next_label_id;
        self.next_label_id += 1;
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

    /// Resolve a label to a JumpTarget.
    /// If the label is already bound, returns Resolved; otherwise Unresolved.
    fn resolve_label(&self, label: Label) -> JumpTarget {
        let Label(id) = label;
        match self.label_positions.get(id).and_then(|pos| *pos) {
            Some(addr) => JumpTarget::Resolved(addr),
            None => JumpTarget::Unresolved(label),
        }
    }

    /// Emit an operation at the current position.
    pub fn emit(&mut self, op: Operation) {
        self.operations.push(op);
    }

    /// Emit a GoTo instruction to the given label.
    pub fn emit_goto(&mut self, label: Label) {
        let target = self.resolve_label(label);
        self.operations.push(Operation::GoTo(target));
    }

    /// Emit a GoToIfFalse instruction: jump to label if register is false.
    pub fn emit_goto_if_false(&mut self, label: Label, reg: Reg) {
        let target = self.resolve_label(label);
        self.operations.push(Operation::GoToIfFalse(target, reg));
    }

    /// Emit a GoToIfEqualValue instruction: jump to label if lhs == rhs.
    pub fn emit_goto_if_equal(&mut self, label: Label, lhs: Reg, rhs: Reg) {
        let target = self.resolve_label(label);
        self.operations.push(Operation::GoToIfEqualValue(target, lhs, rhs));
    }

    /// Finalize the bytecode by resolving all jump targets.
    /// Returns the final list of operations.
    /// Panics if any label was never bound.
    pub fn finalize(mut self) -> Vec<Operation> {
        let label_positions = &self.label_positions;

        // Resolve all unresolved jump targets
        for op in &mut self.operations {
            match op {
                Operation::GoTo(ref mut target) => {
                    *target = resolve_target(target, label_positions);
                }
                Operation::GoToIfFalse(ref mut target, _) => {
                    *target = resolve_target(target, label_positions);
                }
                Operation::GoToIfEqualValue(ref mut target, _, _) => {
                    *target = resolve_target(target, label_positions);
                }
                _ => {}
            }
        }
        self.operations
    }
}

/// Resolve a JumpTarget, converting Unresolved to Resolved.
fn resolve_target(target: &JumpTarget, label_positions: &[Option<usize>]) -> JumpTarget {
    match target {
        JumpTarget::Resolved(addr) => JumpTarget::Resolved(*addr),
        JumpTarget::Unresolved(Label(id)) => {
            let addr = label_positions[*id]
                .expect("Label was never bound");
            JumpTarget::Resolved(addr)
        }
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
            Operation::GoTo(target) => assert_eq!(target.unwrap_resolved(), 0),
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
            Operation::GoTo(target) => assert_eq!(target.unwrap_resolved(), 2), // Should point to Halt
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
            Operation::GoToIfFalse(target, _) => assert_eq!(target.unwrap_resolved(), 3),
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
            Operation::GoTo(target) => assert_eq!(target.unwrap_resolved(), 4),
            _ => panic!("Expected GoTo"),
        }
        match &ops[2] {
            Operation::GoTo(target) => assert_eq!(target.unwrap_resolved(), 4),
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
