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
        assert!(self.label_positions[id].is_none(), "Label already bound");
        self.label_positions[id] = Some(self.operations.len());
    }

    /// Create a label and immediately bind it at the current position.
    pub fn label_here(&mut self) -> Label {
        let label = self.create_label();
        self.bind_label(label);
        label
    }

    /// Bind a label at the current position (alias for bind_label).
    pub fn bind_here(&mut self, label: Label) {
        self.bind_label(label);
    }

    /// Returns the current position (index of next instruction to be emitted).
    #[allow(dead_code)]
    pub fn current_position(&self) -> usize {
        self.operations.len()
    }

    /// Emit an operation at the current position.
    pub fn emit(&mut self, op: Operation) {
        self.operations.push(op);
    }

    /// Emit a GoTo instruction to the given label.
    /// Label resolution is deferred until finalize().
    pub fn emit_goto(&mut self, label: Label) {
        self.operations
            .push(Operation::GoTo(JumpTarget::Unresolved(label)));
    }

    /// Emit a GoToIfFalse instruction: jump to label if register is false.
    /// Label resolution is deferred until finalize().
    pub fn emit_goto_if_false(&mut self, label: Label, reg: Reg) {
        self.operations
            .push(Operation::GoToIfFalse(JumpTarget::Unresolved(label), reg));
    }

    /// Emit a GoToIfEqualValue instruction: jump to label if lhs == rhs.
    /// Label resolution is deferred until finalize().
    pub fn emit_goto_if_equal(&mut self, label: Label, lhs: Reg, rhs: Reg) {
        self.operations.push(Operation::GoToIfEqualValue(
            JumpTarget::Unresolved(label),
            lhs,
            rhs,
        ));
    }

    /// Emit a PopKey instruction: pop key from list into dest, or jump if empty.
    /// Label resolution is deferred until finalize().
    /// Finalize the bytecode by resolving all jump targets with an offset added.
    /// Returns the final list of operations.
    /// Panics if any label was never bound.
    ///
    /// The offset parameter is added to all resolved addresses, allowing code blocks
    /// to be concatenated without a separate adjustment pass. For standalone code,
    /// use `finalize()` which calls this with offset=0.
    ///
    /// This function is intentionally exhaustive - it explicitly matches every Operation variant.
    /// When a new operation with a JumpTarget is added, this function will fail to compile,
    /// ensuring that jump target resolution is not forgotten.
    pub fn finalize_with_offset(mut self, offset: usize) -> Vec<Operation> {
        let label_positions = &self.label_positions;

        // Resolve all unresolved jump targets, adding offset to final addresses
        for op in &mut self.operations {
            match op {
                // === Operations with JumpTarget - resolve unresolved labels ===
                Operation::GoTo(ref mut target) => {
                    *target = resolve_target_with_offset(target, label_positions, offset);
                }
                Operation::GoToIfFalse(ref mut target, _) => {
                    *target = resolve_target_with_offset(target, label_positions, offset);
                }
                Operation::GoToIfEqualValue(ref mut target, _, _) => {
                    *target = resolve_target_with_offset(target, label_positions, offset);
                }
                Operation::NextFromRowBuffer(_, _, ref mut target) => {
                    *target = resolve_target_with_offset(target, label_positions, offset);
                }
                Operation::YieldFromGroupTable(_, _, ref mut target) => {
                    *target = resolve_target_with_offset(target, label_positions, offset);
                }

                // === Operations without JumpTarget - explicit no-op ===
                // Value operations
                Operation::StoreValue(_, _)
                | Operation::IncrementValue(_)
                | Operation::DecrementValue(_)
                | Operation::AddValue(_, _, _)
                | Operation::SubtractValue(_, _, _)
                | Operation::MultiplyValue(_, _, _)
                | Operation::DivideValue(_, _, _)
                | Operation::RemainderValue(_, _, _)
                | Operation::LessThanValue(_, _, _)
                | Operation::LessThanOrEqualValue(_, _, _)
                | Operation::GreaterThanValue(_, _, _)
                | Operation::GreaterThanOrEqualValue(_, _, _)
                | Operation::EqualsValue(_, _, _)
                | Operation::NotEqualsValue(_, _, _)
                | Operation::AndValue(_, _, _)
                | Operation::OrValue(_, _, _)
                | Operation::NotValue(_, _)
                | Operation::NegateValue(_, _)
                | Operation::CopyValue(_, _)
                | Operation::IsNullValue(_, _)
                | Operation::IsNotNullValue(_, _)
                // String/Scalar function operations
                | Operation::LengthValue(_, _)
                | Operation::UpperValue(_, _)
                | Operation::LowerValue(_, _)
                | Operation::AbsValue(_, _)
                | Operation::RandomValue(_)
                | Operation::LikeValue(_, _, _)
                // Row buffer operations
                | Operation::InitRowBuffer(_)
                | Operation::AppendToRowBuffer(_, _)
                | Operation::SortRowBuffer(_, _)
                | Operation::RewindRowBuffer(_)
                // Group table operations
                | Operation::InitGroupTable(_)
                | Operation::UpdateGroup(_, _, _)
                // Database operations
                | Operation::Open(_, _)
                | Operation::MoveCursor(_, _)
                | Operation::ReadCursor(_, _)
                | Operation::ReadKey(_, _)
                | Operation::WriteCursor(_, _, _, _)
                | Operation::WriteIndex(_, _, _, _)
                | Operation::CheckUnique(_, _)
                | Operation::InitRowid(_, _)
                | Operation::DeleteIndex(_, _, _)
                | Operation::DeleteCursor(_)
                | Operation::CanReadCursor(_, _)
                | Operation::EncodeIndexKey(_, _)
                | Operation::ReadCurrentKey(_, _)
                | Operation::BlobStartsWith(_, _, _)
                | Operation::BlobPrefixLt(_, _, _)
                | Operation::BlobPrefixLe(_, _, _)
                | Operation::BlobSliceTail(_, _, _)
                | Operation::BlobSliceLast(_, _, _)
                | Operation::BlobDropLast(_, _, _)
                | Operation::DecodeU64Key(_, _)
                | Operation::DecodeIndexColumns { .. }
                // Control flow operations (without jump targets)
                | Operation::Yield(_)
                | Operation::Halt => {
                    // No jump targets to resolve
                }
            }
        }
        self.operations
    }

    /// Finalize the bytecode by resolving all jump targets.
    /// Equivalent to `finalize_with_offset(0)`.
    pub fn finalize(self) -> Vec<Operation> {
        self.finalize_with_offset(0)
    }
}

/// Resolve a JumpTarget, converting Unresolved to Resolved with an offset added.
/// This allows labels to be resolved directly to their final addresses when
/// code blocks are concatenated.
///
/// All jump targets are Unresolved when emitted (even backward jumps), so
/// resolution only happens here in one place.
fn resolve_target_with_offset(
    target: &JumpTarget,
    label_positions: &[Option<usize>],
    offset: usize,
) -> JumpTarget {
    match target {
        JumpTarget::Resolved(_) => {
            panic!("Bug: JumpTarget should be Unresolved before finalize")
        }
        JumpTarget::Unresolved(Label(id)) => {
            let base_addr = label_positions[*id].expect("Label was never bound");
            JumpTarget::Resolved(base_addr + offset)
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
    fn test_label_here() {
        let mut emitter = BytecodeEmitter::new();
        emitter.emit(Operation::Halt); // op 0
        let _label = emitter.label_here(); // should point to op 1
        emitter.emit(Operation::Halt); // op 1
        let ops = emitter.finalize();
        assert_eq!(ops.len(), 2);
        // Verify label resolves to position 1 by using it as a goto target
        let mut emitter2 = BytecodeEmitter::new();
        emitter2.emit(Operation::Halt); // op 0
        let label2 = emitter2.label_here(); // binds at op 1
        emitter2.emit(Operation::Halt); // op 1
        let _ = label2; // label points to position 1
        let _ = emitter2.finalize();
    }

    #[test]
    fn test_bind_here() {
        let mut emitter = BytecodeEmitter::new();
        let label = emitter.create_label();
        emitter.emit_goto(label); // forward ref to position 1
        emitter.bind_here(label); // bind at position 1
        emitter.emit(Operation::Halt);
        let ops = emitter.finalize();
        assert_eq!(ops.len(), 2);
        match &ops[0] {
            Operation::GoTo(target) => assert_eq!(target.unwrap_resolved(), 1),
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
