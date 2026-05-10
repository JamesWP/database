use crate::engine::program::{self, JumpTarget, Label, MoveOperation, Operation, Reg};
use crate::engine::scalarvalue::ScalarValue;
use crate::planner::{Literal, LogicalPlan, PlanExpr};
use crate::{body, init};

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
    /// Set only by `codegen_join_nested_loop` before compiling the right subtree;
    /// consumed only by `codegen_index_probe` to evaluate `key_expr` against the
    /// current left row. Must be `None` everywhere else. See phase-ac plan
    /// "Codegen context and coupling".
    pub outer_regs: Option<Vec<Reg>>,
}

impl CodegenContext {
    pub fn new() -> Self {
        CodegenContext {
            init_emitter: BytecodeEmitter::new(),
            body_emitter: BytecodeEmitter::new(),
            registers: RegisterAllocator::new(),
            outer_regs: None,
        }
    }

    /// Finalize and combine init + body code.
    /// Layout: init_code + GoTo(body_start) + body_code
    ///
    /// Body code labels are resolved with an offset, so they point to the correct
    /// addresses in the final combined program. This eliminates the need for a
    /// separate jump target adjustment pass.
    pub fn finalize(self) -> Vec<Operation> {
        let init_ops = self.init_emitter.finalize();

        let mut result = Vec::with_capacity(init_ops.len() + 1);

        // Add init code
        result.extend(init_ops);

        // Add jump to body start (which is right after this jump)
        let body_start = result.len() + 1;
        result.push(Operation::GoTo(JumpTarget::addr(body_start)));

        // Add body code, resolving labels with offset baked in
        let offset = result.len();
        let body_ops = self.body_emitter.finalize_with_offset(offset);
        result.extend(body_ops);

        result
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
    /// Entry point to restart this node from scratch for the current outer (left) row.
    /// Required by NestedLoop join; `None` for nodes that do not support reset
    /// (materializing nodes such as Sort, Aggregate, Join(Hash)). Pass-through nodes
    /// (Filter, Project, RowidLookup) propagate this from their child unchanged.
    /// See phase-ac plan "Reset contract".
    pub reset: Option<Label>,
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
/// Generate bytecode for a Scan node.
///
/// When `with_key` is false, output registers contain only the requested
/// columns. When `with_key` is true, the B-tree row key is read between
/// `ReadCursor` and `MoveCursor(Next)` (so it reflects the current row) and
/// appended as the last output register. This is used by
/// `codegen_populate_index` which needs the primary key to build index entries.
pub fn codegen_scan(
    rootpage: u32,
    columns: &[usize],
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // Scan-space encoding: index 0 = B-tree key (ReadKey), index k>0 = CBOR body slot k-1.
    let cursor_reg = ctx.registers.alloc();
    let flag_reg = ctx.registers.alloc();

    // Separate key column (scan index 0) from CBOR columns (scan index k>0).
    let needs_key = columns.contains(&0);
    // CBOR slot indices = scan_index - 1 for each column with scan_index > 0
    let cbor_slot_max = columns.iter().filter(|&&k| k > 0).map(|&k| k - 1).max();
    let num_cbor = cbor_slot_max.map(|m| m + 1).unwrap_or(0);
    let cbor_regs = ctx.registers.alloc_block(num_cbor);

    let key_reg = if needs_key {
        Some(ctx.registers.alloc())
    } else {
        None
    };

    // output_regs[i] = register for columns[i]
    let output_regs: Vec<Reg> = columns
        .iter()
        .map(|&col| {
            if col == 0 {
                key_reg.unwrap()
            } else {
                cbor_regs[col - 1]
            }
        })
        .collect();

    // INIT: Open cursor and move to first row for standalone callers.
    // NestedLoop join jumps to the reset label in the body instead.
    init!(ctx;
        Open(cursor_reg, rootpage);
        MoveCursor(cursor_reg, MoveOperation::First)
    );

    // BODY:
    // RESET: jumped to by NestedLoop join once per left row to restart the scan.
    let reset_label = ctx.body_emitter.label_here();
    body!(ctx; MoveCursor(cursor_reg, MoveOperation::First));

    // CHECK: entry point for iteration
    let check_label = ctx.body_emitter.label_here();
    body!(ctx;
        CanReadCursor(flag_reg, cursor_reg);
        GoToIfFalse(cont.on_done, flag_reg)
    );

    // Read CBOR body if any CBOR columns are needed
    if !cbor_regs.is_empty() {
        body!(ctx; ReadCursor(cbor_regs.clone(), cursor_reg));
    }

    // Read B-tree key if scan index 0 is needed
    if let Some(kr) = key_reg {
        body!(ctx; ReadKey(kr, cursor_reg));
    }

    body!(ctx;
        MoveCursor(cursor_reg, MoveOperation::Next);
        GoTo(cont.on_tuple)
    );

    NodeOutput {
        next: check_label,
        reset: Some(reset_label),
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
    init!(ctx; StoreValue(counter_reg, ScalarValue::Integer(0)));

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
    body!(ctx;
        Bind(child_on_tuple);
        IncrementValue(counter_reg);
        GoTo(child_output.next);
        Bind(child_on_done);
        GoTo(cont.on_tuple)
    );

    // count_next: after yielding once, we're done
    let count_next = ctx.body_emitter.label_here();
    body!(ctx; GoTo(cont.on_done));

    NodeOutput {
        next: count_next,
        output_regs: vec![counter_reg],
        reset: None,
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
        let check_label = ctx.body_emitter.label_here();
        body!(ctx; GoTo(cont.on_done));
        return NodeOutput {
            next: check_label,
            output_regs: vec![],
            reset: None,
        };
    }

    // Allocate output registers
    let output_regs = ctx.registers.alloc_block(num_cols);

    // Allocate index counter and num_rows constant
    let index_reg = ctx.registers.alloc();
    let num_rows_reg = ctx.registers.alloc();
    let cmp_reg = ctx.registers.alloc();

    // INIT: index = 0, num_rows = N
    init!(ctx;
        StoreValue(index_reg, ScalarValue::Integer(0));
        StoreValue(num_rows_reg, ScalarValue::Integer(num_rows as i64))
    );

    // Allocate constant registers for each row index (for dispatch comparison)
    let index_constants: Vec<Reg> = (0..num_rows)
        .map(|i| {
            let reg = ctx.registers.alloc();
            init!(ctx; StoreValue(reg, ScalarValue::Integer(i as i64)));
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
    let check_label = ctx.body_emitter.label_here();
    body!(ctx;
        LessThanValue(cmp_reg, index_reg, num_rows_reg);
        GoToIfFalse(cont.on_done, cmp_reg)
    );

    // DISPATCH: for each row, check if index == i and jump to that row
    for (i, row_label) in row_labels.iter().enumerate() {
        body!(ctx; GoToIfEqualValue(*row_label, index_reg, index_constants[i]));
    }

    // Fallthrough safety: shouldn't reach here, but go to done
    body!(ctx; GoTo(cont.on_done));

    // Emit each row's code
    for (i, row) in rows.iter().enumerate() {
        ctx.body_emitter.bind_here(row_labels[i]);
        for (j, lit) in row.iter().enumerate() {
            let sv = literal_to_scalar(lit);
            body!(ctx; StoreValue(output_regs[j], sv.clone()));
        }
        body!(ctx; GoTo(emit_label));
    }

    // EMIT: increment index, goto on_tuple
    body!(ctx;
        Bind(emit_label);
        IncrementValue(index_reg);
        GoTo(cont.on_tuple)
    );

    NodeOutput {
        next: check_label,
        output_regs,
        reset: None,
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
    init!(ctx;
        StoreValue(value_reg, ScalarValue::Integer(start));
        StoreValue(end_reg, ScalarValue::Integer(end))
    );

    // BODY:
    // CHECK: if value >= end, goto on_done
    let check_label = ctx.body_emitter.label_here();
    body!(ctx;
        LessThanValue(flag_reg, value_reg, end_reg);
        GoToIfFalse(cont.on_done, flag_reg);
        CopyValue(output_reg, value_reg);
        IncrementValue(value_reg);
        GoTo(cont.on_tuple)
    );

    NodeOutput {
        next: check_label,
        output_regs: vec![output_reg],
        reset: None,
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
    ctx.body_emitter.bind_here(filter_check);

    // Compile the predicate expression
    let pred_reg = {
        let mut expr_ctx = ExprContext {
            emitter: &mut ctx.body_emitter,
            registers: &mut ctx.registers,
        };
        compile_expr(predicate, &child_output.output_regs, &mut expr_ctx)
    };

    // If predicate is false, get next from child (reject)
    body!(ctx;
        GoToIfFalse(child_output.next, pred_reg);
        GoTo(cont.on_tuple)
    );

    // Return: delegate to child for next, propagate reset, pass through output registers
    NodeOutput {
        next: child_output.next,
        reset: child_output.reset, // propagate child's reset label unchanged
        output_regs: child_output.output_regs,
    }
}

/// Generate bytecode for an IndexScan node.
///
/// Scans the index B-tree and yields one rowid per matching entry.
/// Knows nothing about the table; a RowidLookup node above fetches columns.
///
/// Output: a single register containing the rowid as an Integer.
pub fn codegen_index_scan(
    index_rootpage: u32,
    lower_bound: &Option<(Literal, bool)>,
    upper_bound: &Option<(Literal, bool)>,
    output_columns: Option<&[usize]>,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    let index_cursor_reg = ctx.registers.alloc();
    let flag_reg = ctx.registers.alloc();
    // For the non-covering path we need a pk register; allocate upfront.
    let pk_reg = ctx.registers.alloc();
    let output_regs = vec![pk_reg];

    // INIT: Open index cursor
    init!(ctx; Open(index_cursor_reg, index_rootpage));

    // Encode lower bound and position cursor
    if let Some((lower_lit, _lower_inclusive)) = lower_bound {
        let lower_key_reg = ctx.registers.alloc();
        let lower_scalar = literal_to_scalar(lower_lit);
        init!(ctx;
            StoreValue(lower_key_reg, lower_scalar);
            EncodeIndexKey(lower_key_reg, lower_key_reg);
            MoveCursor(index_cursor_reg, MoveOperation::Find(lower_key_reg))
        );

        // If exclusive lower bound, skip entries where key starts with the lower bound prefix.
        // TEXT values in encode_index_value are NUL-terminated ([0x03][bytes][0x00]), ensuring
        // that 'a' encoded as [0x03,0x61,0x00] is NOT a prefix of 'apple' [0x03,0x61,0x70,...].
        // So BlobStartsWith correctly identifies exact column-value matches.
        if !_lower_inclusive {
            let skip_check = ctx.body_emitter.label_here();
            let can_read_reg = ctx.registers.alloc();
            let key_blob = ctx.registers.alloc();
            let matches_lower = ctx.registers.alloc();
            let after_skip = ctx.body_emitter.create_label();
            body!(ctx;
                CanReadCursor(can_read_reg, index_cursor_reg);
                GoToIfFalse(cont.on_done, can_read_reg);
                ReadCurrentKey(key_blob, index_cursor_reg);
                BlobStartsWith(matches_lower, key_blob, lower_key_reg);
                GoToIfFalse(after_skip, matches_lower);
                MoveCursor(index_cursor_reg, MoveOperation::Next);
                GoTo(skip_check);
                Bind(after_skip)
            );
        }
    } else {
        // No lower bound: start from the beginning
        init!(ctx; MoveCursor(index_cursor_reg, MoveOperation::First));
    }

    // Encode upper bound register if present
    let upper_key_reg = if let Some((upper_lit, _)) = upper_bound {
        let reg = ctx.registers.alloc();
        let upper_scalar = literal_to_scalar(upper_lit);
        init!(ctx;
            StoreValue(reg, upper_scalar);
            EncodeIndexKey(reg, reg)
        );
        Some((reg, upper_bound.as_ref().unwrap().1))
    } else {
        None
    };

    // BODY: Check bounds and yield rows
    let index_check = ctx.body_emitter.label_here();
    body!(ctx;
        CanReadCursor(flag_reg, index_cursor_reg);
        GoToIfFalse(cont.on_done, flag_reg)
    );

    // Read the current index key once; reused for bound check and rowid extraction
    let key_blob_reg = ctx.registers.alloc();
    body!(ctx; ReadCurrentKey(key_blob_reg, index_cursor_reg));

    // Check upper bound: stop if key prefix exceeds bound
    if let Some((upper_reg, inclusive)) = upper_key_reg {
        let in_range_reg = ctx.registers.alloc();
        if inclusive {
            body!(ctx; BlobPrefixLe(in_range_reg, key_blob_reg, upper_reg));
        } else {
            body!(ctx; BlobPrefixLt(in_range_reg, key_blob_reg, upper_reg));
        }
        body!(ctx; GoToIfFalse(cont.on_done, in_range_reg));
    }

    let (final_output_regs, index_next) = match output_columns {
        None => {
            // Non-covering path: extract rowid from last 8 bytes of the index key.
            let pk_blob_reg = ctx.registers.alloc();
            body!(ctx;
                BlobSliceLast(pk_blob_reg, key_blob_reg, 8);
                DecodeU64Key(pk_reg, pk_blob_reg);
                GoTo(cont.on_tuple)
            );
            let index_next = ctx.body_emitter.label_here();
            body!(ctx;
                MoveCursor(index_cursor_reg, MoveOperation::Next);
                GoTo(index_check)
            );
            (output_regs, index_next)
        }
        Some(cols) => {
            // Covering path: decode column values directly from the index key.
            // Allocate one register per index column position (up to max(cols)+1).
            let num_to_decode = cols.iter().copied().max().map(|m| m + 1).unwrap_or(0);
            let col_regs: Vec<Reg> = ctx.registers.alloc_block(num_to_decode);
            ctx.body_emitter.emit(Operation::DecodeIndexColumns {
                dest: col_regs.clone(),
                src: key_blob_reg,
            });
            body!(ctx; GoTo(cont.on_tuple));
            // Assemble output in the order cols specifies.
            let covering_output: Vec<Reg> = cols.iter().map(|&j| col_regs[j]).collect();
            let index_next = ctx.body_emitter.label_here();
            body!(ctx;
                MoveCursor(index_cursor_reg, MoveOperation::Next);
                GoTo(index_check)
            );
            (covering_output, index_next)
        }
    };

    NodeOutput {
        next: index_next,
        output_regs: final_output_regs,
        reset: None,
    }
}

/// Generate bytecode for an IndexProbe node.
///
/// Dynamic-key counterpart to IndexScan. For each reset (called by the enclosing
/// NestedLoop join once per left row), evaluates `key_expr` against the left row's
/// registers (via `ctx.outer_regs`), probes the index at that key, and yields
/// matching rowids. Column fetching is delegated to a RowidLookup node above.
///
/// `ctx.outer_regs` holds the left row's registers, already populated by the
/// enclosing left loop. The reset label re-evaluates `key_expr` and re-probes
/// the index for the current left row. Only rowids are yielded.
pub fn codegen_index_probe(
    index_rootpage: u32,
    key_expr: &PlanExpr,
    _index_col_idx: usize,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    let outer_regs = ctx
        .outer_regs
        .clone()
        .expect("codegen_index_probe: ctx.outer_regs must be set by enclosing NestedLoop join");

    let index_cursor_reg = ctx.registers.alloc();
    let flag_reg = ctx.registers.alloc();
    let pk_reg = ctx.registers.alloc();
    // key_reg is allocated once and reused across resets
    let key_reg = ctx.registers.alloc();

    // INIT: Open index cursor (no positioning — reset does that)
    init!(ctx; Open(index_cursor_reg, index_rootpage));

    // BODY:
    // RESET: jumped to by NestedLoop join once per left row. Evaluates key_expr
    // against outer_regs (left row) and positions the index cursor at the probe key.
    let reset_label = ctx.body_emitter.label_here();
    let key_compiled = compile_expr(
        key_expr,
        &outer_regs,
        &mut ExprContext {
            emitter: &mut ctx.body_emitter,
            registers: &mut ctx.registers,
        },
    );
    body!(ctx;
        CopyValue(key_reg, key_compiled);
        EncodeIndexKey(key_reg, key_reg);
        MoveCursor(index_cursor_reg, MoveOperation::Find(key_reg))
    );

    // CHECK: entered from reset (fall-through) and from INDEX_NEXT
    let check_label = ctx.body_emitter.label_here();
    let key_blob_reg = ctx.registers.alloc();
    let matches_reg = ctx.registers.alloc();
    let pk_blob_reg = ctx.registers.alloc();
    body!(ctx;
        CanReadCursor(flag_reg, index_cursor_reg);
        GoToIfFalse(cont.on_done, flag_reg);
        ReadCurrentKey(key_blob_reg, index_cursor_reg);
        BlobStartsWith(matches_reg, key_blob_reg, key_reg);
        GoToIfFalse(cont.on_done, matches_reg);
        BlobSliceLast(pk_blob_reg, key_blob_reg, 8);
        DecodeU64Key(pk_reg, pk_blob_reg);
        GoTo(cont.on_tuple)
    );

    // INDEX_NEXT: advance cursor, loop back to CHECK
    let index_next = ctx.body_emitter.label_here();
    body!(ctx;
        MoveCursor(index_cursor_reg, MoveOperation::Next);
        GoTo(check_label)
    );

    NodeOutput {
        next: index_next,
        reset: Some(reset_label),
        output_regs: vec![pk_reg],
    }
}

/// Generate bytecode for a RowidLookup node.
///
/// Pulls rowids from its child (typically IndexScan), uses each to seek
/// the table B-tree, and yields the requested columns as a full row.
pub fn codegen_rowid_lookup(
    input: &LogicalPlan,
    table_rootpage: u32,
    columns: &[usize],
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // columns uses scan-space: 0 = B-tree key, k>0 = CBOR body slot k-1.
    let table_cursor_reg = ctx.registers.alloc();

    let needs_key = columns.contains(&0);
    let cbor_slot_max = columns.iter().filter(|&&k| k > 0).map(|&k| k - 1).max();
    let num_cbor = cbor_slot_max.map(|m| m + 1).unwrap_or(0);
    let cbor_regs = ctx.registers.alloc_block(num_cbor);
    let key_reg = if needs_key {
        Some(ctx.registers.alloc())
    } else {
        None
    };
    let output_regs: Vec<Reg> = columns
        .iter()
        .map(|&col| {
            if col == 0 {
                key_reg.unwrap()
            } else {
                cbor_regs[col - 1]
            }
        })
        .collect();

    // INIT: Open the table cursor
    init!(ctx; Open(table_cursor_reg, table_rootpage));

    // Wire: child's on_tuple → our lookup logic
    let lookup = ctx.body_emitter.create_label();
    let child_cont = NodeContinuation {
        on_tuple: lookup,
        on_done: cont.on_done,
    };

    let child_output = codegen(input, &child_cont, ctx);

    // LOOKUP: child yielded a rowid in child_output.output_regs[0]
    ctx.body_emitter.bind_here(lookup);
    let rowid_reg = child_output.output_regs[0];
    body!(ctx;
        MoveCursor(table_cursor_reg, MoveOperation::Find(rowid_reg))
    );
    if !cbor_regs.is_empty() {
        body!(ctx; ReadCursor(cbor_regs, table_cursor_reg));
    }
    if let Some(kr) = key_reg {
        body!(ctx; ReadKey(kr, table_cursor_reg));
    }
    body!(ctx; GoTo(cont.on_tuple));

    NodeOutput {
        next: child_output.next,
        reset: child_output.reset,
        output_regs,
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
    ctx.body_emitter.bind_here(project_compute);

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
    body!(ctx; GoTo(cont.on_tuple));

    // Return: delegate to child for next, propagate reset, with new output registers
    NodeOutput {
        next: child_output.next,
        reset: child_output.reset, // propagate child's reset label unchanged
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
    init!(ctx;
        StoreValue(counter_reg, ScalarValue::Integer(count as i64));
        StoreValue(zero_reg, ScalarValue::Integer(0))
    );

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
    ctx.body_emitter.bind_here(limit_check);
    body!(ctx;
        GoToIfEqualValue(cont.on_done, counter_reg, zero_reg);
        DecrementValue(counter_reg);
        GoTo(cont.on_tuple)
    );

    // Return: delegate to child for next, pass through output registers
    NodeOutput {
        next: child_output.next,
        output_regs: child_output.output_regs,
        reset: None,
    }
}

/// Generate bytecode for a Distinct node.
///
/// Distinct collects all rows into a GroupTable keyed on all output columns
/// (with empty aggregate specs). The BTreeMap naturally deduplicates.
/// Then yields each unique row.
///
/// ```text
/// INIT:
///   InitGroupTable(table)
///
/// BODY:
///   ... child code ...
///   collect_row:
///     UpdateGroup(table, child_output_regs, [])
///     GoTo(child.next)
///   yield_from_groups:
///     YieldFromGroupTable(output_regs, table, on_done)
///     GoTo(on_tuple)
///   distinct_next:
///     GoTo(yield_from_groups)
/// ```
pub fn codegen_distinct(
    input: &LogicalPlan,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    use crate::engine::program::Operation;

    // Allocate register for group table
    let table_reg = ctx.registers.alloc();

    // INIT: initialize empty group table
    init!(ctx; InitGroupTable(table_reg));

    // Create labels
    let collect_row = ctx.body_emitter.create_label();
    let yield_from_groups = ctx.body_emitter.create_label();

    // Child's on_tuple → collect_row, on_done → yield_from_groups
    let child_cont = NodeContinuation {
        on_tuple: collect_row,
        on_done: yield_from_groups,
    };

    // Compile child
    let child_output = codegen(input, &child_cont, ctx);

    // collect_row: insert row into group table (deduplicates automatically)
    body!(ctx;
        Bind(collect_row);
        UpdateGroup(table_reg, child_output.output_regs.clone(), vec![]);
        GoTo(child_output.next);
        Bind(yield_from_groups)
    );

    let num_outputs = child_output.output_regs.len();
    let output_regs: Vec<Reg> = (0..num_outputs).map(|_| ctx.registers.alloc()).collect();

    ctx.body_emitter.emit(Operation::YieldFromGroupTable(
        output_regs.clone(),
        table_reg,
        JumpTarget::Unresolved(cont.on_done),
    ));
    body!(ctx; GoTo(cont.on_tuple));

    let distinct_next = ctx.body_emitter.label_here();
    body!(ctx; GoTo(yield_from_groups));

    NodeOutput {
        next: distinct_next,
        output_regs,
        reset: None,
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
///     RewindRowBuffer(buffer)
///   yield_loop:
///     NextFromRowBuffer(child_output_regs, buffer, on_done)
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
    init!(ctx; InitRowBuffer(buffer_reg));

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
    body!(ctx;
        Bind(collect_row);
        AppendToRowBuffer(buffer_reg, child_output.output_regs.clone());
        GoTo(child_output.next)
    );

    // sort_and_yield: sort the buffer, then fall through to yield loop
    ctx.body_emitter.bind_here(sort_and_yield);

    // Convert PlanExpr sort keys to column-index-based sort keys
    // For now, we assume sort expressions are simple column references
    let sort_key_specs: Vec<program::SortKeySpec> = sort_keys
        .iter()
        .map(|key| {
            // Extract column index from PlanExpr
            let column_idx = match &key.expr {
                crate::planner::PlanExpr::ColumnRef(column_idx) => *column_idx,
                _ => panic!("ORDER BY only supports column references for now"),
            };
            program::SortKeySpec {
                column_index: column_idx,
                descending: key.descending,
            }
        })
        .collect();

    body!(ctx;
        SortRowBuffer(buffer_reg, sort_key_specs);
        RewindRowBuffer(buffer_reg);
        Bind(yield_loop)
    );
    ctx.body_emitter.emit(Operation::NextFromRowBuffer(
        child_output.output_regs.clone(),
        buffer_reg,
        JumpTarget::Unresolved(cont.on_done),
    ));
    body!(ctx; GoTo(cont.on_tuple));

    // sort_next: after parent processes tuple, yield next row
    let sort_next = ctx.body_emitter.label_here();
    body!(ctx; GoTo(yield_loop));

    NodeOutput {
        next: sort_next,
        output_regs: child_output.output_regs,
        reset: None,
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
    having: Option<&crate::planner::PlanExpr>,
    input: &LogicalPlan,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    use crate::engine::program::AggregateOp;
    use crate::planner::AggregateFunction;

    // Allocate register for group table
    let table_reg = ctx.registers.alloc();

    // INIT: initialize empty group table
    init!(ctx; InitGroupTable(table_reg));

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
    ctx.body_emitter.bind_here(update_group);

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

    body!(ctx;
        UpdateGroup(table_reg, key_regs.clone(), agg_specs);
        GoTo(child_output.next);
        Bind(yield_from_groups)
    );

    // Allocate output registers: group_keys + aggregates
    let num_outputs = group_keys.len() + aggregates.len();
    let output_regs: Vec<Reg> = (0..num_outputs).map(|_| ctx.registers.alloc()).collect();

    ctx.body_emitter.emit(Operation::YieldFromGroupTable(
        output_regs.clone(),
        table_reg,
        JumpTarget::Unresolved(cont.on_done),
    ));

    // If HAVING predicate is present, evaluate it and skip groups that don't pass
    if let Some(pred) = having {
        let mut expr_ctx = ExprContext {
            emitter: &mut ctx.body_emitter,
            registers: &mut ctx.registers,
        };
        let cond_reg = compile_expr(pred, &output_regs, &mut expr_ctx);
        body!(ctx; GoToIfFalse(yield_from_groups, cond_reg));
    }

    body!(ctx; GoTo(cont.on_tuple));

    // agg_next: after parent processes tuple, yield next group
    let agg_next = ctx.body_emitter.label_here();
    body!(ctx; GoTo(yield_from_groups));

    NodeOutput {
        next: agg_next,
        output_regs,
        reset: None,
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
struct IndexWithCursor {
    info: crate::planner::IndexMaintenanceInfo,
    cursor_reg: Reg,
}

fn open_index_cursors(
    indexes: &[crate::planner::IndexMaintenanceInfo],
    ctx: &mut CodegenContext,
) -> Vec<IndexWithCursor> {
    indexes
        .iter()
        .map(|index| {
            let cursor_reg = ctx.registers.alloc();
            ctx.init_emitter
                .emit(Operation::Open(cursor_reg, index.rootpage));
            IndexWithCursor {
                info: index.clone(),
                cursor_reg,
            }
        })
        .collect()
}

/// Emit WriteIndex for all indexes. Passes the index's `unique` flag so the engine
/// enforces uniqueness constraints inline. Used by INSERT (called before WriteCursor).
fn emit_index_writes(
    index_cursors: &[IndexWithCursor],
    row_regs: &[Reg],
    key_reg: Reg,
    ctx: &mut CodegenContext,
) {
    for ic in index_cursors {
        let col_regs: Vec<Reg> = ic.info.column_idxs.iter().map(|&c| row_regs[c]).collect();
        ctx.body_emitter.emit(Operation::WriteIndex(
            ic.cursor_reg,
            col_regs,
            key_reg,
            ic.info.unique,
        ));
    }
}

/// Emit WriteIndex(unique=false) for all indexes. Used by UPDATE.
/// TODO(phase-az): UPDATE should enforce unique constraints on the new values but
/// currently does not. Pass `ic.info.unique` once UPDATE uniqueness checking is implemented.
fn emit_write_indexes(
    index_cursors: &[IndexWithCursor],
    row_regs: &[Reg],
    key_reg: Reg,
    ctx: &mut CodegenContext,
) {
    for ic in index_cursors {
        let col_regs: Vec<Reg> = ic.info.column_idxs.iter().map(|&c| row_regs[c]).collect();
        ctx.body_emitter.emit(Operation::WriteIndex(
            ic.cursor_reg,
            col_regs,
            key_reg,
            false,
        ));
    }
}

fn emit_delete_indexes(
    index_cursors: &[IndexWithCursor],
    row_regs: &[Reg],
    key_reg: Reg,
    ctx: &mut CodegenContext,
) {
    for ic in index_cursors {
        let col_regs: Vec<Reg> = ic.info.column_idxs.iter().map(|&c| row_regs[c]).collect();
        ctx.body_emitter
            .emit(Operation::DeleteIndex(ic.cursor_reg, col_regs, key_reg));
    }
}

pub fn codegen_insert(
    rootpage: u32,
    table_columns: &[usize],
    input: &LogicalPlan,
    indexes: &[crate::planner::IndexMaintenanceInfo],
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // `table_columns` uses insert-space encoding:
    //   index 0   → B-tree key (explicit PK value)
    //   index k>0 → CBOR slot k-1
    //
    // If 0 is present in table_columns: the user supplied the key.
    //   - Use value at insert-space slot 0 as the key.
    //   - WriteCursor with unique=true enforces uniqueness without a separate index.
    //   - No IncrementValue (key is user-supplied, not from the rowid cache).
    //
    // If 0 is absent: auto-assign the key from the rowid cache.
    //   - InitRowid → WriteCursor(unique=false) → IncrementValue.
    let has_user_key = table_columns.contains(&0);

    let cursor_reg = ctx.registers.alloc();
    let key_reg = ctx.registers.alloc();
    let counter_reg = ctx.registers.alloc();

    let index_cursor_regs = open_index_cursors(indexes, ctx);

    // INIT: open cursor; if auto-assigning, initialise the rowid cache.
    init!(ctx; Open(cursor_reg, rootpage));
    if !has_user_key {
        init!(ctx; InitRowid(cursor_reg, key_reg));
    }
    init!(ctx; StoreValue(counter_reg, ScalarValue::Integer(0)));

    let child_on_tuple = ctx.body_emitter.create_label();
    let child_on_done = ctx.body_emitter.create_label();
    let child_cont = NodeContinuation {
        on_tuple: child_on_tuple,
        on_done: child_on_done,
    };

    let child_output = codegen(input, &child_cont, ctx);

    ctx.body_emitter.bind_here(child_on_tuple);

    // Reorder child outputs into insert-space: alloc_block(max+1), copy each
    // child value to its insert-space slot.  Slot 0 is the key (if supplied);
    // slots k>0 are CBOR values.  Unused slot 0 is never read when has_user_key=false.
    let num_slots = table_columns.iter().max().map(|&m| m + 1).unwrap_or(0);
    let reordered = ctx.registers.alloc_block(num_slots);
    for (i, &slot) in table_columns.iter().enumerate() {
        body!(ctx; CopyValue(reordered[slot], child_output.output_regs[i]));
    }

    // Extract key and CBOR value registers.
    let cbor_regs: Vec<Reg> = reordered.iter().skip(1).copied().collect();

    if has_user_key {
        // User-supplied PK: extract from slot 0, enforce uniqueness via WriteCursor.
        body!(ctx; CopyValue(key_reg, reordered[0]));
        // Index writes first so a uniqueness violation aborts before any table write.
        // TODO(phase-az): roll back partial index writes on uniqueness failure.
        emit_index_writes(&index_cursor_regs, &reordered, key_reg, ctx);
        body!(ctx; WriteCursor(cursor_reg, key_reg, cbor_regs.clone(), true));
        body!(ctx;
            IncrementValue(counter_reg);
            GoTo(child_output.next);
            Bind(child_on_done);
            GoTo(cont.on_tuple)
        );
    } else {
        // Auto-assign key from rowid cache.
        emit_index_writes(&index_cursor_regs, &reordered, key_reg, ctx);
        body!(ctx; WriteCursor(cursor_reg, key_reg, cbor_regs.clone(), false));
        body!(ctx;
            IncrementValue(key_reg);
            IncrementValue(counter_reg);
            GoTo(child_output.next);
            Bind(child_on_done);
            GoTo(cont.on_tuple)
        );
    }

    let insert_next = ctx.body_emitter.label_here();
    body!(ctx; GoTo(cont.on_done));

    NodeOutput {
        next: insert_next,
        output_regs: vec![counter_reg],
        reset: None,
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
    indexes: &[crate::planner::IndexMaintenanceInfo],
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

    // Open index cursors in the init phase (one per secondary index)
    let index_cursor_regs = open_index_cursors(indexes, ctx);

    // INIT: open cursor, position to first, init key buffer and counter
    init!(ctx;
        Open(cursor_reg, rootpage);
        MoveCursor(cursor_reg, MoveOperation::First);
        InitRowBuffer(key_list_reg);
        StoreValue(counter_reg, ScalarValue::Integer(0))
    );

    // PHASE 1: Collect keys
    let phase2_start = ctx.body_emitter.create_label();
    let collect_start = ctx.body_emitter.label_here();
    body!(ctx;
        CanReadCursor(flag_reg, cursor_reg);
        GoToIfFalse(phase2_start, flag_reg);
        ReadCursor(read_regs.clone(), cursor_reg)
    );

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
        body!(ctx;
            GoToIfFalse(skip_label, filter_reg);
            ReadKey(key_reg, cursor_reg);
            AppendToRowBuffer(key_list_reg, vec![key_reg]);
            IncrementValue(counter_reg);
            Bind(skip_label)
        );
    } else {
        // No filter: collect all keys
        body!(ctx;
            ReadKey(key_reg, cursor_reg);
            AppendToRowBuffer(key_list_reg, vec![key_reg]);
            IncrementValue(counter_reg)
        );
    }

    body!(ctx;
        MoveCursor(cursor_reg, MoveOperation::Next);
        GoTo(collect_start)
    );

    // PHASE 2: Update collected keys
    let update_loop = ctx.body_emitter.create_label();
    let update_done = ctx.body_emitter.create_label();

    body!(ctx;
        Bind(phase2_start);
        RewindRowBuffer(key_list_reg);
        Bind(update_loop)
    );

    // Advance to next key, or jump to done if buffer is exhausted
    ctx.body_emitter.emit(Operation::NextFromRowBuffer(
        vec![key_reg],
        key_list_reg,
        JumpTarget::Unresolved(update_done),
    ));

    // Seek to this key and re-read row values
    body!(ctx;
        MoveCursor(cursor_reg, MoveOperation::Find(key_reg));
        ReadCursor(read_regs.clone(), cursor_reg)
    );

    // Delete stale index entries (old column values, before applying assignments)
    emit_delete_indexes(&index_cursor_regs, &read_regs, key_reg, ctx);

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
        body!(ctx; CopyValue(new_values[*col_idx], value_reg));
    }

    // Write updated index entries (new column values, after applying assignments)
    emit_write_indexes(&index_cursor_regs, &new_values, key_reg, ctx);

    body!(ctx;
        WriteCursor(cursor_reg, key_reg, new_values, false);
        GoTo(update_loop);
        Bind(update_done);
        GoTo(cont.on_tuple)
    );

    // After yielding, halt
    let after_yield = ctx.body_emitter.label_here();
    body!(ctx; GoTo(cont.on_done));

    NodeOutput {
        next: after_yield,
        output_regs: vec![counter_reg],
        reset: None,
    }
}

/// Generate bytecode for a Materialize node.
///
/// Fills a RowBuffer with all rows from `input` (fill phase runs first in the body),
/// then yields them row-by-row. Sets `reset` so a parent join can rewind and re-iterate
/// the buffer without re-running the fill.
///
/// ```text
/// INIT (init_emitter):
///   InitRowBuffer(r_buf)
///   <inner plan init>
///
/// BODY (body_emitter):
///   <inner plan body — drives fill>
///
///   collect_row:
///     AppendToRowBuffer(r_buf, inner_regs)
///     GoTo(inner.next)
///
///   fill_done / reset_label:   ← both parent and fill_done land here
///     RewindRowBuffer(r_buf)
///
///   yield_next:
///     NextFromRowBuffer(output_regs, r_buf, on_done)
///     GoTo(on_tuple)
///
///   mat_next:                  ← NodeOutput.next
///     GoTo(yield_next)
/// ```
/// Generate bytecode for a Materialize node.
///
/// The fill loop runs entirely in the **init section** (once, before the body loop starts).
/// The body section contains only the yield loop. This means:
///
/// - `NodeOutput.reset` is always valid: calling it rewinds the buffer and re-iterates
///   from the first row without re-running the fill.
/// - Hash and semi joins can compile the left child first (body starts at the left scan)
///   and use `right_output.reset` per left row without any special "fill gate" logic.
///
/// ```text
/// INIT:
///   InitRowBuffer(buffer)
///   <inner plan init: Open(cursor), MoveCursor(First), ...>
///   <inner plan body loop: CanReadCursor → collect_row: AppendToRowBuffer → GoTo(next) ...>
///   collect_row:
///     AppendToRowBuffer(buffer, child_regs)
///     GoTo(child_output.next)
///   fill_done:
///     RewindRowBuffer(buffer)
///   ; GoTo(body_start) inserted by finalize()
///
/// BODY:
///   reset_label:           ← NodeOutput.reset
///     RewindRowBuffer(buffer)
///   yield_next:
///     NextFromRowBuffer(output_regs, buffer) → cont.on_done
///     GoTo(cont.on_tuple)
///   mat_next:              ← NodeOutput.next
///     GoTo(yield_next)
/// ```
pub fn codegen_materialize(
    input: &LogicalPlan,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    use crate::engine::program::Operation;

    let buffer_reg = ctx.registers.alloc();
    init!(ctx; InitRowBuffer(buffer_reg));

    // Create the fill-callback labels in a temporary emitter that will be
    // absorbed into the init section. This keeps the fill loop entirely in init.
    let mut fill_emitter = BytecodeEmitter::new();
    let collect_row = fill_emitter.create_label(); // Label(0) in fill_emitter
    let fill_done = fill_emitter.create_label(); // Label(1) in fill_emitter

    // Redirect body emissions to fill_emitter so the inner plan's loop goes into init.
    let real_body = std::mem::replace(&mut ctx.body_emitter, fill_emitter);
    let child_cont = NodeContinuation {
        on_tuple: collect_row,
        on_done: fill_done,
    };
    let child_output = codegen(input, &child_cont, ctx);
    let fill_emitter = std::mem::replace(&mut ctx.body_emitter, real_body);

    // Absorb fill_emitter into init_emitter (remapping label IDs).
    let label_id_offset = ctx.init_emitter.absorb(fill_emitter);

    // After absorb, labels from fill_emitter are now in init_emitter with remapped IDs.
    let collect_row_in_init = Label(collect_row.0 + label_id_offset);
    let fill_done_in_init = Label(fill_done.0 + label_id_offset);
    let child_next_in_init = Label(child_output.next.0 + label_id_offset);

    // Bind collect_row: append row to buffer and request next child row.
    ctx.init_emitter.bind_here(collect_row_in_init);
    ctx.init_emitter.emit(Operation::AppendToRowBuffer(
        buffer_reg,
        child_output.output_regs.clone(),
    ));
    ctx.init_emitter
        .emit(Operation::GoTo(JumpTarget::Unresolved(child_next_in_init)));

    // Bind fill_done: rewind buffer so the yield loop starts from the first row.
    ctx.init_emitter.bind_here(fill_done_in_init);
    ctx.init_emitter
        .emit(Operation::RewindRowBuffer(buffer_reg));
    // finalize() will append GoTo(body_start) here automatically.

    // Body: yield loop only (fill is complete by the time body starts).
    // Fresh output registers — reads from buffer, not from child cursor.
    let output_regs: Vec<Reg> = (0..child_output.output_regs.len())
        .map(|_| ctx.registers.alloc())
        .collect();

    // reset_label: rewind buffer and restart iteration (used by join per left row).
    let reset_label = ctx.body_emitter.label_here();
    body!(ctx; RewindRowBuffer(buffer_reg));

    let yield_next = ctx.body_emitter.label_here();
    ctx.body_emitter.emit(Operation::NextFromRowBuffer(
        output_regs.clone(),
        buffer_reg,
        JumpTarget::Unresolved(cont.on_done),
    ));
    body!(ctx; GoTo(cont.on_tuple));

    let mat_next = ctx.body_emitter.label_here();
    body!(ctx; GoTo(yield_next));

    NodeOutput {
        next: mat_next,
        reset: Some(reset_label),
        output_regs,
    }
}

/// Generate bytecode for a Hash Join node (strategy = Hash).
///
/// The right child MUST be a `Materialize` node (the optimizer wraps it). Because
/// `codegen_materialize` runs the fill in the **init section**, the buffer is fully
/// populated before the body loop starts. The body therefore starts at the left scan,
/// and per left row calls `right_output.reset` to rewind and re-iterate the right buffer.
///
/// ```text
/// INIT:
///   <left child init: Open(left_cursor), MoveCursor(First)>
///   InitRowBuffer(right_buf)
///   <right child (Materialize) init+fill: Open(right_cursor), fill loop, RewindRowBuffer>
///
/// GoTo(body_start)
///
/// BODY (body_start = left scan's check label):
///   LEFT_CHECK:
///     <left scan body>
///     → on_tuple: LEFT_ON_TUPLE
///     → on_done: cont.on_done
///
///   RESET_LABEL (right_output.reset):  ← Materialize rewind entry
///     RewindRowBuffer(right_buf)
///   YIELD_NEXT:
///     NextFromRowBuffer(right_regs, right_buf) → left_output.next
///     GoTo(INNER_CHECK)
///
///   LEFT_ON_TUPLE:
///     GoTo(RESET_LABEL)               ← rewind right buffer, get first right row
///
///   INNER_CHECK:
///     <evaluate ON condition>
///     GoToIfFalse(right_output.next)  → no match: get next right row
///     GoTo(cont.on_tuple)             → match! emit combined row
///
///   JOIN_NEXT:                        ← NodeOutput.next (parent requests next)
///     GoTo(right_output.next)         → advance right cursor
/// ```
pub fn codegen_join(
    left: &LogicalPlan,
    right: &LogicalPlan,
    on_condition: &PlanExpr,
    _left_column_count: usize,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // Compile left child first — body starts at the left scan (buffer already filled).
    let left_on_tuple = ctx.body_emitter.create_label();
    let left_cont = NodeContinuation {
        on_tuple: left_on_tuple,
        on_done: cont.on_done,
    };
    let left_output = codegen(left, &left_cont, ctx);

    // Compile right child (must be Materialize — the optimizer ensures this).
    // Materialize fills the buffer in init and provides reset + next for per-row iteration.
    let inner_check = ctx.body_emitter.create_label();
    let right_cont = NodeContinuation {
        on_tuple: inner_check,     // after reset: yield right row here
        on_done: left_output.next, // right buffer exhausted → advance left
    };
    let right_output = codegen(right, &right_cont, ctx);
    let right_reset = right_output
        .reset
        .expect("codegen_join(Hash): right child must be Materialize (provides reset)");

    // Combined output: left columns then right columns.
    let mut combined_output = left_output.output_regs.clone();
    combined_output.extend(right_output.output_regs.clone());

    // left_on_tuple: per left row, rewind right buffer and iterate it.
    body!(ctx;
        Bind(left_on_tuple);
        GoTo(right_reset)
    );

    // inner_check: evaluate ON condition; emit row on match, advance right on miss.
    ctx.body_emitter.bind_here(inner_check);
    let pred_reg = compile_expr(
        on_condition,
        &combined_output,
        &mut ExprContext {
            emitter: &mut ctx.body_emitter,
            registers: &mut ctx.registers,
        },
    );
    body!(ctx;
        GoToIfFalse(right_output.next, pred_reg);
        GoTo(cont.on_tuple)
    );

    // JOIN_NEXT: parent calls this to advance to the next matching right row.
    let join_next = ctx.body_emitter.label_here();
    body!(ctx; GoTo(right_output.next));

    NodeOutput {
        next: join_next,
        output_regs: combined_output,
        reset: None,
    }
}

/// Generate bytecode for a NestedLoop Join node.
///
/// For each left row, resets the right child (via its reset label) and iterates
/// all right rows, emitting combined rows where on_condition is true.
///
/// Two couplings between the join and its right subtree:
/// 1. `reset: Option<Label>` on NodeOutput — the right child must provide a reset
///    entry point. Pass-through nodes propagate it; materializing nodes do not
///    support it (we panic). See phase-ac plan "Reset contract".
/// 2. `ctx.outer_regs` — set to the left output registers before compiling the
///    right subtree so that IndexProbe.key_expr can reference them via ColumnRef.
///    Cleared immediately after. See phase-ac plan "Codegen context and coupling".
///
/// The left-column-free zone invariant: no expression inside the right subtree
/// (except IndexProbe.key_expr) may reference left columns. Cross-column
/// conditions belong in on_condition.
pub fn codegen_join_nested_loop(
    left: &LogicalPlan,
    right: &LogicalPlan,
    on_condition: &PlanExpr,
    _left_column_count: usize,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // --- Compile left child ---
    let left_on_tuple = ctx.body_emitter.create_label();
    let left_cont = NodeContinuation {
        on_tuple: left_on_tuple,
        on_done: cont.on_done,
    };
    let left_output = codegen(left, &left_cont, ctx);

    // Set outer_regs so the right subtree (IndexProbe) can reference left registers.
    ctx.outer_regs = Some(left_output.output_regs.clone());

    // --- Compile right child ---
    let right_on_tuple = ctx.body_emitter.create_label();
    let right_on_done = ctx.body_emitter.create_label();
    let right_cont = NodeContinuation {
        on_tuple: right_on_tuple,
        on_done: right_on_done,
    };
    let right_output = codegen(right, &right_cont, ctx);

    // Clear outer_regs — must be None outside of nested-loop right subtree compilation.
    ctx.outer_regs = None;

    let right_reset = right_output.reset.expect(
        "NestedLoop join: right child does not support reset. \
         Materializing nodes (Sort, Aggregate, Join(Hash)) cannot be right children. \
         See phase-ac plan 'Reset contract'.",
    );

    // Combined output: left columns then right columns
    let mut combined_output = left_output.output_regs.clone();
    combined_output.extend(right_output.output_regs.clone());

    // Entry: start with the first left row
    body!(ctx;
        GoTo(left_output.next)
    );

    // left_on_tuple: got a new left row — restart the right child
    body!(ctx;
        Bind(left_on_tuple);
        GoTo(right_reset)
    );

    // right_on_done: right side exhausted — advance left
    body!(ctx;
        Bind(right_on_done);
        GoTo(left_output.next)
    );

    // right_on_tuple: right child yielded a row — evaluate ON condition
    let check_label = ctx.body_emitter.label_here();
    body!(ctx; Bind(right_on_tuple));
    let pred_reg = compile_expr(
        on_condition,
        &combined_output,
        &mut ExprContext {
            emitter: &mut ctx.body_emitter,
            registers: &mut ctx.registers,
        },
    );
    body!(ctx;
        GoToIfFalse(right_output.next, pred_reg);
        GoTo(cont.on_tuple)
    );

    // JOIN_NEXT: parent requests next row — advance right child
    let join_next = ctx.body_emitter.label_here();
    body!(ctx; GoTo(right_output.next));

    let _ = check_label; // used for doc clarity

    NodeOutput {
        next: join_next,
        reset: None,
        output_regs: combined_output,
    }
}

/// Generate bytecode for a semi-join (IN / NOT IN subquery).
///
/// The right side must be a Materialize node. The buffer is filled during INIT.
/// For each left row: rewind right buffer, iterate rows, exit on first match (semi)
/// or on exhaustion without match (anti-semi). Only left columns are yielded.
pub fn codegen_join_semi(
    left: &LogicalPlan,
    right: &LogicalPlan,
    on_condition: &PlanExpr,
    left_column_count: usize,
    negated: bool,
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // BODY entry point labels for the right Materialize block.
    let left_on_tuple = ctx.body_emitter.create_label();
    let right_on_tuple = ctx.body_emitter.create_label();
    let right_done = ctx.body_emitter.create_label();

    // --- Compile left first ---
    let left_cont = NodeContinuation {
        on_tuple: left_on_tuple,
        on_done: cont.on_done,
    };
    let left_output = codegen(left, &left_cont, ctx);

    // --- Compile right (Materialize) ---
    // right_cont.on_tuple → RIGHT_ON_TUPLE, right_cont.on_done → RIGHT_DONE
    let right_cont = NodeContinuation {
        on_tuple: right_on_tuple,
        on_done: right_done,
    };
    let right_output = codegen(right, &right_cont, ctx);
    let right_reset = right_output
        .reset
        .expect("right of semi-join must be Materialize");

    // --- Build combined registers for on_condition evaluation ---
    let combined_regs: Vec<_> = left_output
        .output_regs
        .iter()
        .copied()
        .chain(right_output.output_regs.iter().copied())
        .collect();

    // LEFT_ON_TUPLE: rewind right buffer and start iterating
    ctx.body_emitter.bind_here(left_on_tuple);
    body!(ctx; GoTo(right_reset));

    // RIGHT_ON_TUPLE: evaluate on_condition over combined registers
    ctx.body_emitter.bind_here(right_on_tuple);
    let match_reg = {
        let mut expr_ctx = ExprContext {
            emitter: &mut ctx.body_emitter,
            registers: &mut ctx.registers,
        };
        compile_expr(on_condition, &combined_regs, &mut expr_ctx)
    };

    if !negated {
        // Semi: match → yield left row; no match → next right row
        body!(ctx;
            GoToIfFalse(right_output.next, match_reg);  // no match → try next right
            GoTo(cont.on_tuple)                         // match → yield
        );
    } else {
        // Anti-semi: match found → skip this left row entirely
        body!(ctx;
            GoToIfFalse(right_output.next, match_reg);  // no match → try next right
            GoTo(left_output.next)                      // match found → skip left row
        );
    }

    // RIGHT_DONE: right buffer exhausted for this left row
    ctx.body_emitter.bind_here(right_done);
    if !negated {
        // Semi: no match found → skip left row
        body!(ctx; GoTo(left_output.next));
    } else {
        // Anti-semi: no match found → yield left row
        body!(ctx; GoTo(cont.on_tuple));
    }

    // semi_next: parent requests next left row
    let semi_next = ctx.body_emitter.create_label();
    ctx.body_emitter.bind_here(semi_next);
    body!(ctx; GoTo(left_output.next));

    let _ = left_column_count; // used for documentation clarity

    NodeOutput {
        next: semi_next,
        reset: None,
        output_regs: left_output.output_regs,
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
    indexes: &[crate::planner::IndexMaintenanceInfo],
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

    // Open index cursors in the init phase (one per secondary index)
    let index_cursor_regs = open_index_cursors(indexes, ctx);

    // INIT: open cursor, position to first, init key buffer and counter
    init!(ctx;
        Open(cursor_reg, rootpage);
        MoveCursor(cursor_reg, MoveOperation::First);
        InitRowBuffer(key_list_reg);
        StoreValue(counter_reg, ScalarValue::Integer(0))
    );

    // PHASE 1: Collect keys
    let phase2_start = ctx.body_emitter.create_label();
    let collect_start = ctx.body_emitter.label_here();
    body!(ctx;
        CanReadCursor(flag_reg, cursor_reg);
        GoToIfFalse(phase2_start, flag_reg);
        ReadCursor(read_regs.clone(), cursor_reg)
    );

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
        body!(ctx;
            GoToIfFalse(skip_label, filter_reg);
            ReadKey(key_reg, cursor_reg);
            AppendToRowBuffer(key_list_reg, vec![key_reg]);
            IncrementValue(counter_reg);
            Bind(skip_label)
        );
    } else {
        // No filter: collect all keys
        body!(ctx;
            ReadKey(key_reg, cursor_reg);
            AppendToRowBuffer(key_list_reg, vec![key_reg]);
            IncrementValue(counter_reg)
        );
    }

    body!(ctx;
        MoveCursor(cursor_reg, MoveOperation::Next);
        GoTo(collect_start)
    );

    // PHASE 2: Delete collected keys
    let delete_loop = ctx.body_emitter.create_label();
    let delete_done = ctx.body_emitter.create_label();
    body!(ctx;
        Bind(phase2_start);
        RewindRowBuffer(key_list_reg);
        Bind(delete_loop)
    );

    // Advance to next key, or jump to done if buffer is exhausted
    ctx.body_emitter.emit(Operation::NextFromRowBuffer(
        vec![key_reg],
        key_list_reg,
        JumpTarget::Unresolved(delete_done),
    ));

    // Seek to this key and delete
    body!(ctx; MoveCursor(cursor_reg, MoveOperation::Find(key_reg)));

    // For each secondary index: read indexed column values, then delete the index entry
    if !indexes.is_empty() {
        let phase2_read_regs = ctx.registers.alloc_block(table_columns.len());
        body!(ctx; ReadCursor(phase2_read_regs.clone(), cursor_reg));
        emit_delete_indexes(&index_cursor_regs, &phase2_read_regs, key_reg, ctx);
    }

    body!(ctx;
        DeleteCursor(cursor_reg);
        GoTo(delete_loop);
        Bind(delete_done);
        GoTo(cont.on_tuple)
    );

    // After yielding, halt
    let after_yield = ctx.body_emitter.label_here();
    body!(ctx; GoTo(cont.on_done));

    NodeOutput {
        next: after_yield,
        output_regs: vec![counter_reg],
        reset: None,
    }
}

/// Generate bytecode for a PopulateIndex node.
///
/// Composes `codegen_scan(..., with_key=true)` (which reads the row key before
/// advancing) with a `WriteIndex` call in the tuple handler.
/// Produces no output rows.
///
/// ```text
/// INIT (from codegen_scan with_key=true):
///   Open(index_cursor, index_rootpage)
///   Open(table_cursor, table_rootpage)
///   MoveCursor(table_cursor, First)
///
/// BODY (from codegen_scan + on_tuple handler):
///   check: CanReadCursor(flag, table_cursor)
///          GotoIfFalse(on_done, flag)
///          ReadCursor([col_regs...], table_cursor)
///          ReadKey(pk_reg, table_cursor)     ← before advance
///          MoveCursor(table_cursor, Next)
///          GoTo(on_tuple)
///   on_tuple: WriteIndex(index_cursor, col_reg, pk_reg)
///             GoTo(check)
///   on_done:  GoTo(cont.on_done)
/// ```
pub fn codegen_populate_index(
    input: &LogicalPlan,
    index_rootpage: u32,
    column_idxs: &[usize],
    cont: &NodeContinuation,
    ctx: &mut CodegenContext,
) -> NodeOutput {
    // Open index cursor in INIT (before the child scan opens the table cursor)
    let index_cursor = ctx.registers.alloc();
    init!(ctx; Open(index_cursor, index_rootpage));

    // Set up continuation labels for the child node
    let child_on_tuple = ctx.body_emitter.create_label();
    let child_on_done = ctx.body_emitter.create_label();
    let child_cont = NodeContinuation {
        on_tuple: child_on_tuple,
        on_done: child_on_done,
    };

    // Compile the child (Scan with scan index 0 = key included).
    // output_regs are ordered by the scan's columns slice; index 0 = key register.
    // column_idxs hold scan-space indices; scan columns are [0,1,...,n] so
    // output_regs[scan_space_idx] gives the correct register directly.
    let scan_out = codegen(input, &child_cont, ctx);
    let col_regs: Vec<_> = column_idxs
        .iter()
        .map(|&idx| scan_out.output_regs[idx])
        .collect();
    let pk_reg = scan_out.output_regs[0]; // scan index 0 = B-tree key

    // child_on_tuple: write one index entry, then advance to next row
    body!(ctx;
        Bind(child_on_tuple);
        WriteIndex(index_cursor, col_regs, pk_reg, false);
        GoTo(scan_out.next);
        Bind(child_on_done);
        GoTo(cont.on_done)
    );

    NodeOutput {
        next: scan_out.next,
        output_regs: vec![],
        reset: None,
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
        LogicalPlan::IndexScan {
            index_rootpage,
            index_col_idx: _,
            lower_bound,
            upper_bound,
            output_columns,
        } => codegen_index_scan(
            *index_rootpage,
            lower_bound,
            upper_bound,
            output_columns.as_deref(),
            cont,
            ctx,
        ),
        LogicalPlan::RowidLookup {
            input,
            table_rootpage,
            columns,
        } => codegen_rowid_lookup(input, *table_rootpage, columns, cont, ctx),
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
            having,
        } => codegen_aggregate(group_keys, aggregates, having.as_ref(), input, cont, ctx),
        LogicalPlan::Insert {
            rootpage,
            table_columns,
            input,
            indexes,
        } => codegen_insert(*rootpage, table_columns, input, indexes, cont, ctx),
        LogicalPlan::Update {
            rootpage,
            table_columns,
            assignments,
            filter,
            indexes,
        } => codegen_update(
            *rootpage,
            table_columns,
            assignments,
            filter,
            indexes,
            cont,
            ctx,
        ),
        LogicalPlan::Delete {
            rootpage,
            table_columns,
            filter,
            indexes,
        } => codegen_delete(*rootpage, table_columns, filter, indexes, cont, ctx),
        LogicalPlan::Join {
            left,
            right,
            on_condition,
            strategy,
            left_column_count,
        } => match strategy {
            crate::planner::JoinStrategy::Hash => {
                codegen_join(left, right, on_condition, *left_column_count, cont, ctx)
            }
            crate::planner::JoinStrategy::NestedLoop => {
                codegen_join_nested_loop(left, right, on_condition, *left_column_count, cont, ctx)
            }
            crate::planner::JoinStrategy::Semi { negated } => codegen_join_semi(
                left,
                right,
                on_condition,
                *left_column_count,
                *negated,
                cont,
                ctx,
            ),
        },
        LogicalPlan::IndexProbe {
            index_rootpage,
            key_expr,
            index_col_idx,
        } => codegen_index_probe(*index_rootpage, key_expr, *index_col_idx, cont, ctx),
        LogicalPlan::Distinct { input } => codegen_distinct(input, cont, ctx),
        LogicalPlan::PopulateIndex {
            input,
            index_rootpage,
            column_idxs,
        } => codegen_populate_index(input, *index_rootpage, column_idxs, cont, ctx),
        LogicalPlan::Materialize { input } => codegen_materialize(input, cont, ctx),
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
    body!(ctx;
        Bind(on_tuple);
        Yield(output.output_regs.clone());
        GoTo(output.next);
        Bind(on_done);
        Halt
    );

    let num_registers = ctx.registers.count();
    let ops = ctx.finalize();

    (ops, num_registers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::scalarvalue::ScalarValue;
    use crate::engine::Engine;
    use crate::planner::{BinaryOp, PlanExpr};
    use crate::test::TestDb;
    use crate::{body, init};

    #[test]
    fn test_body_macro_basic() {
        let mut ctx = CodegenContext::new();
        let on_done = ctx.body_emitter.create_label();
        let r0 = ctx.registers.alloc();
        let r1 = ctx.registers.alloc();
        init!(ctx; StoreValue(r0, ScalarValue::Integer(1)));
        body!(ctx;
            GoToIfFalse(on_done, r0);
            StoreValue(r1, ScalarValue::Integer(42));
            Bind(on_done)
        );
        // init: 1 op; body: GoToIfFalse + StoreValue (Bind adds no op)
        // finalize combines: init_ops + GoTo(body_start) + body_ops
        let ops = ctx.finalize();
        // init: StoreValue(r0) = 1 op
        // GoTo(body) = 1 op
        // body: GoToIfFalse + StoreValue = 2 ops
        assert_eq!(ops.len(), 4);
    }

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
            let mut c = cursor.open_cursor();
            c.insert_u64(0, vec![ScalarValue::Integer(1), ScalarValue::Integer(100)]);
            c.insert_u64(1, vec![ScalarValue::Integer(2), ScalarValue::Integer(200)]);
            c.insert_u64(2, vec![ScalarValue::Integer(3), ScalarValue::Integer(300)]);
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
        let mut engine = Engine::with_program(&ops, num_registers, &btree);
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
        let mut engine = Engine::with_program(&ops, num_registers, &btree);
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
            let mut c = cursor.open_cursor();
            c.insert_u64(0, vec![ScalarValue::Integer(10), ScalarValue::Integer(20)]);
            c.insert_u64(1, vec![ScalarValue::Integer(30), ScalarValue::Integer(40)]);
        }

        // Scan-space: col 1 = CBOR slot 0, col 2 = CBOR slot 1
        let plan = LogicalPlan::Scan {
            rootpage: root,
            columns: vec![1, 2],
        };

        let (ops, num_registers) = compile_plan(&plan);

        // Run through engine
        let mut engine = Engine::with_program(&ops, num_registers, &btree);
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

        let mut engine = Engine::with_program(&ops, num_registers, &btree);
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

        let mut engine = Engine::with_program(&ops, num_registers, &btree);
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

        let mut engine = Engine::with_program(&ops, num_registers, &btree);
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

        let mut engine = Engine::with_program(&ops, num_registers, &btree);
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

        let mut engine = Engine::with_program(&ops, num_registers, &btree);
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

        let mut engine = Engine::with_program(&ops, num_registers, &btree);
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

        let mut engine = Engine::with_program(&ops, num_registers, &btree);
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
                left: Box::new(PlanExpr::ColumnRef(0)),
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
        let mut engine = Engine::with_program(&ops, num_registers, &btree);
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
                left: Box::new(PlanExpr::ColumnRef(1)),
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
                    left: Box::new(PlanExpr::ColumnRef(0)),
                    right: Box::new(PlanExpr::Literal(Literal::Integer(3))),
                }),
                right: Box::new(PlanExpr::BinaryOp {
                    op: BinaryOp::LessThan,
                    left: Box::new(PlanExpr::ColumnRef(0)),
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
            columns: vec![PlanExpr::ColumnRef(0)],
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
                left: Box::new(PlanExpr::ColumnRef(0)),
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
                PlanExpr::ColumnRef(0),
                PlanExpr::BinaryOp {
                    op: BinaryOp::Multiply,
                    left: Box::new(PlanExpr::ColumnRef(0)),
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
            columns: vec![PlanExpr::ColumnRef(1), PlanExpr::ColumnRef(0)],
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
                left: Box::new(PlanExpr::ColumnRef(0)),
                right: Box::new(PlanExpr::Literal(Literal::Integer(5))),
            },
            input: Box::new(LogicalPlan::Project {
                columns: vec![PlanExpr::BinaryOp {
                    op: BinaryOp::Multiply,
                    left: Box::new(PlanExpr::ColumnRef(0)),
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
                left: Box::new(PlanExpr::ColumnRef(0)),
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
                columns: vec![PlanExpr::ColumnRef(0)],
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
                left: Box::new(PlanExpr::ColumnRef(0)),
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
                    left: Box::new(PlanExpr::ColumnRef(0)),
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
            table_columns: vec![1, 2, 3],
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
            indexes: vec![],
        };

        let (ops, num_registers) = compile_plan(&plan);

        let mut engine = Engine::with_program(&ops, num_registers, &btree);
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
            table_columns: vec![1, 2],
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
            indexes: vec![],
        };

        let (ops, num_registers) = compile_plan(&insert_plan);
        let insert_yields = Engine::with_program(&ops, num_registers, &btree).run();
        assert_eq!(insert_yields[0][0], ScalarValue::Integer(3));

        // Second: SCAN to read back (scan-space: 1=CBOR slot 0, 2=CBOR slot 1)
        let scan_plan = LogicalPlan::Scan {
            rootpage: root,
            columns: vec![1, 2],
        };

        let (ops, num_registers) = compile_plan(&scan_plan);
        let mut engine = Engine::with_program(&ops, num_registers, &btree);
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
            table_columns: vec![1],
            input: Box::new(LogicalPlan::Values {
                rows: vec![vec![Literal::Integer(42)]],
            }),
            indexes: vec![],
        };

        let (ops, num_registers) = compile_plan(&plan);
        let mut engine = Engine::with_program(&ops, num_registers, &btree);
        let yields = engine.run();

        assert_eq!(yields.len(), 1);
        assert_eq!(yields[0][0], ScalarValue::Integer(1));
    }

    /// INSERT into a table with a unique index — duplicate value must raise ConstraintViolation.
    #[test]
    fn test_insert_unique_index_duplicate_raises_error() {
        use crate::engine::EngineError;
        use crate::planner::IndexMaintenanceInfo;
        let test = TestDb::default();
        let mut btree = test.btree;
        let table_root = btree.create_tree();
        let index_root = btree.create_tree();

        let index = IndexMaintenanceInfo {
            rootpage: index_root,
            column_idxs: vec![1],
            unique: true,
        };

        // First insert (value=42) must succeed.
        let plan = LogicalPlan::Insert {
            rootpage: table_root,
            table_columns: vec![1],
            input: Box::new(LogicalPlan::Values {
                rows: vec![vec![Literal::Integer(42)]],
            }),
            indexes: vec![index.clone()],
        };
        let (ops, num_regs) = compile_plan(&plan);
        let yields = Engine::with_program(&ops, num_regs, &btree).run();
        assert_eq!(yields[0][0], ScalarValue::Integer(1), "first insert count");

        // Second insert with the same indexed value must fail.
        let plan2 = LogicalPlan::Insert {
            rootpage: table_root,
            table_columns: vec![1],
            input: Box::new(LogicalPlan::Values {
                rows: vec![vec![Literal::Integer(42)]],
            }),
            indexes: vec![index],
        };
        let (ops2, num_regs2) = compile_plan(&plan2);
        let mut engine2 = Engine::with_program(&ops2, num_regs2, &btree);
        let result = engine2.run_result();
        assert!(
            matches!(result, Err(EngineError::ConstraintViolation(_))),
            "expected ConstraintViolation, got {:?}",
            result
        );
    }

    /// INSERT into a table with a unique index — distinct values must all succeed.
    #[test]
    fn test_insert_unique_index_distinct_values_succeed() {
        use crate::planner::IndexMaintenanceInfo;

        let test = TestDb::default();
        let mut btree = test.btree;
        let table_root = btree.create_tree();
        let index_root = btree.create_tree();

        let index = IndexMaintenanceInfo {
            rootpage: index_root,
            column_idxs: vec![1],
            unique: true,
        };

        let plan = LogicalPlan::Insert {
            rootpage: table_root,
            table_columns: vec![1],
            input: Box::new(LogicalPlan::Values {
                rows: vec![
                    vec![Literal::Integer(1)],
                    vec![Literal::Integer(2)],
                    vec![Literal::Integer(3)],
                ],
            }),
            indexes: vec![index],
        };
        let (ops, num_regs) = compile_plan(&plan);
        let mut engine = Engine::with_program(&ops, num_regs, &btree);
        let yields = engine.run();
        assert_eq!(yields[0][0], ScalarValue::Integer(3), "all 3 rows inserted");
    }

    /// INSERT into a table with a non-unique index — duplicate values must succeed.
    #[test]
    fn test_insert_nonunique_index_allows_duplicates() {
        use crate::planner::IndexMaintenanceInfo;

        let test = TestDb::default();
        let mut btree = test.btree;
        let table_root = btree.create_tree();
        let index_root = btree.create_tree();

        let index = IndexMaintenanceInfo {
            rootpage: index_root,
            column_idxs: vec![1],
            unique: false,
        };

        let plan = LogicalPlan::Insert {
            rootpage: table_root,
            table_columns: vec![1],
            input: Box::new(LogicalPlan::Values {
                rows: vec![vec![Literal::Integer(7)], vec![Literal::Integer(7)]],
            }),
            indexes: vec![index],
        };
        let (ops, num_regs) = compile_plan(&plan);
        let mut engine = Engine::with_program(&ops, num_regs, &btree);
        let yields = engine.run();
        assert_eq!(yields[0][0], ScalarValue::Integer(2), "both rows inserted");
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
                    left: Box::new(PlanExpr::ColumnRef(0)),
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

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(50))]

        /// Generate random LogicalPlan trees (Sequence + Filter + Project + Limit + Sort + Distinct)
        /// and verify that compile_plan produces valid bytecode without panicking.
        /// "Valid bytecode" means: non-empty and register count is positive.
        #[test]
        fn test_compile_random_plans_no_panic(
            seed_count in 1i64..50,
            filter_val in 0i64..20,
            project_mul in 1i64..10,
            limit_count in 1u64..20,
            include_filter: bool,
            include_project: bool,
            include_limit: bool,
            include_sort: bool,
            include_distinct: bool,
        ) {
            use crate::planner::{BinaryOp, Literal, PlanExpr, SortKey};

            // Build a plan bottom-up: Sequence [→ Filter] [→ Project] [→ Sort] [→ Distinct] [→ Limit]
            let mut plan = LogicalPlan::Sequence { start: 0, end: seed_count };

            if include_filter {
                plan = LogicalPlan::Filter {
                    predicate: PlanExpr::BinaryOp {
                        op: BinaryOp::GreaterThan,
                        left: Box::new(PlanExpr::ColumnRef(0)),
                        right: Box::new(PlanExpr::Literal(Literal::Integer(filter_val))),
                    },
                    input: Box::new(plan),
                };
            }

            if include_project {
                plan = LogicalPlan::Project {
                    columns: vec![PlanExpr::BinaryOp {
                        op: BinaryOp::Multiply,
                        left: Box::new(PlanExpr::ColumnRef(0)),
                        right: Box::new(PlanExpr::Literal(Literal::Integer(project_mul))),
                    }],
                    input: Box::new(plan),
                };
            }

            if include_sort {
                plan = LogicalPlan::Sort {
                    sort_keys: vec![SortKey { expr: PlanExpr::ColumnRef(0), descending: false }],
                    input: Box::new(plan),
                };
            }

            if include_distinct {
                plan = LogicalPlan::Distinct { input: Box::new(plan) };
            }

            if include_limit {
                plan = LogicalPlan::Limit { count: limit_count, input: Box::new(plan) };
            }

            let (ops, num_registers) = compile_plan(&plan);

            // Must produce some bytecode and at least one register
            proptest::prop_assert!(!ops.is_empty(), "compile_plan returned empty bytecode");
            proptest::prop_assert!(num_registers > 0, "compile_plan allocated no registers");
        }
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
            table_columns: vec![1],
            filter: None,
            indexes: vec![],
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

    // ========================================================================
    // NestedLoop Join tests
    // ========================================================================

    /// Insert rows into a btree table.
    fn insert_rows2(btree: &mut crate::storage::BTree, root: u32, rows: &[(i64, i64)]) {
        let mut cursor = btree.open(root);
        let mut c = cursor.open_cursor();
        for (i, (a, b)) in rows.iter().enumerate() {
            c.insert_u64(
                i as u64,
                vec![ScalarValue::Integer(*a), ScalarValue::Integer(*b)],
            );
        }
    }

    fn nlj(left: LogicalPlan, right: LogicalPlan, on: PlanExpr) -> LogicalPlan {
        LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            on_condition: on,
            strategy: crate::planner::JoinStrategy::NestedLoop,
            left_column_count: 2,
        }
    }

    fn on_eq_col0_col2() -> PlanExpr {
        // ON left.col[0] = right.col[0] — in combined space right starts at index 2
        PlanExpr::BinaryOp {
            op: BinaryOp::Equals,
            left: Box::new(PlanExpr::ColumnRef(0)),
            right: Box::new(PlanExpr::ColumnRef(2)),
        }
    }

    fn on_true() -> PlanExpr {
        PlanExpr::Literal(Literal::Bool(true))
    }

    /// Run a plan that requires btree access.
    fn run_plan_with_btree(
        plan: &LogicalPlan,
        btree: &crate::storage::BTree,
    ) -> Vec<Vec<ScalarValue>> {
        let (ops, num_registers) = compile_plan(plan);
        let mut engine = Engine::with_program(&ops, num_registers, &btree);
        engine.run()
    }

    #[test]
    fn nested_loop_join_scan_right() {
        // users(id, val): [(1,10),(2,20)]; orders(user_id, amount): [(1,100),(1,200),(2,300)]
        // ON users.id = orders.user_id → 3 combined rows
        let test = TestDb::default();
        let mut btree = test.btree;
        let left_root = btree.create_tree();
        let right_root = btree.create_tree();
        insert_rows2(&mut btree, left_root, &[(1, 10), (2, 20)]);
        insert_rows2(&mut btree, right_root, &[(1, 100), (1, 200), (2, 300)]);

        let plan = nlj(
            LogicalPlan::Scan {
                rootpage: left_root,
                columns: vec![1, 2],
            },
            LogicalPlan::Scan {
                rootpage: right_root,
                columns: vec![1, 2],
            },
            on_eq_col0_col2(),
        );

        let yields = run_plan_with_btree(&plan, &btree);

        assert_eq!(yields.len(), 3, "expected 3 rows, got: {:?}", yields);
        assert_eq!(
            yields[0],
            vec![
                ScalarValue::Integer(1),
                ScalarValue::Integer(10),
                ScalarValue::Integer(1),
                ScalarValue::Integer(100)
            ]
        );
        assert_eq!(
            yields[1],
            vec![
                ScalarValue::Integer(1),
                ScalarValue::Integer(10),
                ScalarValue::Integer(1),
                ScalarValue::Integer(200)
            ]
        );
        assert_eq!(
            yields[2],
            vec![
                ScalarValue::Integer(2),
                ScalarValue::Integer(20),
                ScalarValue::Integer(2),
                ScalarValue::Integer(300)
            ]
        );
    }

    #[test]
    fn nested_loop_join_filter_scan_right() {
        // right has 3 rows; filter keeps only amount > 100
        // ON left.id = right.user_id → (1,10,1,200), (2,20,2,300)
        let test = TestDb::default();
        let mut btree = test.btree;
        let left_root = btree.create_tree();
        let right_root = btree.create_tree();
        insert_rows2(&mut btree, left_root, &[(1, 10), (2, 20)]);
        insert_rows2(&mut btree, right_root, &[(1, 50), (1, 200), (2, 300)]);

        let right = LogicalPlan::Filter {
            predicate: PlanExpr::BinaryOp {
                op: BinaryOp::GreaterThan,
                left: Box::new(PlanExpr::ColumnRef(1)),
                right: Box::new(PlanExpr::Literal(Literal::Integer(100))),
            },
            input: Box::new(LogicalPlan::Scan {
                rootpage: right_root,
                columns: vec![1, 2],
            }),
        };
        let plan = nlj(
            LogicalPlan::Scan {
                rootpage: left_root,
                columns: vec![1, 2],
            },
            right,
            on_eq_col0_col2(),
        );

        let yields = run_plan_with_btree(&plan, &btree);

        assert_eq!(yields.len(), 2, "expected 2 rows, got: {:?}", yields);
        assert_eq!(yields[0][3], ScalarValue::Integer(200));
        assert_eq!(yields[1][3], ScalarValue::Integer(300));
    }

    #[test]
    fn nested_loop_join_empty_right() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let left_root = btree.create_tree();
        let right_root = btree.create_tree();
        insert_rows2(&mut btree, left_root, &[(1, 10), (2, 20)]);
        // right is empty

        let plan = nlj(
            LogicalPlan::Scan {
                rootpage: left_root,
                columns: vec![1, 2],
            },
            LogicalPlan::Scan {
                rootpage: right_root,
                columns: vec![1, 2],
            },
            on_true(),
        );

        let yields = run_plan_with_btree(&plan, &btree);
        assert_eq!(yields.len(), 0);
    }

    #[test]
    fn nested_loop_join_empty_left() {
        let test = TestDb::default();
        let mut btree = test.btree;
        let left_root = btree.create_tree();
        let right_root = btree.create_tree();
        // left is empty
        insert_rows2(&mut btree, right_root, &[(1, 100), (2, 200)]);

        let plan = nlj(
            LogicalPlan::Scan {
                rootpage: left_root,
                columns: vec![1, 2],
            },
            LogicalPlan::Scan {
                rootpage: right_root,
                columns: vec![1, 2],
            },
            on_true(),
        );

        let yields = run_plan_with_btree(&plan, &btree);
        assert_eq!(yields.len(), 0);
    }

    #[test]
    fn nested_loop_join_index_probe_right() {
        // users(id, name): [(1,"alice"),(2,"bob")]
        // orders(user_id, amount): [(1,100),(1,200),(2,300)] with index on user_id
        // Plan: Join { NestedLoop, Scan(users), RowidLookup(IndexProbe(key=ColumnRef(0))), on: true }
        // Expected: (1,10,1,100), (1,10,1,200), (2,20,2,300)
        let test = TestDb::default();
        let mut btree = test.btree;
        let left_root = btree.create_tree();
        let right_root = btree.create_tree(); // orders table
        let idx_root = btree.create_tree(); // index on orders.user_id

        // users: rows keyed 0..(n-1)
        insert_rows2(&mut btree, left_root, &[(1, 10), (2, 20)]);

        // orders: rows keyed by rowid
        {
            let values = vec![
                (
                    vec![ScalarValue::Integer(1), ScalarValue::Integer(100)],
                    0u64,
                ),
                (
                    vec![ScalarValue::Integer(1), ScalarValue::Integer(200)],
                    1u64,
                ),
                (
                    vec![ScalarValue::Integer(2), ScalarValue::Integer(300)],
                    2u64,
                ),
            ];
            let mut cursor = btree.open(right_root);
            let mut c = cursor.open_cursor();
            for (row, rowid) in &values {
                c.insert_u64(*rowid, row.clone());
            }
        }

        // index on orders.user_id: composite key = [encode_index_value(user_id)][rowid_be_u64]
        {
            use crate::storage::{encode_index_value, encode_u64_key};
            let entries: &[(i64, u64)] = &[(1, 0), (1, 1), (2, 2)];
            let mut cursor = btree.open(idx_root);
            let mut c = cursor.open_cursor();
            for (user_id, rowid) in entries.iter() {
                let col_bytes = encode_index_value(&ScalarValue::Integer(*user_id));
                let mut composite_key = col_bytes;
                composite_key.extend_from_slice(&encode_u64_key(*rowid));
                c.insert(&composite_key, vec![]);
            }
        }

        // Plan: NestedLoop(Scan(users), RowidLookup(IndexProbe(key=left.col[0])))
        // ON condition: Literal(1) (true) — the equi-join is handled by IndexProbe itself
        let plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                rootpage: left_root,
                columns: vec![1, 2],
            }),
            right: Box::new(LogicalPlan::RowidLookup {
                input: Box::new(LogicalPlan::IndexProbe {
                    index_rootpage: idx_root,
                    key_expr: PlanExpr::ColumnRef(0), // left.id
                    index_col_idx: 0,
                }),
                table_rootpage: right_root,
                columns: vec![1, 2],
            }),
            on_condition: on_true(),
            strategy: crate::planner::JoinStrategy::NestedLoop,
            left_column_count: 2,
        };

        let yields = run_plan_with_btree(&plan, &btree);

        assert_eq!(yields.len(), 3, "expected 3 rows, got: {:?}", yields);
        assert_eq!(
            yields[0],
            vec![
                ScalarValue::Integer(1),
                ScalarValue::Integer(10),
                ScalarValue::Integer(1),
                ScalarValue::Integer(100)
            ]
        );
        assert_eq!(
            yields[1],
            vec![
                ScalarValue::Integer(1),
                ScalarValue::Integer(10),
                ScalarValue::Integer(1),
                ScalarValue::Integer(200)
            ]
        );
        assert_eq!(
            yields[2],
            vec![
                ScalarValue::Integer(2),
                ScalarValue::Integer(20),
                ScalarValue::Integer(2),
                ScalarValue::Integer(300)
            ]
        );
    }
}
