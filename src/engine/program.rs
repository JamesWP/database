use super::scalarvalue::ScalarValue;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reg(usize);

/// A label represents a position in bytecode that can be used as a jump target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Label(pub usize);

/// Represents a jump target that may or may not be resolved yet.
#[derive(Clone, Debug)]
pub enum JumpTarget {
    /// Jump target is not yet known (forward reference)
    Unresolved(Label),
    /// Jump target has been resolved to a concrete address
    Resolved(usize),
}

impl JumpTarget {
    /// Create a resolved jump target from a raw address.
    pub fn addr(address: usize) -> Self {
        JumpTarget::Resolved(address)
    }

    /// Create an unresolved jump target from a label.
    #[allow(dead_code)]
    pub fn label(label: Label) -> Self {
        JumpTarget::Unresolved(label)
    }

    /// Get the resolved address, panicking if unresolved.
    pub fn unwrap_resolved(&self) -> usize {
        match self {
            JumpTarget::Resolved(addr) => *addr,
            JumpTarget::Unresolved(label) => {
                panic!("Jump target for label {:?} was never resolved", label)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum MoveOperation {
    First,
    Next,
    Last,
    Find(Reg),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortKeySpec {
    pub column_index: usize, // Index within the row (not register number)
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateOp {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateSpec {
    pub op: AggregateOp,
    pub input_reg: Option<Reg>, // None for COUNT(*)
}

/// VM operations for the bytecode interpreter.
///
/// ## Jump Target Safety
///
/// Operations that contain JumpTarget fields have compile-time safety enforcement:
///
/// **Label Resolution with Offset:**
/// - Labels stay `Unresolved` until the final finalization step
/// - `BytecodeEmitter::finalize_with_offset()` resolves labels with offset baked in
/// - No separate adjustment pass needed - offsets are applied during resolution
/// - Exhaustive matching (no catch-all) ensures new operations are handled
///
/// **How it works:**
/// 1. Body emitter creates operations with `Unresolved(Label)`
/// 2. `CodegenContext::finalize()` calls `body_emitter.finalize_with_offset(offset)`
/// 3. Labels resolve directly to final addresses: `base_addr + offset`
/// 4. Any operation not handled stays `Unresolved` and panics at runtime
///
/// This prevents bugs where:
/// - Jump targets are resolved but not adjusted (eliminated - no separate step)
/// - New operations with JumpTarget are added but not handled (compile error)
///
/// TODO: switch to using {} and named members
#[derive(Clone, Debug)]
pub enum Operation {
    // Value
    StoreValue(Reg, ScalarValue),
    IncrementValue(Reg),                    // Reg = Reg + 1
    DecrementValue(Reg),                    // Reg = Reg - 1
    AddValue(Reg, Reg, Reg),                // Reg = Reg + Reg
    SubtractValue(Reg, Reg, Reg),           // Reg = Reg - Reg
    MultiplyValue(Reg, Reg, Reg),           // Reg = Reg * Reg
    DivideValue(Reg, Reg, Reg),             // Reg = Reg / Reg
    RemainderValue(Reg, Reg, Reg),          // Reg = Reg % Reg
    LessThanValue(Reg, Reg, Reg),           // Reg = Reg < Reg
    LessThanOrEqualValue(Reg, Reg, Reg),    // Reg = Reg <= Reg
    GreaterThanValue(Reg, Reg, Reg),        // Reg = Reg > Reg
    GreaterThanOrEqualValue(Reg, Reg, Reg), // Reg = Reg >= Reg
    EqualsValue(Reg, Reg, Reg),             // Reg = Reg == Reg
    NotEqualsValue(Reg, Reg, Reg),          // Reg = Reg != Reg
    AndValue(Reg, Reg, Reg),                // Reg = Reg && Reg
    OrValue(Reg, Reg, Reg),                 // Reg = Reg || Reg
    NotValue(Reg, Reg),                     // Reg = !Reg
    NegateValue(Reg, Reg),                  // Reg = -Reg (arithmetic negation)
    CopyValue(Reg, Reg),                    // Reg = Reg (copy value)
    IsNullValue(Reg, Reg),                  // Reg = (Reg IS NULL) → Boolean
    IsNotNullValue(Reg, Reg),               // Reg = (Reg IS NOT NULL) → Boolean

    // String/Scalar Functions
    LengthValue(Reg, Reg),    // Reg = LENGTH(Reg)
    UpperValue(Reg, Reg),     // Reg = UPPER(Reg)
    LowerValue(Reg, Reg),     // Reg = LOWER(Reg)
    AbsValue(Reg, Reg),       // Reg = ABS(Reg)
    RandomValue(Reg),         // Reg = random i64 (SQLite RANDOM() semantics)
    LikeValue(Reg, Reg, Reg), // Reg = Reg LIKE Reg (dest, value, pattern)

    // Row Buffer (for sorting)
    InitRowBuffer(Reg),                   // Initialize empty row buffer
    AppendToRowBuffer(Reg, Vec<Reg>),     // Append row to buffer
    SortRowBuffer(Reg, Vec<SortKeySpec>), // Sort rows in buffer
    /// Reset the row buffer's read cursor to the beginning (for re-iteration)
    RewindRowBuffer(Reg),
    /// Read the next row from the buffer without removing it.
    /// If the read cursor is past the end, jump to target.
    /// Otherwise, copy row values into dest_regs and advance the read cursor.
    NextFromRowBuffer(Vec<Reg>, Reg, JumpTarget),

    // Group Table (for GROUP BY aggregation)
    InitGroupTable(Reg), // Initialize empty group table
    UpdateGroup(Reg, Vec<Reg>, Vec<AggregateSpec>), // Update group: (table, keys, agg_specs)
    YieldFromGroupTable(Vec<Reg>, Reg, JumpTarget), // Pop group or jump if empty

    // Db
    Open(Reg, u32),
    MoveCursor(Reg, MoveOperation),
    ReadCursor(Vec<Reg>, Reg), // ReadCursor(dest_regs, cursor): read row values
    ReadKey(Reg, Reg),         // ReadKey(dest, cursor): read btree key as integer
    WriteCursor(Reg, Reg, Vec<Reg>), // WriteCursor(cursor, key, values): insert row
    /// Insert a row into a secondary index B-tree.
    /// When `unique` is true, a conflict on the column-value prefix raises a
    /// `ConstraintViolation` error before any write occurs.
    WriteIndex(Reg, Vec<Reg>, Reg, bool), // WriteIndex(cursor, col_values, pk, unique)
    CheckUnique(Reg, Vec<Reg>), // CheckUnique(cursor, col_values): error if key already exists
    /// Store the next available rowid for the cursor's table into `key_reg`.
    /// Uses a session-scoped cache; falls back to a B-tree seek on the first call per table.
    InitRowid(Reg, Reg), // InitRowid(cursor_reg, key_reg)
    DeleteIndex(Reg, Vec<Reg>, Reg), // DeleteIndex(cursor, col_values, pk): remove from index
    DeleteCursor(Reg),         // DeleteCursor(cursor): delete row at current cursor position
    CanReadCursor(Reg, Reg),   // Reg = CanReadCursor(Reg)
    EncodeIndexKey(Reg, Reg),  // EncodeIndexKey(dest, src): scalar to sortable index blob
    /// Read the raw key bytes of the current cursor entry as a Blob scalar.
    ReadCurrentKey(Reg, Reg), // ReadCurrentKey(dest, cursor)

    // Blob operations (for composing index key comparisons)
    BlobStartsWith(Reg, Reg, Reg), // BlobStartsWith(dest, blob, prefix): blob starts_with prefix
    BlobPrefixLt(Reg, Reg, Reg),   // BlobPrefixLt(dest, blob, bound): blob[..len(bound)] < bound
    BlobPrefixLe(Reg, Reg, Reg),   // BlobPrefixLe(dest, blob, bound): blob[..len(bound)] <= bound
    BlobSliceTail(Reg, Reg, usize), // BlobSliceTail(dest, blob, offset): blob[offset..]
    BlobSliceLast(Reg, Reg, usize), // BlobSliceLast(dest, blob, n): last n bytes of blob
    BlobDropLast(Reg, Reg, usize), // BlobDropLast(dest, blob, n): blob without last n bytes
    DecodeU64Key(Reg, Reg),        // DecodeU64Key(dest, blob): 8-byte blob → u64 as Integer
    /// Decode N column values sequentially from the start of an index key blob.
    /// Walks the type-tagged byte stream, storing each column into the corresponding
    /// dest register. Does NOT touch the trailing 8-byte rowid suffix.
    DecodeIndexColumns {
        dest: Vec<Reg>, // one register per index column to decode (in key order)
        src: Reg,       // blob register with the raw index key
    },

    // Control Flow
    Yield(Vec<Reg>),
    GoTo(JumpTarget),
    GoToIfEqualValue(JumpTarget, Reg, Reg),
    GoToIfFalse(JumpTarget, Reg),
    Halt,
}

impl Operation {
    /// Return a short static name for use in USDT tracing.
    pub fn name(&self) -> &'static str {
        use Operation::*;
        match self {
            StoreValue(..) => "Store",
            IncrementValue(..) => "Inc",
            DecrementValue(..) => "Dec",
            AddValue(..) => "Add",
            SubtractValue(..) => "Sub",
            MultiplyValue(..) => "Mul",
            DivideValue(..) => "Div",
            RemainderValue(..) => "Rem",
            LessThanValue(..) => "Lt",
            LessThanOrEqualValue(..) => "Le",
            GreaterThanValue(..) => "Gt",
            GreaterThanOrEqualValue(..) => "Ge",
            EqualsValue(..) => "Eq",
            NotEqualsValue(..) => "Ne",
            AndValue(..) => "And",
            OrValue(..) => "Or",
            NotValue(..) => "Not",
            NegateValue(..) => "Neg",
            CopyValue(..) => "Copy",
            IsNullValue(..) => "IsNull",
            IsNotNullValue(..) => "IsNotNull",
            LengthValue(..) => "Length",
            UpperValue(..) => "Upper",
            LowerValue(..) => "Lower",
            AbsValue(..) => "Abs",
            RandomValue(..) => "Random",
            LikeValue(..) => "Like",
            InitRowBuffer(..) => "InitRowBuf",
            AppendToRowBuffer(..) => "AppendRowBuf",
            SortRowBuffer(..) => "SortRowBuf",
            RewindRowBuffer(..) => "RewindRowBuf",
            NextFromRowBuffer(..) => "NextRowBuf",
            InitGroupTable(..) => "InitGroupTable",
            UpdateGroup(..) => "UpdateGroup",
            YieldFromGroupTable(..) => "YieldGroup",
            Open(..) => "Open",
            MoveCursor(..) => "MoveCursor",
            ReadCursor(..) => "ReadCursor",
            ReadKey(..) => "ReadKey",
            WriteCursor(..) => "WriteCursor",
            WriteIndex(_, _, _, true) => "WriteIdxUniq",
            WriteIndex(_, _, _, false) => "WriteIndex",
            CheckUnique(..) => "CheckUnique",
            InitRowid(..) => "InitRowid",
            DeleteIndex(..) => "DeleteIndex",
            DeleteCursor(..) => "DeleteCursor",
            CanReadCursor(..) => "CanRead",
            EncodeIndexKey(..) => "EncodeKey",
            ReadCurrentKey(..) => "ReadCurKey",
            BlobStartsWith(..) => "BlobStarts",
            BlobPrefixLt(..) => "BlobPfxLt",
            BlobPrefixLe(..) => "BlobPfxLe",
            BlobSliceTail(..) => "BlobTail",
            BlobSliceLast(..) => "BlobLast",
            BlobDropLast(..) => "BlobDrop",
            DecodeU64Key(..) => "DecodeU64",
            DecodeIndexColumns { .. } => "DecodeIdxCols",
            Yield(..) => "Yield",
            GoTo(..) => "GoTo",
            GoToIfEqualValue(..) => "GoToIfEq",
            GoToIfFalse(..) => "GoToIfFalse",
            Halt => "Halt",
        }
    }
}

pub struct ProgramCode {
    operations: Vec<Operation>,
    curent_operation_index: usize,
}

impl From<&[Operation]> for ProgramCode {
    fn from(value: &[Operation]) -> Self {
        Self {
            operations: value.to_vec(),
            curent_operation_index: 0,
        }
    }
}

impl ProgramCode {
    pub fn advance(&mut self) -> Operation {
        let op = self.curent();

        match op {
            Operation::Halt => {}
            _ => self.curent_operation_index += 1,
        };

        op
    }

    fn curent(&self) -> Operation {
        self.operations
            .get(self.curent_operation_index)
            .unwrap()
            .clone()
    }

    pub(crate) fn set_next_operation_index(&mut self, index: usize) {
        self.curent_operation_index = index;
    }

    pub(crate) fn current_index(&self) -> usize {
        self.curent_operation_index
    }
}

impl Reg {
    pub fn index(&self) -> usize {
        let Reg(index) = self;

        *index
    }

    pub fn new(index: usize) -> Reg {
        Reg(index)
    }
}

impl std::fmt::Display for Reg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "R{}", self.0)
    }
}

impl std::fmt::Display for JumpTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JumpTarget::Resolved(addr) => write!(f, "@{addr}"),
            JumpTarget::Unresolved(label) => write!(f, "?L{}", label.0),
        }
    }
}

impl std::fmt::Display for MoveOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MoveOperation::First => write!(f, "First"),
            MoveOperation::Next => write!(f, "Next"),
            MoveOperation::Last => write!(f, "Last"),
            MoveOperation::Find(reg) => write!(f, "Find({})", reg),
        }
    }
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Operation::*;

        macro_rules! regs_str {
            ($regs:expr) => {
                $regs
                    .iter()
                    .map(|r| format!("{r}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
        }

        match self {
            // Value operations
            StoreValue(r, v) => write!(f, "{:10} {r}, {v}", "Store"),
            IncrementValue(r) => write!(f, "{:10} {r}", "Inc"),
            DecrementValue(r) => write!(f, "{:10} {r}", "Dec"),
            AddValue(d, a, b) => write!(f, "{:10} {d}, {a}, {b}", "Add"),
            SubtractValue(d, a, b) => write!(f, "{:10} {d}, {a}, {b}", "Sub"),
            MultiplyValue(d, a, b) => write!(f, "{:10} {d}, {a}, {b}", "Mul"),
            DivideValue(d, a, b) => write!(f, "{:10} {d}, {a}, {b}", "Div"),
            RemainderValue(d, a, b) => write!(f, "{:10} {d}, {a}, {b}", "Rem"),
            LessThanValue(d, a, b) => write!(f, "{:10} {d}, {a}, {b}", "Lt"),
            LessThanOrEqualValue(d, a, b) => write!(f, "{:10} {d}, {a}, {b}", "Le"),
            GreaterThanValue(d, a, b) => write!(f, "{:10} {d}, {a}, {b}", "Gt"),
            GreaterThanOrEqualValue(d, a, b) => write!(f, "{:10} {d}, {a}, {b}", "Ge"),
            EqualsValue(d, a, b) => write!(f, "{:10} {d}, {a}, {b}", "Eq"),
            NotEqualsValue(d, a, b) => write!(f, "{:10} {d}, {a}, {b}", "Ne"),
            AndValue(d, a, b) => write!(f, "{:10} {d}, {a}, {b}", "And"),
            OrValue(d, a, b) => write!(f, "{:10} {d}, {a}, {b}", "Or"),
            NotValue(d, s) => write!(f, "{:10} {d}, {s}", "Not"),
            NegateValue(d, s) => write!(f, "{:10} {d}, {s}", "Neg"),
            CopyValue(d, s) => write!(f, "{:10} {d}, {s}", "Copy"),
            IsNullValue(d, s) => write!(f, "{:10} {d}, {s}", "IsNull"),
            IsNotNullValue(d, s) => write!(f, "{:10} {d}, {s}", "IsNotNull"),

            // String/Scalar Function operations
            LengthValue(d, s) => write!(f, "{:10} {d}, {s}", "Length"),
            UpperValue(d, s) => write!(f, "{:10} {d}, {s}", "Upper"),
            LowerValue(d, s) => write!(f, "{:10} {d}, {s}", "Lower"),
            AbsValue(d, s) => write!(f, "{:10} {d}, {s}", "Abs"),
            RandomValue(d) => write!(f, "{:10} {d}", "Random"),
            LikeValue(d, val, pat) => write!(f, "{:10} {d}, {val}, {pat}", "Like"),

            // Row buffer operations
            InitRowBuffer(r) => write!(f, "{:10} {r}", "InitRBuf"),
            AppendToRowBuffer(buf, regs) => {
                write!(f, "{:10} {buf}, [{}]", "AppendRow", regs_str!(regs))
            }
            SortRowBuffer(buf, keys) => {
                write!(f, "{:10} {buf}, {} keys", "SortRows", keys.len())
            }
            RewindRowBuffer(buf) => write!(f, "{:10} {buf}", "RewindBuf"),
            NextFromRowBuffer(regs, buf, target) => {
                write!(f, "{:10} [{}], {buf}, {target}", "NextRow", regs_str!(regs))
            }

            // Group table operations
            InitGroupTable(r) => write!(f, "{:10} {r}", "InitGrpTbl"),
            UpdateGroup(table, keys, specs) => {
                write!(
                    f,
                    "{:10} {table}, {} keys, {} aggs",
                    "UpdateGrp",
                    keys.len(),
                    specs.len()
                )
            }
            YieldFromGroupTable(regs, table, target) => {
                write!(
                    f,
                    "{:10} [{}], {table}, {target}",
                    "YieldGrp",
                    regs_str!(regs)
                )
            }

            // Database operations
            Open(r, rootpage) => write!(f, "{:10} {r}, page:{rootpage}", "Open"),
            MoveCursor(r, op) => write!(f, "{:10} {r}, {op}", "MoveCursor"),
            ReadCursor(regs, cursor) => {
                write!(f, "{:10} [{}], {cursor}", "ReadCursor", regs_str!(regs))
            }
            ReadKey(dest, cursor) => write!(f, "{:10} {dest}, {cursor}", "ReadKey"),
            WriteCursor(cursor, key, regs) => {
                write!(f, "{:10} {cursor}, {key}, [{}]", "Write", regs_str!(regs))
            }
            WriteIndex(cursor, vals, pk, unique) => {
                let label = if *unique { "WriteIdxUniq" } else { "WriteIdx" };
                write!(f, "{:10} {cursor}, [{}], {pk}", label, regs_str!(vals))
            }
            CheckUnique(cursor, vals) => {
                write!(f, "{:10} {cursor}, [{}]", "CheckUniq", regs_str!(vals))
            }
            InitRowid(cursor, key) => write!(f, "{:10} {cursor}, {key}", "InitRowid"),
            DeleteIndex(cursor, vals, pk) => {
                write!(f, "{:10} {cursor}, [{}], {pk}", "DelIdx", regs_str!(vals))
            }
            DeleteCursor(cursor) => write!(f, "{:10} {cursor}", "Delete"),
            CanReadCursor(dest, cursor) => write!(f, "{:10} {dest}, {cursor}", "CanRead"),
            EncodeIndexKey(dest, src) => write!(f, "{:10} {dest}, {src}", "EncIdxKey"),
            ReadCurrentKey(dest, cursor) => write!(f, "{:10} {dest}, {cursor}", "ReadKey"),
            BlobStartsWith(dest, blob, prefix) => {
                write!(f, "{:10} {dest}, {blob}, {prefix}", "BlobSW")
            }
            BlobPrefixLt(dest, blob, bound) => {
                write!(f, "{:10} {dest}, {blob}, {bound}", "BlobPfxLt")
            }
            BlobPrefixLe(dest, blob, bound) => {
                write!(f, "{:10} {dest}, {blob}, {bound}", "BlobPfxLe")
            }
            BlobSliceTail(dest, blob, offset) => {
                write!(f, "{:10} {dest}, {blob}, {offset}", "BlobTail")
            }
            BlobSliceLast(dest, blob, n) => write!(f, "{:10} {dest}, {blob}, {n}", "BlobLast"),
            BlobDropLast(dest, blob, n) => write!(f, "{:10} {dest}, {blob}, {n}", "BlobDrop"),
            DecodeU64Key(dest, blob) => write!(f, "{:10} {dest}, {blob}", "DecU64Key"),
            DecodeIndexColumns { dest, src } => {
                write!(f, "{:10} [{}], {src}", "DecIdxCols", regs_str!(dest))
            }

            // Control flow
            Yield(regs) => write!(f, "{:10} [{}]", "Yield", regs_str!(regs)),
            GoTo(target) => write!(f, "{:10} {target}", "GoTo"),
            GoToIfEqualValue(target, a, b) => write!(f, "{:10} {target}, {a}, {b}", "GoToIfEq"),
            GoToIfFalse(target, r) => write!(f, "{:10} {target}, {r}", "GoToIfNot"),
            Halt => write!(f, "Halt"),
        }
    }
}
