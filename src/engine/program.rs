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
    LikeValue(Reg, Reg, Reg), // Reg = Reg LIKE Reg (dest, value, pattern)

    // Key List (for collect-then-mutate pattern)
    InitKeyList(Reg),             // Initialize empty key list
    AppendKey(Reg, Reg),          // AppendKey(list, key): append key to list
    PopKey(Reg, Reg, JumpTarget), // PopKey(dest, list, jump): pop key or jump if empty

    // Row Buffer (for sorting)
    InitRowBuffer(Reg),                            // Initialize empty row buffer
    AppendToRowBuffer(Reg, Vec<Reg>),              // Append row to buffer
    SortRowBuffer(Reg, Vec<SortKeySpec>),          // Sort rows in buffer
    YieldFromRowBuffer(Vec<Reg>, Reg, JumpTarget), // Pop row from buffer or jump if empty
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
    WriteIndex(Reg, Reg, Reg), // WriteIndex(cursor, value, pk): insert into index
    DeleteCursor(Reg),         // DeleteCursor(cursor): delete row at current cursor position
    CanReadCursor(Reg, Reg),   // Reg = CanReadCursor(Reg)
    EncodeIndexKey(Reg, Reg), // EncodeIndexKey(dest, src): scalar to sortable index blob
    /// Read the raw key bytes of the current cursor entry as a Blob scalar.
    ReadCurrentKey(Reg, Reg), // ReadCurrentKey(dest, cursor)
    /// True if blob starts with the given prefix bytes.
    BlobStartsWith(Reg, Reg, Reg), // BlobStartsWith(dest, blob, prefix)
    /// True if the first len(bound) bytes of blob are strictly less than bound.
    BlobPrefixLt(Reg, Reg, Reg), // BlobPrefixLt(dest, blob, bound)
    /// True if the first len(bound) bytes of blob are less than or equal to bound.
    BlobPrefixLe(Reg, Reg, Reg), // BlobPrefixLe(dest, blob, bound)
    /// Extract blob[offset..] as a new Blob scalar.
    BlobSliceTail(Reg, Reg, usize), // BlobSliceTail(dest, blob, offset)
    /// Decode an 8-byte big-endian blob as a u64 rowid → Integer scalar.
    DecodeU64Key(Reg, Reg), // DecodeU64Key(dest, blob)

    // Control Flow
    Yield(Vec<Reg>),
    GoTo(JumpTarget),
    GoToIfEqualValue(JumpTarget, Reg, Reg),
    GoToIfFalse(JumpTarget, Reg),
    Halt,
}

pub(crate) struct ProgramCode {
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
        use colored::Colorize;
        write!(f, "{}", format!("R{}", self.0).yellow())
    }
}

impl std::fmt::Display for JumpTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use colored::Colorize;
        match self {
            JumpTarget::Resolved(addr) => write!(f, "{}", format!("@{}", addr).magenta()),
            JumpTarget::Unresolved(label) => write!(f, "{}", format!("?L{}", label.0).red()),
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
        use colored::Colorize;
        use Operation::*;

        match self {
            // Value operations
            StoreValue(r, v) => write!(f, "{:10} {}, {}", "Store".cyan().bold(), r, v),
            IncrementValue(r) => write!(f, "{:10} {}", "Inc".cyan().bold(), r),
            DecrementValue(r) => write!(f, "{:10} {}", "Dec".cyan().bold(), r),
            AddValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Add".cyan().bold(), d, a, b),
            SubtractValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Sub".cyan().bold(), d, a, b),
            MultiplyValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Mul".cyan().bold(), d, a, b),
            DivideValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Div".cyan().bold(), d, a, b),
            RemainderValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Rem".cyan().bold(), d, a, b),
            LessThanValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Lt".cyan().bold(), d, a, b),
            LessThanOrEqualValue(d, a, b) => {
                write!(f, "{:10} {}, {}, {}", "Le".cyan().bold(), d, a, b)
            }
            GreaterThanValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Gt".cyan().bold(), d, a, b),
            GreaterThanOrEqualValue(d, a, b) => {
                write!(f, "{:10} {}, {}, {}", "Ge".cyan().bold(), d, a, b)
            }
            EqualsValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Eq".cyan().bold(), d, a, b),
            NotEqualsValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Ne".cyan().bold(), d, a, b),
            AndValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "And".cyan().bold(), d, a, b),
            OrValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Or".cyan().bold(), d, a, b),
            NotValue(d, s) => write!(f, "{:10} {}, {}", "Not".cyan().bold(), d, s),
            NegateValue(d, s) => write!(f, "{:10} {}, {}", "Neg".cyan().bold(), d, s),
            CopyValue(d, s) => write!(f, "{:10} {}, {}", "Copy".cyan().bold(), d, s),
            IsNullValue(d, s) => write!(f, "{:10} {}, {}", "IsNull".cyan().bold(), d, s),
            IsNotNullValue(d, s) => write!(f, "{:10} {}, {}", "IsNotNull".cyan().bold(), d, s),

            // String/Scalar Function operations
            LengthValue(d, s) => write!(f, "{:10} {}, {}", "Length".cyan().bold(), d, s),
            UpperValue(d, s) => write!(f, "{:10} {}, {}", "Upper".cyan().bold(), d, s),
            LowerValue(d, s) => write!(f, "{:10} {}, {}", "Lower".cyan().bold(), d, s),
            AbsValue(d, s) => write!(f, "{:10} {}, {}", "Abs".cyan().bold(), d, s),
            LikeValue(d, val, pat) => {
                write!(f, "{:10} {}, {}, {}", "Like".cyan().bold(), d, val, pat)
            }

            // Key list operations
            InitKeyList(r) => write!(f, "{:10} {}", "InitKList".cyan().bold(), r),
            AppendKey(list, key) => write!(f, "{:10} {}, {}", "AppendKey".cyan().bold(), list, key),
            PopKey(dest, list, target) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "PopKey".cyan().bold(),
                    dest,
                    list,
                    target
                )
            }

            // Row buffer operations
            InitRowBuffer(r) => write!(f, "{:10} {}", "InitRBuf".cyan().bold(), r),
            AppendToRowBuffer(buf, regs) => {
                let regs_str: Vec<String> = regs.iter().map(|r| format!("{}", r)).collect();
                write!(
                    f,
                    "{:10} {}, [{}]",
                    "AppendRow".cyan().bold(),
                    buf,
                    regs_str.join(", ")
                )
            }
            SortRowBuffer(buf, keys) => {
                write!(
                    f,
                    "{:10} {}, {} keys",
                    "SortRows".cyan().bold(),
                    buf,
                    keys.len()
                )
            }
            YieldFromRowBuffer(regs, buf, target) => {
                let regs_str: Vec<String> = regs.iter().map(|r| format!("{}", r)).collect();
                write!(
                    f,
                    "{:10} [{}], {}, {}",
                    "YieldRow".cyan().bold(),
                    regs_str.join(", "),
                    buf,
                    target
                )
            }
            RewindRowBuffer(buf) => {
                write!(f, "{:10} {}", "RewindBuf".cyan().bold(), buf)
            }
            NextFromRowBuffer(regs, buf, target) => {
                let regs_str: Vec<String> = regs.iter().map(|r| format!("{}", r)).collect();
                write!(
                    f,
                    "{:10} [{}], {}, {}",
                    "NextRow".cyan().bold(),
                    regs_str.join(", "),
                    buf,
                    target
                )
            }

            // Group table operations
            InitGroupTable(r) => write!(f, "{:10} {}", "InitGrpTbl".cyan().bold(), r),
            UpdateGroup(table, keys, specs) => {
                write!(
                    f,
                    "{:10} {}, {} keys, {} aggs",
                    "UpdateGrp".cyan().bold(),
                    table,
                    keys.len(),
                    specs.len()
                )
            }
            YieldFromGroupTable(regs, table, target) => {
                let regs_str: Vec<String> = regs.iter().map(|r| format!("{}", r)).collect();
                write!(
                    f,
                    "{:10} [{}], {}, {}",
                    "YieldGrp".cyan().bold(),
                    regs_str.join(", "),
                    table,
                    target
                )
            }

            // Database operations
            Open(r, rootpage) => {
                use colored::Colorize;
                write!(
                    f,
                    "{:10} {}, {}",
                    "Open".cyan().bold(),
                    r,
                    format!("page:{}", rootpage).green()
                )
            }
            MoveCursor(r, op) => write!(f, "{:10} {}, {}", "MoveCursor".cyan().bold(), r, op),
            ReadCursor(regs, cursor) => {
                let regs_str: Vec<String> = regs.iter().map(|r| format!("{}", r)).collect();
                write!(
                    f,
                    "{:10} [{}], {}",
                    "ReadCursor".cyan().bold(),
                    regs_str.join(", "),
                    cursor
                )
            }
            ReadKey(dest, cursor) => {
                write!(f, "{:10} {}, {}", "ReadKey".cyan().bold(), dest, cursor)
            }
            WriteCursor(cursor, key, regs) => {
                let regs_str: Vec<String> = regs.iter().map(|r| format!("{}", r)).collect();
                write!(
                    f,
                    "{:10} {}, {}, [{}]",
                    "Write".cyan().bold(),
                    cursor,
                    key,
                    regs_str.join(", ")
                )
            }
            WriteIndex(cursor, val, pk) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "WriteIdx".cyan().bold(),
                    cursor,
                    val,
                    pk
                )
            }
            DeleteCursor(cursor) => {
                write!(f, "{:10} {}", "Delete".cyan().bold(), cursor)
            }
            CanReadCursor(dest, cursor) => {
                write!(f, "{:10} {}, {}", "CanRead".cyan().bold(), dest, cursor)
            }
            EncodeIndexKey(dest, src) => {
                write!(f, "{:10} {}, {}", "EncIdxKey".cyan().bold(), dest, src)
            }
            ReadCurrentKey(dest, cursor) => {
                write!(f, "{:10} {}, {}", "ReadKey".cyan().bold(), dest, cursor)
            }
            BlobStartsWith(dest, blob, prefix) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "BlobSW".cyan().bold(),
                    dest,
                    blob,
                    prefix
                )
            }
            BlobPrefixLt(dest, blob, bound) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "BlobPfxLt".cyan().bold(),
                    dest,
                    blob,
                    bound
                )
            }
            BlobPrefixLe(dest, blob, bound) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "BlobPfxLe".cyan().bold(),
                    dest,
                    blob,
                    bound
                )
            }
            BlobSliceTail(dest, blob, offset) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "BlobTail".cyan().bold(),
                    dest,
                    blob,
                    offset
                )
            }
            DecodeU64Key(dest, blob) => {
                write!(f, "{:10} {}, {}", "DecU64Key".cyan().bold(), dest, blob)
            }

            // Control flow
            Yield(regs) => {
                let regs_str: Vec<String> = regs.iter().map(|r| format!("{}", r)).collect();
                write!(f, "{:10} [{}]", "Yield".cyan().bold(), regs_str.join(", "))
            }
            GoTo(target) => write!(f, "{:10} {}", "GoTo".cyan().bold(), target),
            GoToIfEqualValue(target, a, b) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "GoToIfEq".cyan().bold(),
                    target,
                    a,
                    b
                )
            }
            GoToIfFalse(target, r) => {
                write!(f, "{:10} {}, {}", "GoToIfNot".cyan().bold(), target, r)
            }
            Halt => write!(f, "{}", "Halt".cyan().bold()),
        }
    }
}
