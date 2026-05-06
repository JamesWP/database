use colored::Colorize as _;
use database::engine::program::{JumpTarget, Label, MoveOperation, Operation, Reg};
use database::engine::scalarvalue::ScalarValue;
use std::fmt;

pub struct ColoredScalarValue<'a>(pub &'a ScalarValue);

impl fmt::Display for ColoredScalarValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ScalarValue::*;
        match self.0 {
            Integer(i) => write!(f, "{}", i.to_string().green()),
            Floating(fl) => write!(f, "{}", fl.to_string().green()),
            Boolean(b) => write!(f, "{}", b.to_string().green()),
            String(s) => write!(f, "{}", format!("\"{}\"", s).green()),
            Blob(b) => write!(f, "{}", format!("Blob({})", b.len()).green()),
            Null => write!(f, "{}", "NULL".dimmed()),
        }
    }
}

pub struct ColoredReg(pub Reg);

impl fmt::Display for ColoredReg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", format!("R{}", self.0.index()).yellow())
    }
}

pub struct ColoredJumpTarget<'a>(pub &'a JumpTarget);

impl fmt::Display for ColoredJumpTarget<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            JumpTarget::Resolved(addr) => write!(f, "{}", format!("@{addr}").magenta()),
            JumpTarget::Unresolved(Label(n)) => write!(f, "{}", format!("?L{n}").red()),
        }
    }
}

pub struct ColoredOperation<'a>(pub &'a Operation);

impl fmt::Display for ColoredOperation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Operation::*;

        macro_rules! r {
            ($reg:expr) => {
                ColoredReg(*$reg)
            };
        }
        macro_rules! jt {
            ($target:expr) => {
                ColoredJumpTarget($target)
            };
        }
        macro_rules! regs_str {
            ($regs:expr) => {
                $regs
                    .iter()
                    .map(|r| format!("{}", ColoredReg(*r)))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
        }

        match self.0 {
            // Value operations
            StoreValue(reg, v) => write!(
                f,
                "{:10} {}, {}",
                "Store".cyan().bold(),
                r!(reg),
                ColoredScalarValue(v)
            ),
            IncrementValue(reg) => write!(f, "{:10} {}", "Inc".cyan().bold(), r!(reg)),
            DecrementValue(reg) => write!(f, "{:10} {}", "Dec".cyan().bold(), r!(reg)),
            AddValue(d, a, b) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "Add".cyan().bold(),
                    r!(d),
                    r!(a),
                    r!(b)
                )
            }
            SubtractValue(d, a, b) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "Sub".cyan().bold(),
                    r!(d),
                    r!(a),
                    r!(b)
                )
            }
            MultiplyValue(d, a, b) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "Mul".cyan().bold(),
                    r!(d),
                    r!(a),
                    r!(b)
                )
            }
            DivideValue(d, a, b) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "Div".cyan().bold(),
                    r!(d),
                    r!(a),
                    r!(b)
                )
            }
            RemainderValue(d, a, b) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "Rem".cyan().bold(),
                    r!(d),
                    r!(a),
                    r!(b)
                )
            }
            LessThanValue(d, a, b) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "Lt".cyan().bold(),
                    r!(d),
                    r!(a),
                    r!(b)
                )
            }
            LessThanOrEqualValue(d, a, b) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "Le".cyan().bold(),
                    r!(d),
                    r!(a),
                    r!(b)
                )
            }
            GreaterThanValue(d, a, b) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "Gt".cyan().bold(),
                    r!(d),
                    r!(a),
                    r!(b)
                )
            }
            GreaterThanOrEqualValue(d, a, b) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "Ge".cyan().bold(),
                    r!(d),
                    r!(a),
                    r!(b)
                )
            }
            EqualsValue(d, a, b) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "Eq".cyan().bold(),
                    r!(d),
                    r!(a),
                    r!(b)
                )
            }
            NotEqualsValue(d, a, b) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "Ne".cyan().bold(),
                    r!(d),
                    r!(a),
                    r!(b)
                )
            }
            AndValue(d, a, b) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "And".cyan().bold(),
                    r!(d),
                    r!(a),
                    r!(b)
                )
            }
            OrValue(d, a, b) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "Or".cyan().bold(),
                    r!(d),
                    r!(a),
                    r!(b)
                )
            }
            NotValue(d, s) => write!(f, "{:10} {}, {}", "Not".cyan().bold(), r!(d), r!(s)),
            NegateValue(d, s) => write!(f, "{:10} {}, {}", "Neg".cyan().bold(), r!(d), r!(s)),
            CopyValue(d, s) => write!(f, "{:10} {}, {}", "Copy".cyan().bold(), r!(d), r!(s)),
            IsNullValue(d, s) => write!(f, "{:10} {}, {}", "IsNull".cyan().bold(), r!(d), r!(s)),
            IsNotNullValue(d, s) => {
                write!(f, "{:10} {}, {}", "IsNotNull".cyan().bold(), r!(d), r!(s))
            }

            // String/Scalar functions
            LengthValue(d, s) => write!(f, "{:10} {}, {}", "Length".cyan().bold(), r!(d), r!(s)),
            UpperValue(d, s) => write!(f, "{:10} {}, {}", "Upper".cyan().bold(), r!(d), r!(s)),
            LowerValue(d, s) => write!(f, "{:10} {}, {}", "Lower".cyan().bold(), r!(d), r!(s)),
            AbsValue(d, s) => write!(f, "{:10} {}, {}", "Abs".cyan().bold(), r!(d), r!(s)),
            RandomValue(d) => write!(f, "{:10} {}", "Random".cyan().bold(), r!(d)),
            LikeValue(d, val, pat) => {
                write!(
                    f,
                    "{:10} {}, {}, {}",
                    "Like".cyan().bold(),
                    r!(d),
                    r!(val),
                    r!(pat)
                )
            }

            // Row buffer
            InitRowBuffer(reg) => write!(f, "{:10} {}", "InitRBuf".cyan().bold(), r!(reg)),
            AppendToRowBuffer(buf, regs) => write!(
                f,
                "{:10} {}, [{}]",
                "AppendRow".cyan().bold(),
                r!(buf),
                regs_str!(regs)
            ),
            SortRowBuffer(buf, keys) => write!(
                f,
                "{:10} {}, {} keys",
                "SortRows".cyan().bold(),
                r!(buf),
                keys.len()
            ),
            RewindRowBuffer(buf) => write!(f, "{:10} {}", "RewindBuf".cyan().bold(), r!(buf)),
            NextFromRowBuffer(regs, buf, target) => write!(
                f,
                "{:10} [{}], {}, {}",
                "NextRow".cyan().bold(),
                regs_str!(regs),
                r!(buf),
                jt!(target)
            ),

            // Group table
            InitGroupTable(reg) => write!(f, "{:10} {}", "InitGrpTbl".cyan().bold(), r!(reg)),
            UpdateGroup(table, keys, specs) => write!(
                f,
                "{:10} {}, {} keys, {} aggs",
                "UpdateGrp".cyan().bold(),
                r!(table),
                keys.len(),
                specs.len()
            ),
            YieldFromGroupTable(regs, table, target) => write!(
                f,
                "{:10} [{}], {}, {}",
                "YieldGrp".cyan().bold(),
                regs_str!(regs),
                r!(table),
                jt!(target)
            ),

            // Database
            Open(reg, rootpage) => write!(
                f,
                "{:10} {}, {}",
                "Open".cyan().bold(),
                r!(reg),
                format!("page:{rootpage}").green()
            ),
            MoveCursor(reg, op) => write!(
                f,
                "{:10} {}, {}",
                "MoveCursor".cyan().bold(),
                r!(reg),
                ColoredMoveOp(op)
            ),
            ReadCursor(regs, cursor) => write!(
                f,
                "{:10} [{}], {}",
                "ReadCursor".cyan().bold(),
                regs_str!(regs),
                r!(cursor)
            ),
            ReadKey(dest, cursor) => {
                write!(
                    f,
                    "{:10} {}, {}",
                    "ReadKey".cyan().bold(),
                    r!(dest),
                    r!(cursor)
                )
            }
            WriteCursor(cursor, key, regs, unique) => {
                let label = if *unique { "WriteUniq" } else { "Write" };
                write!(
                    f,
                    "{:10} {}, {}, [{}]",
                    label.cyan().bold(),
                    r!(cursor),
                    r!(key),
                    regs_str!(regs)
                )
            }
            WriteIndex(cursor, vals, pk, unique) => {
                let label = if *unique { "WriteIdxUniq" } else { "WriteIdx" };
                write!(
                    f,
                    "{:10} {}, [{}], {}",
                    label.cyan().bold(),
                    r!(cursor),
                    regs_str!(vals),
                    r!(pk)
                )
            }
            CheckUnique(cursor, vals) => write!(
                f,
                "{:10} {}, [{}]",
                "CheckUniq".cyan().bold(),
                r!(cursor),
                regs_str!(vals)
            ),
            InitRowid(cursor, key) => {
                write!(
                    f,
                    "{:10} {}, {}",
                    "InitRowid".cyan().bold(),
                    r!(cursor),
                    r!(key)
                )
            }
            DeleteIndex(cursor, vals, pk) => write!(
                f,
                "{:10} {}, [{}], {}",
                "DelIdx".cyan().bold(),
                r!(cursor),
                regs_str!(vals),
                r!(pk)
            ),
            DeleteCursor(cursor) => write!(f, "{:10} {}", "Delete".cyan().bold(), r!(cursor)),
            CanReadCursor(dest, cursor) => {
                write!(
                    f,
                    "{:10} {}, {}",
                    "CanRead".cyan().bold(),
                    r!(dest),
                    r!(cursor)
                )
            }
            EncodeIndexKey(dest, src) => {
                write!(
                    f,
                    "{:10} {}, {}",
                    "EncIdxKey".cyan().bold(),
                    r!(dest),
                    r!(src)
                )
            }
            ReadCurrentKey(dest, cursor) => {
                write!(
                    f,
                    "{:10} {}, {}",
                    "ReadKey".cyan().bold(),
                    r!(dest),
                    r!(cursor)
                )
            }
            BlobStartsWith(dest, blob, prefix) => write!(
                f,
                "{:10} {}, {}, {}",
                "BlobSW".cyan().bold(),
                r!(dest),
                r!(blob),
                r!(prefix)
            ),
            BlobPrefixLt(dest, blob, bound) => write!(
                f,
                "{:10} {}, {}, {}",
                "BlobPfxLt".cyan().bold(),
                r!(dest),
                r!(blob),
                r!(bound)
            ),
            BlobPrefixLe(dest, blob, bound) => write!(
                f,
                "{:10} {}, {}, {}",
                "BlobPfxLe".cyan().bold(),
                r!(dest),
                r!(blob),
                r!(bound)
            ),
            BlobSliceTail(dest, blob, offset) => write!(
                f,
                "{:10} {}, {}, {}",
                "BlobTail".cyan().bold(),
                r!(dest),
                r!(blob),
                offset
            ),
            BlobSliceLast(dest, blob, n) => write!(
                f,
                "{:10} {}, {}, {}",
                "BlobLast".cyan().bold(),
                r!(dest),
                r!(blob),
                n
            ),
            BlobDropLast(dest, blob, n) => write!(
                f,
                "{:10} {}, {}, {}",
                "BlobDrop".cyan().bold(),
                r!(dest),
                r!(blob),
                n
            ),
            DecodeU64Key(dest, blob) => {
                write!(
                    f,
                    "{:10} {}, {}",
                    "DecU64Key".cyan().bold(),
                    r!(dest),
                    r!(blob)
                )
            }
            DecodeIndexColumns { dest, src } => write!(
                f,
                "{:10} [{}], {}",
                "DecIdxCols".cyan().bold(),
                regs_str!(dest),
                r!(src)
            ),

            // Control flow
            Yield(regs) => {
                write!(f, "{:10} [{}]", "Yield".cyan().bold(), regs_str!(regs))
            }
            GoTo(target) => write!(f, "{:10} {}", "GoTo".cyan().bold(), jt!(target)),
            GoToIfEqualValue(target, a, b) => write!(
                f,
                "{:10} {}, {}, {}",
                "GoToIfEq".cyan().bold(),
                jt!(target),
                r!(a),
                r!(b)
            ),
            GoToIfFalse(target, reg) => {
                write!(
                    f,
                    "{:10} {}, {}",
                    "GoToIfNot".cyan().bold(),
                    jt!(target),
                    r!(reg)
                )
            }
            Halt => write!(f, "{}", "Halt".cyan().bold()),
        }
    }
}

struct ColoredMoveOp<'a>(&'a MoveOperation);

impl fmt::Display for ColoredMoveOp<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            MoveOperation::First => write!(f, "First"),
            MoveOperation::Next => write!(f, "Next"),
            MoveOperation::Last => write!(f, "Last"),
            MoveOperation::Find(reg) => write!(f, "Find({})", ColoredReg(*reg)),
        }
    }
}
