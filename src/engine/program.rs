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
}

// TODO: switch to using {} and named members
#[derive(Clone, Debug)]
pub enum Operation {
    // Value
    StoreValue(Reg, ScalarValue),
    IncrementValue(Reg),            // Reg = Reg + 1
    DecrementValue(Reg),            // Reg = Reg - 1
    AddValue(Reg, Reg, Reg),        // Reg = Reg + Reg
    SubtractValue(Reg, Reg, Reg),   // Reg = Reg - Reg
    MultiplyValue(Reg, Reg, Reg),   // Reg = Reg * Reg
    DivideValue(Reg, Reg, Reg),          // Reg = Reg / Reg
    RemainderValue(Reg, Reg, Reg),       // Reg = Reg % Reg
    LessThanValue(Reg, Reg, Reg),        // Reg = Reg < Reg
    LessThanOrEqualValue(Reg, Reg, Reg), // Reg = Reg <= Reg
    GreaterThanValue(Reg, Reg, Reg),     // Reg = Reg > Reg
    GreaterThanOrEqualValue(Reg, Reg, Reg), // Reg = Reg >= Reg
    EqualsValue(Reg, Reg, Reg),          // Reg = Reg == Reg
    NotEqualsValue(Reg, Reg, Reg),       // Reg = Reg != Reg
    AndValue(Reg, Reg, Reg),             // Reg = Reg && Reg
    OrValue(Reg, Reg, Reg),              // Reg = Reg || Reg
    NotValue(Reg, Reg),                  // Reg = !Reg
    NegateValue(Reg, Reg),               // Reg = -Reg (arithmetic negation)
    CopyValue(Reg, Reg),                 // Reg = Reg (copy value)

    // Db
    Open(Reg, String),
    MoveCursor(Reg, MoveOperation),
    ReadCursor(Vec<Reg>, Reg), // TODO: allow program to select which columns to read and type check
    CanReadCursor(Reg, Reg),   // Reg = CanReadCursor(Reg)

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
            LessThanOrEqualValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Le".cyan().bold(), d, a, b),
            GreaterThanValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Gt".cyan().bold(), d, a, b),
            GreaterThanOrEqualValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Ge".cyan().bold(), d, a, b),
            EqualsValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Eq".cyan().bold(), d, a, b),
            NotEqualsValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Ne".cyan().bold(), d, a, b),
            AndValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "And".cyan().bold(), d, a, b),
            OrValue(d, a, b) => write!(f, "{:10} {}, {}, {}", "Or".cyan().bold(), d, a, b),
            NotValue(d, s) => write!(f, "{:10} {}, {}", "Not".cyan().bold(), d, s),
            NegateValue(d, s) => write!(f, "{:10} {}, {}", "Neg".cyan().bold(), d, s),
            CopyValue(d, s) => write!(f, "{:10} {}, {}", "Copy".cyan().bold(), d, s),

            // Database operations
            Open(r, table) => {
                use colored::Colorize;
                write!(f, "{:10} {}, {}", "Open".cyan().bold(), r, format!("\"{}\"", table).green())
            }
            MoveCursor(r, op) => write!(f, "{:10} {}, {}", "MoveCursor".cyan().bold(), r, op),
            ReadCursor(regs, cursor) => {
                let regs_str: Vec<String> = regs.iter().map(|r| format!("{}", r)).collect();
                write!(f, "{:10} [{}], {}", "ReadCursor".cyan().bold(), regs_str.join(", "), cursor)
            }
            CanReadCursor(dest, cursor) => write!(f, "{:10} {}, {}", "CanRead".cyan().bold(), dest, cursor),

            // Control flow
            Yield(regs) => {
                let regs_str: Vec<String> = regs.iter().map(|r| format!("{}", r)).collect();
                write!(f, "{:10} [{}]", "Yield".cyan().bold(), regs_str.join(", "))
            }
            GoTo(target) => write!(f, "{:10} {}", "GoTo".cyan().bold(), target),
            GoToIfEqualValue(target, a, b) => {
                write!(f, "{:10} {}, {}, {}", "GoToIfEq".cyan().bold(), target, a, b)
            }
            GoToIfFalse(target, r) => write!(f, "{:10} {}, {}", "GoToIfNot".cyan().bold(), target, r),
            Halt => write!(f, "{}", "Halt".cyan().bold()),
        }
    }
}
